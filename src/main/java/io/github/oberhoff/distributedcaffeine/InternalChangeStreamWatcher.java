/*
 * Copyright © 2023-2026 Dr. Andreas Oberhoff (All rights reserved)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.oberhoff.distributedcaffeine;

import com.mongodb.MongoClientException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field;
import org.bson.BsonObjectId;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field._ID;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalChangeStreamWatcher<K, V> implements InternalLazyInitializer<K, V> {

    private static final Duration WATCHER_INTERVAL = Duration.ofSeconds(1);
    private static final String DOCUMENT_KEY = "documentKey";
    private static final String CLUSTER_TIME = "clusterTime";
    private static final String OPERATION_TYPE = "operationType";
    private static final String FULL_DOCUMENT = "fullDocument";

    private final AtomicBoolean isActivated;
    private final AtomicReference<Throwable> failFastThrowable;
    private final AtomicReference<BsonTimestamp> operationTime;

    private Logger logger;
    private MongoCollection<Document> mongoCollection;
    private InternalCacheManager<K, V> cacheManager;
    private InternalDocumentConverter<K, V> documentConverter;
    private CompletableFuture<Void> watcherCompletableFuture;

    InternalChangeStreamWatcher() {
        this.isActivated = new AtomicBoolean(false);
        this.failFastThrowable = new AtomicReference<>(null);
        this.operationTime = new AtomicReference<>(null);
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.logger = distributedCaffeine.getLogger();
        this.mongoCollection = distributedCaffeine.getMongoCollection();
        this.cacheManager = distributedCaffeine.getCacheManager();
        this.documentConverter = distributedCaffeine.getDocumentConverter();
    }

    void activate() {
        // wait for completion if required
        Optional.ofNullable(watcherCompletableFuture)
                .ifPresent(CompletableFuture::join);

        scheduleChangeStreamWatcher();

        // wait until watching is activated or throw exception after timeout, but fail fast if watching is not possible
        Duration timeoutDuration = Optional.ofNullable(mongoCollection.getTimeout(TimeUnit.SECONDS))
                .filter(seconds -> seconds > 0)
                .map(Duration::ofSeconds)
                .orElseGet(() -> Duration.ofSeconds(30)); // default MongoDB timeout
        Fallback<Boolean> fallback = Fallback.<Boolean>builderOfException(event ->
                        new MongoClientException(format("Watching change streams failed for collection '%s'",
                                mongoCollection.getNamespace().getCollectionName()),
                                Optional.ofNullable(failFastThrowable.get())
                                        .orElseGet(() -> new MongoTimeoutException(format("Timeout after %s seconds",
                                                timeoutDuration.toSeconds())))))
                .handleResult(false)
                .build();
        RetryPolicy<Boolean> retryPolicy = RetryPolicy.<Boolean>builder()
                .handleResult(false)
                .abortIf(result -> nonNull(failFastThrowable.get()))
                .withMaxAttempts(-1)
                .withMaxDuration(timeoutDuration)
                .build();
        Failsafe.with(fallback, retryPolicy)
                .get(this::isActivated);
    }

    void deactivate() {
        isActivated.set(false);
        failFastThrowable.set(null);
        operationTime.set(null);
    }

    boolean isActivated() {
        return isActivated.get();
    }

    private void scheduleChangeStreamWatcher() {
        RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                .abortOn(throwable -> {
                    failFastThrowable.set(throwable);
                    return !isActivated();
                })
                .withMaxAttempts(-1)
                .withDelay(WATCHER_INTERVAL)
                .withDelayFnOn(context -> WATCHER_INTERVAL.multipliedBy(min(context.getAttemptCount(), 10)),
                        Throwable.class)
                .onRetryScheduled(event -> Optional.ofNullable(event.getLastException())
                        .ifPresent(throwable -> logger.log(Level.WARNING,
                                format("Watching change streams failed for collection '%s'. Retrying...",
                                        mongoCollection.getNamespace().getCollectionName()), throwable)))
                .build();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        InternalTaskPolicy<Void> taskPolicy = new InternalTaskPolicy<Void>()
                .withPostExecutionTask(result -> executorService.shutdown());
        watcherCompletableFuture = Failsafe.with(taskPolicy, retryPolicy)
                .with(executorService)
                .runAsync(this::processChangeStreams);
    }

    private void processChangeStreams() {
        // get change stream iterable based on optional operation time
        ChangeStreamIterable<Document> changeStreamIterable;
        if (nonNull(operationTime.get())) {
            changeStreamIterable = mongoCollection.watch(getAggregationPipeline())
                    .startAtOperationTime(operationTime.get())
                    .fullDocument(FullDocument.UPDATE_LOOKUP);
        } else {
            changeStreamIterable = mongoCollection.watch(getAggregationPipeline())
                    .fullDocument(FullDocument.UPDATE_LOOKUP);
        }
        // get the cursor to iterate over inbound change stream documents
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor =
                     changeStreamIterable.cursor()) {
            isActivated.set(true);
            while (isActivated()) {
                ChangeStreamDocument<Document> changeStreamDocument = cursor.tryNext();
                // additional activation check necessary because tryNext() seems to be paced (blocked for a while)
                if (nonNull(changeStreamDocument) && isActivated()) {
                    // set operation time to be used if watching fails and is retried
                    operationTime.set(requireNonNull(changeStreamDocument.getClusterTime(),
                            "clusterTime cannot be null"));
                    processChangeStreamDocument(changeStreamDocument);
                }
            }
        }
    }

    private void processChangeStreamDocument(ChangeStreamDocument<Document> changeStreamDocument) {
        Optional<ObjectId> optionalObjectId = Optional.ofNullable(changeStreamDocument.getDocumentKey())
                .map(bsonDocument -> bsonDocument.getObjectId(_ID.toString(), null))
                .map(BsonObjectId::getValue);
        try {
            OperationType operationType = changeStreamDocument.getOperationType();
            if (nonNull(operationType) && operationType.equals(INSERT)) {
                cacheManager.manageInboundInsert(
                        documentConverter.toCacheDocument(changeStreamDocument.getFullDocument()), true);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING,
                    format("Deserializing of cache entry failed for collection '%s' (%s). Skipping...",
                            mongoCollection.getNamespace().getCollectionName(),
                            nonNull(changeStreamDocument.getFullDocument())
                                    ? changeStreamDocument.getFullDocument()
                                    : changeStreamDocument), e);
            optionalObjectId.ifPresent(cacheManager::manageInboundFailure);
        }
    }

    private List<Bson> getAggregationPipeline() {
        List<String> projectionFields = new ArrayList<>();
        projectionFields.add(DOCUMENT_KEY);
        projectionFields.add(CLUSTER_TIME);
        projectionFields.add(OPERATION_TYPE);
        projectionFields.addAll(Stream.of(Field.values())
                .map(this::fullDocument)
                .toList());
        return List.of(
                Aggregates.match(
                        Filters.and(
                                // TODO add discriminator to filter (depending of connection/watcher is shared)
                                Filters.eq(OPERATION_TYPE, INSERT.getValue()))),
                Aggregates.project(
                        Projections.fields(
                                Projections.include(projectionFields))));
    }

    private String fullDocument(Field field) {
        return format("%s.%s", FULL_DOCUMENT, field.toString());
    }
}
