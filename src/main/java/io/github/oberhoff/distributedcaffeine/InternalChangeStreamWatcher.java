/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.DISCRIMINATOR;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.EXPIRES;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.HASH;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.KEY;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.ORIGIN;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.STALE;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.STATUS;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.TOUCHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.VALUE;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field._ID;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status;
import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.bson.BsonType.BOOLEAN;
import static org.bson.BsonType.DATE_TIME;
import static org.bson.BsonType.INT32;
import static org.bson.BsonType.INT64;
import static org.bson.BsonType.OBJECT_ID;
import static org.bson.BsonType.STRING;

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
    private DistributionMode distributionMode;
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
        this.distributionMode = distributedCaffeine.getDistributionMode();
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
                .withDelayFnOn(context -> WATCHER_INTERVAL.multipliedBy(Math.min(context.getAttemptCount(), 10)),
                        Throwable.class)
                .onRetryScheduled(event -> Optional.ofNullable(event.getLastException())
                        .ifPresent(throwable -> logger.log(Level.WARNING,
                                format("Watching change streams failed for collection '%s'. Retrying...",
                                        mongoCollection.getNamespace().getCollectionName()), throwable)))
                .build();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        InternalTaskPolicy<Void> taskPolicy = new InternalTaskPolicy<Void>()
                .withPostExecutionTask(executorService::shutdown);
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
        // watch only change stream documents of interest (specific operation types, fields or values)
        return List.of(
                Aggregates.match(
                        Filters.and(
                                Filters.eq(OPERATION_TYPE, INSERT.getValue()),
                                Filters.exists(FULL_DOCUMENT),
                                Filters.ne(FULL_DOCUMENT, null),
                                Filters.exists(fullDocument(_ID)),
                                Filters.type(fullDocument(_ID), OBJECT_ID),
                                Filters.ne(fullDocument(_ID), null),
                                Filters.exists(fullDocument(DISCRIMINATOR)),
                                Filters.or(
                                        Filters.eq(fullDocument(DISCRIMINATOR), null),
                                        Filters.and(
                                                Filters.ne(fullDocument(DISCRIMINATOR), null),
                                                Filters.type(fullDocument(DISCRIMINATOR), STRING))),
                                Filters.exists(fullDocument(ORIGIN)),
                                Filters.type(fullDocument(ORIGIN), INT64),
                                Filters.ne(fullDocument(ORIGIN), null),
                                Filters.exists(fullDocument(HASH)),
                                Filters.type(fullDocument(HASH), INT32),
                                Filters.ne(fullDocument(HASH), null),
                                Filters.exists(fullDocument(KEY)),
                                Filters.ne(fullDocument(KEY), null),
                                Filters.exists(fullDocument(VALUE)),
                                Filters.or(
                                        Filters.and(
                                                Filters.ne(fullDocument(VALUE), null),
                                                Filters.in(fullDocument(STATUS),
                                                        aggregateStatusesToStrings(
                                                                Status.CACHED_GROUP,
                                                                Status.EVICTED_GROUP))),
                                        Filters.and(
                                                Filters.eq(fullDocument(VALUE), null),
                                                Filters.in(fullDocument(STATUS),
                                                        aggregateStatusesToStrings(
                                                                Status.INVALIDATED_GROUP)))),
                                Filters.exists(fullDocument(STATUS)),
                                Filters.type(fullDocument(STATUS), STRING),
                                Filters.in(fullDocument(STATUS),
                                        filterStatuses(aggregateStatuses(
                                                Status.CACHED_GROUP,
                                                Status.INVALIDATED_GROUP,
                                                Status.EVICTED_GROUP))),
                                Filters.exists(fullDocument(STALE)),
                                Filters.type(fullDocument(STALE), BOOLEAN),
                                Filters.ne(fullDocument(STALE), null),
                                Filters.exists(fullDocument(TOUCHED)),
                                Filters.type(fullDocument(TOUCHED), DATE_TIME),
                                Filters.ne(fullDocument(TOUCHED), null),
                                Filters.exists(fullDocument(EXPIRES)),
                                Filters.or(
                                        Filters.and(
                                                Filters.eq(fullDocument(EXPIRES), null),
                                                Filters.in(fullDocument(STATUS),
                                                        aggregateStatusesToStrings(
                                                                Status.CACHED_GROUP))),
                                        Filters.and(
                                                Filters.ne(fullDocument(EXPIRES), null),
                                                Filters.type(fullDocument(EXPIRES), DATE_TIME),
                                                Filters.in(fullDocument(STATUS),
                                                        aggregateStatusesToStrings(
                                                                Status.INVALIDATED_GROUP,
                                                                Status.EVICTED_GROUP)))))),
                Aggregates.project(
                        Projections.fields(
                                Projections.include(DOCUMENT_KEY, CLUSTER_TIME, OPERATION_TYPE,
                                        fullDocument(_ID), fullDocument(DISCRIMINATOR), fullDocument(ORIGIN),
                                        fullDocument(HASH), fullDocument(KEY), fullDocument(VALUE),
                                        fullDocument(STATUS), fullDocument(STALE), fullDocument(TOUCHED),
                                        fullDocument(EXPIRES)))));
    }

    private String fullDocument(Field field) {
        return format("%s.%s", FULL_DOCUMENT, field.toString());
    }

    private Status[] aggregateStatuses(Status[]... statuses) {
        return Stream.of(statuses)
                .flatMap(Stream::of)
                .toArray(Status[]::new);
    }

    private String[] aggregateStatusesToStrings(Status[]... statuses) {
        return Stream.of(aggregateStatuses(statuses))
                .map(Status::toString)
                .toArray(String[]::new);
    }

    private String[] filterStatuses(Status... statuses) {
        return Stream.of(statuses)
                .filter(status -> status.isConsideredBy(distributionMode))
                .map(Status::toString)
                .toArray(String[]::new);
    }
}
