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
package io.github.oberhoff.distributedcaffeine.adapter.mongodb;

import com.mongodb.MongoClientException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
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
import io.github.oberhoff.distributedcaffeine.adapter.AbstractSynchronizer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static com.mongodb.client.model.changestream.OperationType.UPDATE;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

@NullMarked
final class MongoSynchronizer<K, V> extends AbstractSynchronizer<K, V> {

    private static final Logger LOGGER = System.getLogger(MongoSynchronizer.class.getName());

    private static final Duration WATCHER_INTERVAL = Duration.ofSeconds(1);
    private static final String DOCUMENT_KEY = "documentKey";
    private static final String CLUSTER_TIME = "clusterTime";
    private static final String OPERATION_TYPE = "operationType";
    private static final String FULL_DOCUMENT = "fullDocument";

    private final MongoCollection<Document> mongoCollection;
    private final AtomicBoolean isActivated;
    private final AtomicReference<@Nullable Throwable> failFastThrowable;
    private final AtomicReference<@Nullable BsonTimestamp> operationTime;

    private @Nullable CompletableFuture<Void> watcherCompletableFuture;

    MongoSynchronizer(MongoClient mongoClient, String databaseName, String collectionName) {
        this.mongoCollection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
        this.isActivated = new AtomicBoolean(false);
        this.failFastThrowable = new AtomicReference<>(null);
        this.operationTime = new AtomicReference<>(null);
        // TODO connection sharing
    }

    @Override
    public void activate() {
        // wait for completion if required
        Optional.ofNullable(watcherCompletableFuture)
                .filter(future -> !future.isDone())
                .ifPresent(CompletableFuture::join);

        scheduleChangeStreamWatcher();

        // wait until watching is activated or throw exception after timeout, but fail fast if watching is not possible
        Duration timeoutDuration = Optional.ofNullable(mongoCollection.getTimeout(TimeUnit.SECONDS))
                .filter(seconds -> seconds > 0)
                .map(Duration::ofSeconds)
                .orElseGet(() -> Duration.ofSeconds(30)); // default MongoDB timeout
        Fallback<Boolean> fallback = Fallback.<Boolean>builderOfException(event ->
                        new MongoClientException(format("Watching change streams failed for cache at '%s'",
                                identifier), Optional.ofNullable(failFastThrowable.get())
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

    @Override
    public void deactivate() {
        isActivated.set(false);
        failFastThrowable.set(null);
        operationTime.set(null);
    }

    @Override
    public boolean isActivated() {
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
                        .ifPresent(throwable -> LOGGER.log(Level.WARNING,
                                format("Watching change streams failed for cache at '%s'. Retrying...",
                                        identifier), throwable)))
                .build();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        watcherCompletableFuture = Failsafe.with(retryPolicy)
                .with(executorService)
                .runAsync(this::processChangeStreams)
                .whenComplete((result, throwable) -> executorService.shutdown());
    }

    private void processChangeStreams() {
        // get change stream iterable based on optional operation time
        ChangeStreamIterable<Document> changeStreamIterable;
        if (nonNull(operationTime.get())) {
            changeStreamIterable = mongoCollection.watch(getAggregationPipeline())
                    .startAtOperationTime(requireNonNull(operationTime.get()))
                    .fullDocument(FullDocument.UPDATE_LOOKUP);
        } else {
            changeStreamIterable = mongoCollection.watch(getAggregationPipeline())
                    .fullDocument(FullDocument.UPDATE_LOOKUP);
        }
        // get the cursor to iterate over inbound change stream documents
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStreamIterable.cursor()) {
            isActivated.set(true);
            while (isActivated()) {
                ChangeStreamDocument<Document> changeStreamDocument = cursor.tryNext();
                // additional activation check necessary because tryNext() seems to be paced (blocked for a while)
                if (nonNull(changeStreamDocument) && isActivated()) {
                    // set operation time to be used if watching fails and is retried
                    operationTime.set(changeStreamDocument.getClusterTime());
                    processChangeStreamDocument(changeStreamDocument);
                }
            }
        }
    }

    private void processChangeStreamDocument(ChangeStreamDocument<Document> changeStreamDocument) {
        OperationType operationType = changeStreamDocument.getOperationType();
        if (nonNull(changeStreamDocument.getFullDocument()) && nonNull(operationType)
                && (operationType.equals(INSERT) || operationType.equals(UPDATE))) {
            CacheEntry<K, V> cacheEntry = MongoRepository.toCacheEntryOrNull(requireNonNull(keySerializer),
                    requireNonNull(valueSerializer), changeStreamDocument.getFullDocument(),
                    LOGGER, requireNonNull(identifier));
            Optional.ofNullable(cacheEntry)
                    .map(Set::of)
                    .ifPresent(cacheEntries -> requireNonNull(retriever).retrieveCacheEntries(cacheEntries));
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
                                Filters.in(OPERATION_TYPE, INSERT.getValue(), UPDATE.getValue())
                                // TODO add discriminator to filter (depending of connection/watcher is shared)
                        )),
                Aggregates.project(
                        Projections.fields(
                                Projections.include(projectionFields))));
    }

    private String fullDocument(Field field) {
        return format("%s.%s", FULL_DOCUMENT, field);
    }
}
