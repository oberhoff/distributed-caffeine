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
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.LazyInitializer;
import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.bson.BsonObjectId;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.changestream.OperationType.DELETE;
import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static com.mongodb.client.model.changestream.OperationType.REPLACE;
import static com.mongodb.client.model.changestream.OperationType.UPDATE;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.CACHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.EVICTED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.EXPIRES;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.HASH;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.ID;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.KEY;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.STATUS;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.TOUCHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.VALUE;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.bson.BsonType.BINARY;
import static org.bson.BsonType.DATE_TIME;
import static org.bson.BsonType.DOCUMENT;
import static org.bson.BsonType.INT32;
import static org.bson.BsonType.OBJECT_ID;
import static org.bson.BsonType.STRING;

class InternalChangeStreamWatcher<K, V> implements LazyInitializer<K, V> {

    private final static String DOCUMENT_KEY = "documentKey";
    private final static String CLUSTER_TIME = "clusterTime";
    private final static String OPERATION_TYPE = "operationType";
    private final static String FULL_DOCUMENT = "fullDocument";

    private DistributedCaffeine<K, V> distributedCaffeine;
    private Logger logger;
    private MongoCollection<Document> mongoCollection;
    private InternalDocumentConverter<K, V> documentConverter;
    private Serializer<K, ?> keySerializer;
    private Serializer<V, ?> valueSerializer;
    private AtomicBoolean isActivated;
    private AtomicReference<Throwable> failFastThrowable;
    private AtomicReference<BsonTimestamp> operationTime;
    private ExecutorService executorService;
    private CompletableFuture<Void> watcherCompletableFuture;

    InternalChangeStreamWatcher() {
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.distributedCaffeine = distributedCaffeine;
        this.logger = distributedCaffeine.getLogger();
        this.mongoCollection = distributedCaffeine.getMongoCollection();
        this.documentConverter = distributedCaffeine.getDocumentConverter();
        this.keySerializer = distributedCaffeine.getKeySerializer();
        this.valueSerializer = distributedCaffeine.getValueSerializer();
        this.isActivated = new AtomicBoolean(false);
        this.failFastThrowable = new AtomicReference<>(null);
        this.operationTime = new AtomicReference<>(null);
    }

    void activate() {
        // wait for completion if required
        Optional.ofNullable(watcherCompletableFuture)
                .ifPresent(CompletableFuture::join);

        executorService = Executors.newSingleThreadExecutor(runnable ->
                new Thread(runnable, Thread.class.getSimpleName()
                        .concat(getClass().getSimpleName())
                        .concat(String.valueOf(runnable.hashCode()))));

        scheduleChangeStreamWatcher();

        // wait until watching is activated or throw exception after timeout, but fail fast if watching is not possible
        Duration timeoutDuration = Duration.ofMinutes(1);
        Fallback<Boolean> fallback = Fallback.<Boolean>builderOfException(event ->
                        new DistributedCaffeineException(format("Watching change streams failed for collection '%s'",
                                mongoCollection.getNamespace().getCollectionName()), failFastThrowable.get()))
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

        // wait async for completion if required and shut down executor service
        Optional.ofNullable(watcherCompletableFuture)
                .ifPresent(completableFuture ->
                        CompletableFuture.runAsync(() -> {
                            completableFuture.join();
                            executorService.shutdownNow();
                        }));
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
                .withDelay(Duration.ofSeconds(1))
                .withDelayFnOn(context -> Duration.ofSeconds(Math.min(context.getAttemptCount(), 10)), Exception.class)
                .onRetryScheduled(event -> Optional.ofNullable(event.getLastException())
                        .ifPresent(exception -> logger.log(Level.WARNING,
                                format("Watching change streams failed for collection '%s'. Retrying...",
                                        mongoCollection.getNamespace().getCollectionName()), exception)))
                .build();
        watcherCompletableFuture = Failsafe.with(retryPolicy)
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
            do { // do-while-loop to break earlier
                ChangeStreamDocument<Document> changeStreamDocument = cursor.tryNext();
                if (nonNull(changeStreamDocument)) {
                    // set operation time to be used if watching fails and is retried
                    operationTime.set(changeStreamDocument.getClusterTime());
                    processChangeStreamDocument(changeStreamDocument);
                }
            } while (isActivated());
        }
    }

    private void processChangeStreamDocument(ChangeStreamDocument<Document> changeStreamDocument) {
        Optional<ObjectId> optionalObjectId = Optional.ofNullable(changeStreamDocument.getDocumentKey())
                .map(bsonDocument -> bsonDocument.getObjectId(ID, null))
                .map(BsonObjectId::getValue);
        try {
            OperationType operationType = changeStreamDocument.getOperationType();
            if (nonNull(operationType)) {
                if (operationType == INSERT) {
                    distributedCaffeine.processInboundInsert(
                            documentConverter.toCacheDocument(
                                    changeStreamDocument.getFullDocument()), true);
                } else if (operationType == UPDATE || operationType == REPLACE) {
                    distributedCaffeine.processInboundUpdate(
                            documentConverter.toCacheDocument(
                                    changeStreamDocument.getFullDocument()));
                } else if (operationType == DELETE) {
                    optionalObjectId.ifPresent(distributedCaffeine::processInboundDelete);
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING,
                    format("Deserializing of cache entry failed for collection '%s' (%s). Skipping...",
                            mongoCollection.getNamespace().getCollectionName(),
                            nonNull(changeStreamDocument.getFullDocument())
                                    ? changeStreamDocument.getFullDocument()
                                    : changeStreamDocument), e);
            optionalObjectId.ifPresent(distributedCaffeine::processInboundFailure);
        }
    }

    private List<Bson> getAggregationPipeline() {
        // watch only change stream documents of interest (specific operation types, fields and values)
        return List.of(
                matchWithoutNull(
                        orWithoutNull(
                                andWithoutNull(
                                        Filters.eq(OPERATION_TYPE, INSERT.getValue()),
                                        Filters.exists(FULL_DOCUMENT),
                                        Filters.ne(FULL_DOCUMENT, null),
                                        Filters.exists(fullDocument(ID)),
                                        Filters.type(fullDocument(ID), OBJECT_ID),
                                        Filters.ne(fullDocument(ID), null),
                                        Filters.exists(fullDocument(HASH)),
                                        Filters.type(fullDocument(HASH), INT32),
                                        Filters.ne(fullDocument(HASH), null),
                                        Filters.exists(fullDocument(KEY)),
                                        Filters.type(fullDocument(KEY), getBsonTypeForKey()),
                                        Filters.ne(fullDocument(KEY), null),
                                        Filters.exists(fullDocument(VALUE)),
                                        orWithoutNull( // no filterOnlyIf needed because status is filtered
                                                andWithoutNull(
                                                        Filters.type(fullDocument(VALUE), getBsonTypeForValue()),
                                                        Filters.ne(fullDocument(VALUE), null),
                                                        Filters.in(fullDocument(STATUS), CACHED, EVICTED)),
                                                andWithoutNull(
                                                        Filters.eq(fullDocument(VALUE), null),
                                                        Filters.eq(fullDocument(STATUS), INVALIDATED))),
                                        Filters.exists(fullDocument(STATUS)),
                                        Filters.type(fullDocument(STATUS), STRING),
                                        Filters.in(fullDocument(STATUS), filterStatus(CACHED, INVALIDATED, EVICTED)),
                                        Filters.exists(fullDocument(TOUCHED)),
                                        Filters.type(fullDocument(TOUCHED), DATE_TIME),
                                        Filters.ne(fullDocument(TOUCHED), null),
                                        Filters.exists(fullDocument(EXPIRES)),
                                        orWithoutNull( // no filterOnlyIf needed because status is filtered
                                                andWithoutNull(
                                                        Filters.eq(fullDocument(EXPIRES), null),
                                                        Filters.eq(fullDocument(STATUS), CACHED)),
                                                andWithoutNull(
                                                        Filters.type(fullDocument(EXPIRES), DATE_TIME),
                                                        Filters.ne(fullDocument(EXPIRES), null),
                                                        Filters.in(fullDocument(STATUS), INVALIDATED, EVICTED)))),
                                filterOnlyIf(distributedCaffeine.isSupportedByDistributionMode(CACHED),
                                        andWithoutNull(
                                                Filters.in(OPERATION_TYPE, UPDATE.getValue(), REPLACE.getValue()),
                                                Filters.exists(FULL_DOCUMENT),
                                                Filters.ne(FULL_DOCUMENT, null),
                                                Filters.exists(fullDocument(ID)),
                                                Filters.type(fullDocument(ID), OBJECT_ID),
                                                Filters.ne(fullDocument(ID), null),
                                                Filters.exists(fullDocument(HASH)),
                                                Filters.type(fullDocument(HASH), INT32),
                                                Filters.ne(fullDocument(HASH), null),
                                                Filters.exists(fullDocument(KEY)),
                                                Filters.type(fullDocument(KEY), getBsonTypeForKey()),
                                                Filters.ne(fullDocument(KEY), null),
                                                Filters.exists(fullDocument(VALUE)),
                                                Filters.type(fullDocument(VALUE), getBsonTypeForValue()),
                                                Filters.ne(fullDocument(VALUE), null),
                                                Filters.exists(fullDocument(STATUS)),
                                                Filters.type(fullDocument(STATUS), STRING),
                                                Filters.eq(fullDocument(STATUS), CACHED),
                                                Filters.exists(fullDocument(TOUCHED)),
                                                Filters.type(fullDocument(TOUCHED), DATE_TIME),
                                                Filters.ne(fullDocument(TOUCHED), null),
                                                Filters.exists(fullDocument(EXPIRES)),
                                                Filters.eq(fullDocument(EXPIRES), null))),
                                filterOnlyIf(distributedCaffeine.isSupportedByDistributionMode(CACHED),
                                        Filters.eq(OPERATION_TYPE, DELETE.getValue())))),
                Aggregates.project(
                        Projections.fields(
                                Projections.include(DOCUMENT_KEY, CLUSTER_TIME, OPERATION_TYPE,
                                        fullDocument(ID), fullDocument(HASH), fullDocument(KEY), fullDocument(VALUE),
                                        fullDocument(STATUS), fullDocument(TOUCHED), fullDocument(EXPIRES)))));
    }

    private String fullDocument(String fieldName) {
        return format("%s.%s", FULL_DOCUMENT, fieldName);
    }

    private Bson andWithoutNull(Bson... filters) {
        List<Bson> filterList = Stream.of(filters)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return filterList.isEmpty()
                ? null
                : Filters.and(filterList);
    }

    private Bson orWithoutNull(Bson... filters) {
        List<Bson> filterList = Stream.of(filters)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return filterList.isEmpty()
                ? null
                : Filters.or(filterList);
    }

    private Bson matchWithoutNull(Bson filter) {
        return Aggregates.match(isNull(filter)
                ? Filters.empty()
                : filter);
    }

    private Bson filterOnlyIf(boolean condition, Bson filter) {
        return condition ? filter : null;
    }

    private String[] filterStatus(String... statuses) {
        return Stream.of(statuses)
                .filter(distributedCaffeine::isSupportedByDistributionMode)
                .toArray(String[]::new);
    }

    private BsonType getBsonTypeForKey() {
        return getBsonType(keySerializer);
    }

    private BsonType getBsonTypeForValue() {
        return getBsonType(valueSerializer);
    }

    @SuppressWarnings("unchecked")
    private <T> BsonType getBsonType(Serializer<T, ?> serializer) {
        if (serializer instanceof ByteArraySerializer) {
            return BINARY;
        } else if (serializer instanceof JsonSerializer) {
            JsonSerializer<T> jsonSerializer = (JsonSerializer<T>) serializer;
            if (jsonSerializer.storeAsBson()) {
                return DOCUMENT;
            } else {
                return STRING;
            }
        } else if (serializer instanceof StringSerializer) {
            return STRING;
        } else {
            throw new DistributedCaffeineException(format("Unknown serializer '%s'",
                    serializer.getClass().getName()));
        }
    }
}
