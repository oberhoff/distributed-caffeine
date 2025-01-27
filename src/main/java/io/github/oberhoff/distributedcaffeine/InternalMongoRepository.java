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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.io.Closeable;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED_GROUP;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_EXTENDED_GROUP;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_SIZE_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_TIME_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.runFailable;
import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

class InternalMongoRepository<K, V> implements InternalLazyInitializer<K, V> {

    private final byte[] random;
    private final int offset;

    private int time;
    private int counter;
    private Logger logger;
    private MongoCollection<Document> mongoCollection;
    private InternalDocumentConverter<K, V> documentConverter;
    private InternalExtendedPersistence extendedPersistence;
    private Long origin;

    InternalMongoRepository() {
        this.random = new byte[5];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(this.random);
        this.offset = secureRandom.nextInt(16 * 1_000_000);
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.logger = distributedCaffeine.getLogger();
        this.mongoCollection = distributedCaffeine.getMongoCollection();
        this.documentConverter = distributedCaffeine.getDocumentConverter();
        this.extendedPersistence = distributedCaffeine.getExtendedPersistence();
        this.origin = distributedCaffeine.getOrigin();

        ensureIndexes();
    }

    Stream<InternalCacheDocument<K, V>> streamCacheDocuments(Bson filter) {
        return createStreamFromClosableIterator(mongoCollection
                .find()
                .filter(filter)
                .iterator())
                .map(this::toCacheDocumentOrNull)
                .filter(Objects::nonNull);
    }

    @SuppressWarnings("resource")
    Stream<Set<InternalCacheDocument<K, V>>> streamCacheDocumentsGroupedByKeyInReverseOrder(Bson filter) {
        MongoCursor<Document> mongoCursor = mongoCollection
                .find()
                .filter(filter)
                .sort(Sorts.ascending(HASH.toString()))
                .iterator();
        Set<Integer> hashes = new LinkedHashSet<>();
        Set<InternalCacheDocument<K, V>> cacheDocuments = new HashSet<>();
        return Stream.generate(() -> {
                    while (mongoCursor.hasNext() && hashes.size() <= 1) {
                        InternalCacheDocument<K, V> cacheDocument = toCacheDocumentOrNull(mongoCursor.next());
                        if (nonNull(cacheDocument)) {
                            hashes.add(cacheDocument.getHash());
                            cacheDocuments.add(cacheDocument);
                        }
                    }
                    Map<K, Set<InternalCacheDocument<K, V>>> keyToCacheDocuments = new HashMap<>();
                    hashes.stream()
                            .findFirst()
                            .ifPresent(hash -> {
                                keyToCacheDocuments.putAll(cacheDocuments.stream()
                                        .filter(it -> Objects.equals(hash, it.getHash()))
                                        .collect(groupingBy(InternalCacheDocument::getKey, toSet())));
                                hashes.remove(hash);
                                cacheDocuments.removeIf(it -> Objects.equals(hash, it.getHash()));
                            });
                    return keyToCacheDocuments;
                })
                .takeWhile(keyToCacheDocuments -> {
                    if (keyToCacheDocuments.isEmpty()) {
                        mongoCursor.close();
                        return false;
                    } else {
                        return true;
                    }
                })
                .flatMap(keyToCacheDocuments -> keyToCacheDocuments.values().stream())
                .map(it -> it.stream()
                        .sorted(Comparator.reverseOrder())
                        .collect(Collectors.toCollection(LinkedHashSet::new)));
    }

    Set<InternalCacheDocument<K, V>> insert(Map<? extends K, ? extends V> map, Status status) {
        if (!map.isEmpty()) {
            Map<K, ObjectId> keyToObjectId = new HashMap<>();
            List<UpdateOneModel<Document>> updates = new ArrayList<>();
            Map<Integer, K> indexToKey = new HashMap<>();
            AtomicInteger index = new AtomicInteger(0);
            Failsafe.with(RetryPolicy.ofDefaults())
                    .run(context -> {
                        if (context.getLastException() instanceof MongoBulkWriteException) {
                            MongoBulkWriteException mongoBulkWriteException = context.getLastException();
                            BulkWriteResult bulkWriteResult = mongoBulkWriteException.getWriteResult();
                            keyToObjectId.putAll(evaluateBulkWriteResult(indexToKey, bulkWriteResult));
                        }
                        updates.clear();
                        indexToKey.clear();
                        index.set(0);
                        map.keySet().stream()
                                .filter(key -> !keyToObjectId.containsKey(key))
                                .forEach(key -> {
                                    V value = map.get(key);
                                    requireNonNull(key, "key cannot be null");
                                    if (!status.isInvalidated()) {
                                        requireNonNull(value, "value cannot be null");
                                    }
                                    // perform upsert to be able to use $currentTime (which is an update operator only)
                                    Bson filter = Filters.eq(_ID.toString(), generateObjectId());
                                    Bson update = toMongoUpdate(key, value, status);
                                    UpdateOptions updateOptions = new UpdateOptions().upsert(true);
                                    updates.add(new UpdateOneModel<>(filter, update, updateOptions));
                                    indexToKey.put(index.getAndIncrement(), key);
                                });
                        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(true);
                        BulkWriteResult bulkWriteResult = mongoCollection.bulkWrite(updates, bulkWriteOptions);
                        keyToObjectId.putAll(evaluateBulkWriteResult(indexToKey, bulkWriteResult));
                    });
            return keyToObjectId.entrySet().stream()
                    .map(entry -> new InternalCacheDocument<K, V>()
                            .setId(entry.getValue())
                            .setHash(entry.getKey().hashCode())
                            .setKey(entry.getKey())
                            .setValue(map.get(entry.getKey()))
                            .setStatus(status))
                    .collect(toSet());
        } else {
            return Set.of();
        }
    }

    void markAsStale(Set<ObjectId> objectIds) {
        if (!objectIds.isEmpty()) {
            Failsafe.with(RetryPolicy.ofDefaults())
                    .run(() -> {
                        Bson filter = Filters.and(
                                Filters.in(_ID.toString(), objectIds),
                                Filters.eq(STALE.toString(), false));
                        Bson update = Updates.combine(
                                Updates.set(STALE.toString(), true),
                                Updates.currentDate(EXPIRES.toString()));
                        mongoCollection.updateMany(filter, update);
                    });
        }
    }

    Set<ObjectId> identifyStaleExtended(Set<Integer> hashes) {
        Set<ObjectId> objectIds = new HashSet<>();
        if (!hashes.isEmpty()) {
            Bson filter = Filters.and(
                    Filters.in(HASH.toString(), hashes),
                    Filters.in(STATUS.toString(),
                            Stream.of(CACHED_GROUP, EVICTED_EXTENDED_GROUP)
                                    .flatMap(Stream::of)
                                    .map(Objects::toString)
                                    .toArray(String[]::new)));
            try (Stream<Document> documentStream = createStreamFromClosableIterator(mongoCollection
                    .aggregate(
                            List.of(
                                    Aggregates.match(filter),
                                    Aggregates.group(format("$%s", HASH),
                                            Accumulators.sum("count", 1),
                                            Accumulators.push("documents", new Document(Map.of(
                                                    _ID.toString(), format("$%s", _ID),
                                                    KEY.toString(), format("$%s", KEY),
                                                    STATUS.toString(), format("$%s", STATUS),
                                                    TOUCHED.toString(), format("$%s", TOUCHED))))),
                                    Aggregates.match(Filters.gt("count", 1))))
                    .iterator())) {
                documentStream
                        .map(document -> document.getList("documents", Document.class))
                        .forEach(documents ->
                                objectIds.addAll(documents.stream()
                                        .map(document -> toCacheDocumentOrNull(document, _ID, KEY, STATUS, TOUCHED))
                                        .filter(Objects::nonNull)
                                        // retain "hashCode -> bucket -> equals" semantics
                                        .collect(groupingBy(InternalCacheDocument::getKey))
                                        .entrySet()
                                        .stream()
                                        .flatMap(entry -> entry.getValue().stream()
                                                .sorted(Comparator.reverseOrder())
                                                .skip(1) // newest cache entry stays untouched
                                                .map(InternalCacheDocument::getId))
                                        .collect(toSet())));
            }
        }
        return objectIds;
    }

    Set<ObjectId> identifyStaleExtendedBySize() {
        Bson filter = Filters.in(STATUS.toString(),
                Stream.of(CACHED_GROUP, EVICTED_EXTENDED_GROUP)
                        .flatMap(Stream::of)
                        .map(Objects::toString)
                        .toArray(String[]::new));
        try (Stream<Document> documentStream = createStreamFromClosableIterator(mongoCollection
                .find(filter)
                .sort(Sorts.descending(TOUCHED.toString(), _ID.toString()))
                .projection(Projections.include(_ID.toString(), KEY.toString(), STATUS.toString()))
                .iterator())) {
            AtomicInteger extendedCounter = new AtomicInteger(0);
            Set<K> newestKeys = new HashSet<>();
            Set<ObjectId> objectIds = new HashSet<>();
            documentStream
                    .map(document -> toCacheDocumentOrNull(document, _ID, KEY, STATUS))
                    .filter(Objects::nonNull)
                    .forEach(cacheDocument -> {
                        if (extendedCounter.get() < extendedPersistence.getExtendedPersistenceSize()
                                && !newestKeys.contains(cacheDocument.getKey())) {
                            newestKeys.add(cacheDocument.getKey());
                            if (cacheDocument.isEvictedExtended()) {
                                extendedCounter.incrementAndGet();
                            }
                        } else {
                            if (cacheDocument.isEvictedExtended()) {
                                objectIds.add(cacheDocument.getId());
                            }
                        }
                    });
            return objectIds;
        }
    }

    private void ensureIndexes() {
        IndexModel indexHashDiscriminator = new IndexModel(Indexes.compoundIndex(
                Indexes.ascending(HASH.toString()),
                Indexes.ascending(DISCRIMINATOR.toString())),
                new IndexOptions()
                        .unique(false)
                        .background(true));
        IndexModel indexHashStatusDiscriminator = new IndexModel(Indexes.compoundIndex(
                Indexes.ascending(HASH.toString()),
                Indexes.ascending(STATUS.toString()),
                Indexes.ascending(DISCRIMINATOR.toString())),
                new IndexOptions()
                        .unique(false)
                        .background(true));
        IndexModel indexStatusStale = new IndexModel(Indexes.compoundIndex(
                Indexes.ascending(STATUS.toString()),
                Indexes.ascending(STALE.toString())),
                new IndexOptions()
                        .unique(false)
                        .background(true));
        IndexModel indexIdStale = new IndexModel(Indexes.compoundIndex(
                Indexes.ascending(_ID.toString()),
                Indexes.ascending(STALE.toString())),
                new IndexOptions()
                        .unique(false)
                        .background(true));
        IndexModel indexStatusTouchedId = new IndexModel(Indexes.compoundIndex(
                Indexes.ascending(STATUS.toString()),
                Indexes.descending(TOUCHED.toString()),
                Indexes.descending(_ID.toString())),
                new IndexOptions()
                        .unique(false)
                        .background(true));

        List<IndexModel> indexes = List.of(
                indexHashDiscriminator,
                indexHashStatusDiscriminator,
                indexStatusTouchedId,
                indexIdStale,
                indexStatusStale);

        mongoCollection.createIndexes(indexes);

        // drop any other index
        List<BsonDocument> indexKeys = indexes.stream()
                .map(IndexModel::getKeys)
                .map(Bson::toBsonDocument)
                .collect(toCollection(ArrayList::new));
        indexKeys.add(new Document(_ID.toString(), 1).toBsonDocument());

        mongoCollection.listIndexes().forEach(existingIndex -> {
            Bson keys = existingIndex.get("key", Document.class);
            if (!indexKeys.contains(keys.toBsonDocument())) {
                mongoCollection.dropIndex(keys);
            }
        });
    }

    private <I extends Iterator<T> & Closeable, T> Stream<T> createStreamFromClosableIterator(I iterator) {
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator,
                Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false)
                .onClose(() -> runFailable(iterator::close));
    }

    private InternalCacheDocument<K, V> toCacheDocumentOrNull(Document document, Field... validationFields) {
        try {
            return documentConverter.toCacheDocument(document, validationFields);
        } catch (Exception e) {
            logger.log(Level.WARNING,
                    format("Deserializing of cache entry failed for collection '%s' (%s). Skipping...",
                            mongoCollection.getNamespace().getCollectionName(), document), e);
            return null;
        }
    }

    private Bson toMongoUpdate(K key, V value, Status status) {
        if (extendedPersistence.hasExtendedPersistence()
                && status.isEvicted() && !status.isEvictedExtended()) {
            throw new IllegalStateException(format("Status must be '%s' or '%s'",
                    EVICTED_SIZE_EXTENDED, EVICTED_TIME_EXTENDED));
        }
        try {
            Object serializedKey = documentConverter.toMongoKey(key);
            Object serializedValue = documentConverter.toMongoValue(value);
            boolean stale;
            Bson expiresUpdate;
            if (status.isInvalidated() || (status.isEvicted() && !status.isEvictedExtended())) {
                stale = true;
                expiresUpdate = Updates.currentDate(EXPIRES.toString());
            } else if (status.isEvictedExtended()) {
                stale = false;
                Instant maximumInstant = LocalDateTime.of(9999, 1, 1, 0, 0)
                        .toInstant(ZoneOffset.UTC);
                if (extendedPersistence.hasExtendedPersistenceByTime()) {
                    Instant now = Instant.now();
                    Duration extendedDuration = extendedPersistence.getExtendedPersistenceTime();
                    Duration maximumDuration = Duration.between(now, maximumInstant);
                    Instant extendedInstant = now.plus(extendedDuration.compareTo(maximumDuration) < 0
                            ? extendedDuration
                            : maximumDuration);
                    expiresUpdate = Updates.set(EXPIRES.toString(), Date.from(extendedInstant));
                } else {
                    expiresUpdate = Updates.set(EXPIRES.toString(), Date.from(maximumInstant));
                }
            } else {
                stale = false;
                expiresUpdate = Updates.set(EXPIRES.toString(), null);
            }
            return Updates.combine(
                    Updates.set(DISCRIMINATOR.toString(), null),
                    Updates.set(ORIGIN.toString(), origin),
                    Updates.set(HASH.toString(), key.hashCode()),
                    Updates.set(KEY.toString(), serializedKey),
                    Updates.set(VALUE.toString(), serializedValue),
                    Updates.set(STATUS.toString(), status.toString()),
                    Updates.set(STALE.toString(), stale),
                    Updates.currentDate(TOUCHED.toString()),
                    expiresUpdate);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Map<K, ObjectId> evaluateBulkWriteResult(Map<Integer, K> indexToKey, BulkWriteResult bulkWriteResult) {
        Map<K, ObjectId> keyToObjectId = new HashMap<>();
        keyToObjectId.putAll(bulkWriteResult.getInserts().stream()
                .filter(bulkWriteInsert -> nonNull(indexToKey.get(bulkWriteInsert.getIndex())))
                .collect(toMap(bulkWriteInsert -> indexToKey.get(bulkWriteInsert.getIndex()),
                        bulkWriteInsert -> extractObjectId(bulkWriteInsert.getId()))));
        keyToObjectId.putAll(bulkWriteResult.getUpserts().stream()
                .filter(bulkWriteUpsert -> nonNull(indexToKey.get(bulkWriteUpsert.getIndex())))
                .collect(toMap(bulkWriteUpsert -> indexToKey.get(bulkWriteUpsert.getIndex()),
                        bulkWriteUpsert -> extractObjectId(bulkWriteUpsert.getId()))));
        return keyToObjectId;
    }

    private ObjectId extractObjectId(BsonValue bsonValue) {
        return Optional.ofNullable(bsonValue)
                .filter(BsonValue::isObjectId)
                .map(BsonValue::asObjectId)
                .map(BsonObjectId::getValue)
                .orElseThrow(() -> new NoSuchElementException(format("No 'objectId' found (%s)",
                        bsonValue)));
    }

    private synchronized ObjectId generateObjectId() {
        int currentTime = (int) Instant.now().getEpochSecond();
        if (time != currentTime) {
            time = currentTime;
            counter = offset;
        } else {
            counter++;
        }
        byte[] bytes = new byte[]{
                b(time, 24), b(time, 16), b(time, 8), b(time, 0),
                random[0], random[1], random[2], random[3], random[4],
                b(counter, 16), b(counter, 8), b(counter, 0)
        };
        return new ObjectId(bytes);
    }

    private byte b(int i, int b) {
        return (byte) (i >> b);
    }
}
