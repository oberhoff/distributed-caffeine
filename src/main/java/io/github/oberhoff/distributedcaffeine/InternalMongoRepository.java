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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.ExtendedPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_SIZE_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_TIME_EXTENDED;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;

class InternalMongoRepository<K, V> implements InternalLazyInitializer<K, V> {

    private final byte[] random;
    private final int offset;

    private int time;
    private int counter;
    private Logger logger;
    private MongoCollection<Document> mongoCollection;
    private InternalDocumentConverter<K, V> documentConverter;
    private ExtendedPersistenceConfigurer extendedPersistenceConfigurer;
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
        this.extendedPersistenceConfigurer = distributedCaffeine.getExtendedPersistenceConfigurer();
        this.origin = distributedCaffeine.getOrigin();

        ensureIndexes();
    }

    @SuppressWarnings("SameParameterValue")
    void consumeCacheDocumentsGroupedByKeyNewestFirstForKeys(
            @Nullable Set<? extends K> keys, @Nullable Set<Status> statuses, @Nullable Boolean stale,
            @Nullable Set<Field> fields,
            Consumer<Stream<Set<InternalCacheDocument<K, V>>>> streamConsumer) {
        if (allNotEmptyButNullAllowed(keys, statuses)) {
            Set<Integer> hashes = isNull(keys)
                    ? null
                    : keys.stream()
                    .map(Objects::hashCode)
                    .collect(toSet());
            consumeCacheDocumentsGroupedByKeyNewestFirstForHashes(
                    hashes, statuses,
                    stale, fields,
                    stream -> streamConsumer.accept(stream
                            // only return requested keys, even if hash collisions occur
                            .filter(cacheDocuments -> isNull(keys) || cacheDocuments.stream()
                                    .anyMatch(cacheDocument -> keys.contains(cacheDocument.getKey())))));
        }
    }

    void consumeCacheDocumentsGroupedByKeyNewestFirstForHashes(
            @Nullable Set<Integer> hashes, @Nullable Set<Status> statuses, @Nullable Boolean stale,
            @Nullable Set<Field> fields,
            Consumer<Stream<Set<InternalCacheDocument<K, V>>>> streamConsumer) {
        if (allNotEmptyButNullAllowed(hashes, statuses)) {
            try (Stream<Set<InternalCacheDocument<K, V>>> stream =
                         streamCacheDocumentsGroupedByKey(hashes, statuses, stale, fields)
                                 .map(entry -> entry.getValue().stream()
                                         .sorted(Comparator.reverseOrder())
                                         .collect(toCollection(LinkedHashSet::new)))) {
                streamConsumer.accept(stream);
            }
        }
    }

    void consumeHashesWithCountGreaterOrEqual(
            @Nullable Set<Integer> hashes, @Nullable Set<Status> statuses, @Nullable Boolean stale,
            int greaterOrEqual,
            Consumer<Stream<Integer>> streamConsumer) {
        if (allNotEmptyButNullAllowed(hashes, statuses)) {
            Bson filter = getFilter(hashes, statuses, stale);
            try (Stream<Integer> stream = streamFromMongoCursor(mongoCollection.aggregate(
                            List.of(
                                    Aggregates.match(filter),
                                    Aggregates.group(format("$%s", HASH),
                                            Accumulators.sum("count", 1)),
                                    Aggregates.match(Filters.gte("count", greaterOrEqual)),
                                    Aggregates.project(Projections.include(_ID.toString()))))
                    .iterator())
                    .map(document -> document.getInteger(_ID.toString()))) {
                streamConsumer.accept(stream);
            }
        }
    }

    long count(@Nullable Set<Status> statuses, @Nullable Boolean stale) {
        if (allNotEmptyButNullAllowed(statuses)) {
            Bson filter = getFilter(null, statuses, stale);
            return mongoCollection.countDocuments(filter);
        } else {
            return 0;
        }
    }

    Set<InternalCacheDocument<K, V>> insert(Map<? extends K, ? extends V> map, Status status) {
        if (!map.isEmpty()) {
            Map<K, ObjectId> keyToObjectId = new HashMap<>();
            List<UpdateOneModel<Document>> updates = new ArrayList<>();
            map.forEach((key, value) -> {
                ObjectId objectId = generateObjectId();
                keyToObjectId.put(key, objectId);
                // perform upsert to be able to use $currentTime (which is an update only operator)
                Bson filter = Filters.eq(_ID.toString(), objectId);
                Bson update = toMongoUpdate(key, value, status);
                UpdateOptions updateOptions = new UpdateOptions().upsert(true);
                updates.add(new UpdateOneModel<>(filter, update, updateOptions));
            });
            mongoCollection.bulkWrite(updates);
            return map.entrySet().stream()
                    .map(entry -> new InternalCacheDocument<K, V>()
                            .setId(keyToObjectId.get(entry.getKey()))
                            .setHash(entry.getKey().hashCode())
                            .setKey(entry.getKey())
                            .setValue(entry.getValue())
                            .setStatus(status))
                    .collect(toSet());
        } else {
            return Set.of();
        }
    }

    void markAsStale(Set<ObjectId> objectIds) {
        if (!objectIds.isEmpty()) {
            Bson filter = Filters.and(
                    Filters.in(_ID.toString(), objectIds),
                    Filters.eq(STALE.toString(), false));
            Bson update = Updates.combine(
                    Updates.set(STALE.toString(), true),
                    Updates.currentDate(EXPIRES.toString()));
            mongoCollection.updateMany(filter, update);
        }
    }

    private void ensureIndexes() {
        IndexModel indexHashStatusStaleDiscriminator = new IndexModel(Indexes.compoundIndex(
                Indexes.ascending(HASH.toString()),
                Indexes.ascending(STATUS.toString()),
                Indexes.ascending(STALE.toString()),
                Indexes.ascending(DISCRIMINATOR.toString())),
                new IndexOptions()
                        .unique(false)
                        .background(true));
        IndexModel indexIdStale = new IndexModel(Indexes.compoundIndex(
                Indexes.ascending(_ID.toString()),
                Indexes.ascending(STALE.toString())),
                new IndexOptions()
                        .unique(false)
                        .background(true));
        IndexModel indexExpires = new IndexModel(
                Indexes.ascending(EXPIRES.toString()),
                new IndexOptions()
                        .unique(false)
                        .background(true)
                        .expireAfter(1L, TimeUnit.MINUTES));

        List<IndexModel> indexes = List.of(
                indexHashStatusStaleDiscriminator,
                indexIdStale,
                indexExpires);

        mongoCollection.createIndexes(indexes);

        // collect keys for necessary indexes
        Set<BsonDocument> indexKeys = indexes.stream()
                .map(IndexModel::getKeys)
                .map(Bson::toBsonDocument)
                .collect(toSet());
        // add key for default index
        indexKeys.add(new Document(_ID.toString(), 1).toBsonDocument());

        // drop any other index not in collected keys
        mongoCollection.listIndexes().forEach(existingIndex -> {
            Bson key = existingIndex.get("key", Document.class);
            if (!indexKeys.contains(key.toBsonDocument())) {
                mongoCollection.dropIndex(key);
            }
        });
    }

    private Stream<Entry<K, Set<InternalCacheDocument<K, V>>>> streamCacheDocumentsGroupedByKey(
            @Nullable Set<Integer> hashes, @Nullable Set<Status> statuses, @Nullable Boolean stale,
            @Nullable Set<Field> fields) {
        Bson filter = getFilter(hashes, statuses, stale);
        Bson projection = getProjection(fields);
        Field[] fieldsArray = isNull(fields)
                ? new Field[0]
                : fields.toArray(new Field[0]);
        MongoCursor<Document> mongoCursor = mongoCollection.find(filter)
                .projection(projection)
                .sort(Sorts.ascending(HASH.toString()))
                .iterator();
        Set<Integer> foundHashes = new LinkedHashSet<>();
        Set<InternalCacheDocument<K, V>> foundCacheDocuments = new HashSet<>();
        return Stream.generate(() -> {
                    while (mongoCursor.hasNext() && foundHashes.size() <= 1) {
                        InternalCacheDocument<K, V> cacheDocument =
                                toCacheDocumentOrNull(mongoCursor.next(), fieldsArray);
                        if (nonNull(cacheDocument)) {
                            foundHashes.add(cacheDocument.getHash());
                            foundCacheDocuments.add(cacheDocument);
                        }
                    }
                    Map<K, Set<InternalCacheDocument<K, V>>> keyToCacheDocuments = new HashMap<>();
                    foundHashes.stream()
                            .findFirst()
                            .ifPresent(hash -> {
                                keyToCacheDocuments.putAll(foundCacheDocuments.stream()
                                        .filter(cacheDocument ->
                                                Objects.equals(hash, cacheDocument.getHash()))
                                        // ensure 'hashCode -> bucket -> equals' semantics
                                        .collect(groupingBy(InternalCacheDocument::getKey, toSet())));
                                foundHashes.remove(hash);
                                foundCacheDocuments.removeIf(cacheDocument ->
                                        Objects.equals(hash, cacheDocument.getHash()));
                            });
                    return keyToCacheDocuments;
                })
                .takeWhile(keyToCacheDocuments -> !keyToCacheDocuments.isEmpty())
                .flatMap(keyToCacheDocuments -> keyToCacheDocuments.entrySet().stream())
                .onClose(mongoCursor::close);
    }

    private <T> Stream<T> streamFromMongoCursor(MongoCursor<T> mongoCursor) {
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(mongoCursor,
                Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false)
                .onClose(mongoCursor::close);
    }

    private Bson getFilter(@Nullable Set<Integer> hashes, @Nullable Set<Status> statuses, @Nullable Boolean stale) {
        if (!allNotEmptyButNullAllowed(hashes, statuses)) {
            throw new IllegalStateException("Sets cannot be empty");
        }
        List<Bson> filters = new ArrayList<>();
        if (nonNull(hashes)) {
            filters.add(Filters.in(HASH.toString(), hashes));
        }
        if (nonNull(statuses)) {
            filters.add(Filters.in(STATUS.toString(), statuses.stream()
                    .map(Objects::toString)
                    .toList()));
        }
        if (nonNull(stale)) {
            filters.add(Filters.eq(STALE.toString(), stale));
        }
        return filters.isEmpty()
                ? Filters.empty()
                : Filters.and(filters);
    }

    private Bson getProjection(@Nullable Set<Field> fields) {
        if (!allNotEmptyButNullAllowed(fields)) {
            throw new IllegalStateException("Set cannot be empty");
        }
        return Projections.include(Stream.of(Field.values())
                .filter(field -> isNull(fields) || fields.contains(field))
                .map(Object::toString)
                .toList());
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
        if (extendedPersistenceConfigurer.isConfigured()
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
                if (extendedPersistenceConfigurer.getMaximumTime().isPresent()) {
                    Instant now = Instant.now();
                    Duration extendedDuration = extendedPersistenceConfigurer.getMaximumTime().orElseThrow();
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

    private boolean allNotEmptyButNullAllowed(Collection<?>... collections) {
        return Stream.of(collections)
                .filter(Objects::nonNull)
                .noneMatch(Collection::isEmpty);
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
