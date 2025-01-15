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
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.LazyInitializer;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.io.Closeable;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.EVICTED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.EXPIRES;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.HASH;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.ID;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.KEY;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.ORPHANED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.STATUS;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.TOUCHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.VALUE;
import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalMongoRepository<K, V> implements LazyInitializer<K, V> {

    private Logger logger;
    private MongoCollection<Document> mongoCollection;
    private InternalObjectIdGenerator objectIdGenerator;
    private InternalDocumentConverter<K, V> documentConverter;

    InternalMongoRepository() {
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.logger = distributedCaffeine.getLogger();
        this.mongoCollection = distributedCaffeine.getMongoCollection();
        this.objectIdGenerator = distributedCaffeine.getObjectIdGenerator();
        this.documentConverter = distributedCaffeine.getDocumentConverter();

        ensureIndexes();
    }

    Stream<InternalCacheDocument<K, V>> streamCacheDocuments(Bson filter, Bson projection) {
        return createStreamFromClosableIterator(mongoCollection.find()
                .filter(filter)
                .projection(projection)
                .iterator())
                .map(document -> {
                    try {
                        return documentConverter.toCacheDocument(document);
                    } catch (Exception e) {
                        logger.log(Level.WARNING,
                                format("Deserializing of cache entry failed for collection '%s' (%s). Skipping...",
                                        mongoCollection.getNamespace().getCollectionName(), document), e);
                        return null;
                    }
                })
                .filter(Objects::nonNull);
    }

    Set<InternalCacheDocument<K, V>> insert(Map<? extends K, ? extends V> map, String status) {
        if (!map.isEmpty()) {
            Map<K, ObjectId> keyToObjectId = new HashMap<>();
            List<UpdateOneModel<Document>> updates = new ArrayList<>();
            Map<Integer, K> indexToKey = new HashMap<>();
            AtomicInteger index = new AtomicInteger(0);
            RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                    .build();
            Failsafe.with(retryPolicy)
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
                                    if (!INVALIDATED.equals(status)) {
                                        requireNonNull(value, "value cannot be null");
                                    }
                                    // perform upsert to be able to use $currentTime (which is an update operator only)
                                    Bson filter = Filters.eq(ID, objectIdGenerator.generate());
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
                    .collect(Collectors.toSet());
        } else {
            return Set.of();
        }
    }

    void updateOrphaned(Set<ObjectId> objectIds) {
        if (!objectIds.isEmpty()) {
            RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                    .build();
            Failsafe.with(retryPolicy)
                    .run(() -> {
                        Bson filter = Filters.and(
                                Filters.in(ID, objectIds),
                                Filters.ne(STATUS, ORPHANED));
                        Bson update = Updates.combine(
                                Updates.set(STATUS, ORPHANED),
                                Updates.currentDate(EXPIRES));
                        mongoCollection.updateMany(filter, update);
                    });
        }
    }

    private void ensureIndexes() {
        IndexModel indexModelHash = new IndexModel(Indexes.ascending(HASH),
                new IndexOptions()
                        .unique(false)
                        .background(true));
        IndexModel indexModelStatus = new IndexModel(Indexes.ascending(STATUS),
                new IndexOptions()
                        .unique(false)
                        .background(true));
        IndexModel indexModelExpires = new IndexModel(Indexes.ascending(EXPIRES),
                new IndexOptions()
                        .unique(false)
                        .background(true)
                        .expireAfter(1L, TimeUnit.MINUTES));
        IndexModel indexModelIdStatus = new IndexModel(Indexes.compoundIndex(
                Indexes.ascending(ID), Indexes.ascending(STATUS)),
                new IndexOptions()
                        .unique(true)
                        .background(true));
        IndexModel indexModelHashStatus = new IndexModel(Indexes.compoundIndex(
                Indexes.ascending(HASH), Indexes.ascending(STATUS)),
                new IndexOptions()
                        .unique(false)
                        .background(true));

        mongoCollection.createIndexes(List.of(indexModelHash, indexModelStatus, indexModelExpires,
                indexModelIdStatus, indexModelHashStatus));
    }

    private <I extends Iterator<T> & Closeable, T> Stream<T> createStreamFromClosableIterator(I iterator) {
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator,
                Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false).onClose(() -> {
            try {
                iterator.close();
            } catch (IOException e) {
                throw new DistributedCaffeineException(e);
            }
        });
    }

    private Bson toMongoUpdate(K key, V value, String status) {
        try {
            Object serializedKey = documentConverter.toMongoKey(key);
            Object serializedValue = documentConverter.toMongoValue(value);
            Bson expires = INVALIDATED.equals(status) || EVICTED.equals(status)
                    ? Updates.currentDate(EXPIRES)
                    : Updates.set(EXPIRES, null);
            return Updates.combine(
                    Updates.set(HASH, key.hashCode()),
                    Updates.set(KEY, serializedKey),
                    Updates.set(VALUE, serializedValue),
                    Updates.set(STATUS, status),
                    Updates.currentDate(TOUCHED),
                    expires);
        } catch (Exception e) {
            throw new DistributedCaffeineException(e);
        }
    }

    private Map<K, ObjectId> evaluateBulkWriteResult(Map<Integer, K> indexToKey, BulkWriteResult bulkWriteResult) {
        Map<K, ObjectId> keyToObjectId = new HashMap<>();
        keyToObjectId.putAll(bulkWriteResult.getInserts().stream()
                .filter(bulkWriteInsert -> nonNull(indexToKey.get(bulkWriteInsert.getIndex())))
                .collect(Collectors.toMap(bulkWriteInsert -> indexToKey.get(bulkWriteInsert.getIndex()),
                        bulkWriteInsert -> extractObjectId(bulkWriteInsert.getId()))));
        keyToObjectId.putAll(bulkWriteResult.getUpserts().stream()
                .filter(bulkWriteUpsert -> nonNull(indexToKey.get(bulkWriteUpsert.getIndex())))
                .collect(Collectors.toMap(bulkWriteUpsert -> indexToKey.get(bulkWriteUpsert.getIndex()),
                        bulkWriteUpsert -> extractObjectId(bulkWriteUpsert.getId()))));
        return keyToObjectId;
    }

    private ObjectId extractObjectId(BsonValue bsonValue) {
        return Optional.ofNullable(bsonValue)
                .filter(BsonValue::isObjectId)
                .map(BsonValue::asObjectId)
                .map(BsonObjectId::getValue)
                .orElseThrow(() -> new DistributedCaffeineException(format("No 'objectId' found (%s)",
                        bsonValue)));
    }
}
