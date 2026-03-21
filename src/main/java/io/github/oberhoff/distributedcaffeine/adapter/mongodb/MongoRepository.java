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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractRepository;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.SerializerAware;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.DISCRIMINATOR;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.HASH;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.KEY;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.OPERATION;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.STATUS;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.TIMESTAMP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.VALUE;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

@NullMarked
final class MongoRepository<K, V> extends AbstractRepository<K, V> {

    private static final Logger LOGGER = System.getLogger(MongoRepository.class.getName());

    private final MongoCollection<Document> mongoCollection;

    MongoRepository(MongoClient mongoClient, String databaseName, String collectionName) {
        this.mongoCollection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
        ensureIndexes();
    }

    @Override
    public void upsertCacheEntries(Collection<CacheEntry<K, V>> cacheEntries) {
        if (!cacheEntries.isEmpty()) {
            List<UpdateOneModel<Document>> updates = new ArrayList<>();
            cacheEntries.forEach(cacheEntry -> {
                try {
                    Bson filter = Filters.and(
                            Filters.eq(HASH.toString(), cacheEntry.getHash()),
                            Filters.eq(DISCRIMINATOR.toString(), cacheEntry.getDiscriminator()));
                    Bson update = Updates.combine(
                            Updates.set(OPERATION.toString(), cacheEntry.getOperation()),
                            Updates.set(KEY.toString(), serializeToMongo(cacheEntry.getKey(),
                                    requireNonNull(keySerializer))),
                            Updates.set(VALUE.toString(), serializeToMongo(cacheEntry.getValue(),
                                    requireNonNull(valueSerializer))),
                            Updates.set(STATUS.toString(), cacheEntry.getStatus().toString()),
                            Updates.set(TIMESTAMP.toString(), Date.from(cacheEntry.getTimestamp())));
                    UpdateOptions updateOptions = new UpdateOptions().upsert(true);
                    updates.add(new UpdateOneModel<>(filter, update, updateOptions));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            mongoCollection.bulkWrite(updates);
        }
    }

    @Override
    public Stream<CacheEntry<K, V>> streamCacheEntries(@Nullable String discriminator, @Nullable Set<String> hashes,
                                                       @Nullable Set<Status> statuses, @Nullable Set<Field> fields,
                                                       boolean orderByTimestampAsc) {
        Bson filter = getFilter(discriminator, hashes, statuses, null);
        Bson projection = getProjection(fields);
        Bson sort = orderByTimestampAsc
                ? Sorts.ascending(TIMESTAMP.toString())
                : null;
        MongoCursor<Document> mongoCursor = mongoCollection
                .find(filter)
                .projection(projection)
                .sort(sort)
                .cursor();
        return streamFromMongoCursor(mongoCursor)
                .map(document -> toCacheEntryOrNull(
                        requireNonNull(keySerializer), requireNonNull(valueSerializer), document,
                        LOGGER, requireNonNull(identifier)))
                .filter(Objects::nonNull);
    }

    @Override
    public void deleteCacheEntries(@Nullable String discriminator, @Nullable Set<String> hashes,
                                   @Nullable Set<Status> statuses, @Nullable Instant olderThan) {
        Bson filter = getFilter(discriminator, hashes, statuses, olderThan);
        mongoCollection.deleteMany(filter);
    }

    @Override
    public long countCacheEntries(@Nullable String discriminator, @Nullable Set<Status> statuses) {
        Bson filter = getFilter(discriminator, null, statuses, null);
        return mongoCollection.countDocuments(filter);
    }

    private void ensureIndexes() {
        IndexModel indexHashDiscriminator = new IndexModel(
                Indexes.compoundIndex(
                        Indexes.ascending(HASH.toString()),
                        Indexes.ascending(DISCRIMINATOR.toString())),
                new IndexOptions()
                        .unique(true)
                        .background(true));
        IndexModel indexHashStatusDiscriminatorTimestamp = new IndexModel(
                Indexes.compoundIndex(
                        Indexes.ascending(HASH.toString()),
                        Indexes.ascending(STATUS.toString()),
                        Indexes.ascending(DISCRIMINATOR.toString()),
                        Indexes.ascending(TIMESTAMP.toString())),
                new IndexOptions()
                        .unique(false)
                        .background(true));

        List<IndexModel> indexes = List.of(indexHashDiscriminator, indexHashStatusDiscriminatorTimestamp);

        mongoCollection.createIndexes(indexes);

        // collect keys for necessary indexes
        Set<BsonDocument> indexKeys = indexes.stream()
                .map(IndexModel::getKeys)
                .map(Bson::toBsonDocument)
                .collect(toSet());
        // add key for default index
        indexKeys.add(new Document("_id", 1).toBsonDocument());

        // drop any other index not in collected keys
        mongoCollection.listIndexes().forEach(existingIndex -> {
            Bson key = existingIndex.get("key", Document.class);
            if (!indexKeys.contains(key.toBsonDocument())) {
                mongoCollection.dropIndex(key);
            }
        });
    }

    private <T> Stream<T> streamFromMongoCursor(MongoCursor<T> mongoCursor) {
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(mongoCursor,
                Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false)
                .onClose(mongoCursor::close);
    }

    private Bson getFilter(@Nullable String discriminator, @Nullable Set<String> hashes,
                           @Nullable Set<Status> statuses, @Nullable Instant olderThan) {
        List<Bson> filters = new ArrayList<>();
        if (nonNull(hashes)) {
            filters.add(Filters.in(HASH.toString(), hashes));
        }
        if (nonNull(statuses)) {
            filters.add(Filters.in(STATUS.toString(), statuses.stream()
                    .map(Objects::toString)
                    .toList()));
        }
        filters.add(Filters.eq(DISCRIMINATOR.toString(), discriminator));
        if (nonNull(olderThan)) {
            filters.add(Filters.lt(TIMESTAMP.toString(), Date.from(olderThan)));
        }
        return Filters.and(filters);
    }

    private Bson getProjection(@Nullable Set<Field> fields) {
        return Projections.include(Stream.of(Field.values())
                .filter(field -> isNull(fields) || fields.contains(field))
                .map(Object::toString)
                .toList());
    }

    static <K, V> @Nullable CacheEntry<K, V> toCacheEntryOrNull(Serializer<K, ?> keySerializer,
                                                                Serializer<V, ?> valueSerializer, Document document,
                                                                Logger logger, String identifier) {
        try {
            return toCacheEntry(keySerializer, valueSerializer, document);
        } catch (Exception e) {
            logger.log(Level.WARNING,
                    format("Deserializing of cache entry failed for document '%s' at '%s'. Skipping...",
                            document, identifier), e);
            return null;
        }
    }

    private static <K, V> CacheEntry<K, V> toCacheEntry(Serializer<K, ?> keySerializer, Serializer<V, ?> valueSerializer,
                                                        Document document) throws Exception {
        return CacheEntry.of(
                document.getString(DISCRIMINATOR.toString()),
                document.getString(HASH.toString()),
                document.getInteger(OPERATION.toString()),
                deserializeFromMongo(document, KEY.toString(), keySerializer),
                deserializeFromMongo(document, VALUE.toString(), valueSerializer),
                Status.of(document.getString(STATUS.toString())),
                document.getDate(TIMESTAMP.toString()).toInstant());
    }

    private static <T> @Nullable Object serializeToMongo(@Nullable T object, Serializer<T, ?> serializer)
            throws Exception {
        Object serializedObject = SerializerAware.serialize(object, serializer);
        if (nonNull(serializedObject)
                && serializer instanceof JsonSerializer<?> jsonSerializer
                && jsonSerializer.storeAsBinaryJson()) {
            serializedObject = convertJsonToBson((String) serializedObject);
        }
        return serializedObject;
    }

    private static <T> @Nullable T deserializeFromMongo(Document document, String fieldName,
                                                        Serializer<T, ?> serializer) throws Exception {
        Object mongoValue = document.get(fieldName);
        if (mongoValue instanceof Binary binary) {
            mongoValue = binary.getData();
        } else if (mongoValue instanceof Bson bson) {
            mongoValue = convertBsonToJson(bson);
        }
        return SerializerAware.deserialize(mongoValue, serializer);
    }

    private static Object convertJsonToBson(String json) {
        String jsonKey = "jsonKey";
        String documentJson = format("{\"%s\":%s}", jsonKey, json);
        Document document = Document.parse(documentJson);
        return document.get(jsonKey);
    }

    private static String convertBsonToJson(Bson bson) {
        String bsonKey = "bsonKey";
        Document document = new Document(bsonKey, bson);
        String json = document.toJson();
        return json.substring(json.indexOf(":") + 1, json.lastIndexOf("}")).strip();
    }
}
