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

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    // shared, immutable config reused for every bulk upsert instead of allocating one per entry
    private static final UpdateOptions UPSERT_OPTIONS = new UpdateOptions().upsert(true);
    // every model in a batch targets a distinct hash, so there is no order to preserve between them. Unordered lets
    // the server keep going after a failed operation (instead of discarding the rest of the batch) and apply them
    // concurrently rather than strictly one by one
    private static final BulkWriteOptions UNORDERED_BULK_WRITE_OPTIONS = new BulkWriteOptions().ordered(false);
    // constant projection of all fields, reused for the common (fields == null) query instead of rebuilding it
    private static final Bson ALL_FIELDS_PROJECTION = Projections.include(Stream.of(Field.values())
            .map(Object::toString)
            .toList());

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
                    // the discriminator is not part of the update: an upsert builds the document to insert from the
                    // equality conditions of its filter, so matching on it here is what stamps it on a new document,
                    // and an existing one already carries it (it could not have been matched otherwise)
                    Bson filter = Filters.and(
                            Filters.eq(HASH.toString(), cacheEntry.getHash()),
                            Filters.eq(DISCRIMINATOR_FIELD, requireNonNull(discriminator)));
                    Bson update = Updates.combine(
                            Updates.set(OPERATION.toString(), cacheEntry.getOperation()),
                            Updates.set(KEY.toString(), serializeToMongo(cacheEntry.getKey(),
                                    requireNonNull(keySerializer))),
                            Updates.set(VALUE.toString(), serializeToMongo(cacheEntry.getValue(),
                                    requireNonNull(valueSerializer))),
                            Updates.set(STATUS.toString(), cacheEntry.getStatus().toString()),
                            Updates.set(TIMESTAMP.toString(), cacheEntry.getTimestamp()));
                    updates.add(new UpdateOneModel<>(filter, update, UPSERT_OPTIONS));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            bulkUpsert(updates);
        }
    }

    // An upsert filtered by something other than '_id' is not atomic against a concurrent insert of the same key:
    // when two cache instances write a key that does not exist yet, both filters match nothing, both attempt an
    // insert, and the loser is rejected by the unique (hash, discriminator) index with a duplicate key error. Since
    // the document exists by then, applying the rejected operations once more turns them into plain updates.
    // Only those are repeated: reapplying operations that already succeeded would write their (by then possibly
    // outdated) values over whatever another instance has written in the meantime.
    // Deliberately a plain catch rather than a Failsafe retry policy: the operations to repeat are not the ones that
    // were attempted but the subset the server rejected, and none of what Failsafe adds (delays, backoff, scheduling)
    // applies to an immediate in-place repetition. Repeating exactly once is enough, because a second duplicate key
    // for the same operations would require the document to be deleted again in between - if that ever happens the
    // exception is reported rather than hidden behind further attempts. Note that more than one repetition would
    // need the rejected subset to be rebased on the operations of the preceding attempt (the error indices refer to
    // those, not to the original list), so do not simply loop over this.
    private void bulkUpsert(List<UpdateOneModel<Document>> updates) {
        try {
            mongoCollection.bulkWrite(updates, UNORDERED_BULK_WRITE_OPTIONS);
        } catch (MongoBulkWriteException e) {
            if (!isDuplicateKeyOnly(e)) {
                throw e;
            }
            mongoCollection.bulkWrite(rejectedUpdates(updates, e), UNORDERED_BULK_WRITE_OPTIONS);
        }
    }

    private static List<UpdateOneModel<Document>> rejectedUpdates(List<UpdateOneModel<Document>> updates,
                                                                  MongoBulkWriteException bulkWriteException) {
        return bulkWriteException.getWriteErrors().stream()
                // the index refers to the position within the operations handed to bulkWrite
                .map(writeError -> updates.get(writeError.getIndex()))
                .toList();
    }

    private static boolean isDuplicateKeyOnly(MongoBulkWriteException bulkWriteException) {
        return !bulkWriteException.getWriteErrors().isEmpty()
                && bulkWriteException.getWriteErrors().stream()
                .allMatch(writeError -> ErrorCategory.fromErrorCode(writeError.getCode())
                        .equals(ErrorCategory.DUPLICATE_KEY));
    }

    @Override
    public Stream<CacheEntry<K, V>> streamCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                                       @Nullable Set<Field> fields, boolean orderByTimestampAsc) {
        Bson filter = getFilter(hashes, statuses, null);
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
    public void updateStatusOfCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                           @Nullable Instant olderThan, Status newStatus) {
        Bson filter = getFilter(hashes, statuses, olderThan);
        Bson update = Updates.combine(
                Updates.set(STATUS.toString(), newStatus.toString()),
                Updates.set(OPERATION.toString(), null), // clearing the operation lets every instance apply it
                Updates.set(TIMESTAMP.toString(), Instant.now()));
        mongoCollection.updateMany(filter, update);
    }

    @Override
    public void deleteCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                   @Nullable Instant olderThan) {
        Bson filter = getFilter(hashes, statuses, olderThan);
        mongoCollection.deleteMany(filter);
    }

    @Override
    public long countCacheEntries(@Nullable Set<Status> statuses) {
        Bson filter = getFilter(null, statuses, null);
        return mongoCollection.countDocuments(filter);
    }

    // Every query this repository issues filters by the discriminator and by nothing else unconditionally, so it
    // leads both indexes: that makes it the equality prefix of every plan and keeps even a query filtering by
    // nothing else off a collection scan.
    //
    // The remaining fields follow the order equality, sort, range:
    // - (discriminator, hash) is unique, which is what the upsert relies on, and because it is unique a filter by
    //   hashes needs exactly one index seek per hash - a status filter alongside it is then evaluated against at
    //   most that many documents, so a wider index adding status and timestamp behind the hash buys nothing.
    // - (discriminator, status, timestamp) serves everything filtering by status, with the timestamp trailing so
    //   that it can be used for the range filter as well as for the ordering (the status filter is a set, which
    //   the server expands into one sorted index range per status and merges, so no blocking sort is needed).
    private void ensureIndexes() {
        IndexModel indexDiscriminatorHash = new IndexModel(
                Indexes.compoundIndex(
                        Indexes.ascending(DISCRIMINATOR_FIELD),
                        Indexes.ascending(HASH.toString())),
                new IndexOptions().unique(true));
        IndexModel indexDiscriminatorStatusTimestamp = new IndexModel(
                Indexes.compoundIndex(
                        Indexes.ascending(DISCRIMINATOR_FIELD),
                        Indexes.ascending(STATUS.toString()),
                        Indexes.ascending(TIMESTAMP.toString())),
                new IndexOptions().unique(false));

        List<IndexModel> indexes = List.of(indexDiscriminatorHash, indexDiscriminatorStatusTimestamp);

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

    private Bson getFilter(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                           @Nullable Instant olderThan) {
        List<Bson> filters = new ArrayList<>();
        // first because it is the only one always present and the one both indexes lead with. The server normalizes
        // the order of the conditions before planning, so this documents intent rather than steering it
        filters.add(Filters.eq(DISCRIMINATOR_FIELD, requireNonNull(discriminator)));
        if (nonNull(hashes)) {
            filters.add(Filters.in(HASH.toString(), hashes));
        }
        if (nonNull(statuses)) {
            filters.add(Filters.in(STATUS.toString(), statuses.stream()
                    .map(Objects::toString)
                    .toList()));
        }
        if (nonNull(olderThan)) {
            filters.add(Filters.lt(TIMESTAMP.toString(), olderThan));
        }
        return Filters.and(filters);
    }

    private Bson getProjection(@Nullable Set<Field> fields) {
        return isNull(fields)
                ? ALL_FIELDS_PROJECTION
                : Projections.include(Stream.of(Field.values())
                .filter(fields::contains)
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
                document.getString(HASH.toString()),
                document.getString(OPERATION.toString()),
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
        } else if (nonNull(mongoValue)
                && serializer instanceof JsonSerializer<?> jsonSerializer
                && jsonSerializer.storeAsBinaryJson()) {
            // symmetric to serializeToMongo: the value was stored as native BSON (any type, including scalars such
            // as strings, numbers or booleans - not just documents/arrays), so convert it back to its JSON
            // representation for the serializer. Deciding based on the serializer (rather than on the stored type)
            // ensures scalars are also converted; otherwise a stored scalar would be handed to the serializer as-is
            // (e.g. an unquoted string) and fail to deserialize.
            mongoValue = convertBsonToJson(mongoValue);
        }
        return SerializerAware.deserialize(mongoValue, serializer);
    }

    private static Object convertJsonToBson(String json) {
        String jsonKey = "jsonKey";
        String documentJson = format("{\"%s\":%s}", jsonKey, json);
        Document document = Document.parse(documentJson);
        return document.get(jsonKey);
    }

    private static String convertBsonToJson(Object bsonValue) {
        String bsonKey = "bsonKey";
        Document document = new Document(bsonKey, bsonValue);
        String json = document.toJson();
        return json.substring(json.indexOf(":") + 1, json.lastIndexOf("}")).strip();
    }
}
