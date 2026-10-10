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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.cursor.TimeoutMode;
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
import com.mongodb.connection.ClusterType;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractRepository;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntryMetadata;
import io.github.oberhoff.distributedcaffeine.adapter.SerializerAware;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
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
import java.util.concurrent.TimeUnit;
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

final class MongoRepository<K, V> extends AbstractRepository<K, V> {

    private static final Logger LOGGER = System.getLogger(MongoRepository.class.getName());
    private static final int NAMESPACE_NOT_FOUND_ERROR_CODE = 26;
    private static final UpdateOptions UPSERT_OPTIONS = new UpdateOptions().upsert(true);
    private static final BulkWriteOptions UNORDERED_BULK_WRITE_OPTIONS = new BulkWriteOptions().ordered(false);
    private static final Bson ALL_FIELDS_PROJECTION = Projections.include(Stream.of(Field.values())
            .map(Object::toString)
            .toList());
    private static final Bson METADATA_FIELDS_PROJECTION = Projections.include(Stream.of(Field.values())
            .filter(field -> field != KEY && field != VALUE)
            .map(Object::toString)
            .toList());

    private final MongoCollection<Document> mongoCollection;

    MongoRepository(MongoClient mongoClient, String databaseName, String collectionName) {
        // Majority rather than whatever the client brings, because anything weaker is not a cheaper version of the
        // same thing here: change streams notify only for majority-committed changes, so a write that is
        // acknowledged earlier is not distributed any earlier either - it is only acknowledged before it is certain
        // to be distributable at all. Should it then be rolled back, no other cache instance ever saw it, while the
        // one that wrote it is already holding it locally, and nothing says so until that key is written again.
        // Not a precaution against misconfiguration alone: before MongoDB 5.0 the server's own default is w:1, and
        // a client shared with an application that chose w:1 would hand it over on any version.
        // The primary rather than whatever the client brings, for a similar reason: what is read here is a snapshot
        // that the change stream continues from the moment it was opened, so it must be at least as recent as that
        // moment. A secondary lagging behind answers with an older one, and a change made in between is then in
        // neither - not in the snapshot, and not in the stream, which never looks back past where it began - so the
        // cache instance holds a stale value that nothing corrects until that key is written again. The change
        // stream itself keeps the client's preference, since what it delivers is majority-committed on any member.
        // And the server's default read concern rather than whatever the client brings, for the same snapshot: a
        // change stream begins after the latest write the primary has applied, while a majority read leaves out
        // what is applied but not yet replicated - so a write made just before the change stream was opened is in
        // neither, once again. Measured on a replica set whose secondaries were held back from replicating: the
        // default read saw such a write, a majority read did not, and the change stream never delivered it. What
        // the default read may see in addition is a write that is rolled back later, which takes the primary
        // failing - and that fails the change stream on it as well, which has the cache instance reconcile
        this.mongoCollection = mongoClient.getDatabase(databaseName).getCollection(collectionName)
                .withWriteConcern(WriteConcern.MAJORITY)
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.DEFAULT);
        ensureUnsharded(mongoClient);
        ensureIndexes();
    }

    @Override
    public void publishCacheEntries(Collection<CacheEntry<K, V>> cacheEntries) {
        if (!cacheEntries.isEmpty()) {
            List<UpdateOneModel<Document>> updates = new ArrayList<>();
            cacheEntries.forEach(cacheEntry -> {
                try {
                    Bson filter = Filters.and(
                            Filters.eq(HASH.toString(), cacheEntry.getHash()),
                            Filters.eq(DISCRIMINATOR_FIELD, discriminator));
                    Bson update = Updates.combine(
                            Updates.set(OPERATION.toString(), cacheEntry.getOperation()),
                            Updates.set(KEY.toString(), serializeToMongo(cacheEntry.getKey(), keySerializer)),
                            Updates.set(VALUE.toString(), serializeToMongo(cacheEntry.getValue(), valueSerializer)),
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

    @Override
    public Stream<CacheEntry<K, V>> streamCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                                       Order order) {
        return streamDocuments(hashes, statuses, order, ALL_FIELDS_PROJECTION)
                .map(document -> toCacheEntryOrNull(keySerializer, valueSerializer, document, LOGGER, identifier))
                .filter(Objects::nonNull);
    }

    @Override
    public Stream<CacheEntryMetadata> streamCacheEntryMetadata(@Nullable Set<String> hashes,
                                                               @Nullable Set<Status> statuses,
                                                               Order order) {
        return streamDocuments(hashes, statuses, order, METADATA_FIELDS_PROJECTION)
                .map(document -> toCacheEntryMetadataOrNull(document, identifier))
                .filter(Objects::nonNull);
    }

    private Stream<Document> streamDocuments(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                             Order order, Bson projection) {
        Bson filter = getFilter(hashes, statuses, null);
        Bson sort = switch (order) {
            case ASCENDING -> Sorts.ascending(TIMESTAMP.toString());
            case DESCENDING -> Sorts.descending(TIMESTAMP.toString());
            case UNORDERED -> null;
        };
        FindIterable<Document> findIterable = mongoCollection
                .find(filter)
                .projection(projection)
                .sort(sort);
        // A client timeout bounds an operation as a whole, and a cursor counts as one by default, from opening it to
        // exhausting it. What is streamed here is consumed as it is applied, which for a large cache takes far longer
        // than any one round trip - so the timeout is applied to every batch fetched instead, which is what it is
        // there for: noticing a store that does not answer, not limiting how much a cache instance may read. Only
        // with a timeout, because the mode means nothing - and is refused - without one
        if (nonNull(mongoCollection.getTimeout(TimeUnit.MILLISECONDS))) {
            findIterable = findIterable.timeoutMode(TimeoutMode.ITERATION);
        }
        MongoCursor<Document> mongoCursor = findIterable.cursor();
        return streamFromMongoCursor(mongoCursor);
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
    public long countCacheEntries(@Nullable Set<Status> statuses, @Nullable Instant notOlderThan) {
        Bson filter = isNull(notOlderThan)
                ? getFilter(null, statuses, null)
                : Filters.and(getFilter(null, statuses, null), Filters.gte(TIMESTAMP.toString(), notOlderThan));
        return mongoCollection.countDocuments(filter);
    }

    // A collection in a sharded cluster is fine, a sharded collection is not. MongoDB requires a unique index to
    // begin with the shard key and an upsert to name all of it, and this adapter relies on both: its unique
    // (discriminator, hash) index keeps one document per key, and its upserts are filtered by exactly these fields -
    // so any other shard key leaves either refused, and creating the index would fail below with nothing pointing
    // here. Not worth supporting the one shard key that would fit either: the collection is about as large as what
    // a single cache instance holds in memory, and a change stream on a sharded collection waits for every shard
    // before it delivers anything. Checked before the indexes, so that a sharded collection is named as the cause.
    // The cluster type is only known once the client has reached a server, hence the ping. Through the router
    // collStats tells whether a collection is sharded - including one that does not exist yet, which is not - and
    // takes no more than the read role grants. Only a database that does not exist yet has it fail, as on a first
    // start, and nothing in it is sharded either
    private void ensureUnsharded(MongoClient mongoClient) {
        MongoNamespace namespace = mongoCollection.getNamespace();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(namespace.getDatabaseName());
        mongoDatabase.runCommand(new Document("ping", 1), ReadPreference.primary());
        if (mongoClient.getClusterDescription().getType() == ClusterType.SHARDED) {
            Document collectionStats;
            try {
                collectionStats = mongoDatabase.runCommand(
                        new Document("collStats", namespace.getCollectionName()), ReadPreference.primary());
            } catch (MongoCommandException e) {
                if (e.getErrorCode() == NAMESPACE_NOT_FOUND_ERROR_CODE) {
                    return;
                }
                throw e;
            }
            if (Boolean.TRUE.equals(collectionStats.get("sharded"))) {
                throw new IllegalStateException(format("The collection '%s' is sharded, which is not supported - "
                        + "it may live in a sharded cluster, but must not be sharded itself", namespace));
            }
        }
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
    //   the server expands into one sorted index range per status and merges, so no blocking sort is needed - in
    //   either direction, since an index is walked backwards just as well).
    // Indexes of anyone else are left alone, so that whoever uses the collection beyond caching may index it as
    // needed. Should a later version need other indexes than these, it is the ones it no longer needs that it drops,
    // rather than all it does not know
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

        mongoCollection.createIndexes(List.of(indexDiscriminatorHash, indexDiscriminatorStatusTimestamp));
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

    private List<UpdateOneModel<Document>> rejectedUpdates(List<UpdateOneModel<Document>> updates,
                                                           MongoBulkWriteException bulkWriteException) {
        return bulkWriteException.getWriteErrors().stream()
                // the index refers to the position within the operations handed to bulkWrite
                .map(writeError -> updates.get(writeError.getIndex()))
                .toList();
    }

    private boolean isDuplicateKeyOnly(MongoBulkWriteException bulkWriteException) {
        return !bulkWriteException.getWriteErrors().isEmpty()
                && bulkWriteException.getWriteErrors().stream()
                .allMatch(writeError -> ErrorCategory.fromErrorCode(writeError.getCode())
                        .equals(ErrorCategory.DUPLICATE_KEY));
    }

    private <T> Stream<T> streamFromMongoCursor(MongoCursor<T> mongoCursor) {
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(mongoCursor,
                Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false)
                .onClose(mongoCursor::close);
    }

    private Bson getFilter(@Nullable Set<String> hashes, @Nullable Set<Status> statuses, @Nullable Instant olderThan) {
        List<Bson> filters = new ArrayList<>();
        // first because it is the only one always present and the one both indexes lead with. The server normalizes
        // the order of the conditions before planning, so this documents intent rather than steering it
        filters.add(Filters.eq(DISCRIMINATOR_FIELD, discriminator));
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

    static <K, V> @Nullable CacheEntry<K, V> toCacheEntryOrNull(Serializer<K, ?> keySerializer,
                                                                Serializer<V, ?> valueSerializer, Document document,
                                                                Logger logger, String identifier) {
        try {
            return CacheEntry.of(
                    requireNonNull(document.getString(HASH.toString()), "hash cannot be null"),
                    document.getString(OPERATION.toString()),
                    deserializeFromMongo(document, KEY.toString(), keySerializer),
                    deserializeFromMongo(document, VALUE.toString(), valueSerializer),
                    Status.of(requireNonNull(document.getString(STATUS.toString()), "status cannot be null")),
                    requireNonNull(document.getDate(TIMESTAMP.toString()), "timestamp cannot be null").toInstant());
        } catch (Exception e) {
            logger.log(Level.WARNING,
                    format("Reading of cache entry failed for document '%s' at '%s'. Skipping...",
                            document, identifier), e);
            return null;
        }
    }

    private static @Nullable CacheEntryMetadata toCacheEntryMetadataOrNull(Document document, String identifier) {
        try {
            return CacheEntryMetadata.of(
                    requireNonNull(document.getString(HASH.toString()), "hash cannot be null"),
                    document.getString(OPERATION.toString()),
                    Status.of(requireNonNull(document.getString(STATUS.toString()), "status cannot be null")),
                    requireNonNull(document.getDate(TIMESTAMP.toString()), "timestamp cannot be null")
                            .toInstant());
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING,
                    format("Reading of cache entry metadata failed for document '%s' at '%s'. Skipping...",
                            document, identifier), e);
            return null;
        }
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
            mongoValue = convertBsonToJson(mongoValue);
        }
        return SerializerAware.deserialize(mongoValue, serializer);
    }

    private static @Nullable Object convertJsonToBson(String json) {
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
