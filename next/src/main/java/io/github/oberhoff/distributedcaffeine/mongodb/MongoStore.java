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
package io.github.oberhoff.distributedcaffeine.mongodb;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.Expected;
import io.github.oberhoff.distributedcaffeine.spi.ExpectedEntry;
import io.github.oberhoff.distributedcaffeine.spi.ReadResult;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;


/**
 * The store of one cache instance on MongoDB. Every write is an update pipeline computing the version on the server
 * (a new document starts at {@code $$NOW} in microseconds, every later write adds 1) and the write time
 * ({@code $$NOW}); a write announces itself through its change event. Single-document writes go through
 * {@code findOneAndUpdate}, which returns the version (a batch write could not); a row created on "missing" is an
 * upsert filtered on a missing version, so an existing document makes it a duplicate key, a conflict.
 * <p>
 * Every write stamps the document with the cluster time ({@code s}, {@link Row#stamp()}), which orders writes
 * across documents and never goes back, unlike {@code $$NOW} (milliseconds, the primary's clock).
 * Every read also returns the cache's control document, which holds the stamp of the last {@code invalidateAll}
 * ({@code t}, the cut-off), announced through its own change event.
 * <p>
 * Failures (VISION.md, "Write retries"): the driver retries single-document writes and reads once itself (retryable
 * writes, deduplicated by the server); beyond that, one attempt each, classified for the core, which retries: what
 * the driver labels transient is retryable, a write broken after sending has an unknown outcome, a timeout is
 * neither retried nor, for a write, known.
 */
final class MongoStore implements Store, ChangeFeed {

    private static final String FIELD_STATE = "state";
    private static final String FIELD_VERSION = "version";
    private static final String FIELD_VALUE = "value";
    private static final String FIELD_WRITE_TIME = "writeTime";
    // the stamp of a write (Row#stamp): the cluster time, which orders writes across documents and never goes back
    private static final String FIELD_STAMP = "s";
    private static final String CLUSTER_TIME = "$$CLUSTER_TIME";
    // the control document's: the stamp of the last invalidateAll
    private static final String FIELD_CUTOFF = "t";
    // the basis of a write (Row#basis): the cut-off it was decided under, as a stamp (a long, compared with the cut-off
    // read before every use)
    private static final String FIELD_BASIS = "b";
    private static final BsonInt64 NO_BASIS = new BsonInt64(Row.NO_BASIS);
    private static final String WRITE_TIME = "$writeTime";
    private static final String NOW = "$$NOW";
    private static final String SUBTRACT = "$subtract";
    private static final String FIELD_DISCRIMINATOR = "_id.d";
    private static final int CHUNK = 1_000;
    // the per-key calls of a batch in flight at a time (the driver's pool has 100 connections per server by default).
    // Measured on a single-node replica set, a put of 100: 45.5 ms one after another, 27.2 ms with 4, 13.6 ms with 16,
    // 9.4 ms with 64 on the store alone; with the instances' change streams open, 28 ms with 16 and worse with 64
    private static final int CONCURRENCY = 16;
    private static final AtomicInteger WRITER_THREADS = new AtomicInteger();
    // idle threads end after 60 s
    private static final ExecutorService WRITERS = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "distributed-caffeine-mongodb-" + WRITER_THREADS.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });
    private static final int DUPLICATE_KEY = 11000;
    private static final BsonDocument NOW_MICROS = Document.parse(
            "{$multiply: [{$toLong: '$$NOW'}, {$toLong: 1000}]}").toBsonDocument();
    // a per-key counter: a new document starts at the store's time, which lies above every version an earlier one of
    // the key had; every later write adds 1
    private static final BsonDocument VERSION = new BsonDocument("$cond", new BsonArray(List.of(
            Document.parse("{$eq: [{$type: '$version'}, 'missing']}").toBsonDocument(), NOW_MICROS,
            Document.parse("{$add: ['$version', {$toLong: 1}]}").toBsonDocument())));

    // causal consistency: see MongoClusterTime
    private static final ClientSessionOptions CAUSAL = ClientSessionOptions.builder().causallyConsistent(true).build();

    private enum Kind {
        READ, WRITE
    }

    private final MongoClient client;
    private final MongoLocation location;
    private final MongoCollection<BsonDocument> collection;
    private final MongoClusterTime clusterTime = new MongoClusterTime();

    MongoStore(MongoClient client, MongoLocation location, Duration timeout) {
        this.client = client;
        this.location = location;
        this.collection = MongoSchema.database(client, location, timeout).getCollection(location.collectionName,
                BsonDocument.class);
    }

    /**
     * Seeds the store's cluster time with a majority write of its own (a fresh start time on the control document,
     * which announces nothing): the primary that acknowledges it has a majority snapshot including everything
     * majority-committed before, so even the first reads of a cache that has seen nothing yet cannot be answered from
     * an older snapshot (as a primary re-elected after a rollback did on MongoDB 5).
     */
    void seedClusterTime() {
        run(Kind.WRITE, session -> collection.updateOne(session, Filters.eq("_id", location.controlId),
                Updates.currentDate("started"), new UpdateOptions().upsert(true)));
    }

    // reads

    @Override
    public ReadResult read(Collection<String> hashes, Duration maxAge) {
        return run(Kind.READ, session -> {
            List<BsonValue> ids = new ArrayList<>();
            hashes.forEach(hash -> ids.add(location.id(hash)));
            ids.add(location.controlId);
            Map<String, Row> rows = new LinkedHashMap<>();
            long invalidatedAll = 0;
            for (BsonDocument document : find(session, Filters.and(Filters.in("_id", ids), Filters.or(
                            Filters.eq("_id", location.controlId), Filters.ne(FIELD_STATE, State.VALUE.name()),
                            younger(maxAge))), ids.size())
                    .projection(projection())) {
                if (document.getDocument("_id").containsKey("h")) {
                    Row row = row(document);
                    rows.put(row.hash(), row);
                } else {
                    invalidatedAll = stamp(document.get(FIELD_CUTOFF));
                }
            }
            return new ReadResult(rows, invalidatedAll);
        });
    }

    @Override
    public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        return run(Kind.READ, session -> {
            List<BsonValue> ids = ids(hashes);
            ids.add(location.controlId);
            Map<String, Long> versions = new LinkedHashMap<>();
            Map<String, Long> stamps = new LinkedHashMap<>();
            Map<String, Long> bases = new LinkedHashMap<>();
            long invalidatedAll = 0;
            for (BsonDocument document : find(session, Filters.and(Filters.in("_id", ids), Filters.or(
                            Filters.eq("_id", location.controlId), Filters.and(
                                    Filters.eq(FIELD_STATE, State.VALUE.name()), younger(maxAge)))), ids.size())
                    .projection(Projections.include(FIELD_VERSION, FIELD_STAMP, FIELD_CUTOFF, FIELD_BASIS))) {
                BsonDocument id = document.getDocument("_id");
                if (id.containsKey("h")) {
                    String hash = id.getString("h").getValue();
                    versions.put(hash, document.getInt64(FIELD_VERSION).getValue());
                    stamps.put(hash, stamp(document.get(FIELD_STAMP)));
                    bases.put(hash, basis(document));
                } else {
                    invalidatedAll = stamp(document.get(FIELD_CUTOFF));
                }
            }
            return new VersionsResult(versions, stamps, bases, invalidatedAll);
        });
    }

    // writes

    @Override
    public Map<String, Long> put(Collection<Entry> entries) {
        List<Entry> sorted = sorted(entries);
        List<@Nullable BsonDocument> written = concurrently(sorted, entry -> run(Kind.WRITE, session ->
                collection.findOneAndUpdate(session, Filters.eq("_id", location.id(entry.hash())),
                        valuePipeline(entry, Row.NO_BASIS), upsert())));
        Map<String, Long> versions = new LinkedHashMap<>();
        for (int i = 0; i < sorted.size(); i++) {
            versions.put(sorted.get(i).hash(), version(written.get(i)));
        }
        return versions;
    }

    @Override
    public Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge) {
        Map<String, ExpectedEntry> byHash = new TreeMap<>();
        entries.forEach(expected -> byHash.put(expected.entry().hash(), expected));
        List<ExpectedEntry> all = new ArrayList<>(byHash.values());
        List<@Nullable BsonDocument> written = concurrently(all, expected -> {
            Entry entry = expected.entry();
            return run(Kind.WRITE, session -> expected.expectsMissing()
                    ? createIfMissing(session, entry.hash(), valuePipeline(entry, expected.basis()))
                    : collection.findOneAndUpdate(session, Filters.and(Filters.eq("_id", location.id(entry.hash())),
                    Filters.eq(FIELD_VERSION, expected.expectedVersion())), valuePipeline(entry, expected.basis()),
                    after()));
        });
        Map<String, WriteResult> results = new LinkedHashMap<>();
        List<String> conflicting = new ArrayList<>();
        for (int i = 0; i < all.size(); i++) {
            BsonDocument document = written.get(i);
            if (document != null) {
                results.put(all.get(i).entry().hash(), WriteResult.written(version(document)));
            } else {
                conflicting.add(all.get(i).entry().hash());
            }
        }
        if (!conflicting.isEmpty()) {
            try {
                results.putAll(conflicts(conflicting, maxAge));
            } catch (StoreException e) {
                throw results.isEmpty() ? e : e.afterPartlyApplied();
            }
        }
        return results;
    }

    // an upsert that may only insert: an existing document has a version, so the filter misses it and the insert
    // fails on the duplicate key; null then
    private @Nullable BsonDocument createIfMissing(ClientSession session, String hash, List<Bson> pipeline) {
        try {
            return collection.findOneAndUpdate(session, Filters.and(Filters.eq("_id", location.id(hash)),
                    Filters.exists(FIELD_VERSION, false)), pipeline, upsert());
        } catch (MongoServerException e) {
            if (e.getCode() == DUPLICATE_KEY || ErrorCategory.fromErrorCode(e.getCode()) == ErrorCategory.DUPLICATE_KEY) {
                return null;
            }
            throw e;
        }
    }

    // what prevented a conditional write: the row as a read sees it (a value too old to count is no row, but its
    // version is reported), or nothing
    private Map<String, WriteResult> conflicts(List<String> hashes, Duration maxAge) {
        return run(Kind.READ, session -> {
            Map<String, WriteResult> results = new LinkedHashMap<>();
            hashes.forEach(hash -> results.put(hash, WriteResult.conflict(null, 0)));
            for (BsonDocument document : find(session, Filters.in("_id", ids(hashes)), hashes.size())
                    .projection(projection())) {
                Row row = row(document);
                boolean counts = row.state() != State.VALUE || row.age().compareTo(maxAge) < 0;
                results.put(row.hash(), WriteResult.conflict(counts ? row : null, row.version()));
            }
            return results;
        });
    }

    @Override
    public Map<String, Long> invalidate(Collection<String> hashes) {
        List<String> sorted = new ArrayList<>(new TreeSet<>(hashes));
        List<@Nullable BsonDocument> written = concurrently(sorted, hash -> run(Kind.WRITE, session ->
                collection.findOneAndUpdate(session, Filters.eq("_id", location.id(hash)), tombstonePipeline(),
                        upsert())));
        Map<String, Long> versions = new LinkedHashMap<>();
        for (int i = 0; i < sorted.size(); i++) {
            versions.put(sorted.get(i), version(written.get(i)));
        }
        return versions;
    }

    @Override
    public Map<String, Long> invalidateIfUnchanged(Collection<Expected> expectations) {
        Map<String, Long> expected = Expected.byHash(expectations);
        List<Map.Entry<String, Long>> all = new ArrayList<>(new TreeMap<>(expected).entrySet());
        List<@Nullable BsonDocument> written = concurrently(all, entry -> run(Kind.WRITE, session ->
                entry.getValue() == ExpectedEntry.MISSING
                        ? createIfMissing(session, entry.getKey(), tombstonePipeline())
                        : collection.findOneAndUpdate(session, Filters.and(Filters.eq("_id",
                        location.id(entry.getKey())), Filters.eq(FIELD_VERSION, entry.getValue())),
                        tombstonePipeline(), after())));
        Map<String, Long> versions = new LinkedHashMap<>();
        for (int i = 0; i < all.size(); i++) {
            BsonDocument document = written.get(i);
            if (document != null) {
                versions.put(all.get(i).getKey(), version(document));
            }
        }
        return versions;
    }

    // the control document only: its stamp is the cut-off (announced by its change event), whatever the size of the
    // cache
    @Override
    public long invalidateAll() {
        BsonDocument control = run(Kind.WRITE, session -> collection.findOneAndUpdate(session,
                Filters.eq("_id", location.controlId), List.of(new BsonDocument("$set",
                        new BsonDocument(FIELD_CUTOFF, new BsonString(CLUSTER_TIME)))), upsert()));
        return stamp(requireNonNull(control, "the control document").get(FIELD_CUTOFF));
    }

    // the stamp of the last invalidateAll, 0 if none (maintenance)
    private long cutoff(ClientSession session) {
        BsonDocument control = find(session, Filters.eq("_id", location.controlId), 1)
                .projection(Projections.include(FIELD_CUTOFF)).first();
        return control == null ? 0 : stamp(control.get(FIELD_CUTOFF));
    }

    // visible under the cut-off (Row.visible): written after the last invalidateAll and decided under it; a
    // document without a basis (written before bases were stored) was decided on nothing read, as a read takes it
    private static Bson visible(long cutoff) {
        return cutoff == 0 ? Filters.empty() : Filters.and(Filters.gt(FIELD_STAMP, timestamp(cutoff)),
                Filters.or(Filters.gte(FIELD_BASIS, cutoff), Filters.exists(FIELD_BASIS, false)));
    }

    // maintenance: idempotent on several instances at once

    @Override
    public int expire(Duration maxAge) {
        return run(Kind.WRITE, session -> (int) collection.updateMany(session, Filters.and(
                        Filters.eq(FIELD_DISCRIMINATOR, location.discriminator),
                        Filters.eq(FIELD_STATE, State.VALUE.name()), visible(cutoff(session)),
                        Filters.expr(new Document("$lte", List.of(WRITE_TIME,
                                new Document(SUBTRACT, List.of(NOW, maxAge.toMillis())))))),
                List.of(new BsonDocument("$set", new BsonDocument(FIELD_STATE, new BsonString(State.EXPIRED.name()))
                                .append(FIELD_VERSION, VERSION).append(FIELD_WRITE_TIME, new BsonString(NOW))
                                .append(FIELD_STAMP, new BsonString(CLUSTER_TIME)).append(FIELD_BASIS, NO_BASIS)),
                        new BsonDocument("$unset", new BsonString(FIELD_VALUE))))
                .getModifiedCount());
    }

    // keeps the newest maxSize values (ties by hash); the documents beyond that rank are the same for every instance
    // pruning at once, so overlapping runs never prune more; a document rewritten since it was found is kept
    @Override
    public int pruneToSize(long maxSize) {
        return run(Kind.WRITE, session -> deleteUnchanged(session, find(session, Filters.and(
                        Filters.eq(FIELD_DISCRIMINATOR, location.discriminator),
                        Filters.eq(FIELD_STATE, State.VALUE.name()), visible(cutoff(session))))
                .sort(Sorts.orderBy(Sorts.descending(FIELD_WRITE_TIME), Sorts.ascending("_id.h")))
                .skip((int) Math.min(Integer.MAX_VALUE, maxSize))));
    }

    // found first, then deleted: MongoDB before 5.0 refuses $$NOW in the filter of a delete (error 51144); with the
    // rows written at or before the last invalidateAll (any state, never the control document, which has no hash)
    @Override
    public int pruneTombstones(Duration retention) {
        return run(Kind.WRITE, session -> {
            long cutoff = cutoff(session);
            Bson old = Filters.and(Filters.in(FIELD_STATE, State.TOMBSTONE.name(), State.EXPIRED.name()),
                    Filters.expr(new Document("$lte", List.of(WRITE_TIME,
                            new Document(SUBTRACT, List.of(NOW, retention.toMillis()))))));
            Bson doomed = cutoff == 0 ? old : Filters.or(old, Filters.lte(FIELD_STAMP, timestamp(cutoff)),
                    Filters.exists(FIELD_STAMP, false), Filters.lt(FIELD_BASIS, cutoff));
            return deleteUnchanged(session, find(session, Filters.and(
                    Filters.eq(FIELD_DISCRIMINATOR, location.discriminator), Filters.exists("_id.h"), doomed)));
        });
    }

    // deletes the documents found, each only if not rewritten since (its version unchanged), in chunks
    private int deleteUnchanged(ClientSession session, FindIterable<BsonDocument> found) {
        List<Bson> doomed = found.projection(Projections.include(FIELD_VERSION))
                .map(document -> Filters.and(Filters.eq("_id", document.get("_id")),
                        Filters.eq(FIELD_VERSION, document.getInt64(FIELD_VERSION).getValue())))
                .into(new ArrayList<>());
        int deleted = 0;
        for (int from = 0; from < doomed.size(); from += CHUNK) {
            deleted += (int) collection.deleteMany(session, Filters.or(doomed.subList(from,
                    Math.min(doomed.size(), from + CHUNK)))).getDeletedCount();
        }
        return deleted;
    }

    @Override
    public ChangeSubscription subscribe(ChangeListener listener) {
        return MongoWatcher.subscribe(client, location, listener, clusterTime);
    }

    @Override
    public void close() {
        // the client is the user's; subscriptions are closed by their owner
    }

    // operations

    // one attempt, in a causally consistent session; the failure classified for the core, which retries (VISION.md,
    // "Write retries"). The driver retries single-document writes and reads once itself
    private <T> T run(Kind kind, Function<ClientSession, T> work) {
        boolean write = kind == Kind.WRITE;
        try (ClientSession session = client.startSession(CAUSAL)) {
            BsonTimestamp latest = clusterTime.get();
            if (latest != null) {
                session.advanceOperationTime(latest);
            }
            T result = work.apply(session);
            clusterTime.advance(session.getOperationTime());
            return result;
        } catch (StoreException e) {
            throw e;
        } catch (MongoTimeoutException e) {
            // waited its whole time already (for a server, a pooled connection, or the answer): another attempt
            // would most likely wait as long again. A write may have been applied if the answer was waited for, which
            // the driver's timeout does not tell apart from waiting for a server (measured), so it is unknown
            throw new StoreException("Accessing " + location + " got no answer in time" + (write
                    ? ", the outcome unknown" : "") + ": " + e.getMessage(), e, write, false);
        } catch (MongoException e) {
            boolean unknown = write && mayHaveCommitted(e);
            throw new StoreException("Accessing " + location + " failed" + (unknown ? ", the outcome unknown" : "")
                    + ": " + e.getMessage(), e, unknown, isTransient(e));
        } catch (RuntimeException e) {
            // a document this store cannot read (written by hand or by another program): a store failure, not
            // one of the caller's own
            throw new StoreException("Accessing " + location + " failed: " + e, e, false, false);
        }
    }

    private static boolean isTransient(MongoException e) {
        return e instanceof MongoSocketException
                || e instanceof MongoNotPrimaryException || e instanceof MongoNodeIsRecoveringException
                || e instanceof MongoWriteConcernException || e instanceof MongoExecutionTimeoutException
                || e instanceof MongoConnectionPoolClearedException || e.hasErrorLabel("RetryableWriteError") || e.hasErrorLabel("TransientTransactionError");
    }

    // sent, and broken before the answer: the write may have committed (a timeout is decided before, in run)
    private static boolean mayHaveCommitted(MongoException e) {
        return (e instanceof MongoSocketException && !(e instanceof MongoSocketOpenException))
                || e instanceof MongoWriteConcernException;
    }


    // a client timeout (CSOT) bounds every batch of a read, not the read as a whole: it is there to notice a store that
    // does not answer, not to limit how much a read returns (pruning reads every value beyond the maximum size)
    private FindIterable<BsonDocument> find(ClientSession session, Bson filter) {
        FindIterable<BsonDocument> find = collection.find(session, filter);
        return collection.getTimeout(TimeUnit.MILLISECONDS) == null ? find : find.timeoutMode(TimeoutMode.ITERATION);
    }

    // by key: at most one document per id, all in the first batch. One more than can match, so that the server sees
    // the cursor exhausted: the default first batch of 101 documents made a read of 100 keys and the control document
    // ask for a second, empty batch (about 300 µs, measured by MongoLogicalInvalidateAllExperimentTest)
    private FindIterable<BsonDocument> find(ClientSession session, Bson filter, int ids) {
        return find(session, filter).batchSize(ids + 1);
    }

    // documents

    private List<Bson> valuePipeline(Entry entry, long basis) {
        return List.of(new BsonDocument("$set", new BsonDocument()
                .append("key", literal(MongoLocation.toBson(entry.key(), location.keyFormat)))
                .append(FIELD_VALUE, literal(MongoLocation.toBson(entry.value(), location.valueFormat)))
                .append(FIELD_STATE, new BsonString(State.VALUE.name()))
                .append(FIELD_VERSION, VERSION)
                .append(FIELD_WRITE_TIME, new BsonString(NOW))
                .append(FIELD_STAMP, new BsonString(CLUSTER_TIME))
                .append(FIELD_BASIS, literal(new BsonInt64(basis)))));
    }

    private static List<Bson> tombstonePipeline() {
        return List.of(new BsonDocument("$set", new BsonDocument()
                        .append(FIELD_STATE, new BsonString(State.TOMBSTONE.name()))
                        .append(FIELD_VERSION, VERSION)
                        .append(FIELD_WRITE_TIME, new BsonString(NOW))
                        .append(FIELD_STAMP, new BsonString(CLUSTER_TIME))
                        .append(FIELD_BASIS, literal(NO_BASIS))),
                new BsonDocument("$unset", new BsonString(FIELD_VALUE)));
    }

    // a value as it is, never a field path or an operator
    private static BsonDocument literal(BsonValue value) {
        return new BsonDocument("$literal", value);
    }

    private static BsonDocument projection() {
        return new BsonDocument("key", new BsonInt64(1)).append(FIELD_VALUE, new BsonInt64(1))
                .append(FIELD_STATE, new BsonInt64(1)).append(FIELD_VERSION, new BsonInt64(1))
                .append("age", Document.parse("{$subtract: ['$$NOW', '$writeTime']}").toBsonDocument())
                .append(FIELD_STAMP, new BsonInt64(1)).append(FIELD_CUTOFF, new BsonInt64(1))
                .append(FIELD_BASIS, new BsonInt64(1));
    }

    private static Bson younger(Duration maxAge) {
        return Filters.expr(new Document("$gt", List.of(WRITE_TIME,
                new Document(SUBTRACT, List.of(NOW, maxAge.toMillis())))));
    }

    @SuppressWarnings("java:S4449") // the value is a nullable array (byte @Nullable []), which Sonar does not read
    private Row row(BsonDocument document) {
        String hash = document.getDocument("_id").getString("h").getValue();
        State state = State.valueOf(document.getString(FIELD_STATE).getValue());
        BsonValue key = document.get("key");
        BsonValue value = document.get(FIELD_VALUE);
        BsonValue age = document.get("age");
        long ageMillis = age != null && age.isNumber() ? Math.max(0, age.asNumber().longValue()) : 0;
        return new Row(hash, key == null || key.isNull() ? new byte[0] : MongoLocation.fromBson(key, location.keyFormat),
                state == State.VALUE && value != null ? MongoLocation.fromBson(value, location.valueFormat) : null,
                state, document.getInt64(FIELD_VERSION).getValue(), Duration.ofMillis(ageMillis),
                stamp(document.get(FIELD_STAMP)), basis(document));
    }

    // a document written before bases were stored has none: decided on nothing read
    private static long basis(BsonDocument document) {
        BsonValue basis = document.get(FIELD_BASIS);
        return basis != null && basis.isInt64() ? basis.asInt64().getValue() : Row.NO_BASIS;
    }

    // a cluster time as a long that orders like it (seconds, then the increment); 0 for none (written before stamps)
    static long stamp(@Nullable BsonValue value) {
        if (value == null || !value.isTimestamp()) {
            return 0;
        }
        BsonTimestamp timestamp = value.asTimestamp();
        return ((long) timestamp.getTime() << 32) | (timestamp.getInc() & 0xFFFF_FFFFL);
    }

    private static BsonTimestamp timestamp(long stamp) {
        return new BsonTimestamp((int) (stamp >>> 32), (int) stamp);
    }

    private List<BsonValue> ids(Collection<String> hashes) {
        List<BsonValue> ids = new ArrayList<>();
        hashes.forEach(hash -> ids.add(location.id(hash)));
        return ids;
    }

    private static long version(@Nullable BsonDocument written) {
        if (written == null) {
            throw new StoreException("A write returned no document", false);
        }
        return written.getInt64(FIELD_VERSION).getValue();
    }

    private static FindOneAndUpdateOptions upsert() {
        return new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
                .projection(Projections.include(FIELD_VERSION, FIELD_CUTOFF));
    }

    private static FindOneAndUpdateOptions after() {
        return new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
                .projection(Projections.include(FIELD_VERSION));
    }

    // the per-key calls of a batch, concurrently: MongoDB writes one document per call, so one after another a batch
    // cost a round trip per key (100 keys: 46 ms, measured; VISION.md, "Performance"). At most CONCURRENCY at a time,
    // each its own round trip and session, on threads of their own (the calls block). Returns each call's result in
    // the order of the items, null where it returned none (a conflict). A failure leaves the batch's outcome unknown
    // if another call applied, as a failure partway did one after another; otherwise it is thrown as it is
    private static <I> List<@Nullable BsonDocument> concurrently(List<I> items,
                                                                Function<I, @Nullable BsonDocument> call) {
        if (items.size() <= 1) {
            return items.isEmpty() ? List.of() : Collections.singletonList(call.apply(items.get(0)));
        }
        Semaphore slots = new Semaphore(CONCURRENCY);
        List<CompletableFuture<@Nullable BsonDocument>> calls = new ArrayList<>();
        for (I item : items) {
            slots.acquireUninterruptibly();
            calls.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return call.apply(item);
                } finally {
                    slots.release();
                }
            }, WRITERS));
        }
        List<@Nullable BsonDocument> results = new ArrayList<>();
        @Nullable StoreException failure = null;
        boolean applied = false;
        for (CompletableFuture<@Nullable BsonDocument> pending : calls) {
            try {
                BsonDocument result = pending.join();
                applied |= result != null;
                results.add(result);
            } catch (CompletionException e) {
                if (!(e.getCause() instanceof StoreException storeException)) {
                    throw e;
                }
                failure = failure == null ? storeException : failure;
                results.add(null);
            }
        }
        if (failure != null) {
            throw applied ? failure.afterPartlyApplied() : failure;
        }
        return results;
    }

    private static List<Entry> sorted(Collection<Entry> entries) {
        Map<String, Entry> byHash = new TreeMap<>();
        entries.forEach(entry -> byHash.put(entry.hash(), entry));
        return new ArrayList<>(byHash.values());
    }
}
