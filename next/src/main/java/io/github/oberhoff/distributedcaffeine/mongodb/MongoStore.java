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
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.model.Updates;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
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
import java.util.List;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
 * The store of one cache instance on MongoDB. Every write is an update pipeline setting the version the core chose
 * (compared only for equality here) and the write time ({@code $$NOW}); a write announces itself through its change
 * event. One {@code updateOne} per key: unconditional as an upsert, conditional on a version as a filter on it (no
 * match is a conflict), and on "missing" as an upsert filtered on a missing version, so that an existing document
 * makes it a duplicate key, a conflict.
 * <p>
 * The control row of {@code invalidateAll} ({@link Store#CONTROL_HASH}) is a document like any other here: the core
 * writes it and reads it with the keys, and compares the ages of one read (all from one {@code $$NOW}). Maintenance
 * never touches reserved documents (hashes starting with {@code #}), leaves the documents the control row hides
 * (written at or before it) to {@link #pruneTombstones}, which deletes them.
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
    private static final String FIELD_HASH = "_id.h";
    private static final String WRITE_TIME = "$writeTime";
    // how long ago a document was written, by the server's clock: one $$NOW per query
    private static final BsonDocument AGE = Document.parse("{$subtract: ['$$NOW', '$writeTime']}").toBsonDocument();
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
     * Seeds the store's cluster time with a majority write of its own (a fresh start time on a document of its own
     * without a hash, which announces nothing): the primary that acknowledges it has a majority snapshot including
     * everything majority-committed before, so even the first reads of a cache that has seen nothing yet cannot be
     * answered from an older snapshot (as a primary re-elected after a rollback did on MongoDB 5).
     */
    void seedClusterTime() {
        run(Kind.WRITE, session -> collection.updateOne(session, Filters.eq("_id", location.startId),
                Updates.currentDate("started"), new UpdateOptions().upsert(true)));
    }

    // reads

    // every document of the keys (the core adds the control row's): a value at least as old as the maximum age
    // without its value (and so read as expired, at its version, which a write can be conditioned on); the ages of
    // one find all from one $$NOW
    @Override
    public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
        return run(Kind.READ, session -> {
            List<BsonValue> ids = ids(hashes);
            Map<String, Row> rows = new LinkedHashMap<>();
            for (BsonDocument document : find(session, Filters.in("_id", ids), ids.size())
                    .projection(projection(maxAge))) {
                Row row = row(document);
                rows.put(row.hash(), row);
            }
            return rows;
        });
    }

    @Override
    public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        return run(Kind.READ, session -> {
            List<BsonValue> ids = ids(hashes);
            Map<String, Long> versions = new LinkedHashMap<>();
            Map<String, Duration> ages = new LinkedHashMap<>();
            for (BsonDocument document : find(session, Filters.and(Filters.in("_id", ids),
                            Filters.eq(FIELD_STATE, State.VALUE.name()), younger(maxAge)), ids.size())
                    .projection(new BsonDocument(FIELD_VERSION, new BsonInt64(1)).append("age", AGE))) {
                String hash = document.getDocument("_id").getString("h").getValue();
                versions.put(hash, document.getInt64(FIELD_VERSION).getValue());
                ages.put(hash, age(document));
            }
            return new VersionsResult(versions, ages);
        });
    }

    // writes

    // one updateOne per key, in the order of the hashes, so that concurrent batches meet in the same order
    @Override
    public Set<String> write(Collection<Write> writes) {
        Map<String, Write> byHash = new TreeMap<>();
        writes.forEach(write -> byHash.put(write.hash(), write));
        List<Write> sorted = new ArrayList<>(byHash.values());
        List<Boolean> applied = concurrently(sorted, write -> run(Kind.WRITE, session -> apply(session, write)));
        Set<String> conflicting = new LinkedHashSet<>();
        for (int i = 0; i < sorted.size(); i++) {
            if (!applied.get(i)) {
                conflicting.add(sorted.get(i).hash());
            }
        }
        return conflicting;
    }

    // whether the write applied: unconditional always; on a version, if the document still had it; on "missing", an
    // upsert that may only insert: an existing document has a version, so the filter misses it and the insert fails on
    // the duplicate key
    private boolean apply(ClientSession session, Write write) {
        Bson id = Filters.eq("_id", location.id(write.hash()));
        List<Bson> pipeline = write.isTombstone() ? tombstonePipeline(write.version()) : valuePipeline(write);
        if (write.isUnconditional()) {
            collection.updateOne(session, id, pipeline, new UpdateOptions().upsert(true));
            return true;
        }
        if (!write.expectsMissing()) {
            UpdateResult result = collection.updateOne(session, Filters.and(id,
                    Filters.eq(FIELD_VERSION, write.expected())), pipeline);
            return result.getMatchedCount() > 0;
        }
        try {
            collection.updateOne(session, Filters.and(id, Filters.exists(FIELD_VERSION, false)), pipeline,
                    new UpdateOptions().upsert(true));
            return true;
        } catch (MongoServerException e) {
            if (e.getCode() == DUPLICATE_KEY || ErrorCategory.fromErrorCode(e.getCode()) == ErrorCategory.DUPLICATE_KEY) {
                return false;
            }
            throw e;
        }
    }

    // maintenance's view of the cache for expiry and size pruning: its own rows, no reserved ones, and none the
    // control row hides (written at or before it, by its write time read first): those are pruneTombstones' to delete
    private Bson maintained(ClientSession session) {
        Bson own = Filters.and(Filters.eq(FIELD_DISCRIMINATOR, location.discriminator), Filters.exists(FIELD_HASH),
                Filters.not(Filters.regex(FIELD_HASH, "^#")));
        BsonDateTime control = controlTime(session);
        return control == null ? own : Filters.and(own, Filters.gt(FIELD_WRITE_TIME, control));
    }

    // the write time of the control row, null if there is none
    private @Nullable BsonDateTime controlTime(ClientSession session) {
        BsonDocument control = find(session, Filters.eq("_id", location.id(Store.CONTROL_HASH)), 1)
                .projection(Projections.include(FIELD_WRITE_TIME)).first();
        BsonValue time = control == null ? null : control.get(FIELD_WRITE_TIME);
        return time != null && time.isDateTime() ? time.asDateTime() : null;
    }

    // maintenance: idempotent on several instances at once

    @Override
    public int expire(Duration maxAge, long version) {
        return run(Kind.WRITE, session -> (int) collection.updateMany(session, Filters.and(maintained(session),
                        Filters.eq(FIELD_STATE, State.VALUE.name()),
                        Filters.expr(new Document("$lte", List.of(WRITE_TIME,
                                new Document(SUBTRACT, List.of(NOW, maxAge.toMillis())))))),
                List.of(new BsonDocument("$set", new BsonDocument(FIELD_STATE, new BsonString(State.EXPIRED.name()))
                                .append(FIELD_VERSION, literal(new BsonInt64(version)))
                                .append(FIELD_WRITE_TIME, new BsonString(NOW))),
                        new BsonDocument("$unset", new BsonString(FIELD_VALUE))))
                .getModifiedCount());
    }

    // keeps the newest maxSize values (ties by hash); the documents beyond that rank are the same for every instance
    // pruning at once, so overlapping runs never prune more; a document rewritten since it was found is kept
    @Override
    public int pruneToSize(long maxSize) {
        return run(Kind.WRITE, session -> deleteUnchanged(session, find(session, Filters.and(maintained(session),
                        Filters.eq(FIELD_STATE, State.VALUE.name())))
                .sort(Sorts.orderBy(Sorts.descending(FIELD_WRITE_TIME), Sorts.ascending("_id.h")))
                .skip((int) Math.min(Integer.MAX_VALUE, maxSize))));
    }

    // found first, then deleted: MongoDB before 5.0 refuses $$NOW in the filter of a delete (error 51144); with the
    // rows written at or before the control row (any state), never a reserved one
    @Override
    public int pruneTombstones(Duration retention) {
        return run(Kind.WRITE, session -> {
            BsonDateTime control = controlTime(session);
            Bson old = Filters.and(Filters.in(FIELD_STATE, State.TOMBSTONE.name(), State.EXPIRED.name()),
                    Filters.expr(new Document("$lte", List.of(WRITE_TIME,
                            new Document(SUBTRACT, List.of(NOW, retention.toMillis()))))));
            Bson doomed = control == null ? old : Filters.or(old, Filters.lte(FIELD_WRITE_TIME, control));
            return deleteUnchanged(session, find(session, Filters.and(
                    Filters.eq(FIELD_DISCRIMINATOR, location.discriminator), Filters.exists(FIELD_HASH),
                    Filters.not(Filters.regex(FIELD_HASH, "^#")), doomed)));
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

    private List<Bson> valuePipeline(Write write) {
        return List.of(new BsonDocument("$set", new BsonDocument()
                .append("key", literal(MongoLocation.toBson(requireNonNull(write.key()), location.keyFormat)))
                .append(FIELD_VALUE, literal(MongoLocation.toBson(requireNonNull(write.value()), location.valueFormat)))
                .append(FIELD_STATE, new BsonString(State.VALUE.name()))
                .append(FIELD_VERSION, literal(new BsonInt64(write.version())))
                .append(FIELD_WRITE_TIME, new BsonString(NOW))));
    }

    // keeps the document's key; a new tombstone has none
    private static List<Bson> tombstonePipeline(long version) {
        return List.of(new BsonDocument("$set", new BsonDocument()
                        .append(FIELD_STATE, new BsonString(State.TOMBSTONE.name()))
                        .append(FIELD_VERSION, literal(new BsonInt64(version)))
                        .append(FIELD_WRITE_TIME, new BsonString(NOW))),
                new BsonDocument("$unset", new BsonString(FIELD_VALUE)));
    }

    // a value as it is, never a field path or an operator
    private static BsonDocument literal(BsonValue value) {
        return new BsonDocument("$literal", value);
    }

    // a value only if younger than the maximum age (row() reads a value document without one as expired)
    private static BsonDocument projection(Duration maxAge) {
        BsonDocument youngValue = new BsonDocument("$cond", new BsonArray(List.of(
                new Document("$gt", List.of(WRITE_TIME, new Document(SUBTRACT, List.of(NOW, maxAge.toMillis()))))
                        .toBsonDocument(), new BsonString("$" + FIELD_VALUE), new BsonString("$$REMOVE"))));
        return new BsonDocument("key", new BsonInt64(1)).append(FIELD_VALUE, youngValue)
                .append(FIELD_STATE, new BsonInt64(1)).append(FIELD_VERSION, new BsonInt64(1))
                .append("age", AGE);
    }

    private static Duration age(BsonDocument document) {
        BsonValue age = document.get("age");
        return Duration.ofMillis(age != null && age.isNumber() ? Math.max(0, age.asNumber().longValue()) : 0);
    }

    private static Bson younger(Duration maxAge) {
        return Filters.expr(new Document("$gt", List.of(WRITE_TIME,
                new Document(SUBTRACT, List.of(NOW, maxAge.toMillis())))));
    }

    @SuppressWarnings("java:S4449") // the value is a nullable array (byte @Nullable []), which Sonar does not read
    private Row row(BsonDocument document) {
        String hash = document.getDocument("_id").getString("h").getValue();
        State stored = State.valueOf(document.getString(FIELD_STATE).getValue());
        BsonValue key = document.get("key");
        BsonValue value = document.get(FIELD_VALUE);
        // a value projected away as too old: expired, at its version
        State state = stored == State.VALUE && value == null ? State.EXPIRED : stored;
        return new Row(hash, key == null || key.isNull() ? new byte[0] : MongoLocation.fromBson(key, location.keyFormat),
                state == State.VALUE && value != null ? MongoLocation.fromBson(value, location.valueFormat) : null,
                state, document.getInt64(FIELD_VERSION).getValue(), age(document));
    }

    private List<BsonValue> ids(Collection<String> hashes) {
        List<BsonValue> ids = new ArrayList<>();
        hashes.forEach(hash -> ids.add(location.id(hash)));
        return ids;
    }

    // the per-key calls of a batch, concurrently: MongoDB writes one document per call, so one after another a batch
    // cost a round trip per key (100 keys: 46 ms, measured; VISION.md, "Performance"). At most CONCURRENCY at a time,
    // each its own round trip and session, on threads of their own (the calls block). Returns each call's result in
    // the order of the items, whether each applied (false: a conflict). A failure leaves the batch's outcome unknown
    // if another call applied, as a failure partway did one after another; otherwise it is thrown as it is
    private static <I> List<Boolean> concurrently(List<I> items, Function<I, Boolean> call) {
        if (items.size() <= 1) {
            return items.isEmpty() ? List.of() : Collections.singletonList(call.apply(items.get(0)));
        }
        Semaphore slots = new Semaphore(CONCURRENCY);
        List<CompletableFuture<Boolean>> calls = new ArrayList<>();
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
        List<Boolean> results = new ArrayList<>();
        @Nullable StoreException failure = null;
        boolean applied = false;
        for (CompletableFuture<Boolean> pending : calls) {
            try {
                boolean result = pending.join();
                applied |= result;
                results.add(result);
            } catch (CompletionException e) {
                if (!(e.getCause() instanceof StoreException storeException)) {
                    throw e;
                }
                failure = failure == null ? storeException : failure;
                results.add(false);
            }
        }
        if (failure != null) {
            throw applied ? failure.afterPartlyApplied() : failure;
        }
        return results;
    }
}
