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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.ExpectedEntry;
import io.github.oberhoff.distributedcaffeine.spi.Listener;
import io.github.oberhoff.distributedcaffeine.spi.ReadResult;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.Subscription;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


/**
 * The store of one cache instance on MongoDB. Every write is an update pipeline computing the version on the server
 * ({@code max(old + 1, $$NOW in microseconds)}; {@code $$NOW} has millisecond resolution) and the write time
 * ({@code $$NOW}); a write announces itself through its change event. Single-document writes go through
 * {@code findOneAndUpdate}, which returns the version (a batch write could not); a row created on "missing" is an
 * upsert filtered on a missing version, so an existing document makes it a duplicate key, a conflict.
 * <p>
 * Every read also returns the cache's control document, whose {@code $$NOW} projection is the read's time (also when
 * no row exists); the control document holds the time of the last {@code invalidateAll}, announced through its own
 * change event. Times are the floor of the millisecond, the {@code invalidateAll} time its ceiling, so a load that
 * read in the same millisecond as an {@code invalidateAll} counts as before it.
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
    private static final String WRITE_TIME = "$writeTime";
    private static final String NOW = "$$NOW";
    private static final String SUBTRACT = "$subtract";
    private static final String FIELD_DISCRIMINATOR = "_id.d";
    private static final int CHUNK = 1_000;
    private static final int DUPLICATE_KEY = 11000;
    private static final BsonDocument NOW_MICROS = Document.parse(
            "{$multiply: [{$toLong: '$$NOW'}, {$toLong: 1000}]}").toBsonDocument();
    private static final BsonDocument VERSION = new BsonDocument("$max", new BsonArray(List.of(
            Document.parse("{$add: [{$ifNull: ['$version', {$toLong: 0}]}, {$toLong: 1}]}").toBsonDocument(),
            NOW_MICROS)));

    // causal consistency: see MongoClusterTime
    private static final ClientSessionOptions CAUSAL = ClientSessionOptions.builder().causallyConsistent(true).build();

    private enum Kind {
        READ, WRITE
    }

    private final MongoClient client;
    private final MongoLocation location;
    private final MongoCollection<BsonDocument> collection;
    private final MongoClusterTime clusterTime = new MongoClusterTime();
    private final Duration timeout;

    MongoStore(MongoClient client, MongoLocation location, Duration timeout) {
        this.client = client;
        this.location = location;
        this.timeout = timeout;
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
            long time = 0;
            for (BsonDocument document : find(session, Filters.and(Filters.in("_id", ids), Filters.or(
                            Filters.eq("_id", location.controlId), Filters.ne(FIELD_STATE, State.VALUE.name()),
                            younger(maxAge))))
                    .projection(projection())) {
                time = document.getInt64("now").getValue() * 1_000;
                if (document.getDocument("_id").containsKey("h")) {
                    Row row = row(document);
                    rows.put(row.hash(), row);
                }
            }
            return new ReadResult(rows, time > 0 ? time : serverTimeMicros(session));
        });
    }

    @Override
    public Map<String, Long> readVersions(Collection<String> hashes, Duration maxAge) {
        if (hashes.isEmpty()) {
            return Map.of();
        }
        return run(Kind.READ, session -> {
            Map<String, Long> versions = new LinkedHashMap<>();
            for (BsonDocument document : find(session, Filters.and(Filters.in("_id", ids(hashes)),
                            Filters.eq(FIELD_STATE, State.VALUE.name()), younger(maxAge)))
                    .projection(Projections.include(FIELD_VERSION))) {
                versions.put(document.getDocument("_id").getString("h").getValue(),
                        document.getInt64(FIELD_VERSION).getValue());
            }
            return versions;
        });
    }

    // writes

    @Override
    public Map<String, Long> put(Collection<Entry> entries) {
        Map<String, Long> versions = new LinkedHashMap<>();
        try {
            for (Entry entry : sorted(entries)) {
                BsonDocument written = run(Kind.WRITE, session -> collection.findOneAndUpdate(session, 
                        Filters.eq("_id", location.id(entry.hash())), valuePipeline(entry, VERSION), upsert()));
                versions.put(entry.hash(), version(written));
            }
        } catch (StoreException e) {
            throw versions.isEmpty() ? e : e.afterPartlyApplied();
        }
        return versions;
    }

    @Override
    public Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge) {
        Map<String, ExpectedEntry> byHash = new TreeMap<>();
        entries.forEach(expected -> byHash.put(expected.entry().hash(), expected));
        Map<String, WriteResult> results = new LinkedHashMap<>();
        List<String> conflicting = new ArrayList<>();
        try {
            for (ExpectedEntry expected : byHash.values()) {
                Entry entry = expected.entry();
                BsonDocument written = run(Kind.WRITE, session -> expected.expectsMissing()
                        ? createIfMissing(session, entry.hash(), valuePipeline(entry, new BsonDocument("$literal",
                        new BsonInt64(expected.createdVersion()))))
                        : collection.findOneAndUpdate(session, Filters.and(Filters.eq("_id", location.id(entry.hash())),
                        Filters.eq(FIELD_VERSION, expected.expectedVersion())), valuePipeline(entry, VERSION), after()));
                if (written != null) {
                    results.put(entry.hash(), WriteResult.written(version(written)));
                } else {
                    conflicting.add(entry.hash());
                }
            }
            if (!conflicting.isEmpty()) {
                results.putAll(conflicts(conflicting, maxAge));
            }
        } catch (StoreException e) {
            throw results.isEmpty() ? e : e.afterPartlyApplied();
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
            for (BsonDocument document : find(session, Filters.in("_id", ids(hashes))).projection(projection())) {
                Row row = row(document);
                boolean counts = row.state() != State.VALUE || row.age().compareTo(maxAge) < 0;
                results.put(row.hash(), WriteResult.conflict(counts ? row : null, row.version()));
            }
            return results;
        });
    }

    @Override
    public Map<String, Long> invalidate(Collection<String> hashes) {
        Map<String, Long> versions = new LinkedHashMap<>();
        try {
            for (String hash : new TreeSet<>(hashes)) {
                BsonDocument written = run(Kind.WRITE, session -> collection.findOneAndUpdate(session, 
                        Filters.eq("_id", location.id(hash)), tombstonePipeline(), upsert()));
                versions.put(hash, version(written));
            }
        } catch (StoreException e) {
            throw versions.isEmpty() ? e : e.afterPartlyApplied();
        }
        return versions;
    }

    @Override
    public Map<String, Long> invalidateIfUnchanged(Map<String, Long> expected) {
        Map<String, Long> versions = new LinkedHashMap<>();
        try {
            for (Map.Entry<String, Long> entry : new TreeMap<>(expected).entrySet()) {
                String hash = entry.getKey();
                BsonDocument written = run(Kind.WRITE, session -> entry.getValue() == ExpectedEntry.MISSING
                        ? createIfMissing(session, hash, tombstonePipeline())
                        : collection.findOneAndUpdate(session, Filters.and(Filters.eq("_id", location.id(hash)),
                        Filters.eq(FIELD_VERSION, entry.getValue())), tombstonePipeline(), after()));
                if (written != null) {
                    versions.put(hash, version(written));
                }
            }
        } catch (StoreException e) {
            throw versions.isEmpty() ? e : e.afterPartlyApplied();
        }
        return versions;
    }

    // first the control document takes the time T (the ceiling of the millisecond), announced by its change event;
    // then every row gets a version of at least T (value rows become tombstones). Not atomic across documents: a load
    // in flight conflicts on whichever document it read, before or after the sweep reached it
    @Override
    public long invalidateAll() {
        BsonDocument control = run(Kind.WRITE, session -> collection.findOneAndUpdate(session, 
                Filters.eq("_id", location.controlId), List.of(new BsonDocument("$set", new BsonDocument("t",
                        Document.parse("{$max: [{$add: [{$ifNull: ['$t', {$toLong: 0}]}, {$toLong: 1}]}, "
                                + "{$add: [{$multiply: [{$toLong: '$$NOW'}, {$toLong: 1000}]}, {$toLong: 999}]}]}")
                                .toBsonDocument()))), upsert()));
        long time = control.getInt64("t").getValue();
        BsonDocument isValue = Document.parse("{$eq: ['$state', 'VALUE']}").toBsonDocument();
        run(Kind.WRITE, session -> collection.updateMany(session, Filters.and(
                Filters.eq(FIELD_DISCRIMINATOR, location.discriminator), Filters.exists("_id.h")), List.of(new BsonDocument("$set", new BsonDocument()
                .append(FIELD_STATE, cond(isValue, new BsonString(State.TOMBSTONE.name()), new BsonString("$state")))
                .append(FIELD_VALUE, cond(isValue, new BsonString("$$REMOVE"), new BsonString("$value")))
                .append(FIELD_WRITE_TIME, cond(isValue, new BsonString(NOW), new BsonString(WRITE_TIME)))
                .append(FIELD_VERSION, new BsonDocument("$max", new BsonArray(List.of(
                        Document.parse("{$add: ['$version', {$toLong: 1}]}").toBsonDocument(),
                        new BsonInt64(time)))))))));
        return time;
    }

    // maintenance: idempotent on several instances at once

    @Override
    public int expire(Duration maxAge) {
        return run(Kind.WRITE, session -> (int) collection.updateMany(session, Filters.and(
                        Filters.eq(FIELD_DISCRIMINATOR, location.discriminator),
                        Filters.eq(FIELD_STATE, State.VALUE.name()),
                        Filters.expr(new Document("$lte", List.of(WRITE_TIME,
                                new Document(SUBTRACT, List.of(NOW, maxAge.toMillis())))))),
                List.of(new BsonDocument("$set", new BsonDocument(FIELD_STATE, new BsonString(State.EXPIRED.name()))
                                .append(FIELD_VERSION, VERSION)),
                        new BsonDocument("$unset", new BsonString(FIELD_VALUE))))
                .getModifiedCount());
    }

    // keeps the newest maxSize values (ties by hash); the documents beyond that rank are the same for every instance
    // pruning at once, so overlapping runs never prune more; a document rewritten since it was found is kept
    @Override
    public int pruneToSize(long maxSize) {
        return run(Kind.WRITE, session -> deleteUnchanged(session, find(session, Filters.and(
                        Filters.eq(FIELD_DISCRIMINATOR, location.discriminator),
                        Filters.eq(FIELD_STATE, State.VALUE.name())))
                .sort(Sorts.orderBy(Sorts.descending(FIELD_WRITE_TIME), Sorts.ascending("_id.h")))
                .skip((int) Math.min(Integer.MAX_VALUE, maxSize))));
    }

    // found first, then deleted: MongoDB before 5.0 refuses $$NOW in the filter of a delete (error 51144)
    @Override
    public int pruneTombstones(Duration retention) {
        return run(Kind.WRITE, session -> deleteUnchanged(session, find(session, Filters.and(
                Filters.eq(FIELD_DISCRIMINATOR, location.discriminator),
                Filters.in(FIELD_STATE, State.TOMBSTONE.name(), State.EXPIRED.name()),
                Filters.expr(new Document("$lte", List.of(WRITE_TIME,
                        new Document(SUBTRACT, List.of(NOW, retention.toMillis())))))))));
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
    public Subscription subscribe(Listener listener) {
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

    // documents

    private List<Bson> valuePipeline(Entry entry, BsonDocument version) {
        return List.of(new BsonDocument("$set", new BsonDocument()
                .append("key", literal(MongoLocation.toBson(entry.key(), location.keyFormat)))
                .append(FIELD_VALUE, literal(MongoLocation.toBson(entry.value(), location.valueFormat)))
                .append(FIELD_STATE, new BsonString(State.VALUE.name()))
                .append(FIELD_VERSION, version)
                .append(FIELD_WRITE_TIME, new BsonString(NOW))));
    }

    private static List<Bson> tombstonePipeline() {
        return List.of(new BsonDocument("$set", new BsonDocument()
                        .append(FIELD_STATE, new BsonString(State.TOMBSTONE.name()))
                        .append(FIELD_VERSION, VERSION)
                        .append(FIELD_WRITE_TIME, new BsonString(NOW))),
                new BsonDocument("$unset", new BsonString(FIELD_VALUE)));
    }

    // a value as it is, never a field path or an operator
    private static BsonDocument literal(BsonValue value) {
        return new BsonDocument("$literal", value);
    }

    private static BsonDocument cond(BsonDocument condition, BsonValue then, BsonValue otherwise) {
        return new BsonDocument("$cond", new BsonArray(List.of(condition, then, otherwise)));
    }

    private static BsonDocument projection() {
        return new BsonDocument("key", new BsonInt64(1)).append(FIELD_VALUE, new BsonInt64(1))
                .append(FIELD_STATE, new BsonInt64(1)).append(FIELD_VERSION, new BsonInt64(1))
                .append("age", Document.parse("{$subtract: ['$$NOW', '$writeTime']}").toBsonDocument())
                .append("now", Document.parse("{$toLong: '$$NOW'}").toBsonDocument());
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
                state, document.getInt64(FIELD_VERSION).getValue(), Duration.ofMillis(ageMillis));
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
                .projection(Projections.include(FIELD_VERSION, "t"));
    }

    private static FindOneAndUpdateOptions after() {
        return new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
                .projection(Projections.include(FIELD_VERSION));
    }

    // the control document is missing (dropped by hand): the server's clock from hello
    private long serverTimeMicros(ClientSession session) {
        MongoDatabase admin = client.getDatabase("admin").withTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
        Document hello = MongoSchema.hello(command -> admin.runCommand(session, command));
        return hello.getDate("localTime").toInstant().toEpochMilli() * 1_000;
    }

    private static List<Entry> sorted(Collection<Entry> entries) {
        Map<String, Entry> byHash = new TreeMap<>();
        entries.forEach(entry -> byHash.put(entry.hash(), entry));
        return new ArrayList<>(byHash.values());
    }
}
