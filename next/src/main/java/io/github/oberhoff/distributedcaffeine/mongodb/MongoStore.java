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
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
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
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * The store of one cache instance on MongoDB. Every write is an update pipeline setting the version the core chose
 * (compared only for equality here) and the write time ({@code $$NOW}); a write announces itself through its change
 * event. A batch of writes is one unordered bulk write, one round trip, with an upsert per key whose outcome
 * MongoDB reports per write (see {@link #write}).
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
    public WriteResult write(Collection<Write> writes) {
        Map<String, Write> byHash = new TreeMap<>();
        writes.forEach(write -> byHash.put(write.hash(), write));
        List<Write> sorted = new ArrayList<>(byHash.values());
        if (sorted.isEmpty()) {
            return WriteResult.allApplied();
        }
        List<UpdateOneModel<BsonDocument>> models = new ArrayList<>();
        sorted.forEach(write -> models.add(model(write)));
        return WriteResult.refused(run(Kind.WRITE, session -> conflicts(sorted, bulkWrite(session, sorted, models))));
    }

    // what one bulk write reports per write: the indexes of those that failed on the duplicate key (a document already
    // there: a conflict for a write on "missing" or on a version), and of those that upserted (no document there: for a
    // write on a version, a conflict, written as a tombstone by its pipeline)
    private record Outcome(Set<Integer> duplicates, Set<Integer> upserted) {
    }

    /**
     * Every write of a batch in one round trip: one unordered bulk write of single-document upserts, whose outcome
     * MongoDB reports per write by its index, so that no conflict needs a read (VISION.md, build plan, step 5):
     * <ul>
     *     <li>advancing: an upsert filtered on the id and a smaller version; a document of a larger or equal one makes
     *     the insert fail on the duplicate key, the refusal (the version only grows);</li>
     *     <li>on "missing": an upsert filtered on the id and a missing version; an existing document has a version, so
     *     the filter misses it and the insert fails on the duplicate key, the conflict;</li>
     *     <li>on a version: an upsert filtered on the id and that version; a document with another one makes the
     *     insert fail on the duplicate key, the conflict; no document at all (pruned since it was read) makes it
     *     insert, reported as an upsert: a conflict too, and the pipeline, finding none of a stored document's fields,
     *     writes it as a tombstone, so that no value from a failed condition is ever visible.</li>
     * </ul>
     * Unordered, so that one key's conflict does not stop the others; a failure other than a conflict leaves the
     * batch's outcome unknown if another write of it applied.
     */
    private Outcome bulkWrite(ClientSession session, List<Write> sorted, List<UpdateOneModel<BsonDocument>> models) {
        try {
            BulkWriteResult result = collection.bulkWrite(session, models, new BulkWriteOptions().ordered(false));
            return new Outcome(Set.of(), upserted(result));
        } catch (MongoBulkWriteException e) {
            Set<Integer> duplicates = new HashSet<>();
            List<BulkWriteError> failures = new ArrayList<>();
            for (BulkWriteError error : e.getWriteErrors()) {
                if (isDuplicateKey(error.getCode())) {
                    duplicates.add(error.getIndex());
                } else {
                    failures.add(error);
                }
            }
            BulkWriteResult result = e.getWriteResult();
            boolean applied = result.getMatchedCount() > 0 || !result.getUpserts().isEmpty()
                    || duplicates.size() < sorted.size() - failures.size();
            if (e.getWriteConcernError() != null) {
                // applied on the primary, not confirmed by the majority: the outcome of every write unknown
                throw new StoreException("Writing to " + location + " was not confirmed, the outcome unknown: "
                        + e.getMessage(), e, true, true);
            }
            if (!failures.isEmpty()) {
                // every duplicate key is a conflict or a refusal (above); any other failure of a write is the store's
                StoreException failure = new StoreException("Writing to " + location + " failed: " + e.getMessage(),
                        e, false, false);
                throw applied ? failure.afterPartlyApplied() : failure;
            }
            return new Outcome(duplicates, upserted(result));
        }
    }

    private static Set<Integer> upserted(BulkWriteResult result) {
        Set<Integer> upserted = new HashSet<>();
        result.getUpserts().forEach(upsert -> upserted.add(upsert.getIndex()));
        return upserted;
    }

    private static Set<String> conflicts(List<Write> sorted, Outcome outcome) {
        Set<String> conflicting = new LinkedHashSet<>();
        for (int index = 0; index < sorted.size(); index++) {
            Write write = sorted.get(index);
            boolean versioned = !write.isAdvance() && !write.expectsMissing();
            if (outcome.duplicates().contains(index) || (versioned && outcome.upserted().contains(index))) {
                conflicting.add(write.hash());
            }
        }
        return conflicting;
    }

    private static boolean isDuplicateKey(int code) {
        return code == DUPLICATE_KEY || ErrorCategory.fromErrorCode(code) == ErrorCategory.DUPLICATE_KEY;
    }

    // the upsert of one write, its filter by its condition (see bulkWrite)
    private UpdateOneModel<BsonDocument> model(Write write) {
        Bson id = Filters.eq("_id", location.id(write.hash()));
        Bson filter;
        List<Bson> pipeline;
        if (write.isAdvance()) {
            filter = Filters.and(id, Filters.lt(FIELD_VERSION, write.version()));
            pipeline = write.isTombstone() ? tombstonePipeline(write.version()) : valuePipeline(write, false);
        } else if (write.expectsMissing()) {
            filter = Filters.and(id, Filters.exists(FIELD_VERSION, false));
            pipeline = write.isTombstone() ? tombstonePipeline(write.version()) : valuePipeline(write, false);
        } else {
            filter = Filters.and(id, Filters.eq(FIELD_VERSION, write.expected()));
            pipeline = write.isTombstone() ? tombstonePipeline(write.version()) : valuePipeline(write, true);
        }
        return new UpdateOneModel<>(filter, pipeline, new UpdateOptions().upsert(true));
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
                        // the version only grows: a document written at a larger one since is left for a later run
                        Filters.lt(FIELD_VERSION, version),
                        Filters.expr(new Document("$lte", List.of(WRITE_TIME,
                                new Document(SUBTRACT, List.of(NOW, maxAge.toMillis())))))),
                List.of(new BsonDocument("$set", new BsonDocument(FIELD_STATE, new BsonString(State.EXPIRED.name()))
                                .append(FIELD_VERSION, literal(new BsonInt64(version)))
                                .append(FIELD_WRITE_TIME, new BsonString(NOW))),
                        new BsonDocument("$unset", new BsonString(FIELD_VALUE))))
                .getModifiedCount());
    }

    // keeps the newest maxSize values (ties by hash); the documents beyond that rank are the same for every instance
    // pruning at once, so overlapping runs never prune more; of those, the ones younger than the minimum age stay (by
    // the server's clock: the age projected with the rank); a document rewritten since it was found is kept
    @Override
    public int pruneToSize(long maxSize, Duration minAge) {
        return run(Kind.WRITE, session -> deleteUnchanged(session, find(session, Filters.and(maintained(session),
                        Filters.eq(FIELD_STATE, State.VALUE.name())))
                .sort(Sorts.orderBy(Sorts.descending(FIELD_WRITE_TIME), Sorts.ascending("_id.h")))
                .skip((int) Math.min(Integer.MAX_VALUE, maxSize))
                .projection(Projections.fields(Projections.include(FIELD_VERSION), new BsonDocument("age", AGE))),
                document -> age(document).compareTo(minAge) >= 0));
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
        return deleteUnchanged(session, found.projection(Projections.include(FIELD_VERSION)), document -> true);
    }

    // the same for the documents found that the condition accepts (found with their version)
    private int deleteUnchanged(ClientSession session, FindIterable<BsonDocument> found,
                                Predicate<BsonDocument> accepted) {
        List<Bson> doomed = new ArrayList<>();
        for (BsonDocument document : found) {
            if (accepted.test(document)) {
                doomed.add(Filters.and(Filters.eq("_id", document.get("_id")),
                        Filters.eq(FIELD_VERSION, document.getInt64(FIELD_VERSION).getValue())));
            }
        }
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

    // onVersion: a write on a version, which upserts where no document is (pruned since it was read): there, with no
    // write time yet (only the filter's fields), it writes a tombstone instead, the write being a conflict
    private List<Bson> valuePipeline(Write write, boolean onVersion) {
        BsonValue value = literal(MongoLocation.toBson(requireNonNull(write.value()), location.valueFormat));
        BsonValue state = new BsonString(State.VALUE.name());
        if (onVersion) {
            BsonDocument isNew = new BsonDocument("$eq", new BsonArray(List.of(
                    new BsonDocument("$type", new BsonString(WRITE_TIME)), new BsonString("missing"))));
            value = new BsonDocument("$cond", new BsonArray(List.of(isNew, new BsonString("$$REMOVE"), value)));
            state = new BsonDocument("$cond", new BsonArray(List.of(isNew,
                    new BsonString(State.TOMBSTONE.name()), state)));
        }
        return List.of(new BsonDocument("$set", new BsonDocument()
                .append("key", literal(MongoLocation.toBson(requireNonNull(write.key()), location.keyFormat)))
                .append(FIELD_VALUE, value)
                .append(FIELD_STATE, state)
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
        // negative for a document written after the query's $$NOW (the query reads document by document), as it is,
        // so that the ages of one query still compare like the write times (Store#read)
        return Duration.ofMillis(age != null && age.isNumber() ? age.asNumber().longValue() : 0);
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
        byte[] bytes = stored == State.VALUE && value != null
                ? MongoLocation.fromBson(value, location.valueFormat) : null;
        // a value projected away as too old: expired, at its version
        State state = stored == State.VALUE && bytes == null ? State.EXPIRED : stored;
        return new Row(hash, key == null || key.isNull() ? new byte[0] : MongoLocation.fromBson(key, location.keyFormat),
                bytes, state, document.getInt64(FIELD_VERSION).getValue(), age(document));
    }

    private List<BsonValue> ids(Collection<String> hashes) {
        List<BsonValue> ids = new ArrayList<>();
        hashes.forEach(hash -> ids.add(location.id(hash)));
        return ids;
    }
}
