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
package io.github.oberhoff.distributedcaffeine.experiments;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Today's operations against those of the logical {@code invalidateAll} (VISION.md, "invalidateAll, logical"): reads
 * projecting {@code $$CLUSTER_TIME} instead of {@code $$NOW}, {@code readVersions} with the control document, puts
 * with the cluster time as version, and {@code invalidateAll} as one document instead of the sweep (with a change
 * stream open, as in a cache, counting its events). Same collection, same data, majority journaled writes in a causally
 * consistent session, the variants alternating in rounds.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=MongoLogicalInvalidateAllExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it
@SuppressWarnings("java:S2699")
class MongoLogicalInvalidateAllExperimentTest {

    private static final int KEYS = 10_000;
    private static final int OPERATIONS = 2_000;
    private static final int ROUNDS = 3;
    private static final int WRITERS = 16;
    private static final BsonDocument CONTROL = new BsonDocument("d", new BsonString("d"))
            .append("c", new BsonString("control"));
    private static final Bson YOUNGER = Filters.expr(Document.parse(
            "{$gt: ['$writeTime', {$subtract: ['$$NOW', 3600000]}]}"));
    private static final Document PROJECTION_TODAY = new Document("value", 1).append("state", 1).append("version", 1)
            .append("age", new Document("$subtract", List.of("$$NOW", "$writeTime")))
            .append("now", new Document("$toLong", "$$NOW"));
    private static final Document PROJECTION_LOGICAL = new Document("value", 1).append("state", 1).append("version", 1)
            .append("t", 1).append("age", new Document("$subtract", List.of("$$NOW", "$writeTime")))
            .append("now", "$$CLUSTER_TIME");
    private static final List<BsonDocument> PUT_TODAY = List.of(BsonDocument.parse("""
            {$set: {value: '%VALUE%', state: 'VALUE', writeTime: '$$NOW', version: {$max: [
                    {$add: [{$ifNull: ['$version', {$toLong: 0}]}, {$toLong: 1}]},
                    {$multiply: [{$toLong: '$$NOW'}, {$toLong: 1000}]}]}}}""".replace("%VALUE%", "v".repeat(100))));
    private static final List<BsonDocument> PUT_LOGICAL = List.of(BsonDocument.parse("""
            {$set: {value: '%VALUE%', state: 'VALUE', writeTime: '$$NOW', version: '$$CLUSTER_TIME'}}"""
            .replace("%VALUE%", "v".repeat(100))));
    private static final FindOneAndUpdateOptions UPSERT_AFTER = new FindOneAndUpdateOptions().upsert(true)
            .returnDocument(ReturnDocument.AFTER);

    @ParameterizedTest
    @ValueSource(strings = {"mongo:9"})
    void measure(String image) throws Exception {
        try (MongoDBContainer container = new MongoDBContainer(image).withReplicaSet()) {
            container.start();
            try (MongoClient client = MongoClients.create(container.getReplicaSetUrl())) {
                MongoCollection<BsonDocument> rows = client.getDatabase("li")
                        .withReadConcern(ReadConcern.MAJORITY).withReadPreference(ReadPreference.primary())
                        .withWriteConcern(WriteConcern.MAJORITY.withJournal(true))
                        .getCollection("rows", BsonDocument.class);
                System.out.printf("%s, %d keys, %d operations per variant and round, %d rounds%n", image, KEYS,
                        OPERATIONS, ROUNDS);
                for (int round = 1; round <= ROUNDS; round++) {
                    fill(rows, KEYS);
                    single(client, "read, 1 key       today  ", read(rows, PROJECTION_TODAY, 1));
                    single(client, "read, 1 key       logical", read(rows, PROJECTION_LOGICAL, 1));
                    single(client, "read, 100 keys    today  ", read(rows, PROJECTION_TODAY, 100));
                    single(client, "read, 100 keys    logical", read(rows, PROJECTION_LOGICAL, 100));
                    single(client, "readVersions, 100 today  ", versions(rows, false));
                    single(client, "readVersions, 100 logical", versions(rows, true));
                    single(client, "readVersions, 100 logical, state in the projection", versionsProjected(rows));
                    single(client, "readVersions, 100 today's query, one more id", (session, index) -> {
                        List<BsonValue> ids = ids(index, 100);
                        ids.add(CONTROL);
                        rows.find(session, Filters.and(Filters.in("_id", ids), Filters.eq("state", "VALUE"), YOUNGER))
                                .projection(new Document("version", 1)).into(new ArrayList<>());
                    });
                    single(client, "readVersions, 100 today's query, t projected", (session, index) -> rows
                            .find(session, Filters.and(Filters.in("_id", ids(index, 100)), Filters.eq("state", "VALUE"),
                                    YOUNGER))
                            .projection(new Document("version", 1).append("t", 1)).into(new ArrayList<>()));
                    single(client, "put, 1 writer     today  ", put(rows, PUT_TODAY));
                    single(client, "put, 1 writer     logical", put(rows, PUT_LOGICAL));
                    fill(rows, KEYS); // today's version formula cannot add to the cluster time the logical one wrote
                    parallel(client, "put, 16 writers   today  ", put(rows, PUT_TODAY));
                    parallel(client, "put, 16 writers   logical", put(rows, PUT_LOGICAL));
                }
                for (int count : new int[]{10_000, 100_000}) {
                    fill(rows, count);
                    long[] today = invalidateAll(client, rows, true);
                    fill(rows, count);
                    long[] logical = invalidateAll(client, rows, false);
                    System.out.printf("invalidateAll, %,d rows: today %,d µs and %,d events, logical %,d µs and %,d "
                            + "events%n", count, today[0], today[1], logical[0], logical[1]);
                }
            }
        }
    }

    private static BsonValue id(String hash) {
        return new BsonDocument("d", new BsonString("d")).append("h", new BsonString(hash));
    }

    private static void fill(MongoCollection<BsonDocument> rows, int count) {
        rows.drop();
        List<BsonDocument> documents = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            documents.add(new BsonDocument("_id", id("k" + i)).append("value", new BsonString("v".repeat(100)))
                    .append("state", new BsonString("VALUE"))
                    .append("version", new org.bson.BsonInt64(i))
                    .append("writeTime", new org.bson.BsonDateTime(System.currentTimeMillis())));
            if (documents.size() == 1_000) {
                rows.insertMany(documents);
                documents.clear();
            }
        }
        if (!documents.isEmpty()) {
            rows.insertMany(documents);
        }
        rows.insertOne(new BsonDocument("_id", CONTROL).append("t", new org.bson.BsonInt64(0)));
    }

    private static List<BsonValue> ids(int index, int keys) {
        List<BsonValue> ids = new ArrayList<>();
        for (int i = 0; i < keys; i++) {
            ids.add(id("k" + (1 + (index * 7919L + i) % KEYS)));
        }
        return ids;
    }

    private static BiConsumer<ClientSession, Integer> read(MongoCollection<BsonDocument> rows, Document projection,
                                                           int keys) {
        return (session, index) -> {
            List<BsonValue> ids = ids(index, keys);
            ids.add(CONTROL);
            rows.find(session, Filters.and(Filters.in("_id", ids), Filters.or(Filters.eq("_id", CONTROL),
                    Filters.ne("state", "VALUE"), YOUNGER))).projection(projection).into(new ArrayList<>());
        };
    }

    private static BiConsumer<ClientSession, Integer> versions(MongoCollection<BsonDocument> rows, boolean logical) {
        return (session, index) -> {
            List<BsonValue> ids = ids(index, 100);
            Bson filter = Filters.and(Filters.in("_id", ids), Filters.eq("state", "VALUE"), YOUNGER);
            if (logical) {
                ids.add(CONTROL);
                filter = Filters.and(Filters.in("_id", ids), Filters.or(Filters.eq("_id", CONTROL),
                        Filters.and(Filters.eq("state", "VALUE"), YOUNGER)));
            }
            rows.find(session, filter).batchSize(ids.size() + 1)
                    .projection(new Document("version", 1).append("t", 1)).into(new ArrayList<>());
        };
    }

    // no $or: the control document is one more id, the state decided in the projection
    private static BiConsumer<ClientSession, Integer> versionsProjected(MongoCollection<BsonDocument> rows) {
        Document projection = new Document("t", 1).append("version", Document.parse(
                "{$cond: [{$and: [{$eq: ['$state', 'VALUE']}, {$gt: ['$writeTime', {$subtract: ['$$NOW', 3600000]}]}]},"
                        + " '$version', '$$REMOVE']}"));
        return (session, index) -> {
            List<BsonValue> ids = ids(index, 100);
            ids.add(CONTROL);
            rows.find(session, Filters.in("_id", ids)).batchSize(ids.size() + 1)
                    .projection(projection).into(new ArrayList<>());
        };
    }

    private static BiConsumer<ClientSession, Integer> put(MongoCollection<BsonDocument> rows,
                                                          List<BsonDocument> pipeline) {
        return (session, index) -> rows.findOneAndUpdate(session, Filters.eq("_id", id("k" + (1 + index % KEYS))),
                pipeline, UPSERT_AFTER);
    }

    private static void single(MongoClient client, String label, BiConsumer<ClientSession, Integer> operation) {
        try (ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true)
                .build())) {
            for (int i = 0; i < OPERATIONS / 4; i++) {
                operation.accept(session, i);
            }
            long[] nanos = new long[OPERATIONS];
            for (int i = 0; i < OPERATIONS; i++) {
                long start = System.nanoTime();
                operation.accept(session, i);
                nanos[i] = System.nanoTime() - start;
            }
            Arrays.sort(nanos);
            System.out.printf("  %s  p50 %4d µs  p99 %5d µs%n", label, nanos[OPERATIONS / 2] / 1_000,
                    nanos[OPERATIONS * 99 / 100] / 1_000);
        }
    }

    private static void parallel(MongoClient client, String label, BiConsumer<ClientSession, Integer> operation)
            throws Exception {
        int callers = WRITERS;
        // not try-with-resources: ExecutorService is AutoCloseable only from Java 19, the baseline is 17
        @SuppressWarnings("resource")
        ExecutorService executor = Executors.newFixedThreadPool(callers);
        try {
            long start = System.nanoTime();
            List<Future<?>> futures = new ArrayList<>();
            for (int caller = 0; caller < callers; caller++) {
                int offset = caller * (KEYS / callers);
                futures.add(executor.submit(() -> {
                    try (ClientSession session = client.startSession(ClientSessionOptions.builder()
                            .causallyConsistent(true).build())) {
                        for (int i = 0; i < OPERATIONS; i++) {
                            operation.accept(session, offset + i % (KEYS / callers));
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
            long elapsed = System.nanoTime() - start;
            System.out.printf("  %s  %,7d puts/s%n", label, (long) callers * OPERATIONS * 1_000_000_000L / elapsed);
        } finally {
            executor.shutdown();
        }
    }

    // the time of the call, and the events a change stream open on the collection receives for it within 10 s
    private static long[] invalidateAll(MongoClient client, MongoCollection<BsonDocument> rows, boolean sweep)
            throws InterruptedException {
        AtomicLong events = new AtomicLong();
        Thread watcher;
        try (MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor = rows.watch()
                .maxAwaitTime(200, TimeUnit.MILLISECONDS).cursor()) {
            watcher = new Thread(() -> {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (System.nanoTime() < deadline) {
                    if (cursor.tryNext() != null) {
                        events.incrementAndGet();
                    }
                }
            });
            watcher.start();
            long start = System.nanoTime();
            try (ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true)
                    .build())) {
                rows.findOneAndUpdate(session, Filters.eq("_id", CONTROL), List.of(BsonDocument.parse(
                        "{$set: {t: '$$CLUSTER_TIME'}}")), UPSERT_AFTER);
                if (sweep) {
                    rows.updateMany(session, Filters.exists("_id.h"), List.of(BsonDocument.parse("""
                            {$set: {state: {$cond: [{$eq: ['$state', 'VALUE']}, 'TOMBSTONE', '$state']},
                                value: {$cond: [{$eq: ['$state', 'VALUE']}, '$$REMOVE', '$value']},
                                version: {$add: ['$version', {$toLong: 1}]}}}""")));
                }
            }
            long micros = (System.nanoTime() - start) / 1_000;
            watcher.join();
            return new long[]{micros, events.get()};
        }
    }
}
