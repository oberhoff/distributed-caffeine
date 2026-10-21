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

import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;
import io.github.oberhoff.distributedcaffeine.testing.Awaiting;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.mongodb.client.model.Filters.eq;

/**
 * Follow-up of measurement 2 in VISION.md: what do values in events cost instances that do not hold the written key?
 * 10,000 keys, 8 instances each holding a fraction of them, 4 writers writing random keys. Variants: A raw (value in
 * the event, events read as raw BSON, only held keys decoded), A decoded (value in the event, every event decoded),
 * B (value projected out on the server, holders read their keys per delivered batch, read concern majority).
 * Single-node replica set.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=MongoNonHolderExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
@SuppressWarnings({"java:S2699", "java:S2925"})
class MongoNonHolderExperimentTest {

    private static final int KEYS = 10_000;
    private static final int INSTANCES = 8;
    private static final int WRITERS = 4;
    private static final int WRITES = 20_000;

    private enum Variant {A_RAW, A_DECODED, B}

    @Test
    void measure() throws Exception {
        try (MongoDBContainer container = new MongoDBContainer("mongo:8").withReplicaSet()) {
            container.start();
            String url = container.getReplicaSetUrl("experiment");
            StringBuilder out = new StringBuilder("\n=== non-holders, 8 instances, 10,000 keys, 4 writers, 20,000 writes ===\n")
                    .append(String.format("%-10s %6s %6s %10s %14s %14s %14s %14s %10s%n", "variant", "value", "held",
                            "writes/s", "CPU ms/inst.", "MB recv/inst.", "held p50 µs", "held p99 µs", "reads/wr"));
            for (int valueSize : new int[]{1_024, 10_240}) {
                for (double heldFraction : new double[]{0.01, 0.10}) {
                    for (Variant variant : new Variant[]{Variant.A_RAW, Variant.A_DECODED, Variant.B,
                            Variant.A_RAW, Variant.A_DECODED, Variant.B}) {
                        out.append(run(url, variant, valueSize, heldFraction));
                    }
                }
            }
            System.out.println(out);
        }
    }

    private static String run(String url, Variant variant, int valueSize, double heldFraction) throws Exception {
        String payload = "x".repeat(valueSize);
        try (MongoClient writerClient = MongoClients.create(url)) {
            MongoCollection<Document> rows = writerClient.getDatabase("experiment").getCollection("rows");
            rows.drop();
            List<Document> initial = new ArrayList<>();
            for (int k = 0; k < KEYS; k++) {
                initial.add(new Document("_id", key(k)).append("seq", -1).append("value", payload));
            }
            rows.insertMany(initial, new InsertManyOptions().ordered(false));

            long[] started = new long[WRITES];
            AtomicBoolean stop = new AtomicBoolean();
            CountDownLatch ready = new CountDownLatch(INSTANCES);
            ThreadMXBean threads = ManagementFactory.getThreadMXBean();
            AtomicLong cpuNanos = new AtomicLong();
            AtomicLong bytes = new AtomicLong();
            AtomicLong reads = new AtomicLong();
            AtomicInteger finished = new AtomicInteger();
            List<long[]> heldLatencies = new ArrayList<>();
            List<Thread> instanceThreads = new ArrayList<>();
            List<MongoClient> clients = new ArrayList<>();

            List<Bson> pipeline = variant == Variant.B
                    ? List.of(Document.parse("{$match: {operationType: {$in: ['insert', 'update', 'replace']}}}"),
                    Document.parse("{$project: {'fullDocument.value': 0, 'updateDescription.updatedFields.value': 0}}"))
                    : List.of(Document.parse("{$match: {operationType: {$in: ['insert', 'update', 'replace']}}}"));

            for (int i = 0; i < INSTANCES; i++) {
                boolean[] held = new boolean[KEYS];
                for (int k = 0; k < KEYS; k++) {
                    held[k] = Math.floorMod((k * 31 + i * 7919) * 2654435761L, 1_000_000L) < heldFraction * 1_000_000;
                }
                long[] latency = new long[WRITES];
                heldLatencies.add(latency); // read after the threads filling it were joined
                MongoClient client = MongoClients.create(url);
                clients.add(client);
                Thread thread = new Thread(() -> {
                    MongoCollection<Document> watched = client.getDatabase("experiment").getCollection("rows")
                            .withReadConcern(ReadConcern.MAJORITY);
                    int seen = 0;
                    // whole events as BSON: raw (decoded lazily, on access) or eagerly decoded
                    try (MongoCursor<? extends BsonDocument> cursor = variant == Variant.A_DECODED
                            ? open(watched, pipeline, BsonDocument.class)
                            : open(watched, pipeline, RawBsonDocument.class)) {
                        ready.countDown();
                        while (!stop.get()) {
                            BsonDocument first = cursor.tryNext();
                            if (first == null) {
                                continue;
                            }
                            List<BsonDocument> batch = new ArrayList<>(List.of(first));
                            while (cursor.available() > 0) {
                                batch.add(cursor.next());
                            }
                            Map<String, List<Integer>> toRead = new HashMap<>();
                            for (BsonDocument event : batch) {
                                seen++;
                                if (event instanceof RawBsonDocument raw) {
                                    bytes.addAndGet(raw.getByteBuffer().remaining());
                                }
                                String id = event.getDocument("documentKey").getString("_id").getValue();
                                int k = Integer.parseInt(id.substring(1));
                                if (!held[k]) {
                                    continue;
                                }
                                int seq = seqOf(event);
                                if (seq < 0) {
                                    continue;
                                }
                                if (variant == Variant.B) {
                                    toRead.computeIfAbsent(id, ignored -> new ArrayList<>()).add(seq);
                                } else if (valueOf(event) != null) {
                                    latency[seq] = (System.nanoTime() - started[seq]) / 1_000;
                                }
                            }
                            if (!toRead.isEmpty()) {
                                reads.incrementAndGet();
                                for (Document row : watched.find(Filters.in("_id", toRead.keySet()))) {
                                    row.getString("value");
                                }
                                long now = System.nanoTime();
                                toRead.values().forEach(seqs -> seqs.forEach(seq ->
                                        latency[seq] = (now - started[seq]) / 1_000));
                            }
                            if (seen >= WRITES) {
                                break;
                            }
                        }
                    } finally {
                        cpuNanos.addAndGet(threads.getCurrentThreadCpuTime());
                        finished.incrementAndGet();
                    }
                });
                instanceThreads.add(thread);
                thread.start();
            }
            ready.await();
            Thread.sleep(300);

            AtomicInteger next = new AtomicInteger();
            long begin = System.nanoTime();
            List<Thread> writers = new ArrayList<>();
            for (int w = 0; w < WRITERS; w++) {
                Thread writer = new Thread(() -> {
                    for (int seq = next.getAndIncrement(); seq < WRITES; seq = next.getAndIncrement()) {
                        int k = ThreadLocalRandom.current().nextInt(KEYS);
                        started[seq] = System.nanoTime();
                        rows.updateOne(eq("_id", key(k)), new Document("$set",
                                new Document("seq", seq).append("value", seq + payload)), new UpdateOptions().upsert(true));
                    }
                });
                writers.add(writer);
                writer.start();
            }
            for (Thread writer : writers) {
                writer.join();
            }
            double seconds = (System.nanoTime() - begin) / 1e9;
            Awaiting.awaitUpTo(() -> finished.get() >= INSTANCES, Duration.ofMinutes(2), Duration.ofMillis(50));
            stop.set(true);
            for (Thread thread : instanceThreads) {
                thread.join();
            }
            clients.forEach(MongoClient::close);

            List<Long> all = new ArrayList<>();
            for (long[] latency : heldLatencies) {
                for (long value : latency) {
                    if (value > 0) {
                        all.add(value);
                    }
                }
            }
            long[] sorted = all.stream().mapToLong(Long::longValue).toArray();
            Arrays.sort(sorted);
            return String.format("%-10s %5dK %5.0f%% %10.0f %14.0f %14s %14d %14d %10.2f%n", variant,
                    valueSize / 1_024, heldFraction * 100, WRITES / seconds,
                    cpuNanos.get() / 1e6 / INSTANCES,
                    variant == Variant.A_DECODED ? "n/a" : String.format("%.1f", bytes.get() / 1e6 / INSTANCES),
                    sorted.length == 0 ? -1 : sorted[sorted.length / 2],
                    sorted.length == 0 ? -1 : sorted[Math.min(sorted.length - 1, sorted.length * 99 / 100)],
                    (double) reads.get() / WRITES);
        }
    }

    private static <T extends BsonDocument> MongoCursor<T> open(MongoCollection<Document> watched, List<Bson> pipeline,
                                                                Class<T> eventClass) {
        return watched.watch(pipeline).maxAwaitTime(20, TimeUnit.MILLISECONDS).withDocumentClass(eventClass).cursor();
    }

    private static String key(int k) {
        return String.format("k%05d", k);
    }

    private static int seqOf(BsonDocument event) {
        if (event.containsKey("fullDocument") && event.get("fullDocument").isDocument()) {
            return event.getDocument("fullDocument").getInt32("seq").getValue();
        }
        BsonDocument updated = event.getDocument("updateDescription").getDocument("updatedFields");
        return updated.containsKey("seq") ? updated.getInt32("seq").getValue() : -1;
    }

    private static String valueOf(BsonDocument event) {
        if (event.containsKey("fullDocument") && event.get("fullDocument").isDocument()) {
            BsonDocument document = event.getDocument("fullDocument");
            return document.containsKey("value") ? document.getString("value").getValue() : null;
        }
        BsonDocument updated = event.getDocument("updateDescription").getDocument("updatedFields");
        return updated.containsKey("value") ? updated.getString("value").getValue() : null;
    }
}
