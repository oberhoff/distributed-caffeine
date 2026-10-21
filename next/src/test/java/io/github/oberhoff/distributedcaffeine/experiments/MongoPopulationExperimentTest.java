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

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.github.oberhoff.distributedcaffeine.testing.Awaiting;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.client.model.Filters.eq;

/**
 * Measurement 2 of VISION.md ("Distribution: messages carry no values"): how much later do holders have a new value
 * when they read it after the event (B) instead of taking it from the event (A), and what does that cost the store?
 * One writer, N holders of one hot key, each holder with its own client and change stream (as separate instances).
 * In B the value is projected out of events on the server and a holder reads the rows of every batch of events that
 * has arrived in one query. Single-node replica set.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=MongoPopulationExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
@SuppressWarnings({"java:S2699", "java:S2925"})
class MongoPopulationExperimentTest {

    private static final int WARM_UP = 200;
    private static final int WRITES = 1_000;
    private static final String VALUE = "x".repeat(1_024);

    @Test
    void measure() throws Exception {
        try (MongoDBContainer container = new MongoDBContainer("mongo:8").withReplicaSet()) {
            container.start();
            String url = container.getReplicaSetUrl("experiment");
            try (MongoClient writerClient = MongoClients.create(url)) {
                MongoDatabase admin = writerClient.getDatabase("admin");
                StringBuilder out = new StringBuilder("\n=== population, ")
                        .append(admin.runCommand(new Document("buildInfo", 1)).getString("version")).append(" ===\n")
                        .append(String.format("%-52s %10s %10s %12s %12s %10s%n", "variant", "ack p50",
                                "ack p99", "applied p50", "applied p99", "reads/write"));
                for (boolean journalDefault : new boolean[]{true, false}) {
                    MongoWriteConcernExperimentTest.setJournalDefault(admin, journalDefault);
                    for (int holders : new int[]{1, 4, 16}) {
                        for (boolean readHeld : new boolean[]{false, true, false, true}) {
                            String variant = String.format("%s, %2d holders, journal default %s",
                                    readHeld ? "B read held" : "A value in event", holders, journalDefault);
                            out.append(run(variant, url, writerClient, holders, readHeld));
                        }
                    }
                }
                System.out.println(out);
            }
        }
    }

    private static String run(String variant, String url, MongoClient writerClient, int holderCount, boolean readHeld)
            throws Exception {
        MongoCollection<Document> rows = writerClient.getDatabase("experiment").getCollection("rows");
        rows.deleteMany(new Document());
        int total = WARM_UP + WRITES;
        long[] started = new long[total];
        long[] acknowledged = new long[total];
        long[][] applied = new long[holderCount][total];
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch ready = new CountDownLatch(holderCount);
        List<Thread> holders = new ArrayList<>();
        List<MongoClient> clients = new ArrayList<>();

        List<Bson> pipeline = readHeld
                ? List.of(Document.parse("{$match: {operationType: {$in: ['insert', 'update', 'replace']}}}"),
                Document.parse("{$project: {'fullDocument.value': 0, 'updateDescription.updatedFields.value': 0}}"))
                : List.of(Document.parse("{$match: {operationType: {$in: ['insert', 'update', 'replace']}}}"));

        for (int h = 0; h < holderCount; h++) {
            int holder = h;
            MongoClient client = MongoClients.create(url);
            clients.add(client);
            Thread thread = new Thread(() -> {
                MongoCollection<Document> watched = client.getDatabase("experiment").getCollection("rows");
                try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor =
                             watched.watch(pipeline).maxAwaitTime(20, TimeUnit.MILLISECONDS).cursor()) {
                    ready.countDown();
                    while (!stop.get()) {
                        ChangeStreamDocument<Document> first = cursor.tryNext();
                        if (first == null) {
                            continue;
                        }
                        List<ChangeStreamDocument<Document>> batch = new ArrayList<>(List.of(first));
                        while (cursor.available() > 0) {
                            batch.add(cursor.next());
                        }
                        if (readHeld) {
                            // one query for the batch; the key is held, so every event in it is read
                            Document row = watched.find(Filters.in("_id", "hot")).first();
                            long now = System.nanoTime();
                            // only the writes whose events arrived count, at the moment their read returned; the
                            // row may already hold a later write that is not majority-committed yet (no event)
                            if (row != null) {
                                for (ChangeStreamDocument<Document> event : batch) {
                                    Integer seq = seqOf(event);
                                    if (seq != null && applied[holder][seq] == 0) {
                                        applied[holder][seq] = now;
                                    }
                                }
                            }
                        } else {
                            long now = System.nanoTime();
                            for (ChangeStreamDocument<Document> event : batch) {
                                Integer seq = seqOf(event);
                                if (seq != null && valueOf(event) != null && applied[holder][seq] == 0) {
                                    applied[holder][seq] = now;
                                }
                            }
                        }
                    }
                }
            });
            holders.add(thread);
            thread.start();
        }
        ready.await();
        Thread.sleep(300);

        long queriesBefore = queries(writerClient);
        for (int seq = 0; seq < total; seq++) {
            started[seq] = System.nanoTime();
            rows.updateOne(eq("_id", "hot"), new Document("$set", new Document("seq", seq).append("value", seq + VALUE)),
                    new UpdateOptions().upsert(true));
            acknowledged[seq] = System.nanoTime();
        }
        Awaiting.awaitUpTo(() -> allApplied(applied, total - 1), Duration.ofSeconds(30), Duration.ofMillis(50));
        long queriesAfter = queries(writerClient);
        stop.set(true);
        for (Thread thread : holders) {
            thread.join();
        }
        clients.forEach(MongoClient::close);

        long[] ack = new long[WRITES];
        long[] latency = new long[WRITES * holderCount];
        int missing = 0;
        for (int i = 0; i < WRITES; i++) {
            int seq = WARM_UP + i;
            ack[i] = (acknowledged[seq] - started[seq]) / 1_000;
            for (int h = 0; h < holderCount; h++) {
                if (applied[h][seq] == 0) {
                    missing++;
                    latency[i * holderCount + h] = Long.MAX_VALUE;
                } else {
                    latency[i * holderCount + h] = (applied[h][seq] - started[seq]) / 1_000;
                }
            }
        }
        double readsPerWrite = (double) (queriesAfter - queriesBefore) / total;
        return String.format("%-52s %10d %10d %12d %12d %10.2f%s%n", variant,
                percentile(ack, 50), percentile(ack, 99), percentile(latency, 50), percentile(latency, 99),
                readsPerWrite, missing > 0 ? "  (" + missing + " not applied)" : "");
    }

    private static Integer seqOf(ChangeStreamDocument<Document> event) {
        if (event.getFullDocument() != null) {
            return event.getFullDocument().getInteger("seq");
        }
        if (event.getUpdateDescription() != null && event.getUpdateDescription().getUpdatedFields() != null
                && event.getUpdateDescription().getUpdatedFields().containsKey("seq")) {
            return event.getUpdateDescription().getUpdatedFields().getInt32("seq").getValue();
        }
        return null;
    }

    private static String valueOf(ChangeStreamDocument<Document> event) {
        if (event.getFullDocument() != null) {
            return event.getFullDocument().getString("value");
        }
        if (event.getUpdateDescription() != null && event.getUpdateDescription().getUpdatedFields() != null
                && event.getUpdateDescription().getUpdatedFields().containsKey("value")) {
            return event.getUpdateDescription().getUpdatedFields().getString("value").getValue();
        }
        return null;
    }

    private static boolean allApplied(long[][] applied, int seq) {
        for (long[] holder : applied) {
            if (holder[seq] == 0) {
                return false;
            }
        }
        return true;
    }

    private static long queries(MongoClient client) {
        Document counters = client.getDatabase("admin").runCommand(new Document("serverStatus", 1))
                .get("opcounters", Document.class);
        return ((Number) counters.get("query")).longValue();
    }

    private static long percentile(long[] values, int percentile) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[Math.min(sorted.length - 1, sorted.length * percentile / 100)];
    }
}
