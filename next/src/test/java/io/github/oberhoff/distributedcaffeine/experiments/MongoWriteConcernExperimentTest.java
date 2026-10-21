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

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The journal experiment of VISION.md ("MongoDB write concern"): does the server-side
 * {@code writeConcernMajorityJournalDefault: false} give majority writes the speed of w:1, and do change streams then
 * deliver earlier? Per variant, sequential writes to one hot key; per write the time until the writer's
 * acknowledgement and until a separate watcher receives the event. Single-node replica set.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=MongoWriteConcernExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
@SuppressWarnings({"java:S2699", "java:S2925"})
class MongoWriteConcernExperimentTest {

    private static final int WARM_UP = 200;
    private static final int WRITES = 1_000;

    @ParameterizedTest
    @ValueSource(strings = {"mongo:5", "mongo:6", "mongo:7", "mongo:8", "mongo:9"})
    void measure(String image) throws Exception {
        try (MongoDBContainer container = new MongoDBContainer(image).withReplicaSet()) {
            container.start();
            String url = container.getReplicaSetUrl("experiment");
            try (MongoClient writerClient = MongoClients.create(url);
                 MongoClient watcherClient = MongoClients.create(url)) {
                MongoDatabase admin = writerClient.getDatabase("admin");
                StringBuilder out = new StringBuilder("\n=== write concern, ").append(image).append(" (")
                        .append(admin.runCommand(new Document("buildInfo", 1)).getString("version")).append(") ===\n")
                        .append(String.format("%-40s %12s %12s %12s %12s %12s%n", "variant",
                                "ack p50 µs", "ack p99 µs", "event p50 µs", "event p99 µs", "ev-ack p50"));
                for (boolean journalDefault : new boolean[]{true, false, true, false}) {
                    setJournalDefault(admin, journalDefault);
                    for (WriteConcern writeConcern : List.of(WriteConcern.MAJORITY, WriteConcern.W1)) {
                        String variant = (writeConcern.equals(WriteConcern.MAJORITY) ? "majority" : "w:1")
                                + ", writeConcernMajorityJournalDefault=" + journalDefault;
                        out.append(run(variant, writerClient, watcherClient, writeConcern));
                    }
                }
                Document log = admin.runCommand(new Document("getLog", "global"));
                out.append("log lines mentioning the setting: ");
                long count = log.getList("log", String.class).stream()
                        .filter(line -> line.contains("writeConcernMajorityJournalDefault")).count();
                out.append(count).append('\n');
                log.getList("log", String.class).stream()
                        .filter(line -> line.contains("writeConcernMajorityJournalDefault"))
                        .limit(3)
                        .forEach(line -> out.append("  ").append(line, 0, Math.min(400, line.length())).append('\n'));
                System.out.println(out);
            }
        }
    }

    static void setJournalDefault(MongoDatabase admin, boolean value) {
        Document config = admin.runCommand(new Document("replSetGetConfig", 1)).get("config", Document.class);
        if (config.getBoolean("writeConcernMajorityJournalDefault", true) == value) {
            return;
        }
        config.put("writeConcernMajorityJournalDefault", value);
        config.put("version", config.getInteger("version") + 1);
        admin.runCommand(new Document("replSetReconfig", config));
    }

    static String run(String variant, MongoClient writerClient, MongoClient watcherClient,
                              WriteConcern writeConcern) throws Exception {
        MongoCollection<Document> rows = writerClient.getDatabase("experiment").getCollection("rows")
                .withWriteConcern(writeConcern);
        rows.withWriteConcern(WriteConcern.MAJORITY).deleteMany(new Document());
        int total = WARM_UP + WRITES;
        long[] started = new long[total];
        long[] acknowledged = new long[total];
        long[] received = new long[total];
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);

        Thread watcher = new Thread(() -> {
            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = watcherClient
                    .getDatabase("experiment").getCollection("rows").watch()
                    .maxAwaitTime(20, TimeUnit.MILLISECONDS).cursor()) {
                ready.countDown();
                int seen = 0;
                while (!stop.get() && seen < total) {
                    ChangeStreamDocument<Document> event = cursor.tryNext();
                    if (event == null) {
                        continue;
                    }
                    long now = System.nanoTime();
                    Integer seq = null;
                    if (event.getFullDocument() != null) {
                        seq = event.getFullDocument().getInteger("seq");
                    } else if (event.getUpdateDescription() != null
                            && event.getUpdateDescription().getUpdatedFields() != null
                            && event.getUpdateDescription().getUpdatedFields().containsKey("seq")) {
                        seq = event.getUpdateDescription().getUpdatedFields().getInt32("seq").getValue();
                    }
                    if (seq != null) {
                        received[seq] = now;
                        seen++;
                    }
                }
            } finally {
                done.countDown();
            }
        });
        watcher.start();
        ready.await();

        for (int seq = 0; seq < total; seq++) {
            started[seq] = System.nanoTime();
            rows.updateOne(eq("_id", "hot"), new Document("$set", new Document("seq", seq)),
                    new UpdateOptions().upsert(true));
            acknowledged[seq] = System.nanoTime();
        }
        assertThat(done.await(30, TimeUnit.SECONDS)).as("all events seen").isTrue();
        stop.set(true);
        watcher.join();

        long[] ack = new long[WRITES];
        long[] event = new long[WRITES];
        long[] eventAfterAck = new long[WRITES];
        int missing = 0;
        for (int i = 0; i < WRITES; i++) {
            int seq = WARM_UP + i;
            ack[i] = (acknowledged[seq] - started[seq]) / 1_000;
            if (received[seq] == 0) {
                missing++;
                event[i] = Long.MAX_VALUE;
                eventAfterAck[i] = Long.MAX_VALUE;
            } else {
                event[i] = (received[seq] - started[seq]) / 1_000;
                eventAfterAck[i] = (received[seq] - acknowledged[seq]) / 1_000;
            }
        }
        return String.format("%-40s %12d %12d %12d %12d %12d%s%n", variant,
                percentile(ack, 50), percentile(ack, 99), percentile(event, 50), percentile(event, 99),
                percentile(eventAfterAck, 50), missing > 0 ? "  (" + missing + " events missing)" : "");
    }

    private static long percentile(long[] values, int percentile) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[Math.min(sorted.length - 1, sorted.length * percentile / 100)];
    }
}
