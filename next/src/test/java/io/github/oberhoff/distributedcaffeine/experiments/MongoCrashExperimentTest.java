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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.github.oberhoff.distributedcaffeine.testing.Awaiting;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Objects.requireNonNull;

/**
 * The crash test of VISION.md ("MongoDB write concern"): with {@code writeConcernMajorityJournalDefault: false},
 * does a hard crash lose acknowledged majority writes, were events for them already delivered, and does the change
 * stream afterwards resume silently (no reconciliation would happen) or report an error (the adapter's
 * {@code onGap()})? Journal default {@code true} is the control.
 * <p>
 * The container runs {@code mongod} in a restart loop, so {@code kill -9} crashes the process while the container,
 * its port and its data stay. Single-node replica set.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=MongoCrashExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
@SuppressWarnings({"java:S2699", "java:S2925"})
class MongoCrashExperimentTest {

    private static final int ROUNDS = 3;
    private static final int KILL_AFTER = 1_000;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("resource") // the builder methods return the container itself, which the try closes
    void crash(boolean journalDefault) throws Exception {
        try (GenericContainer<?> container = new GenericContainer<>("mongo:8")
                .withExposedPorts(27017)
                .withCommand("bash", "-c",
                        "while true; do mongod --replSet rs0 --bind_ip_all --quiet; sleep 0.2; done")
                .waitingFor(Wait.forListeningPort())) {
            container.start();
            String url = "mongodb://" + container.getHost() + ":" + container.getMappedPort(27017)
                    + "/?directConnection=true";
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(url))
                    .applyToClusterSettings(builder -> builder.serverSelectionTimeout(60, TimeUnit.SECONDS))
                    .applyToSocketSettings(builder -> builder.readTimeout(10, TimeUnit.SECONDS))
                    .writeConcern(WriteConcern.MAJORITY)
                    .retryWrites(false)
                    .build();
            try (MongoClient admin = MongoClients.create(settings)) {
                admin.getDatabase("admin").runCommand(new Document("replSetInitiate", new Document("_id", "rs0")
                        .append("members", List.of(new Document("_id", 0).append("host", "localhost:27017")))
                        .append("writeConcernMajorityJournalDefault", journalDefault)));
                awaitPrimary(admin);
            }
            StringBuilder out = new StringBuilder("\n=== crash, writeConcernMajorityJournalDefault=")
                    .append(journalDefault).append(" ===\n");
            for (int round = 1; round <= ROUNDS; round++) {
                out.append(round(container, settings, "rows" + round)).append('\n');
            }
            System.out.println(out);
        }
    }

    private static String round(GenericContainer<?> container, MongoClientSettings settings, String name)
            throws Exception {
        try (MongoClient writerClient = MongoClients.create(settings);
             MongoClient watcherClient = MongoClients.create(settings)) {
            MongoCollection<Document> rows = writerClient.getDatabase("experiment").getCollection(name);
            rows.insertOne(new Document("_id", "warm-up"));

            AtomicInteger maxEventSeq = new AtomicInteger();
            AtomicInteger eventsAfterRestart = new AtomicInteger();
            AtomicBoolean crashed = new AtomicBoolean();
            AtomicBoolean stop = new AtomicBoolean();
            AtomicReference<BsonDocument> lastToken = new AtomicReference<>();
            WatcherLog watcherLog = new WatcherLog();

            Thread watcher = new Thread(() -> {
                MongoCollection<Document> watched = watcherClient.getDatabase("experiment").getCollection(name);
                MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor =
                        watched.watch().maxAwaitTime(50, TimeUnit.MILLISECONDS).cursor();
                while (!stop.get()) {
                    try {
                        ChangeStreamDocument<Document> event = cursor.tryNext();
                        if (cursor.getResumeToken() != null) {
                            lastToken.set(cursor.getResumeToken());
                        }
                        if (event == null) {
                            continue;
                        }
                        Integer seq = null;
                        if (event.getFullDocument() != null) {
                            seq = event.getFullDocument().getInteger("seq");
                        } else if (event.getUpdateDescription() != null
                                && event.getUpdateDescription().getUpdatedFields() != null
                                && event.getUpdateDescription().getUpdatedFields().containsKey("seq")) {
                            seq = event.getUpdateDescription().getUpdatedFields().getInt32("seq").getValue();
                        }
                        if (seq != null) {
                            if (crashed.get()) {
                                eventsAfterRestart.incrementAndGet();
                                if (eventsAfterRestart.get() <= 2) {
                                    watcherLog.add("event after restart: seq " + seq);
                                }
                            }
                            maxEventSeq.accumulateAndGet(seq, Math::max);
                        }
                    } catch (MongoException e) {
                        watcherLog.add("cursor failed: " + describe(e));
                        closeQuietly(cursor);
                        // what the adapter would do: reopen from the last token once the server answers again
                        cursor = reopen(watched, lastToken.get(), watcherLog, stop);
                        if (cursor == null) {
                            return;
                        }
                    }
                }
                cursor.close();
            });
            watcher.start();
            Thread.sleep(500);

            int lastAcknowledged = 0;
            Thread killer = null;
            AtomicBoolean killed = new AtomicBoolean();
            for (int seq = 1; !killed.get(); seq++) {
                if (seq == KILL_AFTER) {
                    killer = new Thread(() -> {
                        try {
                            container.execInContainer("bash", "-c", "kill -9 $(pidof mongod)");
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        } finally {
                            killed.set(true);
                        }
                    });
                    killer.start();
                }
                try {
                    rows.updateOne(eq("_id", "hot"), new Document("$set", new Document("seq", seq)),
                            new UpdateOptions().upsert(true));
                    lastAcknowledged = seq;
                } catch (MongoException e) {
                    break;
                }
            }
            requireNonNull(killer).join();
            crashed.set(true);
            int maxEventBeforeCrash = maxEventSeq.get();

            try (MongoClient reader = MongoClients.create(settings)) {
                awaitPrimary(reader);
                Document hot = reader.getDatabase("experiment").getCollection(name).find(eq("_id", "hot")).first();
                int stored = hot == null ? 0 : hot.getInteger("seq");
                // a write after the restart, to see whether the watcher delivers anything again
                reader.getDatabase("experiment").getCollection(name).updateOne(eq("_id", "hot"),
                        new Document("$set", new Document("seq", 1_000_000)), new UpdateOptions().upsert(true));
                Thread.sleep(15_000);
                stop.set(true);
                watcher.join(30_000);
                return String.format("last acknowledged %d, stored after restart %d (lost acknowledged: %d), "
                                + "max event before crash %d (events for lost writes: %d), events after restart %d%n  %s",
                        lastAcknowledged, stored, lastAcknowledged - stored, maxEventBeforeCrash,
                        Math.max(0, maxEventBeforeCrash - stored), eventsAfterRestart.get(), watcherLog.joined());
            }
        }
    }

    // what the watching thread saw, read by the test thread at the end
    private static final class WatcherLog {

        private final List<String> lines = new ArrayList<>();

        synchronized void add(String line) {
            lines.add(line);
        }

        // skips a failure the previous line reported already (a reopen failing again the same way)
        synchronized void addReopenFailure(String text) {
            if (lines.isEmpty() || !lines.get(lines.size() - 1).endsWith(text)) {
                lines.add("reopen failed: " + text);
            }
        }

        synchronized String joined() {
            return String.join("\n  ", lines);
        }
    }

    @SuppressWarnings("BusyWait") // the pause between attempts is a backoff, not a wait for a condition
    private static MongoChangeStreamCursor<ChangeStreamDocument<Document>> reopen(
            MongoCollection<Document> watched, BsonDocument token, WatcherLog watcherLog, AtomicBoolean stop) {
        long deadline = System.currentTimeMillis() + 60_000;
        while (!stop.get() && System.currentTimeMillis() < deadline) {
            try {
                MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = watched.watch()
                        .resumeAfter(token).maxAwaitTime(50, TimeUnit.MILLISECONDS).cursor();
                cursor.tryNext();
                watcherLog.add("reopened from the last token: OK");
                return cursor;
            } catch (MongoException e) {
                watcherLog.addReopenFailure(describe(e));
                if (e.getCode() == 286 || e.getCode() == 280 || e.getCode() == 260) {
                    return null; // history lost / not resumable: this is where the adapter reports onGap()
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return null;
    }

    private static void closeQuietly(MongoChangeStreamCursor<?> cursor) {
        try {
            cursor.close();
        } catch (MongoException e) {
            // the server is gone already
        }
    }

    private static String describe(MongoException e) {
        String message = String.valueOf(e.getMessage()).replaceAll("\\s+", " ");
        return e.getClass().getSimpleName() + " code " + e.getCode() + ": "
                + message.substring(0, Math.min(160, message.length()));
    }

    private static void awaitPrimary(MongoClient client) {
        if (!Awaiting.awaitUpTo(() -> isWritablePrimary(client), Duration.ofSeconds(60), Duration.ofMillis(200))) {
            throw new IllegalStateException("no primary");
        }
    }

    private static boolean isWritablePrimary(MongoClient client) {
        try {
            Document hello = client.getDatabase("admin").runCommand(new Document("hello", 1));
            return hello.getBoolean("isWritablePrimary", false);
        } catch (MongoException e) {
            return false; // still starting
        }
    }
}
