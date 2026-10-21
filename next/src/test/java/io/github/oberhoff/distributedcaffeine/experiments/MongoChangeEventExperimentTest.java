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
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;

/**
 * Measurement 1 of VISION.md ("Distribution: messages carry no values"): what a pipeline upsert produces in change
 * events. Questions: are writes reported as {@code update} events with {@code updatedFields}; does
 * {@code updatedFields} always contain the version; does it contain the state exactly when the state changes; does a
 * server-side projection remove the value from events. A classic {@code $set} update is observed for comparison.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=MongoChangeEventExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
@SuppressWarnings({"java:S2699", "java:S2925"})
class MongoChangeEventExperimentTest {

    private static final String HASH = "h1";

    @ParameterizedTest
    @ValueSource(strings = {"mongo:4", "mongo:5", "mongo:6", "mongo:7", "mongo:8", "mongo:9"})
    void observe(String image) {
        try (MongoDBContainer container = new MongoDBContainer(image).withReplicaSet()) {
            container.start();
            try (MongoClient client = MongoClients.create(container.getReplicaSetUrl("experiment"))) {
                MongoCollection<Document> rows = client.getDatabase("experiment").getCollection("rows");
                rows.drop();
                rows.insertOne(new Document("_id", "warm-up"));

                List<Bson> raw = List.of(Document.parse("{$match: {operationType: {$in: ['insert', 'update', 'replace', 'delete']}}}"));
                List<Bson> projected = List.of(
                        Document.parse("{$match: {operationType: {$in: ['insert', 'update', 'replace']}}}"),
                        Document.parse("{$project: {'fullDocument.value': 0, 'updateDescription.updatedFields.value': 0}}"));

                try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> rawCursor =
                             rows.watch(raw).maxAwaitTime(200, TimeUnit.MILLISECONDS).cursor();
                     MongoChangeStreamCursor<ChangeStreamDocument<Document>> projectedCursor =
                             rows.watch(projected).maxAwaitTime(200, TimeUnit.MILLISECONDS).cursor()) {

                    List<String> steps = new ArrayList<>();
                    step(steps, "insert (pipeline put v1)", () -> putPipeline(rows, "v1"));
                    step(steps, "pipeline put, same value v1", () -> putPipeline(rows, "v1"));
                    step(steps, "pipeline put, new value v2", () -> putPipeline(rows, "v2"));
                    step(steps, "tombstone after value", () -> markPipeline(rows, "TOMBSTONE"));
                    step(steps, "tombstone after tombstone", () -> markPipeline(rows, "TOMBSTONE"));
                    step(steps, "pipeline put after tombstone v3", () -> putPipeline(rows, "v3"));
                    step(steps, "expired after value", () -> markPipeline(rows, "EXPIRED"));
                    step(steps, "pipeline put after expired v4", () -> putPipeline(rows, "v4"));
                    step(steps, "classic $set, new value v5", () -> putClassic(rows, 100));
                    step(steps, "classic $set, same value v5", () -> putClassic(rows, 101));
                    step(steps, "delete", () -> rows.deleteOne(eq("_id", HASH)));

                    List<ChangeStreamDocument<Document>> rawEvents = drain(rawCursor, steps.size());
                    List<ChangeStreamDocument<Document>> projectedEvents = drain(projectedCursor, steps.size() - 1);

                    StringBuilder out = new StringBuilder("\n=== ").append(image).append(" (")
                            .append(client.getDatabase("admin").runCommand(new Document("buildInfo", 1)).getString("version"))
                            .append(") ===\n");
                    for (int i = 0; i < steps.size(); i++) {
                        out.append(String.format("%-34s raw: %s%n", steps.get(i), i < rawEvents.size() ? describe(rawEvents.get(i)) : "<missing>"));
                        if (i < steps.size() - 1) {
                            out.append(String.format("%-34s prj: %s%n", "", i < projectedEvents.size() ? describe(projectedEvents.get(i)) : "<missing>"));
                        }
                    }
                    System.out.println(out);
                }
            }
        }
    }

    /**
     * Does {@code upsert = true} change the event type of pipeline updates, and does it cost time? Same sequence as
     * {@link #observe(String)}, once with and once without upsert on every write (the first write is always an
     * upsert, it creates the document).
     */
    @ParameterizedTest
    @ValueSource(strings = {"mongo:4", "mongo:5", "mongo:6", "mongo:7", "mongo:8", "mongo:9"})
    void upsertFlag(String image) {
        try (MongoDBContainer container = new MongoDBContainer(image).withReplicaSet()) {
            container.start();
            try (MongoClient client = MongoClients.create(container.getReplicaSetUrl("experiment"))) {
                MongoCollection<Document> rows = client.getDatabase("experiment").getCollection("rows");
                StringBuilder out = new StringBuilder("\n=== upsert flag, ").append(image).append(" ===\n");
                for (boolean upsert : new boolean[]{false, true}) {
                    rows.drop();
                    rows.insertOne(new Document("_id", "warm-up"));
                    List<Bson> raw = List.of(Document.parse("{$match: {operationType: {$in: ['insert', 'update', 'replace']}}}"));
                    try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor =
                                 rows.watch(raw).maxAwaitTime(200, TimeUnit.MILLISECONDS).cursor()) {
                        UpdateOptions options = new UpdateOptions().upsert(upsert);
                        putPipeline(rows, "v1", new UpdateOptions().upsert(true));
                        putPipeline(rows, "v1", options);
                        putPipeline(rows, "v2", options);
                        markPipeline(rows, "TOMBSTONE", options);
                        markPipeline(rows, "TOMBSTONE", options);
                        putPipeline(rows, "v3", options);
                        markPipeline(rows, "EXPIRED", options);
                        putPipeline(rows, "v4", options);
                        List<String> types = new ArrayList<>();
                        for (ChangeStreamDocument<Document> event : drain(cursor, 8)) {
                            types.add(event.getOperationTypeString());
                        }
                        out.append(String.format("upsert=%-5s events: %s%n", upsert, types));
                    }
                }
                // time per write: alternating value/tombstone (field set changes) and value/value (same fields)
                for (boolean upsert : new boolean[]{false, true, false, true}) {
                    UpdateOptions options = new UpdateOptions().upsert(upsert);
                    rows.drop();
                    putPipeline(rows, "v", new UpdateOptions().upsert(true));
                    int writes = 2_000;
                    long start = System.nanoTime();
                    for (int i = 0; i < writes; i++) {
                        if (i % 2 == 0) {
                            markPipeline(rows, "TOMBSTONE", options);
                        } else {
                            putPipeline(rows, "v" + i, options);
                        }
                    }
                    long alternating = (System.nanoTime() - start) / writes / 1_000;
                    start = System.nanoTime();
                    for (int i = 0; i < writes; i++) {
                        putPipeline(rows, "w" + i, options);
                    }
                    long sameFields = (System.nanoTime() - start) / writes / 1_000;
                    out.append(String.format("upsert=%-5s µs/write: alternating value/tombstone %d, value/value %d%n",
                            upsert, alternating, sameFields));
                }
                System.out.println(out);
            }
        }
    }

    /**
     * Do events of documents close to the 16 MB limit break the stream, raw and with the value projected out?
     * Pipeline updates may be reported as {@code replace} events, which carry the whole document.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mongo:4", "mongo:5", "mongo:6", "mongo:7", "mongo:8", "mongo:9"})
    void largeValues(String image) {
        try (MongoDBContainer container = new MongoDBContainer(image).withReplicaSet()) {
            container.start();
            try (MongoClient client = MongoClients.create(container.getReplicaSetUrl("experiment"))) {
                MongoCollection<Document> rows = client.getDatabase("experiment").getCollection("rows");
                rows.drop();
                // the largest value whose document still fits, found by shrinking from the limit
                int size = 16 * 1024 * 1024;
                while (true) {
                    try {
                        rows.insertOne(new Document("_id", "probe").append("key", "the-key")
                                .append("value", "x".repeat(size)).append("state", "VALUE")
                                .append("version", 1L).append("writeTime", java.time.Instant.now()));
                        rows.deleteOne(eq("_id", "probe"));
                        break;
                    } catch (RuntimeException e) {
                        size -= 64;
                    }
                }
                int fitting = size;
                StringBuilder out = new StringBuilder("\n=== large values, ").append(image).append(" ===\n");
                for (int margin : new int[]{0, 1024, 64 * 1024, 1024 * 1024}) {
                    rows.deleteMany(new Document());
                    int valueSize = fitting - margin;
                    List<Bson> raw = List.of(Document.parse("{$match: {operationType: {$in: ['insert', 'update', 'replace']}}}"));
                    List<Bson> projected = List.of(
                            Document.parse("{$match: {operationType: {$in: ['insert', 'update', 'replace']}}}"),
                            Document.parse("{$project: {'fullDocument.value': 0, 'updateDescription.updatedFields.value': 0}}"));
                    try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> rawCursor =
                                 rows.watch(raw).maxAwaitTime(200, TimeUnit.MILLISECONDS).cursor();
                         MongoChangeStreamCursor<ChangeStreamDocument<Document>> projectedCursor =
                                 rows.watch(projected).maxAwaitTime(200, TimeUnit.MILLISECONDS).cursor()) {
                        putPipeline(rows, "a".repeat(valueSize));
                        putPipeline(rows, "b".repeat(valueSize));
                        markPipeline(rows, "TOMBSTONE");
                        out.append(String.format("value %,d bytes (limit margin %,d): raw: %s | prj: %s%n",
                                valueSize, margin, outcome(rawCursor), outcome(projectedCursor)));
                    }
                }
                System.out.println(out);
            }
        }
    }

    private static String outcome(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) {
        try {
            List<ChangeStreamDocument<Document>> events = drain(cursor, 3);
            List<String> types = new ArrayList<>();
            for (ChangeStreamDocument<Document> event : events) {
                types.add(event.getOperationTypeString());
            }
            return types.toString();
        } catch (RuntimeException e) {
            return "FAILED " + e.getClass().getSimpleName() + ": " + e.getMessage().replaceAll("\\s+", " ").substring(0, Math.min(140, e.getMessage().length()));
        }
    }

    private static void step(List<String> steps, String name, Runnable action) {
        action.run();
        steps.add(name);
    }

    private static Document version() {
        return Document.parse("{$max: [{$add: [{$ifNull: ['$version', {$toLong: 0}]}, {$toLong: 1}]}, "
                + "{$multiply: [{$toLong: '$$NOW'}, {$toLong: 1000}]}]}");
    }

    private static void putPipeline(MongoCollection<Document> rows, String value) {
        putPipeline(rows, value, new UpdateOptions().upsert(true));
    }

    private static void putPipeline(MongoCollection<Document> rows, String value, UpdateOptions options) {
        Document set = new Document("key", new Document("$literal", "the-key"))
                .append("value", new Document("$literal", value))
                .append("state", "VALUE")
                .append("version", version())
                .append("writeTime", "$$NOW");
        rows.updateOne(eq("_id", HASH), List.of(new Document("$set", set)), options);
    }

    private static void markPipeline(MongoCollection<Document> rows, String state) {
        markPipeline(rows, state, new UpdateOptions());
    }

    private static void markPipeline(MongoCollection<Document> rows, String state, UpdateOptions options) {
        Document set = new Document("state", state)
                .append("version", version())
                .append("writeTime", "$$NOW");
        rows.updateOne(eq("_id", HASH), List.of(new Document("$set", set), new Document("$unset", "value")), options);
    }

    // a classic $set of the value v5
    private static void putClassic(MongoCollection<Document> rows, long version) {
        rows.updateOne(eq("_id", HASH), new Document("$set", new Document("key", "the-key")
                .append("value", "v5").append("state", "VALUE").append("version", version))
                .append("$currentDate", new Document("writeTime", true)), new UpdateOptions().upsert(true));
    }

    private static List<ChangeStreamDocument<Document>> drain(
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor, int expected) {
        List<ChangeStreamDocument<Document>> events = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 10_000;
        while (events.size() < expected && System.currentTimeMillis() < deadline) {
            ChangeStreamDocument<Document> event = cursor.tryNext();
            if (event != null && (event.getDocumentKey() == null
                    || !"warm-up".equals(event.getDocumentKey().get("_id").asString().getValue()))) {
                events.add(event);
            }
        }
        return events;
    }

    private static String describe(ChangeStreamDocument<Document> event) {
        StringBuilder text = new StringBuilder(String.valueOf(event.getOperationTypeString()));
        if (event.getUpdateDescription() != null) {
            Map<String, Object> updated = event.getUpdateDescription().getUpdatedFields() == null
                    ? Map.of() : new Document(event.getUpdateDescription().getUpdatedFields());
            text.append(" updated=").append(new TreeSet<>(updated.keySet()));
            if (updated.containsKey("state")) {
                text.append(" state=").append(event.getUpdateDescription().getUpdatedFields().getString("state").getValue());
            }
            text.append(" removed=").append(event.getUpdateDescription().getRemovedFields());
        }
        if (event.getFullDocument() != null) {
            text.append(" fullDocument=").append(new TreeSet<>(event.getFullDocument().keySet()));
        }
        return text.toString();
    }
}
