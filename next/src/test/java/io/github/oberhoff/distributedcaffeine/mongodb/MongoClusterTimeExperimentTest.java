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
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Whether {@code $$CLUSTER_TIME} can serve as the version counter (VISION.md, "invalidateAll: logical"): usable in an
 * update pipeline and in a find projection, and strictly ordered: a read before an {@code invalidateAll} below its
 * time, a write after it above, also across a failover.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=MongoClusterTimeExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it
@NullMarked
@SuppressWarnings("java:S2699")
class MongoClusterTimeExperimentTest {

    private static final int ROUNDS = 2_000;
    private static final List<BsonDocument> SET_CLUSTER_TIME = List.of(
            BsonDocument.parse("{$set: {v: '$$CLUSTER_TIME'}}"));

    @ParameterizedTest
    @ValueSource(strings = {"mongo:4.4.0", "mongo:9"})
    void ordering_on_one_primary(String image) {
        try (MongoDBContainer container = new MongoDBContainer(image).withReplicaSet()) {
            container.start();
            try (MongoClient client = MongoClients.create(container.getReplicaSetUrl())) {
                rounds(image, client, null);
            }
        }
    }

    @Test
    void ordering_across_a_failover() {
        try (MongoReplicaSet replicaSet = new MongoReplicaSet("mongo:9", false);
             MongoClient client = MongoClients.create(replicaSet.settings().build())) {
            rounds("mongo:9 with a step-down every 200 rounds", client, replicaSet);
        }
    }

    // per round: read (the read's time), invalidateAll (T), write (the write's version), as the store would
    private static void rounds(String label, MongoClient client, @Nullable MongoReplicaSet replicaSet) {
        MongoCollection<Document> rows = client.getDatabase("ct").withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true))
                .withReadPreference(ReadPreference.primary()).getCollection("rows");
        rows.insertOne(new Document("_id", "control"));
        FindOneAndUpdateOptions after = new FindOneAndUpdateOptions().upsert(true)
                .returnDocument(ReturnDocument.AFTER);
        int readNotBelow = 0;
        int readEqual = 0;
        int writeNotAbove = 0;
        int readNotAboveWrite = 0;
        BsonTimestamp previousWrite = null;
        int failovers = 0;
        try (ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true)
                .build())) {
            for (int round = 0; round < ROUNDS; round++) {
                if (replicaSet != null && round > 0 && round % 200 == 0) {
                    replicaSet.stepDown();
                    replicaSet.awaitFailover();
                    failovers++;
                }
                BsonTimestamp read = retrying(() -> requireNonNull(rows.find(session, Filters.eq("_id", "control"))
                        .projection(new Document("now", "$$CLUSTER_TIME")).first()).get("now", BsonTimestamp.class));
                BsonTimestamp t = retrying(() -> requireNonNull(rows.findOneAndUpdate(session,
                        Filters.eq("_id", "control"), SET_CLUSTER_TIME, after)).get("v", BsonTimestamp.class));
                String key = "k" + round;
                BsonTimestamp written = retrying(() -> requireNonNull(rows.findOneAndUpdate(session,
                        Filters.eq("_id", key), SET_CLUSTER_TIME, after)).get("v", BsonTimestamp.class));
                if (previousWrite != null && read.compareTo(previousWrite) <= 0) {
                    readNotAboveWrite++;
                }
                if (read.compareTo(t) >= 0) {
                    readNotBelow++;
                }
                if (read.compareTo(t) == 0) {
                    readEqual++;
                }
                if (written.compareTo(t) <= 0) {
                    writeNotAbove++;
                }
                previousWrite = written;
            }
        }
        System.out.printf("%s: %d rounds, %d failovers; read >= T: %d (equal: %d); write <= T: %d; read <= the "
                + "write before: %d%n", label, ROUNDS, failovers, readNotBelow, readEqual, writeNotAbove,
                readNotAboveWrite);
    }

    @SuppressWarnings("BusyWait") // the pause is the backoff while a new primary is elected, not a wait for a condition
    private static <T> T retrying(java.util.function.Supplier<T> call) {
        for (int attempt = 1; ; attempt++) {
            try {
                return call.get();
            } catch (com.mongodb.MongoException e) {
                if (attempt == 20) {
                    throw e;
                }
                try {
                    Thread.sleep(250);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }
}
