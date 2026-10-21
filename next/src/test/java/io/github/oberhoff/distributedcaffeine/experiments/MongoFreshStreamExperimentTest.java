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
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Does a change stream opened without a resume token deliver every write acknowledged after its opening returned?
 * Opens a database change stream (majority read concern, as the adapter does), writes one document at once
 * (majority), and waits up to 2 s for its event; 200 rounds per server version.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=MongoFreshStreamExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
@SuppressWarnings({"java:S2699", "java:S2925"})
class MongoFreshStreamExperimentTest {

    @ParameterizedTest
    @ValueSource(strings = {"mongo:4.4", "mongo:8"})
    void run(String image) {
        try (MongoDBContainer container = new MongoDBContainer(image).withReplicaSet()) {
            container.start();
            try (MongoClient client = MongoClients.create(container.getReplicaSetUrl())) {
                MongoDatabase database = client.getDatabase("fresh").withReadConcern(ReadConcern.MAJORITY)
                        .withWriteConcern(WriteConcern.MAJORITY);
                MongoCollection<Document> rows = database.getCollection("rows");
                rows.insertOne(new Document("_id", "warm-up"));
                int lost = 0;
                long slowest = 0;
                for (int round = 0; round < 200; round++) {
                    try (MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor = database
                            .watch(List.of(BsonDocument.parse("{$match: {'ns.coll': 'rows'}}")), BsonDocument.class)
                            .maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor()) {
                        rows.updateOne(new Document("_id", "k"), new Document("$set", new Document("round", round)),
                                new com.mongodb.client.model.UpdateOptions().upsert(true));
                        long start = System.nanoTime();
                        boolean arrived = false;
                        while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2)) {
                            ChangeStreamDocument<BsonDocument> event = cursor.tryNext();
                            if (event != null) {
                                arrived = true;
                                break;
                            }
                        }
                        slowest = Math.max(slowest, System.nanoTime() - start);
                        if (!arrived) {
                            lost++;
                        }
                    }
                }
                System.out.printf("%s: %d of 200 writes after a fresh open lost, slowest %d ms%n", image, lost,
                        TimeUnit.NANOSECONDS.toMillis(slowest));
            }
        }
    }
}
