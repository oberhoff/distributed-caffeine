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
package io.github.oberhoff.distributedcaffeine.adapter.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import io.github.oberhoff.distributedcaffeine.experiments.MaintenanceExperiment;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * What one maintenance call costs on a large cache (see {@link MaintenanceExperiment}) on MongoDB 9, a single-node
 * replica set in a container. Run with {@code -Dexperiments=true}.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@EnabledIfSystemProperty(named = "experiments", matches = "true")
@Timeout(3_600)
@SuppressWarnings("java:S2699") // an experiment reports what it measures, it asserts nothing
class MongoMaintenanceExperimentTest {

    @Test
    void measures_one_maintenance_call_on_a_large_cache() throws Exception {
        try (MongoTestDatabase database = MongoTestDatabase.start("mongo:9", MongoTopology.SINGLE_NODE_REPLICA_SET)) {
            MongoAdapter<String, String> adapter = MongoAdapter.newBuilder(database.client, "maintenance", "cache")
                    .build();
            adapter.createStore(StoreSettings.of(Format.TEXT, Format.TEXT)).close(); // creates the collection
            MongoCollection<Document> collection = database.client.getDatabase("maintenance").getCollection("cache");
            MaintenanceExperiment.run("MongoDB 9 (single-node replica set, local container)",
                    new MaintenanceExperiment.Backend() {
                        @Override
                        public Store open() {
                            return adapter.createStore(StoreSettings.of(Format.TEXT, Format.TEXT));
                        }

                        @Override
                        public void fill(int rows) {
                            collection.deleteMany(Filters.exists("_id.h"));
                            // each its own write time, as in a cache (milliseconds apart: 17 minutes for a million)
                            Instant written = Instant.now().minus(2, ChronoUnit.HOURS);
                            List<Document> batch = new ArrayList<>();
                            for (int i = 0; i < rows; i++) {
                                batch.add(new Document("_id", new Document("d", MongoAdapter.DEFAULT_DISCRIMINATOR)
                                        .append("h", "h" + i)).append("key", "k" + i).append("value", "x".repeat(100))
                                        .append("state", "VALUE").append("version", (long) i + 1)
                                        .append("writeTime", written.plusMillis(i)));
                                if (batch.size() == 10_000) {
                                    collection.insertMany(batch);
                                    batch.clear();
                                }
                            }
                            if (!batch.isEmpty()) {
                                collection.insertMany(batch);
                            }
                        }
                    }, new int[]{Integer.MAX_VALUE, 10_000, 100_000}, 1_000_000);
        }
    }
}
