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

import io.github.oberhoff.distributedcaffeine.EndToEndBenchmark;
import io.github.oberhoff.distributedcaffeine.RefillBenchmark;
import io.github.oberhoff.distributedcaffeine.StoreDeadlineBenchmark;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * The end-to-end benchmark (see {@link EndToEndBenchmark}) on MongoDB 9, a single-node replica set in a container.
 * Run with {@code -Dexperiments=true}.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@EnabledIfSystemProperty(named = "experiments", matches = "true")
@Timeout(1_800)
@SuppressWarnings("java:S2699") // an experiment reports what it measures, it asserts nothing
class MongoBenchmarkExperimentTest {

    @Test
    void measures_the_operations_end_to_end() throws Exception {
        try (MongoTestDatabase ignored = MongoTestDatabase.start("mongo:9", MongoTopology.SINGLE_NODE_REPLICA_SET);
             MongoCacheBackend backend = new MongoCacheBackend()) {
            EndToEndBenchmark.run("MongoDB 9 (single-node replica set, local container)", backend);
        }
    }

    @Test
    void measures_a_deadline_for_store_calls() throws Exception {
        try (MongoTestDatabase ignored = MongoTestDatabase.start("mongo:9", MongoTopology.SINGLE_NODE_REPLICA_SET)) {
            StoreDeadlineBenchmark.run("MongoDB 9 (single-node replica set, local container)", MongoCacheBackend::new);
        }
    }

    @Test
    void measures_a_refill_while_serving() throws Exception {
        try (MongoTestDatabase ignored = MongoTestDatabase.start("mongo:9", MongoTopology.SINGLE_NODE_REPLICA_SET);
             MongoCacheBackend backend = new MongoCacheBackend()) {
            RefillBenchmark.run("MongoDB 9 (single-node replica set, local container)", backend, 3, 10_000,
                    100_000);
            RefillBenchmark.run("MongoDB 9 (single-node replica set, local container)", backend, 1_024, 10_000,
                    100_000);
        }
    }
}
