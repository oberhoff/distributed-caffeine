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

import io.github.oberhoff.distributedcaffeine.CacheBackend;
import io.github.oberhoff.distributedcaffeine.EndToEndBenchmark;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * The end-to-end benchmark (see {@link EndToEndBenchmark}) on the in-memory store: no network, so it shows what the
 * core decides (store reads per get under contention, conflicts), not what a store costs. Run with
 * {@code -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
@Timeout(1_800)
class InMemoryBenchmarkExperimentTest {

    @Test
    void measures_the_operations_end_to_end() throws Exception {
        try (CacheBackend backend = CacheBackend.of(new InMemoryDatabase())) {
            EndToEndBenchmark.run("in-memory store", backend);
        }
    }
}
