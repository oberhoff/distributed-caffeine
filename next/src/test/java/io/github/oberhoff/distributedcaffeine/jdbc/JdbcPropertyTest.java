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
package io.github.oberhoff.distributedcaffeine.jdbc;

import io.github.oberhoff.distributedcaffeine.InternalPropertyTest;
import io.github.oberhoff.distributedcaffeine.PropertyBackend;
import io.github.oberhoff.distributedcaffeine.spi.PrimitiveStore;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.testing.ScanOnlyStore;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The property test (see {@link InternalPropertyTest}) on the plain JDBC adapter over MariaDB: the generic store built
 * from its primitives, real SQL transactions and a real clock, distribution only by the core's polls. No faults of
 * its own (none are injected into the database); the races of the instances among themselves and with the polls
 * remain.
 */
@NullMarked
@Timeout(600)
class JdbcPropertyTest {

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4})
    void the_store_stays_fresh_and_every_instance_converges_on_it(long seed) throws Exception {
        assertThat(new InternalPropertyTest().run(seed, new JdbcBackend(), 25)).as("phases checked").isEqualTo(25);
    }

    // idle once every store began two scans after the last heal, the second one after everything written before it
    private static final class JdbcBackend implements PropertyBackend {

        private final String table = JdbcTestDatabase.freshTable();
        private final List<ScanOnlyStore> scanning = new CopyOnWriteArrayList<>();
        private final Map<ScanOnlyStore, Long> scansAtHeal = new ConcurrentHashMap<>();

        @Override
        public Store open(StoreSettings settings) {
            PrimitiveStore store = (PrimitiveStore) JdbcAdapter.newBuilder(JdbcTestDatabase.DATA_SOURCE, table).build()
                    .createStore(settings);
            ScanOnlyStore scanned = new ScanOnlyStore(store);
            scanning.add(scanned);
            return scanned;
        }

        @Override
        public void fault(Random random, Consumer<String> trace) {
            // none
        }

        @Override
        public void heal() {
            scanning.forEach(store -> scansAtHeal.put(store, store.scans()));
        }

        @Override
        public boolean isIdle() {
            return scanning.stream().allMatch(store -> store.scans() >= scansAtHeal.getOrDefault(store, 0L) + 2);
        }
    }
}
