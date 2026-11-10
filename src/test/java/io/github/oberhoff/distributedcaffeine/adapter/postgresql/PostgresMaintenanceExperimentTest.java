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
package io.github.oberhoff.distributedcaffeine.adapter.postgresql;

import io.github.oberhoff.distributedcaffeine.experiments.MaintenanceExperiment;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.testing.TestStoreSettings;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * What one maintenance call costs on a large cache (see {@link MaintenanceExperiment}) on PostgreSQL 18, a single
 * server in a container. Run with {@code -Dexperiments=true}.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@EnabledIfSystemProperty(named = "experiments", matches = "true")
@Timeout(3_600)
// the SQL runs against a test container, unknown to the IDE; an experiment reports what it measures, it asserts nothing
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2699"})
class PostgresMaintenanceExperimentTest {

    @Test
    void measures_one_maintenance_call_on_a_large_cache() throws Exception {
        try (PostgresTestDatabase database =
                PostgresTestDatabase.start("postgres:18", PostgresTopology.SINGLE_SERVER)) {
            PostgresAdapter<String, String> adapter = PostgresAdapter.newBuilder(database.dataSource, "maintenance",
                    "cache").withListenerDataSource(database.listener).build();
            adapter.createStore(TestStoreSettings.of(Format.TEXT, Format.TEXT)).close(); // creates the table
            MaintenanceExperiment.run("PostgreSQL 18 (single server, local container)",
                    new MaintenanceExperiment.Backend() {
                        @Override
                        public Store open() {
                            return adapter.createStore(TestStoreSettings.of(Format.TEXT, Format.TEXT));
                        }

                        @Override
                        public void fill(int rows) {
                            database.execute("DELETE FROM \"maintenance\".\"cache\"");
                            database.execute("INSERT INTO \"maintenance\".\"cache\" (discriminator, id, key, value, "
                                    + "state, version, write_time) SELECT '" + PostgresAdapter.DEFAULT_DISCRIMINATOR
                                    + "', 'h' || g, 'k' || g, repeat('x', 100), 'VALUE', g, "
                                    + "now() - interval '2 hours' + g * interval '1 microsecond' "
                                    + "FROM generate_series(0, "
                                    + (rows - 1) + ") AS g"); // each its own write time, as in a cache
                            database.execute("VACUUM ANALYZE \"maintenance\".\"cache\"");
                        }
                    }, new int[]{Integer.MAX_VALUE, 10_000, 100_000}, 1_000_000);
        }
    }
}
