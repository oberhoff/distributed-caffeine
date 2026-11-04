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
package io.github.oberhoff.distributedcaffeine.adapter.jdbc;

import io.github.oberhoff.distributedcaffeine.EndToEndBenchmark;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * The end-to-end benchmark (see {@link EndToEndBenchmark}) on MariaDB 13 and MySQL 26, a single server each in a
 * container, through each product's own driver; changes polled. Run with {@code -Dexperiments=true}.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@EnabledIfSystemProperty(named = "experiments", matches = "true")
@Timeout(1_800)
@SuppressWarnings("java:S2699") // an experiment reports what it measures, it asserts nothing
class JdbcBenchmarkExperimentTest {

    @Test
    void measures_the_operations_end_to_end_on_mariadb() throws Exception {
        measure("MariaDB 13 (single server, local container)", "mariadb:13", JdbcTestDatabase.MARIADB_CONNECTOR_J);
    }

    @Test
    void measures_the_operations_end_to_end_on_mysql() throws Exception {
        measure("MySQL 26 (single server, local container)", "mysql:26", JdbcTestDatabase.MYSQL_CONNECTOR_J);
    }

    private static void measure(String label, String image, String driver) throws Exception {
        try (JdbcTestDatabase ignored = JdbcTestDatabase.start(image, driver);
             JdbcCacheBackend backend = new JdbcCacheBackend()) {
            EndToEndBenchmark.run(label, backend);
        }
    }
}
