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

import io.github.oberhoff.distributedcaffeine.AdapterConformanceTest;
import io.github.oberhoff.distributedcaffeine.AdapterOutageTest;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

/**
 * The suites where versions differ, on one MariaDB or MySQL image through one driver (the subclass names both): the
 * storage and change scan contracts, the adapters' own tests (schema, refusals, connections, timeouts), the
 * property test, the classification of failures for retries and a restart of the server. The database is started with
 * the class and stopped after its suites.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class JdbcCompatibilityTests {

    private final String image;
    private final String driver;
    private @Nullable JdbcTestDatabase database;

    // driver: the URL scheme (JdbcTestDatabase.MARIADB_CONNECTOR_J, MYSQL_CONNECTOR_J)
    JdbcCompatibilityTests(String image, String driver) {
        this.image = image;
        this.driver = driver;
    }

    @BeforeAll
    void startDatabase() {
        database = JdbcTestDatabase.start(image, driver);
    }

    @AfterAll
    void stopDatabase() {
        if (database != null) {
            database.close();
        }
    }

    @Nested
    class StoreContract extends JdbcStoreContractTest {
    }

    @Nested
    class ChangeScanContract extends JdbcChangeScanContractTest {
    }

    @Nested
    class Conformance extends AdapterConformanceTest {

        @Override
        protected AdapterConformance conformance() {
            return new JdbcConformance();
        }
    }

    @Nested
    class Adapter extends JdbcAdapterTest {
    }

    @Nested
    class Outages extends AdapterOutageTest {

        @Override
        protected Outage outage() {
            return new JdbcOutage();
        }
    }

    @Nested
    class Property extends JdbcPropertyTest {
    }

    @Nested
    class Retry extends JdbcRetryTest {
    }

    // the server back before it ends, for the suites after it
    @Nested
    class Restart extends JdbcRestartTest {
    }
}
