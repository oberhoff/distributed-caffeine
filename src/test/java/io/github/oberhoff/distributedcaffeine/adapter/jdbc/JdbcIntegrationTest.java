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

import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;

import static io.github.oberhoff.distributedcaffeine.adapter.jdbc.JdbcTestDatabase.MARIADB_CONNECTOR_J;
import static io.github.oberhoff.distributedcaffeine.adapter.jdbc.JdbcTestDatabase.MYSQL_CONNECTOR_J;
import static io.github.oberhoff.distributedcaffeine.testing.Topology.Tests.RUNS_ON_GITHUB;

/**
 * Every MariaDB and MySQL image the suites run on, one at a time (VISION.md, "MariaDB and MySQL", step 3): from the
 * oldest supported (MariaDB 10.2, MySQL 5.7: the first with index keys long enough for the table in utf8mb4) to the
 * latest, each major version's first release and its latest, the servers storing text as utf8mb4. A first release
 * runs the suites where versions differ ({@link JdbcCompatibilityTests}), the latest every suite
 * ({@link JdbcServerTests}); the failover ({@link JdbcReplicationTests}) on the oldest supported and on the latest of
 * each major. MariaDB through MariaDB Connector/J; MySQL through MySQL Connector/J and MariaDB Connector/J alike,
 * the failover through MySQL Connector/J, and through both on the latest. MySQL publishes arm64 images from 8.0.29
 * only: 5.7 and early 8.0 run emulated on Apple Silicon. With {@code -DrunsOnGitHub=true}, only the latest of each
 * product runs, through its own driver. The first releases of MariaDB 10.2 and MySQL 5.7 and 8.0 have x86 images
 * only, and their entrypoint's helper (gosu, built with an old Go runtime) crashes when emulated: on x86 they run
 * (MySQL 5.7.13 for 5.7.9, as earlier images are published in a manifest format Docker no longer pulls), on ARM
 * (Apple Silicon) the first releases that start emulated stand for them: MariaDB 10.2.31, MySQL 5.7.29 and 8.0.19.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class JdbcIntegrationTest {

    @Nested
    @DisplayName("MariaDB 10.2.6")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @DisabledOnOs(architectures = "aarch64")
    class MariaDB_10_2_6 extends JdbcCompatibilityTests {

        MariaDB_10_2_6() {
            super("mariadb:10.2.6", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 10.2.31 (for 10.2.6 on ARM)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @EnabledOnOs(architectures = "aarch64")
    class MariaDB_10_2_31 extends JdbcCompatibilityTests {

        MariaDB_10_2_31() {
            super("mariadb:10.2.31", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 10.2.6 (Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @DisabledOnOs(architectures = "aarch64")
    class MariaDB_10_2_6__Replication extends JdbcReplicationTests {

        MariaDB_10_2_6__Replication() {
            super("mariadb:10.2.6", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 10.2.31 (for 10.2.6 on ARM; Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @EnabledOnOs(architectures = "aarch64")
    class MariaDB_10_2_31__Replication extends JdbcReplicationTests {

        MariaDB_10_2_31__Replication() {
            super("mariadb:10.2.31", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 10 latest")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MariaDB_10_latest extends JdbcServerTests {

        MariaDB_10_latest() {
            super("mariadb:10", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 10 latest (Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MariaDB_10_latest__Replication extends JdbcReplicationTests {

        MariaDB_10_latest__Replication() {
            super("mariadb:10", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 11.0.2")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MariaDB_11_0_2 extends JdbcCompatibilityTests {

        MariaDB_11_0_2() {
            super("mariadb:11.0.2", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 11 latest")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MariaDB_11_latest extends JdbcServerTests {

        MariaDB_11_latest() {
            super("mariadb:11", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 11 latest (Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MariaDB_11_latest__Replication extends JdbcReplicationTests {

        MariaDB_11_latest__Replication() {
            super("mariadb:11", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 12.0.2")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MariaDB_12_0_2 extends JdbcCompatibilityTests {

        MariaDB_12_0_2() {
            super("mariadb:12.0.2", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 12 latest")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MariaDB_12_latest extends JdbcServerTests {

        MariaDB_12_latest() {
            super("mariadb:12", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 12 latest (Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MariaDB_12_latest__Replication extends JdbcReplicationTests {

        MariaDB_12_latest__Replication() {
            super("mariadb:12", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 13.0.2")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MariaDB_13_0_2 extends JdbcCompatibilityTests {

        MariaDB_13_0_2() {
            super("mariadb:13.0.2", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 13 latest")
    class MariaDB_13_latest extends JdbcServerTests {

        MariaDB_13_latest() {
            super("mariadb:13", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MariaDB 13 latest (Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MariaDB_13_latest__Replication extends JdbcReplicationTests {

        MariaDB_13_latest__Replication() {
            super("mariadb:13", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 5.7.13 (MySQL Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @DisabledOnOs(architectures = "aarch64")
    class MySQL_5_7_13 extends JdbcCompatibilityTests {

        MySQL_5_7_13() {
            super("mysql:5.7.13", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 5.7.29 (for 5.7.13 on ARM; MySQL Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @EnabledOnOs(architectures = "aarch64")
    class MySQL_5_7_29 extends JdbcCompatibilityTests {

        MySQL_5_7_29() {
            super("mysql:5.7.29", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 5.7.13 (MariaDB Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @DisabledOnOs(architectures = "aarch64")
    class MySQL_5_7_13__MariaDB_Connector_J extends JdbcCompatibilityTests {

        MySQL_5_7_13__MariaDB_Connector_J() {
            super("mysql:5.7.13", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 5.7.29 (for 5.7.13 on ARM; MariaDB Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @EnabledOnOs(architectures = "aarch64")
    class MySQL_5_7_29__MariaDB_Connector_J extends JdbcCompatibilityTests {

        MySQL_5_7_29__MariaDB_Connector_J() {
            super("mysql:5.7.29", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 5.7.13 (Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @DisabledOnOs(architectures = "aarch64")
    class MySQL_5_7_13__Replication extends JdbcReplicationTests {

        MySQL_5_7_13__Replication() {
            super("mysql:5.7.13", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 5.7.29 (for 5.7.13 on ARM; Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @EnabledOnOs(architectures = "aarch64")
    class MySQL_5_7_29__Replication extends JdbcReplicationTests {

        MySQL_5_7_29__Replication() {
            super("mysql:5.7.29", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 5.7 latest (MySQL Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_5_7_latest extends JdbcServerTests {

        MySQL_5_7_latest() {
            super("mysql:5.7", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 5.7 latest (MariaDB Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_5_7_latest__MariaDB_Connector_J extends JdbcServerTests {

        MySQL_5_7_latest__MariaDB_Connector_J() {
            super("mysql:5.7", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 5.7 latest (Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_5_7_latest__Replication extends JdbcReplicationTests {

        MySQL_5_7_latest__Replication() {
            super("mysql:5.7", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 8.0.11 (MySQL Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @DisabledOnOs(architectures = "aarch64")
    class MySQL_8_0_11 extends JdbcCompatibilityTests {

        MySQL_8_0_11() {
            super("mysql:8.0.11", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 8.0.19 (for 8.0.11 on ARM; MySQL Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @EnabledOnOs(architectures = "aarch64")
    class MySQL_8_0_19 extends JdbcCompatibilityTests {

        MySQL_8_0_19() {
            super("mysql:8.0.19", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 8.0.11 (MariaDB Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @DisabledOnOs(architectures = "aarch64")
    class MySQL_8_0_11__MariaDB_Connector_J extends JdbcCompatibilityTests {

        MySQL_8_0_11__MariaDB_Connector_J() {
            super("mysql:8.0.11", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 8.0.19 (for 8.0.11 on ARM; MariaDB Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    @EnabledOnOs(architectures = "aarch64")
    class MySQL_8_0_19__MariaDB_Connector_J extends JdbcCompatibilityTests {

        MySQL_8_0_19__MariaDB_Connector_J() {
            super("mysql:8.0.19", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 8 latest (MySQL Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_8_latest extends JdbcServerTests {

        MySQL_8_latest() {
            super("mysql:8", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 8 latest (MariaDB Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_8_latest__MariaDB_Connector_J extends JdbcServerTests {

        MySQL_8_latest__MariaDB_Connector_J() {
            super("mysql:8", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 8 latest (Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_8_latest__Replication extends JdbcReplicationTests {

        MySQL_8_latest__Replication() {
            super("mysql:8", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 9.0.0 (MySQL Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_9_0_0 extends JdbcCompatibilityTests {

        MySQL_9_0_0() {
            super("mysql:9.0.0", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 9.0.0 (MariaDB Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_9_0_0__MariaDB_Connector_J extends JdbcCompatibilityTests {

        MySQL_9_0_0__MariaDB_Connector_J() {
            super("mysql:9.0.0", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 9 latest (MySQL Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_9_latest extends JdbcServerTests {

        MySQL_9_latest() {
            super("mysql:9", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 9 latest (MariaDB Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_9_latest__MariaDB_Connector_J extends JdbcServerTests {

        MySQL_9_latest__MariaDB_Connector_J() {
            super("mysql:9", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 9 latest (Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_9_latest__Replication extends JdbcReplicationTests {

        MySQL_9_latest__Replication() {
            super("mysql:9", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 26.7.0 (MySQL Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_26_7_0 extends JdbcCompatibilityTests {

        MySQL_26_7_0() {
            super("mysql:26.7.0", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 26.7.0 (MariaDB Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_26_7_0__MariaDB_Connector_J extends JdbcCompatibilityTests {

        MySQL_26_7_0__MariaDB_Connector_J() {
            super("mysql:26.7.0", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 26 latest (MySQL Connector/J)")
    class MySQL_26_latest extends JdbcServerTests {

        MySQL_26_latest() {
            super("mysql:26", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 26 latest (MariaDB Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_26_latest__MariaDB_Connector_J extends JdbcServerTests {

        MySQL_26_latest__MariaDB_Connector_J() {
            super("mysql:26", MARIADB_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 26 latest (Primary with Asynchronous Replica)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_26_latest__Replication extends JdbcReplicationTests {

        MySQL_26_latest__Replication() {
            super("mysql:26", MYSQL_CONNECTOR_J);
        }
    }

    @Nested
    @DisplayName("MySQL 26 latest (Primary with Asynchronous Replica, MariaDB Connector/J)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class MySQL_26_latest__Replication__MariaDB_Connector_J extends JdbcReplicationTests {

        MySQL_26_latest__Replication__MariaDB_Connector_J() {
            super("mysql:26", MARIADB_CONNECTOR_J);
        }
    }
}
