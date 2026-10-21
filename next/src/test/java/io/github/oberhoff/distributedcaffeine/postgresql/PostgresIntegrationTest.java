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
package io.github.oberhoff.distributedcaffeine.postgresql;

import io.github.oberhoff.distributedcaffeine.postgresql.PostgresTopologyTests.Setup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import static io.github.oberhoff.distributedcaffeine.postgresql.PostgresTopology.PRIMARY_WITH_ASYNCHRONOUS_STANDBY;
import static io.github.oberhoff.distributedcaffeine.postgresql.PostgresTopology.PRIMARY_WITH_SYNCHRONOUS_STANDBY;
import static io.github.oberhoff.distributedcaffeine.postgresql.PostgresTopology.SINGLE_SERVER;
import static io.github.oberhoff.distributedcaffeine.postgresql.PostgresTopology.SINGLE_SERVER_BEHIND_TRANSACTION_POOLER;
import static io.github.oberhoff.distributedcaffeine.postgresql.PostgresTopology.SINGLE_SERVER_WITH_SMALL_NOTIFY_QUEUE;
import static io.github.oberhoff.distributedcaffeine.testing.Topology.Tests.RUNS_ON_GITHUB;

/**
 * Every PostgreSQL image and topology the suites run on, one at a time: from 9.6, the oldest supported, to the latest,
 * each major version's first release and its latest; for 9.6 the first image that still starts (9.6.0 to 9.6.4 crash in
 * their gosu, built with a Go runtime of 2016). The pooler on the oldest and the latest, the small notification
 * queue from 17 on (where it can be configured), on the latest of a major version. With {@code -DrunsOnGitHub=true},
 * only the latest image runs.
 */
class PostgresIntegrationTest {

    @Nested
    @DisplayName("PostgreSQL 9.6.5 (Single Server)")
    @Setup(image = "postgres:9.6.5", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_9_6_5__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 9.6.5 (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:9.6.5", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_9_6_5__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 9.6.5 (Primary with Synchronous Standby)")
    @Setup(image = "postgres:9.6.5", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_9_6_5__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 9.6.5 (Single Server behind Transaction Pooler)")
    @Setup(image = "postgres:9.6.5", topology = SINGLE_SERVER_BEHIND_TRANSACTION_POOLER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_9_6_5__Single_Server_behind_Transaction_Pooler extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 9 latest (Single Server)")
    @Setup(image = "postgres:9", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_9_latest__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 9 latest (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:9", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_9_latest__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 9 latest (Primary with Synchronous Standby)")
    @Setup(image = "postgres:9", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_9_latest__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 10.0 (Single Server)")
    @Setup(image = "postgres:10.0", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_10_0__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 10.0 (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:10.0", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_10_0__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 10.0 (Primary with Synchronous Standby)")
    @Setup(image = "postgres:10.0", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_10_0__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 10 latest (Single Server)")
    @Setup(image = "postgres:10", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_10_latest__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 10 latest (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:10", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_10_latest__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 10 latest (Primary with Synchronous Standby)")
    @Setup(image = "postgres:10", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_10_latest__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 11.0 (Single Server)")
    @Setup(image = "postgres:11.0", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_11_0__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 11.0 (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:11.0", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_11_0__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 11.0 (Primary with Synchronous Standby)")
    @Setup(image = "postgres:11.0", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_11_0__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 11 latest (Single Server)")
    @Setup(image = "postgres:11", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_11_latest__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 11 latest (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:11", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_11_latest__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 11 latest (Primary with Synchronous Standby)")
    @Setup(image = "postgres:11", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_11_latest__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 12.0 (Single Server)")
    @Setup(image = "postgres:12.0", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_12_0__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 12.0 (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:12.0", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_12_0__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 12.0 (Primary with Synchronous Standby)")
    @Setup(image = "postgres:12.0", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_12_0__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 12 latest (Single Server)")
    @Setup(image = "postgres:12", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_12_latest__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 12 latest (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:12", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_12_latest__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 12 latest (Primary with Synchronous Standby)")
    @Setup(image = "postgres:12", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_12_latest__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 13.0 (Single Server)")
    @Setup(image = "postgres:13.0", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_13_0__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 13.0 (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:13.0", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_13_0__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 13.0 (Primary with Synchronous Standby)")
    @Setup(image = "postgres:13.0", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_13_0__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 13 latest (Single Server)")
    @Setup(image = "postgres:13", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_13_latest__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 13 latest (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:13", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_13_latest__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 13 latest (Primary with Synchronous Standby)")
    @Setup(image = "postgres:13", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_13_latest__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 14.0 (Single Server)")
    @Setup(image = "postgres:14.0", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_14_0__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 14.0 (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:14.0", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_14_0__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 14.0 (Primary with Synchronous Standby)")
    @Setup(image = "postgres:14.0", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_14_0__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 14 latest (Single Server)")
    @Setup(image = "postgres:14", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_14_latest__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 14 latest (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:14", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_14_latest__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 14 latest (Primary with Synchronous Standby)")
    @Setup(image = "postgres:14", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_14_latest__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 15.0 (Single Server)")
    @Setup(image = "postgres:15.0", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_15_0__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 15.0 (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:15.0", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_15_0__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 15.0 (Primary with Synchronous Standby)")
    @Setup(image = "postgres:15.0", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_15_0__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 15 latest (Single Server)")
    @Setup(image = "postgres:15", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_15_latest__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 15 latest (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:15", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_15_latest__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 15 latest (Primary with Synchronous Standby)")
    @Setup(image = "postgres:15", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_15_latest__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 16.0 (Single Server)")
    @Setup(image = "postgres:16.0", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_16_0__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 16.0 (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:16.0", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_16_0__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 16.0 (Primary with Synchronous Standby)")
    @Setup(image = "postgres:16.0", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_16_0__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 16 latest (Single Server)")
    @Setup(image = "postgres:16", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_16_latest__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 16 latest (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:16", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_16_latest__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 16 latest (Primary with Synchronous Standby)")
    @Setup(image = "postgres:16", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_16_latest__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 17.0 (Single Server)")
    @Setup(image = "postgres:17.0", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_17_0__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 17.0 (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:17.0", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_17_0__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 17.0 (Primary with Synchronous Standby)")
    @Setup(image = "postgres:17.0", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_17_0__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 17 latest (Single Server)")
    @Setup(image = "postgres:17", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_17_latest__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 17 latest (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:17", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_17_latest__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 17 latest (Primary with Synchronous Standby)")
    @Setup(image = "postgres:17", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_17_latest__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 17 latest (Single Server with Small Notify Queue)")
    @Setup(image = "postgres:17", topology = SINGLE_SERVER_WITH_SMALL_NOTIFY_QUEUE)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_17_latest__Single_Server_with_Small_Notify_Queue extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 18.0 (Single Server)")
    @Setup(image = "postgres:18.0", topology = SINGLE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_18_0__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 18.0 (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:18.0", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_18_0__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 18.0 (Primary with Synchronous Standby)")
    @Setup(image = "postgres:18.0", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Postgres_18_0__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 18 latest (Single Server)")
    @Setup(image = "postgres:18", topology = SINGLE_SERVER)
    class Postgres_18_latest__Single_Server extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 18 latest (Primary with Asynchronous Standby)")
    @Setup(image = "postgres:18", topology = PRIMARY_WITH_ASYNCHRONOUS_STANDBY)
    class Postgres_18_latest__Primary_with_Asynchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 18 latest (Primary with Synchronous Standby)")
    @Setup(image = "postgres:18", topology = PRIMARY_WITH_SYNCHRONOUS_STANDBY)
    class Postgres_18_latest__Primary_with_Synchronous_Standby extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 18 latest (Single Server with Small Notify Queue)")
    @Setup(image = "postgres:18", topology = SINGLE_SERVER_WITH_SMALL_NOTIFY_QUEUE)
    class Postgres_18_latest__Single_Server_with_Small_Notify_Queue extends PostgresTopologyTests {
    }

    @Nested
    @DisplayName("PostgreSQL 18 latest (Single Server behind Transaction Pooler)")
    @Setup(image = "postgres:18", topology = SINGLE_SERVER_BEHIND_TRANSACTION_POOLER)
    class Postgres_18_latest__Single_Server_behind_Transaction_Pooler extends PostgresTopologyTests {
    }
}
