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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.sql.DataSource;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The PostgreSQL adapter behind PgBouncer ({@link PostgresTopology#SINGLE_SERVER_BEHIND_TRANSACTION_POOLER}): the
 * data source through it in transaction mode works (the contract suites run so); a listener behind it receives
 * nothing, since a transaction-mode pooler lends its server session only for a transaction, and a data source in
 * statement mode cannot run the adapter's transactions: both fail the build, naming the cause.
 */
@Timeout(120)
abstract class PostgresPoolerTest {

    private static final Duration LONG = Duration.ofHours(1);

    private final PostgresTestDatabase database = PostgresTestDatabase.get();
    private final String table = PostgresTestDatabase.freshTable();

    private DistributedCaffeine<String, String> strings(DataSource dataSource, ConnectionFactory listener) {
        return DistributedCaffeine.<String, String>newBuilder(PostgresAdapter.newBuilder(dataSource, "pooler", table)
                        .withListener(listener).build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000));
    }

    @Test
    void a_listener_behind_a_pooler_in_transaction_mode_fails_the_build() {
        ConnectionFactory pooled = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(database.poolerHost())
                .port(database.poolerPort())
                .username(database.username())
                .password(database.password())
                .database(database.databaseName())
                .connectTimeout(Duration.ofSeconds(5))
                .build());
        DistributedCaffeine<String, String> builder = strings(database.dataSource, pooled);

        assertThatThrownBy(builder::build).isInstanceOf(StoreException.class)
                .hasMessageContaining("transaction-mode pooler");
    }

    @Test
    void a_data_source_behind_a_pooler_in_statement_mode_fails_the_build() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + database.poolerHost() + ":" + database.poolerPort() + "/"
                + PostgresTestDatabase.STATEMENT_POOLED + "?prepareThreshold=0");
        config.setUsername(database.username());
        config.setPassword(database.password());
        config.setMaximumPoolSize(4);
        config.setInitializationFailTimeout(-1);
        try (HikariDataSource pooled = new HikariDataSource(config)) {
            DistributedCaffeine<String, String> builder = strings(pooled, database.listener);

            assertThatThrownBy(builder::build).isInstanceOf(StoreException.class)
                    .hasStackTraceContaining("statement pooling mode");
        }
    }
}
