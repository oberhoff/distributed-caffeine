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

import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The PostgreSQL adapter when a write cannot be announced: the notification queue is full, because a session
 * listening in the database (any application's) stays in a transaction and so consumes nothing. PostgreSQL then
 * refuses every write that notifies at its commit, so nothing is written unannounced, and the write fails as certainly
 * not committed, naming the cause. How PostgreSQL recovers once that session ended is not tested: it kept refusing
 * in every attempt measured (a session listening anew, VACUUM, both) unless no session listened at all and the
 * database was vacuumed. On a server with a small queue (256 pages, 2 MB; PostgreSQL 17 or later,
 * {@link PostgresTopology#SINGLE_SERVER_WITH_SMALL_NOTIFY_QUEUE}), so that it fills in a moment.
 */
@NullMarked
@Timeout(120)
@SuppressWarnings("SqlNoDataSourceInspection") // the SQL runs against the test container
abstract class PostgresNotifyQueueTest {

    private final PostgresTestDatabase database = PostgresTestDatabase.get();

    @Test
    void a_write_that_cannot_be_announced_is_refused_as_certainly_not_written() throws Exception {
        try (DistributedCache<String, String> cache = DistributedCaffeine.<String, String>newBuilder(
                        PostgresAdapter.newBuilder(database.dataSource, "queue", "cache")
                                .withListenerDataSource(database.listener).build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000))
                .build()) {
            // a session of another application, not of the pool: when it ends, its backend ends
            Connection stuck = java.sql.DriverManager.getConnection(database.jdbcUrl(), database.username(),
                    database.password());
            cache.put("k", "before");

            // a listening session left idle in a transaction: the queue cannot drain past it
            try (Statement statement = stuck.createStatement()) {
                statement.execute("LISTEN elsewhere");
            }
            stuck.setAutoCommit(false);
            try (Statement statement = stuck.createStatement()) {
                statement.execute("SELECT 1");
            }
            fillQueue();

            assertThatThrownBy(() -> cache.put("k", "unannounced"))
                    .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isFalse())
                    .hasMessageContaining("NOTIFY queue is full");
            assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value())
                    .as("nothing written unannounced").isEqualTo("before");

            stuck.rollback();
            stuck.close();
        }
    }

    // notifications until PostgreSQL refuses one: the queue is full
    private void fillQueue() throws SQLException {
        try (Connection connection = database.dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (int index = 0; index < 10_000; index++) {
                try {
                    statement.execute("SELECT pg_notify('flood', repeat('x', 7000))");
                } catch (SQLException e) {
                    if ("54000".equals(e.getSQLState())) {
                        return;
                    }
                    throw e;
                }
            }
        }
        throw new AssertionError("the notification queue never filled");
    }
}
