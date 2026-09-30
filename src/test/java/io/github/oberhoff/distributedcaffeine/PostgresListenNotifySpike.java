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
package io.github.oberhoff.distributedcaffeine;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Spike, not part of any suite - the class name matches none of surefire's include patterns, so it runs only when
 * asked for by name. It answers what the design of a LISTEN/NOTIFY adapter rests on and what no amount of reading
 * settles: whether pgjdbc hands a notification to a listener that is doing nothing else, and how the payload behaves
 * at its documented edges.
 * <p>
 * Delete once a real adapter carries these answers in its own tests.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Spike: PostgreSQL LISTEN/NOTIFY as a distribution transport")
final class PostgresListenNotifySpike {

    private static final String CHANNEL = "distributed_caffeine_spike";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private PostgreSQLContainer container;

    @BeforeAll
    void beforeAll() {
        container = new PostgreSQLContainer("postgres:17");
        container.start();
    }

    @AfterAll
    void afterAll() {
        container.stop();
    }

    @DisplayName("that a listener doing nothing else is handed a notification, and how quickly")
    @Test
    void notifications_reach_an_idle_listener() throws Exception {
        try (Connection listener = connect(); Connection writer = connect()) {
            listen(listener, CHANNEL);

            // parked before anything is published and issuing no query afterwards, which is the whole question:
            // does the driver read the socket while it waits, or only when something else drives the connection
            CompletableFuture<PGNotification[]> parked =
                    CompletableFuture.supplyAsync(() -> getNotifications(listener, TIMEOUT));
            Thread.sleep(500);

            long start = System.nanoTime();
            publish(writer, CHANNEL, "0123456789abcdef0123456789abcdef");
            PGNotification[] notifications = parked.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            System.out.printf("%n=== idle listener woken after %d ms ===%n", elapsedMillis);

            assertThat(notifications).hasSize(1);
            assertThat(notifications[0].getName()).isEqualTo(CHANNEL);
            assertThat(notifications[0].getParameter()).isEqualTo("0123456789abcdef0123456789abcdef");
            // generous, because what is being established is "promptly rather than at the timeout", not a latency
            assertThat(elapsedMillis).isLessThan(TIMEOUT.toMillis() / 2);
        }
    }

    @DisplayName("that one transaction folds identical payloads and keeps distinct ones")
    @Test
    void identical_payloads_are_folded_within_a_transaction() throws Exception {
        try (Connection listener = connect(); Connection writer = connect()) {
            listen(listener, CHANNEL);

            writer.setAutoCommit(false);
            publish(writer, CHANNEL, "hash-a");
            publish(writer, CHANNEL, "hash-b");
            publish(writer, CHANNEL, "hash-a");
            writer.commit();

            PGNotification[] notifications = getNotifications(listener, TIMEOUT);

            System.out.printf("=== three publishes (one a duplicate) arrived as %d notifications ===%n",
                    notifications.length);

            assertThat(notifications).extracting(PGNotification::getParameter)
                    .containsExactlyInAnyOrder("hash-a", "hash-b");
        }
    }

    @DisplayName("that the payload limit is what the documentation says, and how it fails")
    @Test
    void payloads_are_bounded() throws Exception {
        try (Connection listener = connect(); Connection writer = connect()) {
            listen(listener, CHANNEL);

            // 240 hashes of 32 hex characters plus separators is what a batch would carry
            String large = "x".repeat(7999);
            publish(writer, CHANNEL, large);
            assertThat(getNotifications(listener, TIMEOUT)).hasSize(1);

            assertThatThrownBy(() -> publish(writer, CHANNEL, "x".repeat(8000)))
                    .isInstanceOf(SQLException.class)
                    .satisfies(thrown ->
                            System.out.printf("=== over the limit: %s ===%n", thrown.getMessage().strip()));
        }
    }

    @DisplayName("that a session receives its own notifications, which is how an own echo would arrive")
    @Test
    void a_session_receives_its_own_notifications() throws Exception {
        try (Connection connection = connect()) {
            listen(connection, CHANNEL);
            publish(connection, CHANNEL, "own-echo");

            PGNotification[] notifications = getNotifications(connection, TIMEOUT);

            assertThat(notifications).hasSize(1);
            assertThat(notifications[0].getParameter()).isEqualTo("own-echo");
            // the backend process that published it, which is how a listener could tell its own writes apart
            assertThat(notifications[0].getPID())
                    .isEqualTo(connection.unwrap(PGConnection.class).getBackendPID());
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
    }

    private void listen(Connection connection, String channel) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("LISTEN " + channel);
        }
    }

    // through pg_notify rather than the NOTIFY statement, so that channel and payload are parameters instead of
    // string literals a publisher would have to quote itself
    private void publish(Connection connection, String channel, String payload) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_notify(?, ?)")) {
            statement.setString(1, channel);
            statement.setString(2, payload);
            statement.execute();
        }
    }

    private PGNotification[] getNotifications(Connection connection, Duration timeout) {
        try {
            PGNotification[] notifications = connection.unwrap(PGConnection.class)
                    .getNotifications((int) timeout.toMillis());
            return notifications == null ? new PGNotification[0] : notifications;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
