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

import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How the MariaDB and MySQL store classifies failures for the core's retries (VISION.md, "Write retries", and "MariaDB
 * and MySQL", step 4). With injected failures: a rejected statement or a connection that cannot be obtained is
 * retryable and certainly not applied, a broken connection retryable with a write's outcome unknown, other errors not
 * retryable, a batch failing after a part of it was written of unknown outcome; and the retries through a cache: the
 * store tries once, the core again. With the server's own failures, through the driver under test (the drivers report
 * the same error differently, which the classification must not depend on): a deadlock, a lock wait timeout, and the
 * session of a write killed while it waits.
 */
@NullMarked
@Timeout(120)
// the SQL runs against the test database, unknown to the IDE
@SuppressWarnings("SqlNoDataSourceInspection")
abstract class JdbcRetryTest {

    private static final Duration LONG = Duration.ofHours(1);
    // an unconditional write; versions are the core's, any positive one does here
    private static final Write WRITE = write("h", "v", 1);

    private final AtomicInteger passingStatements = new AtomicInteger();
    private final AtomicInteger failingStatements = new AtomicInteger();
    private final AtomicInteger failingConnections = new AtomicInteger();
    private final AtomicInteger executions = new AtomicInteger();
    private volatile SQLException injected = deadlock();

    private static Write write(String hash, String value, long version) {
        return Write.value(hash, hash.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8),
                version);
    }

    // as the server reports them (SQLState, error code), not as a driver may have mapped them to exception classes
    private static SQLException deadlock() {
        return new SQLException("injected: Deadlock found when trying to get lock", "40001", 1213);
    }

    private static SQLException linkFailure() {
        return new SQLException("injected: Communications link failure", "08S01", 0);
    }

    private static SQLException syntaxError() {
        return new SQLException("injected: You have an error in your SQL syntax", "42000", 1064);
    }

    // the database's data source, with the next statements reading or writing rows failing; not the polls' scans
    // (which a cache runs meanwhile) nor the schema's check
    private DataSource faulty() {
        DataSource real = JdbcTestDatabase.dataSource();
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getConnection")) {
                        if (failingConnections.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                            throw new SQLException("injected: cannot connect", "08001");
                        }
                        return connection((Connection) invoke(real, method, args));
                    }
                    return invoke(real, method, args);
                });
    }

    private Connection connection(Connection real) {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    Object result = invoke(real, method, args);
                    if (method.getName().equals("prepareStatement") && ((String) args[0]).contains("discriminator")
                            && !((String) args[0]).contains("write_time >")) {
                        return statement((PreparedStatement) result);
                    }
                    return result;
                });
    }

    private PreparedStatement statement(PreparedStatement real) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getName().startsWith("execute")) {
                executions.incrementAndGet();
                if (passingStatements.getAndUpdate(n -> Math.max(0, n - 1)) == 0
                        && failingStatements.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                    throw injected;
                }
            }
            return invoke(real, method, args);
        };
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, handler);
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    // counting starts after the schema's check
    private Store store() {
        Store store = JdbcTestDatabase.adapter(faulty(), JdbcTestDatabase.freshTable(),
                JdbcTestDatabase.DEFAULT_DISCRIMINATOR).createStore(StoreSettings.of(Format.TEXT, Format.TEXT));
        executions.set(0);
        return store;
    }

    private static Store store(DataSource dataSource, String table) {
        return JdbcTestDatabase.adapter(dataSource, table, JdbcTestDatabase.DEFAULT_DISCRIMINATOR)
                .createStore(StoreSettings.of(Format.TEXT, Format.TEXT));
    }

    // no maintenance meanwhile, which would run statements of its own
    private DistributedCache<String, String> cache() {
        DistributedCache<String, String> cache = DistributedCaffeine.<String, String>newBuilder(
                        JdbcTestDatabase.adapter(faulty(), JdbcTestDatabase.freshTable(),
                                JdbcTestDatabase.DEFAULT_DISCRIMINATOR))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10_000).withMaintenanceInterval(LONG))
                .build();
        executions.set(0);
        return cache;
    }

    private static void assertFailure(Runnable call, boolean outcomeUnknown, boolean retryable) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(StoreException.class, e -> {
            assertThat(e.isOutcomeUnknown()).as("outcome unknown: " + e.getMessage()).isEqualTo(outcomeUnknown);
            assertThat(e.isRetryable()).as("retryable: " + e.getMessage()).isEqualTo(retryable);
        });
    }

    // the store: one attempt, classified

    @Test
    void a_rejected_statement_is_retryable_and_certainly_not_applied() {
        try (Store store = store()) {
            failingStatements.set(1);
            injected = deadlock();

            assertFailure(() -> store.write(List.of(WRITE)), false, true);
            assertThat(executions).as("tried once").hasValue(1);
        }
    }

    @Test
    void a_connection_that_cannot_be_obtained_is_retryable_and_certainly_not_applied() {
        try (Store store = store()) {
            failingConnections.set(1);

            assertFailure(() -> store.write(List.of(WRITE)), false, true);
        }
    }

    @Test
    void a_broken_connection_is_retryable_with_a_writes_outcome_unknown() {
        try (Store store = store()) {
            injected = linkFailure();
            failingStatements.set(1);
            assertFailure(() -> store.write(List.of(WRITE)), true, true);
            failingStatements.set(1);
            assertFailure(() -> store.read(List.of("h"), LONG), false, true);
        }
    }

    @Test
    void other_errors_are_not_retryable() {
        try (Store store = store()) {
            failingStatements.set(1);
            injected = syntaxError();

            assertFailure(() -> store.write(List.of(WRITE)), false, false);
        }
    }

    // each row in a transaction of its own: a later row failing leaves the earlier ones written
    @Test
    void a_batch_failing_after_a_part_of_it_was_written_has_its_outcome_unknown() {
        try (Store store = store()) {
            passingStatements.set(2); // the first row's UPDATE (finding none) and INSERT, then the second's fails
            failingStatements.set(1);
            injected = deadlock();

            assertThatThrownBy(() -> store.write(List.of(write("a", "v", 1), write("b", "v", 1))))
                    .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isTrue())
                    .hasMessageContaining("part of the write");
        }
    }

    // through a cache: the core retries

    @Test
    void a_rejected_put_is_written_by_the_cores_next_attempt() {
        try (DistributedCache<String, String> cache = cache()) {
            failingStatements.set(2);
            injected = deadlock();

            cache.put("k", "v");

            assertThat(executions.get()).as("two rejected, then the third attempt").isGreaterThanOrEqualTo(3);
            assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).isEqualTo("v");
        }
    }

    @Test
    void a_put_on_a_broken_connection_is_not_repeated_but_a_conditional_write_is() {
        try (DistributedCache<String, String> cache = cache()) {
            injected = linkFailure();
            failingStatements.set(1);
            assertThatThrownBy(() -> cache.put("k", "v")).isInstanceOfSatisfying(StoreException.class,
                    e -> assertThat(e.isOutcomeUnknown()).isTrue());
            assertThat(executions).hasValue(1);

            executions.set(0);
            failingStatements.set(1);
            assertThat(cache.asMap().putIfAbsent("other", "v")).isNull();
            assertThat(executions.get()).as("broken, then repeated").isGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void exhausted_retries_fail_after_three_attempts() {
        try (DistributedCache<String, String> cache = cache()) {
            failingStatements.set(5);
            injected = deadlock();

            assertThatThrownBy(() -> cache.put("k", "v")).isInstanceOfSatisfying(StoreException.class,
                    e -> assertThat(e.isOutcomeUnknown()).isFalse()).hasMessageContaining("after 3 attempts");
            assertThat(executions).hasValue(3);
        }
    }

    // the server's own failures, through the driver under test

    @Test
    void a_real_deadlock_is_retryable_and_certainly_not_applied() throws Exception {
        String table = JdbcTestDatabase.freshTable() + "_deadlock";
        try (Connection setup = JdbcTestDatabase.dataSource().getConnection();
             Statement statement = setup.createStatement()) {
            statement.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, v INT)");
            statement.execute("INSERT INTO " + table + " VALUES (1, 0), (2, 0)");
        }
        try (HikariDataSource pool = JdbcTestDatabase.pool(2, null);
             Connection first = pool.getConnection(); Connection second = pool.getConnection()) {
            long waiter = id(first);
            first.setAutoCommit(false);
            second.setAutoCommit(false);
            update(first, table, 1);
            update(second, table, 2);
            // each then waits for the other's row: the server picks one as the victim
            CompletableFuture<@Nullable SQLException> waiting = CompletableFuture.supplyAsync(() -> {
                try {
                    update(first, table, 2);
                    return null;
                } catch (SQLException e) {
                    return e;
                }
            });
            assertThat(awaitUpTo(() -> busy(waiter), Duration.ofSeconds(10), Duration.ofMillis(50)))
                    .as("the first session waiting").isTrue();
            SQLException victim = null;
            try {
                update(second, table, 1);
            } catch (SQLException e) {
                victim = e;
            }
            second.rollback();
            SQLException other = waiting.get(30, TimeUnit.SECONDS);
            first.rollback();
            SQLException deadlock = requireNonNull(victim != null ? victim : other, "no session was the victim");

            assertThat(deadlock.getErrorCode()).as(deadlock.toString()).isEqualTo(1213);
            assertThat(JdbcStore.rejected(deadlock)).as("rejected: " + deadlock).isTrue();
            assertThat(JdbcStore.connectionLost(deadlock)).as("not a lost connection: " + deadlock).isFalse();
        }
    }

    private static void update(Connection connection, String table, int id) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE " + table + " SET v = v + 1 WHERE id = " + id);
        }
    }

    // the session's id on the server
    private static long id(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT CONNECTION_ID()")) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    // whether the session runs a statement now (waiting for a lock, here), by the process list as the administrator
    // sees it: every session, whatever user and driver
    private static boolean busy(long session) {
        try (Connection root = JdbcTestDatabase.root();
             PreparedStatement statement = root.prepareStatement(
                     "SELECT COMMAND FROM information_schema.PROCESSLIST WHERE ID = ?")) {
            statement.setLong(1, session);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() && !"Sleep".equals(resultSet.getString(1));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    // a row locked by another session for longer than the store's sessions wait for a lock (1 s here): the server
    // gives up the store's statement (1205), whose transaction the store rolls back
    @Test
    void a_real_lock_wait_timeout_is_retryable_and_certainly_not_applied() throws SQLException {
        String table = JdbcTestDatabase.freshTable();
        try (HikariDataSource pool = JdbcTestDatabase.pool(2, "SET SESSION innodb_lock_wait_timeout = 1");
             Store store = store(pool, table)) {
            store.write(List.of(WRITE));
            try (Connection locker = JdbcTestDatabase.dataSource().getConnection()) {
                lock(locker, table);
                try {
                    assertFailure(() -> store.write(List.of(write("h", "w", 2))), false, true);
                } finally {
                    locker.rollback();
                    locker.setAutoCommit(true);
                }
            }

            Row row = requireNonNull(store.read(List.of("h"), LONG).get("h"));
            assertThat(row.version()).as("not applied").isEqualTo(1);
        }
    }

    // the session of a write waiting for a lock killed (as a server restarting, or an administrator, does it): the
    // connection breaks mid-statement, and whether the write applied is unknown to the store
    @Test
    void a_write_whose_session_is_killed_has_its_outcome_unknown() throws Exception {
        String table = JdbcTestDatabase.freshTable();
        // one connection: the store's session is the one whose id is read here
        try (HikariDataSource pool = JdbcTestDatabase.pool(1, null); Store store = store(pool, table)) {
            store.write(List.of(WRITE));
            long session;
            try (Connection connection = pool.getConnection()) {
                session = id(connection);
            }
            try (Connection locker = JdbcTestDatabase.dataSource().getConnection()) {
                lock(locker, table);
                try {
                    CompletableFuture<Void> writing = CompletableFuture.runAsync(
                            () -> store.write(List.of(write("h", "w", 2))));
                    assertThat(awaitUpTo(() -> busy(session), Duration.ofSeconds(10), Duration.ofMillis(50)))
                            .as("the write waiting").isTrue();
                    try (Connection root = JdbcTestDatabase.root(); Statement statement = root.createStatement()) {
                        statement.execute("KILL CONNECTION " + session);
                    }

                    assertThatThrownBy(() -> writing.get(30, TimeUnit.SECONDS)).cause()
                            .isInstanceOfSatisfying(StoreException.class, e -> {
                                assertThat(e.isOutcomeUnknown()).as("outcome unknown: " + e.getMessage()).isTrue();
                                assertThat(e.isRetryable()).as("retryable: " + e.getMessage()).isTrue();
                            });
                } finally {
                    locker.rollback();
                    locker.setAutoCommit(true);
                }
            }
        }
    }

    private static void lock(Connection connection, String table) throws SQLException {
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT id FROM " + table + " WHERE id = 'h' FOR UPDATE")) {
            assertThat(resultSet.next()).as("the row locked").isTrue();
        }
    }
}
