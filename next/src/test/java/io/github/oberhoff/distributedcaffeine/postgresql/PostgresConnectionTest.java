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
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.DistributionMode;
import io.github.oberhoff.distributedcaffeine.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.testing.SilencingProxy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The PostgreSQL adapter's connections when something is wrong with them: a listening session that goes silent or
 * stops in the middle of a notification (found by the core's heartbeat, and re-established when the core resumes it), a listener that cannot receive what is written
 * (another database), a data source that cannot be written through (a standby), and the listening session several
 * caches share. Poolers: {@link PostgresPoolerTest}.
 */
@Timeout(120)
// every cache, store and proxy a test opens is closed after it (@AfterEach); SQL runs against the test container
@SuppressWarnings({"resource", "SqlNoDataSourceInspection"})
abstract class PostgresConnectionTest {

    private static final Duration LONG = Duration.ofHours(1);
    // the core's heartbeat in these tests: a silent session is noticed within about two intervals
    private static final Duration HEARTBEAT = Duration.ofSeconds(2);
    // two heartbeats, a reconnect (up to its timeout), and some slack
    private static final Duration DETECTION = Duration.ofSeconds(30);
    private static final AtomicInteger NAMES = new AtomicInteger();

    private final PostgresTestDatabase database = PostgresTestDatabase.get();
    private final String table = PostgresTestDatabase.freshTable();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        // the caches before the proxies their sessions run through
        for (int index = closeables.size() - 1; index >= 0; index--) {
            closeables.get(index).close();
        }
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    // a listener data source of its own, so that its session is not shared with other tests
    private DataSource listener(String host, int port, String databaseName, String application) {
        return database.direct(host, port, databaseName, application);
    }

    private DataSource listener(String application) {
        return listener(database.host(), database.port(),
                database.databaseName(), application);
    }

    private static String application() {
        return "dc-connection-" + NAMES.incrementAndGet();
    }

    private PostgresAdapter<String, String> adapter(DataSource dataSource, DataSource listener) {
        return PostgresAdapter.newBuilder(dataSource, "connection", table).withListenerDataSource(listener).build();
    }

    private DistributedCaffeine<String, String> strings(PostgresAdapter<String, String> adapter) {
        return strings(adapter, HEARTBEAT);
    }

    private DistributedCaffeine<String, String> strings(PostgresAdapter<String, String> adapter, Duration heartbeat) {
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000).withHeartbeatInterval(heartbeat));
    }

    private DistributedCache<String, String> cache(DataSource listener) {
        return track(strings(adapter(database.dataSource, listener)).build());
    }

    private DistributedCache<String, String> writer() {
        return track(strings(adapter(database.dataSource, database.listener))
                .withDistributionMode(DistributionMode.NONE).build());
    }

    private int sessions(String application) {
        try (Connection connection = database.dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT count(*) FROM pg_stat_activity WHERE application_name = '" + application + "'")) {
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void awaitState(DistributedCache<?, ?> cache, SynchronizationState state) {
        assertThat(awaitUpTo(() -> cache.distributedPolicy().getSynchronizationState() == state, DETECTION,
                Duration.ofMillis(50))).as("state " + state).isTrue();
    }

    // a listening session the network silently stops carrying

    @Test
    void a_listening_session_that_went_silent_is_replaced_and_what_it_missed_is_revalidated() throws Exception {
        SilencingProxy proxy = track(new SilencingProxy(database.host(),
                database.port()));
        DistributedCache<String, String> writer = writer();
        DistributedCache<String, String> holder = cache(listener("127.0.0.1", proxy.port(),
                database.databaseName(), application()));
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        proxy.silenceOpenConnections();
        writer.put("k", "written while the session was silent");

        awaitState(holder, SynchronizationState.DEGRADED);
        awaitState(holder, SynchronizationState.SYNCHRONIZED);
        assertThat(holder.getIfPresent("k")).isEqualTo("written while the session was silent");
    }

    @Test
    void a_listening_session_stuck_in_the_middle_of_a_notification_is_aborted_and_replaced() throws Exception {
        SilencingProxy proxy = track(new SilencingProxy(database.host(),
                database.port()));
        DistributedCache<String, String> writer = writer();
        DistributedCache<String, String> holder = cache(listener("127.0.0.1", proxy.port(),
                database.databaseName(), application()));
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        // the next notification is cut off after its first byte, which leaves the session waiting for the rest
        proxy.silenceOpenConnectionsAfter(1);
        writer.put("k", "cut off");

        awaitState(holder, SynchronizationState.DEGRADED);
        awaitState(holder, SynchronizationState.SYNCHRONIZED);
        assertThat(holder.getIfPresent("k")).isEqualTo("cut off");
    }

    // a data source the network silently stops carrying

    @Test
    void a_statement_on_a_silent_connection_fails_within_the_timeout_instead_of_hanging() throws Exception {
        SilencingProxy proxy = track(new SilencingProxy(database.host(),
                database.port()));
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://127.0.0.1:" + proxy.port() + "/" + database.databaseName());
        config.setUsername(database.username());
        config.setPassword(database.password());
        config.setMaximumPoolSize(1);
        HikariDataSource pool = track(new HikariDataSource(config));
        DistributedCache<String, String> cache = track(strings(PostgresAdapter.newBuilder(pool, "connection", table)
                .withListenerDataSource(database.listener).build())
                .withStore(store -> store.withTimeout(Duration.ofSeconds(2)))
                .build());
        cache.put("k", "before");

        // at once, so that the pool hands the silenced connection out again without checking it first
        proxy.silenceOpenConnections();
        long started = System.nanoTime();
        assertThatThrownBy(() -> cache.put("k", "during"))
                .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isTrue());
        assertThat(Duration.ofNanos(System.nanoTime() - started)).as("failing after the timeout, not hanging")
                .isLessThan(Duration.ofSeconds(6));

        // the broken connection is gone from the pool; a new one is carried
        cache.put("k", "after");
        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).isEqualTo("after");
    }

    // a slow database: slow, but not broken

    @Test
    void a_slow_database_is_waited_for_and_not_taken_for_a_broken_one() throws Exception {
        SilencingProxy proxy = track(new SilencingProxy(database.host(),
                database.port()));
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://127.0.0.1:" + proxy.port() + "/" + database.databaseName());
        config.setUsername(database.username());
        config.setPassword(database.password());
        config.setMaximumPoolSize(4);
        HikariDataSource pool = track(new HikariDataSource(config));
        DataSource listener = listener("127.0.0.1", proxy.port(), database.databaseName(),
                application());
        // the default heartbeat (10 s): a beat takes a few slow round trips, well within it
        Duration heartbeat = Duration.ofSeconds(10);
        DistributedCache<String, String> writer = track(strings(adapter(pool, listener), heartbeat)
                .withDistributionMode(DistributionMode.NONE).build());
        DistributedCache<String, String> holder = track(strings(adapter(pool, listener), heartbeat).build());
        writer.put("k", "before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");

        // every chunk either way a quarter of a second late: a round trip of at least half a second
        proxy.delay(Duration.ofMillis(250));
        long until = System.nanoTime() + heartbeat.multipliedBy(2).plusSeconds(2).toNanos();
        for (int index = 0; System.nanoTime() < until; index++) {
            String value = "slow " + index;
            writer.put("k", value);
            await(() -> holder.policy().getIfPresentQuietly("k") == null);
            assertThat(holder.getIfPresent("k")).isEqualTo(value);
            assertThat(holder.distributedPolicy().getSynchronizationState()).as("heartbeats are late, not missing")
                    .isEqualTo(SynchronizationState.SYNCHRONIZED);
            assertThat(writer.distributedPolicy().getSynchronizationState())
                    .isEqualTo(SynchronizationState.SYNCHRONIZED);
        }
    }

    @Test
    void a_listening_session_slower_than_the_probe_timeout_is_taken_for_a_broken_one_until_it_is_fast_again()
            throws Exception {
        SilencingProxy proxy = track(new SilencingProxy(database.host(),
                database.port()));
        DistributedCache<String, String> writer = writer();
        DistributedCache<String, String> holder = cache(listener("127.0.0.1", proxy.port(),
                database.databaseName(), application()));
        writer.put("k", "before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");

        // a notification takes longer to arrive than a heartbeat interval: by design the same as a silent session
        proxy.delay(HEARTBEAT.plusSeconds(1));
        awaitState(holder, SynchronizationState.DEGRADED);
        writer.put("k", "written while the session was too slow");
        proxy.delay(Duration.ZERO);

        awaitState(holder, SynchronizationState.SYNCHRONIZED);
        assertThat(holder.getIfPresent("k")).isEqualTo("written while the session was too slow");
    }

    // listeners that cannot receive what is written

    @Test
    void a_listener_on_another_database_receives_nothing_and_fails_the_build() {
        String elsewhere = "elsewhere_" + NAMES.incrementAndGet();
        database.execute("CREATE DATABASE " + elsewhere);
        DistributedCaffeine<String, String> builder = strings(adapter(database.dataSource,
                listener(database.host(), database.port(), elsewhere,
                        application())));

        assertThatThrownBy(builder::build).isInstanceOf(StoreException.class)
                .hasMessageContaining("received no notification");
    }

    // data sources that cannot be written through

    @Test
    void a_data_source_reaching_a_standby_is_refused() {
        DistributedCaffeine<String, String> builder = strings(adapter(standby(), database.listener));

        assertThatThrownBy(builder::build).isInstanceOf(StoreException.class).hasMessageContaining("standby");
    }

    // the listening session caches share

    @Test
    void a_listener_failing_on_a_notification_leaves_the_others_on_the_session_undisturbed() {
        DataSource shared = listener(application());
        AtomicInteger failed = new AtomicInteger();
        Store raw = track(adapter(database.dataSource, shared).createStore(StoreSettings.of(Format.TEXT, Format.TEXT)));
        track(((ChangeFeed) raw).subscribe(new ChangeListener() {
            @Override
            public void onChanges(List<Change> messages) {
                failed.incrementAndGet();
                throw new IllegalStateException("a listener failing on purpose");
            }

            @Override
            public void onGap() {
                // nothing held
            }

            @Override
            public void onLost(Throwable cause) {
                // nothing held, and resumed by the caches on the same session
            }
        }));
        DistributedCache<String, String> writer = writer();
        DistributedCache<String, String> holder = cache(shared);

        for (String value : List.of("first", "second")) {
            writer.put("k", value);
            assertThat(holder.getIfPresent("k")).isEqualTo(value);
            writer.put("k", value + ", replaced");
            await(() -> holder.policy().getIfPresentQuietly("k") == null);
        }
        assertThat(failed).as("the failing listener keeps receiving").hasValueGreaterThanOrEqualTo(2);
        assertThat(holder.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);
    }

    @Test
    void a_cache_slow_to_apply_what_arrives_holds_up_no_other_cache_on_the_session() throws Exception {
        SilencingProxy proxy = track(new SilencingProxy(database.host(),
                database.port()));
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://127.0.0.1:" + proxy.port() + "/" + database.databaseName());
        config.setUsername(database.username());
        config.setPassword(database.password());
        config.setMaximumPoolSize(2);
        HikariDataSource slowPool = track(new HikariDataSource(config));
        DataSource shared = listener(application());
        DistributedCache<String, String> writer = writer();
        // updates its copy by reading the new value, through a data source whose every round trip takes 2 s
        DistributedCache<String, String> slow = track(strings(adapter(slowPool, shared))
                .withDistributionMode(DistributionMode.UPDATE).build());
        DistributedCache<String, String> fast = cache(shared);
        writer.put("k", "old");
        assertThat(slow.getIfPresent("k")).isEqualTo("old");
        assertThat(fast.getIfPresent("k")).isEqualTo("old");

        proxy.delayRequests(Duration.ofSeconds(2));
        writer.put("k", "new");

        assertThat(awaitUpTo(() -> fast.policy().getIfPresentQuietly("k") == null, Duration.ofMillis(1_500),
                Duration.ofMillis(10))).as("the other cache applies the message at once").isTrue();
        assertThat(slow.policy().getIfPresentQuietly("k")).as("still reading the new value").isEqualTo("old");
        assertThat(awaitUpTo(() -> "new".equals(slow.policy().getIfPresentQuietly("k")), Duration.ofSeconds(20),
                Duration.ofMillis(50))).as("and the slow one gets there").isTrue();
    }

    @Test
    void the_shared_session_is_opened_once_and_closed_with_its_last_cache() {
        String application = application();
        DataSource shared = listener(application);
        DistributedCache<String, String> first = cache(shared);
        DistributedCache<String, String> second = cache(shared);
        assertThat(sessions(application)).isEqualTo(1);

        first.close();
        assertThat(sessions(application)).as("still used by the second").isEqualTo(1);
        second.close();
        await(() -> sessions(application) == 0);

        DistributedCache<String, String> again = cache(shared);
        DistributedCache<String, String> writer = writer();
        assertThat(sessions(application)).isEqualTo(1);
        writer.put("k", "old");
        assertThat(again.getIfPresent("k")).isEqualTo("old");
        writer.put("k", "new");
        await(() -> again.policy().getIfPresentQuietly("k") == null);
    }

    @Test
    void a_cache_joining_the_shared_session_while_it_reconnects_receives_what_is_written() {
        String application = application();
        DataSource shared = listener(application);
        DistributedCache<String, String> first = cache(shared);
        DistributedCache<String, String> writer = writer();

        database.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = '"
                + application + "'");
        DistributedCache<String, String> joining = cache(shared);

        writer.put("k", "old");
        assertThat(joining.getIfPresent("k")).isEqualTo("old");
        assertThat(first.getIfPresent("k")).isEqualTo("old");
        awaitState(joining, SynchronizationState.SYNCHRONIZED);
        writer.put("k", "new");
        await(() -> joining.policy().getIfPresentQuietly("k") == null
                && first.policy().getIfPresentQuietly("k") == null);
    }

    // reconnecting: failing again and again, a connection broken

    @Test
    void a_reconnect_failing_again_and_again_is_retried_until_it_succeeds() {
        String application = application();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger attempts = new AtomicInteger();
        DistributedCache<String, String> holder = cache(failing(listener(application), failures, attempts));
        DistributedCache<String, String> writer = writer();
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        failures.set(2);
        terminate(application);
        awaitState(holder, SynchronizationState.DEGRADED);
        writer.put("k", "written while reconnecting");

        awaitState(holder, SynchronizationState.SYNCHRONIZED);
        assertThat(attempts).as("the first connect, two failed reconnects, one that succeeded").hasValue(4);
        assertThat(holder.getIfPresent("k")).isEqualTo("written while reconnecting");
    }

    @Test
    void a_session_broken_by_a_message_it_cannot_decode_is_closed_and_replaced() throws Exception {
        String application = application();
        SilencingProxy proxy = track(new SilencingProxy(database.host(),
                database.port()));
        DistributedCache<String, String> writer = writer();
        DistributedCache<String, String> holder = cache(listener("127.0.0.1", proxy.port(),
                database.databaseName(), application));
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        // a backend message whose length is shorter than the length itself: the session fails on its own thread
        proxy.sendToClients(new byte[]{'~', 0, 0, 0, 0});
        writer.put("k", "written after the broken message");

        awaitState(holder, SynchronizationState.SYNCHRONIZED);
        await(() -> "written after the broken message".equals(holder.getIfPresent("k")));
        assertThat(awaitUpTo(() -> sessions(application) == 1, DETECTION, Duration.ofMillis(100)))
                .as("the broken session closed, not left open next to its replacement").isTrue();
    }

    @Test
    void a_pool_that_timed_out_waiting_for_a_connection_is_not_asked_again() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername(database.username());
        config.setPassword(database.password());
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(1_000);
        HikariDataSource pool = track(new HikariDataSource(config));
        Store store = track(adapter(pool, database.listener).createStore(StoreSettings.of(Format.TEXT, Format.TEXT)));

        try (Connection held = pool.getConnection()) {
            assertThat(held.isValid(1)).isTrue();
            long started = System.nanoTime();
            assertThatThrownBy(() -> store.read(List.of("h"), LONG)).isInstanceOf(StoreException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - started)).as("one wait for the pool, not three")
                    .isLessThan(Duration.ofSeconds(2));
        }
    }

    // helpers

    private void terminate(String application) {
        database.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = '"
                + application + "'");
    }

    // a listener data source whose next connects fail, as long as failures are left
    private DataSource failing(DataSource real, AtomicInteger failures, AtomicInteger attempts) {
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getConnection")) {
                        attempts.incrementAndGet();
                        if (failures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                            throw new SQLException("injected: cannot connect", "08001");
                        }
                    }
                    return invoke(real, method, args);
                });
    }

    // the database as a standby would answer it: pg_is_in_recovery() is true. Fresh physical connections, so that
    // none is known as a primary already
    private DataSource standby() {
        PGSimpleDataSource direct = new PGSimpleDataSource();
        direct.setUrl(database.jdbcUrl());
        direct.setUser(database.username());
        direct.setPassword(database.password());
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object result = invoke(direct, method, args);
                    return method.getName().equals("getConnection") ? inRecovery((Connection) result) : result;
                });
    }

    private Connection inRecovery(Connection real) {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "isWrapperFor" -> false;
                    case "createStatement" -> {
                        Statement statement = (Statement) invoke(real, method, args);
                        yield Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Statement.class},
                                (p, m, a) -> m.getName().equals("executeQuery")
                                        && ((String) a[0]).contains("pg_is_in_recovery")
                                        ? invoke(statement, m, new Object[]{"SELECT true"})
                                        : invoke(statement, m, a));
                    }
                    default -> invoke(real, method, args);
                });
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
