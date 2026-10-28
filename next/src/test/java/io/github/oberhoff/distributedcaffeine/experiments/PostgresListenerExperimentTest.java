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
package io.github.oberhoff.distributedcaffeine.experiments;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.PGStream;
import org.postgresql.core.QueryExecutorBase;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Listening without R2DBC (VISION.md, "PostgreSQL listening without R2DBC"): how fast notifications reach one session
 * per instance, and at what cost to PostgreSQL, for pgjdbc's own getNotifications (limited to ~750/s per session),
 * notifications batched per writer after the commit, and pgjdbc's stream taken over (what the adapter does). The
 * R2DBC variant it was measured against is gone with the dependency; its figures stay recorded in VISION.md. One
 * channel; writes paced by two writers, to random keys of 10,000 or to one hot key. Measures the time until a
 * notification arrives (over all instances) and PostgreSQL's CPU from the container's cgroup counters.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=PostgresListenerExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses;
// each writer thread (read) prepares its own statement, as the adapter does
// the SQL runs against a test container, unknown to the IDE
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2699", "java:S2925", "java:S9142"})
class PostgresListenerExperimentTest {

    private static final int KEYS = 10_000;
    private static final int WRITERS = 2;
    private static final double SECONDS = 4;

    private static final String UPSERT = """
            WITH w AS (
                INSERT INTO rows AS r (id, key, value, state, version, write_time)
                VALUES (?, ?, ?, 'VALUE', (extract(epoch from clock_timestamp()) * 1000000)::bigint, now())
                ON CONFLICT (id) DO UPDATE SET value = excluded.value, state = 'VALUE',
                    version = greatest(r.version + 1, excluded.version), write_time = now()
                RETURNING r.id, r.version)
            SELECT w.version, pg_notify('rows', 'WRITTEN|' || w.id || '|' || w.version || '|' || ?) FROM w""";

    // the write without its notification: sent after the commit, batched per writer (the BATCHED variants)
    private static final String UPSERT_ONLY = """
            WITH w AS (
                INSERT INTO rows AS r (id, key, value, state, version, write_time)
                VALUES (?, ?, ?, 'VALUE', (extract(epoch from clock_timestamp()) * 1000000)::bigint, now())
                ON CONFLICT (id) DO UPDATE SET value = excluded.value, state = 'VALUE',
                    version = greatest(r.version + 1, excluded.version), write_time = now()
                RETURNING r.id, r.version)
            SELECT w.version FROM w""";

    // TAKEOVER: pgjdbc connects through a pooled DataSource, then the listener reads pgjdbc's stream itself
    // BATCHED: pgjdbc listening, notifications sent by each writer after commit, at most one per window, each
    // carrying every write of the window
    private enum Listener {
        JDBC(0), BATCHED_2MS(2), BATCHED_5MS(5), TAKEOVER(0);

        final int windowMillis;

        Listener(int windowMillis) {
            this.windowMillis = windowMillis;
        }

        boolean batched() {
            return windowMillis > 0;
        }
    }

    @Test
    void measure() throws Exception {
        try (PostgreSQLContainer container = new PostgreSQLContainer("postgres:18")
                .withCommand("postgres", "-c", "max_connections=200")) {
            container.start();
            try (Connection setup = connect(container); Statement statement = setup.createStatement()) {
                statement.execute(PostgresStatementExperimentTest.SCHEMA);
            }
            StringBuilder out = new StringBuilder("\n=== listener client, postgres:18, one session per instance ===\n")
                    .append(String.format("%-8s %10s %10s %10s %12s %12s %16s %10s%n", "listener", "keys",
                            "instances", "target/s", "arrival p50", "arrival p99", "PG CPU ms/1000w", "reached/s"));
            Listener[] measured = {Listener.TAKEOVER, Listener.JDBC};
            for (int rate : new int[]{1_500, 3_000, 6_000}) {
                for (Listener listener : measured) {
                    out.append(run(container, listener, 4, rate, false));
                }
            }
            for (Listener listener : measured) {
                out.append(run(container, listener, 4, 1_500, true));
            }
            System.out.println(out);
        }
    }

    private static String run(PostgreSQLContainer container, Listener kind, int instances, int rate, boolean hotKey)
            throws Exception {
        int total = (int) (rate * SECONDS);
        long[] started = new long[total];
        List<long[]> latencies = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(instances);
        CountDownLatch stopped = new CountDownLatch(instances);
        List<AutoCloseable> closeables = new ArrayList<>();

        for (int instance = 0; instance < instances; instance++) {
            long[] latency = new long[total];
            latencies.add(latency);
            if (kind == Listener.TAKEOVER) {
                HikariConfig config = new HikariConfig();
                config.setJdbcUrl(container.getJdbcUrl());
                config.setUsername(container.getUsername());
                config.setPassword(container.getPassword());
                config.setMaximumPoolSize(2);
                HikariDataSource dataSource = new HikariDataSource(config);
                Connection connection = dataSource.getConnection();
                try (Statement statement = connection.createStatement()) {
                    statement.execute("LISTEN rows");
                }
                PGStream stream = takeOver(connection);
                Thread thread = new Thread(() -> {
                    try {
                        boolean stop = false;
                        while (!stop) {
                            int type = stream.receiveChar();
                            int length = stream.receiveInteger4();
                            if (type == 'A') {
                                stream.receiveInteger4(); // the notifying backend's pid
                                stream.receiveCanonicalString(); // the channel
                                stop = recordArrival(stream.receiveString(), latency, started, System.nanoTime());
                            } else {
                                stream.skip(length - 4);
                            }
                        }
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    } finally {
                        stopped.countDown();
                    }
                });
                thread.start();
                closeables.add(() -> {
                    // taken over: never back to the pool, which evicts an aborted connection
                    connection.abort(Runnable::run);
                    dataSource.close();
                });
                ready.countDown();
            } else {
                Thread thread = new Thread(() -> {
                    try (Connection listener = connect(container)) {
                        try (Statement statement = listener.createStatement()) {
                            statement.execute("LISTEN rows");
                        }
                        PGConnection notifications = listener.unwrap(PGConnection.class);
                        ready.countDown();
                        boolean stop = false;
                        while (!stop) {
                            PGNotification[] batch = notifications.getNotifications(0);
                            if (batch == null) {
                                continue;
                            }
                            long now = System.nanoTime();
                            for (PGNotification notification : batch) {
                                stop |= recordArrival(notification.getParameter(), latency, started, now);
                            }
                        }
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    } finally {
                        stopped.countDown();
                    }
                });
                thread.start();
            }
        }
        ready.await();
        Thread.sleep(500);

        long cpuBefore = postgresCpuMicros(container);
        AtomicInteger next = new AtomicInteger();
        long begin = System.nanoTime();
        List<Thread> writers = new ArrayList<>();
        for (int w = 0; w < WRITERS; w++) {
            BlockingQueue<String> pending = new LinkedBlockingQueue<>();
            Thread sender = kind.batched() ? sender(container, pending, kind.windowMillis) : null;
            Thread writer = new Thread(() -> {
                try (Connection connection = connect(container);
                     PreparedStatement statement = connection.prepareStatement(kind.batched() ? UPSERT_ONLY : UPSERT)) {
                    for (int seq = next.getAndIncrement(); seq < total; seq = next.getAndIncrement()) {
                        long due = begin + (long) seq * 1_000_000_000L / rate;
                        while (System.nanoTime() < due) {
                            Thread.onSpinWait();
                        }
                        String id = hotKey ? "hot" : "k" + (int) ((seq * 2654435761L) % KEYS);
                        statement.setString(1, id);
                        statement.setBytes(2, id.getBytes(StandardCharsets.UTF_8));
                        statement.setBytes(3, ("v" + seq).getBytes(StandardCharsets.UTF_8));
                        if (!kind.batched()) {
                            statement.setInt(4, seq);
                        }
                        started[seq] = System.nanoTime();
                        try (ResultSet resultSet = statement.executeQuery()) {
                            resultSet.next();
                            if (kind.batched()) {
                                pending.add("WRITTEN|" + id + "|" + resultSet.getLong(1) + "|" + seq);
                            }
                        }
                    }
                    if (sender != null) {
                        pending.add("");
                        sender.join();
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            writers.add(writer);
            writer.start();
        }
        for (Thread writer : writers) {
            writer.join();
        }
        double reached = total / ((System.nanoTime() - begin) / 1e9);
        Thread.sleep(3_000);
        long cpuAfter = postgresCpuMicros(container);
        try (Connection connection = connect(container); Statement statement = connection.createStatement()) {
            statement.execute("NOTIFY rows, 'STOP'");
        }
        assertThat(stopped.await(30, TimeUnit.SECONDS)).as("all notifications seen").isTrue();
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }

        long[] all = new long[total * instances];
        int index = 0;
        int missing = 0;
        for (long[] latency : latencies) {
            for (long value : latency) {
                if (value == 0) {
                    missing++;
                    value = Long.MAX_VALUE;
                }
                all[index++] = value;
            }
        }
        Arrays.sort(all);
        return String.format("%-8s %10s %10d %10d %10.1f ms %10.1f ms %16.0f %10.0f%s%n", kind,
                hotKey ? "1 (hot)" : "10,000", instances, rate,
                all[all.length / 2] / 1_000.0, all[Math.min(all.length - 1, all.length * 99 / 100)] / 1_000.0,
                (cpuAfter - cpuBefore) / 1_000.0 / (total / 1_000.0), reached,
                missing > 0 ? "  (" + missing + " missing)" : "");
    }

    // the stream behind a pgjdbc connection, also through a pool's proxy: the field is protected, with no getter
    private static PGStream takeOver(Connection connection) throws Exception {
        Field field = QueryExecutorBase.class.getDeclaredField("pgStream");
        field.setAccessible(true);
        return (PGStream) field.get(connection.unwrap(BaseConnection.class).getQueryExecutor());
    }

    // returns true for the stop marker; a batched payload carries several writes, separated by ';'
    private static boolean recordArrival(String payload, long[] latency, long[] started, long now) {
        if (payload.equals("STOP")) {
            return true;
        }
        for (String part : payload.split(";", -1)) {
            int seq = Integer.parseInt(part.substring(part.lastIndexOf('|') + 1));
            latency[seq] = (now - started[seq]) / 1_000;
        }
        return false;
    }

    // sends the writes queued after their commits: at most one notification per window, each with every write
    // queued meanwhile (up to PostgreSQL's 8000 bytes per payload); an empty entry ends it
    private static Thread sender(PostgreSQLContainer container, BlockingQueue<String> pending, int windowMillis) {
        Thread thread = new Thread(() -> {
            try (Connection connection = connect(container);
                 PreparedStatement notify = connection.prepareStatement("SELECT pg_notify('rows', ?)")) {
                long last = 0;
                boolean end = false;
                while (!end) {
                    String first = pending.take();
                    long due = last + windowMillis * 1_000_000L;
                    long wait = due - System.nanoTime();
                    if (wait > 0) {
                        LockSupport.parkNanos(wait);
                    }
                    List<String> drained = new ArrayList<>();
                    drained.add(first);
                    pending.drainTo(drained);
                    end = drained.remove("");
                    StringBuilder payload = new StringBuilder();
                    for (String part : drained) {
                        if (payload.length() + part.length() + 1 > 7_900) {
                            send(notify, payload);
                        }
                        if (!payload.isEmpty()) {
                            payload.append(';');
                        }
                        payload.append(part);
                    }
                    if (!payload.isEmpty()) {
                        send(notify, payload);
                    }
                    last = System.nanoTime();
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        thread.start();
        return thread;
    }

    private static void send(PreparedStatement notify, StringBuilder payload) throws Exception {
        notify.setString(1, payload.toString());
        try (ResultSet resultSet = notify.executeQuery()) {
            resultSet.next();
        }
        payload.setLength(0);
    }

    private static long postgresCpuMicros(PostgreSQLContainer container) throws Exception {
        String stat = container.execInContainer("cat", "/sys/fs/cgroup/cpu.stat").getStdout();
        for (String line : stat.split("\n", -1)) {
            if (line.startsWith("usage_usec")) {
                return Long.parseLong(line.split(" ", -1)[1].trim());
            }
        }
        throw new IllegalStateException("no cpu.stat: " + stat);
    }

    private static Connection connect(PostgreSQLContainer container) throws Exception {
        return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword());
    }
}
