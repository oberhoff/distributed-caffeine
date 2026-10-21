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

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.Disposable;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Open question 1 of VISION.md, option 2: does a listener that drains notifications promptly (r2dbc-postgresql,
 * Netty-based) keep propagation prompt with one session per instance, above pgjdbc's ~750/s per-session limit? One
 * channel; writes paced by two writers, to random keys of 10,000 or to one hot key. Measures the time until a
 * notification arrives (over all instances) and PostgreSQL's CPU from the container's cgroup counters.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=PostgresR2dbcListenerExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses;
// each writer thread (read) prepares its own statement, as the adapter does
// the SQL runs against a test container, unknown to the IDE
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2699", "java:S2925", "java:S9142"})
class PostgresR2dbcListenerExperimentTest {

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

    private enum Listener {JDBC, R2DBC}

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
            for (int round = 0; round < 2; round++) {
                for (int rate : new int[]{1_500, 3_000, 6_000}) {
                    for (Listener listener : Listener.values()) {
                        out.append(run(container, listener, 4, rate, false));
                    }
                }
                out.append(run(container, Listener.R2DBC, 16, 3_000, false));
                for (Listener listener : Listener.values()) {
                    out.append(run(container, listener, 4, 1_500, true));
                }
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
            if (kind == Listener.JDBC) {
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
            } else {
                PostgresqlConnectionFactory factory = new PostgresqlConnectionFactory(
                        PostgresqlConnectionConfiguration.builder()
                                .host(container.getHost())
                                .port(container.getMappedPort(5432))
                                .username(container.getUsername())
                                .password(container.getPassword())
                                .database(container.getDatabaseName())
                                .build());
                PostgresqlConnection connection = requireNonNull(factory.create().block());
                connection.createStatement("LISTEN rows").execute()
                        .flatMap(PostgresqlResult::getRowsUpdated).then().block();
                Disposable subscription = connection.getNotifications().subscribe(notification -> {
                    long now = System.nanoTime();
                    if (recordArrival(requireNonNull(notification.getParameter()), latency, started, now)) {
                        stopped.countDown();
                    }
                });
                closeables.add(() -> {
                    subscription.dispose();
                    connection.close().block();
                });
                ready.countDown();
            }
        }
        ready.await();
        Thread.sleep(500);

        long cpuBefore = postgresCpuMicros(container);
        AtomicInteger next = new AtomicInteger();
        long begin = System.nanoTime();
        List<Thread> writers = new ArrayList<>();
        for (int w = 0; w < WRITERS; w++) {
            Thread writer = new Thread(() -> {
                try (Connection connection = connect(container);
                     PreparedStatement statement = connection.prepareStatement(UPSERT)) {
                    for (int seq = next.getAndIncrement(); seq < total; seq = next.getAndIncrement()) {
                        long due = begin + (long) seq * 1_000_000_000L / rate;
                        while (System.nanoTime() < due) {
                            Thread.onSpinWait();
                        }
                        String id = hotKey ? "hot" : "k" + (int) ((seq * 2654435761L) % KEYS);
                        statement.setString(1, id);
                        statement.setBytes(2, id.getBytes(StandardCharsets.UTF_8));
                        statement.setBytes(3, ("v" + seq).getBytes(StandardCharsets.UTF_8));
                        statement.setInt(4, seq);
                        started[seq] = System.nanoTime();
                        try (ResultSet resultSet = statement.executeQuery()) {
                            resultSet.next();
                        }
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

    // returns true for the stop marker
    private static boolean recordArrival(String payload, long[] latency, long[] started, long now) {
        if (payload.equals("STOP")) {
            return true;
        }
        int seq = Integer.parseInt(payload.substring(payload.lastIndexOf('|') + 1));
        latency[seq] = (now - started[seq]) / 1_000;
        return false;
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
