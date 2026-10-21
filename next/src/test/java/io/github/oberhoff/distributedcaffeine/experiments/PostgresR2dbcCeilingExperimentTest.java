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
 * Connection sharing in VISION.md: how far does one R2DBC session carry the summed notifications of many caches
 * ({@code DATABASE} sharing)? One session listening on 20 channels, eight paced writers spreading writes over all
 * channels at rising total rates. Measures arrival time, lost notifications, PostgreSQL's CPU and the rate reached.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=PostgresR2dbcCeilingExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses;
// each writer thread (read) prepares its own statement, as the adapter does
// the SQL runs against a test container, unknown to the IDE
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2699", "java:S2925", "java:S9142"})
class PostgresR2dbcCeilingExperimentTest {

    private static final int CHANNELS = 20;
    private static final int KEYS = 10_000;
    private static final int WRITERS = 16;
    private static final double SECONDS = 4;

    private static final String UPSERT = """
            WITH w AS (
                INSERT INTO rows AS r (id, key, value, state, version, write_time)
                VALUES (?, ?, ?, 'VALUE', (extract(epoch from clock_timestamp()) * 1000000)::bigint, now())
                ON CONFLICT (id) DO UPDATE SET value = excluded.value, state = 'VALUE',
                    version = greatest(r.version + 1, excluded.version), write_time = now()
                RETURNING r.id, r.version)
            SELECT w.version, pg_notify(?, 'WRITTEN|' || w.id || '|' || w.version || '|' || ?) FROM w""";

    @Test
    void measure() throws Exception {
        // -DsynchronousCommit=off lifts PostgreSQL's own write limit (commit flushes), to find the session's
        String synchronousCommit = System.getProperty("synchronousCommit", "on");
        try (PostgreSQLContainer container = new PostgreSQLContainer("postgres:18")
                .withCommand("postgres", "-c", "max_connections=200", "-c", "synchronous_commit=" + synchronousCommit)) {
            container.start();
            try (Connection setup = connect(container); Statement statement = setup.createStatement()) {
                statement.execute(PostgresStatementExperimentTest.SCHEMA);
            }
            StringBuilder out = new StringBuilder("\n=== one R2DBC session, 20 channels, postgres:18, synchronous_commit=")
                    .append(synchronousCommit).append(" ===\n")
                    .append(String.format("%10s %10s %12s %12s %10s %16s%n", "target/s", "reached/s",
                            "arrival p50", "arrival p99", "lost", "PG CPU ms/1000w"));
            for (int round = 0; round < 2; round++) {
                for (int rate : new int[]{5_000, 10_000, 20_000, 40_000, 80_000}) {
                    out.append(run(container, rate));
                }
            }
            System.out.println(out);
        }
    }

    private static String run(PostgreSQLContainer container, int rate) throws Exception {
        int total = (int) (rate * SECONDS);
        long[] started = new long[total];
        long[] latency = new long[total];
        CountDownLatch stopped = new CountDownLatch(1);

        PostgresqlConnection listener = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(container.getHost())
                .port(container.getMappedPort(5432))
                .username(container.getUsername())
                .password(container.getPassword())
                .database(container.getDatabaseName())
                .build()).create().block();
        requireNonNull(listener);
        for (int channel = 0; channel < CHANNELS; channel++) {
            listener.createStatement("LISTEN c_" + channel).execute()
                    .flatMap(PostgresqlResult::getRowsUpdated).then().block();
        }
        Disposable subscription = listener.getNotifications().subscribe(notification -> {
            long now = System.nanoTime();
            String payload = requireNonNull(notification.getParameter());
            if (payload.equals("STOP")) {
                stopped.countDown();
                return;
            }
            int seq = Integer.parseInt(payload.substring(payload.lastIndexOf('|') + 1));
            latency[seq] = (now - started[seq]) / 1_000;
        });
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
                        String id = "k" + (int) ((seq * 2654435761L) % KEYS);
                        statement.setString(1, id);
                        statement.setBytes(2, id.getBytes(StandardCharsets.UTF_8));
                        statement.setBytes(3, ("v" + seq).getBytes(StandardCharsets.UTF_8));
                        statement.setString(4, "c_" + (seq % CHANNELS));
                        statement.setInt(5, seq);
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
            statement.execute("NOTIFY c_0, 'STOP'");
        }
        assertThat(stopped.await(60, TimeUnit.SECONDS)).as("all notifications seen").isTrue();
        subscription.dispose();
        listener.close().block();

        int lost = 0;
        long[] sorted = new long[total];
        for (int seq = 0; seq < total; seq++) {
            if (latency[seq] == 0) {
                lost++;
                sorted[seq] = Long.MAX_VALUE;
            } else {
                sorted[seq] = latency[seq];
            }
        }
        Arrays.sort(sorted);
        return String.format("%10d %10.0f %10.1f ms %10.1f ms %10d %16.0f%n", rate, reached,
                sorted[total / 2] / 1_000.0, sorted[Math.min(total - 1, total * 99 / 100)] / 1_000.0, lost,
                (cpuAfter - cpuBefore) / 1_000.0 / (total / 1_000.0));
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
