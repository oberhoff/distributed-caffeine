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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.testcontainers.postgresql.PostgreSQLContainer;

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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Open question 1 of VISION.md: does partitioning notifications over several channels, each with its own LISTEN
 * session per instance, keep propagation prompt above the ~750/s per-session limit of pgjdbc? 10,000 keys written at
 * random by two paced writers; the channel of a write is the bucket of its key. 4 instances, each with one session
 * per channel. Measures the time until a notification arrives (over all instances) and PostgreSQL's CPU from the
 * container's cgroup counters.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=PostgresChannelPartitioningExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses;
// each writer thread (read) prepares its own statement, as the adapter does
// the SQL runs against a test container, unknown to the IDE
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2699", "java:S2925", "java:S9142"})
class PostgresChannelPartitioningExperimentTest {

    private static final int KEYS = 10_000;
    private static final int INSTANCES = 4;
    private static final int WRITERS = 2;
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
        try (PostgreSQLContainer container = new PostgreSQLContainer("postgres:18")
                .withCommand("postgres", "-c", "max_connections=200")) {
            container.start();
            try (Connection setup = connect(container); Statement statement = setup.createStatement()) {
                statement.execute(PostgresStatementExperimentTest.SCHEMA);
            }
            StringBuilder out = new StringBuilder("\n=== channel partitioning, postgres:18, 4 instances, 10,000 keys ===\n")
                    .append(String.format("%-10s %10s %10s %12s %12s %16s%n", "channels", "writes/s", "sessions",
                            "arrival p50", "arrival p99", "PG CPU ms/1000w"));
            for (int rate : new int[]{1_500, 3_000, 6_000}) {
                for (int channels : new int[]{1, 2, 4, 8, 1, 2, 4, 8}) {
                    out.append(run(container, channels, rate));
                }
            }
            System.out.println(out);
        }
    }

    private static String run(PostgreSQLContainer container, int channels, int rate) throws Exception {
        int total = (int) (rate * SECONDS);
        long[] started = new long[total];
        List<long[]> latencies = new ArrayList<>();
        AtomicInteger stopped = new AtomicInteger();
        int sessions = INSTANCES * channels;
        CountDownLatch ready = new CountDownLatch(sessions);
        List<Thread> listeners = new ArrayList<>();

        for (int instance = 0; instance < INSTANCES; instance++) {
            for (int channel = 0; channel < channels; channel++) {
                String name = "rows_" + channel;
                long[] latency = new long[total];
                latencies.add(latency);
                Thread thread = new Thread(() -> {
                    try (Connection listener = connect(container)) {
                        try (Statement statement = listener.createStatement()) {
                            statement.execute("LISTEN " + name);
                        }
                        PGConnection notifications = listener.unwrap(PGConnection.class);
                        ready.countDown();
                        while (true) {
                            PGNotification[] batch = notifications.getNotifications(0);
                            if (batch == null) {
                                continue;
                            }
                            long now = System.nanoTime();
                            boolean stop = false;
                            for (PGNotification notification : batch) {
                                String payload = notification.getParameter();
                                if (payload.equals("STOP")) {
                                    stop = true;
                                    continue;
                                }
                                int seq = Integer.parseInt(payload.substring(payload.lastIndexOf('|') + 1));
                                latency[seq] = (now - started[seq]) / 1_000;
                            }
                            if (stop) {
                                break;
                            }
                        }
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    } finally {
                        stopped.incrementAndGet();
                    }
                });
                listeners.add(thread);
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
            Thread writer = new Thread(() -> {
                try (Connection connection = connect(container);
                     PreparedStatement statement = connection.prepareStatement(UPSERT)) {
                    for (int seq = next.getAndIncrement(); seq < total; seq = next.getAndIncrement()) {
                        long due = begin + (long) seq * 1_000_000_000L / rate;
                        while (System.nanoTime() < due) {
                            Thread.onSpinWait();
                        }
                        int key = (int) ((seq * 2654435761L) % KEYS);
                        String id = "k" + key;
                        statement.setString(1, id);
                        statement.setBytes(2, id.getBytes(StandardCharsets.UTF_8));
                        statement.setBytes(3, ("v" + seq).getBytes(StandardCharsets.UTF_8));
                        statement.setString(4, "rows_" + Math.floorMod(id.hashCode(), channels));
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
        double achieved = total / ((System.nanoTime() - begin) / 1e9);
        Thread.sleep(3_000);
        long cpuAfter = postgresCpuMicros(container);
        try (Connection connection = connect(container); Statement statement = connection.createStatement()) {
            for (int channel = 0; channel < channels; channel++) {
                statement.execute("NOTIFY rows_" + channel + ", 'STOP'");
            }
        }
        for (Thread listener : listeners) {
            listener.join(30_000);
        }

        long[] all = new long[total * INSTANCES];
        int index = 0;
        int missing = 0;
        // per instance, every seq arrives on exactly one of its channels
        for (int instance = 0; instance < INSTANCES; instance++) {
            for (int seq = 0; seq < total; seq++) {
                long value = 0;
                for (int channel = 0; channel < channels; channel++) {
                    value = Math.max(value, latencies.get(instance * channels + channel)[seq]);
                }
                if (value == 0) {
                    missing++;
                    value = Long.MAX_VALUE;
                }
                all[index++] = value;
            }
        }
        Arrays.sort(all);
        return String.format("%-10d %10.0f %10d %10.1f ms %10.1f ms %16.0f%s%n", channels, achieved, sessions,
                all[all.length / 2] / 1_000.0, all[Math.min(all.length - 1, all.length * 99 / 100)] / 1_000.0,
                (cpuAfter - cpuBefore) / 1_000.0 / (total / 1_000.0),
                missing > 0 ? "  (" + missing + " missing)" : "");
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
