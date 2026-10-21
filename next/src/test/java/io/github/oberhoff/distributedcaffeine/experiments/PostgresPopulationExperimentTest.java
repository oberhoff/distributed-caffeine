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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The PostgreSQL counterpart of {@link MongoPopulationExperimentTest}: one writer, N holders of one hot key, each holder
 * with its own LISTEN connection and its own read connection (as separate instances). A holder reads the held row
 * once per batch of notifications. Also the writer alone, with and without the notification in its statement.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=PostgresPopulationExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses;
// each writer thread (read) prepares its own statement, as the adapter does
// the SQL runs against a test container, unknown to the IDE
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2699", "java:S2925", "java:S9142"})
class PostgresPopulationExperimentTest {

    private static final int WARM_UP = 200;
    private static final int WRITES = 1_000;
    private static final String VALUE = "x".repeat(1_024);

    @Test
    void measure() throws Exception {
        try (PostgreSQLContainer container = new PostgreSQLContainer("postgres:18")) {
            container.start();
            try (Connection setup = connect(container); Statement statement = setup.createStatement()) {
                statement.execute(PostgresStatementExperimentTest.SCHEMA);
            }
            StringBuilder out = new StringBuilder("\n=== population, postgres:18 ===\n")
                    .append(String.format("%-40s %10s %10s %12s %12s %10s%n", "variant", "ack p50", "ack p99",
                            "applied p50", "applied p99", "reads/write"));
            for (int round = 0; round < 2; round++) {
                out.append(run(container, "writer alone, no NOTIFY, unpaced", 0, false, 0));
                out.append(run(container, "writer alone, with NOTIFY, unpaced", 0, true, 0));
                for (int rate : new int[]{500, 1_500}) {
                    for (int holders : new int[]{1, 4, 16}) {
                        out.append(run(container, String.format("%2d holders, read held, %,d/s", holders, rate),
                                holders, true, rate));
                    }
                }
            }
            System.out.println(out);
        }
    }

    private static String run(PostgreSQLContainer container, String variant, int holderCount, boolean notify,
                              int ratePerSecond) throws Exception {
        int total = WARM_UP + WRITES;
        long[] started = new long[total];
        long[] acknowledged = new long[total];
        long[][] applied = new long[holderCount][total];
        Map<Long, Integer> seqByVersion = new ConcurrentHashMap<>();
        AtomicLong reads = new AtomicLong();
        CountDownLatch ready = new CountDownLatch(holderCount);
        List<Thread> holders = new ArrayList<>();

        for (int h = 0; h < holderCount; h++) {
            int holder = h;
            Thread thread = new Thread(() -> {
                try (Connection listener = connect(container); Connection reader = connect(container)) {
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
                        List<Long> versions = new ArrayList<>();
                        for (PGNotification notification : batch) {
                            if (notification.getParameter().equals("STOP")) {
                                stop = true;
                                continue;
                            }
                            versions.add(Long.parseLong(notification.getParameter().split("\\|", -1)[2]));
                        }
                        if (versions.isEmpty()) {
                            continue;
                        }
                        // one read for the batch, the only held key
                        try (PreparedStatement statement = reader.prepareStatement(PostgresStatementExperimentTest.READ)) {
                            statement.setArray(1, reader.createArrayOf("text", new String[]{"hot"}));
                            statement.setString(2, "1 hour");
                            try (ResultSet resultSet = statement.executeQuery()) {
                                while (resultSet.next()) {
                                    resultSet.getBytes("value");
                                }
                            }
                        }
                        reads.incrementAndGet();
                        long now = System.nanoTime();
                        for (long version : versions) {
                            Integer seq = seqByVersion.get(version);
                            if (seq != null && applied[holder][seq] == 0) {
                                applied[holder][seq] = now;
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            holders.add(thread);
            thread.start();
        }
        ready.await();
        Thread.sleep(300);

        try (Connection writer = connect(container)) {
            String sql = notify ? PostgresStatementExperimentTest.UPSERT : PostgresStatementExperimentTest.UPSERT_WITHOUT_NOTIFY;
            try (PreparedStatement statement = writer.prepareStatement(sql)) {
                long begin = System.nanoTime();
                for (int seq = 0; seq < total; seq++) {
                    if (ratePerSecond > 0) {
                        long due = begin + seq * 1_000_000_000L / ratePerSecond;
                        while (System.nanoTime() < due) {
                            Thread.onSpinWait();
                        }
                    }
                    statement.setString(1, "hot");
                    statement.setBytes(2, "hot".getBytes(StandardCharsets.UTF_8));
                    statement.setBytes(3, (seq + VALUE).getBytes(StandardCharsets.UTF_8));
                    started[seq] = System.nanoTime();
                    long version;
                    try (ResultSet resultSet = statement.executeQuery()) {
                        resultSet.next();
                        version = resultSet.getLong(1);
                    }
                    acknowledged[seq] = System.nanoTime();
                    seqByVersion.put(version, seq);
                }
            }
            // a holder may see a notification before the writer has mapped its version: wait, then stop
            Thread.sleep(2_000);
            try (Statement statement = writer.createStatement()) {
                statement.execute("NOTIFY rows, 'STOP'");
            }
        }
        for (Thread thread : holders) {
            thread.join(30_000);
        }

        long[] ack = new long[WRITES];
        long[] latency = new long[Math.max(1, WRITES * holderCount)];
        int missing = 0;
        for (int i = 0; i < WRITES; i++) {
            int seq = WARM_UP + i;
            ack[i] = (acknowledged[seq] - started[seq]) / 1_000;
            for (int h = 0; h < holderCount; h++) {
                if (applied[h][seq] == 0) {
                    missing++;
                    latency[i * holderCount + h] = Long.MAX_VALUE;
                } else {
                    latency[i * holderCount + h] = (applied[h][seq] - started[seq]) / 1_000;
                }
            }
        }
        return String.format("%-40s %10d %10d %12s %12s %10s%s%n", variant, percentile(ack, 50), percentile(ack, 99),
                holderCount == 0 ? "-" : String.valueOf(percentile(latency, 50)),
                holderCount == 0 ? "-" : String.valueOf(percentile(latency, 99)),
                holderCount == 0 ? "-" : String.format("%.2f", (double) reads.get() / total),
                missing > 0 ? "  (" + missing + " not applied)" : "");
    }

    private static Connection connect(PostgreSQLContainer container) throws Exception {
        return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword());
    }

    private static long percentile(long[] values, int percentile) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[Math.min(sorted.length - 1, sorted.length * percentile / 100)];
    }
}
