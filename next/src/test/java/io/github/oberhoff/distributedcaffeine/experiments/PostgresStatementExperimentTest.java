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

import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The PostgreSQL statements of VISION.md, each a single statement that writes and notifies: the upsert with the store
 * assigned version, the conditional write (ordering rule 6), tombstones, expiry and the read of held keys. Checks:
 * versions increase strictly and are at least the server time in microseconds; notifications arrive only on commit;
 * exactly one of concurrent conditional writes with the same expected version wins (also creating a missing row);
 * tombstones and expiry notify with their kind; two concurrent expiries notify each row exactly once; the read filters
 * by maximum age and returns the age.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=PostgresStatementExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
// the SQL runs against a test container, unknown to the IDE
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2699", "java:S2925"})
class PostgresStatementExperimentTest {

    static final String SCHEMA = """
            CREATE TABLE rows (
                id text PRIMARY KEY,
                key bytea NOT NULL,
                value bytea,
                state text NOT NULL,
                version bigint NOT NULL,
                write_time timestamptz NOT NULL)""";

    private static final String NOW_MICROS = "(extract(epoch from clock_timestamp()) * 1000000)::bigint";

    static final String UPSERT = """
            WITH w AS (
                INSERT INTO rows AS r (id, key, value, state, version, write_time)
                VALUES (?, ?, ?, 'VALUE', %s, now())
                ON CONFLICT (id) DO UPDATE SET value = excluded.value, state = 'VALUE',
                    version = greatest(r.version + 1, excluded.version), write_time = now()
                RETURNING r.id, r.version)
            SELECT w.version, pg_notify('rows', 'WRITTEN|' || w.id || '|' || w.version) FROM w""".formatted(NOW_MICROS);

    static final String UPSERT_WITHOUT_NOTIFY = """
            INSERT INTO rows AS r (id, key, value, state, version, write_time)
            VALUES (?, ?, ?, 'VALUE', %s, now())
            ON CONFLICT (id) DO UPDATE SET value = excluded.value, state = 'VALUE',
                version = greatest(r.version + 1, excluded.version), write_time = now()
            RETURNING r.version""".formatted(NOW_MICROS);

    // written only if the row still has the expected version
    private static final String PUT_IF_VERSION = """
            WITH w AS (
                INSERT INTO rows AS r (id, key, value, state, version, write_time)
                VALUES (?, ?, ?, 'VALUE', %s, now())
                ON CONFLICT (id) DO UPDATE SET value = excluded.value, state = 'VALUE',
                    version = greatest(r.version + 1, excluded.version), write_time = now()
                WHERE r.version = ?
                RETURNING r.id, r.version)
            SELECT w.version, pg_notify('rows', 'WRITTEN|' || w.id || '|' || w.version) FROM w""".formatted(NOW_MICROS);

    // written only if the row is still missing
    private static final String PUT_IF_MISSING = """
            WITH w AS (
                INSERT INTO rows AS r (id, key, value, state, version, write_time)
                VALUES (?, ?, ?, 'VALUE', %s, now())
                ON CONFLICT (id) DO NOTHING
                RETURNING r.id, r.version)
            SELECT w.version, pg_notify('rows', 'WRITTEN|' || w.id || '|' || w.version) FROM w""".formatted(NOW_MICROS);

    private static final String INVALIDATE = """
            WITH w AS (
                UPDATE rows AS r SET value = NULL, state = 'TOMBSTONE',
                    version = greatest(r.version + 1, %s), write_time = now()
                WHERE r.id = ANY (?)
                RETURNING r.id, r.version)
            SELECT w.version, pg_notify('rows', 'INVALIDATED|' || w.id || '|' || w.version) FROM w""".formatted(NOW_MICROS);

    private static final String EXPIRE = """
            WITH w AS (
                UPDATE rows AS r SET value = NULL, state = 'EXPIRED',
                    version = greatest(r.version + 1, %s)
                WHERE r.state = 'VALUE' AND r.write_time < now() - ?::interval
                RETURNING r.id, r.version)
            SELECT w.version, pg_notify('rows', 'EXPIRED|' || w.id || '|' || w.version) FROM w""".formatted(NOW_MICROS);

    static final String READ = """
            SELECT id, value, state, version, extract(epoch from now() - write_time) AS age
            FROM rows WHERE id = ANY (?) AND write_time > now() - ?::interval""";

    @ParameterizedTest
    @ValueSource(strings = {"postgres:13", "postgres:14", "postgres:15", "postgres:16", "postgres:17", "postgres:18"})
    void check(String image) throws Exception {
        try (PostgreSQLContainer container = new PostgreSQLContainer(image)) {
            container.start();
            String url = container.getJdbcUrl();
            List<String> results = new ArrayList<>();
            try (Connection writer = DriverManager.getConnection(url, container.getUsername(), container.getPassword());
                 Connection listener = DriverManager.getConnection(url, container.getUsername(), container.getPassword())) {
                try (Statement statement = writer.createStatement()) {
                    statement.execute(SCHEMA);
                }
                try (Statement statement = listener.createStatement()) {
                    statement.execute("LISTEN rows");
                }
                PGConnection notifications = listener.unwrap(PGConnection.class);

                // 1. versions strictly increase and are at least the server time in microseconds
                long previous = 0;
                boolean increasing = true;
                boolean atLeastServerTime = true;
                for (int i = 0; i < 1_000; i++) {
                    long before = serverMicros(writer);
                    long version = upsert(writer, "k1", "v" + i);
                    increasing &= version > previous;
                    atLeastServerTime &= version >= before;
                    previous = version;
                }
                drain(notifications, 2_000);
                results.add(check("1,000 upserts: versions strictly increasing", increasing));
                results.add(check("1,000 upserts: version >= server time in µs before the write", atLeastServerTime));

                // 2. notifications only on commit
                writer.setAutoCommit(false);
                upsert(writer, "k2", "uncommitted");
                List<String> beforeCommit = drain(notifications, 300);
                writer.commit();
                List<String> afterCommit = drain(notifications, 1_000);
                upsert(writer, "k2", "rolled back");
                writer.rollback();
                List<String> afterRollback = drain(notifications, 500);
                writer.setAutoCommit(true);
                results.add(check("no notification before commit", beforeCommit.isEmpty()));
                results.add(check("one notification after commit", afterCommit.size() == 1));
                results.add(check("no notification after rollback", afterRollback.isEmpty()));

                // 3. concurrent conditional writes with the same expected version: exactly one wins
                long expected = upsert(writer, "k3", "base");
                drain(notifications, 500);
                int winners = race(url, container, 8, connection -> putIfVersion(connection, expected));
                List<String> raceNotifications = drain(notifications, 1_000);
                results.add(check("8 concurrent putIfVersion, same expected version: exactly one wins (" + winners + ")",
                        winners == 1));
                results.add(check("... and exactly one notification (" + raceNotifications.size() + ")",
                        raceNotifications.size() == 1));
                int creators = race(url, container, 8, PostgresStatementExperimentTest::putIfMissing);
                List<String> createNotifications = drain(notifications, 1_000);
                results.add(check("8 concurrent putIfMissing: exactly one wins (" + creators + ")", creators == 1));
                results.add(check("... and exactly one notification (" + createNotifications.size() + ")",
                        createNotifications.size() == 1));

                // 4. tombstones notify with their kind, one per row
                for (int i = 0; i < 5; i++) {
                    upsert(writer, "t" + i, "v");
                }
                drain(notifications, 500);
                try (PreparedStatement statement = writer.prepareStatement(INVALIDATE)) {
                    statement.setArray(1, writer.createArrayOf("text", new String[]{"t0", "t1", "t2", "t3", "t4"}));
                    statement.executeQuery().close();
                }
                List<String> tombstones = drain(notifications, 1_000);
                results.add(check("invalidating 5 rows in one statement: 5 INVALIDATED notifications (" + tombstones.size() + ")",
                        tombstones.size() == 5 && tombstones.stream().allMatch(n -> n.startsWith("INVALIDATED|"))));

                // 5. two concurrent expiries notify each expired row exactly once
                try (Statement statement = writer.createStatement()) {
                    statement.execute("UPDATE rows SET write_time = now() - interval '2 hours' WHERE state = 'VALUE'");
                }
                int expiredRows = countValues(writer);
                drain(notifications, 500);
                int expiredByBoth = race(url, container, 2, PostgresStatementExperimentTest::expire);
                List<String> expired = drain(notifications, 1_000);
                results.add(check("2 concurrent expiries of " + expiredRows + " rows: " + expiredByBoth
                        + " rows expired in total, " + expired.size() + " EXPIRED notifications",
                        expiredByBoth == expiredRows && expired.size() == expiredRows
                                && expired.stream().allMatch(n -> n.startsWith("EXPIRED|"))));

                // 6. the read filters by maximum age and returns the age
                upsert(writer, "fresh", "v");
                try (Statement statement = writer.createStatement()) {
                    statement.execute("UPDATE rows SET write_time = now() - interval '10 minutes' WHERE id = 'k1'");
                }
                List<String> read = new ArrayList<>();
                double freshAge = -1;
                try (PreparedStatement statement = writer.prepareStatement(READ)) {
                    statement.setArray(1, writer.createArrayOf("text", new String[]{"k1", "fresh"}));
                    statement.setString(2, "5 minutes");
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) {
                            read.add(resultSet.getString("id"));
                            if (resultSet.getString("id").equals("fresh")) {
                                freshAge = resultSet.getDouble("age");
                            }
                        }
                    }
                }
                results.add(check("read with maximum age 5 min: the 10 min old row is filtered, the fresh one returned "
                        + read, read.equals(List.of("fresh"))));
                results.add(check("... with its age in seconds (" + freshAge + ")", freshAge >= 0 && freshAge < 5));
            }
            System.out.println("\n=== statements, " + image + " ===\n" + String.join("\n", results) + "\n");
        }
    }

    static long upsert(Connection connection, String id, String value) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(UPSERT)) {
            statement.setString(1, id);
            statement.setBytes(2, id.getBytes(StandardCharsets.UTF_8));
            statement.setBytes(3, value.getBytes(StandardCharsets.UTF_8));
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }

    // the racers' write of k3
    private static int putIfVersion(Connection connection, long expected) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(PUT_IF_VERSION)) {
            statement.setString(1, "k3");
            statement.setBytes(2, "k3".getBytes(StandardCharsets.UTF_8));
            statement.setBytes(3, "racer".getBytes(StandardCharsets.UTF_8));
            statement.setLong(4, expected);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? 1 : 0;
            }
        }
    }

    // the creators' write of k4
    private static int putIfMissing(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(PUT_IF_MISSING)) {
            statement.setString(1, "k4");
            statement.setBytes(2, "k4".getBytes(StandardCharsets.UTF_8));
            statement.setBytes(3, "creator".getBytes(StandardCharsets.UTF_8));
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? 1 : 0;
            }
        }
    }

    // rows older than an hour
    private static int expire(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(EXPIRE)) {
            statement.setString(1, "1 hour");
            int rows = 0;
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    rows++;
                }
            }
            return rows;
        }
    }

    private interface Racer {
        int run(Connection connection) throws SQLException;
    }

    // every racer on its own connection, all released at once; returns the sum of their results
    private static int race(String url, PostgreSQLContainer container, int racers, Racer racer) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(racers);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Integer>> futures = new ArrayList<>();
            List<Connection> connections = new ArrayList<>();
            for (int i = 0; i < racers; i++) {
                connections.add(DriverManager.getConnection(url, container.getUsername(), container.getPassword()));
            }
            for (Connection connection : connections) {
                Callable<Integer> task = () -> {
                    start.await();
                    return racer.run(connection);
                };
                futures.add(executor.submit(task));
            }
            start.countDown();
            int sum = 0;
            for (Future<Integer> future : futures) {
                sum += future.get();
            }
            for (Connection connection : connections) {
                connection.close();
            }
            return sum;
        } finally {
            executor.shutdownNow();
        }
    }

    private static long serverMicros(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT " + NOW_MICROS)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private static int countValues(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM rows WHERE state = 'VALUE'")) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    static List<String> drain(PGConnection connection, int waitMillis) throws SQLException {
        List<String> payloads = new ArrayList<>();
        long deadline = System.currentTimeMillis() + waitMillis;
        while (System.currentTimeMillis() < deadline) {
            PGNotification[] batch = connection.getNotifications(50);
            if (batch != null) {
                for (PGNotification notification : batch) {
                    payloads.add(notification.getParameter());
                }
            }
        }
        return payloads;
    }

    private static String check(String what, boolean ok) {
        return (ok ? "OK    " : "FAIL  ") + what;
    }
}
