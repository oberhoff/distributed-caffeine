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
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Today's statements against those of the logical {@code invalidateAll} (VISION.md, "invalidateAll, logical"): the
 * read and {@code readVersions} with the lookup of the control row, the put with {@code nextval} instead of
 * {@code clock_timestamp()}, and {@code invalidateAll} as one row instead of the sweep. Same table, same data, the
 * variants alternating in rounds.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=PostgresLogicalInvalidateAllExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it
// the SQL runs against a test container, unknown to the IDE
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2699"})
class PostgresLogicalInvalidateAllExperimentTest {

    private static final String NOW_MICROS = "(extract(epoch from clock_timestamp()) * 1000000)::bigint";
    private static final String NEXT = "nextval('dc_versions')";
    private static final String CONTROL = "coalesce((SELECT version FROM t WHERE discriminator = 'd' "
            + "AND id = '#invalidated'), 0)";
    private static final int KEYS = 10_000;
    private static final int OPERATIONS = 4_000;
    private static final int ROUNDS = 3;
    private static final int WRITERS = 16;

    private static final String READ_TODAY = """
            SELECT r.id, r.value, r.state, r.version, n.now
            FROM (SELECT %s AS now) AS n LEFT JOIN t AS r
                ON r.discriminator = 'd' AND r.id = ANY (?) AND (r.state <> 'VALUE' OR r.write_time > now() - interval
                '1 hour')""".formatted(NOW_MICROS);
    private static final String READ_LOGICAL = """
            SELECT r.id, r.value, r.state, r.version, n.now
            FROM (SELECT (SELECT last_value FROM dc_versions) + 1 AS now, %s AS invalidated) AS n LEFT JOIN t AS r
                ON r.discriminator = 'd' AND r.id = ANY (?) AND r.version > n.invalidated
                AND (r.state <> 'VALUE' OR r.write_time > now() - interval '1 hour')""".formatted(CONTROL);
    private static final String VERSIONS_TODAY = """
            SELECT id, version FROM t WHERE discriminator = 'd' AND id = ANY (?) AND state = 'VALUE'
                AND write_time > now() - interval '1 hour'""";
    private static final String VERSIONS_LOGICAL = VERSIONS_TODAY + " AND version > " + CONTROL;
    private static final String PUT = """
            WITH w AS (
                INSERT INTO t AS r (discriminator, id, key, value, state, version, write_time)
                VALUES ('d', ?, ?, ?, 'VALUE', %s, now())
                ON CONFLICT (discriminator, id) DO UPDATE SET key = excluded.key, value = excluded.value,
                    state = 'VALUE', version = greatest(r.version + 1, excluded.version), write_time = now()
                RETURNING r.id, r.version)
            SELECT w.version, pg_notify('c', 'WRITTEN|' || w.id || '|' || w.version) FROM w""";
    private static final String PUT_TODAY = PUT.formatted(NOW_MICROS);
    private static final String PUT_LOGICAL = PUT.formatted(NEXT);
    private static final String INVALIDATE_ALL_TODAY = """
            WITH n AS (SELECT %s AS now),
            w AS (
                UPDATE t AS r SET value = NULL,
                    state = CASE WHEN r.state = 'VALUE' THEN 'TOMBSTONE' ELSE r.state END,
                    version = greatest(r.version + 1, (SELECT now FROM n)),
                    write_time = CASE WHEN r.state = 'VALUE' THEN now() ELSE r.write_time END
                WHERE r.discriminator = 'd'
                RETURNING 1)
            SELECT n.now, pg_notify('c', 'INVALIDATED_ALL||' || n.now)
            FROM n, (SELECT count(*) FROM w) AS c""".formatted(NOW_MICROS);
    private static final String INVALIDATE_ALL_LOGICAL = """
            WITH w AS (
                INSERT INTO t AS r (discriminator, id, state, version, write_time)
                VALUES ('d', '#invalidated', 'CONTROL', %s, now())
                ON CONFLICT (discriminator, id) DO UPDATE SET version = greatest(r.version + 1, excluded.version)
                RETURNING r.version)
            SELECT w.version, pg_notify('c', 'INVALIDATED_ALL||' || w.version) FROM w""".formatted(NEXT);

    @FunctionalInterface
    private interface Operation {
        void run(Connection connection, int index) throws SQLException;
    }

    @ParameterizedTest
    @ValueSource(strings = {"postgres:18"})
    void measure(String image) throws Exception {
        try (PostgreSQLContainer container = new PostgreSQLContainer(image)) {
            container.start();
            String url = container.getJdbcUrl();
            try (Connection setup = DriverManager.getConnection(url, container.getUsername(),
                    container.getPassword())) {
                schema(setup);
                System.out.printf("%s, %d keys, %d operations per variant and round, %d rounds%n", image, KEYS,
                        OPERATIONS, ROUNDS);
                fill(setup, KEYS);
                explain(setup, "read, 100 keys today", READ_TODAY);
                explain(setup, "read, 100 keys logical", READ_LOGICAL);
                for (int round = 1; round <= ROUNDS; round++) {
                    fill(setup, KEYS);
                    single(url, container, "read, 1 key       today  ", read(READ_TODAY, 1));
                    single(url, container, "read, 1 key       logical", read(READ_LOGICAL, 1));
                    single(url, container, "read, 100 ids     today  ", read(READ_TODAY.replace("r.value,", ""), 100));
                    single(url, container, "read, 100 keys    today  ", read(READ_TODAY, 100));
                    single(url, container, "read, 100 keys    logical", read(READ_LOGICAL, 100));
                    single(url, container, "readVersions, 100 today  ", read(VERSIONS_TODAY, 100));
                    single(url, container, "readVersions, 100 logical", read(VERSIONS_LOGICAL, 100));
                    single(url, container, "put, 1 writer     today  ", put(PUT_TODAY));
                    single(url, container, "put, 1 writer     logical", put(PUT_LOGICAL));
                    parallel(url, container, "put, 16 writers   today  ", put(PUT_TODAY));
                    parallel(url, container, "put, 16 writers   logical", put(PUT_LOGICAL));
                }
                for (int rows : new int[]{10_000, 100_000}) {
                    fill(setup, rows);
                    long today = once(setup, INVALIDATE_ALL_TODAY);
                    fill(setup, rows);
                    long logical = once(setup, INVALIDATE_ALL_LOGICAL);
                    System.out.printf("invalidateAll, %,d rows: today %,d µs, logical %,d µs%n", rows, today,
                            logical);
                }
            }
        }
    }

    private static void schema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE t (
                        discriminator text NOT NULL,
                        id text NOT NULL,
                        key bytea,
                        value bytea,
                        state text NOT NULL,
                        version bigint NOT NULL,
                        write_time timestamptz NOT NULL,
                        PRIMARY KEY (discriminator, id))""");
            statement.execute("CREATE INDEX t_maintenance ON t (discriminator, state, write_time)");
            statement.execute("CREATE SEQUENCE dc_versions START WITH " + System.currentTimeMillis() * 1_000);
        }
    }

    // the keys as fresh value rows, and the control row of an earlier invalidateAll (so that the lookup finds one)
    private static void fill(Connection connection, int rows) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE t");
            statement.execute("""
                    INSERT INTO t (discriminator, id, key, value, state, version, write_time)
                    SELECT 'd', 'k' || i, convert_to('k' || i, 'UTF8'), convert_to(repeat('v', 100), 'UTF8'), 'VALUE',
                        %s, now() FROM generate_series(1, %d) AS i""".formatted(NEXT, rows));
            statement.execute("INSERT INTO t VALUES ('d', '#invalidated', NULL, NULL, 'CONTROL', 1, now())");
            statement.execute("VACUUM ANALYZE t");
        }
    }

    private static Operation read(String sql, int keys) {
        return (connection, index) -> {
            String[] ids = new String[keys];
            for (int i = 0; i < keys; i++) {
                ids[i] = "k" + (1 + (index * 7919L + i) % KEYS);
            }
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setArray(1, connection.createArrayOf("text", ids));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        resultSet.getObject(2);
                    }
                }
            }
        };
    }

    private static Operation put(String sql) {
        byte[] value = "v".repeat(100).getBytes(StandardCharsets.UTF_8);
        return (connection, index) -> {
            String id = "k" + (1 + index % KEYS);
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, id);
                statement.setBytes(2, id.getBytes(StandardCharsets.UTF_8));
                statement.setBytes(3, value);
                try (ResultSet resultSet = statement.executeQuery()) {
                    resultSet.next();
                }
            }
        };
    }

    // latency of one caller: p50 and p99
    private static void single(String url, PostgreSQLContainer container, String label, Operation operation)
            throws SQLException {
        try (Connection connection = DriverManager.getConnection(url, container.getUsername(),
                container.getPassword())) {
            for (int i = 0; i < OPERATIONS / 4; i++) {
                operation.run(connection, i);
            }
            long[] nanos = new long[OPERATIONS];
            for (int i = 0; i < OPERATIONS; i++) {
                long start = System.nanoTime();
                operation.run(connection, i);
                nanos[i] = System.nanoTime() - start;
            }
            Arrays.sort(nanos);
            System.out.printf("  %s  p50 %4d µs  p99 %5d µs%n", label, nanos[OPERATIONS / 2] / 1_000,
                    nanos[OPERATIONS * 99 / 100] / 1_000);
        }
    }

    // throughput of concurrent callers, each on its own connection and keys
    private static void parallel(String url, PostgreSQLContainer container, String label, Operation operation)
            throws Exception {
        int callers = WRITERS;
        List<Connection> connections = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(callers);
        try {
            for (int i = 0; i < callers; i++) {
                connections.add(DriverManager.getConnection(url, container.getUsername(), container.getPassword()));
            }
            long start = System.nanoTime();
            List<Future<?>> futures = new ArrayList<>();
            for (int caller = 0; caller < callers; caller++) {
                Connection connection = connections.get(caller);
                int offset = caller * (KEYS / callers);
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < OPERATIONS; i++) {
                        operation.run(connection, offset + i % (KEYS / callers));
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
            long elapsed = System.nanoTime() - start;
            System.out.printf("  %s  %,7d puts/s%n", label, (long) callers * OPERATIONS * 1_000_000_000L / elapsed);
        } finally {
            executor.shutdown();
            for (Connection connection : connections) {
                connection.close();
            }
        }
    }

    // the plan the server picks once the prepared statement went generic (after five executions)
    private static void explain(Connection connection, String label, String sql) throws SQLException {
        String[] ids = new String[100];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = "k" + (1 + i * 97);
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET plan_cache_mode = force_generic_plan");
        }
        try (PreparedStatement statement = connection.prepareStatement("EXPLAIN (ANALYZE, COSTS OFF) " + sql)) {
            statement.setArray(1, connection.createArrayOf("text", ids));
            System.out.println(label + ", generic plan:");
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    System.out.println("    " + resultSet.getString(1));
                }
            }
        } finally {
            try (Statement statement = connection.createStatement()) {
                statement.execute("RESET plan_cache_mode");
            }
        }
    }

    private static long once(Connection connection, String sql) throws SQLException {
        long start = System.nanoTime();
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
        }
        return (System.nanoTime() - start) / 1_000;
    }
}
