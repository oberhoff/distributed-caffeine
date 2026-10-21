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
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Should the PostgreSQL adapter use R2DBC alone (also for reads and writes) instead of JDBC plus R2DBC for listening?
 * Compares pgjdbc with HikariCP against r2dbc-postgresql with r2dbc-pool (both pools of 10) on this design's
 * statements: the upsert with {@code pg_notify}, the conditional write, and a batched read of 100 rows by
 * {@code = ANY}. Sequential latency (one caller) and throughput with 8 concurrent callers. Plus functional checks:
 * array parameters, {@code bytea}, the SQLState of a server error, and a {@code postAllocate} hook per new connection.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=PostgresJdbcVersusR2dbcExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
// the SQL runs against a test container, unknown to the IDE
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2699", "java:S2925"})
class PostgresJdbcVersusR2dbcExperimentTest {

    private static final int KEYS = 10_000;
    private static final int SEQUENTIAL = 3_000;
    private static final double SECONDS = 4;
    private static final int CONCURRENCY = 8;

    private static final String NOW_MICROS = "(extract(epoch from clock_timestamp()) * 1000000)::bigint";

    private static final String UPSERT_R2DBC = """
            WITH w AS (
                INSERT INTO rows AS r (id, key, value, state, version, write_time)
                VALUES ($1, $2, $3, 'VALUE', %s, now())
                ON CONFLICT (id) DO UPDATE SET value = excluded.value, state = 'VALUE',
                    version = greatest(r.version + 1, excluded.version), write_time = now()
                RETURNING r.id, r.version)
            SELECT w.version, pg_notify('rows', 'WRITTEN|' || w.id || '|' || w.version) FROM w""".formatted(NOW_MICROS);

    private static final String PUT_IF_VERSION_JDBC = """
            WITH w AS (
                INSERT INTO rows AS r (id, key, value, state, version, write_time)
                VALUES (?, ?, ?, 'VALUE', %s, now())
                ON CONFLICT (id) DO UPDATE SET value = excluded.value, state = 'VALUE',
                    version = greatest(r.version + 1, excluded.version), write_time = now()
                WHERE r.version = ?
                RETURNING r.id, r.version)
            SELECT w.version, pg_notify('rows', 'WRITTEN|' || w.id || '|' || w.version) FROM w""".formatted(NOW_MICROS);

    private static final String PUT_IF_VERSION_R2DBC = PUT_IF_VERSION_JDBC
            .replaceFirst("\\?", "\\$1").replaceFirst("\\?", "\\$2").replaceFirst("\\?", "\\$3")
            .replaceFirst("\\?", "\\$4");

    private static final String READ_R2DBC = """
            SELECT id, value, state, version, extract(epoch from now() - write_time) AS age
            FROM rows WHERE id = ANY ($1) AND write_time > now() - $2::interval""";

    private static final String VALUE = "x".repeat(1_024);

    @Test
    void measure() throws Exception {
        try (PostgreSQLContainer container = new PostgreSQLContainer("postgres:18")) {
            container.start();
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(container.getJdbcUrl());
            config.setUsername(container.getUsername());
            config.setPassword(container.getPassword());
            config.setMaximumPoolSize(10);
            AtomicInteger allocated = new AtomicInteger();
            try (HikariDataSource dataSource = new HikariDataSource(config)) {
                ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(
                                new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                                        .host(container.getHost())
                                        .port(container.getMappedPort(5432))
                                        .username(container.getUsername())
                                        .password(container.getPassword())
                                        .database(container.getDatabaseName())
                                        .build()))
                        .initialSize(10)
                        .maxSize(10)
                        // the standby check of the adapter would run here, once per new physical connection
                        .postAllocate(connection -> Mono.from(connection.createStatement(
                                        "SELECT pg_is_in_recovery()").execute())
                                .flatMap(result -> Mono.from(result.map((row, meta) -> row.get(0, Boolean.class))))
                                .doOnNext(recovery -> allocated.incrementAndGet())
                                .then())
                        .build());
                try {
                    try (java.sql.Connection connection = dataSource.getConnection();
                         Statement statement = connection.createStatement()) {
                        statement.execute(PostgresStatementExperimentTest.SCHEMA);
                        statement.execute("INSERT INTO rows SELECT 'k' || i, convert_to('k' || i, 'UTF8'), "
                                + "convert_to('" + VALUE + "', 'UTF8'), 'VALUE', 1, now() FROM generate_series(0, "
                                + (KEYS - 1) + ") i");
                    }
                    StringBuilder out = new StringBuilder("\n=== pgjdbc + HikariCP versus r2dbc-postgresql + r2dbc-pool, postgres:18 ===\n");
                    out.append(functional(pool, allocated));
                    out.append(String.format("%-46s %12s %12s %14s%n", "operation", "seq p50 µs", "seq p99 µs",
                            "8 conc. ops/s"));
                    for (int round = 0; round < 2; round++) {
                        out.append(row("upsert + pg_notify, JDBC", sequential(() -> jdbcUpsert(dataSource)),
                                concurrentJdbc(() -> jdbcUpsert(dataSource))));
                        out.append(row("upsert + pg_notify, R2DBC", sequential(() -> r2dbcUpsert(pool).block()),
                                concurrentR2dbc(() -> r2dbcUpsert(pool))));
                        out.append(row("conditional write (conflict), JDBC", sequential(() -> jdbcPutIfVersion(dataSource)),
                                concurrentJdbc(() -> jdbcPutIfVersion(dataSource))));
                        out.append(row("conditional write (conflict), R2DBC", sequential(() -> r2dbcPutIfVersion(pool).block()),
                                concurrentR2dbc(() -> r2dbcPutIfVersion(pool))));
                        out.append(row("read of 100 rows by = ANY, JDBC", sequential(() -> jdbcRead(dataSource)),
                                concurrentJdbc(() -> jdbcRead(dataSource))));
                        out.append(row("read of 100 rows by = ANY, R2DBC", sequential(() -> r2dbcRead(pool).block()),
                                concurrentR2dbc(() -> r2dbcRead(pool))));
                    }
                    System.out.println(out);
                } finally {
                    pool.dispose();
                }
            }
        }
    }

    private static String functional(ConnectionPool pool, AtomicInteger allocated) {
        StringBuilder out = new StringBuilder();
        // arrays and bytea round trip
        Long counted = Mono.usingWhen(pool.create(), connection -> Flux.from(connection.createStatement(READ_R2DBC)
                        .bind("$1", new String[]{"k1", "k2", "k3"}).bind("$2", "1 hour").execute())
                .flatMap(result -> result.map((row, meta) -> row.get("value", ByteBuffer.class)))
                .filter(buffer -> buffer.remaining() == VALUE.length())
                .count(), Connection::close).block();
        long rows = requireNonNull(counted);
        out.append("array parameter and bytea: ").append(rows == 3 ? "OK" : "FAIL " + rows).append('\n');
        // SQLState of a server error (unique violation)
        String sqlState;
        try {
            Mono.usingWhen(pool.create(), connection -> Mono.from(connection.createStatement(
                            "INSERT INTO rows (id, key, state, version, write_time) VALUES ('k1', '\\x00', 'VALUE', 1, now())")
                    .execute()).flatMap(result -> Mono.from(result.getRowsUpdated())), Connection::close).block();
            sqlState = "no error";
        } catch (R2dbcException e) {
            sqlState = e.getSqlState();
        }
        out.append("SQLState of a unique violation: ").append(sqlState)
                .append("23505".equals(sqlState) ? " OK" : " FAIL").append('\n');
        out.append("postAllocate hook ran for ").append(allocated.get()).append(" new connections\n");
        return out.toString();
    }

    private static String row(String name, long[] sequential, double concurrent) {
        return String.format("%-46s %12d %12d %14.0f%n", name, sequential[0], sequential[1], concurrent);
    }

    private static long[] sequential(Runnable operation) {
        for (int i = 0; i < 300; i++) {
            operation.run();
        }
        long[] latencies = new long[SEQUENTIAL];
        for (int i = 0; i < SEQUENTIAL; i++) {
            long start = System.nanoTime();
            operation.run();
            latencies[i] = (System.nanoTime() - start) / 1_000;
        }
        Arrays.sort(latencies);
        return new long[]{latencies[SEQUENTIAL / 2], latencies[SEQUENTIAL * 99 / 100]};
    }

    private static double concurrentJdbc(Runnable operation) throws Exception {
        AtomicLong done = new AtomicLong();
        long end = System.nanoTime() + (long) (SECONDS * 1e9);
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < CONCURRENCY; t++) {
            Thread thread = new Thread(() -> {
                while (System.nanoTime() < end) {
                    operation.run();
                    done.incrementAndGet();
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        return done.get() / SECONDS;
    }

    private static double concurrentR2dbc(Supplier<Mono<?>> operation) {
        AtomicLong done = new AtomicLong();
        long end = System.nanoTime() + (long) (SECONDS * 1e9);
        Flux.range(0, CONCURRENCY)
                .flatMap(lane -> Mono.defer(operation)
                        .doOnSuccess(ignored -> done.incrementAndGet())
                        .repeat(() -> System.nanoTime() < end), CONCURRENCY)
                .blockLast(Duration.ofSeconds(60));
        return done.get() / SECONDS;
    }

    private static String randomKey() {
        return "k" + ThreadLocalRandom.current().nextInt(KEYS);
    }

    private static void jdbcUpsert(HikariDataSource dataSource) {
        String id = randomKey();
        try (java.sql.Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(PostgresStatementExperimentTest.UPSERT)) {
            statement.setString(1, id);
            statement.setBytes(2, id.getBytes(StandardCharsets.UTF_8));
            statement.setBytes(3, VALUE.getBytes(StandardCharsets.UTF_8));
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Mono<Long> r2dbcUpsert(ConnectionPool pool) {
        String id = randomKey();
        return Mono.usingWhen(pool.create(), connection -> Mono.from(connection.createStatement(UPSERT_R2DBC)
                        .bind("$1", id)
                        .bind("$2", ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)))
                        .bind("$3", ByteBuffer.wrap(VALUE.getBytes(StandardCharsets.UTF_8)))
                        .execute())
                .flatMap(result -> Mono.from(result.map((row, meta) -> row.get(0, Long.class)))), Connection::close);
    }

    // expected version 0 never matches: the conflict path, which still takes the row lock and evaluates the update
    private static void jdbcPutIfVersion(HikariDataSource dataSource) {
        String id = randomKey();
        try (java.sql.Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(PUT_IF_VERSION_JDBC)) {
            statement.setString(1, id);
            statement.setBytes(2, id.getBytes(StandardCharsets.UTF_8));
            statement.setBytes(3, VALUE.getBytes(StandardCharsets.UTF_8));
            statement.setLong(4, 0L);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Mono<Long> r2dbcPutIfVersion(ConnectionPool pool) {
        String id = randomKey();
        return Mono.usingWhen(pool.create(), connection -> Flux.from(connection.createStatement(PUT_IF_VERSION_R2DBC)
                        .bind("$1", id)
                        .bind("$2", ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)))
                        .bind("$3", ByteBuffer.wrap(VALUE.getBytes(StandardCharsets.UTF_8)))
                        .bind("$4", 0L)
                        .execute())
                .flatMap(result -> result.map((row, meta) -> row.get(0, Long.class)))
                .count(), Connection::close);
    }

    private static String[] hundredKeys() {
        String[] keys = new String[100];
        int start = ThreadLocalRandom.current().nextInt(KEYS - 100);
        for (int i = 0; i < 100; i++) {
            keys[i] = "k" + (start + i);
        }
        return keys;
    }

    private static void jdbcRead(HikariDataSource dataSource) {
        try (java.sql.Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(PostgresStatementExperimentTest.READ)) {
            statement.setArray(1, connection.createArrayOf("text", hundredKeys()));
            statement.setString(2, "1 hour");
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    resultSet.getBytes("value");
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Mono<Long> r2dbcRead(ConnectionPool pool) {
        String[] keys = hundredKeys();
        return Mono.usingWhen(pool.create(), connection -> Flux.from(connection.createStatement(READ_R2DBC)
                        .bind("$1", keys).bind("$2", "1 hour").execute())
                .flatMap(result -> result.map((row, meta) -> row.get("value", ByteBuffer.class)))
                .count(), Connection::close);
    }
}
