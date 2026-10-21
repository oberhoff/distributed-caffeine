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

import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.ExpectedEntry;
import io.github.oberhoff.distributedcaffeine.spi.Format;
import io.github.oberhoff.distributedcaffeine.spi.Listener;
import io.github.oberhoff.distributedcaffeine.spi.ReadResult;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.Subscription;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import io.r2dbc.spi.ConnectionFactory;
import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.WeakHashMap;


/**
 * The store of one cache instance on PostgreSQL (VISION.md, "PostgreSQL: statements and propagation measured"). Every
 * write is one statement that writes and notifies ({@code pg_notify} over the rows a CTE returns), so a notification
 * exists exactly when the write committed; the version is {@code greatest(old + 1, server time in microseconds)}.
 * Batches are written in the order of their hashes, so concurrent batches lock rows in the same order.
 * <p>
 * Failures (VISION.md, "Write retries"): one attempt each, classified for the core, which retries: a statement the
 * server rejected (deadlock, serialization failure, lock not available) or a connection that could not be obtained
 * is retryable and certainly not applied; a connection lost during a statement is retryable with a write's outcome
 * unknown; a statement without an answer within the timeout is not retryable, a write's outcome unknown.
 */
// the SQL runs against the user's database, unknown to the IDE; what is formatted into it are the quoted table name,
// placeholders and fixed expressions, never values, which are always bound
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2077"})
class PostgresStore implements Store {

    private static final System.Logger LOGGER = System.getLogger("io.github.oberhoff.distributedcaffeine.postgresql");
    private static final int CHUNK = 1_000;
    private static final String NOW_MICROS = "(extract(epoch from clock_timestamp()) * 1000000)::bigint";
    private static final String AGE_MICROS = "(extract(epoch from (now() - write_time)) * 1000000)::bigint";
    private static final String YOUNGER = "write_time > now() - (?::bigint * interval '1 microsecond')";
    // physical connections checked for not being a standby, once each (weakly held, as the pool's connections come
    // and go)
    private static final Map<Object, Boolean> PRIMARIES = Collections.synchronizedMap(new WeakHashMap<>());

    private enum Kind {
        READ, WRITE
    }

    private interface Work<T> {
        T run(Connection connection) throws SQLException;
    }

    private final DataSource dataSource;
    private final PostgresTable table;
    private final Duration timeout;
    private final String t;

    private PostgresStore(DataSource dataSource, PostgresTable table, Duration timeout) {
        this.dataSource = dataSource;
        this.table = table;
        this.timeout = timeout;
        this.t = table.qualified;
    }

    // with a listener, a store announcing its writes by LISTEN/NOTIFY; without, one supporting only
    // DistributionMode.NONE
    static PostgresStore of(DataSource dataSource, PostgresTable table,
                            @Nullable ConnectionFactory listenerConnectionFactory, Duration timeout) {
        return listenerConnectionFactory == null ? new PostgresStore(dataSource, table, timeout)
                : new Listening(dataSource, table, listenerConnectionFactory, timeout);
    }

    // reads

    // the rows and the server's time, in one statement: the time comes back even if no row matches
    @Override
    public ReadResult read(Collection<String> hashes, Duration maxAge) {
        return run(Kind.READ, connection -> {
            Map<String, Row> rows = new LinkedHashMap<>();
            long time = 0;
            try (PreparedStatement statement = connection.prepareStatement("""
                    SELECT r.id, r.key, r.value, r.state, r.version, %s, n.now
                    FROM (SELECT %s AS now) AS n LEFT JOIN %s AS r
                        ON r.discriminator = ? AND r.id = ANY (?) AND (r.state <> 'VALUE' OR %s)"""
                    .formatted(AGE_MICROS.replace("write_time", "r.write_time"), NOW_MICROS, t,
                            YOUNGER.replace("write_time", "r.write_time")))) {
                statement.setString(1, table.discriminator);
                statement.setArray(2, connection.createArrayOf("text", hashes.toArray()));
                statement.setLong(3, micros(maxAge));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        time = resultSet.getLong(7);
                        if (resultSet.getString(1) != null) {
                            Row row = row(resultSet);
                            rows.put(row.hash(), row);
                        }
                    }
                }
            }
            return new ReadResult(rows, time);
        });
    }

    @Override
    public Map<String, Long> readVersions(Collection<String> hashes, Duration maxAge) {
        if (hashes.isEmpty()) {
            return Map.of();
        }
        return run(Kind.READ, connection -> {
            Map<String, Long> versions = new LinkedHashMap<>();
            try (PreparedStatement statement = connection.prepareStatement("""
                    SELECT id, version FROM %s
                    WHERE discriminator = ? AND id = ANY (?) AND state = 'VALUE' AND %s""".formatted(t, YOUNGER))) {
                statement.setString(1, table.discriminator);
                statement.setArray(2, connection.createArrayOf("text", hashes.toArray()));
                statement.setLong(3, micros(maxAge));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        versions.put(resultSet.getString(1), resultSet.getLong(2));
                    }
                }
            }
            return versions;
        });
    }

    // writes

    @Override
    public Map<String, Long> put(Collection<Entry> entries) {
        Map<String, Long> versions = new LinkedHashMap<>();
        try {
            for (List<Entry> chunk : chunks(sorted(entries))) {
                String sql = """
                        WITH i (id, key, value) AS (VALUES %s),
                        w AS (
                            INSERT INTO %s AS r (discriminator, id, key, value, state, version, write_time)
                            SELECT ?, i.id, i.key, i.value, 'VALUE', %s, now() FROM i ORDER BY i.id
                            ON CONFLICT (discriminator, id) DO UPDATE SET key = excluded.key, value = excluded.value,
                                state = 'VALUE', version = greatest(r.version + 1, excluded.version),
                                write_time = excluded.write_time
                            RETURNING r.id, r.version)
                        SELECT w.id, w.version, pg_notify(?, 'WRITTEN|' || w.id || '|' || w.version) FROM w"""
                        .formatted(rows(chunk.size(), "(?::text, ?::%s, ?::%s)"), t, NOW_MICROS);
                versions.putAll(run(Kind.WRITE, connection -> {
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        int index = bindEntries(statement, 1, chunk, null);
                        statement.setString(index++, table.discriminator);
                        statement.setString(index, table.channel);
                        return versions(statement);
                    }
                }));
            }
        } catch (StoreException e) {
            throw versions.isEmpty() ? e : e.afterPartlyApplied();
        }
        return versions;
    }

    @Override
    public Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge) {
        Map<String, ExpectedEntry> byHash = new TreeMap<>();
        entries.forEach(expected -> byHash.put(expected.entry().hash(), expected));
        Map<String, WriteResult> results = new LinkedHashMap<>();
        try {
            for (List<ExpectedEntry> chunk : chunks(new ArrayList<>(byHash.values()))) {
                String sql = """
                        WITH i (id, key, value, expected, created) AS (VALUES %s),
                        u AS (
                            UPDATE %s AS r SET key = i.key, value = i.value, state = 'VALUE',
                                version = greatest(r.version + 1, %s), write_time = now()
                            FROM i WHERE r.discriminator = ? AND r.id = i.id AND i.expected <> -1
                                AND r.version = i.expected
                            RETURNING r.id, r.version),
                        n AS (
                            INSERT INTO %s AS r (discriminator, id, key, value, state, version, write_time)
                            SELECT ?, i.id, i.key, i.value, 'VALUE', i.created, now() FROM i WHERE i.expected = -1
                            ORDER BY i.id
                            ON CONFLICT (discriminator, id) DO NOTHING
                            RETURNING r.id, r.version),
                        w AS (SELECT id, version FROM u UNION ALL SELECT id, version FROM n)
                        SELECT w.id, w.version, pg_notify(?, 'WRITTEN|' || w.id || '|' || w.version) FROM w"""
                        .formatted(rows(chunk.size(), "(?::text, ?::%s, ?::%s, ?::bigint, ?::bigint)"), t, NOW_MICROS,
                                t);
                Map<String, Long> written = run(Kind.WRITE, connection -> {
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        List<Entry> chunkEntries = chunk.stream().map(ExpectedEntry::entry).toList();
                        int index = bindEntries(statement, 1, chunkEntries, chunk);
                        statement.setString(index++, table.discriminator);
                        statement.setString(index++, table.discriminator);
                        statement.setString(index, table.channel);
                        return versions(statement);
                    }
                });
                List<String> conflicting = new ArrayList<>();
                for (ExpectedEntry expected : chunk) {
                    String hash = expected.entry().hash();
                    if (written.containsKey(hash)) {
                        results.put(hash, WriteResult.written(written.get(hash)));
                    } else {
                        conflicting.add(hash);
                    }
                }
                if (!conflicting.isEmpty()) {
                    results.putAll(conflicts(conflicting, maxAge));
                }
            }
        } catch (StoreException e) {
            throw results.isEmpty() ? e : e.afterPartlyApplied();
        }
        return results;
    }

    // what prevented a conditional write: the row as a read sees it (a value too old to count is no row, but its
    // version is reported), or nothing
    private Map<String, WriteResult> conflicts(List<String> hashes, Duration maxAge) {
        return run(Kind.READ, connection -> {
            Map<String, WriteResult> results = new LinkedHashMap<>();
            hashes.forEach(hash -> results.put(hash, WriteResult.conflict(null, 0)));
            try (PreparedStatement statement = connection.prepareStatement("""
                    SELECT id, key, value, state, version, %s, (state <> 'VALUE' OR %s) FROM %s
                    WHERE discriminator = ? AND id = ANY (?)""".formatted(AGE_MICROS, YOUNGER, t))) {
                statement.setLong(1, micros(maxAge));
                statement.setString(2, table.discriminator);
                statement.setArray(3, connection.createArrayOf("text", hashes.toArray()));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Row row = row(resultSet);
                        boolean counts = resultSet.getBoolean(7);
                        results.put(row.hash(), WriteResult.conflict(counts ? row : null, row.version()));
                    }
                }
            }
            return results;
        });
    }

    @Override
    public Map<String, Long> invalidate(Collection<String> hashes) {
        Map<String, Long> versions = new LinkedHashMap<>();
        try {
            for (List<String> chunk : chunks(new ArrayList<>(new TreeSet<>(hashes)))) {
                String sql = """
                        WITH w AS (
                            INSERT INTO %s AS r (discriminator, id, key, value, state, version, write_time)
                            SELECT ?, i.id, NULL, NULL, 'TOMBSTONE', %s, now() FROM unnest(?::text[]) AS i (id)
                            ORDER BY i.id
                            ON CONFLICT (discriminator, id) DO UPDATE SET value = NULL, state = 'TOMBSTONE',
                                version = greatest(r.version + 1, excluded.version), write_time = excluded.write_time
                            RETURNING r.id, r.version)
                        SELECT w.id, w.version, pg_notify(?, 'INVALIDATED|' || w.id || '|' || w.version) FROM w"""
                        .formatted(t, NOW_MICROS);
                versions.putAll(run(Kind.WRITE, connection -> {
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, table.discriminator);
                        statement.setArray(2, connection.createArrayOf("text", chunk.toArray()));
                        statement.setString(3, table.channel);
                        return versions(statement);
                    }
                }));
            }
        } catch (StoreException e) {
            throw versions.isEmpty() ? e : e.afterPartlyApplied();
        }
        return versions;
    }

    @Override
    public Map<String, Long> invalidateIfUnchanged(Map<String, Long> expected) {
        Map<String, Long> byHash = new TreeMap<>(expected);
        Map<String, Long> versions = new LinkedHashMap<>();
        try {
            for (List<String> chunk : chunks(new ArrayList<>(byHash.keySet()))) {
                String sql = """
                        WITH i (id, expected) AS (VALUES %s),
                        u AS (
                            UPDATE %s AS r SET value = NULL, state = 'TOMBSTONE',
                                version = greatest(r.version + 1, %s), write_time = now()
                            FROM i WHERE r.discriminator = ? AND r.id = i.id AND i.expected <> -1
                                AND r.version = i.expected
                            RETURNING r.id, r.version),
                        n AS (
                            INSERT INTO %s AS r (discriminator, id, key, value, state, version, write_time)
                            SELECT ?, i.id, NULL, NULL, 'TOMBSTONE', %s, now() FROM i WHERE i.expected = -1
                            ORDER BY i.id
                            ON CONFLICT (discriminator, id) DO NOTHING
                            RETURNING r.id, r.version),
                        w AS (SELECT id, version FROM u UNION ALL SELECT id, version FROM n)
                        SELECT w.id, w.version, pg_notify(?, 'INVALIDATED|' || w.id || '|' || w.version) FROM w"""
                        .formatted(rows(chunk.size(), "(?::text, ?::bigint)"), t, NOW_MICROS, t, NOW_MICROS);
                versions.putAll(run(Kind.WRITE, connection -> {
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        int index = 1;
                        for (String hash : chunk) {
                            statement.setString(index++, hash);
                            statement.setLong(index++, byHash.get(hash));
                        }
                        statement.setString(index++, table.discriminator);
                        statement.setString(index++, table.discriminator);
                        statement.setString(index, table.channel);
                        return versions(statement);
                    }
                }));
            }
        } catch (StoreException e) {
            throw versions.isEmpty() ? e : e.afterPartlyApplied();
        }
        return versions;
    }

    // every row of the cache gets a version of at least the sweep's time (value rows become tombstones), so every load
    // in flight conflicts; the time is announced and returned
    @Override
    public long invalidateAll() {
        String sql = """
                WITH n AS (SELECT %s AS now),
                w AS (
                    UPDATE %s AS r SET value = NULL,
                        state = CASE WHEN r.state = 'VALUE' THEN 'TOMBSTONE' ELSE r.state END,
                        version = greatest(r.version + 1, (SELECT now FROM n)),
                        write_time = CASE WHEN r.state = 'VALUE' THEN now() ELSE r.write_time END
                    WHERE r.discriminator = ?
                    RETURNING 1)
                SELECT n.now, pg_notify(?, 'INVALIDATED_ALL||' || n.now)
                FROM n, (SELECT count(*) FROM w) AS c""".formatted(NOW_MICROS, t);
        return run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, table.discriminator);
                statement.setString(2, table.channel);
                try (ResultSet resultSet = statement.executeQuery()) {
                    resultSet.next();
                    return resultSet.getLong(1);
                }
            }
        });
    }

    // maintenance: idempotent on several instances at once

    @Override
    public int expire(Duration maxAge) {
        String sql = """
                WITH w AS (
                    UPDATE %s AS r SET value = NULL, state = 'EXPIRED', version = greatest(r.version + 1, %s)
                    WHERE r.discriminator = ? AND r.state = 'VALUE'
                        AND r.write_time <= now() - (?::bigint * interval '1 microsecond')
                    RETURNING r.id, r.version)
                SELECT w.id, w.version, pg_notify(?, 'EXPIRED|' || w.id || '|' || w.version) FROM w"""
                .formatted(t, NOW_MICROS);
        return run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, table.discriminator);
                statement.setLong(2, micros(maxAge));
                statement.setString(3, table.channel);
                return versions(statement).size();
            }
        });
    }

    // keeps the newest maxSize values (ties by hash); rows beyond that rank are the same for every instance pruning
    // at once, so overlapping runs never prune more
    @Override
    public int pruneToSize(long maxSize) {
        String sql = """
                DELETE FROM %1$s WHERE discriminator = ? AND id IN (
                    SELECT id FROM %1$s WHERE discriminator = ? AND state = 'VALUE'
                    ORDER BY write_time DESC, id ASC OFFSET ?)""".formatted(t);
        return run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, table.discriminator);
                statement.setString(2, table.discriminator);
                statement.setLong(3, maxSize);
                return statement.executeUpdate();
            }
        });
    }

    @Override
    public int pruneTombstones(Duration retention) {
        String sql = """
                DELETE FROM %s WHERE discriminator = ? AND state <> 'VALUE'
                    AND write_time <= now() - (?::bigint * interval '1 microsecond')""".formatted(t);
        return run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, table.discriminator);
                statement.setLong(2, micros(retention));
                return statement.executeUpdate();
            }
        });
    }

    @Override
    public void close() {
        // the data source is the user's; subscriptions are closed by their owner
    }

    // statements

    // one attempt; the failure classified for the core, which retries (VISION.md, "Write retries")
    @SuppressWarnings("java:S1141") // the statement's failure is classified apart from the connection's
    private <T> T run(Kind kind, Work<T> work) {
        Connection connection;
        try {
            connection = dataSource.getConnection();
        } catch (SQLTransientConnectionException e) {
            // the pool waited for a connection for its whole timeout already: waiting again only multiplies that
            throw new StoreException("Accessing " + table + " failed: " + e.getMessage(), e, false, false);
        } catch (SQLException e) {
            throw new StoreException("Connecting for " + table + " failed: " + e.getMessage(), e, false, true);
        }
        try (connection) {
            int previousTimeout = bound(connection, timeout);
            try {
                requirePrimary(connection);
                boolean autoCommit = connection.getAutoCommit();
                try {
                    T result = work.run(connection);
                    if (!autoCommit) {
                        connection.commit();
                    }
                    return result;
                } catch (SQLException e) {
                    if (!autoCommit) {
                        rollbackQuietly(connection);
                    }
                    throw e;
                }
            } finally {
                unbound(connection, previousTimeout);
            }
        } catch (SQLException e) {
            throw classified(kind, e);
        } catch (StoreException e) {
            throw e;
        } catch (RuntimeException e) {
            // a row this store cannot read (written by hand or by another program), or a driver failing
            // unexpectedly: a store failure, not one of the caller's own
            throw new StoreException("Accessing " + table + " failed: " + e, e, false, false);
        }
    }

    // what a failed statement means for the core: applied or not, worth another attempt or not
    private StoreException classified(Kind kind, SQLException e) {
        boolean write = kind == Kind.WRITE;
        if (rejected(e)) {
            // rolled back by the server (deadlock, serialization failure, lock not available)
            return new StoreException("Accessing " + table + " was rejected: " + e.getMessage(), e, false, true);
        }
        if (missing(e)) {
            // the table was dropped under the caches: created again, and every cache of it told that every row is
            // gone; the statement can be tried again
            recreate();
            return new StoreException(table + " was missing and was created again: " + e.getMessage(), e, false,
                    true);
        }
        if (timedOut(e)) {
            // no answer within the timeout: another attempt would most likely wait as long again
            return new StoreException("Accessing " + table + " got no answer within " + timeout.toMillis() + " ms"
                    + (write ? ", the outcome unknown" : "") + ": " + e.getMessage(), e, write, false);
        }
        if (connectionLost(e)) {
            return new StoreException("Accessing " + table + " lost its connection" + (write
                    ? ", the outcome unknown" : "") + ": " + e.getMessage(), e, write, true);
        }
        if (notifyQueueFull(e)) {
            // refused at commit, so nothing was written: a listening session somewhere in the database (any
            // application's) does not consume its notifications, typically one left idle in a transaction.
            // PostgreSQL 18 kept refusing even after that session ended, in every attempt to recover it measured
            // (listening anew, VACUUM, both); only no listening session at all and a VACUUM did
            return new StoreException("Writing to " + table + " failed: the NOTIFY queue is full, so PostgreSQL "
                    + "refuses every write that notifies; a session listening in this database does not consume "
                    + "its notifications (pg_stat_activity shows it idle in transaction). PostgreSQL may keep "
                    + "refusing after that session ended, until no session listens and the database is "
                    + "vacuumed: " + e.getMessage(), e, false, false);
        }
        return new StoreException("Accessing " + table + " failed: " + e.getMessage(), e, false, false);
    }

    /**
     * Bounds how long the connection waits for the database to answer (the socket's read timeout): without it, a
     * connection the network silently stopped carrying (a partition, a firewall dropping idle connections) blocks
     * its caller for good. Returns the previous bound, for {@link #unbound}.
     */
    static int bound(Connection connection, Duration timeout) throws SQLException {
        int previous = connection.getNetworkTimeout();
        connection.setNetworkTimeout(Runnable::run, (int) Math.min(Integer.MAX_VALUE, timeout.toMillis()));
        return previous;
    }

    // restores the bound of a connection going back to its pool; a connection broken meanwhile is discarded anyway
    static void unbound(Connection connection, int previous) {
        try {
            connection.setNetworkTimeout(Runnable::run, previous);
        } catch (SQLException ignored) {
            // the connection is broken
        }
    }

    // the table (or its schema) does not exist (undefined_table)
    private static boolean missing(SQLException e) {
        return "42P01".equals(e.getSQLState());
    }

    private void recreate() {
        LOGGER.log(System.Logger.Level.WARNING, "Table {0} was dropped while caches use it; creating it again", table);
        PostgresSchema.ensure(dataSource, table, false, timeout);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT pg_notify(?, 'INVALIDATED_ALL||' || " + NOW_MICROS + ")")) {
            statement.setString(1, table.tableChannel);
            statement.executeQuery().close();
        } catch (SQLException e) {
            throw new StoreException("Announcing the recreated table " + table + " failed: " + e.getMessage(), e,
                    false);
        }
    }

    // PostgreSQL refused the commit because its notification queue is full (program_limit_exceeded)
    private static boolean notifyQueueFull(SQLException e) {
        return "54000".equals(e.getSQLState()) && e.getMessage() != null && e.getMessage().contains("NOTIFY queue");
    }

    // the database did not answer within the bound
    private static boolean timedOut(SQLException e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof SocketTimeoutException) {
                return true;
            }
        }
        return false;
    }

    // rejected by the server, which rolled the statement back completely (autocommit): deadlock, serialization
    // failure, lock not available
    private static boolean rejected(SQLException e) {
        String state = e.getSQLState();
        return "40P01".equals(state) || "40001".equals(state) || "55P03".equals(state);
    }

    // the connection broke during the statement: it may have committed
    private static boolean connectionLost(SQLException e) {
        String state = e.getSQLState();
        return state != null && state.startsWith("08");
    }


    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // the connection is broken anyway
        }
    }

    // a standby lags behind, so its reads would contradict the version rules (and writes and LISTEN fail there):
    // refused, checked once per physical connection
    static void requirePrimary(Connection connection) throws SQLException {
        Object physical = connection.isWrapperFor(PGConnection.class) ? connection.unwrap(PGConnection.class)
                : connection;
        if (PRIMARIES.containsKey(physical)) {
            return;
        }
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT pg_is_in_recovery()")) {
            resultSet.next();
            if (resultSet.getBoolean(1)) {
                throw new StoreException("The data source reaches a standby; distributed-caffeine needs the primary",
                        false);
            }
        }
        PRIMARIES.put(physical, Boolean.TRUE);
    }

    @SuppressWarnings("java:S4449") // the value is a nullable array (byte @Nullable []), which Sonar does not read
    private Row row(ResultSet resultSet) throws SQLException {
        String hash = resultSet.getString(1);
        byte[] key = bytes(resultSet, 2, table.keyFormat);
        State state = State.valueOf(resultSet.getString(4));
        byte[] value = state == State.VALUE ? bytes(resultSet, 3, table.valueFormat) : null;
        long ageMicros = Math.max(0, resultSet.getLong(6));
        return new Row(hash, key == null ? new byte[0] : key, value, state, resultSet.getLong(5),
                Duration.ofNanos(ageMicros * 1_000));
    }

    private static byte @Nullable [] bytes(ResultSet resultSet, int column, Format format) throws SQLException {
        if (format == Format.BINARY) {
            return resultSet.getBytes(column);
        }
        String text = resultSet.getString(column);
        return text == null ? null : text.getBytes(StandardCharsets.UTF_8);
    }

    private static void bind(PreparedStatement statement, int index, byte[] bytes, Format format) throws SQLException {
        if (format == Format.BINARY) {
            statement.setBytes(index, bytes);
        } else {
            statement.setString(index, new String(bytes, StandardCharsets.UTF_8));
        }
    }

    // the VALUES rows: id, key, value and, for conditional writes, the expected version; returns the next index
    private int bindEntries(PreparedStatement statement, int index, List<Entry> entries,
                            @Nullable List<ExpectedEntry> expected) throws SQLException {
        for (int i = 0; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            statement.setString(index++, entry.hash());
            bind(statement, index++, entry.key(), table.keyFormat);
            bind(statement, index++, entry.value(), table.valueFormat);
            if (expected != null) {
                statement.setLong(index++, expected.get(i).expectedVersion());
                statement.setLong(index++, expected.get(i).createdVersion());
            }
        }
        return index;
    }

    // VALUES (...), (...) for n rows, the key and value casts filled in
    private String rows(int count, String row) {
        String filled = row.contains("%s")
                ? row.formatted(PostgresTable.columnType(table.keyFormat), PostgresTable.columnType(table.valueFormat))
                : row;
        return String.join(", ", Collections.nCopies(count, filled));
    }

    private static Map<String, Long> versions(PreparedStatement statement) throws SQLException {
        Map<String, Long> versions = new LinkedHashMap<>();
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                versions.put(resultSet.getString(1), resultSet.getLong(2));
            }
        }
        return versions;
    }

    // one entry per hash (the last wins), in the order of the hashes
    private static List<Entry> sorted(Collection<Entry> entries) {
        Map<String, Entry> byHash = new TreeMap<>();
        entries.forEach(entry -> byHash.put(entry.hash(), entry));
        return new ArrayList<>(byHash.values());
    }

    private static <T> List<List<T>> chunks(List<T> items) {
        List<List<T>> chunks = new ArrayList<>();
        for (int from = 0; from < items.size(); from += CHUNK) {
            chunks.add(items.subList(from, Math.min(items.size(), from + CHUNK)));
        }
        return chunks;
    }

    private static long micros(Duration duration) {
        return duration.toNanos() / 1_000;
    }

    // the store with its change feed: notifications on the cache's own channel, and on the table's, where what
    // removes every row at once is announced
    private static final class Listening extends PostgresStore implements ChangeFeed {

        private final ConnectionFactory listenerConnectionFactory;

        Listening(DataSource dataSource, PostgresTable table, ConnectionFactory listenerConnectionFactory,
                  Duration timeout) {
            super(dataSource, table, timeout);
            this.listenerConnectionFactory = listenerConnectionFactory;
        }

        @Override
        public Subscription subscribe(Listener listener) {
            Subscription own = PostgresListener.subscribe(listenerConnectionFactory, super.dataSource,
                    super.table.channel, listener);
            Subscription whole;
            try {
                whole = PostgresListener.subscribe(listenerConnectionFactory, super.dataSource,
                        super.table.tableChannel, listener);
            } catch (RuntimeException e) {
                own.close();
                throw e;
            }
            return new Subscription() {
                @Override
                public void resume() {
                    own.resume();
                    whole.resume();
                }

                @Override
                public void close() {
                    whole.close();
                    own.close();
                }
            };
        }
    }
}
