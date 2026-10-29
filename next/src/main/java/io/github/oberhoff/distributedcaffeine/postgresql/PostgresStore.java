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
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;

import javax.sql.DataSource;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.WeakHashMap;


/**
 * The store of one cache instance on PostgreSQL (VISION.md, "PostgreSQL: statements and propagation measured"). Every
 * write is one statement that writes and notifies ({@code pg_notify} over the rows a CTE returns), so a notification
 * exists exactly when the write committed. Versions are the core's: a row gets the version its write carries, and a
 * conditional write compares the stored one for equality, nothing else.
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
final class PostgresStore implements Store, ChangeFeed {

    private static final int CHUNK = 1_000;
    // the age by the server's clock; now() is the transaction's start, so every age of one statement is measured at
    // the same instant and the core can compare them (with the control row's, Store#CONTROL_HASH)
    private static final String AGE_MICROS = "(extract(epoch from (now() - write_time)) * 1000000)::bigint";
    private static final String YOUNGER = "write_time > now() - (?::bigint * interval '1 microsecond')";
    // the core's reserved rows (Store#CONTROL_HASH, the heartbeat's): never touched by maintenance
    private static final String NOT_RESERVED = "left(r.id, 1) <> '#'";
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
    private final DataSource listenerDataSource;
    private final PostgresTable table;
    private final Duration timeout;
    private final String t;

    PostgresStore(DataSource dataSource, DataSource listenerDataSource, PostgresTable table, Duration timeout) {
        this.dataSource = dataSource;
        this.listenerDataSource = listenerDataSource;
        this.table = table;
        this.timeout = timeout;
        this.t = table.qualified;
    }

    // the change feed: notifications on the cache's own channel, through the listening session of the listener's
    // data source (shared with every other cache listening through it)
    @Override
    public ChangeSubscription subscribe(ChangeListener listener) {
        return PostgresListener.subscribe(listenerDataSource, dataSource, table.channel, listener);
    }

    // reads

    // every row as it is, also rows the control row hides (the core compares the ages, all measured at the statement's
    // now()). Each id a lookup of its own by the primary key (LATERAL, kept apart by LIMIT 1, at most one row by the
    // key anyway): with statistics behind a table that grew fast, the planner took the discriminator's prefix of the
    // maintenance index and filtered every row of the cache (measured: 60,000 rows read for 100 hits), also when the
    // ids were joined. A value row at least as old as the maximum age reads as EXPIRED (no value, its version): no
    // value counts for it, but a write is conditioned on its version (the store reports nothing else when a write
    // conflicts)
    @Override
    public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
        return run(Kind.READ, connection -> {
            Map<String, Row> rows = new LinkedHashMap<>();
            try (PreparedStatement statement = connection.prepareStatement("""
                    SELECT r.id, r.key, r.value,
                        CASE WHEN r.state = 'VALUE' AND NOT (%s) THEN 'EXPIRED' ELSE r.state END,
                        r.version, %s
                    FROM unnest(?::text[]) AS i (id)
                        CROSS JOIN LATERAL (SELECT * FROM %s AS r WHERE r.discriminator = ? AND r.id = i.id
                            LIMIT 1) AS r"""
                    .formatted(YOUNGER.replace("write_time", "r.write_time"),
                            AGE_MICROS.replace("write_time", "r.write_time"), t))) {
                statement.setLong(1, micros(maxAge));
                statement.setArray(2, connection.createArrayOf("text", hashes.toArray()));
                statement.setString(3, table.discriminator);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Row row = row(resultSet);
                        rows.put(row.hash(), row);
                    }
                }
            }
            return rows;
        });
    }

    // the row r is not hidden by the control row: written after it, or there is none (Store#CONTROL_HASH); binds the
    // discriminator. One lookup by the primary key per row
    private String notHidden() {
        return ("NOT EXISTS (SELECT 1 FROM %s AS c WHERE c.discriminator = ? AND c.id = '%s' "
                + "AND r.write_time <= c.write_time)").formatted(t, CONTROL_HASH);
    }

    @Override
    public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        return run(Kind.READ, connection -> {
            Map<String, Long> versions = new LinkedHashMap<>();
            Map<String, Duration> ages = new LinkedHashMap<>();
            try (PreparedStatement statement = connection.prepareStatement("""
                    SELECT r.id, r.version, %s
                    FROM unnest(?::text[]) AS i (id)
                        CROSS JOIN LATERAL (SELECT * FROM %s AS r WHERE r.discriminator = ? AND r.id = i.id
                            AND r.state = 'VALUE' AND %s LIMIT 1) AS r"""
                    .formatted(AGE_MICROS.replace("write_time", "r.write_time"), t,
                            YOUNGER.replace("write_time", "r.write_time")))) {
                statement.setArray(1, connection.createArrayOf("text", hashes.toArray()));
                statement.setString(2, table.discriminator);
                statement.setLong(3, micros(maxAge));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        versions.put(resultSet.getString(1), resultSet.getLong(2));
                        ages.put(resultSet.getString(1), age(resultSet.getLong(3)));
                    }
                }
            }
            return new VersionsResult(versions, ages);
        });
    }

    // writes

    /**
     * Every kind of write in one statement per chunk, so that a batch mixing them is one round trip: advancing ones as
     * an upsert that updates only a row of a smaller version (a: the version only grows; a row of a larger or equal one
     * is not returned, a refusal), those conditioned on a version as an update matching it (u), those conditioned on
     * no row as an insert doing nothing on a conflict (n); each one atomic step per key, in the order of the hashes. A
     * tombstone keeps the row's key ({@code coalesce}), and has none where no row existed. The write time is the
     * server's now(), which the control row's (Store#CONTROL_HASH) is compared with. Every row written is announced at
     * its version in the same statement; the ids not returned conflicted.
     */
    @Override
    public WriteResult write(Collection<Write> writes) {
        Map<String, Write> byHash = new TreeMap<>();
        writes.forEach(write -> byHash.put(write.hash(), write));
        Set<String> conflicts = new LinkedHashSet<>();
        boolean applied = false;
        try {
            for (List<Write> chunk : chunks(new ArrayList<>(byHash.values()))) {
                String sql = """
                        WITH i (id, key, value, version, expected) AS (VALUES %1$s),
                        a AS (
                            INSERT INTO %2$s AS r (discriminator, id, key, value, state, version, write_time)
                            SELECT ?, i.id, i.key, i.value, %3$s, i.version, now() FROM i
                            WHERE i.expected = %4$d
                            ORDER BY i.id
                            ON CONFLICT (discriminator, id) DO UPDATE SET key = coalesce(excluded.key, r.key),
                                value = excluded.value, state = excluded.state, version = excluded.version,
                                write_time = excluded.write_time
                                WHERE r.version < excluded.version
                            RETURNING r.id, r.version, r.state),
                        u AS (
                            UPDATE %2$s AS r SET key = coalesce(i.key, r.key), value = i.value, state = %3$s,
                                version = i.version, write_time = now()
                            FROM i WHERE r.discriminator = ? AND r.id = i.id AND i.expected > %4$d
                                AND r.version = i.expected
                            RETURNING r.id, r.version, r.state),
                        n AS (
                            INSERT INTO %2$s AS r (discriminator, id, key, value, state, version, write_time)
                            SELECT ?, i.id, i.key, i.value, %3$s, i.version, now() FROM i
                            WHERE i.expected = %5$d
                            ORDER BY i.id
                            ON CONFLICT (discriminator, id) DO NOTHING
                            RETURNING r.id, r.version, r.state),
                        w AS (SELECT * FROM a UNION ALL SELECT * FROM u UNION ALL SELECT * FROM n)
                        SELECT w.id, pg_notify(?, CASE WHEN w.state = 'VALUE' THEN 'WRITTEN|' ELSE 'INVALIDATED|' END
                            || w.id || '|' || w.version) FROM w"""
                        .formatted(rows(chunk.size()), t,
                                "CASE WHEN i.value IS NULL THEN 'TOMBSTONE' ELSE 'VALUE' END", Write.ADVANCE,
                                Write.MISSING);
                Set<String> written = run(Kind.WRITE, connection -> {
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        int index = bindWrites(statement, chunk);
                        statement.setString(index++, table.discriminator);
                        statement.setString(index++, table.discriminator);
                        statement.setString(index++, table.discriminator);
                        statement.setString(index, table.channel);
                        return ids(statement);
                    }
                });
                applied = true;
                for (Write write : chunk) {
                    if (!written.contains(write.hash())) {
                        conflicts.add(write.hash());
                    }
                }
            }
        } catch (StoreException e) {
            throw applied ? e.afterPartlyApplied() : e;
        }
        return WriteResult.refused(conflicts);
    }

    // maintenance: idempotent on several instances at once

    // neither reserved rows nor rows the control row hides (they count as missing already, and pruneTombstones deletes
    // them)
    @Override
    public int expire(Duration maxAge, long version) {
        String sql = """
                WITH w AS (
                    UPDATE %s AS r SET value = NULL, state = 'EXPIRED', version = ?, write_time = now()
                    WHERE r.discriminator = ? AND r.state = 'VALUE'
                        AND r.write_time <= now() - (?::bigint * interval '1 microsecond') AND r.version < ?
                        AND %s AND %s
                    RETURNING r.id, r.version)
                SELECT w.id, w.version, pg_notify(?, 'EXPIRED|' || w.id || '|' || w.version) FROM w"""
                .formatted(t, NOT_RESERVED, notHidden());
        return run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, version);
                statement.setString(2, table.discriminator);
                statement.setLong(3, micros(maxAge));
                // the version only grows: a row written at a larger one since (a clock ahead) is left for a later run
                statement.setLong(4, version);
                statement.setString(5, table.discriminator);
                statement.setString(6, table.channel);
                return ids(statement).size();
            }
        });
    }

    // keeps the newest maxSize values (ties by hash), counting neither reserved rows nor rows the control row hides;
    // rows beyond that rank are the same for every instance pruning at once, so overlapping runs never prune more; of
    // those, the ones younger than the minimum age stay
    @Override
    public int pruneToSize(long maxSize, Duration minAge) {
        String sql = """
                DELETE FROM %1$s WHERE discriminator = ?
                    AND write_time <= now() - (?::bigint * interval '1 microsecond') AND id IN (
                    SELECT id FROM %1$s AS r WHERE discriminator = ? AND state = 'VALUE' AND %2$s AND %3$s
                    ORDER BY write_time DESC, id ASC OFFSET ?)""".formatted(t, NOT_RESERVED, notHidden());
        return run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, table.discriminator);
                statement.setLong(2, micros(minAge));
                statement.setString(3, table.discriminator);
                statement.setString(4, table.discriminator);
                statement.setLong(5, maxSize);
                return statement.executeUpdate();
            }
        });
    }

    // tombstones and expired rows after their grace period, and every row the control row hides (invalidated by
    // invalidateAll, so without a grace period): what invalidateAll left behind goes here, in the background; never a
    // reserved row
    @Override
    public int pruneTombstones(Duration retention) {
        String sql = """
                DELETE FROM %s AS r WHERE discriminator = ? AND %s AND (NOT %s
                    OR state <> 'VALUE' AND write_time <= now() - (?::bigint * interval '1 microsecond'))"""
                .formatted(t, NOT_RESERVED, notHidden());
        return run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, table.discriminator);
                statement.setString(2, table.discriminator);
                statement.setLong(3, micros(retention));
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
            // dropped under the caches: the table is the library's, changing it by hand is not supported, so it is
            // not created again behind the caches' backs (a TRUNCATE likewise is not announced)
            return new StoreException(table + " does not exist (dropped while caches use it?); the caches create it "
                    + "when they are built again: " + e.getMessage(), e, false, false);
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
        return new Row(hash, key == null ? new byte[0] : key, value, state, resultSet.getLong(5),
                age(resultSet.getLong(6)));
    }

    // an age in microseconds; negative for a write time after the statement's now() (a clock stepped back), as it is,
    // so that the ages of the statement still compare like its rows' write times (Store#read)
    private static Duration age(long micros) {
        return Duration.ofNanos(micros * 1_000);
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

    // the VALUES rows: id, key, value (both null for a tombstone, bound with the type the casts expect), version and
    // expected version (or ANY, MISSING); returns the next index
    private int bindWrites(PreparedStatement statement, List<Write> writes) throws SQLException {
        int index = 1;
        for (Write write : writes) {
            statement.setString(index++, write.hash());
            bindNullable(statement, index++, write.key(), table.keyFormat);
            bindNullable(statement, index++, write.value(), table.valueFormat);
            statement.setLong(index++, write.version());
            statement.setLong(index++, write.expected());
        }
        return index;
    }

    private static void bindNullable(PreparedStatement statement, int index, byte @Nullable [] bytes, Format format)
            throws SQLException {
        if (bytes != null) {
            bind(statement, index, bytes, format);
        } else {
            statement.setNull(index, format == Format.BINARY ? Types.BINARY : Types.VARCHAR);
        }
    }

    // VALUES (...), (...) for n writes (id, key, value, version, expected), the key and value casts filled in
    private String rows(int count) {
        String row = "(?::text, ?::%s, ?::%s, ?::bigint, ?::bigint)".formatted(
                PostgresTable.columnType(table.keyFormat), PostgresTable.columnType(table.valueFormat));
        return String.join(", ", Collections.nCopies(count, row));
    }

    // the ids a writing statement returns as its first column: those written
    private static Set<String> ids(PreparedStatement statement) throws SQLException {
        Set<String> ids = new LinkedHashSet<>();
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                ids.add(resultSet.getString(1));
            }
        }
        return ids;
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
}
