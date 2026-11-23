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
package io.github.oberhoff.distributedcaffeine.adapter.postgresql;

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
import org.postgresql.core.BaseConnection;
import org.postgresql.core.TransactionState;

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
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
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

import static java.util.Objects.requireNonNull;


/**
 * The store of one cache instance on PostgreSQL (VISION.md, "The PostgreSQL adapter"). Every
 * write is one statement that writes and notifies ({@code pg_notify} over the rows a CTE returns), so a notification
 * exists exactly when the write committed. Versions are the store's own (VISION.md, "Versions applied as
 * well as each store can", point 6): every write takes the next value of the table's sequence, or one above the stored
 * version where that is larger, the version proposed ignored; written back to the core with the result. The sequence
 * is drawn by every write, so it lies above every version of the table: a row created anew (its tombstone deleted)
 * starts above whatever an instance still holds of it, and no clock, of an instance or the server, reaches a row.
 * Batches are written in the order of their hashes, so concurrent batches lock rows in the same order.
 * <p>
 * Failures (VISION.md, "Write retries"): one attempt each, classified for the core, which retries: a statement the
 * server rejected (deadlock, serialization failure, lock not available) or a connection that could not be obtained
 * is retryable and certainly not applied; a connection lost during a statement is retryable with a write's outcome
 * unknown; a statement without an answer within the timeout is not retryable, a write's outcome unknown.
 *
 * @author Andreas Oberhoff
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
    // the same two on the row of a lookup, aliased r
    private static final String ROW_AGE_MICROS = AGE_MICROS.replace("write_time", "r.write_time");
    private static final String ROW_YOUNGER = YOUNGER.replace("write_time", "r.write_time");
    // a value row r not past its own expiry time (Write#expiresAfter), and that maximum age from its write time
    private static final String ROW_NOT_PAST = "(r.expire_time IS NULL OR r.expire_time > now())";
    private static final String ROW_EXPIRES_AFTER_MICROS =
            "(extract(epoch from (r.expire_time - r.write_time)) * 1000000)::bigint";
    // the value's own expiry time a write sets (Write#expiresAfter): now() plus its maximum age in microseconds (the
    // VALUES' exp), NULL without one
    private static final String EXPIRY_TIME = "now() + i.exp * interval '1 microsecond'";
    // the core's reserved rows (Store#CONTROL_HASH, the heartbeat's): never touched by maintenance
    private static final String NOT_RESERVED = "left(r.id, 1) <> '#'";
    // physical connections checked for not being a standby, once each (weakly held, as the pool's connections come
    // and go)
    private static final Map<Object, Boolean> PRIMARIES = Collections.synchronizedMap(new WeakHashMap<>());

    private enum Kind {
        READ, WRITE
    }

    // what a maintenance batch changed, and where it ended (its cursor)
    private record Batch<C>(int changed, @Nullable C last) {
    }

    private interface Work<T> {
        T run(Connection connection) throws SQLException;
    }

    private final DataSource dataSource;
    private final DataSource listenerDataSource;
    private final PostgresTable table;
    private final Duration timeout;
    private final String t;
    // where the last full batch of expire (a write time, in microseconds since 1970) and of pruneTombstones (an id)
    // ended, so that the next batch starts there instead of reading past the dead index entries the batches before
    // left behind (until vacuum): from the start each time, a million rows took 30 and 27 s in batches of 10,000,
    // against 9 and 3 s in one statement (VISION.md, "Maintenance in paced batches"). Null after a batch that was not
    // full, so a run starts over and finds what a batch skipped (rows locked meanwhile). Only the core's maintenance
    // calls them, one batch at a time
    private volatile @Nullable Long expireCursor;
    private volatile @Nullable String pruneCursor;

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
        return PostgresListener.subscribe(listenerDataSource, dataSource, table.channel, table.name(),
                listener);
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
        String sql = """
                SELECT r.id, r.key, r.value,
                    CASE WHEN r.state = 'VALUE' AND NOT (%s AND %s) THEN 'EXPIRED' ELSE r.state END,
                    r.version, %s, %s
                FROM unnest(?::text[]) AS i (id)
                    CROSS JOIN LATERAL (SELECT * FROM %s AS r WHERE r.discriminator = ? AND r.id = i.id
                        LIMIT 1) AS r""".formatted(ROW_YOUNGER, ROW_NOT_PAST, ROW_AGE_MICROS,
                ROW_EXPIRES_AFTER_MICROS, t);
        return run(Kind.READ, connection -> {
            Map<String, Row> rows = new LinkedHashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
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
        String sql = """
                SELECT r.id, r.version, %s
                FROM unnest(?::text[]) AS i (id)
                    CROSS JOIN LATERAL (SELECT * FROM %s AS r WHERE r.discriminator = ? AND r.id = i.id
                        AND r.state = 'VALUE' AND %s AND %s LIMIT 1) AS r""".formatted(ROW_AGE_MICROS, t, ROW_YOUNGER,
                ROW_NOT_PAST);
        return run(Kind.READ, connection -> {
            Map<String, Long> versions = new LinkedHashMap<>();
            Map<String, Duration> ages = new LinkedHashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
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

    // the server's clock, as every write time is set: now(), the start of the statement's transaction
    @Override
    public Instant time() {
        return run(Kind.READ, connection -> {
            try (PreparedStatement statement = connection.prepareStatement("SELECT now()");
                 ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getObject(1, OffsetDateTime.class).toInstant();
            }
        });
    }

    // writes

    /**
     * Every kind of write in one statement per chunk, so that a batch mixing them is one round trip: advancing ones as
     * an upsert (a: never refused), those conditioned on a version as an update matching it (u), those conditioned on
     * no row as an insert doing nothing on a conflict (n); each one atomic step per key, in the order of the hashes.
     * Each applies at a version assigned under the row's lock, {@code GREATEST(stored + 1, nextval())}: the sequence
     * alone where the store numbered every version, one above the stored one only over a row of a larger version
     * (written by hand, or by a sequence set back). A tombstone keeps the row's key ({@code coalesce}), and has none
     * where no row existed. The write time is the server's now(), which the control row's (Store#CONTROL_HASH) is
     * compared with. Every row written is announced at its version in the same statement and returned with it; the ids
     * not returned conflicted.
     */
    @Override
    public WriteResult write(Collection<Write> writes) {
        Map<String, Write> byHash = new TreeMap<>();
        writes.forEach(write -> byHash.put(write.hash(), write));
        Set<String> conflicts = new LinkedHashSet<>();
        Map<String, Long> assigned = new LinkedHashMap<>();
        boolean applied = false;
        try {
            for (List<Write> chunk : chunks(new ArrayList<>(byHash.values()))) {
                String sql = """
                        WITH i (id, key, value, expected, exp) AS (VALUES %1$s),
                        a AS (
                            INSERT INTO %2$s AS r (discriminator, id, key, value, state, version, write_time,
                                expire_time)
                            SELECT ?, i.id, i.key, i.value, %3$s, %6$s, now(), %7$s FROM i
                            WHERE i.expected = %4$d
                            ORDER BY i.id
                            ON CONFLICT (discriminator, id) DO UPDATE SET key = coalesce(excluded.key, r.key),
                                value = excluded.value, state = excluded.state,
                                version = GREATEST(r.version + 1, %6$s), write_time = excluded.write_time,
                                expire_time = excluded.expire_time
                            RETURNING r.id, r.version, r.state),
                        u AS (
                            UPDATE %2$s AS r SET key = coalesce(i.key, r.key), value = i.value, state = %3$s,
                                version = GREATEST(r.version + 1, %6$s), write_time = now(), expire_time = %7$s
                            FROM i WHERE r.discriminator = ? AND r.id = i.id AND i.expected > %4$d
                                AND r.version = i.expected
                            RETURNING r.id, r.version, r.state),
                        n AS (
                            INSERT INTO %2$s AS r (discriminator, id, key, value, state, version, write_time,
                                expire_time)
                            SELECT ?, i.id, i.key, i.value, %3$s, %6$s, now(), %7$s FROM i
                            WHERE i.expected = %5$d
                            ORDER BY i.id
                            ON CONFLICT (discriminator, id) DO NOTHING
                            RETURNING r.id, r.version, r.state),
                        w AS (SELECT * FROM a UNION ALL SELECT * FROM u UNION ALL SELECT * FROM n)
                        SELECT w.id, w.version, pg_notify(?, CASE WHEN w.state = 'VALUE' THEN 'WRITTEN|'
                            ELSE 'INVALIDATED|' END || w.id || '|' || w.version) FROM w"""
                        .formatted(rows(chunk.size()), t,
                                "CASE WHEN i.value IS NULL THEN 'TOMBSTONE' ELSE 'VALUE' END", Write.ADVANCE,
                                Write.MISSING, table.nextVersion(), EXPIRY_TIME);
                Map<String, Long> written = run(Kind.WRITE, connection -> {
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        int index = bindWrites(statement, chunk);
                        statement.setString(index++, table.discriminator);
                        statement.setString(index++, table.discriminator);
                        statement.setString(index++, table.discriminator);
                        statement.setString(index, table.channel);
                        return versions(statement);
                    }
                });
                applied = true;
                for (Write write : chunk) {
                    Long version = written.get(write.hash());
                    if (version == null) {
                        conflicts.add(write.hash());
                    } else if (version != write.version()) {
                        assigned.put(write.hash(), version);
                    }
                }
            }
        } catch (StoreException e) {
            throw applied ? e.afterPartlyApplied() : e;
        }
        return new WriteResult(conflicts, assigned);
    }

    // the control row backdated (Store#writeControl): an upsert as write's, its write time the last microsecond
    // (timestamptz) before the given time, and never earlier than the row's, so that the cut-off only moves forward.
    // Exact in integer microseconds from the epoch, no floating point between
    @Override
    public WriteResult writeControl(Write control, Instant before) {
        if (!CONTROL_HASH.equals(control.hash()) || !control.isAdvance() || control.isTombstone()) {
            throw new IllegalArgumentException("control must be an advancing value write of the control row");
        }
        Instant writeTime = before.minusNanos(1).truncatedTo(ChronoUnit.MICROS);
        long micros = Math.addExact(Math.multiplyExact(writeTime.getEpochSecond(), 1_000_000L),
                writeTime.getNano() / 1_000);
        String sql = """
                WITH a AS (
                    INSERT INTO %1$s AS r (discriminator, id, key, value, state, version, write_time)
                    VALUES (?, ?, ?::%3$s, ?::%4$s, 'VALUE', %2$s,
                        timestamptz 'epoch' + ?::bigint * interval '1 microsecond')
                    ON CONFLICT (discriminator, id) DO UPDATE SET key = excluded.key, value = excluded.value,
                        state = excluded.state, version = GREATEST(r.version + 1, %2$s),
                        write_time = GREATEST(r.write_time, excluded.write_time)
                    RETURNING r.id, r.version)
                SELECT a.id, a.version, pg_notify(?, 'WRITTEN|' || a.id || '|' || a.version) FROM a"""
                .formatted(t, table.nextVersion(), PostgresTable.columnType(table.keyFormat),
                        PostgresTable.columnType(table.valueFormat));
        Map<String, Long> written = run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, table.discriminator);
                statement.setString(2, CONTROL_HASH);
                bind(statement, 3, requireNonNull(control.key()), table.keyFormat);
                bind(statement, 4, requireNonNull(control.value()), table.valueFormat);
                statement.setLong(5, micros);
                statement.setString(6, table.channel);
                return versions(statement);
            }
        });
        Long version = written.get(CONTROL_HASH);
        if (version == null) {
            return new WriteResult(Set.of(CONTROL_HASH), Map.of());
        }
        return new WriteResult(Set.of(), version == control.version() ? Map.of() : Map.of(CONTROL_HASH, version));
    }

    // maintenance: idempotent on several instances at once

    // neither reserved rows nor rows the control row hides (they count as missing already, and pruneTombstones deletes
    // them); each expiry assigned a version as a write is, the given one ignored. At most the limit, by the maintenance
    // index; rows another instance expires (or a write changes) at the same moment are skipped, not waited for: they
    // are expired by it, or by the next batch
    // first the rows past their own expiry time (Write#expireAfterCreate), by their partial index, the earliest
    // first; then, within what is left of the limit, those at least as old as the maximum age
    @Override
    public int expire(Duration maxAge, long version, int limit) {
        int own = expireOwn(limit);
        return own >= limit ? own : own + expireAged(maxAge, limit - own);
    }

    // a row leaves the index's range once expired (its expiry time cleared), so no cursor is needed
    private int expireOwn(int limit) {
        String sql = """
                WITH d AS (
                    SELECT r.id FROM %1$s AS r
                    WHERE r.discriminator = ? AND r.expire_time <= now() AND r.state = 'VALUE' AND %3$s AND %4$s
                    ORDER BY r.expire_time LIMIT ? FOR UPDATE OF r SKIP LOCKED),
                w AS (
                    UPDATE %1$s AS r SET value = NULL, state = 'EXPIRED', version = GREATEST(r.version + 1, %2$s),
                        write_time = now(), expire_time = NULL
                    FROM d WHERE r.discriminator = ? AND r.id = d.id
                    RETURNING r.id, r.version)
                SELECT w.id, w.version, pg_notify(?, 'EXPIRED|' || w.id || '|' || w.version) FROM w"""
                .formatted(t, table.nextVersion(), NOT_RESERVED, notHidden());
        return run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, table.discriminator);
                statement.setString(2, table.discriminator);
                statement.setInt(3, limit);
                statement.setString(4, table.discriminator);
                statement.setString(5, table.channel);
                return versions(statement).size();
            }
        });
    }

    private int expireAged(Duration maxAge, int limit) {
        String sql = """
                WITH d AS (
                    SELECT r.id, (extract(epoch from r.write_time) * 1000000)::bigint AS micros FROM %1$s AS r
                    WHERE r.discriminator = ? AND r.state = 'VALUE'
                        AND r.write_time <= now() - (?::bigint * interval '1 microsecond') AND %3$s AND %4$s
                        AND (?::bigint IS NULL
                            OR r.write_time >= timestamptz 'epoch' + ?::bigint * interval '1 microsecond')
                    ORDER BY r.write_time LIMIT ? FOR UPDATE OF r SKIP LOCKED),
                w AS (
                    UPDATE %1$s AS r SET value = NULL, state = 'EXPIRED', version = GREATEST(r.version + 1, %2$s),
                        write_time = now(), expire_time = NULL
                    FROM d WHERE r.discriminator = ? AND r.id = d.id
                    RETURNING r.id, r.version)
                SELECT w.id, w.version, pg_notify(?, 'EXPIRED|' || w.id || '|' || w.version), max(d.micros) OVER ()
                FROM w JOIN d ON d.id = w.id"""
                .formatted(t, table.nextVersion(), NOT_RESERVED, notHidden());
        Long cursor = expireCursor;
        expireCursor = null; // kept only by a full batch, once committed
        Batch<Long> batch = run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, table.discriminator);
                statement.setLong(2, micros(maxAge));
                statement.setString(3, table.discriminator);
                setNullableLong(statement, 4, cursor);
                setNullableLong(statement, 5, cursor);
                statement.setInt(6, limit);
                statement.setString(7, table.discriminator);
                statement.setString(8, table.channel);
                int expired = 0;
                long last = 0;
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        expired++;
                        last = resultSet.getLong(4);
                    }
                }
                return new Batch<>(expired, last);
            }
        });
        if (batch.changed() >= limit) {
            expireCursor = batch.last();
        }
        return batch.changed();
    }

    // keeps the newest maxSize values (ties by hash), counting neither reserved rows nor rows the control row hides.
    // Rows beyond that rank are the same for every instance pruning at once, so overlapping runs never prune more; of
    // those, the ones younger than the minimum age stay. At most the limit, the oldest first: the boundary (the newest
    // row beyond the rank, b) is found by reading the index as far as the rank, then the batch from the old end up to
    // it. Taken from the newest end beyond the rank instead, every batch read past the rows the batches before had
    // deleted (dead until vacuumed): 26 s for a million rows in batches of 10,000, against 2 s in one statement
    @Override
    public int pruneToSize(long maxSize, Duration minAge, int limit) {
        String values = "r.discriminator = ? AND r.state = 'VALUE' AND %s AND %s".formatted(NOT_RESERVED,
                notHidden());
        String sql = """
                WITH b AS (
                    SELECT r.write_time, r.id FROM %1$s AS r WHERE %2$s
                    ORDER BY r.write_time DESC, r.id ASC OFFSET ? LIMIT 1),
                d AS (
                    SELECT r.id FROM %1$s AS r, b WHERE %2$s
                        AND (r.write_time < b.write_time OR r.write_time = b.write_time AND r.id >= b.id)
                        AND r.write_time <= now() - (?::bigint * interval '1 microsecond')
                    ORDER BY r.write_time ASC, r.id DESC LIMIT ?)
                DELETE FROM %1$s AS r USING d WHERE r.discriminator = ? AND r.id = d.id""".formatted(t, values);
        return run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, table.discriminator);
                statement.setString(2, table.discriminator);
                statement.setLong(3, maxSize);
                statement.setString(4, table.discriminator);
                statement.setString(5, table.discriminator);
                statement.setLong(6, micros(minAge));
                statement.setInt(7, limit);
                statement.setString(8, table.discriminator);
                return statement.executeUpdate();
            }
        });
    }

    // tombstones and expired rows after their grace period, and every row the control row hides (invalidated by
    // invalidateAll, so without a grace period): what invalidateAll left behind goes here, in the background; never a
    // reserved row
    // at most the limit, in the order of the ids (its rows are of every state); rows another instance deletes at the
    // same moment are skipped, not waited for
    @Override
    public int pruneTombstones(Duration retention, int limit) {
        String sql = """
                WITH d AS (
                    SELECT r.id FROM %1$s AS r WHERE r.discriminator = ? AND %2$s AND (NOT %3$s
                        OR r.state <> 'VALUE' AND r.write_time <= now() - (?::bigint * interval '1 microsecond'))
                        AND (?::text IS NULL OR r.id > ?)
                    ORDER BY r.id LIMIT ? FOR UPDATE OF r SKIP LOCKED),
                x AS (DELETE FROM %1$s AS r USING d WHERE r.discriminator = ? AND r.id = d.id RETURNING r.id)
                SELECT count(*), max(x.id) FROM x"""
                .formatted(t, NOT_RESERVED, notHidden());
        String cursor = pruneCursor;
        pruneCursor = null; // kept only by a full batch, once committed
        Batch<String> batch = run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, table.discriminator);
                statement.setString(2, table.discriminator);
                statement.setLong(3, micros(retention));
                statement.setString(4, cursor);
                statement.setString(5, cursor);
                statement.setInt(6, limit);
                statement.setString(7, table.discriminator);
                try (ResultSet resultSet = statement.executeQuery()) {
                    resultSet.next();
                    return new Batch<>(resultSet.getInt(1), resultSet.getString(2));
                }
            }
        });
        if (batch.changed() >= limit) {
            pruneCursor = batch.last();
        }
        return batch.changed();
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
            throw new StoreException(accessing() + " failed: " + e.getMessage(), e, false, false);
        } catch (SQLException e) {
            throw new StoreException("Connecting to " + table + " failed: " + e.getMessage(), e, false, true);
        }
        try (connection) {
            requireNoTransaction(connection, table.name());
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
            throw new StoreException(accessing() + " failed: " + e, e, false, false);
        }
    }

    // a connection inside a transaction of the caller, as a data source handing out the caller's connection does (a
    // transaction-aware one, such as Spring's TransactionAwareDataSourceProxy): the commit after this store's work
    // would commit the caller's transaction, and a rollback after a failure roll it back, so it is refused before
    // anything runs on it (found 2026-11-17: a put inside a transaction rolled back afterwards committed the caller's
    // rows written before it; VISION.md, "A deadline of the core's own"). pgjdbc knows from the protocol whether a
    // transaction is open, at no cost; a connection that does not unwrap to pgjdbc's (a wrapping driver) is not checked
    static void requireNoTransaction(Connection connection, String table) throws SQLException {
        if (connection.isWrapperFor(BaseConnection.class)
                && connection.unwrap(BaseConnection.class).getTransactionState() != TransactionState.IDLE) {
            throw new StoreException(table + ": The data source handed out a connection inside a transaction, which "
                    + "the cache would commit along with its own work. Give the adapter a plain pool, not a "
                    + "transaction-aware data source", null, false, false);
        }
    }

    // how a failure message names the table
    private String accessing() {
        return "Accessing " + table;
    }

    // what a failed statement means for the core: applied or not, worth another attempt or not
    private StoreException classified(Kind kind, SQLException e) {
        boolean write = kind == Kind.WRITE;
        if (rejected(e)) {
            // rolled back by the server (deadlock, serialization failure, lock not available)
            return new StoreException(accessing() + " was rejected: " + e.getMessage(), e, false, true);
        }
        if (missing(e)) {
            // dropped under the caches: the table is the library's, changing it by hand is not supported, so it is
            // not created again behind the caches' backs (a TRUNCATE likewise is not announced)
            return new StoreException(table + " does not exist (dropped while caches use it?); the caches create it "
                    + "when they are built again: " + e.getMessage(), e, false, false);
        }
        if (timedOut(e)) {
            // no answer within the timeout: another attempt would most likely wait as long again
            return new StoreException(accessing() + " got no answer within " + timeout.toMillis() + " ms"
                    + (write ? ", the outcome unknown" : "") + ": " + e.getMessage(), e, write, false);
        }
        if (connectionLost(e)) {
            return new StoreException(accessing() + " lost its connection" + (write
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
        return new StoreException(accessing() + " failed: " + e.getMessage(), e, false, false);
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
    static boolean connectionLost(SQLException e) {
        String state = e.getSQLState();
        return state != null && state.startsWith("08");
    }

    // the server refused the user or the database: SQLState class 28 (invalid authorization) or 3D (no such
    // database), also behind a pool's timeout carrying it as a cause (HikariCP's "Connection is not available"); or a
    // pooler in statement mode refused the schema's transaction, closing the connection (PgBouncer, protocol_violation
    // 08P01, like a connection lost): a configuration error for good, not a store out of reach (VISION.md, "Starting
    // without the store")
    static boolean refused(SQLException e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            String state = cause instanceof SQLException sql ? sql.getSQLState() : null;
            if (state != null && (state.startsWith("28") || state.startsWith("3D"))) {
                return true;
            }
            if ("08P01".equals(state) && cause.getMessage() != null
                    && cause.getMessage().contains("statement pooling mode")) {
                return true;
            }
        }
        return false;
    }


    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // the connection is broken anyway
        }
    }

    // a standby lags behind, so its reads would contradict the version rules (and writes and LISTEN fail there):
    // refused, checked once per physical connection. Retryable, as MariaDB's and MySQL's read-only server and Redis's
    // replica are: during a failover the address may reach the old primary, now a standby, or the standby not yet
    // promoted, so a cache built then starts without its store and connects once a primary answers there (VISION.md,
    // "Starting without the store"); a data source that reaches a standby for good shows as a cache that stays
    // DEGRADED, warning every 5 minutes
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
                throw new StoreException("The data source reaches a standby; distributed-caffeine needs the primary "
                        + "(with several hosts in the URL, set targetServerType=primary; during a failover, try "
                        + "again)", null, false, true);
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
        long version = resultSet.getLong(5);
        Duration age = age(resultSet.getLong(6));
        long expiresAfter = resultSet.getLong(7);
        Duration own = resultSet.wasNull() ? null : age(expiresAfter);
        return new Row(hash, key == null ? new byte[0] : key, value, state, version, age, own);
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

    // the VALUES rows: id, key, value (both null for a tombstone, bound with the type the casts expect) and expected
    // version (or ADVANCE, MISSING); returns the next index
    private int bindWrites(PreparedStatement statement, List<Write> writes) throws SQLException {
        int index = 1;
        for (Write write : writes) {
            statement.setString(index++, write.hash());
            bindNullable(statement, index++, write.key(), table.keyFormat);
            bindNullable(statement, index++, write.value(), table.valueFormat);
            statement.setLong(index++, write.expected());
            setNullableLong(statement, index++, microsOrNull(write.expiresAfter()));
        }
        return index;
    }

    private static void setNullableLong(PreparedStatement statement, int index, @Nullable Long value)
            throws SQLException {
        if (value != null) {
            statement.setLong(index, value);
        } else {
            statement.setNull(index, Types.BIGINT);
        }
    }

    private static void bindNullable(PreparedStatement statement, int index, byte @Nullable [] bytes, Format format)
            throws SQLException {
        if (bytes != null) {
            bind(statement, index, bytes, format);
        } else {
            statement.setNull(index, format == Format.BINARY ? Types.BINARY : Types.VARCHAR);
        }
    }

    // VALUES (...), (...) for n writes (id, key, value, expected), the key and value casts filled in
    private String rows(int count) {
        String row = "(?::text, ?::%s, ?::%s, ?::bigint, ?::bigint)".formatted(
                PostgresTable.columnType(table.keyFormat), PostgresTable.columnType(table.valueFormat));
        return String.join(", ", Collections.nCopies(count, row));
    }

    // the ids a writing statement returns as its first column, those written, with the versions assigned (the second)
    private static Map<String, Long> versions(PreparedStatement statement) throws SQLException {
        Map<String, Long> versions = new LinkedHashMap<>();
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                versions.put(resultSet.getString(1), resultSet.getLong(2));
            }
        }
        return versions;
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

    private static @Nullable Long microsOrNull(@Nullable Duration duration) {
        return duration == null ? null : micros(duration);
    }
}
