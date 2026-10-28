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
package io.github.oberhoff.distributedcaffeine.jdbc;

import io.github.oberhoff.distributedcaffeine.spi.PrimitiveStore;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;
import java.io.Serial;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// The store of one cache on a table: the primitives, in plain JDBC and portable SQL (UPDATE then INSERT for an
// upsert, INSERT ... SELECT for a conditional insert; VISION.md, "Design study", step 5), each in a transaction of its
// own. Versions are the core's, stored as given and compared for equality, so a write needs no read back (SQL has no
// portable RETURNING); the write time, for ages, is the database's clock, in UTC, and every read or scan measures the
// ages of its rows against one reading of it, so that they compare with the control row's (invalidateAll). What SQL
// does not standardise (the type of bytes and times, the time now) comes from a dialect, by the database's product
// name. Keys and values are stored as bytes, whatever the format. Tested on MariaDB.
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2077"}) // the SQL runs against the user's database; quoted names
final class JdbcStore extends PrimitiveStore {

    private static final int CHUNK = 1_000;

    private final DataSource dataSource;
    private final Dialect dialect;
    private final String table;
    private final String discriminator;
    private final int timeoutSeconds;

    JdbcStore(DataSource dataSource, Dialect dialect, String table, String discriminator, Duration timeout) {
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.table = table;
        this.discriminator = discriminator;
        this.timeoutSeconds = (int) Math.max(1, Math.min(Integer.MAX_VALUE, (timeout.toMillis() + 999) / 1_000));
    }

    // what SQL leaves to each database: the types of bytes and times, the time now, and how a time comes back
    enum Dialect {
        // MariaDB: a BLOB holds 64 KB only; a DATETIME in UTC, with no time zone of the session in between
        MARIADB("LONGBLOB", "DATETIME(6)", "UTC_TIMESTAMP(6)") {
            @Override
            Instant time(ResultSet resultSet, int column) throws SQLException {
                return resultSet.getObject(column, LocalDateTime.class).toInstant(ZoneOffset.UTC);
            }

            @Override
            Object parameter(Instant time) {
                return LocalDateTime.ofInstant(time, ZoneOffset.UTC);
            }
        },
        STANDARD("BLOB", "TIMESTAMP(6) WITH TIME ZONE", "CURRENT_TIMESTAMP") {
            @Override
            Instant time(ResultSet resultSet, int column) throws SQLException {
                return resultSet.getObject(column, OffsetDateTime.class).toInstant();
            }

            @Override
            Object parameter(Instant time) {
                return time.atOffset(ZoneOffset.UTC);
            }
        };

        final String bytes;
        final String timestamp;
        final String now;

        Dialect(String bytes, String timestamp, String now) {
            this.bytes = bytes;
            this.timestamp = timestamp;
            this.now = now;
        }

        abstract Instant time(ResultSet resultSet, int column) throws SQLException;

        abstract Object parameter(Instant time);

        static Dialect of(Connection connection) throws SQLException {
            String product = connection.getMetaData().getDatabaseProductName();
            return product.equalsIgnoreCase("MariaDB") ? MARIADB : STANDARD;
        }
    }

    // the schema: the table and its index for scans by write time, created where missing. A table of the layout
    // before 2026-10-28 (with stamp and basis columns for the cut-off of invalidateAll) is refused, naming the
    // remedy: its rows cannot be written without them. Returns the database's dialect
    static Dialect ensure(DataSource dataSource, String table) {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            Dialect dialect = Dialect.of(connection);
            if (exists(connection, table)) {
                requireLayout(connection, table);
                return dialect;
            }
            try {
                statement.execute("""
                        CREATE TABLE %1$s (
                            discriminator VARCHAR(200) NOT NULL,
                            id VARCHAR(200) NOT NULL,
                            k %2$s,
                            v %2$s,
                            state VARCHAR(16) NOT NULL,
                            version BIGINT NOT NULL,
                            write_time %3$s NOT NULL,
                            PRIMARY KEY (discriminator, id))""".formatted(table, dialect.bytes, dialect.timestamp));
                statement.execute("CREATE INDEX %1$s_written ON %1$s (discriminator, write_time)".formatted(table));
            } catch (SQLException e) {
                if (!exists(connection, table)) { // not just another instance creating it at the same time
                    throw e;
                }
            }
            return dialect;
        } catch (SQLException e) {
            throw new StoreException("Creating the table " + table + " failed: " + e.getMessage(), e, false);
        }
    }

    private static void requireLayout(Connection connection, String table) throws SQLException {
        try (ResultSet columns = connection.getMetaData().getColumns(connection.getCatalog(), null, "%", "%")) {
            while (columns.next()) {
                if (table.equalsIgnoreCase(columns.getString("TABLE_NAME"))
                        && "stamp".equalsIgnoreCase(columns.getString("COLUMN_NAME"))) {
                    throw new StoreException("The table " + table + " has the layout of an earlier version (stamp "
                            + "and basis columns); drop it while no cache uses it, and it is created anew", false);
                }
            }
        }
    }

    // in the connection's own database only, where another may hold a table of the name
    private static boolean exists(Connection connection, String table) throws SQLException {
        try (ResultSet tables = connection.getMetaData().getTables(connection.getCatalog(), null, "%",
                new String[]{"TABLE"})) {
            while (tables.next()) {
                if (table.equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
                    return true;
                }
            }
        }
        return false;
    }

    // reads

    @Override
    protected Map<String, Row> readRows(Collection<String> hashes) {
        Map<String, Row> rows = new LinkedHashMap<>();
        List<String> all = new ArrayList<>(hashes);
        for (int from = 0; from < all.size(); from += CHUNK) {
            List<String> chunk = all.subList(from, Math.min(all.size(), from + CHUNK));
            String sql = "SELECT id, k, v, state, version, write_time, %s FROM %s WHERE "
                    .formatted(dialect.now, table) + "discriminator = ? AND id IN (" + String.join(", ",
                    Collections.nCopies(chunk.size(), "?")) + ")";
            rows.putAll(run(Kind.READ, connection -> {
                Map<String, Row> found = new LinkedHashMap<>();
                try (PreparedStatement statement = prepare(connection, sql)) {
                    statement.setString(1, discriminator);
                    for (int i = 0; i < chunk.size(); i++) {
                        statement.setString(i + 2, chunk.get(i));
                    }
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) {
                            State state = State.valueOf(resultSet.getString(4));
                            byte[] key = resultSet.getBytes(2);
                            byte[] value = state == State.VALUE ? resultSet.getBytes(3) : null;
                            String hash = resultSet.getString(1);
                            found.put(hash, new Row(hash, key == null ? new byte[0] : key, value, state,
                                    resultSet.getLong(5), age(resultSet, 6, 7)));
                        }
                    }
                }
                return found;
            }));
        }
        return rows;
    }

    @Override
    protected List<PrimitiveStore.Head> scanRows() {
        String sql = "SELECT id, state, version, write_time, %s FROM %s WHERE discriminator = ?"
                .formatted(dialect.now, table);
        return run(Kind.READ, connection -> {
            try (PreparedStatement statement = prepare(connection, sql)) {
                statement.setString(1, discriminator);
                return heads(statement);
            }
        });
    }

    // the threshold from the database's own clock, so that no instance clock is compared with a stored time
    @Override
    protected List<PrimitiveStore.Head> scanRowsWrittenWithin(Duration within) {
        return run(Kind.READ, connection -> {
            Instant now;
            try (PreparedStatement statement = prepare(connection, "SELECT " + dialect.now);
                 ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                now = dialect.time(resultSet, 1);
            }
            String sql = "SELECT id, state, version, write_time, %s FROM %s WHERE discriminator = ? AND write_time > ?"
                    .formatted(dialect.now, table);
            try (PreparedStatement statement = prepare(connection, sql)) {
                statement.setString(1, discriminator);
                statement.setObject(2, dialect.parameter(now.minus(within.toNanos() / 1_000, ChronoUnit.MICROS)));
                return heads(statement);
            }
        });
    }

    private List<PrimitiveStore.Head> heads(PreparedStatement statement) throws SQLException {
        List<PrimitiveStore.Head> heads = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                heads.add(new PrimitiveStore.Head(resultSet.getString(1), State.valueOf(resultSet.getString(2)),
                        resultSet.getLong(3), age(resultSet, 4, 5)));
            }
        }
        return heads;
    }

    // the age against the statement's one reading of the clock, the same for every row it returns
    private Duration age(ResultSet resultSet, int writeTime, int now) throws SQLException {
        Duration age = Duration.between(dialect.time(resultSet, writeTime), dialect.time(resultSet, now));
        return age.isNegative() ? Duration.ZERO : age;
    }

    // writes: each in one transaction; whether a conditional one applied is its update count

    // UPDATE, and INSERT where no row matched; one inserted meanwhile by another instance is updated after all, in
    // the same transaction (no portable SQL upserts: MERGE is not MariaDB's, ON DUPLICATE KEY not the standard's).
    // A null key keeps the row's key (a tombstone)
    @Override
    protected void upsertRow(String hash, byte @Nullable [] key, byte @Nullable [] value, State state, long version) {
        String update = ("UPDATE %s SET k = COALESCE(?, k), v = ?, state = ?, version = ?, write_time = %s "
                + "WHERE discriminator = ? AND id = ?").formatted(table, dialect.now);
        String insert = ("INSERT INTO %s (discriminator, id, k, v, state, version, write_time) VALUES "
                + "(?, ?, ?, ?, ?, ?, %s)").formatted(table, dialect.now);
        this.<@Nullable Void>run(Kind.WRITE, connection -> {
            if (updated(connection, update, hash, key, value, state, version)) {
                return null;
            }
            try (PreparedStatement statement = prepare(connection, insert)) {
                statement.setString(1, discriminator);
                statement.setString(2, hash);
                bind(statement, 3, key, value, state);
                statement.setLong(6, version);
                statement.executeUpdate();
            } catch (SQLException e) {
                if (!isDuplicate(e) || !updated(connection, update, hash, key, value, state, version)) {
                    throw e;
                }
            }
            return null;
        });
    }

    private boolean updated(Connection connection, String update, String hash, byte @Nullable [] key,
                            byte @Nullable [] value, State state, long version) throws SQLException {
        try (PreparedStatement statement = prepare(connection, update)) {
            bind(statement, 1, key, value, state);
            statement.setLong(4, version);
            statement.setString(5, discriminator);
            statement.setString(6, hash);
            return statement.executeUpdate() > 0;
        }
    }

    private static boolean isDuplicate(SQLException e) {
        return e instanceof SQLIntegrityConstraintViolationException
                || (e.getSQLState() != null && e.getSQLState().startsWith("23"));
    }

    @Override
    protected boolean replaceRow(String hash, long expected, long version, byte @Nullable [] key,
                                 byte @Nullable [] value, State state) {
        String sql = ("UPDATE %s SET k = COALESCE(?, k), v = ?, state = ?, version = ?, write_time = %s "
                + "WHERE discriminator = ? AND id = ? AND version = ?").formatted(table, dialect.now);
        return run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = prepare(connection, sql)) {
                bind(statement, 1, key, value, state);
                statement.setLong(4, version);
                statement.setString(5, discriminator);
                statement.setString(6, hash);
                statement.setLong(7, expected);
                return statement.executeUpdate() > 0;
            }
        });
    }

    // a plain INSERT: the primary key refuses a second row of the id, which is the answer
    @Override
    protected boolean insertRow(String hash, long version, byte @Nullable [] key, byte @Nullable [] value,
                                State state) {
        String sql = ("INSERT INTO %s (discriminator, id, k, v, state, version, write_time) VALUES "
                + "(?, ?, ?, ?, ?, ?, %s)").formatted(table, dialect.now);
        try {
            return run(Kind.INSERT, connection -> {
                try (PreparedStatement statement = prepare(connection, sql)) {
                    statement.setString(1, discriminator);
                    statement.setString(2, hash);
                    bind(statement, 3, key, value, state);
                    statement.setLong(6, version);
                    return statement.executeUpdate() > 0;
                }
            });
        } catch (Duplicate e) {
            return false;
        }
    }

    @Override
    protected boolean deleteRow(String hash, long version) {
        return run(Kind.WRITE, connection -> {
            try (PreparedStatement statement = prepare(connection,
                    "DELETE FROM %s WHERE discriminator = ? AND id = ? AND version = ?".formatted(table))) {
                statement.setString(1, discriminator);
                statement.setString(2, hash);
                statement.setLong(3, version);
                return statement.executeUpdate() == 1;
            }
        });
    }

    @Override
    public void close() {
        // the data source is the user's
    }

    // helpers

    private static void bind(PreparedStatement statement, int index, byte @Nullable [] key, byte @Nullable [] value,
                             State state) throws SQLException {
        statement.setBytes(index, key);
        statement.setBytes(index + 1, value);
        statement.setString(index + 2, state.name());
    }

    private PreparedStatement prepare(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setQueryTimeout(timeoutSeconds);
        return statement;
    }

    private enum Kind {
        READ, WRITE, INSERT
    }

    @FunctionalInterface
    private interface Work<T extends @Nullable Object> {
        T run(Connection connection) throws SQLException;
    }

    // an insert finding the id taken
    private static final class Duplicate extends RuntimeException {
        @Serial
        private static final long serialVersionUID = 1L;

        Duplicate() {
            super(null, null, false, false);
        }
    }

    // one connection, one transaction for a write; failures classified for the core (VISION.md, "Write retries").
    // The connection goes back as it came: a data source that does not reset its connections would otherwise hand
    // the next caller one in a transaction never ended, whose reads may see an old snapshot. A read on a connection
    // that came without auto-commit ends its transaction too, for the same reason
    private <T extends @Nullable Object> T run(Kind kind, Work<T> work) {
        Connection connection;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new StoreException("Connecting to " + table + " failed: " + e.getMessage(), e, false, true);
        }
        try (connection) {
            boolean autoCommit = connection.getAutoCommit();
            if (kind == Kind.READ) {
                try {
                    return work.run(connection);
                } finally {
                    if (!autoCommit) {
                        rollbackQuietly(connection);
                    }
                }
            }
            connection.setAutoCommit(false);
            try {
                T result = work.run(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(connection);
                throw e;
            } finally {
                if (autoCommit) {
                    restoreAutoCommitQuietly(connection);
                }
            }
        } catch (SQLException e) {
            throw classified(kind, e);
        }
    }

    // after the write's outcome is decided: a connection broken by now is the data source's to discard
    private static void restoreAutoCommitQuietly(Connection connection) {
        try {
            connection.setAutoCommit(true);
        } catch (SQLException ignored) {
            // the outcome stands as committed or classified
        }
    }

    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // the connection is broken: the write did not commit, or its outcome is unknown, as classified
        }
    }

    private RuntimeException classified(Kind kind, SQLException e) {
        boolean write = kind != Kind.READ;
        if (isDuplicate(e)) {
            // a row inserted meanwhile: for an insert the answer, for an upsert a race lost, rolled back
            return kind == Kind.INSERT ? new Duplicate() : new StoreException("Writing to " + table
                    + " was rejected (a row inserted meanwhile): " + e.getMessage(), e, false, true);
        }
        if (e instanceof SQLTransactionRollbackException) {
            return new StoreException("Accessing " + table + " was rejected: " + e.getMessage(), e, false, true);
        }
        if (e instanceof SQLTimeoutException) {
            return new StoreException("Accessing " + table + " got no answer within " + timeoutSeconds + " s"
                    + (write ? ", the outcome unknown" : "") + ": " + e.getMessage(), e, write, false);
        }
        if (e instanceof SQLTransientConnectionException || e instanceof SQLNonTransientConnectionException
                || e instanceof SQLRecoverableException) {
            return new StoreException("Accessing " + table + " lost its connection" + (write
                    ? ", the outcome unknown" : "") + ": " + e.getMessage(), e, write, true);
        }
        return new StoreException("Accessing " + table + " failed: " + e.getMessage(), e, false, false);
    }
}
