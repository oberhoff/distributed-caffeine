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
package io.github.oberhoff.distributedcaffeine.adapter.jdbc;

import io.github.oberhoff.distributedcaffeine.spi.PrimitiveStore;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;
import java.io.Serial;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
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
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * The store of one cache on a table: the primitives, in plain JDBC and portable SQL (UPDATE then INSERT for an upsert,
 * a plain INSERT for a conditional one; VISION.md, "Design study", step 5), each statement committed as it runs.
 * Versions are the core's, stored as given and compared for equality, so a write needs no read back (SQL has no
 * portable RETURNING); the write time, for ages, is the database's clock, in UTC, and every read or scan measures the
 * ages of its rows against one reading of it, so that they compare with the control row's (invalidateAll). What SQL
 * does not standardise (the type of bytes and times, the time now) comes from the dialect of the adapter's database
 * (MariaDbAdapter, MySqlAdapter), which the schema's check confirms. Keys and values are stored as bytes, whatever the
 * format.
 *
 * @author Andreas Oberhoff
 */
// the SQL runs against the user's database, unknown to the IDE; what is formatted into it are quoted names only
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2077"})
final class JdbcStore extends PrimitiveStore {

    private static final int CHUNK = 1_000;

    private final DataSource dataSource;
    private final Dialect dialect;
    // the identity of the server the last scan reached (Dialect#server), from the schema's check before the first
    // scan
    private volatile String server;
    private final String table;
    private final String discriminator;
    private final Duration timeout;
    private final int timeoutSeconds;

    // server: the identity the schema's check found (ensure)
    @SuppressWarnings("java:S2637") // every field is assigned here, the volatile server included
    JdbcStore(DataSource dataSource, Dialect dialect, String server, String table, String discriminator,
              Duration timeout) {
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.server = server;
        this.table = table;
        this.discriminator = discriminator;
        this.timeout = timeout;
        this.timeoutSeconds = seconds(timeout);
    }

    // what SQL leaves to each database: the types of bytes and times, the time now, how a time comes back, and the
    // answering server's identity; with the product it is for, told by its version string, and the oldest version
    // supported. MySQL shares MariaDB's SQL (the MySQL trial, 2026-11-02, VISION.md "MariaDB and MySQL")
    enum Dialect {
        // MySQL Connector/J refused on MariaDB: neither Oracle nor MariaDB supports that pairing. MariaDB
        // Connector/J is taken on MySQL, which MariaDB supports and users choose for its licence (LGPL, not GPL)
        MARIADB("MariaDB", "MariaDbAdapter", 10, 2, "MySQL Connector/J"),
        MYSQL("MySQL", "MySqlAdapter", 5, 7, null);

        // a BLOB holds 64 KB only
        final String bytes = "LONGBLOB";
        // a DATETIME in UTC, with no time zone of the session in between
        final String timestamp = "DATETIME(6)";
        final String now = "UTC_TIMESTAMP(6)";
        // both need a distinct server_id on every server that replicates, so a failover changes it
        final String server = "@@server_id";
        final String product;
        final String adapter;
        // the oldest version supported: the first with large index prefixes by default, without which the primary
        // key (two VARCHAR(200)) exceeds 767 bytes in utf8mb4, the character set of today's deployments (VISION.md,
        // "MariaDB and MySQL", step 4: older lines refuse the table)
        private final int major;
        private final int minor;
        // the driver refused (by the name it reports), null where every driver is taken
        private final @Nullable String refusedDriver;

        Dialect(String product, String adapter, int major, int minor, @Nullable String refusedDriver) {
            this.product = product;
            this.adapter = adapter;
            this.major = major;
            this.minor = minor;
            this.refusedDriver = refusedDriver;
        }

        // whether the driver of that name is taken; one of another name (a wrapper around a driver, a later one)
        // is, untested
        boolean takes(String driver) {
            return refusedDriver == null || !driver.startsWith(refusedDriver);
        }

        Instant time(ResultSet resultSet, int column) throws SQLException {
            return resultSet.getObject(column, LocalDateTime.class).toInstant(ZoneOffset.UTC);
        }

        Object parameter(Instant time) {
            return LocalDateTime.ofInstant(time, ZoneOffset.UTC);
        }

        // the product a server's VERSION() names: MariaDB's says so ("11.8.2-MariaDB-ubu2404"), MySQL's and its
        // builds' ("8.4.2", "8.0.36-28" of Percona Server) do not. Not the driver's product name: Connector/J names
        // MySQL whatever server it reaches
        static Dialect of(String version) {
            return version.contains("MariaDB") ? MARIADB : MYSQL;
        }

        // whether a server's VERSION() is this product's floor or later
        boolean supports(String version) {
            Matcher matcher = VERSION.matcher(version);
            if (!matcher.lookingAt()) {
                return false;
            }
            int foundMajor = Integer.parseInt(matcher.group(1));
            return foundMajor > major || (foundMajor == major && Integer.parseInt(matcher.group(2)) >= minor);
        }

        String floor() {
            return major + "." + minor;
        }
    }

    private static final Pattern VERSION = Pattern.compile("(\\d+)\\.(\\d+)");

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_]\\w{0,50}");

    // the adapters' checks of what their builders are given: a table name formatted into SQL, so an identifier only
    static String requireTableName(String tableName) {
        requireNonNull(tableName, "tableName cannot be null");
        if (!IDENTIFIER.matcher(tableName).matches()) {
            throw new IllegalArgumentException("tableName must be letters, digits and underscores: " + tableName);
        }
        return tableName;
    }

    // a discriminator fitting its column
    static String requireDiscriminator(String discriminator) {
        requireNonNull(discriminator, "discriminator cannot be null");
        if (discriminator.isEmpty() || discriminator.length() > 200) {
            throw new IllegalArgumentException("discriminator must be 1 to 200 characters");
        }
        return discriminator;
    }

    // the schema: the table and its index for scans by write time, created where missing. A table of the layout
    // before 2026-10-28 (with stamp and basis columns for the cut-off of invalidateAll) is refused, naming the
    // remedy: its rows cannot be written without them. First the server is checked: the dialect's product (the
    // other one refused, naming its adapter) at its floor or later, reached by a driver the dialect takes. Returns
    // the answering server's identity: taken before the store reads anything, as a value read before the first scan
    // may be lost by a failover too, which the first scan would not notice if it learnt the identity itself (found:
    // caches whose first poll came after the failover held the lost key). Bounded by the timeout like every
    // operation: the metadata calls by the network timeout only, they are no statements
    static String ensure(DataSource dataSource, Dialect dialect, String table, Duration timeout) {
        try (Connection connection = dataSource.getConnection()) {
            int previousTimeout = bound(connection, timeout);
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(seconds(timeout));
                return ensure(connection, statement, dialect, table);
            } finally {
                unbound(connection, previousTimeout);
            }
        } catch (SQLException e) {
            throw new StoreException("Creating the table " + table + " failed: " + e.getMessage(), e, false);
        }
    }

    private static String ensure(Connection connection, Statement statement, Dialect dialect, String table)
            throws SQLException {
        String version;
        String server;
        try (ResultSet resultSet = statement.executeQuery("SELECT VERSION(), " + dialect.server)) {
            resultSet.next();
            version = resultSet.getString(1);
            server = resultSet.getString(2);
        }
        Dialect found = Dialect.of(version);
        if (found != dialect) {
            throw new StoreException("The data source reaches " + found.product + " (" + version + "), not "
                    + dialect.product + ": use " + found.adapter, false);
        }
        if (!dialect.supports(version)) {
            throw new StoreException(dialect.product + " " + version + " is older than the oldest supported, "
                    + dialect.floor(), false);
        }
        String driver = connection.getMetaData().getDriverName();
        if (!dialect.takes(driver)) {
            throw new StoreException(driver + " is not supported for " + dialect.product + ": use MariaDB "
                    + "Connector/J (org.mariadb.jdbc:mariadb-java-client)", false);
        }
        if (exists(connection, table)) {
            requireLayout(connection, table);
            return server;
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
        return server;
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
    @SuppressWarnings("java:S6909") // each chunk prepares a statement of its own, the discriminator set on each
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

    // the threshold from the database's own clock, so that no instance clock is compared with a stored time. With it
    // the server's identity: a scan answered by another server than the last (a failover, which with asynchronous
    // replication, MySQL's and MariaDB's default, may lose writes the caches learned of) fails once, which the core
    // takes as a gap (VISION.md, "MariaDB and MySQL", step 4): what caches hold is checked against the store, also
    // keys the new server has no row of, which no scan could report
    @Override
    protected List<PrimitiveStore.Head> scanRowsWrittenWithin(Duration within) {
        return run(Kind.READ, connection -> {
            Instant now;
            try (PreparedStatement statement = prepare(connection, "SELECT " + dialect.now + ", " + dialect.server);
                 ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                now = dialect.time(resultSet, 1);
                String answering = resultSet.getString(2);
                String before = server;
                server = answering;
                if (!before.equals(answering)) {
                    throw new StoreException("Another server answers for " + table + " (server_id " + before + ", now "
                            + answering + "): changes may have been missed", null, false, true);
                }
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
        // negative for a row written after that reading (a clock stepped back), as it is (Store#read)
        return Duration.between(dialect.time(resultSet, writeTime), dialect.time(resultSet, now));
    }

    // writes: each statement conditional by itself, committed as it runs (run); whether a conditional one applied is
    // its update count

    // UPDATE over a smaller version (the version only grows), and INSERT where no row matched; a row already there,
    // inserted meanwhile by another instance, refuses the insert, and the update decides once more (no portable SQL
    // upserts: MERGE is not MariaDB's, ON DUPLICATE KEY not the standard's). A null key
    // keeps the row's key (a tombstone)
    @Override
    protected boolean advanceRow(String hash, byte @Nullable [] key, byte @Nullable [] value, State state,
                                 long version) {
        String update = ("UPDATE %s SET k = COALESCE(?, k), v = ?, state = ?, version = ?, write_time = %s "
                + "WHERE discriminator = ? AND id = ? AND version < ?").formatted(table, dialect.now);
        String insert = ("INSERT INTO %s (discriminator, id, k, v, state, version, write_time) VALUES "
                + "(?, ?, ?, ?, ?, ?, %s)").formatted(table, dialect.now);
        return run(Kind.WRITE, connection -> {
            if (updated(connection, update, hash, key, value, state, version)) {
                return true;
            }
            try (PreparedStatement statement = prepare(connection, insert)) {
                statement.setString(1, discriminator);
                statement.setString(2, hash);
                bind(statement, 3, key, value, state);
                statement.setLong(6, version);
                statement.executeUpdate();
                return true;
            } catch (SQLException e) {
                if (!isDuplicate(e)) {
                    throw e;
                }
                return updated(connection, update, hash, key, value, state, version);
            }
        });
    }

    private boolean updated(Connection connection, String update, String hash, byte @Nullable [] key,
                            byte @Nullable [] value, State state, long version) throws SQLException {
        try (PreparedStatement statement = prepare(connection, update)) {
            bind(statement, 1, key, value, state);
            statement.setLong(4, version);
            statement.setString(5, discriminator);
            statement.setString(6, hash);
            statement.setLong(7, version);
            return statement.executeUpdate() > 0;
        }
    }

    private static boolean isDuplicate(SQLException e) {
        return e instanceof SQLIntegrityConstraintViolationException || sqlStateClass(e, "23");
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
        try {
            statement.setQueryTimeout(timeoutSeconds);
            return statement;
        } catch (SQLException | RuntimeException e) {
            // the caller only closes what it received
            try {
                statement.close();
            } catch (SQLException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private enum Kind {
        READ, WRITE, INSERT
    }

    @FunctionalInterface
    private interface Work<T> {
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

    // one connection per primitive; failures classified for the core (VISION.md, "Write retries"). The connection
    // goes back as it came: a data source that does not reset its connections would otherwise hand
    // the next caller one in a transaction never ended, whose reads may see an old snapshot. A read on a connection
    // that came without auto-commit ends its transaction too, for the same reason
    private <T> T run(Kind kind, Work<T> work) {
        Connection connection;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new StoreException("Connecting to " + table + " failed: " + e.getMessage(), e, false, true);
        }
        try (connection) {
            return bounded(connection, kind, work);
        } catch (SQLException e) {
            throw classified(kind, e);
        }
    }

    // the work under the timeout; a failure classified while the connection is still at hand, before it goes back to
    // the data source
    private <T> T bounded(Connection connection, Kind kind, Work<T> work) throws SQLException {
        int previousTimeout = bound(connection, timeout);
        long start = System.nanoTime();
        try {
            return run(connection, kind, work);
        } catch (SQLException e) {
            throw classified(kind, e, connection, System.nanoTime() - start);
        } finally {
            unbound(connection, previousTimeout);
        }
    }

    private static <T> T run(Connection connection, Kind kind, Work<T> work) throws SQLException {
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
        // a write needs no transaction of its own: each of its statements is conditional by itself (a version, a
        // primary key), and on MariaDB and MySQL a statement that fails (an insert finding the row there) leaves the
        // session usable for the next, unlike PostgreSQL's aborted transaction. In auto-commit, each commits as it
        // runs: one round trip per statement, instead of two more to switch auto-commit off and on and one to
        // commit (measured: a put 874 µs on MariaDB 13 before, VISION.md "MariaDB and MySQL", step 6)
        if (autoCommit) {
            return work.run(connection);
        }
        // a connection that came within a transaction: the write in it, ended, as before
        try {
            T result = work.run(connection);
            connection.commit();
            return result;
        } catch (SQLException | RuntimeException e) {
            rollbackQuietly(connection);
            throw e;
        }
    }

    // the timeout in whole seconds, rounded up, for the query timeout of a statement (at least one: zero means none)
    private static int seconds(Duration timeout) {
        return (int) Math.max(1, Math.min(Integer.MAX_VALUE, (timeout.toMillis() + 999) / 1_000));
    }

    // bounds every wait of the connection for the database by the timeout, where the driver supports it. The query
    // timeout of each statement alone does not do that: drivers enforce it by sending the database a cancel, which a
    // network that silently stopped carrying never delivers, so the statement would wait for good (and with it the
    // caller). The network timeout fires first (the query timeout is rounded up to whole seconds) and breaks the
    // connection, which the data source discards. Returns the bound to restore, or -1 where the driver has none and
    // the query timeout must do
    static int bound(Connection connection, Duration timeout) throws SQLException {
        try {
            int previous = connection.getNetworkTimeout();
            connection.setNetworkTimeout(Runnable::run, (int) Math.min(Integer.MAX_VALUE, timeout.toMillis()));
            return previous;
        } catch (SQLFeatureNotSupportedException e) {
            return -1;
        }
    }

    // restores the bound of a connection going back to its pool; a connection broken meanwhile is discarded anyway
    static void unbound(Connection connection, int previous) {
        if (previous < 0) {
            return;
        }
        try {
            connection.setNetworkTimeout(Runnable::run, previous);
        } catch (SQLException ignored) {
            // the connection is broken
        }
    }

    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // the connection is broken: the write did not commit, or its outcome is unknown, as classified
        }
    }

    // how a failure message names the table
    private String accessing() {
        return "Accessing " + table;
    }

    private RuntimeException classified(Kind kind, SQLException e) {
        return classified(kind, e, false, false);
    }

    // a failure the exception does not explain is judged by what it left behind, the same for every driver: one
    // after waiting the whole timeout is a timeout, and one that left the connection unusable is a connection lost.
    // MySQL Connector/J 8 and 9 report both as "No operations allowed after statement closed." (SQLState S1009, the
    // statement closed under the call by the driver's own timeout or by the broken connection), which taken as a
    // plain failure would make a write that may have landed certainly not applied. Asked only then: the check of
    // the connection may cost a round trip, which a failure explained by the exception does not need
    private RuntimeException classified(Kind kind, SQLException e, Connection connection, long elapsedNanos) {
        if (isDuplicate(e) || rejected(e) || timedOut(e) || connectionLost(e)) {
            return classified(kind, e, false, false);
        }
        boolean waitedOut = elapsedNanos >= timeout.toNanos();
        return classified(kind, e, waitedOut, !waitedOut && !valid(connection));
    }

    private static boolean valid(Connection connection) {
        try {
            return connection.isValid(1);
        } catch (SQLException ignored) {
            return false;
        }
    }

    private RuntimeException classified(Kind kind, SQLException e, boolean waitedOut, boolean broken) {
        boolean write = kind != Kind.READ;
        if (isDuplicate(e)) {
            // a row inserted meanwhile: for an insert the answer, for an upsert a race lost, rolled back
            return kind == Kind.INSERT ? new Duplicate() : new StoreException("Writing to " + table
                    + " was rejected (a row inserted meanwhile): " + e.getMessage(), e, false, true);
        }
        if (rejected(e)) {
            return new StoreException(accessing() + " was rejected: " + e.getMessage(), e, false, true);
        }
        if (waitedOut || timedOut(e)) {
            // no answer within the timeout, by the query timeout or the network timeout (which also breaks the
            // connection, so it comes as a connection lost): another attempt would most likely wait as long again
            return new StoreException(accessing() + " got no answer within " + timeout.toMillis() + " ms"
                    + (write ? ", the outcome unknown" : "") + ": " + e.getMessage(), e, write, false);
        }
        if (broken || connectionLost(e)) {
            return new StoreException(accessing() + " lost its connection" + (write
                    ? ", the outcome unknown" : "") + ": " + e.getMessage(), e, write, true);
        }
        return new StoreException(accessing() + " failed: " + e.getMessage(), e, false, false);
    }

    // The server's error decides, whatever the driver made of it: drivers differ in the exception classes and the
    // SQLStates they report for the same error (a lock wait timeout's own SQLState is HY000, which a driver may
    // remap to 40001 or not), the error codes MariaDB and MySQL share do not; the classes stay for what else the
    // driver knows

    // rolled back by the server, so certainly not applied, and worth another attempt: a deadlock (1213), a lock
    // wait timeout (1205; the store rolls back what the server left of the transaction), a write refused by a
    // read-only server (1290: a primary demoted by a failover, or a replica reached before the address moved)
    static boolean rejected(SQLException e) {
        int code = e.getErrorCode();
        return e instanceof SQLTransactionRollbackException || sqlStateClass(e, "40") || code == 1213 || code == 1205
                || code == 1290;
    }

    // the connection broke: SQLState class 08, or a class saying so
    static boolean connectionLost(SQLException e) {
        return e instanceof SQLTransientConnectionException || e instanceof SQLNonTransientConnectionException
                || e instanceof SQLRecoverableException || sqlStateClass(e, "08");
    }

    private static boolean sqlStateClass(SQLException e, String sqlStateClass) {
        String sqlState = e.getSQLState();
        return sqlState != null && sqlState.startsWith(sqlStateClass);
    }

    // the database did not answer within the bound: the query timeout fired, or the network timeout (in the causes)
    private static boolean timedOut(SQLException e) {
        if (e instanceof SQLTimeoutException) {
            return true;
        }
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof SocketTimeoutException) {
                return true;
            }
        }
        return false;
    }
}
