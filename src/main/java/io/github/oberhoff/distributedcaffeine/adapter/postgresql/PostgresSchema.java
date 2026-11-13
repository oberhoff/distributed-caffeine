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

import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * On start (VISION.md, "Adapter setup" and "Migration"): a table with the schema marker of this version and the
 * expected shape is used; a missing one is created (with its schema, index and marker) under an advisory lock, or the
 * start is refused with the DDL if the rights are missing; one without the marker is refused or, with withSchemaReset()
 * and if it may be dropped (see requireResettable), dropped and recreated. A missing index only warns.
 *
 * @author Andreas Oberhoff
 */
// the SQL runs against the user's database, unknown to the IDE; what is formatted into it are quoted names only
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2077", "SqlSourceToSinkFlow"})
final class PostgresSchema {

    // the library's one logger, as the core's: the message names the adapter (README, "Deployment")
    private static final Logger LOGGER = System.getLogger("io.github.oberhoff.distributedcaffeine");
    private static final String INSUFFICIENT_PRIVILEGE = "42501";
    // PostgreSQL 9.6, the oldest supported release (README, Requirements), as server_version_num
    private static final int MINIMUM_VERSION_NUM = 90600;

    private PostgresSchema() {
    }

    static void ensure(DataSource dataSource, PostgresTable table, boolean schemaReset, Duration timeout) {
        try (Connection connection = dataSource.getConnection()) {
            int previousTimeout = PostgresStore.bound(connection, timeout);
            try {
                ensure(connection, table, schemaReset);
            } finally {
                PostgresStore.unbound(connection, previousTimeout);
            }
        } catch (SQLException e) {
            // a store not reached is worth another try, as in every operation, and so is a server restarting or a
            // creation that collided with another; anything else is the setup's
            throw new StoreException("Checking table " + table + " failed: " + e.getMessage(), e, false,
                    (PostgresStore.connectionLost(e) && !PostgresStore.refused(e)) || restarting(e) || collided(e));
        }
    }

    // a server shutting down or starting up (operator intervention: admin_shutdown, crash_shutdown,
    // cannot_connect_now, "the database system is starting up"), as during a restart: not reached yet
    private static boolean restarting(SQLException e) {
        String state = e.getSQLState();
        return "57P01".equals(state) || "57P02".equals(state) || "57P03".equals(state);
    }

    // the table, its sequence or its schema created meanwhile by another start (duplicate_table, duplicate_schema,
    // or unique_violation on the catalog when both create at once): the advisory lock serialises the starts of one
    // table, but not the schema shared with other tables, nor a transaction of an attempt that lost its connection
    // and goes on at the server until it notices. Checked again at the next attempt, which finds what was created
    private static boolean collided(SQLException e) {
        String state = e.getSQLState();
        return "42P07".equals(state) || "42P06".equals(state) || "23505".equals(state);
    }

    private static void ensure(Connection connection, PostgresTable table, boolean schemaReset) throws SQLException {
        requireVersion(connection);
        PostgresStore.requirePrimary(connection);
        String marker = marker(connection, table);
        if (marker == null || !marker.equals(PostgresTable.MARKER)) {
            // a concurrent start may have committed the table between the two reads, with its marker (one
            // transaction), so the marker is read again once the table is seen (found: caches built together on a new
            // table refused it as one without a marker)
            if (exists(connection, table)) {
                marker = marker(connection, table);
                if (!PostgresTable.MARKER.equals(marker)) {
                    requireResettable(connection, table, marker, schemaReset); // checked again under the lock
                }
            }
            create(connection, table, schemaReset);
        }
        checkShape(connection, table);
        checkIndex(connection, table);
    }

    // checked first, so that an old server is told so rather than failing a later statement unclearly
    private static void requireVersion(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT current_setting('server_version_num')::int, current_setting('server_version')")) {
            resultSet.next();
            if (resultSet.getInt(1) < MINIMUM_VERSION_NUM) {
                throw new StoreException("PostgreSQL " + resultSet.getString(2) + " is older than the oldest "
                        + "supported, 9.6", false);
            }
        }
    }

    // What withSchemaReset() may drop: a table of distributed-caffeine 0.x (no comment, and its columns discriminator,
    // hash, status and timestamp), or an empty one. Never one of another version of this library, even with the
    // option: two versions left with it on would drop each other's table on every start (a later version decides
    // itself which earlier markers it resets). Never a foreign table with rows either.
    private static void requireResettable(Connection connection, PostgresTable table, @Nullable String marker,
            boolean schemaReset) throws SQLException {
        if (marker != null && marker.startsWith(PostgresTable.MARKER_PREFIX)) {
            throw new StoreException("Table " + table + " was created by another version of distributed-caffeine ('"
                    + marker + "'; this one uses '" + PostgresTable.MARKER + "'). Stop all instances of that version, "
                    + "then drop it or configure another name. withSchemaReset() never drops it, so that two versions "
                    + "cannot drop each other's table.", false);
        }
        if (!schemaReset) {
            throw new StoreException("Table " + table + " exists without the schema marker '" + PostgresTable.MARKER
                    + "' (e.g. a table of distributed-caffeine 0.x). Stop all instances using it, then drop it or "
                    + "configure another name, or enable withSchemaReset().", false);
        }
        if (marker == null && hasLegacyColumns(connection, table)) {
            return;
        }
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT 1 FROM " + table.qualified + " LIMIT 1")) {
            if (resultSet.next()) {
                throw new StoreException("Table " + table + " holds rows that are not distributed-caffeine's; "
                        + "withSchemaReset() drops only tables of distributed-caffeine. Configure another name, or "
                        + "drop it if it is no longer needed.", false);
            }
        }
    }

    // the columns every table of distributed-caffeine 0.x has
    private static boolean hasLegacyColumns(Connection connection, PostgresTable table) throws SQLException {
        String[] names = {"discriminator", "hash", "status", "timestamp"};
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT count(*) FROM pg_attribute
                WHERE attrelid = to_regclass(?) AND attnum > 0 AND NOT attisdropped AND attname = ANY (?)""")) {
            statement.setString(1, table.qualified);
            statement.setArray(2, connection.createArrayOf("text", names));
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt(1) == names.length;
            }
        }
    }

    // the table's comment: null where it has none, or does not exist
    private static @Nullable String marker(Connection connection, PostgresTable table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT obj_description(to_regclass(?)::oid, 'pg_class')")) {
            statement.setString(1, table.qualified);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getString(1);
            }
        }
    }

    private static boolean exists(Connection connection, PostgresTable table) throws SQLException {
        return exists(connection, table.qualified);
    }

    // a table or sequence, by its qualified name
    private static boolean exists(Connection connection, String qualified) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
            statement.setString(1, qualified);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getBoolean(1);
            }
        }
    }

    // in one transaction under an advisory lock, so that concurrent starts create (or reset) the table once
    private static void create(Connection connection, PostgresTable table, boolean schemaReset) throws SQLException {
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            try (PreparedStatement lock = connection.prepareStatement("SELECT pg_advisory_xact_lock(hashtext(?))")) {
                lock.setString(1, "distributed-caffeine " + table.qualified);
                lock.execute();
            }
            String marker = marker(connection, table);
            if (PostgresTable.MARKER.equals(marker)) {
                connection.commit();
                connection.setAutoCommit(autoCommit);
                return; // created by a concurrent start meanwhile
            }
            try (Statement statement = connection.createStatement()) {
                if (exists(connection, table)) {
                    requireResettable(connection, table, marker, schemaReset);
                    LOGGER.log(Level.WARNING, "{0}: Dropping the table of distributed-caffeine 0.x, or empty "
                            + "(withSchemaReset)", table.name());
                    statement.execute("DROP TABLE " + table.qualified);
                }
                if (!schemaExists(connection, table)) {
                    statement.execute("CREATE SCHEMA " + PostgresTable.quote(table.schemaName));
                }
                statement.execute(table.ddl());
            }
            connection.commit();
            LOGGER.log(Level.INFO, "{0}: Created the table with its index, version sequence and schema marker",
                    table.name());
        } catch (SQLException e) {
            restore(connection, autoCommit, e);
            if (INSUFFICIENT_PRIVILEGE.equals(e.getSQLState())) {
                throw new StoreException("Table " + table + " is missing and cannot be created by this user ("
                        + e.getMessage() + "). Create it with:\n" + table.ddl(), e, false);
            }
            throw e;
        } catch (RuntimeException e) {
            restore(connection, autoCommit, e);
            throw e;
        }
        connection.setAutoCommit(autoCommit);
    }

    // rolls back and restores the auto-commit mode after a failure without hiding it: a failure that broke the
    // connection (a pooler in statement mode refusing the transaction closes it) fails both here as well
    private static void restore(Connection connection, boolean autoCommit, Exception failure) {
        try {
            connection.rollback();
            connection.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            failure.addSuppressed(e);
        }
    }

    private static boolean schemaExists(Connection connection, PostgresTable table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = ?)")) {
            statement.setString(1, table.schemaName);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getBoolean(1);
            }
        }
    }

    // the columns and their types; the key and value columns follow the cache's formats, so caches with other
    // serializers cannot share a table
    private static void checkShape(Connection connection, PostgresTable table) throws SQLException {
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("discriminator", "text");
        expected.put("id", "text");
        expected.put("key", PostgresTable.columnType(table.keyFormat));
        expected.put("value", PostgresTable.columnType(table.valueFormat));
        expected.put("state", "text");
        expected.put("version", "bigint");
        expected.put("write_time", "timestamp with time zone");
        Map<String, String> actual = new LinkedHashMap<>();
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute
                WHERE attrelid = to_regclass(?) AND attnum > 0 AND NOT attisdropped""")) {
            statement.setString(1, table.qualified);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    actual.put(resultSet.getString(1), resultSet.getString(2));
                }
            }
        }
        for (Map.Entry<String, String> column : expected.entrySet()) {
            String type = actual.get(column.getKey());
            if (!Objects.equals(type, column.getValue())) {
                throw new StoreException("Table " + table + " does not fit this cache: column " + column.getKey()
                        + " is " + (type == null ? "missing" : type) + ", expected " + column.getValue()
                        + (column.getKey().equals("key") || column.getKey().equals("value")
                        ? " (the table was created for other serializers; use another table)" : ""), false);
            }
        }
        // the floor of the versions the store assigns: without it, no write succeeds
        if (!exists(connection, table.sequence)) {
            throw new StoreException("Table " + table + " has no version sequence " + table.sequence
                    + ". See the DDL:\n" + table.ddl(), false);
        }
    }

    private static void checkIndex(Connection connection, PostgresTable table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_attribute a1 ON a1.attrelid = i.indrelid
                    AND a1.attnum = i.indkey[0] JOIN pg_attribute a2 ON a2.attrelid = i.indrelid
                    AND a2.attnum = i.indkey[1] JOIN pg_attribute a3 ON a3.attrelid = i.indrelid
                    AND a3.attnum = i.indkey[2]
                WHERE i.indrelid = to_regclass(?) AND a1.attname = 'discriminator' AND a2.attname = 'state'
                    AND a3.attname = 'write_time')""")) {
            statement.setString(1, table.qualified);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                if (!resultSet.getBoolean(1)) {
                    LOGGER.log(Level.WARNING, "{0}: Table {1} has no index on (discriminator, state, write_time); "
                            + "expiry and pruning will be slow. Create it as in the DDL:\n{2}", table.name(), table,
                            table.ddl());
                }
            }
        }
    }
}
