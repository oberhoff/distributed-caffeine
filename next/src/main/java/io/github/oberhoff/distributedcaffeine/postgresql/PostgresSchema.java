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

import io.github.oberhoff.distributedcaffeine.spi.StoreException;

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

// On start (VISION.md, "Adapter setup" and "Migration"): a table with the schema marker of this version and the
// expected shape is used; a missing one is created (with its schema, index and marker) under an advisory lock, or the
// start is refused with the DDL if the rights are missing; one without the marker is refused, or dropped and
// recreated with withSchemaReset(). A missing index only warns.
// the SQL runs against the user's database, unknown to the IDE; what is formatted into it are quoted names only
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2077"})
final class PostgresSchema {

    private static final Logger LOGGER = System.getLogger("io.github.oberhoff.distributedcaffeine.postgresql");
    private static final String INSUFFICIENT_PRIVILEGE = "42501";

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
            throw new StoreException("Checking table " + table + " failed: " + e.getMessage(), e, false);
        }
    }

    private static void ensure(Connection connection, PostgresTable table, boolean schemaReset) throws SQLException {
        PostgresStore.requirePrimary(connection);
        String marker = marker(connection, table);
        if (marker == null || !marker.equals(PostgresTable.MARKER)) {
            boolean exists = exists(connection, table);
            if (exists && !schemaReset) {
                throw new StoreException("Table " + table + " exists without the schema marker '"
                        + PostgresTable.MARKER + "' (e.g. a table of an earlier layout, or of distributed-caffeine "
                        + "0.x). Stop all instances using it, then drop it or configure another name, or enable "
                        + "withSchemaReset().", false);
            }
            create(connection, table, schemaReset);
        }
        checkShape(connection, table);
        checkIndex(connection, table);
    }

    private static String marker(Connection connection, PostgresTable table) throws SQLException {
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
        try (PreparedStatement statement = connection.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
            statement.setString(1, table.qualified);
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
                    if (!schemaReset) {
                        throw new StoreException("Table " + table + " was created meanwhile without the schema marker",
                                false);
                    }
                    LOGGER.log(Level.WARNING, "Dropping table {0}, which has no schema marker (withSchemaReset)", table);
                    statement.execute("DROP TABLE " + table.qualified);
                }
                if (!schemaExists(connection, table)) {
                    statement.execute("CREATE SCHEMA " + PostgresTable.quote(table.schemaName));
                }
                statement.execute(table.ddl());
            }
            connection.commit();
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
                    LOGGER.log(Level.WARNING, "Table {0} has no index on (discriminator, state, write_time); "
                            + "expiry and pruning will be slow. See the DDL:\n{1}", table, table.ddl());
                }
            }
        }
    }
}
