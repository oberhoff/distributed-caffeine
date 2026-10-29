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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.testing.SilencingProxy;
import org.jspecify.annotations.NullMarked;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.mysql.MySQLContainer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;

/**
 * A MySQL or MariaDB primary (by the image) with an asynchronously replicating replica (GTID based, each product's
 * own), as both run by default, and the address
 * applications reach "the primary" by: a local proxy, which a failover points at the promoted replica, closing every
 * connection open, as a proxy or a virtual address moved by a failover manager does. Replication can be held back, so
 * that writes confirmed by the primary never reach the replica.
 */
@NullMarked
final class JdbcReplication implements AutoCloseable {

    private static final Duration SETTLING = Duration.ofSeconds(60);

    private final Network network = Network.newNetwork();
    // MariaDB's replication differs from MySQL's in every statement: GTIDs always on, positions of its own
    // (gtid_binlog_pos, gtid_slave_pos), CHANGE MASTER, no super_read_only, no performance_schema by default
    private final boolean mariadb;
    private final JdbcDatabaseContainer<?> primary;
    private final JdbcDatabaseContainer<?> replica;
    private final SilencingProxy proxy;
    // the applications' pool, through the proxy
    final HikariDataSource dataSource;

    // driver: the URL scheme of the applications' pool (JdbcTestDatabase)
    JdbcReplication(String image, String driver) throws IOException {
        mariadb = image.startsWith("mariadb");
        primary = node(image, "primary", 1);
        replica = node(image, "replica", 2);
        primary.start();
        replica.start();
        replicate();
        proxy = new SilencingProxy(primary.getHost(), primary.getMappedPort(3306));
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:" + driver + "://127.0.0.1:" + proxy.port() + "/" + primary.getDatabaseName()
                + (mariadb ? "" : "?allowPublicKeyRetrieval=true&useSSL=false"));
        config.setUsername(primary.getUsername());
        config.setPassword(primary.getPassword());
        config.setMaximumPoolSize(8);
        config.setConnectionTimeout(5_000);
        dataSource = new HikariDataSource(config);
    }

    private JdbcDatabaseContainer<?> node(String image, String alias, int serverId) {
        JdbcDatabaseContainer<?> node = mariadb ? new MariaDBContainer(image) : new MySQLContainer(image);
        node.withNetwork(network).withNetworkAliases(alias);
        if (mariadb) {
            node.withCommand("--server-id=" + serverId, "--log-bin=binlog");
        } else {
            node.withCommand("--server-id=" + serverId, "--log-bin=binlog", "--gtid-mode=ON",
                    "--enforce-gtid-consistency=ON");
        }
        return node;
    }

    // the replica follows the primary from the state both were created in (the same database and user, made by each
    // container's own initialisation): its own transactions forgotten, the primary's taken as applied
    private void replicate() {
        String executed = executed();
        execute(primary, "CREATE USER IF NOT EXISTS 'replication'@'%' IDENTIFIED BY 'replication'",
                "GRANT REPLICATION SLAVE ON *.* TO 'replication'@'%'");
        if (mariadb) {
            execute(replica, "RESET MASTER", "SET GLOBAL gtid_slave_pos = '" + executed + "'",
                    "CHANGE MASTER TO MASTER_HOST = 'primary', MASTER_PORT = 3306, MASTER_USER = 'replication', "
                            + "MASTER_PASSWORD = 'replication', MASTER_USE_GTID = slave_pos",
                    "START SLAVE", "SET GLOBAL read_only = ON");
            require(this::inSync, "the replica replicating");
            return;
        }
        resetBinaryLogs(replica);
        execute(replica, "SET GLOBAL gtid_purged = '" + executed + "'");
        // the replica fetches the primary's key for caching_sha2_password, the default from 8.0
        executeFirst(replica, "CHANGE REPLICATION SOURCE TO SOURCE_HOST = 'primary', SOURCE_PORT = 3306, "
                        + "SOURCE_USER = 'replication', SOURCE_PASSWORD = 'replication', SOURCE_AUTO_POSITION = 1, "
                        + "GET_SOURCE_PUBLIC_KEY = 1",
                "CHANGE MASTER TO MASTER_HOST = 'primary', MASTER_PORT = 3306, MASTER_USER = 'replication', "
                        + "MASTER_PASSWORD = 'replication', MASTER_AUTO_POSITION = 1, GET_MASTER_PUBLIC_KEY = 1",
                "CHANGE MASTER TO MASTER_HOST = 'primary', MASTER_PORT = 3306, MASTER_USER = 'replication', "
                        + "MASTER_PASSWORD = 'replication', MASTER_AUTO_POSITION = 1");
        executeFirst(replica, "START REPLICA", "START SLAVE");
        execute(replica, "SET GLOBAL super_read_only = ON");
        require(this::inSync, "the replica replicating");
    }

    // the GTIDs the primary executed (MariaDB: those in its binary log, as replicas follow them)
    private String executed() {
        return query(primary, mariadb ? "SELECT @@GLOBAL.gtid_binlog_pos" : "SELECT @@GLOBAL.gtid_executed")
                .replace("\n", "");
    }

    // the adapter of the topology's product
    <K, V> Adapter<K, V> adapter(String table) {
        return JdbcTestDatabase.adapter(!mariadb, dataSource, table, JdbcTestDatabase.DEFAULT_DISCRIMINATOR);
    }

    // whether the replica replicates and has applied everything the primary executed
    boolean inSync() {
        if (mariadb) {
            return query(replica, "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS "
                    + "WHERE VARIABLE_NAME = 'SLAVE_RUNNING'").equals("ON")
                    && query(replica, "SELECT MASTER_GTID_WAIT('" + executed() + "', 0)").equals("0");
        }
        String running = query(replica, "SELECT SERVICE_STATE FROM performance_schema.replication_applier_status");
        return running.contains("ON") && query(replica, "SELECT GTID_SUBSET(@@GLOBAL.gtid_executed, '"
                + executed() + "')").equals("1")
                && query(primary, "SELECT GTID_SUBSET(@@GLOBAL.gtid_executed, '"
                + query(replica, "SELECT @@GLOBAL.gtid_executed").replace("\n", "") + "')").equals("1");
    }

    void awaitInSync() {
        require(this::inSync, "the replica in sync with the primary");
    }

    // what the primary confirms from now on never reaches the replica, as when a primary fails before it shipped
    void holdBackReplication() {
        awaitInSync();
        executeFirst(replica, "STOP REPLICA", "STOP SLAVE");
    }

    // the replica promoted (replication ended, writable) and the applications' address moved to it; the old primary
    // stopped, as a failed one is gone
    void failover() {
        if (mariadb) {
            execute(replica, "STOP SLAVE", "RESET SLAVE ALL", "SET GLOBAL read_only = OFF");
        } else {
            executeFirst(replica, "STOP REPLICA", "STOP SLAVE");
            executeFirst(replica, "RESET REPLICA ALL", "RESET SLAVE ALL");
            execute(replica, "SET GLOBAL super_read_only = OFF", "SET GLOBAL read_only = OFF");
        }
        proxy.retarget(replica.getHost(), replica.getMappedPort(3306));
        primary.stop();
    }

    // a row of a table as the given server has it, or "-" (tests: what a failover left)
    String row(boolean onPrimary, String table, String id) {
        return query(onPrimary ? primary : replica, "SELECT CONCAT(state, ' ', version) FROM " + table
                + " WHERE id = '" + id + "'");
    }

    private static Connection root(JdbcDatabaseContainer<?> node) throws SQLException {
        return DriverManager.getConnection(node.getJdbcUrl(), "root", node.getPassword());
    }

    private static void execute(JdbcDatabaseContainer<?> node, String... statements) {
        try (Connection connection = root(node); Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("on " + node.getNetworkAliases() + ": " + e.getMessage(), e);
        }
    }

    // the server's binary logs and executed GTIDs forgotten, by the statement of either name (renamed in MySQL 8.4)
    private static void resetBinaryLogs(JdbcDatabaseContainer<?> node) {
        executeFirst(node, "RESET BINARY LOGS AND GTIDS", "RESET MASTER");
    }

    // the first of the statements the server takes: MySQL renamed its replication statements (REPLICA for SLAVE in
    // 8.0.22, SOURCE for MASTER in 8.0.23, binary logs in 8.4), and only later releases drop the old names
    private static void executeFirst(JdbcDatabaseContainer<?> node, String first, String... alternatives) {
        try {
            execute(node, first);
        } catch (IllegalStateException e) {
            if (alternatives.length == 0) {
                throw e;
            }
            executeFirst(node, alternatives[0], Arrays.copyOfRange(alternatives, 1, alternatives.length));
        }
    }

    private static String query(JdbcDatabaseContainer<?> node, String sql) {
        try (Connection connection = root(node); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            return result.next() && result.getString(1) != null ? result.getString(1) : "-";
        } catch (SQLException e) {
            return "unreachable: " + e.getMessage();
        }
    }

    private static void require(java.util.function.BooleanSupplier condition, String what) {
        if (!awaitUpTo(condition, SETTLING, Duration.ofMillis(200))) {
            throw new IllegalStateException("not within " + SETTLING + ": " + what);
        }
    }

    @Override
    public void close() throws IOException {
        dataSource.close();
        proxy.close();
        for (JdbcDatabaseContainer<?> node : List.of(primary, replica)) {
            node.stop();
        }
        network.close();
    }
}
