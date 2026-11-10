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

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports.Binding;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.testing.Ports;
import io.github.oberhoff.distributedcaffeine.testing.TestStoreSettings;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.mysql.MySQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * A MariaDB, or a MySQL, in a container for the JDBC adapters' suites, behind a connection pool as an application
 * would use it: started by the image's test class before its suites and stopped after them, one at a time
 * ({@link JdbcIntegrationTest}), a fresh table per use. The server stores text as utf8mb4, as today's deployments
 * do; it is reached through the driver the class names: the product's own, or MariaDB Connector/J for MySQL too (the
 * pairings the adapters take).
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class JdbcTestDatabase implements AutoCloseable {

    // the URL schemes choosing the driver (each driver takes its own only)
    static final String MARIADB_CONNECTOR_J = "mariadb";
    static final String MYSQL_CONNECTOR_J = "mysql";
    // the same in both adapters
    static final String DEFAULT_DISCRIMINATOR = MariaDbAdapter.DEFAULT_DISCRIMINATOR;
    private static final AtomicInteger TABLES = new AtomicInteger();
    private static volatile @Nullable JdbcTestDatabase current;

    private final JdbcDatabaseContainer<?> container;
    private final boolean mysql;
    private final String driver;
    private final HikariDataSource dataSource;

    private JdbcTestDatabase(String image, String driver) {
        this.mysql = image.startsWith("mysql");
        this.driver = driver;
        List<JdbcDatabaseContainer<?>> started = new ArrayList<>(1);
        Ports.startOnFree(1, ports -> started.add(server(image, ports.get(0))));
        this.container = started.get(0);
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(urlOf(driver, container.getHost(), container.getMappedPort(3306)));
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setMaximumPoolSize(16);
        this.dataSource = new HikariDataSource(config);
    }

    // started at a fixed host port, which it keeps across a restart (stopServer, startServer)
    private JdbcDatabaseContainer<?> server(String image, int port) {
        JdbcDatabaseContainer<?> server = mysql ? new MySQLContainer(image) : new MariaDBContainer(image);
        server.withCommand("--character-set-server=utf8mb4")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(requireNonNull(cmd.getHostConfig())
                        .withPortBindings(new PortBinding(Binding.bindPort(port), ExposedPort.tcp(3306)))));
        try {
            server.start();
        } catch (RuntimeException e) {
            server.stop();
            throw e;
        }
        return server;
    }

    // the image reached through the driver of that URL scheme, for the suites until it is closed
    static JdbcTestDatabase start(String image, String driver) {
        if (current != null) {
            throw new IllegalStateException("a JDBC test database is running already");
        }
        JdbcTestDatabase started = new JdbcTestDatabase(image, driver);
        current = started;
        return started;
    }

    private static JdbcTestDatabase get() {
        return requireNonNull(current, "no JDBC test database is running: the JDBC suites run nested in "
                + "JdbcIntegrationTest");
    }

    static DataSource dataSource() {
        return get().dataSource;
    }

    static boolean mysql() {
        return get().mysql;
    }

    // statements needing more than the test user's rights on its database (users, grants); the containers set the
    // root password to the test user's
    static void asRoot(String... statements) throws SQLException {
        JdbcTestDatabase database = get();
        try (Connection connection = DriverManager.getConnection(database.urlOf(database.driver,
                database.container.getHost(), database.container.getMappedPort(3306)), "root",
                database.container.getPassword());
             java.sql.Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    // a pool of a user that may read and write the test database but not create tables in it, closed by the caller
    static HikariDataSource readerDataSource() throws SQLException {
        JdbcTestDatabase database = get();
        asRoot("CREATE USER IF NOT EXISTS 'reader'@'%' IDENTIFIED BY 'reader'",
                "GRANT SELECT, INSERT, UPDATE, DELETE ON " + database.container.getDatabaseName()
                        + ".* TO 'reader'@'%'");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.urlOf(database.driver, database.container.getHost(),
                database.container.getMappedPort(3306)));
        config.setUsername("reader");
        config.setPassword("reader");
        config.setMaximumPoolSize(2);
        return new HikariDataSource(config);
    }

    static String host() {
        return get().container.getHost();
    }

    static int port() {
        return get().container.getMappedPort(3306);
    }

    // MySQL's default authentication sends the password encrypted by the server's key, which a connection without
    // TLS must be allowed to fetch
    static String url(String driver, String host, int port) {
        return get().urlOf(driver, host, port);
    }

    private String urlOf(String driver, String host, int port) {
        return "jdbc:" + driver + "://" + host + ":" + port + "/" + container.getDatabaseName()
                + (mysql ? "?allowPublicKeyRetrieval=true&useSSL=false" : "");
    }

    private static HikariDataSource pool(String driver, String host, int port) {
        JdbcTestDatabase database = get();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url(driver, host, port));
        config.setUsername(database.container.getUsername());
        config.setPassword(database.container.getPassword());
        config.setMaximumPoolSize(1);
        // connecting on first use, so that a server the driver cannot connect to fails the store's creation, not
        // the pool's
        config.setInitializationFailTimeout(-1);
        return new HikariDataSource(config);
    }

    // a pool of its own through the suites' driver, each session set up by the given statement (null for none);
    // connecting on first use and waiting at most 2 s for a connection, so that a server down fails fast
    static HikariDataSource pool(int size, @Nullable String sessionSql) {
        JdbcTestDatabase database = get();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url(database.driver, host(), port()));
        config.setUsername(database.container.getUsername());
        config.setPassword(database.container.getPassword());
        config.setMaximumPoolSize(size);
        config.setConnectionTimeout(2_000);
        config.setInitializationFailTimeout(-1);
        if (sessionSql != null) {
            config.setConnectionInitSql(sessionSql);
        }
        return new HikariDataSource(config);
    }

    // a session of the server's administrator (root), for what the application's user may not see or do: which
    // sessions wait for a lock, killing them
    static Connection root() throws SQLException {
        JdbcDatabaseContainer<?> container = get().container;
        return DriverManager.getConnection(url(get().driver, host(), port()), "root", container.getPassword());
    }

    // the server, as a restart for an update does it: on its data, at its port (the client is Testcontainers' own,
    // shared, closed with it)
    @SuppressWarnings("resource")
    static void stopServer() {
        DockerClientFactory.instance().client().stopContainerCmd(get().container.getContainerId()).exec();
    }

    @SuppressWarnings("resource")
    static void startServer() {
        DockerClientFactory.instance().client().startContainerCmd(get().container.getContainerId()).exec();
    }

    // a pool of one connection through MySQL Connector/J, whatever driver the suites use
    static HikariDataSource throughConnectorJ() {
        return pool(MYSQL_CONNECTOR_J, host(), port());
    }

    // a pool of one connection reaching the database through the given local port (a proxy in front of it), so that a
    // connection silenced there is the one handed out next
    static HikariDataSource through(int port) {
        return pool(get().driver, "127.0.0.1", port);
    }

    static String freshTable() {
        return "cache_" + TABLES.incrementAndGet();
    }

    static Store open(String table, String discriminator) {
        return adapter(dataSource(), table, discriminator).createStore(TestStoreSettings.of(Format.TEXT, Format.TEXT));
    }

    // the adapter of the product under test
    static <K, V> Adapter<K, V> adapter(DataSource dataSource, String table, String discriminator) {
        return adapter(mysql(), dataSource, table, discriminator);
    }

    // the adapter of the other product, which the test database is not
    static <K, V> Adapter<K, V> otherAdapter(String table) {
        return adapter(!mysql(), dataSource(), table, DEFAULT_DISCRIMINATOR);
    }

    static <K, V> Adapter<K, V> adapter(boolean mysql, DataSource dataSource, String table, String discriminator) {
        return mysql ? MySqlAdapter.newBuilder(dataSource, table).withDiscriminator(discriminator).build()
                : MariaDbAdapter.newBuilder(dataSource, table).withDiscriminator(discriminator).build();
    }

    @Override
    public void close() {
        dataSource.close();
        container.stop();
        current = null;
    }
}
