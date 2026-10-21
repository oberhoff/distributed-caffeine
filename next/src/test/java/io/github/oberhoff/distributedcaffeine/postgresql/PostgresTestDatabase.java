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

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports.Binding;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.testing.Ports;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.jspecify.annotations.Nullable;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static java.util.Objects.requireNonNull;

/**
 * The PostgreSQL store of the topology running now, started by its test class before its suites and stopped after
 * them ({@link PostgresTopologyTests}): one at a time, so the suites find it with {@link #get()}. Every server on a
 * network of its own and a fixed host port, which it keeps across a restart; a pooled JDBC data source and one
 * listener connection factory (so that the suites share the listening session, as caches of one database do).
 * Built per topology:
 * <ul>
 *     <li>single server, and one with a notification queue of 256 pages (PostgreSQL 17 or later);</li>
 *     <li>primary with a standby: the standby copied from the primary by {@code pg_basebackup -R} and streaming from
 *     it; synchronous with {@code synchronous_standby_names = '*'}, set on the primary before the copy, so that a
 *     promoted standby would wait for a standby of its own in turn (which promoting here takes back);</li>
 *     <li>single server behind PgBouncer: the data source through it in transaction mode, the listener directly (it
 *     needs a session of its own); a second database name lends the server in statement mode.</li>
 * </ul>
 * A failover suite breaks its topology on purpose; {@link #ensureIntact()} builds it anew before the next test.
 */
// the SQL runs against the test containers; containers and the shared Docker client are closed with the store
@SuppressWarnings({"SqlNoDataSourceInspection", "resource"})
final class PostgresTestDatabase implements AutoCloseable {

    static final String LISTENER_APPLICATION = "dc-test-listener";
    static final String STATEMENT_POOLED = "statement_pooled";
    private static final String USER = "test";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "test";
    private static final String PGDATA = "/var/lib/postgresql/data/pgdata";
    private static final String POOLER_IMAGE = "pgbouncer/pgbouncer:latest";
    private static final int POOLER_PORT = 6432;
    private static final Duration READY = Duration.ofMinutes(2);
    private static final AtomicInteger TABLES = new AtomicInteger();
    private static volatile @Nullable PostgresTestDatabase current;

    final PostgresTopology topology;
    private final String image;
    private final int major;
    private final List<GenericContainer<?>> containers = new ArrayList<>();
    private Network network;
    private GenericContainer<?> primary;
    private @Nullable GenericContainer<?> standby;
    private @Nullable GenericContainer<?> pooler;
    private int primaryPort;
    private int standbyPort;
    private boolean broken;
    HikariDataSource dataSource;
    ConnectionFactory listener;

    private PostgresTestDatabase(String image, PostgresTopology topology) {
        this.image = image;
        this.topology = topology;
        this.major = major(image);
        build();
    }

    /**
     * Starts the store of a topology; the suites find it with {@link #get()} until it is closed.
     *
     * @param image    the image
     * @param topology the topology
     * @return the store
     */
    static PostgresTestDatabase start(String image, PostgresTopology topology) {
        if (current != null) {
            throw new IllegalStateException("another PostgreSQL topology is running: one at a time");
        }
        PostgresTestDatabase started = new PostgresTestDatabase(image, topology);
        current = started;
        return started;
    }

    /**
     * Returns the store of the topology running now.
     *
     * @return the store
     */
    static PostgresTestDatabase get() {
        return requireNonNull(current, "no PostgreSQL topology is running: the PostgreSQL suites run nested in "
                + "PostgresIntegrationTest");
    }

    static String freshTable() {
        return "cache_" + TABLES.incrementAndGet();
    }

    // the major version of an image tag such as postgres:9.6.0 or postgres:18; 9 for 9.6
    static int major(String image) {
        String tag = image.substring(image.indexOf(':') + 1);
        int dot = tag.indexOf('.');
        return Integer.parseInt(dot < 0 ? tag : tag.substring(0, dot));
    }

    private void build() {
        network = Network.newNetwork();
        List<Integer> ports = Ports.startOnFree(topology.hasStandby() ? 2 : 1, this::startServers);
        primaryPort = ports.get(0);
        standbyPort = topology.hasStandby() ? ports.get(1) : -1;
        if (topology == PostgresTopology.SINGLE_SERVER_BEHIND_TRANSACTION_POOLER) {
            startPooler();
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(pooler == null ? jdbcUrl() : "jdbc:postgresql://" + pooler.getHost() + ":"
                + pooler.getMappedPort(POOLER_PORT) + "/" + DATABASE + "?prepareThreshold=0");
        config.setUsername(USER);
        config.setPassword(PASSWORD);
        config.setMaximumPoolSize(16);
        config.setInitializationFailTimeout(-1);
        dataSource = new HikariDataSource(config);
        listener = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(host())
                .port(primaryPort)
                .username(USER)
                .password(PASSWORD)
                .database(DATABASE)
                .applicationName(LISTENER_APPLICATION)
                .build());
        broken = false;
    }

    // the primary, then the standby if any; on failure, what was started is stopped again
    private void startServers(List<Integer> ports) {
        primaryPort = ports.get(0);
        List<String> command = new ArrayList<>(List.of("postgres"));
        if (topology.hasStandby()) {
            command.addAll(List.of("-c", "wal_level=replica", "-c", "max_wal_senders=5"));
        }
        if (topology == PostgresTopology.SINGLE_SERVER_WITH_SMALL_NOTIFY_QUEUE) {
            command.addAll(List.of("-c", "max_notify_queue_pages=256"));
        }
        primary = server(ports.get(0))
                .withNetworkAliases("primary")
                // replication connections from the standby, which the image does not allow by default
                .withCopyToContainer(Transferable.of("echo 'host replication all all trust' >> \"$PGDATA/pg_hba.conf\"\n"),
                        "/docker-entrypoint-initdb.d/replication.sh")
                .withCommand(command.toArray(String[]::new))
                .waitingFor(Wait.forLogMessage("(?s).*database system is ready to accept connections.*", 2));
        try {
            primary.start();
            if (!topology.hasStandby()) {
                return;
            }
            if (topology == PostgresTopology.PRIMARY_WITH_SYNCHRONOUS_STANDBY) {
                // after initdb (which would wait for a standby there cannot be yet), before the copy
                execute("ALTER SYSTEM SET synchronous_standby_names = '*'");
                execute("SELECT pg_reload_conf()");
            }
            int port = ports.get(1);
            standby = server(port)
                    .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("bash", "-c", """
                            set -e
                            until pg_isready -h primary -U test; do sleep 1; done
                            mkdir -p "$PGDATA" && chown postgres "$PGDATA" && chmod 700 "$PGDATA"
                            gosu postgres pg_basebackup -h primary -U test -D "$PGDATA" -R -X stream
                            exec gosu postgres postgres -c hot_standby=on
                            """).withHostConfig(Objects.requireNonNull(cmd.getHostConfig()).withPortBindings(
                            new PortBinding(Binding.bindPort(port), ExposedPort.tcp(5432)))))
                    .waitingFor(Wait.forLogMessage("(?s).*ready to accept read.only connections.*", 1));
            standby.start();
            String state = topology == PostgresTopology.PRIMARY_WITH_SYNCHRONOUS_STANDBY ? "sync" : "async";
            require(() -> state.equals(queryQuietly(ports.get(0),
                    "SELECT sync_state FROM pg_stat_replication WHERE state = 'streaming'")), "standby " + state);
        } catch (RuntimeException e) {
            stopContainers();
            throw e;
        }
    }

    @SuppressWarnings("resource") // stopped with the store
    private GenericContainer<?> server(int port) {
        GenericContainer<?> server = new GenericContainer<>(image)
                .withNetwork(network)
                .withEnv("POSTGRES_USER", USER)
                .withEnv("POSTGRES_PASSWORD", PASSWORD)
                .withEnv("POSTGRES_DB", DATABASE)
                .withEnv("PGDATA", PGDATA)
                .withExposedPorts(5432)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(Objects.requireNonNull(cmd.getHostConfig())
                        .withPortBindings(new PortBinding(Binding.bindPort(port), ExposedPort.tcp(5432)))));
        containers.add(server);
        return server;
    }

    // PgBouncer in transaction mode, logging in as the image's user for anyone; the second name in statement mode
    @SuppressWarnings("resource") // stopped with the store
    private void startPooler() {
        String server = "host=primary port=5432 user=" + USER + " password=" + PASSWORD;
        pooler = new GenericContainer<>(POOLER_IMAGE)
                .withNetwork(network)
                .withEnv("DATABASES", STATEMENT_POOLED + " = " + server + " dbname=" + DATABASE
                        + " pool_mode=statement,* = " + server)
                .withEnv("PGBOUNCER_POOL_MODE", "transaction")
                .withEnv("PGBOUNCER_LISTEN_PORT", String.valueOf(POOLER_PORT))
                .withEnv("PGBOUNCER_MAX_CLIENT_CONN", "1000")
                .withEnv("PGBOUNCER_DEFAULT_POOL_SIZE", "20")
                .withEnv("PGBOUNCER_IGNORE_STARTUP_PARAMETERS", "extra_float_digits,options")
                .withExposedPorts(POOLER_PORT)
                .waitingFor(Wait.forLogMessage("(?s).*process up.*", 1));
        containers.add(pooler);
        pooler.start();
    }

    String host() {
        return "localhost";
    }

    int port() {
        return primaryPort;
    }

    int standbyPort() {
        requireStandby();
        return standbyPort;
    }

    String username() {
        return USER;
    }

    String password() {
        return PASSWORD;
    }

    String databaseName() {
        return DATABASE;
    }

    int major() {
        return major;
    }

    String jdbcUrl() {
        return "jdbc:postgresql://" + host() + ":" + primaryPort + "/" + DATABASE;
    }

    String poolerHost() {
        return requireNonNull(pooler, topology + " has no pooler").getHost();
    }

    int poolerPort() {
        return requireNonNull(pooler, topology + " has no pooler").getMappedPort(POOLER_PORT);
    }

    // terminates every listening session: the listener reconnects, which is a gap
    void terminateListeners() {
        execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = '"
                + LISTENER_APPLICATION + "'");
    }

    void execute(String sql) {
        try (Connection connection = DriverManager.getConnection(jdbcUrl(), USER, PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Runs a statement on the standby directly; the first column of its first row, if any.
     *
     * @param sql the statement
     * @return the first column of the first row, or null
     * @throws SQLException if it fails
     */
    @Nullable String onStandby(String sql) throws SQLException {
        return query(standbyPort(), sql);
    }

    private static @Nullable String query(int port, String sql) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:" + port + "/"
                + DATABASE, USER, PASSWORD);
             Statement statement = connection.createStatement()) {
            if (!statement.execute(sql)) {
                return null;
            }
            try (ResultSet resultSet = statement.getResultSet()) {
                return resultSet.next() ? resultSet.getString(1) : null;
            }
        }
    }

    private static @Nullable String queryQuietly(int port, String sql) {
        try {
            return query(port, sql);
        } catch (SQLException e) {
            return null;
        }
    }

    // the servers, as a restart for an update or a failure does it: on their data, at their ports

    void stopPrimary() {
        broken = true;
        DockerClientFactory.instance().client().stopContainerCmd(primary.getContainerId()).exec();
    }

    void killPrimary() {
        broken = true;
        DockerClientFactory.instance().client().killContainerCmd(primary.getContainerId()).exec();
    }

    void startPrimary() {
        DockerClientFactory.instance().client().startContainerCmd(primary.getContainerId()).exec();
    }

    /**
     * Promotes the standby ({@code pg_ctl promote}, which every supported version has) and waits until it writes; a
     * synchronous standby promoted alone stops waiting for a standby of its own.
     */
    void promoteStandby() {
        requireStandby();
        broken = true;
        GenericContainer<?> promoted = requireNonNull(standby);
        try {
            ExecResult result = promoted.execInContainer("gosu", "postgres", "pg_ctl", "promote", "-D", PGDATA);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("promoting failed: " + result.getStderr());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        require(() -> "f".equals(queryQuietly(standbyPort, "SELECT pg_is_in_recovery()")), "standby promoted");
        if (topology == PostgresTopology.PRIMARY_WITH_SYNCHRONOUS_STANDBY) {
            queryQuietly(standbyPort, "ALTER SYSTEM RESET synchronous_standby_names");
            queryQuietly(standbyPort, "SELECT pg_reload_conf()");
        }
    }

    // a test that cut the standby off from the primary
    void markBroken() {
        broken = true;
    }

    /**
     * Builds the topology anew if a test broke it (a primary stopped, a standby promoted).
     */
    void ensureIntact() {
        if (broken) {
            dataSource.close();
            stopContainers();
            build();
        }
    }

    private void requireStandby() {
        if (!topology.hasStandby()) {
            throw new IllegalStateException(topology + " has no standby");
        }
    }

    private static void require(java.util.function.BooleanSupplier condition, String what) {
        if (!awaitUpTo(condition, READY, Duration.ofMillis(250))) {
            throw new AssertionError(what + " not reached within " + READY.toSeconds() + " s");
        }
    }

    private void stopContainers() {
        for (int index = containers.size() - 1; index >= 0; index--) {
            containers.get(index).stop();
        }
        containers.clear();
        standby = null;
        pooler = null;
        network.close();
    }

    @Override
    public void close() {
        current = null;
        try {
            dataSource.close();
        } finally {
            stopContainers();
        }
    }
}
