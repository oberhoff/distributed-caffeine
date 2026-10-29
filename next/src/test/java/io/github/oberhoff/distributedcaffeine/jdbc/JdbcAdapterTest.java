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

import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.DistributionMode;
import io.github.oberhoff.distributedcaffeine.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.testing.SilencingProxy;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * The plain JDBC adapter itself, on MariaDB: its schema, its discriminators, and caches distributing through the core's
 * polls only.
 */
@NullMarked
@Timeout(60)
@SuppressWarnings("resource") // every cache a test opens is closed after it (@AfterEach)
abstract class JdbcAdapterTest {

    private final String table = JdbcTestDatabase.freshTable();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private DistributedCache<String, String> cache(String discriminator) {
        DistributedCache<String, String> cache = DistributedCaffeine.<String, String>newBuilder(
                        JdbcTestDatabase.adapter(JdbcTestDatabase.dataSource(), table, discriminator))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000)
                        .withPollInterval(Duration.ofMillis(50)))
                .build();
        closeables.add(cache);
        return cache;
    }

    @Test
    void the_table_and_its_index_are_created_once() throws SQLException {
        cache(JdbcTestDatabase.DEFAULT_DISCRIMINATOR);
        cache("another");

        // whatever case the database keeps names in (MySQL on Linux: as created, case-sensitive)
        try (Connection connection = JdbcTestDatabase.dataSource().getConnection();
             ResultSet tables = connection.getMetaData().getTables(connection.getCatalog(), null, null, null)) {
            boolean found = false;
            while (tables.next()) {
                found |= tables.getString("TABLE_NAME").equalsIgnoreCase(table);
            }
            assertThat(found).as("the table " + table).isTrue();
        }
    }

    @Test
    @SuppressWarnings("SqlNoDataSourceInspection") // the SQL runs against the test database, unknown to the IDE
    void a_table_of_the_earlier_layout_is_refused_naming_the_remedy() throws SQLException {
        try (Connection connection = JdbcTestDatabase.dataSource().getConnection();
             java.sql.Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + table + " (discriminator VARCHAR(200) NOT NULL, "
                    + "id VARCHAR(200) NOT NULL, stamp BIGINT NOT NULL, basis BIGINT NOT NULL, "
                    + "PRIMARY KEY (discriminator, id))");
        }

        assertThatThrownBy(() -> cache(JdbcTestDatabase.DEFAULT_DISCRIMINATOR)).isInstanceOf(StoreException.class)
                .hasMessageContaining("earlier version").hasMessageContaining("drop it");
    }

    @Test
    void a_table_name_other_than_an_identifier_is_refused() {
        DataSource dataSource = JdbcTestDatabase.dataSource();
        assertThatThrownBy(() -> MariaDbAdapter.newBuilder(dataSource, "cache; DROP TABLE x"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MySqlAdapter.newBuilder(dataSource, "cache; DROP TABLE x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // either driver reaches either server: the product is told by the server's version, and the adapter of the
    // other one refused, naming the right one
    @Test
    void the_other_products_adapter_is_refused_naming_the_right_one() {
        String right = JdbcTestDatabase.mysql() ? "MySqlAdapter" : "MariaDbAdapter";
        Adapter<String, String> other = JdbcTestDatabase.otherAdapter(table);
        StoreSettings settings = StoreSettings.of(Format.TEXT, Format.TEXT);
        assertThatThrownBy(() -> other.createStore(settings))
                .isInstanceOf(StoreException.class).hasMessageContaining("use " + right);
    }

    // neither Oracle nor MariaDB supports MySQL Connector/J on MariaDB: where it connects, it is refused; on MariaDB
    // 11.0 it does not even connect (it takes the version for MySQL 8's and asks for transaction_isolation, which
    // MariaDB has from 11.1 on)
    @Test
    void mysql_connector_j_is_refused_on_mariadb_naming_the_right_driver() {
        assumeFalse(JdbcTestDatabase.mysql(), "MariaDB only");
        HikariDataSource connectorJ = JdbcTestDatabase.throughConnectorJ();
        closeables.add(connectorJ);

        Adapter<String, String> adapter =
                JdbcTestDatabase.adapter(connectorJ, table, JdbcTestDatabase.DEFAULT_DISCRIMINATOR);
        StoreSettings settings = StoreSettings.of(Format.TEXT, Format.TEXT);
        assertThatThrownBy(() -> adapter.createStore(settings))
                .isInstanceOf(StoreException.class)
                .hasMessageMatching("(?s).*(use MariaDB Connector/J|Creating the table " + table + " failed).*");
    }

    // by the error codes both products share and the SQLState classes, whatever exception class the driver chose
    @Test
    void failures_are_classified_by_the_servers_error_whatever_the_driver() {
        assertThat(JdbcStore.rejected(new SQLException("deadlock", "40001", 1213))).isTrue();
        assertThat(JdbcStore.rejected(new SQLException("lock wait timeout", "HY000", 1205))).isTrue();
        assertThat(JdbcStore.rejected(new SQLException("read only", "HY000", 1290))).isTrue();
        assertThat(JdbcStore.rejected(new SQLException("serialization", "40001", 0))).isTrue();
        assertThat(JdbcStore.rejected(new SQLException("syntax", "42000", 1064))).isFalse();
        assertThat(JdbcStore.connectionLost(new SQLException("link failure", "08S01", 0))).isTrue();
        assertThat(JdbcStore.connectionLost(new SQLException("lock wait timeout", "HY000", 1205))).isFalse();
    }

    @Test
    void the_product_and_its_floor_are_told_by_the_servers_version() {
        assertThat(JdbcStore.Dialect.of("11.8.2-MariaDB-ubu2404")).isEqualTo(JdbcStore.Dialect.MARIADB);
        assertThat(JdbcStore.Dialect.of("8.0.36-28")).isEqualTo(JdbcStore.Dialect.MYSQL);
        assertThat(JdbcStore.Dialect.MARIADB.supports("10.2.6-MariaDB")).isTrue();
        assertThat(JdbcStore.Dialect.MARIADB.supports("10.1.48-MariaDB")).isFalse();
        assertThat(JdbcStore.Dialect.MARIADB.supports("13.0.2-MariaDB")).isTrue();
        assertThat(JdbcStore.Dialect.MYSQL.supports("5.7.9-log")).isTrue();
        assertThat(JdbcStore.Dialect.MYSQL.supports("5.6.51")).isFalse();
        assertThat(JdbcStore.Dialect.MYSQL.supports("26.10.0")).isTrue();
    }

    @Test
    void caches_distribute_through_the_cores_polls() {
        DistributedCache<String, String> writer = cache(JdbcTestDatabase.DEFAULT_DISCRIMINATOR);
        DistributedCache<String, String> holder = cache(JdbcTestDatabase.DEFAULT_DISCRIMINATOR);
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        writer.put("k", "new");
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("new");

        writer.invalidateAll();
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isNull();
        assertThat(holder.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);
    }

    @Test
    void discriminators_keep_caches_in_one_table_apart() {
        DistributedCache<String, String> orders = cache("orders");
        DistributedCache<String, String> users = cache("users");

        orders.put("k", "order");
        users.put("k", "user");
        users.invalidateAll();

        assertThat(orders.distributedPolicy().getFromStore("k")).isNotNull();
        assertThat(users.distributedPolicy().getFromStore("k")).isNull();
    }

    // a data source that does not reset its connections: one connection, handed out again, closed by nobody
    private static DataSource sharing(Connection shared) {
        Connection unclosable = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    try {
                        return method.invoke(shared, arguments);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> method.getName().equals("getConnection") ? unclosable
                        : method.invoke(JdbcTestDatabase.dataSource(), arguments));
    }

    @Test
    void a_connection_goes_back_to_the_data_source_as_it_came() throws SQLException {
        try (Connection shared = JdbcTestDatabase.dataSource().getConnection();
             Store store = JdbcTestDatabase.<String, String>adapter(sharing(shared), table,
                     JdbcTestDatabase.DEFAULT_DISCRIMINATOR).createStore(StoreSettings.of(Format.TEXT, Format.TEXT))) {
            store.write(List.of(write(1)));
            assertThat(shared.getAutoCommit()).as("written in a transaction, then auto-commit again").isTrue();

            shared.setAutoCommit(false);
            store.write(List.of(write(2)));
            assertThat(shared.getAutoCommit()).as("came without auto-commit, goes back without").isFalse();
            assertThat(store.read(List.of("h"), Duration.ofHours(1))).containsKey("h");
        }
    }

    // an unconditional write of one key; versions are the core's to choose, any positive one does here
    // a data source the network silently stops carrying: the query timeout alone would not end the wait, MariaDB's
    // driver enforces it in the database (max_statement_time), which never receives the statement

    @Test
    void an_operation_on_a_silent_connection_fails_within_the_timeout_instead_of_hanging() throws Exception {
        SilencingProxy proxy = new SilencingProxy(JdbcTestDatabase.host(), JdbcTestDatabase.port());
        closeables.add(proxy);
        HikariDataSource pool = JdbcTestDatabase.through(proxy.port());
        closeables.add(pool);
        DistributedCache<String, String> cache = DistributedCaffeine.<String, String>newBuilder(
                        JdbcTestDatabase.adapter(pool, table, JdbcTestDatabase.DEFAULT_DISCRIMINATOR))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000)
                        .withOperationTimeout(Duration.ofSeconds(2)))
                .withDistributionMode(DistributionMode.NONE) // no polls taking the pool's only connection
                .build();
        closeables.add(cache);
        cache.put("k", "before");

        // a write: failing with its outcome unknown, and not retried. At once after silencing, so that the pool hands
        // the silenced connection out again without checking it first
        proxy.silenceOpenConnections();
        long started = System.nanoTime();
        assertThatThrownBy(() -> cache.put("k", "during"))
                .isInstanceOfSatisfying(StoreException.class, e -> {
                    assertThat(e.getMessage()).contains("got no answer within"); // a timeout, not a connection lost
                    assertThat(e.isOutcomeUnknown()).isTrue();
                });
        assertThat(Duration.ofNanos(System.nanoTime() - started)).as("failing after the timeout, not hanging")
                .isLessThan(Duration.ofSeconds(6));

        // the broken connection is gone from the pool; a new one is carried
        cache.put("k", "after");

        // a read: failing, its outcome no question
        proxy.silenceOpenConnections();
        started = System.nanoTime();
        assertThatThrownBy(() -> cache.distributedPolicy().getFromStore("k"))
                .isInstanceOfSatisfying(StoreException.class, e -> {
                    assertThat(e.getMessage()).contains("got no answer within");
                    assertThat(e.isOutcomeUnknown()).isFalse();
                });
        assertThat(Duration.ofNanos(System.nanoTime() - started)).as("failing after the timeout, not hanging")
                .isLessThan(Duration.ofSeconds(6));

        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).isEqualTo("after");
    }

    private static Write write(long version) {
        return Write.value("h", "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8), version);
    }
}
