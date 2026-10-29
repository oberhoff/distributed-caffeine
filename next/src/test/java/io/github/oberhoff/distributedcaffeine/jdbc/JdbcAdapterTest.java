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

import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.spi.Write;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The plain JDBC adapter itself, on MariaDB: its schema, its discriminators, and caches distributing through the core's
 * polls only.
 */
@NullMarked
@Timeout(60)
@SuppressWarnings("resource") // every cache a test opens is closed after it (@AfterEach)
class JdbcAdapterTest {

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
                        JdbcAdapter.newBuilder(JdbcTestDatabase.DATA_SOURCE, table).withDiscriminator(discriminator)
                                .build())
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
        cache(JdbcAdapter.DEFAULT_DISCRIMINATOR);
        cache("another");

        try (Connection connection = JdbcTestDatabase.DATA_SOURCE.getConnection();
             ResultSet tables = connection.getMetaData().getTables(null, null,
                     table.toUpperCase(java.util.Locale.ROOT), null)) {
            assertThat(tables.next()).isTrue();
        }
    }

    @Test
    @SuppressWarnings("SqlNoDataSourceInspection") // the SQL runs against the test database, unknown to the IDE
    void a_table_of_the_earlier_layout_is_refused_naming_the_remedy() throws SQLException {
        try (Connection connection = JdbcTestDatabase.DATA_SOURCE.getConnection();
             java.sql.Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + table + " (discriminator VARCHAR(200) NOT NULL, "
                    + "id VARCHAR(200) NOT NULL, stamp BIGINT NOT NULL, basis BIGINT NOT NULL, "
                    + "PRIMARY KEY (discriminator, id))");
        }

        assertThatThrownBy(() -> cache(JdbcAdapter.DEFAULT_DISCRIMINATOR)).isInstanceOf(StoreException.class)
                .hasMessageContaining("earlier version").hasMessageContaining("drop it");
    }

    @Test
    void a_table_name_other_than_an_identifier_is_refused() {
        assertThatThrownBy(() -> JdbcAdapter.newBuilder(JdbcTestDatabase.DATA_SOURCE, "cache; DROP TABLE x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void caches_distribute_through_the_cores_polls() {
        DistributedCache<String, String> writer = cache(JdbcAdapter.DEFAULT_DISCRIMINATOR);
        DistributedCache<String, String> holder = cache(JdbcAdapter.DEFAULT_DISCRIMINATOR);
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
                        : method.invoke(JdbcTestDatabase.DATA_SOURCE, arguments));
    }

    @Test
    void a_connection_goes_back_to_the_data_source_as_it_came() throws SQLException {
        try (Connection shared = JdbcTestDatabase.DATA_SOURCE.getConnection();
             Store store = JdbcAdapter.newBuilder(sharing(shared), table).<String, String>build()
                     .createStore(StoreSettings.of(Format.TEXT, Format.TEXT))) {
            store.write(List.of(write(1)));
            assertThat(shared.getAutoCommit()).as("written in a transaction, then auto-commit again").isTrue();

            shared.setAutoCommit(false);
            store.write(List.of(write(2)));
            assertThat(shared.getAutoCommit()).as("came without auto-commit, goes back without").isFalse();
            assertThat(store.read(List.of("h"), Duration.ofHours(1))).containsKey("h");
        }
    }

    // an unconditional write of one key; versions are the core's to choose, any positive one does here
    private static Write write(long version) {
        return Write.value("h", "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8), version);
    }
}
