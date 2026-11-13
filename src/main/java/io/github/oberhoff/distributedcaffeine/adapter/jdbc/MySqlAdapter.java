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

import io.github.oberhoff.distributedcaffeine.adapter.jdbc.JdbcStore.Dialect;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;

import javax.sql.DataSource;

import static java.util.Objects.requireNonNull;

/**
 * Stores a cache in a MySQL table, through a JDBC {@link DataSource} of MySQL Connector/J
 * ({@code com.mysql:mysql-connector-j}) or MariaDB Connector/J ({@code org.mariadb.jdbc:mariadb-java-client}). Several
 * caches can share a table, told apart by their discriminator; the table is created if missing.
 * <pre>{@code
 * DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(
 *         MySqlAdapter.newBuilder(dataSource, tableName).build())
 *     .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100_000))
 *     .build();
 * }</pre>
 * Changes made elsewhere arrive by polling, within about the poll interval ({@code StoreConfigurer.withPollInterval},
 * 1 second by default).
 * <p>
 * Requires MySQL 5.7 or later, or a build of it such as Percona Server. A MariaDB server is refused (use
 * {@link MariaDbAdapter}), as is a Galera cluster node (Percona XtraDB Cluster). A failover to an asynchronous
 * replica, MySQL's default, may lose writes the primary had confirmed; the caches notice the failover and check what
 * they hold against the store. Semi-synchronous replication ({@code AFTER_SYNC}) loses no confirmed write.
 * <p>
 * The cache's operation timeout applies to each connection it borrows and to each statement (in whole seconds), so
 * caches sharing a data source may have different ones.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public final class MySqlAdapter<K, V> implements Adapter<K, V> {

    // what the class Javadoc leaves out: before 5.7 the index limit with utf8mb4 refuses the table. A driver of another
    // name, such as a wrapper around one of the two, is taken, untested. Failover: each poll learns which server
    // answers (its server_id); on a change, as on any failed poll, the cache checks what it holds against the store.
    // Tested with utf8mb4 and through both drivers on each major's first release and its latest: 5.7.29, 5.7.44,
    // 8.0.19, 8.4.13, 9.0.0, 9.7.4, 26.7.0, 26.10.0, the cache suites on the latest of each; failovers of a primary
    // with an asynchronous replica on 5.7.29 and the latest of each. 5.7.29 stands for 5.7.9, whose image Docker no
    // longer pulls; 8.0.19 for 8.0.11 on ARM, where its image does not start emulated (on x86 the suites run 8.0.11).

    /**
     * The discriminator of a cache that names none.
     */
    public static final String DEFAULT_DISCRIMINATOR = "default";

    private final DataSource dataSource;
    private final String tableName;
    private final String discriminator;

    private MySqlAdapter(Builder builder) {
        this.dataSource = builder.dataSource;
        this.tableName = builder.tableName;
        this.discriminator = builder.discriminator;
    }

    /**
     * Starts an adapter for a cache in a table.
     *
     * @param dataSource the database
     * @param tableName  the table (letters, digits and underscores, not starting with a digit)
     * @return the builder
     */
    public static Builder newBuilder(DataSource dataSource, String tableName) {
        return new Builder(dataSource, tableName);
    }

    @Override
    public Store createStore(StoreSettings settings) {
        String server = JdbcStore.ensure(dataSource, Dialect.MYSQL, tableName, discriminator,
                settings.operationTimeout());
        return new JdbcStore(dataSource, Dialect.MYSQL, server, tableName, discriminator,
                settings.operationTimeout());
    }

    // caches on the same client wait once for a store that does not answer (Adapter#client)
    @Override
    public Object client() {
        return dataSource;
    }

    @Override
    public String toString() {
        return JdbcStore.name(Dialect.MYSQL, tableName, discriminator);
    }

    /**
     * Builds a {@link MySqlAdapter}.
     */
    public static final class Builder {

        private final DataSource dataSource;
        private final String tableName;
        private String discriminator = DEFAULT_DISCRIMINATOR;

        private Builder(DataSource dataSource, String tableName) {
            this.dataSource = requireNonNull(dataSource, "dataSource cannot be null");
            this.tableName = JdbcStore.requireTableName(tableName);
        }

        /**
         * Sets the discriminator, which tells the caches sharing the table apart. Optional:
         * {@value MySqlAdapter#DEFAULT_DISCRIMINATOR} by default.
         *
         * @param discriminator the discriminator (not blank, at most 200 characters)
         * @return this builder
         */
        public Builder withDiscriminator(String discriminator) {
            this.discriminator = JdbcStore.requireDiscriminator(discriminator);
            return this;
        }

        /**
         * Builds the adapter.
         *
         * @param <K> the key type of the cache
         * @param <V> the value type of the cache
         * @return the adapter
         */
        public <K, V> MySqlAdapter<K, V> build() {
            return new MySqlAdapter<>(this);
        }
    }
}
