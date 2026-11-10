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
 * Stores a cache in a MariaDB table, through a JDBC {@link DataSource} of MariaDB Connector/J
 * ({@code org.mariadb.jdbc:mariadb-java-client}). Several caches can share a table, told apart by their discriminator;
 * the table is created if missing.
 * <pre>{@code
 * DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(
 *         MariaDbAdapter.newBuilder(dataSource, tableName).build())
 *     .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100_000))
 *     .build();
 * }</pre>
 * Changes made elsewhere arrive by polling, within about the poll interval ({@code StoreConfigurer.withPollInterval},
 * one second by default).
 * <p>
 * Requires MariaDB 10.2 or later. A MySQL server is refused (use {@link MySqlAdapter}), as is MySQL Connector/J, which
 * is not supported on MariaDB. A Galera cluster node is refused. A failover to an asynchronous replica, MariaDB's
 * default, may lose writes the primary had confirmed; the caches notice the failover and check what they hold against
 * the store.
 * <p>
 * The cache's operation timeout applies to each connection it borrows and to each statement (in whole seconds), so
 * caches sharing a data source may have different ones.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public final class MariaDbAdapter<K, V> implements Adapter<K, V> {

    // what the class Javadoc leaves out: before 10.2 the index limit with utf8mb4 refuses the table. A driver of
    // another name, such as a wrapper around MariaDB Connector/J, is taken, untested. Failover: each poll learns which
    // server answers (its server_id, distinct on every server that replicates); on a change, as on any failed poll, the
    // cache checks what it holds against the store. Galera loses updates even without faults (Jepsen, 2026), which
    // breaks the conditional writes. A table of the layout before 2026-10-28 (stamp and basis columns) is refused.
    // Tested with utf8mb4 on each major's first release and its latest: 10.2.31, 10.11.19, 11.0.2, 11.8.9, 12.0.2,
    // 12.3.3, 13.0.2, the cache suites on the latest of each; failovers of a primary with an asynchronous replica on
    // 10.2.31 and the latest of each. 10.2.31 stands for 10.2.6, whose image does not start emulated on ARM; on x86 the
    // suites run 10.2.6.

    /**
     * The discriminator of a cache that names none.
     */
    public static final String DEFAULT_DISCRIMINATOR = "default";

    private final DataSource dataSource;
    private final String tableName;
    private final String discriminator;

    private MariaDbAdapter(Builder builder) {
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
        String server = JdbcStore.ensure(dataSource, Dialect.MARIADB, tableName, settings.operationTimeout());
        return new JdbcStore(dataSource, Dialect.MARIADB, server, tableName, discriminator,
                settings.operationTimeout());
    }

    @Override
    public String toString() {
        return "MariaDB " + tableName + "/" + discriminator;
    }

    /**
     * Builds a {@link MariaDbAdapter}.
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
         * {@value MariaDbAdapter#DEFAULT_DISCRIMINATOR} by default.
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
        public <K, V> MariaDbAdapter<K, V> build() {
            return new MariaDbAdapter<>(this);
        }
    }
}
