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

import io.github.oberhoff.distributedcaffeine.jdbc.JdbcStore.Dialect;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;

import javax.sql.DataSource;

import static java.util.Objects.requireNonNull;

/**
 * An adapter for MariaDB 10.2 or later, through a JDBC data source of MariaDB Connector/J
 * ({@code org.mariadb.jdbc:mariadb-java-client}).
 * Changes reach the other instances by polling: each instance reads the changes since its last poll every poll
 * interval ({@code StoreConfigurer.withPollInterval}, one second by default), so they arrive within about an
 * interval.
 * <p>
 * The data source must reach MariaDB: a MySQL server is refused when the store is created, naming
 * {@link MySqlAdapter}, as is a version before 10.2 (whose index limit, with the character set utf8mb4, refuses
 * the table), and MySQL Connector/J, which neither Oracle nor MariaDB supports on MariaDB. A driver of another name,
 * such as a wrapper around MariaDB Connector/J, is taken, untested.
 * <p>
 * A failover that promotes an asynchronous replica, as MariaDB replicates by default, may lose writes the
 * primary had confirmed. The caches then hold none of them for long: each poll learns which server answers (by its
 * {@code server_id}, distinct on every server that replicates), and on a change, as on any failed poll, the cache
 * checks what it holds against the store.
 * Galera Cluster, where several servers take writes, is not supported: it loses updates even without faults (found
 * by Jepsen in 2026), which breaks the store's conditional writes.
 * <p>
 * Tested, with the character set utf8mb4, on each major version's first release and its latest: 10.2.31, 10.11.19,
 * 11.0.2, 11.8.9, 12.0.2, 12.3.3 and 13.0.2, the cache suites on the latest of each; failovers of a primary with
 * an asynchronous replica on 10.2.31 and on the latest of each. 10.2.31 stands for 10.2.6, the first 10.2 release,
 * whose image does not start emulated on ARM; on x86 the suites run 10.2.6 instead.
 * <p>
 * A table holds the caches of one or more discriminators; it and its index are created if missing (a table of the
 * layout before 2026-10-28, with stamp and basis columns, is refused: drop it while no cache uses it).
 * <pre>{@code
 * DistributedCaffeine.newBuilder(MariaDbAdapter.newBuilder(dataSource, "orders_cache").build())
 *     .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100_000))
 *     .<String, Order>build();
 * }</pre>
 * <p>
 * The cache's operation timeout ({@code StoreConfigurer.withOperationTimeout}) becomes the network timeout of each
 * connection borrowed for an operation, restored before the connection goes back, and the query timeout of each
 * statement (rounded up to whole seconds), so caches sharing a data source may have different ones.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 */
public final class MariaDbAdapter<K, V> implements Adapter<K, V> {

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
        return "MariaDbAdapter[" + tableName + ", " + discriminator + "]";
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
         * @param discriminator the discriminator (not empty, at most 200 characters)
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
