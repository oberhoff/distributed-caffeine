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
 * An adapter for MySQL 5.7 or later, through a JDBC data source of MySQL Connector/J
 * ({@code com.mysql:mysql-connector-j}) or MariaDB Connector/J ({@code org.mariadb.jdbc:mariadb-java-client}), which
 * MariaDB supports on MySQL too; both are tested. A driver of another name, such as a wrapper around one of them, is
 * taken, untested.
 * Changes reach the other instances by polling: each instance reads the changes since its last poll every poll
 * interval ({@code StoreConfigurer.withPollInterval}, one second by default), so they arrive within about an
 * interval.
 * <p>
 * The data source must reach MySQL (or a build of it, such as Percona Server): a MariaDB server is refused when the
 * store is created, naming {@link MariaDbAdapter}, as is a version before 5.7 (whose index limit, with the character
 * set utf8mb4, refuses the table).
 * <p>
 * A failover that promotes an asynchronous replica, as MySQL replicates by default, may lose writes the
 * primary had confirmed. The caches then hold none of them for long: each poll learns which server answers (by its
 * {@code server_id}, distinct on every server that replicates), and on a change, as on any failed poll, the cache
 * checks what it holds against the store.
 * Semi-synchronous replication ({@code AFTER_SYNC}) loses no confirmed write.
 * <p>
 * Tested, with the character set utf8mb4 and through both drivers, on each major version's first release and its
 * latest: 5.7.29, 5.7.44, 8.0.19, 8.4.13, 9.0.0, 9.7.4, 26.7.0 and 26.10.0, the cache suites on the latest of each;
 * failovers of a primary with an asynchronous replica on 5.7.29 and on the latest of each. 5.7.29 and 8.0.19 stand
 * for 5.7.9 and 8.0.11, the first releases, whose images do not start emulated on ARM; on x86 the suites run 5.7.13
 * (the first 5.7 image Docker still pulls) and 8.0.11 instead.
 * <p>
 * A table holds the caches of one or more discriminators; it and its index are created if missing (a table of the
 * layout before 2026-10-28, with stamp and basis columns, is refused: drop it while no cache uses it).
 * <pre>{@code
 * DistributedCaffeine.newBuilder(MySqlAdapter.newBuilder(dataSource, "orders_cache").build())
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
public final class MySqlAdapter<K, V> implements Adapter<K, V> {

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
        String server = JdbcStore.ensure(dataSource, Dialect.MYSQL, tableName, settings.operationTimeout());
        return new JdbcStore(dataSource, Dialect.MYSQL, server, tableName, discriminator,
                settings.operationTimeout());
    }

    @Override
    public String toString() {
        return "MySqlAdapter[" + tableName + ", " + discriminator + "]";
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
        public <K, V> MySqlAdapter<K, V> build() {
            return new MySqlAdapter<>(this);
        }
    }
}
