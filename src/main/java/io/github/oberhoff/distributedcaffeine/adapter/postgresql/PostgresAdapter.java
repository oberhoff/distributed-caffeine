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
package io.github.oberhoff.distributedcaffeine.adapter.postgresql;

import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * Stores a cache in a PostgreSQL table, through the application's JDBC {@link DataSource}. Several caches can share a
 * table, told apart by their discriminator.
 * <pre>{@code
 * PostgresAdapter<Key, Value> adapter = PostgresAdapter.newBuilder(dataSource, schemaName, tableName)
 *     .withDiscriminator(discriminator)
 *     .build();
 * }</pre>
 * An adapter serves one cache: building a second cache with the same adapter fails while the first is open.
 * <p>
 * Requires {@code org.postgresql:postgresql} and PostgreSQL 9.6 or later; a standby is refused. The cache's operation
 * timeout applies to each connection it borrows, so caches sharing a data source may have different ones.
 * <p>
 * To receive changes (every distribution mode but {@code NONE}), the caches on a data source hold one of its
 * connections for as long as they listen. A pool's leak detection (such as HikariCP's {@code leakDetectionThreshold})
 * reports it as a leak, which it is not. A pooler in transaction mode (such as PgBouncer's
 * {@code pool_mode = transaction}) cannot listen: give such a setup a data source of its own for listening
 * ({@link Builder#withListenerDataSource(DataSource)}).
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public final class PostgresAdapter<K, V> implements Adapter<K, V> {

    /**
     * The discriminator used if none is configured.
     */
    public static final String DEFAULT_DISCRIMINATOR = "default";


    private final DataSource dataSource;
    private final String schemaName;
    private final String tableName;
    private final String discriminator;
    private final DataSource listenerDataSource;
    private final boolean schemaReset;

    private PostgresAdapter(Builder builder) {
        this.dataSource = builder.dataSource;
        this.schemaName = builder.schemaName;
        this.tableName = builder.tableName;
        this.discriminator = builder.discriminator;
        this.listenerDataSource = builder.listenerDataSource != null ? builder.listenerDataSource : builder.dataSource;
        this.schemaReset = builder.schemaReset;
    }

    /**
     * Starts a builder for an adapter on the given table.
     *
     * @param dataSource the data source for reads, writes and listening (not a standby)
     * @param schemaName the schema of the table (at most 63 bytes, quoted, so case is kept)
     * @param tableName  the table (at most 63 bytes, quoted)
     * @return the builder
     */
    public static Builder newBuilder(DataSource dataSource, String schemaName, String tableName) {
        return new Builder(dataSource, schemaName, tableName);
    }

    @Override
    public Store createStore(StoreSettings settings) {
        PostgresTable table = new PostgresTable(schemaName, tableName, discriminator, settings.keyFormat(),
                settings.valueFormat());
        PostgresSchema.ensure(dataSource, table, schemaReset, settings.operationTimeout());
        return new PostgresStore(dataSource, listenerDataSource, table, settings.operationTimeout());
    }

    @Override
    public String toString() {
        return "PostgresAdapter[" + schemaName + "." + tableName + ", discriminator " + discriminator + "]";
    }

    /**
     * The builder of a {@link PostgresAdapter}.
     */
    public static final class Builder {

        private final DataSource dataSource;
        private final String schemaName;
        private final String tableName;
        private String discriminator = DEFAULT_DISCRIMINATOR;
        private @Nullable DataSource listenerDataSource;
        private boolean schemaReset;

        private Builder(DataSource dataSource, String schemaName, String tableName) {
            requireDriver();
            this.dataSource = requireNonNull(dataSource, "dataSource cannot be null");
            this.schemaName = checkedName(schemaName, "schemaName");
            this.tableName = checkedName(tableName, "tableName");
        }

        /**
         * Sets the discriminator that tells this cache's rows apart from those of other caches in the same table.
         * Optional: {@value #DEFAULT_DISCRIMINATOR} by default.
         *
         * @param discriminator the discriminator (not blank)
         * @return this builder
         */
        public Builder withDiscriminator(String discriminator) {
            requireNonNull(discriminator, "discriminator cannot be null");
            if (discriminator.isBlank()) {
                throw new IllegalArgumentException("discriminator cannot be blank");
            }
            this.discriminator = discriminator;
            return this;
        }

        /**
         * Sets a data source of its own to listen for changes through, for a data source that cannot listen, such as
         * one through a pooler in transaction mode. It must reach the same database, directly or through a pooler in
         * session mode, with pgjdbc's connections ({@code org.postgresql}), also behind a pool. Optional.
         *
         * @param listenerDataSource the data source to listen through
         * @return this builder
         */
        public Builder withListenerDataSource(DataSource listenerDataSource) {
            this.listenerDataSource = requireNonNull(listenerDataSource, "listenerDataSource cannot be null");
            return this;
        }

        /**
         * Drops and recreates the table if another version of this library created it. Only while no instance of any
         * cache in the table runs.
         *
         * @return this builder
         */
        public Builder withSchemaReset() {
            this.schemaReset = true;
            return this;
        }


        /**
         * Builds the adapter; the table is checked or created when the cache is built.
         *
         * @param <K> the key type of the cache
         * @param <V> the value type of the cache
         * @return the adapter
         */
        public <K, V> PostgresAdapter<K, V> build() {
            return new PostgresAdapter<>(this);
        }

        private static String checkedName(String name, String what) {
            requireNonNull(name, what + " cannot be null");
            if (name.isBlank()) {
                throw new IllegalArgumentException(what + " cannot be blank");
            }
            // PostgreSQL would truncate it silently, and the table would then not be found under the given name
            if (name.getBytes(StandardCharsets.UTF_8).length > PostgresTable.MAXIMUM_NAME_BYTES) {
                throw new IllegalArgumentException(what + " is longer than " + PostgresTable.MAXIMUM_NAME_BYTES
                        + " bytes: " + name);
            }
            return name;
        }

        // the driver is an optional dependency of this library: a clear message where it is missing
        private static void requireDriver() {
            try {
                Class.forName("org.postgresql.PGConnection", false, PostgresAdapter.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("PostgresAdapter requires org.postgresql:postgresql on the classpath",
                        e);
            }
        }
    }
}
