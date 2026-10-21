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

import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.r2dbc.spi.ConnectionFactory;
import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * Where one cache lives in PostgreSQL: a table (shared by several caches through the discriminator), read and
 * written through the user's JDBC {@link DataSource}, and listened to through an R2DBC connection factory.
 * <pre>{@code
 * PostgresAdapter<String, Order> adapter = PostgresAdapter.<String, Order>newBuilder(dataSource, "app", "cache")
 *     .withDiscriminator("orders")
 *     .withListener(jdbcUrl, user, password)
 *     .build();
 * }</pre>
 * One adapter per cache: {@code build()} of a second cache with the same adapter object fails while the first is
 * open.
 * <p>
 * Requires {@code org.postgresql:postgresql}; listening additionally requires {@code org.postgresql:r2dbc-postgresql}
 * (not needed with {@code DistributionMode.NONE}). PostgreSQL 9.6 or later; standbys are refused.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
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
    private final @Nullable ConnectionFactory listenerConnectionFactory;
    private final boolean schemaReset;

    private PostgresAdapter(Builder builder) {
        this.dataSource = builder.dataSource;
        this.schemaName = builder.schemaName;
        this.tableName = builder.tableName;
        this.discriminator = builder.discriminator;
        this.listenerConnectionFactory = builder.listenerConnectionFactory;
        this.schemaReset = builder.schemaReset;
    }

    /**
     * Starts a builder for an adapter on the given table.
     *
     * @param dataSource the data source for reads and writes (not a standby)
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
        PostgresSchema.ensure(dataSource, table, schemaReset, settings.timeout());
        return PostgresStore.of(dataSource, table, listenerConnectionFactory, settings.timeout());
    }

    @Override
    public String toString() {
        return "PostgresAdapter[" + schemaName + "." + tableName + ", discriminator " + discriminator
                + (listenerConnectionFactory == null ? ", without listener (withListener)" : "") + "]";
    }

    /**
     * The builder of a {@link PostgresAdapter}.
     */
    public static final class Builder {

        private final DataSource dataSource;
        private final String schemaName;
        private final String tableName;
        private String discriminator = DEFAULT_DISCRIMINATOR;
        private @Nullable ConnectionFactory listenerConnectionFactory;
        private boolean schemaReset;

        private Builder(DataSource dataSource, String schemaName, String tableName) {
            requireDriver("org.postgresql.PGConnection", "org.postgresql:postgresql");
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
         * Sets the R2DBC connection factory to listen for notifications through (required unless the cache uses
         * {@code DistributionMode.NONE}). Listening sessions are shared per connection factory (by identity), so
         * pass the same object to every adapter of a database. It must reach the database directly or through a
         * session-mode pooler: transaction-mode poolers cannot carry {@code LISTEN}, which the start detects.
         *
         * @param connectionFactory the connection factory (not a pool)
         * @return this builder
         */
        public Builder withListener(ConnectionFactory connectionFactory) {
            requireDriver("io.r2dbc.postgresql.PostgresqlConnectionFactory", "org.postgresql:r2dbc-postgresql");
            this.listenerConnectionFactory = requireNonNull(connectionFactory, "connectionFactory cannot be null");
            return this;
        }

        /**
         * Sets the listener from JDBC connection details: {@code jdbc:postgresql://host:port/database?options}
         * becomes {@code r2dbc:postgresql://...}. Every call creates a new connection factory, so for sharing one
         * listening session between the caches of a database, create it once with {@link #withListener(ConnectionFactory)}.
         *
         * @param jdbcUrl  the JDBC URL
         * @param user     the user
         * @param password the password
         * @return this builder
         */
        public Builder withListener(String jdbcUrl, String user, String password) {
            requireDriver("io.r2dbc.postgresql.PostgresqlConnectionFactory", "org.postgresql:r2dbc-postgresql");
            this.listenerConnectionFactory = PostgresListener.connectionFactory(jdbcUrl, user, password);
            return this;
        }

        /**
         * Drops and recreates the table if it exists without the schema marker of this version (e.g. a table of the
         * current generation under the same name). Only when every instance of every cache in that table is
         * stopped; concurrent starts are serialized by an advisory lock.
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

        private static void requireDriver(String className, String dependency) {
            try {
                Class.forName(className, false, PostgresAdapter.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("PostgresAdapter requires " + dependency + " on the classpath", e);
            }
        }
    }
}
