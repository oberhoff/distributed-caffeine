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

import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.PrimitiveStore;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;

import javax.sql.DataSource;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * An adapter for SQL databases reached through plain JDBC (VISION.md, "Design study: portable store primitives",
 * step 5): it implements only the primitives of {@link PrimitiveStore} in portable SQL (sequences, {@code UPDATE}
 * then {@code INSERT} for an upsert, {@code INSERT ... SELECT} for a conditional insert), which builds the rest of
 * the store from them, and the cache polls it for changes, having no notifications. What SQL leaves to each database
 * (the types of bytes and times, the time now) comes from the product name. Tested on MariaDB (10.3 or later, which
 * has sequences); databases speaking the standard (H2, HSQLDB) should work but are untested. Not MySQL (no
 * sequences), PostgreSQL (its own adapter), SQL Server, Oracle or DB2 (their own types or syntax), which would need
 * a dialect each. A table holds the caches of one or more discriminators; it, its index and its sequence are created
 * if missing.
 * <pre>{@code
 * DistributedCaffeine.newBuilder(JdbcAdapter.newBuilder(dataSource, "orders_cache").build())
 *     .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100_000))
 *     .<String, Order>build();
 * }</pre>
 * Tested on MariaDB. Databases with their own notifications have adapters of their own, which are faster (one round
 * trip per batch, changes pushed instead of polled).
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 */
public final class JdbcAdapter<K, V> implements Adapter<K, V> {

    /**
     * The discriminator of a cache that names none.
     */
    public static final String DEFAULT_DISCRIMINATOR = "default";

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,50}");

    private final DataSource dataSource;
    private final String table;
    private final String discriminator;

    private JdbcAdapter(Builder builder) {
        this.dataSource = builder.dataSource;
        this.table = builder.table;
        this.discriminator = builder.discriminator;
    }

    /**
     * Starts an adapter for a cache in a table.
     *
     * @param dataSource the database
     * @param table      the table (letters, digits and underscores, not starting with a digit)
     * @return the builder
     */
    public static Builder newBuilder(DataSource dataSource, String table) {
        return new Builder(dataSource, table);
    }

    @Override
    public Store createStore(StoreSettings settings) {
        JdbcStore.Dialect dialect = JdbcStore.ensure(dataSource, table);
        return new JdbcStore(dataSource, dialect, table, discriminator, settings.timeout());
    }

    @Override
    public String toString() {
        return "JdbcAdapter[" + table + ", " + discriminator + "]";
    }

    /**
     * Builds a {@link JdbcAdapter}.
     */
    public static final class Builder {

        private final DataSource dataSource;
        private final String table;
        private String discriminator = DEFAULT_DISCRIMINATOR;

        private Builder(DataSource dataSource, String table) {
            this.dataSource = requireNonNull(dataSource, "dataSource cannot be null");
            this.table = requireNonNull(table, "table cannot be null");
            if (!IDENTIFIER.matcher(table).matches()) {
                throw new IllegalArgumentException("table must be letters, digits and underscores: " + table);
            }
        }

        /**
         * Sets the discriminator, which tells the caches sharing the table apart. Optional:
         * {@value JdbcAdapter#DEFAULT_DISCRIMINATOR} by default.
         *
         * @param discriminator the discriminator (not empty, at most 200 characters)
         * @return this builder
         */
        public Builder withDiscriminator(String discriminator) {
            requireNonNull(discriminator, "discriminator cannot be null");
            if (discriminator.isEmpty() || discriminator.length() > 200) {
                throw new IllegalArgumentException("discriminator must be 1 to 200 characters");
            }
            this.discriminator = discriminator;
            return this;
        }

        /**
         * Builds the adapter.
         *
         * @param <K> the key type of the cache
         * @param <V> the value type of the cache
         * @return the adapter
         */
        public <K, V> JdbcAdapter<K, V> build() {
            return new JdbcAdapter<>(this);
        }
    }
}
