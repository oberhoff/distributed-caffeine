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

import io.github.oberhoff.distributedcaffeine.adapter.AbstractAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.DiscriminatorAware;

import javax.sql.DataSource;

import static io.github.oberhoff.distributedcaffeine.adapter.DiscriminatorAware.DEFAULT_DISCRIMINATOR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of an adapter based on PostgreSQL.
 * <p>
 * Instances are constructed using the builder pattern instance returned by
 * {@link PostgresAdapter#newBuilder(DataSource, String, String)}.
 * <p>
 * <b>Note:</b> An adapter instance belongs to exactly one cache instance and cannot be shared between them.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
 */
public final class PostgresAdapter<K, V> extends AbstractAdapter<K, V> {

    /**
     * Modes that define how cache instances share the connection they listen for notifications on.
     * <p>
     * Listening holds a connection for as long as a cache instance synchronizes - a session of its own on the server,
     * subscribed to the channels of the cache instances it serves. Sharing reduces the number of those connections and
     * sessions, and the work the server does to deliver notifications to them. In exchange, cache instances on a
     * shared connection share its fate: when it fails, all of them are reconciled. And a single connection delivers
     * notifications promptly only up to the order of a thousand per second, so write-heavy cache instances are
     * better spread over several connections with {@link #TABLE} or {@link #INSTANCE}.
     * <p>
     * <b>Note:</b> Only cache instances whose adapters are configured with the same sharing mode, the same data source
     * and the same listener data source (which defaults to the data source) share a connection. Data sources are told
     * apart by identity, so two data source instances reaching the same database do not share.
     *
     * @author Andreas Oberhoff
     */
    public enum ListenerSharingMode {

        /**
         * Every cache instance listens on a connection of its own (no sharing).
         */
        INSTANCE,

        /**
         * Cache instances on the same table share one connection, whatever their discriminator.
         */
        TABLE,

        /**
         * Cache instances on the same database share one connection, whatever their table. This is the default
         * listener sharing mode.
         */
        DATABASE
    }

    private PostgresAdapter(Builder builder) {
        // through a second constructor so that the synchronizer can be handed the repository it reads through:
        // what a notification carries is which records changed, not the records themselves
        this(new PostgresRepository<>(builder.dataSource, builder.schemaName, builder.tableName), builder);
    }

    private PostgresAdapter(PostgresRepository<K, V> repository, Builder builder) {
        super(repository,
                new PostgresSynchronizer<>(builder.dataSource, builder.listenerDataSource, builder.listenerSharingMode,
                        builder.schemaName, builder.tableName, repository),
                String.join(":", "postgresql", builder.schemaName, builder.tableName,
                        builder.discriminator), builder.discriminator);
    }

    /**
     * Returns a new builder pattern instance for configuring and constructing an adapter based on PostgreSQL as the
     * underlying store. The builder pattern instance is finalized with {@link Builder#build()} to construct the
     * adapter instance.
     * <p>
     * Exemplary usage:
     * <pre>
     * PostgresAdapter&#60;Key, Value&#62; postgresAdapter = PostgresAdapter.newBuilder(dataSource, schemaName, tableName)
     *     ...
     *     .build();
     * </pre>
     *
     * @param dataSource the data source used by the adapter
     * @param schemaName the schema name used by the adapter
     * @param tableName  the table name used by the adapter
     * @return builder pattern instance for configuring and constructing an adapter
     * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
     */
    public static Builder newBuilder(DataSource dataSource, String schemaName, String tableName) {
        return new Builder(dataSource, schemaName, tableName);
    }

    /**
     * Builder pattern class for configuring and constructing an adapter.
     *
     * @author Andreas Oberhoff
     */
    public static final class Builder {

        private final DataSource dataSource;
        private DataSource listenerDataSource;
        private ListenerSharingMode listenerSharingMode;
        private final String schemaName;
        private final String tableName;
        private String discriminator;

        private Builder(DataSource dataSource, String schemaName, String tableName) {
            requireNonNull(dataSource, "dataSource cannot be null");
            requireNonNull(schemaName, "schemaName cannot be null");
            requireNonNull(tableName, "tableName cannot be null");
            this.dataSource = dataSource;
            this.schemaName = checkedName(schemaName, "schemaName");
            this.tableName = checkedName(tableName, "tableName");
            // set defaults
            this.listenerDataSource = dataSource;
            this.listenerSharingMode = ListenerSharingMode.DATABASE;
            this.discriminator = DEFAULT_DISCRIMINATOR;
        }

        /**
         * Specifies the discriminator used by the adapter to distinguish between cache entries from different caches
         * that share a table in PostgreSQL.
         * <p>
         * <b>Note:</b> {@link DiscriminatorAware#DEFAULT_DISCRIMINATOR} is used as default if this method is skipped.
         *
         * @param discriminator the discriminator used by the adapter
         * @return a builder pattern instance for chaining additional methods
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
         * Specifies a separate data source for the connection the adapter listens for notifications on, while
         * reading and writing keep using the data source passed to
         * {@link PostgresAdapter#newBuilder(DataSource, String, String)}.
         * <p>
         * Listening relies on {@code LISTEN}, which needs a connection of its own to the server that writes go to,
         * held for as long as synchronization lasts. A connection pooler in transaction mode cannot provide that,
         * because it hands a server connection to a client only for the duration of a single transaction. Such
         * poolers are common in front of managed cloud databases and database clusters, often as their default
         * endpoint. Reading and writing may go through such a pooler while listening uses a direct or session-mode
         * connection specified here.
         * <p>
         * Listening is checked when it begins, and starting synchronization fails if notifications sent through
         * the data source do not reach the listening connection.
         * <p>
         * <b>Note:</b> The listener data source defaults to the data source if this method is skipped.
         *
         * @param listenerDataSource the data source the adapter takes its listening connection from
         * @return a builder pattern instance for chaining additional methods
         */
        public Builder withListenerDataSource(DataSource listenerDataSource) {
            requireNonNull(listenerDataSource, "listenerDataSource cannot be null");
            this.listenerDataSource = listenerDataSource;
            return this;
        }

        /**
         * Specifies the mode that defines how cache instances share the connection they listen for notifications on.
         * <p>
         * <b>Note:</b> {@link ListenerSharingMode#DATABASE} is used as default if this method is skipped.
         *
         * @param listenerSharingMode listener sharing mode defining how cache instances share the connection they
         *                            listen for notifications on
         * @return a builder pattern instance for chaining additional methods
         */
        public Builder withListenerSharingMode(ListenerSharingMode listenerSharingMode) {
            requireNonNull(listenerSharingMode, "listenerSharingMode cannot be null");
            this.listenerSharingMode = listenerSharingMode;
            return this;
        }

        /**
         * Constructs an adapter instance.
         * <p>
         * <b>Note:</b> An adapter instance belongs to exactly one cache instance and cannot be shared between them.
         *
         * @param <K> the key type of the cache
         * @param <V> the value type of the cache
         * @return the new adapter instance
         */
        public <K, V> PostgresAdapter<K, V> build() {
            return new PostgresAdapter<>(this);
        }

        // Taken as it is, down to its case, and quoted wherever it reaches a statement - which is what makes a
        // name that would otherwise need explaining usable: a table a migration tool called 'cache-entries', a
        // schema with a space in it, a name that is not ASCII at all. Only an empty one is refused, because
        // PostgreSQL has no such identifier to address.
        // What keeps this safe is the quoting rather than a shape required here: an embedded quote is doubled on
        // the way in, so a name cannot end the identifier it sits in and become statement text of its own
        private static String checkedName(String name, String what) {
            if (name.isEmpty()) {
                throw new IllegalArgumentException(format("%s cannot be empty", what));
            }
            return name;
        }
    }
}
