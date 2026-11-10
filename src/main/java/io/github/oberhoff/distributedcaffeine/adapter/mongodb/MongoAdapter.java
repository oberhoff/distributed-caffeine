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
package io.github.oberhoff.distributedcaffeine.adapter.mongodb;

import com.mongodb.client.MongoClient;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * Stores a cache in a MongoDB collection, through the application's {@link MongoClient}. Several caches can share a
 * collection, told apart by their discriminator.
 * <pre>{@code
 * MongoAdapter<Key, Value> adapter = MongoAdapter
 *     .newBuilder(mongoClient, databaseName, collectionName)
 *     .withDiscriminator(discriminator)
 *     .build();
 * }</pre>
 * An adapter serves one cache: building a second cache with the same adapter fails while the first is open.
 * <p>
 * Requires {@code org.mongodb:mongodb-driver-sync} and MongoDB 4.4 or later, as a replica set or a sharded cluster; the
 * collection itself must not be sharded. The cache reads and writes on the primary with majority read and write
 * concern, whatever the client's defaults.
 * <p>
 * The cache's operation timeout applies to its own operations only, so caches sharing a client may have different ones.
 * A timeout the client sets itself (the connection string option {@code timeoutMS}) applies instead.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public final class MongoAdapter<K, V> implements Adapter<K, V> {

    // what the class Javadoc leaves out: change streams are shared per client and database.

    /**
     * The discriminator used if none is configured.
     */
    public static final String DEFAULT_DISCRIMINATOR = "default";

    private static final int MAXIMUM_NAMESPACE_BYTES = 255;
    private static final int MAXIMUM_DATABASE_BYTES = 63;

    private final MongoClient client;
    private final String databaseName;
    private final String collectionName;
    private final String discriminator;
    private final boolean schemaReset;

    private MongoAdapter(Builder builder) {
        this.client = builder.client;
        this.databaseName = builder.databaseName;
        this.collectionName = builder.collectionName;
        this.discriminator = builder.discriminator;
        this.schemaReset = builder.schemaReset;
    }

    /**
     * Starts a builder for an adapter on the given collection.
     *
     * @param client         the client (a replica set or a sharded cluster through mongos)
     * @param databaseName   the database
     * @param collectionName the collection (not sharded)
     * @return the builder
     */
    public static Builder newBuilder(MongoClient client, String databaseName, String collectionName) {
        return new Builder(client, databaseName, collectionName);
    }

    @Override
    public Store createStore(StoreSettings settings) {
        MongoLocation location = new MongoLocation(databaseName, collectionName, discriminator, settings.keyFormat(),
                settings.valueFormat());
        MongoSchema.ensure(client, location, schemaReset, settings.operationTimeout());
        MongoStore store = new MongoStore(client, location, settings.operationTimeout());
        store.seedClusterTime();
        return store;
    }

    @Override
    public String toString() {
        return "MongoDB " + databaseName + "." + collectionName + "/" + discriminator;
    }

    /**
     * The builder of a {@link MongoAdapter}.
     */
    public static final class Builder {

        private final MongoClient client;
        private final String databaseName;
        private final String collectionName;
        private String discriminator = DEFAULT_DISCRIMINATOR;
        private boolean schemaReset;

        private Builder(MongoClient client, String databaseName, String collectionName) {
            try {
                // the library is an automatic module, which reads every module: the IDE's module check does not apply
                //noinspection Java9ReflectionClassVisibility
                Class.forName("com.mongodb.client.MongoClient", false, MongoAdapter.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("MongoAdapter requires org.mongodb:mongodb-driver-sync on the "
                        + "classpath", e);
            }
            this.client = requireNonNull(client, "client cannot be null");
            this.databaseName = checkedDatabaseName(databaseName);
            this.collectionName = checkedCollectionName(collectionName);
            int namespace = (databaseName + "." + collectionName).getBytes(StandardCharsets.UTF_8).length;
            if (namespace > MAXIMUM_NAMESPACE_BYTES) {
                throw new IllegalArgumentException("the namespace " + databaseName + "." + collectionName
                        + " is longer than " + MAXIMUM_NAMESPACE_BYTES + " bytes");
            }
        }

        /**
         * Sets the discriminator that tells this cache's documents apart from those of other caches in the same
         * collection. Optional: {@value #DEFAULT_DISCRIMINATOR} by default.
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
         * Drops and recreates the collection if distributed-caffeine 0.x created it, or if it is empty; one of another
         * version of this generation, or holding other documents, is never dropped. Meant for the migration from 0.x:
         * instances of 0.x still running go on among themselves, apart from the new ones, until they are stopped.
         * Remove it once the migration is done.
         *
         * @return this builder
         */
        public Builder withSchemaReset() {
            this.schemaReset = true;
            return this;
        }


        /**
         * Builds the adapter; the collection is checked or created when the cache is built.
         *
         * @param <K> the key type of the cache
         * @param <V> the value type of the cache
         * @return the adapter
         */
        public <K, V> MongoAdapter<K, V> build() {
            return new MongoAdapter<>(this);
        }

        private static String checkedDatabaseName(String name) {
            requireNonNull(name, "databaseName cannot be null");
            if (name.isBlank() || name.chars().anyMatch(c -> "/\\. \"$*<>:|?\0".indexOf(c) >= 0)) {
                throw new IllegalArgumentException("databaseName is empty or contains characters MongoDB forbids "
                        + "(/\\. \"$*<>:|? or NUL): " + name);
            }
            if (name.getBytes(StandardCharsets.UTF_8).length > MAXIMUM_DATABASE_BYTES) {
                throw new IllegalArgumentException("databaseName is longer than " + MAXIMUM_DATABASE_BYTES + " bytes");
            }
            return name;
        }

        private static String checkedCollectionName(String name) {
            requireNonNull(name, "collectionName cannot be null");
            if (name.isBlank() || name.indexOf('$') >= 0 || name.indexOf('\0') >= 0 || name.startsWith("system.")) {
                throw new IllegalArgumentException("collectionName is empty, contains $ or NUL, or starts with "
                        + "system.: " + name);
            }
            return name;
        }
    }
}
