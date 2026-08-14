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
import io.github.oberhoff.distributedcaffeine.adapter.AbstractAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;

import static io.github.oberhoff.distributedcaffeine.adapter.Repository.DEFAULT_DISCRIMINATOR;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of an adapter based on MongoDB as the underlying store.
 * <p>
 * Instances are constructed using the builder pattern instance returned by
 * {@link MongoAdapter#newBuilder(MongoClient, String, String)}.
 * <p>
 * <b>Note:</b> An adapter instance belongs to exactly one cache instance and cannot be shared between them.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
 */
public final class MongoAdapter<K, V> extends AbstractAdapter<K, V> {

    private MongoAdapter(Builder builder) {
        super(new MongoRepository<>(builder.mongoClient, builder.databaseName, builder.collectionName),
                new MongoSynchronizer<>(builder.mongoClient, builder.databaseName, builder.collectionName),
                String.join(":", "mongodb", builder.databaseName, builder.collectionName,
                        builder.discriminator), builder.discriminator);
    }

    /**
     * Returns a new builder pattern instance for configuring and constructing an adapter based on MongoDB as the
     * underlying store. The builder pattern instance is finalized with {@link Builder#build()} to construct the
     * adapter instance.
     * <p>
     * Exemplary usage:
     * <pre>
     * MongoAdapter&#60;Key, Value&#62; mongoAdapter = MongoAdapter.newBuilder(mongoClient, databaseName, collectionName)
     *     ...
     *     .build();
     * </pre>
     *
     * @param mongoClient    the mongo client used by the adapter
     * @param databaseName   the database name used by the adapter
     * @param collectionName the collection name used by the adapter
     * @return builder pattern instance for configuring and constructing an adapter
     * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
     */
    public static Builder newBuilder(MongoClient mongoClient, String databaseName, String collectionName) {
        return new Builder(mongoClient, databaseName, collectionName);
    }

    /**
     * Builder pattern class for configuring and constructing an adapter.
     *
     * @author Andreas Oberhoff
     */
    public static final class Builder {

        private final MongoClient mongoClient;
        private final String databaseName;
        private final String collectionName;
        private String discriminator;

        private Builder(MongoClient mongoClient, String databaseName, String collectionName) {
            requireNonNull(mongoClient, "mongoClient cannot be null");
            requireNonNull(databaseName, "databaseName cannot be null");
            requireNonNull(collectionName, "collectionName cannot be null");
            this.mongoClient = mongoClient;
            this.databaseName = databaseName;
            this.collectionName = collectionName;
            // set defaults
            this.discriminator = DEFAULT_DISCRIMINATOR;
        }

        /**
         * Specifies the discriminator used by the adapter to distinguish between cache entries from different caches
         * that share a collection in MongoDB.
         * <p>
         * <b>Note:</b> {@link Repository#DEFAULT_DISCRIMINATOR} is used as default if this method is skipped.
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
         * Constructs an adapter instance.
         * <p>
         * <b>Note:</b> An adapter instance belongs to exactly one cache instance and cannot be shared between them.
         *
         * @param <K> the key type of the cache
         * @param <V> the value type of the cache
         * @return the new adapter instance
         */
        public <K, V> MongoAdapter<K, V> build() {
            return new MongoAdapter<>(this);
        }
    }
}
