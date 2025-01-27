/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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
package io.github.oberhoff.distributedcaffeine;

import com.mongodb.client.MongoCollection;
import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Interface representing an access point for inspecting and performing low-level operations on the cache instance
 * (similar to {@link com.github.benmanes.caffeine.cache.Policy}).
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 */
public interface DistributedPolicy<K, V> {

    /**
     * Get the MongoDB collection used for distributed synchronization between cache instances.
     *
     * @return the mongo collection
     */
    MongoCollection<Document> getMongoCollection();

    /**
     * Start distributed synchronization for this cache instance if it was stopped before. After starting, changes to
     * this cache instance are distributed to other cache instances and changes to other cache instances are distributed
     * to this cache instance. Persisted cache entries from the MongoDB collection are synchronized into this cache
     * instance with priority (only if the configured {@link DistributionMode} includes population), so that previously
     * existing cache entries in this cache instance might be overwritten or removed.
     */
    void startSynchronization();

    /**
     * Stop distributed synchronization for this cache instance. After stopping, changes to this cache instance are not
     * distributed to other cache instances, nor are changes to other cache instances distributed to this cache
     * instance. Therefore, this cache instance behaves like a cache instance without distributed synchronization
     * functionality. This also releases the connection to MongoDB.
     */
    void stopSynchronization();

    /**
     * Get the configured serializer for key objects which is always an implementation of one of the following
     * interfaces:
     * <ul>
     *     <li>{@link ByteArraySerializer} for serializing an object to a byte array representation</li>
     *     <li>{@link StringSerializer} for serializing an object to a string representation</li>
     *     <li>{@link JsonSerializer} for serializing an object to a JSON representation (encoded as String or BSON)
     *     </li>
     * </ul>
     *
     * @return the serializer for key objects
     */
    Serializer<K, ?> getKeySerializer();

    /**
     * Get the configured serializer for value objects which is always an implementation of one of the following
     * interfaces:
     * <ul>
     *     <li>{@link ByteArraySerializer} for serializing an object to a byte array representation</li>
     *     <li>{@link StringSerializer} for serializing an object to a string representation</li>
     *     <li>{@link JsonSerializer} for serializing an object to a JSON representation (encoded as String or BSON)
     *     </li>
     * </ul>
     *
     * @return the serializer for value objects
     */
    Serializer<V, ?> getValueSerializer();

    /**
     * Get the cache entry mapped to the specified key directly from the MongoDB collection bypassing this cache
     * instance.
     * <p>
     * Evicted cache entries can also be included, which can be particularly useful if extended persistence is
     * configured using {@link DistributedCaffeine.Builder#withExtendedPersistence(Integer)} or
     * {@link DistributedCaffeine.Builder#withExtendedPersistence(Duration)}.
     *
     * @param key            the key whose associated cache entry is to be returned
     * @param includeEvicted {@code true} if evicted cache entries should also be included, {@code false} otherwise
     * @return the cache entry to which the specified key is mapped, or null if no mapping is found
     */
    CacheEntry<K, V> getFromMongo(K key, boolean includeEvicted);

    /**
     * Get the cache entries mapped to the specified keys directly from the MongoDB collection bypassing this cache
     * instance.
     * <p>
     * Evicted cache entries can also be included, which can be particularly useful if extended persistence is
     * configured using {@link DistributedCaffeine.Builder#withExtendedPersistence(Integer)} or
     * {@link DistributedCaffeine.Builder#withExtendedPersistence(Duration)}.
     *
     * @param keys           the keys whose associated cache entries are to be returned
     * @param includeEvicted {@code true} if evicted cache entries should also be included, {@code false} otherwise
     * @return a list of cache entries to which the specified keys are mapped, keys without mapping are omitted
     */
    List<CacheEntry<K, V>> getAllFromMongo(Iterable<? extends K> keys, boolean includeEvicted);

    /**
     * Interface representing a cache entry containing key and value along with some metadata.
     *
     * @param <K> the key type of the cache
     * @param <V> the value type of the cache
     */
    @SuppressWarnings("unused")
    interface CacheEntry<K, V> {

        /**
         * Get the id of the cache entry.
         *
         * @return the id
         */
        ObjectId getId();

        /**
         * Get the key of the cache entry.
         *
         * @return the key
         */
        K getKey();

        /**
         * Get the value of the cache entry.
         *
         * @return the value
         */
        V getValue();

        /**
         * Get the status of the cache entry.
         *
         * @return the status
         */
        String getStatus();

        /**
         * Get the touched instant of the cache entry.
         *
         * @return the touched instant
         */
        Instant getTouched();

        /**
         * Get the expires instant of the cache entry.
         *
         * @return the expires instant
         */
        Instant getExpires();

        /**
         * Indicates whether the cache entry is already evicted or not.
         *
         * @return {@code true} if the cache entry is already evicted, otherwise {@code false}
         */
        boolean isEvicted();
    }
}
