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
package io.github.oberhoff.distributedcaffeine;

import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.Configurer;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import org.jspecify.annotations.Nullable;

import java.util.Set;

/**
 * Interface representing an access point for inspecting and performing low-level operations on the cache instance
 * (similar to {@link com.github.benmanes.caffeine.cache.Policy}).
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@SuppressWarnings("java:S1452")
public interface DistributedPolicy<K, V> {

    /**
     * States of the distributed synchronization.
     */
    enum SynchronizationState {

        /**
         * Synchronization is running and the underlying store is answering.
         */
        SYNCHRONIZED,

        /**
         * Synchronization is running, but the underlying store is not answering. Cache entries held by this cache
         * instance are still served, while writes are refused until the store answers again.
         */
        DEGRADED,

        /**
         * Synchronization is not running, so this cache instance behaves like one without distributed
         * synchronization functionality.
         */
        STOPPED
    }

    /**
     * Get the adapter that manages distributed synchronization between cache instances optionally persistence of cache
     * entries.
     *
     * @return the adapter
     */
    Adapter<K, V> getAdapter();

    /**
     * Starts distributed synchronization for this cache instance if it was stopped before. After starting, changes to
     * this cache instance are distributed to other cache instances and changes to other cache instances are distributed
     * to this cache instance.
     * <p>
     * If persistence is configured using {@link DistributedCaffeine#withPersistence(Configurer)} for cached entries
     * (and a {@link DistributionMode} that includes population is configured but without configuring a cold start
     * explicitly), those retained cache entries from the underlying store are synchronized into this cache instance
     * with priority, so that previously existing cache entries might be overwritten or even removed.
     * <p>
     * If persistence is not configured, nothing is read back, so no cache entry can be confirmed by the underlying
     * store and all of them are removed instead, leaving this cache instance to continue with an empty cache. The same
     * applies whenever synchronization is restored after an interruption, which happens on its own without this
     * method being called, because a cache entry missed in the meantime cannot be told apart from one that never
     * changed.
     */
    void startSynchronization();

    /**
     * Stops distributed synchronization for this cache instance. After stopping, changes to this cache instance are not
     * distributed to other cache instances, nor are changes to other cache instances distributed to this cache
     * instance. Therefore, this cache instance behaves like a cache instance without distributed synchronization
     * functionality. This also releases connections an adapter has established.
     */
    void stopSynchronization();

    /**
     * Returns how distributed synchronization for this cache instance is currently faring.
     * <p>
     * {@link SynchronizationState#STOPPED} means that synchronization is not running, either because it was never
     * started or because {@link #stopSynchronization()} was called. {@link SynchronizationState#SYNCHRONIZED} means
     * that it is running and the underlying store is answering. {@link SynchronizationState#DEGRADED} means that it
     * is running but the underlying store is not answering, so writes to this cache instance are being refused
     * rather than attempted until it does.
     * <p>
     * <b>Note:</b> This is deliberately distinct from whether synchronization was started: a cache instance whose
     * underlying store has become unreachable goes on reporting that it was started, because it is - and it resumes
     * on its own once the store answers again, without this method being called or anything else being done.
     *
     * @return the current state of distributed synchronization for this cache instance
     */
    SynchronizationState getSynchronizationState();

    /**
     * Returns the retained cache entry mapped to the specified key directly from the underlying store bypassing this
     * cache instance.
     * <p>
     * Retention depends on the persistence configured using {@link DistributedCaffeine#withPersistence(Configurer)}
     * (separately for cached and evicted entries).
     *
     * @param key            the key whose associated cache entry is to be returned
     * @param includeEvicted {@code true} if retained evicted cache entries should also be included, otherwise
     *                       {@code false}
     * @return the cache entry to which the specified key is mapped, or null if no mapping is found
     * @throws NullPointerException if the specified key is null
     */
    @Nullable CacheEntry<K, V> getFromStore(K key, boolean includeEvicted);

    /**
     * Returns the retained cache entries mapped to the specified keys directly from the underlying store bypassing this
     * cache instance.
     * <p>
     * Retention depends on the persistence configured using {@link DistributedCaffeine#withPersistence(Configurer)}
     * (separately for cached and evicted entries).
     *
     * @param keys           the keys whose associated cache entries are to be returned
     * @param includeEvicted {@code true} if retained evicted cache entries should also be included, otherwise
     *                       {@code false}
     * @return a set of cache entries to which the specified keys are mapped, keys without mapping are omitted
     * @throws NullPointerException if the specified collection is null or contains a null element
     */
    Set<CacheEntry<K, V>> getAllFromStore(Iterable<? extends K> keys, boolean includeEvicted);
}
