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

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * The distributed side of a cache instance.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Andreas Oberhoff
 */
public interface DistributedPolicy<K, V> {

    /**
     * Returns the state of this instance's synchronization with the store; also after {@code close()}.
     *
     * @return the state
     */
    SynchronizationState getSynchronizationState();

    /**
     * Reads the key's value from the store, bypassing this instance's copy and without keeping what it reads. Values
     * older than the store's maximum age are not returned, nor values this instance cannot read (counted as
     * {@code unreadableRowCount}), which every read treats as missing.
     *
     * @param key the key
     * @return the stored value, or {@code null} if the store holds none
     */
    @Nullable StoredValue<V> getFromStore(K key);

    /**
     * Reads the keys' values from the store, bypassing this instance's copies and without keeping what it reads.
     * Values older than the store's maximum age are not returned, nor values this instance cannot read (counted as
     * {@code unreadableRowCount}), which every read treats as missing.
     *
     * @param keys the keys
     * @return the stored values found, by key
     */
    Map<K, StoredValue<V>> getAllFromStore(Iterable<? extends K> keys);

    /**
     * Returns a snapshot of this instance's store and synchronization activity. Empty unless the cache records stats
     * ({@code recordStats()}), as Caffeine's {@code stats()}; also after {@code close()}.
     *
     * @return the snapshot
     */
    DistributedStats stats();

    /**
     * Returns access to the store's maximum age as this instance applies it, if the cache was built with one
     * ({@link StoreConfigurer#withMaximumAge(Duration)}), as Caffeine's {@code policy().expireAfterWrite()} gives
     * access to the local expiry.
     *
     * @return the maximum age, or empty if the cache was built without one
     */
    Optional<StoreMaximumAge> storeMaximumAge();

    /**
     * Returns access to the store's maximum size as this instance applies it, if the cache was built with one
     * ({@link StoreConfigurer#withMaximumSize(long)}), as Caffeine's {@code policy().eviction()} gives access to the
     * local maximum size.
     *
     * @return the maximum size, or empty if the cache was built without one
     */
    Optional<StoreMaximumSize> storeMaximumSize();

    /**
     * The store's maximum age as an instance applies it. The instances of a cache need the same maximum age: set it on
     * every instance, for example from a configuration service. While they differ, as while a change reaches them,
     * values stay consistent: the shortest age expires values in the store for every instance, which then load them
     * again, and each instance bounds its own copies by its own age.
     *
     * @author Andreas Oberhoff
     */
    interface StoreMaximumAge {

        /**
         * Returns the maximum age this instance applies.
         *
         * @return the maximum age
         */
        Duration getMaximumAge();

        /**
         * Sets the maximum age this instance applies, at once: its reads count values at least as old as missing, its
         * maintenance expires them in the store, and its copies expire at the new maximum age after their value's
         * write at the latest (not with {@link StoreConfigurer#withMaximumAgeForStoreOnly()}). A longer maximum age
         * lengthens the copies held as far as their local expiry allows, except under an {@code expireAfter} expiry,
         * whose copies keep what is left of them.
         *
         * @param maximumAge the maximum age (positive; with {@link StoreConfigurer#withMaximumAgeForStoreOnly()}, at
         *                   least five heartbeat intervals)
         * @throws IllegalArgumentException if the maximum age is not positive, or, with
         *                                  {@link StoreConfigurer#withMaximumAgeForStoreOnly()}, shorter than five
         *                                  heartbeat intervals
         */
        void setMaximumAge(Duration maximumAge);
    }

    /**
     * The store's maximum size as an instance applies it. The instances of a cache need the same maximum size: set it
     * on every instance, for example from a configuration service. While they differ, the store is pruned to the
     * smallest.
     *
     * @author Andreas Oberhoff
     */
    interface StoreMaximumSize {

        /**
         * Returns the maximum size this instance's maintenance prunes the store to.
         *
         * @return the maximum number of values
         */
        long getMaximumSize();

        /**
         * Sets the maximum size this instance's maintenance prunes the store to, from its next run on.
         *
         * @param maximumSize the maximum number of values (positive)
         * @throws IllegalArgumentException if the maximum size is not positive
         */
        void setMaximumSize(long maximumSize);
    }
}
