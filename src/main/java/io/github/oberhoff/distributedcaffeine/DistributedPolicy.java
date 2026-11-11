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

import java.util.Map;

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
}
