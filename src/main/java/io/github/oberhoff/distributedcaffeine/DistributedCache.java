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

import com.github.benmanes.caffeine.cache.Cache;
import org.jspecify.annotations.NullMarked;

/**
 * Interface representing a distributed cache instance. Cache entries are added manually and remain in the cache until
 * either invalidated or evicted.
 * <p>
 * <b>Note:</b> Invalidating by key stops every cache instance from serving that cache entry, no matter which of them
 * the invalidation was requested on and whether that one held the cache entry at all. It therefore always reaches the
 * underlying store, even when nothing was found in memory to invalidate.
 * <p>
 * <b>Attention:</b> {@link com.github.benmanes.caffeine.cache.Cache#invalidateAll()} (without keys) is an exception to
 * that: it only invalidates what the cache instance it is called on currently holds, leaving cache entries held
 * exclusively by other ones untouched. Invalidate by key to reach those.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@NullMarked
public interface DistributedCache<K, V> extends Cache<K, V> {

    /**
     * Returns an access point for inspecting and performing low-level operations on the cache instance.
     *
     * @return the distributed policy as an access point
     */
    DistributedPolicy<K, V> distributedPolicy();
}
