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
 * <b>Note:</b> {@link com.github.benmanes.caffeine.cache.Cache#invalidateAll()} (without keys) reaches just as far,
 * although it cannot name the cache entries it invalidates. What is removed is therefore decided by each cache
 * instance for itself once the invalidation arrives, so all of them end up empty no matter which cache entries each of
 * them held, and cache entries only the underlying store still holds are invalidated as well - leaving those would
 * keep them reloadable right afterwards.
 * <p>
 * <b>Attention:</b> What the cache instance requesting it does afterwards is not undone by it, but an operation of
 * another cache instance racing it is subject to last write wins, as there is no order between cache instances to
 * appeal to.
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
