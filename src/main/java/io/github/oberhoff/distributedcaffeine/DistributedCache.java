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
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Interface representing a distributed cache instance. Cache entries are added manually and remain in the cache until
 * either invalidated or evicted.
 * <p>
 * <b>Note:</b> Invalidations are distributed to other cache instances independently of what this cache instance holds.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public interface DistributedCache<K, V> extends Cache<K, V> {

    @Override
    @Nullable V getIfPresent(K key);

    @Override
    Map<K, V> getAllPresent(Iterable<? extends K> keys);

    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    @Nullable V get(K key, Function<? super K, ? extends @Nullable V> mappingFunction);

    @Override
    Map<K, V> getAll(Iterable<? extends K> keys, Function<? super Set<? extends K>,
            ? extends Map<? extends K, ? extends V>> mappingFunction);

    @Override
    void put(K key, V value);

    @Override
    void putAll(Map<? extends K, ? extends V> map);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Note:</b> Invalidation is distributed to other cache instances even if this cache instance does not hold a
     * mapping for the specified key.
     */
    @Override
    void invalidate(K key);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Note:</b> Invalidations are distributed to other cache instances even if this cache instance holds mappings
     * for only some or none of the specified keys.
     */
    @Override
    void invalidateAll(Iterable<? extends K> keys);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Note:</b> Invalidating all mappings is distributed to other cache instances, so that they remove their
     * mappings as well.
     */
    @Override
    void invalidateAll();

    @Override
    long estimatedSize();

    @Override
    CacheStats stats();

    @Override
    ConcurrentMap<K, V> asMap();

    @Override
    void cleanUp();

    @Override
    Policy<K, V> policy();

    /**
     * Returns an access point for inspecting and performing low-level operations on the cache instance.
     *
     * @return the distributed policy as an access point
     */
    DistributedPolicy<K, V> distributedPolicy();
}
