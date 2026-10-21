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

import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

// The synchronous facade: translates Caffeine's Cache methods to the core.
class InternalDistributedCache<K, V> implements DistributedCache<K, V> {

    final InternalCore<K, V> core;
    private final DistributedPolicy<K, V> distributedPolicy;
    private final InternalConcurrentMap<K, V> map;
    private final Policy<K, V> policy;

    InternalDistributedCache(InternalCore<K, V> core) {
        this.core = core;
        this.distributedPolicy = new InternalDistributedPolicy<>(core);
        this.map = new InternalConcurrentMap<>(core);
        this.policy = new InternalPolicy<>(core, map);
    }

    @Override
    public @Nullable V getIfPresent(K key) {
        return core.getIfPresent(key);
    }

    @Override
    public @Nullable V get(K key, Function<? super K, ? extends @Nullable V> mappingFunction) {
        requireNonNull(mappingFunction, "mappingFunction cannot be null");
        return core.get(key, mappingFunction::apply);
    }

    @Override
    public Map<K, V> getAllPresent(Iterable<? extends K> keys) {
        return core.getAllPresent(keys);
    }

    @Override
    public Map<K, V> getAll(Iterable<? extends K> keys,
                            Function<? super Set<? extends K>, ? extends Map<? extends K, ? extends V>> mappingFunction) {
        requireNonNull(mappingFunction, "mappingFunction cannot be null");
        return core.getAll(keys, mappingFunction::apply);
    }

    @Override
    public void put(K key, V value) {
        core.put(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        core.putAll(requireNonNull(map, "map cannot be null"));
    }

    @Override
    public void invalidate(K key) {
        core.invalidate(key);
    }

    @Override
    public void invalidateAll(Iterable<? extends K> keys) {
        core.invalidateAll(keys);
    }

    @Override
    public void invalidateAll() {
        core.invalidateAll();
    }

    @Override
    public long estimatedSize() {
        core.checkOpen();
        return core.l1().synchronous().estimatedSize();
    }

    @Override
    public CacheStats stats() {
        core.checkOpen();
        return core.l1().synchronous().stats();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        core.checkOpen();
        return map;
    }

    @Override
    public void cleanUp() {
        core.checkOpen();
        core.l1().synchronous().cleanUp();
    }

    @Override
    public Policy<K, V> policy() {
        core.checkOpen();
        return policy;
    }

    @Override
    public DistributedPolicy<K, V> distributedPolicy() {
        return distributedPolicy;
    }

    @Override
    public void close() {
        core.close();
    }
}
