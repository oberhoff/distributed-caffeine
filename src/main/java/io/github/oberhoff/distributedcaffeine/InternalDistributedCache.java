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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullIterable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullMap;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalDistributedCache<K, V> implements DistributedCache<K, V>, InternalLazyInitializer<K, V> {

    protected DistributedCaffeine<K, V> distributedCaffeine;
    protected Cache<K, V> cache;
    protected Policy<K, V> policy;
    protected InternalCacheManager<K, V> cacheManager;
    protected InternalSynchronizationLock synchronizationLock;

    InternalDistributedCache() {
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.distributedCaffeine = distributedCaffeine;
        this.cache = distributedCaffeine.getCache();
        this.policy = distributedCaffeine.getCache().policy();
        this.cacheManager = distributedCaffeine.getCacheManager();
        this.synchronizationLock = distributedCaffeine.getSynchronizationLock();
    }

    @Override
    public V getIfPresent(K key) {
        requireNonNull(key);
        return cache.getIfPresent(key);
    }

    @Override
    public Map<K, V> getAllPresent(Iterable<? extends K> keys) {
        Set<K> keySet = requireNonNullIterable(keys);
        return cache.getAllPresent(keySet);
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> mappingFunction) {
        requireNonNull(key);
        requireNonNull(mappingFunction);
        Function<K, V> distributedMapping = mappingKey -> {
            V value = mappingFunction.apply(mappingKey);
            if (nonNull(value)) {
                cacheManager.putDistributed(mappingKey, value);
            }
            return value;
        };
        return synchronizationLock.getLocked(() ->
                cache.get(key, distributedMapping));
    }

    @Override
    public Map<K, V> getAll(Iterable<? extends K> keys, Function<? super Set<? extends K>,
            ? extends Map<? extends K, ? extends V>> mappingFunction) {
        Set<K> keySet = requireNonNullIterable(keys);
        requireNonNull(mappingFunction);
        Function<? super Set<? extends K>, ? extends Map<? extends K, ? extends V>> distributedMapping = mappingKeys ->
                cacheManager.putAllDistributed(requireNonNullMap(mappingFunction.apply(mappingKeys)));
        return synchronizationLock.getLocked(() ->
                cache.getAll(keySet, distributedMapping));
    }

    @Override
    public void put(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        synchronizationLock.runLocked(() ->
                cache.put(key, cacheManager.putDistributed(key, value)));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        requireNonNullMap(map);
        synchronizationLock.runLocked(() ->
                cache.putAll(cacheManager.putAllDistributed(map)));
    }

    @Override
    public void invalidate(K key) {
        requireNonNull(key);
        synchronizationLock.runLocked(() ->
                cache.invalidate(cacheManager.invalidateDistributed(key)));
    }

    @Override
    public void invalidateAll(Iterable<? extends K> keys) {
        Set<K> keySet = requireNonNullIterable(keys);
        synchronizationLock.runLocked(() ->
                cache.invalidateAll(cacheManager.invalidateAllDistributed(keySet)));
    }

    @Override
    public void invalidateAll() {
        synchronizationLock.runLocked(() -> {
            Set<K> keySet = cache.asMap().keySet();
            cacheManager.invalidateAllDistributed(keySet);
            keySet.clear();
        });
    }

    @Override
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    @Override
    public CacheStats stats() {
        return cache.stats();
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        InternalConcurrentMap<K, V> internalConcurrentMap = new InternalConcurrentMap<>();
        internalConcurrentMap.initialize(distributedCaffeine);
        return internalConcurrentMap;
    }

    @Override
    public Policy<K, V> policy() {
        InternalPolicy<K, V> internalPolicy = new InternalPolicy<>();
        internalPolicy.initialize(distributedCaffeine);
        return internalPolicy;
    }

    @Override
    public DistributedPolicy<K, V> distributedPolicy() {
        InternalDistributedPolicy<K, V> internalDistributedPolicy = new InternalDistributedPolicy<>();
        internalDistributedPolicy.initialize(distributedCaffeine);
        return internalDistributedPolicy;
    }
}
