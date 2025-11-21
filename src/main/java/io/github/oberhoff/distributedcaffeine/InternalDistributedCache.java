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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullIterable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.isNull;
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
        return cache.getIfPresent(key);
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> mappingFunction) {
        requireNonNull(key);
        requireNonNull(mappingFunction);
        // custom implementation to share logic
        return getCommon(key, mappingFunction);
    }

    @Override
    public Map<K, V> getAllPresent(Iterable<? extends K> keys) {
        return cache.getAllPresent(keys);
    }

    @Override
    public Map<K, V> getAll(Iterable<? extends K> keys,
                            Function<? super Set<? extends K>,
                                    ? extends Map<? extends K, ? extends V>> mappingFunction) {
        Set<K> keySet = requireNonNullIterable(keys);
        requireNonNull(mappingFunction);
        // custom implementation to share logic
        return getAllCommon(keySet, mappingFunction);
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

    protected V getCommon(K key, Function<? super K, ? extends V> mappingFunction) {
        V oldValue = policy.getIfPresentQuietly(key);
        if (isNull(oldValue)) {
            V newValue = mappingFunction.apply(key);
            if (nonNull(newValue)) {
                synchronizationLock.runLocked(() ->
                        cache.put(key, cacheManager.putDistributed(key, newValue)));
            }
            return newValue;
        }
        return oldValue;
    }

    protected Map<K, V> getAllCommon(Set<K> keys, Function<? super Set<? extends K>,
            ? extends Map<? extends K, ? extends V>> mappingFunction) {
        Set<K> keysWithNullValues = new HashSet<>();
        Map<K, V> keyToOldValue = new HashMap<>();
        keys.forEach(key -> {
            V oldValue = policy.getIfPresentQuietly(key);
            if (isNull(oldValue)) {
                keysWithNullValues.add(key);
            } else {
                keyToOldValue.put(key, oldValue);
            }
        });
        // take existing entries into account (might be overwritten by loaded entries)
        Map<K, V> keyToNewValue = new HashMap<>(keyToOldValue);
        // retain 'all returned entries will be cached' semantics
        keyToNewValue.putAll(requireNonNullMap(mappingFunction.apply(keysWithNullValues))); // key/value null check
        synchronizationLock.runLocked(() ->
                cache.putAll(cacheManager.putAllDistributed(keyToNewValue)));
        // retain 'only the entries for keys will be returned' semantics
        keyToNewValue.keySet().removeIf(key -> !keys.contains(key));
        return unmodifiableMap(keyToNewValue);
    }
}
