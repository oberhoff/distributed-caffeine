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

import static io.github.oberhoff.distributedcaffeine.InternalKey.ik;
import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.iks;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.im;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.m;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullIterable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullMap;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.s;
import static io.github.oberhoff.distributedcaffeine.InternalValue.iv;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalDistributedCache<K, V> implements DistributedCache<K, V>, InternalLazyInitializer<K, V> {

    protected InternalInstanceRegistry<K, V> instanceRegistry;
    protected Cache<InternalKey<K>, InternalValue<V>> cache;
    protected Policy<InternalKey<K>, InternalValue<V>> policy;
    protected InternalCacheManager<K, V> cacheManager;
    protected InternalSynchronizationLock synchronizationLock;

    InternalDistributedCache() {
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.instanceRegistry = instanceRegistry;
        this.cache = instanceRegistry.getCache();
        this.policy = instanceRegistry.getCache().policy();
        this.cacheManager = instanceRegistry.getCacheManager();
        this.synchronizationLock = instanceRegistry.getSynchronizationLock();
    }

    @Override
    public V getIfPresent(K key) {
        requireNonNull(key);
        return v(cache.getIfPresent(ik(key)));
    }

    @Override
    public Map<K, V> getAllPresent(Iterable<? extends K> keys) {
        Set<K> keySet = requireNonNullIterable(keys);
        return m(cache.getAllPresent(iks(keySet)));
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> mappingFunction) {
        requireNonNull(key);
        requireNonNull(mappingFunction);
        Function<InternalKey<K>, InternalValue<V>> distributedMapping = mappingKey -> {
            InternalValue<V> value = iv(mappingFunction.apply(k(mappingKey)));
            if (nonNull(value)) {
                cacheManager.putDistributed(mappingKey, value);
            }
            return value;
        };
        return synchronizationLock.getLocked(() ->
                v(cache.get(ik(key), distributedMapping)));
    }

    @Override
    public Map<K, V> getAll(Iterable<? extends K> keys, Function<? super Set<? extends K>,
            ? extends Map<? extends K, ? extends V>> mappingFunction) {
        Set<K> keySet = requireNonNullIterable(keys);
        requireNonNull(mappingFunction);
        Function<? super Set<? extends InternalKey<K>>,
                ? extends Map<? extends InternalKey<K>, ? extends InternalValue<V>>> distributedMapping = mappingKeys ->
                cacheManager.putAllDistributed(im(mappingFunction.apply(s(mappingKeys))));
        return synchronizationLock.getLocked(() ->
                m(cache.getAll(iks(keySet), distributedMapping)));
    }

    @Override
    public void put(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        synchronizationLock.runLocked(() ->
                cache.put(ik(key), cacheManager.putDistributed(ik(key), iv(value))));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        requireNonNullMap(map);
        synchronizationLock.runLocked(() ->
                cache.putAll(cacheManager.putAllDistributed(im(map))));
    }

    @Override
    public void invalidate(K key) {
        requireNonNull(key);
        synchronizationLock.runLocked(() ->
                cache.invalidate(cacheManager.invalidateDistributed(ik(key))));
    }

    @Override
    public void invalidateAll(Iterable<? extends K> keys) {
        Set<K> keySet = requireNonNullIterable(keys);
        synchronizationLock.runLocked(() ->
                cache.invalidateAll(cacheManager.invalidateAllDistributed(iks(keySet))));
    }

    @Override
    public void invalidateAll() {
        synchronizationLock.runLocked(() -> {
            Set<InternalKey<K>> keySet = cache.asMap().keySet();
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
        return instanceRegistry.initializeNowAndLazy(new InternalConcurrentMap<>());
    }

    @Override
    public Policy<K, V> policy() {
        return instanceRegistry.initializeNowAndLazy(new InternalPolicy<>());
    }

    @Override
    public DistributedPolicy<K, V> distributedPolicy() {
        return instanceRegistry.initializeNowAndLazy(new InternalDistributedPolicy<>());
    }
}
