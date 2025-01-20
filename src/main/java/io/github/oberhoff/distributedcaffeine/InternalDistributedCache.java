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
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.LazyInitializer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.String.format;

class InternalDistributedCache<K, V> implements DistributedCache<K, V>, LazyInitializer<K, V> {

    protected DistributedCaffeine<K, V> distributedCaffeine;
    protected Cache<K, V> cache;
    protected InternalSynchronizationLock synchronizationLock;

    private String origin;
    private String mongoCollectionName;

    InternalDistributedCache() {
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.distributedCaffeine = distributedCaffeine;
        this.cache = distributedCaffeine.getCache();
        this.synchronizationLock = distributedCaffeine.getSynchronizationLock();
        this.origin = distributedCaffeine.getObjectIdGenerator().getOrigin();
        this.mongoCollectionName = distributedCaffeine.getMongoCollection().getNamespace().getCollectionName();
    }

    @Override
    public @Nullable V getIfPresent(@NonNull K key) {
        return cache.getIfPresent(key);
    }

    @Override
    public V get(@NonNull K key, @NonNull Function<? super K, ? extends V> mappingFunction) {
        synchronizationLock.lock();
        try {
            return cache.get(key, mappedKey ->
                    distributedCaffeine.putDistributed(mappedKey, mappingFunction.apply(mappedKey)));
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public @NonNull Map<K, V> getAllPresent(@NonNull Iterable<? extends K> keys) {
        return cache.getAllPresent(keys);
    }

    @Override
    public @NonNull Map<K, V> getAll(@NonNull Iterable<? extends K> keys,
                                     @NonNull Function<? super Set<? extends K>, ? extends Map<? extends K, ? extends V>> mappingFunction) {
        synchronizationLock.lock();
        try {
            return cache.getAll(keys, mappedKeys ->
                    distributedCaffeine.putAllDistributed(mappingFunction.apply(mappedKeys)));
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public void put(@NonNull K key, @NonNull V value) {
        synchronizationLock.lock();
        try {
            cache.put(key, distributedCaffeine.putDistributed(key, value));
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public void putAll(@NonNull Map<? extends K, ? extends V> map) {
        synchronizationLock.lock();
        try {
            cache.putAll(distributedCaffeine.putAllDistributed(map));
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public void invalidate(@NonNull K key) {
        synchronizationLock.lock();
        try {
            cache.invalidate(distributedCaffeine.invalidateDistributed(key));
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public void invalidateAll(@NonNull Iterable<? extends K> keys) {
        synchronizationLock.lock();
        try {
            Set<K> keySet = StreamSupport.stream(keys.spliterator(), false)
                    .collect(Collectors.toSet());
            cache.invalidateAll(distributedCaffeine.invalidateAllDistributed(keySet));
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public void invalidateAll() {
        synchronizationLock.lock();
        try {
            distributedCaffeine.invalidateAllDistributed(cache.asMap().keySet());
            cache.invalidateAll();
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    @Override
    public @NonNull CacheStats stats() {
        return cache.stats();
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    @Override
    public @NonNull ConcurrentMap<K, V> asMap() {
        InternalConcurrentMap<K, V> concurrentMap = new InternalConcurrentMap<>();
        concurrentMap.initialize(distributedCaffeine);
        return concurrentMap;
    }

    @Override
    public @NonNull Policy<K, V> policy() {
        InternalPolicy<K, V> policy = new InternalPolicy<>();
        policy.initialize(distributedCaffeine);
        return policy;
    }

    @Override
    public DistributedPolicy<K, V> distributedPolicy() {
        InternalDistributedPolicy<K, V> distributedPolicy = new InternalDistributedPolicy<>();
        distributedPolicy.initialize(distributedCaffeine);
        return distributedPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalDistributedCache<?, ?> that = (InternalDistributedCache<?, ?>) o;
        return Objects.equals(this.origin, that.origin);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(origin);
    }

    @Override
    public String toString() {
        String cacheClassName = cache instanceof LoadingCache
                ? DistributedLoadingCache.class.getSimpleName()
                : DistributedCache.class.getSimpleName();
        return format("%s{id=%s, collection='%s'}",
                cacheClassName, origin, mongoCollectionName);
    }
}
