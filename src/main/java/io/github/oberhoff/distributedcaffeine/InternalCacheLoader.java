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

import com.github.benmanes.caffeine.cache.CacheLoader;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.ExtendedPersistenceConfigurer;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullMap;
import static java.util.Objects.nonNull;

class InternalCacheLoader<K, V> implements CacheLoader<K, V>, InternalLazyInitializer<K, V> {

    private static final String LOAD_ALL = "loadAll";

    private final CacheLoader<K, V> cacheLoader;

    private InternalCacheManager<K, V> cacheManager;
    private InternalMongoRepository<K, V> mongoRepository;
    private ExtendedPersistenceConfigurer extendedPersistenceConfigurer;

    InternalCacheLoader(CacheLoader<K, V> cacheLoader) {
        this.cacheLoader = cacheLoader;
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.cacheManager = distributedCaffeine.getCacheManager();
        this.mongoRepository = distributedCaffeine.getMongoRepository();
        this.extendedPersistenceConfigurer = distributedCaffeine.getExtendedPersistenceConfigurer();
    }

    @Override
    @SuppressWarnings({"java:S2583"})
    public V load(K key) throws Exception {
        V value = extendedPersistenceConfigurer.hasCacheLoaderStrategy()
                ? loadExtendedFromMongo(key)
                : null;
        value = nonNull(value)
                ? value
                : cacheLoader.load(key);
        return nonNull(value)
                ? cacheManager.putDistributedLoaded(key, value)
                : null;
    }

    @Override
    @SuppressWarnings({"java:S2589"})
    public Map<? extends K, ? extends V> loadAll(Set<? extends K> keys) throws Exception {
        HashMap<K, V> keyToValue = new HashMap<>();
        Set<K> keysToLoad = new HashSet<>(keys);
        if (extendedPersistenceConfigurer.hasCacheLoaderStrategy()) {
            keyToValue.putAll(loadAllExtendedFromMongo(keysToLoad));
            keysToLoad.removeAll(keyToValue.keySet());
        }
        if (!keysToLoad.isEmpty()) {
            // retain the original 'use loadAll() if overridden' semantics
            if (hasLoadAll()) {
                keyToValue.putAll(requireNonNullMap(cacheLoader.loadAll(keysToLoad)));
            } else {
                for (K key : keysToLoad) {
                    V value = cacheLoader.load(key);
                    if (nonNull(value)) {
                        keyToValue.put(key, value);
                    }
                }
            }
        }
        return cacheManager.putAllDistributedLoaded(keyToValue);
    }

    // should never be invoked due to custom implementation
    @Override
    public CompletableFuture<? extends V> asyncLoad(K key, Executor executor) throws Exception {
        throw new IllegalAccessException();
    }

    // should never be invoked
    @Override
    public CompletableFuture<? extends Map<? extends K, ? extends V>> asyncLoadAll(
            Set<? extends K> keys, Executor executor) throws Exception {
        throw new IllegalAccessException();
    }

    // should never be invoked
    @Override
    public V reload(K key, V oldValue) throws Exception {
        throw new IllegalAccessException();
    }

    // only invoked internally if expireAfterWrite is used (special handling needed)
    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<? extends V> asyncReload(K key, V oldValue, Executor executor) {
        return (extendedPersistenceConfigurer.hasCacheLoaderStrategy()
                ? CompletableFuture.supplyAsync(() -> loadExtendedFromMongo(key), executor)
                : CompletableFuture.completedFuture((V) null))
                .thenCompose(newValue -> nonNull(newValue)
                        ? CompletableFuture.completedFuture(newValue)
                        : (CompletableFuture<V>) getFailable(() -> cacheLoader.asyncReload(key, oldValue, executor)))
                // retain the original 'remove if null' semantics
                .thenApply(newValue -> nonNull(newValue)
                        // special handling, no lock required
                        ? cacheManager.putDistributedRefreshAfterWrite(key, newValue, oldValue)
                        // special handling, no lock required
                        : cacheManager.invalidateDistributedRefreshAfterWrite(key, oldValue));
    }

    // invoked by custom implementation
    @SuppressWarnings("unchecked")
    CompletableFuture<V> asyncLoadDelegated(K key, Executor executor) {
        return (extendedPersistenceConfigurer.hasCacheLoaderStrategy()
                ? CompletableFuture.supplyAsync(() -> loadExtendedFromMongo(key), executor)
                : CompletableFuture.completedFuture((V) null))
                .thenCompose(newValue -> nonNull(newValue)
                        ? CompletableFuture.completedFuture(newValue)
                        : (CompletableFuture<V>) getFailable(() -> cacheLoader.asyncLoad(key, executor)));
    }

    // invoked by custom implementation
    @SuppressWarnings("unchecked")
    CompletableFuture<V> asyncReloadDelegated(K key, V oldValue, Executor executor) {
        return (extendedPersistenceConfigurer.hasCacheLoaderStrategy()
                ? CompletableFuture.supplyAsync(() -> loadExtendedFromMongo(key), executor)
                : CompletableFuture.completedFuture((V) null))
                .thenCompose(newValue -> nonNull(newValue)
                        ? CompletableFuture.completedFuture(newValue)
                        : (CompletableFuture<V>) getFailable(() -> cacheLoader.asyncReload(key, oldValue, executor)));
    }

    // based on com.github.benmanes.caffeine.cache.LocalLoadingCache.hasLoadAll()
    boolean hasLoadAll() {
        Method defaultLoadAll = getFailable(() -> CacheLoader.class.getMethod(LOAD_ALL, Set.class));
        Method instanceLoadAll = getFailable(() -> cacheLoader.getClass().getMethod(LOAD_ALL, Set.class));
        return !defaultLoadAll.equals(instanceLoadAll);
    }

    private V loadExtendedFromMongo(K key) {
        return loadAllExtendedFromMongo(Set.of(key)).get(key);
    }

    private Map<? extends K, ? extends V> loadAllExtendedFromMongo(Set<? extends K> keys) {
        Map<K, V> keyToValue = new HashMap<>();
        // no data store filter on status and stale because the newest cache entry is needed
        mongoRepository.consumeCacheDocumentsGroupedByKeyNewestFirstForKeys(
                keys, null,
                null, null,
                stream -> stream.forEach(cacheDocuments -> cacheDocuments.stream()
                        .findFirst()
                        .filter(cacheDocument -> !cacheDocument.isStale())
                        .filter(InternalCacheDocument::isEvictedExtended)
                        .ifPresent(cacheDocument ->
                                keyToValue.put(cacheDocument.getKey(), cacheDocument.getValue()))));
        return keyToValue;
    }
}
