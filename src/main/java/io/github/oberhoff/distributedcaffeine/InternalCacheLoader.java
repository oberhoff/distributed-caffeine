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
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.im;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullMap;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.s;
import static io.github.oberhoff.distributedcaffeine.InternalValue.iv;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_EXTENDED_GROUP;
import static java.util.Objects.nonNull;

@SuppressWarnings("java:S1450")
class InternalCacheLoader<K, V> implements CacheLoader<InternalKey<K>, InternalValue<V>>,
        InternalLazyInitializer<K, V> {

    private static final String LOAD_ALL = "loadAll";

    private final CacheLoader<K, V> cacheLoader;

    private Repository<K, V> repository;
    private InternalCacheManager<K, V> cacheManager;
    private ExtendedPersistenceConfigurer extendedPersistenceConfigurer;
    private InternalHasher<K> hasher;

    InternalCacheLoader(CacheLoader<K, V> cacheLoader) {
        this.cacheLoader = cacheLoader;
        // see also initialize()
    }

    InternalCacheLoader<K, V> neutralize() {
        // cache manager is initially deactivated
        this.cacheManager = new InternalCacheManager<>();
        return this;
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.repository = instanceRegistry.getAdapter().getRepository();
        this.cacheManager = instanceRegistry.getCacheManager();
        this.extendedPersistenceConfigurer = instanceRegistry.getExtendedPersistenceConfigurer();
        this.hasher = instanceRegistry.getHasher();
    }

    @Override
    @SuppressWarnings({"java:S2583"})
    public InternalValue<V> load(InternalKey<K> key) throws Exception {
        V value = extendedPersistenceConfigurer.hasCacheLoaderStrategy()
                ? loadExtendedFromStore(k(key))
                : null;
        value = nonNull(value)
                ? value
                : cacheLoader.load(k(key));
        return nonNull(value)
                ? cacheManager.putDistributedLoaded(key, iv(value))
                : null;
    }

    @Override
    @SuppressWarnings({"java:S2589"})
    public Map<? extends InternalKey<K>, ? extends InternalValue<V>> loadAll(Set<? extends InternalKey<K>> keys)
            throws Exception {
        HashMap<K, V> keyToValue = new HashMap<>();
        Set<K> keysToLoad = new HashSet<>(s(keys));
        if (extendedPersistenceConfigurer.hasCacheLoaderStrategy()) {
            keyToValue.putAll(loadAllExtendedFromStore(keysToLoad));
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
        return cacheManager.putAllDistributedLoaded(im(keyToValue));
    }

    // should never be invoked due to custom implementation
    @Override
    public CompletableFuture<? extends InternalValue<V>> asyncLoad(InternalKey<K> key, Executor executor)
            throws Exception {
        throw new IllegalAccessException();
    }

    // should never be invoked
    @Override
    public CompletableFuture<? extends Map<? extends InternalKey<K>, ? extends InternalValue<V>>> asyncLoadAll(
            Set<? extends InternalKey<K>> keys, Executor executor) throws Exception {
        throw new IllegalAccessException();
    }

    // should never be invoked
    @Override
    public InternalValue<V> reload(InternalKey<K> key, InternalValue<V> oldValue) throws Exception {
        throw new IllegalAccessException();
    }

    // only invoked internally if refreshAfterWrite is used (special handling needed)
    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<? extends InternalValue<V>> asyncReload(InternalKey<K> key, InternalValue<V> oldValue,
                                                                     Executor executor) {
        return (extendedPersistenceConfigurer.hasCacheLoaderStrategy()
                ? CompletableFuture.supplyAsync(() -> loadExtendedFromStore(k(key)), executor)
                : CompletableFuture.completedFuture((V) null))
                .thenComposeAsync(newValue -> nonNull(newValue)
                                ? CompletableFuture.completedFuture(newValue)
                                : (CompletableFuture<V>) getFailable(() ->
                                cacheLoader.asyncReload(k(key), v(oldValue), executor)),
                        executor)
                // retain the original 'remove if null' semantics
                .thenApplyAsync(newValue -> nonNull(newValue)
                                // special handling, no lock required
                                ? cacheManager.putDistributedRefreshAfterWrite(key, iv(newValue), oldValue)
                                // special handling, no lock required
                                : cacheManager.invalidateDistributedRefreshAfterWrite(key, oldValue),
                        executor);
    }

    // invoked by custom implementation
    @SuppressWarnings("unchecked")
    CompletableFuture<V> asyncLoadDelegated(K key, Executor executor) {
        return (extendedPersistenceConfigurer.hasCacheLoaderStrategy()
                ? CompletableFuture.supplyAsync(() -> loadExtendedFromStore(key), executor)
                : CompletableFuture.completedFuture((V) null))
                .thenComposeAsync(newValue -> nonNull(newValue)
                                ? CompletableFuture.completedFuture(newValue)
                                : (CompletableFuture<V>) getFailable(() ->
                                cacheLoader.asyncLoad(key, executor)),
                        executor);
    }

    // invoked by custom implementation
    @SuppressWarnings("unchecked")
    CompletableFuture<V> asyncReloadDelegated(K key, V oldValue, Executor executor) {
        return (extendedPersistenceConfigurer.hasCacheLoaderStrategy()
                ? CompletableFuture.supplyAsync(() -> loadExtendedFromStore(key), executor)
                : CompletableFuture.completedFuture((V) null))
                .thenComposeAsync(newValue -> nonNull(newValue)
                                ? CompletableFuture.completedFuture(newValue)
                                : (CompletableFuture<V>) getFailable(() ->
                                cacheLoader.asyncReload(key, oldValue, executor)),
                        executor);
    }

    // based on com.github.benmanes.caffeine.cache.LocalLoadingCache.hasLoadAll()
    boolean hasLoadAll() {
        Method defaultLoadAll = getFailable(() -> CacheLoader.class.getMethod(LOAD_ALL, Set.class));
        Method instanceLoadAll = getFailable(() -> cacheLoader.getClass().getMethod(LOAD_ALL, Set.class));
        return !defaultLoadAll.equals(instanceLoadAll);
    }

    private V loadExtendedFromStore(K key) {
        return loadAllExtendedFromStore(Set.of(key)).get(key);
    }

    private Map<? extends K, ? extends V> loadAllExtendedFromStore(Set<? extends K> keys) {
        // TODO discriminator
        try (Stream<CacheEntry<K, V>> cacheEntryStream = getFailable(() -> repository.streamCacheEntries(
                null,
                hasher.getHashes(keys),
                EVICTED_EXTENDED_GROUP,
                null,
                false))) {
            return cacheEntryStream
                    .collect(HashMap::new, (hashMap, cacheEntry) -> // allow null values
                            hashMap.put(cacheEntry.getKey(), cacheEntry.getValue()), HashMap::putAll);
        }
    }
}
