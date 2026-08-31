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
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalKey.ik;
import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.im;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.nullable;
import static io.github.oberhoff.distributedcaffeine.InternalValue.iv;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_RETAINED_GROUP;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("java:S1450")
class InternalCacheLoader<K, V> implements CacheLoader<InternalKey<K>, @Nullable InternalValue<V>>,
        InternalInitializable<K, V> {

    private static final String LOAD_ALL = "loadAll";

    private final CacheLoader<K, V> cacheLoader;

    @SuppressWarnings("NotNullFieldNotInitialized")
    private Repository<K, V> repository;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalCacheManager<K, V> cacheManager;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private EvictedEntryPersistenceConfigurer evictedEntryPersistenceConfigurer;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalHasher<K> hasher;
    private boolean hasLoadAll;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalCacheLoader(CacheLoader<K, V> cacheLoader) {
        this.cacheLoader = cacheLoader;
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.repository = instanceRegistry.getAdapter().getRepository();
        this.cacheManager = instanceRegistry.getCacheManager();
        this.evictedEntryPersistenceConfigurer = instanceRegistry.getEvictedEntryPersistenceConfigurer();
        this.hasher = instanceRegistry.getHasher();
        this.hasLoadAll = hasLoadAll();
    }

    @Override
    @SuppressWarnings("java:S2638")
    public @Nullable InternalValue<V> load(InternalKey<K> key) throws Exception {
        V value = evictedEntryPersistenceConfigurer.hasCacheLoaderStrategy()
                ? loadFromStore(key)
                : null;
        value = nonNull(value)
                ? value
                : nullable(cacheLoader.load(k(key)));
        return nonNull(value)
                ? cacheManager.putDistributedLoaded(key, iv(value))
                : null;
    }

    @Override
    public Map<? extends InternalKey<K>, ? extends InternalValue<V>> loadAll(Set<? extends InternalKey<K>> keys)
            throws Exception {
        HashMap<K, V> keyToValue = new HashMap<>();
        Set<K> keysToLoad = keys.stream()
                .map(InternalKey::k)
                .collect(toCollection(HashSet::new));
        if (evictedEntryPersistenceConfigurer.hasCacheLoaderStrategy()) {
            keyToValue.putAll(loadAllFromStore(keys));
            keysToLoad.removeAll(keyToValue.keySet());
        }
        if (!keysToLoad.isEmpty()) {
            // retain the original 'use loadAll() if overridden' semantics
            // accepted drawback: Caffeine loads the keys one by one when loadAll() is not overridden
            // (LocalLoadingCache.loadSequentially), which this cache instance cannot do without turning the single
            // store write below into one per key. Loading them here instead means the batch counts as one load in the
            // statistics rather than one per key, and that a loader failing partway through discards the values
            // loaded before it, which Caffeine would have cached by then
            if (hasLoadAll) {
                keyToValue.putAll(cacheLoader.loadAll(keysToLoad));
            } else {
                for (K key : keysToLoad) {
                    V value = nullable(cacheLoader.load(key));
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
    public CompletableFuture<? extends @Nullable InternalValue<V>> asyncLoad(InternalKey<K> key, Executor executor)
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
    @SuppressWarnings("java:S2638")
    public @Nullable InternalValue<V> reload(InternalKey<K> key, InternalValue<V> oldValue)
            throws Exception {
        throw new IllegalAccessException();
    }

    // only invoked internally if refreshAfterWrite is used (special handling needed)
    @Override
    @SuppressWarnings({"unchecked", "java:S2638"})
    public CompletableFuture<? extends @Nullable InternalValue<V>> asyncReload(InternalKey<K> key,
                                                                               InternalValue<V> oldValue,
                                                                               Executor executor) {
        // An entry the data store has not confirmed since synchronization was stopped is not this cache instance's
        // to distribute, while refreshing it locally is still what the application asked for. So the reload goes
        // through the application's cache loader alone - reading the store would bring back what synchronizing is
        // about to settle - and reaches neither publishing method. What comes back is of no activation either, so
        // nothing is distributed for it and synchronizing removes it unless the store turns out to back it
        if (!cacheManager.hasCurrentActivationId(oldValue)) {
            return getFailable(() -> cacheLoader.asyncReload(k(key), v(oldValue), executor))
                    .thenApply(InternalValue::ivn);
        }
        return (evictedEntryPersistenceConfigurer.hasCacheLoaderStrategy()
                ? CompletableFuture.supplyAsync(() -> loadFromStore(key), executor)
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
    CompletableFuture<@Nullable V> asyncLoadDelegated(K key, Executor executor) {
        return (evictedEntryPersistenceConfigurer.hasCacheLoaderStrategy()
                ? CompletableFuture.supplyAsync(() -> loadFromStore(ik(key)), executor)
                : CompletableFuture.completedFuture((V) null))
                .thenComposeAsync(newValue -> nonNull(newValue)
                                ? CompletableFuture.completedFuture(newValue)
                                : (CompletableFuture<V>) getFailable(() ->
                                cacheLoader.asyncLoad(key, executor)),
                        executor);
    }

    // invoked by custom implementation
    @SuppressWarnings("unchecked")
    CompletableFuture<@Nullable V> asyncReloadDelegated(K key, V oldValue, Executor executor) {
        return (evictedEntryPersistenceConfigurer.hasCacheLoaderStrategy()
                ? CompletableFuture.supplyAsync(() -> loadFromStore(ik(key)), executor)
                : CompletableFuture.completedFuture((V) null))
                .thenComposeAsync(newValue -> nonNull(newValue)
                                ? CompletableFuture.completedFuture(newValue)
                                : (CompletableFuture<V>) getFailable(() ->
                                cacheLoader.asyncReload(key, oldValue, executor)),
                        executor);
    }

    // based on com.github.benmanes.caffeine.cache.LocalLoadingCache.hasLoadAll(), resolved once (see initialize())
    // just like Caffeine does when it builds its bulk mapping function, instead of on every bulk load
    private boolean hasLoadAll() {
        Method defaultLoadAll = getFailable(() -> CacheLoader.class.getMethod(LOAD_ALL, Set.class));
        Method instanceLoadAll = getFailable(() -> cacheLoader.getClass().getMethod(LOAD_ALL, Set.class));
        return !Objects.equals(defaultLoadAll, instanceLoadAll);
    }

    private @Nullable V loadFromStore(InternalKey<K> key) {
        return loadAllFromStore(Set.of(key)).get(k(key));
    }

    private Map<K, V> loadAllFromStore(Set<? extends InternalKey<K>> keys) {
        // the memoizing overload caches each hash on its key instance, so a subsequent publish that reuses the same
        // instance (putDistributedLoaded / refreshAfterWrite on the single-key load path) does not recompute it
        Set<String> hashes = keys.stream()
                .map(hasher::getHash)
                .collect(toSet());
        try (Stream<CacheEntry<K, V>> cacheEntryStream = getFailable(() -> repository.streamCacheEntries(
                hashes,
                EVICTED_RETAINED_GROUP,
                false))) {
            //noinspection NullableProblems
            return cacheEntryStream
                    .filter(cacheEntry -> nonNull(cacheEntry.getValue()))
                    .collect(toMap(CacheEntry::getKey, CacheEntry::getValue));
        }
    }
}
