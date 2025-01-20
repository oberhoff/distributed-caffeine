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

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Policy;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.LazyInitializer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

class InternalCacheLoader<K, V> implements CacheLoader<K, V>, LazyInitializer<K, V> {

    private static final String LOAD_ALL = "loadAll";
    // private static final String ASYNC_LOAD_ALL = "asyncLoadAll";

    private final CacheLoader<K, V> cacheLoader;

    private DistributedCaffeine<K, V> distributedCaffeine;
    private Policy<K, V> policy;
    private InternalSynchronizationLock synchronizationLock;

    InternalCacheLoader(CacheLoader<K, V> cacheLoader) {
        this.cacheLoader = cacheLoader;
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.distributedCaffeine = distributedCaffeine;
        this.policy = distributedCaffeine.getCache().policy();
        this.synchronizationLock = distributedCaffeine.getSynchronizationLock();
    }

    @Override
    public @Nullable V load(@NonNull K key) throws Exception {
        synchronizationLock.lock();
        try {
            return Optional.ofNullable(cacheLoader.load(key))
                    .map(value -> distributedCaffeine.putDistributed(key, value))
                    .orElse(null);
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public @NonNull Map<? extends K, ? extends V> loadAll(@NonNull Set<? extends K> keys) throws Exception {
        if (hasLoadAll(cacheLoader)) {
            synchronizationLock.lock();
            try {
                return distributedCaffeine.putAllDistributed(cacheLoader.loadAll(keys));
            } finally {
                synchronizationLock.unlock();
            }
        } else {
            return keys.stream()
                    .map(key -> {
                        try {
                            return Optional.ofNullable(load(key))
                                    .map(value -> Map.entry(key, value))
                                    .orElse(null);
                        } catch (Exception e) {
                            throw new DistributedCaffeineException(e);
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }
    }

    @Override
    public @Nullable V reload(@NonNull K key, @NonNull V oldValue) throws Exception {
        synchronizationLock.lock();
        try {
            V value = cacheLoader.reload(key, oldValue);
            // retain the original 'remove if null' semantic
            if (nonNull(value)) {
                return distributedCaffeine.putDistributed(key, value);
            } else {
                distributedCaffeine.invalidateDistributed(key);
                return null;
            }
        } finally {
            synchronizationLock.unlock();
        }
    }

    /*
     * This method is only used by refresh operations (if no value found) so special handling works as expected.
     * Of course this only applies as long as AsyncCache and AsyncLoadingCache are not supported.
     */
    @Override
    public @NonNull CompletableFuture<? extends V> asyncLoad(@NonNull K key, @NonNull Executor executor)
            throws Exception {
        return cacheLoader.asyncLoad(key, executor)
                .thenApply(newValue -> {
                    if (nonNull(newValue)) {
                        // special handling, no lock required
                        V oldValue = policy.getIfPresentQuietly(key);
                        return distributedCaffeine.putDistributedRefresh(key, newValue, oldValue);
                    } else {
                        return null;
                    }
                });
    }

    @Override
    public @NonNull CompletableFuture<? extends Map<? extends K, ? extends V>> asyncLoadAll(
            @NonNull Set<? extends K> keys, @NonNull Executor executor) {
        throw new UnsupportedOperationException();
        /*
         *  AsyncCache and AsyncLoadingCache are not supported (yet).
         *
        if (hasAsyncLoadAll(cacheLoader)) {
            return cacheLoader.asyncLoadAll(keys, executor)
                    .thenApply(map -> distributedCaffeine.putAllDistributedAsync(map));
        } else {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return loadAll(keys);
                } catch (Exception e) {
                    throw new DistributedCaffeineException(e);
                }
            }, executor);
        } */
    }

    /*
     * This method is only used by refresh operations (if value found) so special handling works as expected.
     */
    @Override
    public @NonNull CompletableFuture<? extends V> asyncReload(@NonNull K key, @NonNull V oldValue,
                                                               @NonNull Executor executor) throws Exception {
        return cacheLoader.asyncReload(key, oldValue, executor)
                .thenApply(newValue -> {
                    // retain the original 'remove if null' semantic
                    if (nonNull(newValue)) {
                        // special handling, no lock required
                        return distributedCaffeine.putDistributedRefresh(key, newValue, oldValue);
                    } else {
                        // special handling, no lock required
                        return distributedCaffeine.invalidateDistributedRefresh(key, oldValue);
                    }
                });
    }

    /*
     * Copy of com.github.benmanes.caffeine.cache.LocalLoadingCache#hasLoadAll(CacheLoader).
     */
    private boolean hasLoadAll(CacheLoader<?, ?> loader) throws NoSuchMethodException {
        Method classLoadAll = loader.getClass().getMethod(LOAD_ALL, Set.class);
        Method defaultLoadAll = CacheLoader.class.getMethod(LOAD_ALL, Set.class);
        return !classLoadAll.equals(defaultLoadAll);
    }

    /*
     * Copy of com.github.benmanes.caffeine.cache.LocalAsyncLoadingCache#canBulkLoad(AsyncCacheLoader).
     *
    private boolean hasAsyncLoadAll(AsyncCacheLoader<?, ?> loader) throws NoSuchMethodException {
        Class<?> defaultLoaderClass = AsyncCacheLoader.class;
        if (loader instanceof CacheLoader<?, ?>) {
            defaultLoaderClass = CacheLoader.class;
            Method classLoadAll = loader.getClass().getMethod(LOAD_ALL, Set.class);
            Method defaultLoadAll = CacheLoader.class.getMethod(LOAD_ALL, Set.class);
            if (!classLoadAll.equals(defaultLoadAll)) {
                return true;
            }
        }
        Method classAsyncLoadAll = loader.getClass().getMethod(ASYNC_LOAD_ALL, Set.class, Executor.class);
        Method defaultAsyncLoadAll = defaultLoaderClass.getMethod(ASYNC_LOAD_ALL, Set.class, Executor.class);
        return !classAsyncLoadAll.equals(defaultAsyncLoadAll);
    }*/
}
