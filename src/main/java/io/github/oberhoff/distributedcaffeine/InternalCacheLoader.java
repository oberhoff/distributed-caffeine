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
import com.mongodb.client.model.Filters;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.ExtendedPersistenceConfig;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.LazyInitializer;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.HASH;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalCacheLoader<K, V> implements CacheLoader<K, V>, LazyInitializer<K, V> {

    private static final String LOAD_ALL = "loadAll";
    // private static final String ASYNC_LOAD_ALL = "asyncLoadAll";

    private final CacheLoader<K, V> cacheLoader;

    private DistributedCaffeine<K, V> distributedCaffeine;
    private Policy<K, V> policy;
    private InternalSynchronizationLock synchronizationLock;
    private InternalMongoRepository<K, V> mongoRepository;
    private ExtendedPersistenceConfig extendedPersistenceConfig;

    InternalCacheLoader(CacheLoader<K, V> cacheLoader) {
        this.cacheLoader = cacheLoader;
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.distributedCaffeine = distributedCaffeine;
        this.policy = distributedCaffeine.getCache().policy();
        this.synchronizationLock = distributedCaffeine.getSynchronizationLock();
        this.mongoRepository = distributedCaffeine.getMongoRepository();
        this.extendedPersistenceConfig = distributedCaffeine.getExtendedPersistenceConfig();
    }

    @Override
    public @Nullable V load(@NonNull K key) throws Exception {
        synchronizationLock.lock();
        try {
            return Optional.ofNullable(loadDelegated(key))
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
                return distributedCaffeine.putAllDistributed(loadAllDelegated(keys));
            } finally {
                synchronizationLock.unlock();
            }
        } else {
            return keys.stream()
                    .map(key -> Optional.ofNullable(getFailable(() -> load(key)))
                            .map(value -> Map.entry(key, value))
                            .orElse(null))
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
     * This method is only used by refresh operations (if no value found) so special handling can be used.
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
         *  AsyncCache and AsyncLoadingCache are not supported.
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
     * This method is only used by refresh operations (if value found) so special handling can be used.
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
    private boolean hasLoadAll(CacheLoader<?, ?> cacheLoader) throws NoSuchMethodException {
        Method classLoadAll = cacheLoader.getClass().getMethod(LOAD_ALL, Set.class);
        Method defaultLoadAll = CacheLoader.class.getMethod(LOAD_ALL, Set.class);
        return !classLoadAll.equals(defaultLoadAll);
    }

    /*
     * Copy of com.github.benmanes.caffeine.cache.LocalAsyncLoadingCache#canBulkLoad(AsyncCacheLoader).
     *
    private boolean hasAsyncLoadAll(AsyncCacheLoader<?, ?> cacheLoader) throws NoSuchMethodException {
        Class<?> defaultLoaderClass = AsyncCacheLoader.class;
        if (cacheLoader instanceof CacheLoader<?, ?>) {
            defaultLoaderClass = CacheLoader.class;
            Method classLoadAll = cacheLoader.getClass().getMethod(LOAD_ALL, Set.class);
            Method defaultLoadAll = CacheLoader.class.getMethod(LOAD_ALL, Set.class);
            if (!classLoadAll.equals(defaultLoadAll)) {
                return true;
            }
        }
        Method classAsyncLoadAll = cacheLoader.getClass().getMethod(ASYNC_LOAD_ALL, Set.class, Executor.class);
        Method defaultAsyncLoadAll = defaultLoaderClass.getMethod(ASYNC_LOAD_ALL, Set.class, Executor.class);
        return !classAsyncLoadAll.equals(defaultAsyncLoadAll);
    }*/

    private V loadDelegated(K key) throws Exception {
        if (extendedPersistenceConfig.isExtendedPersistence()) {
            return Optional.ofNullable(loadExtendedFromMongo(key))
                    .orElseGet(() -> getFailable(() -> cacheLoader.load(key)));
        } else {
            return cacheLoader.load(key);
        }
    }

    private Map<? extends K, ? extends V> loadAllDelegated(Set<? extends K> keys) throws Exception {
        if (extendedPersistenceConfig.isExtendedPersistence()) {
            HashMap<K, V> result = new HashMap<>(loadAllExtendedFromMongo(keys));
            Set<K> notFoundInMongo = new HashSet<>(keys);
            notFoundInMongo.removeAll(result.keySet());
            result.putAll(cacheLoader.loadAll(notFoundInMongo));
            return result;
        } else {
            return cacheLoader.loadAll(keys);
        }
    }

    private V loadExtendedFromMongo(K key) {
        return loadAllExtendedFromMongo(Set.of(key)).entrySet().stream()
                .filter(entry -> entry.getKey().equals(key))
                .map(Entry::getValue)
                .findFirst()
                .orElse(null);
    }

    private Map<? extends K, ? extends V> loadAllExtendedFromMongo(Set<? extends K> keys) {
        List<Integer> hashes = keys.stream()
                .map(key -> requireNonNull(key, "key cannot be null"))
                .map(Objects::hashCode)
                .collect(Collectors.toList());
        Bson filter = Filters.in(HASH.toString(), hashes);
        Map<K, V> result = new HashMap<>();
        try (Stream<InternalCacheDocument<K, V>> cacheDocumentStream =
                     mongoRepository.streamCacheDocuments(filter)) {
            // retain "hashCode -> bucket -> equals" semantic
            cacheDocumentStream
                    .filter(cacheDocument -> keys.contains(cacheDocument.getKey()))
                    .collect(Collectors.groupingBy(InternalCacheDocument::getKey))
                    .forEach((key, cacheDocuments) -> cacheDocuments.stream()
                            .max(Comparator.naturalOrder())
                            .filter(InternalCacheDocument::isExtended)
                            .ifPresent(cacheDocument -> result.put(cacheDocument.getKey(), cacheDocument.getValue())));
        }
        return result;
    }
}
