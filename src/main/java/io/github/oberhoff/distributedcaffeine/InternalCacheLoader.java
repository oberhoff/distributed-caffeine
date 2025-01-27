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
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NonNull;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.HASH;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("squid:S1452")
class InternalCacheLoader<K, V> implements CacheLoader<K, V>, InternalLazyInitializer<K, V> {

    private static final String LOAD_ALL = "loadAll";

    private final CacheLoader<K, V> cacheLoader;

    private InternalCacheManager<K, V> cacheManager;
    private InternalMongoRepository<K, V> mongoRepository;
    private InternalExtendedPersistence extendedPersistence;

    InternalCacheLoader(CacheLoader<K, V> cacheLoader) {
        this.cacheLoader = cacheLoader;
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.cacheManager = distributedCaffeine.getCacheManager();
        this.mongoRepository = distributedCaffeine.getMongoRepository();
        this.extendedPersistence = distributedCaffeine.getExtendedPersistence();
    }

    @Override // should never be invoked due to custom implementation (loadDelegated() must be used)
    public V load(@NonNull K key) throws Exception {
        throw new IllegalAccessException();
    }

    @Override // should never be invoked due to custom implementation (loadAllDelegated() must be used)
    public @NonNull Map<? extends K, ? extends V> loadAll(@NonNull Set<? extends K> keys) throws Exception {
        throw new IllegalAccessException();
    }

    @Override // should never be invoked due to custom implementation (asyncLoadDelegated() must be used)
    public @NonNull CompletableFuture<? extends V> asyncLoad(@NonNull K key, @NonNull Executor executor)
            throws Exception {
        throw new IllegalAccessException();
    }

    @Override // should never be invoked due to custom implementation (asyncLoadAllDelegated() must be used)
    public @NonNull CompletableFuture<? extends Map<? extends K, ? extends V>> asyncLoadAll(
            @NonNull Set<? extends K> keys, @NonNull Executor executor) throws Exception {
        throw new IllegalAccessException();
    }

    @Override // should never be invoked due to custom implementation (reloadDelegated() must be used)
    public V reload(@NonNull K key, @NonNull V oldValue) throws Exception {
        throw new IllegalAccessException();
    }

    @Override // only invoked internally if expireAfterWrite is used (special handling needed)
    public @NonNull CompletableFuture<? extends V> asyncReload(@NonNull K key, @NonNull V oldValue,
                                                               @NonNull Executor executor) throws Exception {
        return cacheLoader.asyncReload(key, oldValue, executor)
                .thenApply(newValue -> {
                    // retain the original 'remove if null' semantics
                    if (nonNull(newValue)) {
                        // special handling, no lock required
                        return cacheManager.putDistributedRefreshAfterWrite(key, newValue, oldValue);
                    } else {
                        // special handling, no lock required
                        return cacheManager.invalidateDistributedRefreshAfterWrite(key, oldValue);
                    }
                });
    }

    // invoked by custom implementation
    V loadDelegated(K key) throws Exception {
        if (extendedPersistence.hasExtendedPersistenceLoader()) {
            V newValue = loadExtendedFromMongo(key);
            if (nonNull(newValue)) {
                return newValue;
            } else {
                return cacheLoader.load(key);
            }
        } else {
            return cacheLoader.load(key);
        }
    }

    // invoked by custom implementation
    Map<? extends K, ? extends V> loadAllDelegated(Set<? extends K> keys) throws Exception {
        if (extendedPersistence.hasExtendedPersistenceLoader()) {
            HashMap<K, V> keyToNewValue = new HashMap<>(loadAllExtendedFromMongo(keys));
            Set<K> notFound = new HashSet<>(keys);
            notFound.removeAll(keyToNewValue.keySet());
            keyToNewValue.putAll(cacheLoader.loadAll(notFound));
            return keyToNewValue;
        } else {
            return cacheLoader.loadAll(keys);
        }
    }

    @NonNull // invoked by custom implementation
    @SuppressWarnings("unchecked")
    CompletableFuture<? extends V> asyncLoadDelegated(@NonNull K key, @NonNull Executor executor) throws Exception {
        if (extendedPersistence.hasExtendedPersistenceLoader()) {
            return CompletableFuture.supplyAsync(() -> loadExtendedFromMongo(key), executor)
                    .thenCompose(newValue -> {
                        if (nonNull(newValue)) {
                            return CompletableFuture.completedFuture(newValue);
                        } else {
                            return (CompletableFuture<V>) getFailable(() ->
                                            cacheLoader.asyncLoad(key, executor),
                                    CompletionException::new);
                        }
                    });
        } else {
            return cacheLoader.asyncLoad(key, executor);
        }
    }

    @NonNull // invoked by custom implementation
    @SuppressWarnings("unchecked")
    CompletableFuture<? extends V> asyncReloadDelegated(@NonNull K key, @NonNull V oldValue, @NonNull Executor executor)
            throws Exception {
        if (extendedPersistence.hasExtendedPersistenceLoader()) {
            return CompletableFuture.supplyAsync(() -> loadExtendedFromMongo(key), executor)
                    .thenCompose(newValue -> {
                        if (nonNull(newValue)) {
                            return CompletableFuture.completedFuture(newValue);
                        } else {
                            return (CompletableFuture<V>) getFailable(() ->
                                            cacheLoader.asyncReload(key, oldValue, executor),
                                    CompletionException::new);
                        }
                    });
        } else {
            return cacheLoader.asyncReload(key, oldValue, executor);
        }
    }

    // based on com.github.benmanes.caffeine.cache.LocalLoadingCache.hasLoadAll()
    boolean hasLoadAll() {
        Method instanceLoadAll = getFailable(() -> cacheLoader.getClass().getMethod(LOAD_ALL, Set.class));
        Method defaultLoadAll = getFailable(() -> CacheLoader.class.getMethod(LOAD_ALL, Set.class));
        return !instanceLoadAll.equals(defaultLoadAll);
    }

    private V loadExtendedFromMongo(K key) {
        return loadAllExtendedFromMongo(Set.of(key)).get(key);
    }

    private Map<? extends K, ? extends V> loadAllExtendedFromMongo(Set<? extends K> keys) {
        List<Integer> hashes = keys.stream()
                .map(Objects::hashCode)
                .collect(toList());
        Bson filter = Filters.in(HASH.toString(), hashes);
        Map<K, V> keyToValue = new HashMap<>();
        try (Stream<InternalCacheDocument<K, V>> cacheDocumentStream =
                     mongoRepository.streamCacheDocuments(filter)) {
            cacheDocumentStream
                    .filter(cacheDocument -> keys.contains(cacheDocument.getKey()))
                    // retain "hashCode -> bucket -> equals" semantics
                    .collect(groupingBy(InternalCacheDocument::getKey))
                    .forEach((key, cacheDocuments) -> cacheDocuments.stream()
                            .max(Comparator.naturalOrder())
                            .filter(InternalCacheDocument::isEvictedExtended)
                            .ifPresent(cacheDocument ->
                                    keyToValue.put(cacheDocument.getKey(), cacheDocument.getValue())));
        }
        return keyToValue;
    }
}
