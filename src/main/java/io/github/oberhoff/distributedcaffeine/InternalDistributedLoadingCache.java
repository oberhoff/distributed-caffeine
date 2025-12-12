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

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static io.github.oberhoff.distributedcaffeine.InternalUtils.entry;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullIterable;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalDistributedLoadingCache<K, V> extends InternalDistributedCache<K, V>
        implements DistributedLoadingCache<K, V> {

    private final ConcurrentMap<K, CompletableFuture<V>> refreshOperations;

    private Logger logger;
    private LoadingCache<K, V> loadingCache;
    private InternalCacheLoader<K, V> cacheLoader;
    private Executor executor;
    private StatsCounter statsCounter;

    InternalDistributedLoadingCache() {
        this.refreshOperations = new ConcurrentHashMap<>();
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        super.initialize(distributedCaffeine);
        this.logger = distributedCaffeine.getLogger();
        this.loadingCache = (LoadingCache<K, V>) cache;
        this.cacheLoader = distributedCaffeine.getCacheLoader();
        this.executor = distributedCaffeine.getExecutor();
        this.statsCounter = distributedCaffeine.getStatsCounter();
    }

    @Override
    public V get(K key) {
        requireNonNull(key);
        return synchronizationLock.getLocked(() ->
                loadingCache.get(key));
    }

    @Override
    public Map<K, V> getAll(Iterable<? extends K> keys) {
        Set<K> keySet = requireNonNullIterable(keys);
        return synchronizationLock.getLocked(() ->
                loadingCache.getAll(keySet));
    }

    @Override
    public CompletableFuture<V> refresh(K key) {
        requireNonNull(key);
        // custom implementation to bypass problematic internal asynchronous handling
        // accepted drawback: no mapping of in-flight refresh operations in policy.refreshes()
        return refreshAll(Set.of(key))
                .thenApply(map -> map.get(key));
    }

    @Override
    public CompletableFuture<Map<K, V>> refreshAll(Iterable<? extends K> keys) {
        Set<K> keySet = requireNonNullIterable(keys);
        // custom implementation to bypass problematic internal asynchronous handling
        // accepted drawback: no mapping of in-flight refresh operations in 'policy.refreshes()'
        return CompletableFuture.supplyAsync(() -> {
            Map<K, V> keyToNewValue = keySet.stream()
                    .map(key -> entry(key, policy.getIfPresentQuietly(key)))
                    .map(entry -> entry(entry.getKey(), getOrCreateRefreshOperation(entry.getKey(), entry.getValue())))
                    .toList().stream() // intermediate step to ensure concurrency
                    .map(entry -> entry(entry.getKey(), entry.getValue().join()))
                    .collect(HashMap::new, (hashMap, entry) -> // allow null values
                            hashMap.put(entry.getKey(), entry.getValue()), HashMap::putAll);
            // retain the original 'remove if null' semantics
            Map<K, V> keysWithNewValues = new HashMap<>();
            Set<K> keysWithNullValues = new HashSet<>();
            keyToNewValue.forEach((key, newValue) -> {
                if (nonNull(newValue)) {
                    keysWithNewValues.put(key, newValue);
                } else {
                    keysWithNullValues.add(key);
                }
            });
            synchronizationLock.runLocked(() -> {
                cache.putAll(cacheManager.putAllDistributedRefresh(keysWithNewValues));
                cache.invalidateAll(cacheManager.invalidateAllDistributedRefresh(keysWithNullValues));
            });
            return unmodifiableMap(keysWithNewValues);
        }, executor);
    }

    private CompletableFuture<V> getOrCreateRefreshOperation(K key, V oldValue) {
        // retain the original 'only one concurrent refresh operation per key' semantics
        return refreshOperations.compute(key, (k, refreshOperation) -> {
            if (isNull(refreshOperation) || refreshOperation.isCompletedExceptionally()) {
                // retain the original 'load if null, reload if not null' semantics
                return (isNull(oldValue)
                        ? getFailable(() -> cacheLoader.asyncLoadDelegated(key, executor),
                        CompletionException::new)
                        : getFailable(() -> cacheLoader.asyncReloadDelegated(key, oldValue, executor),
                        CompletionException::new))
                        // intention: retain the original 'log exception and swallow' semantics
                        // but strange: exceptions are still thrown, so this behavior is imitated
                        // asynchronous because refreshOperations must not be modified during computation
                        // additionally count stats due to custom implementation
                        .whenCompleteAsync((v, e) -> {
                            refreshOperations.remove(key);
                            if (isNull(e)) {
                                statsCounter.recordLoadSuccess(1);
                            } else {
                                statsCounter.recordLoadFailure(1);
                                logger.log(Level.WARNING,
                                        format("Exception thrown during refresh for %s", key), e);
                            }
                        }, executor);
            } else {
                return refreshOperation;
            }
        });
    }
}
