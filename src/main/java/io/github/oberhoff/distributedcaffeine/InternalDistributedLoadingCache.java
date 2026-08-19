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
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.github.oberhoff.distributedcaffeine.InternalKey.ik;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.entry;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.iks;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.im;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.m;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullIterable;
import static io.github.oberhoff.distributedcaffeine.InternalValue.vn;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalDistributedLoadingCache<K, V> extends InternalDistributedCache<K, V>
        implements DistributedLoadingCache<K, V> {

    // a refresh resolving to null is a valid outcome (it means the key is to be removed), so the value of these
    // operations is nullable - unlike the map itself, which never holds a null future
    private final ConcurrentMap<K, CompletableFuture<@Nullable V>> refreshOperations;

    @SuppressWarnings("NotNullFieldNotInitialized")
    private Logger logger;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private LoadingCache<InternalKey<K>, InternalValue<V>> loadingCache;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalCacheLoader<K, V> cacheLoader;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Executor executor;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private StatsCounter statsCounter;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalDistributedLoadingCache() {
        this.refreshOperations = new ConcurrentHashMap<>();
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        super.initialize(instanceRegistry);
        this.logger = instanceRegistry.getLogger();
        this.loadingCache = (LoadingCache<InternalKey<K>, InternalValue<V>>) cache;
        this.cacheLoader = requireNonNull(instanceRegistry.getCacheLoader());
        this.executor = instanceRegistry.getExecutor();
        this.statsCounter = instanceRegistry.getStatsCounter();
    }

    // a cache loader resolving to null means that nothing is cached, so the result is nullable. Caffeine documents
    // the same for its own method ("or null if the computed value is null") but cannot express it for a value type
    // that is not nullable, and making it one would declare every other method of this cache as holding nullable
    // values, which none of them do
    @Override
    public @Nullable V get(K key) {
        requireNonNull(key);
        return synchronizationLock.getLockedOrNull(() ->
                vn(loadingCache.get(ik(key))));
    }

    @Override
    public Map<K, V> getAll(Iterable<? extends K> keys) {
        Set<K> keySet = requireNonNullIterable(keys);
        return requireNonNull(synchronizationLock.getLocked(() ->
                m(loadingCache.getAll(iks(keySet)))));
    }

    @Override
    public CompletableFuture<@Nullable V> refresh(K key) {
        requireNonNull(key);
        // custom implementation to bypass problematic internal asynchronous handling
        // accepted drawback: no mapping of in-flight refresh operations in policy.refreshes()
        return refreshAll(Set.of(key))
                .<@Nullable V>thenApplyAsync(map -> map.get(key), executor);
    }

    @Override
    public CompletableFuture<Map<K, V>> refreshAll(Iterable<? extends K> keys) {
        Set<K> keySet = requireNonNullIterable(keys);
        // custom implementation to bypass problematic internal asynchronous handling
        // accepted drawback: no mapping of in-flight refresh operations in 'policy.refreshes()'
        Map<K, CompletableFuture<@Nullable V>> keyToCompletableFutureOfValues = keySet.stream()
                .map(key -> entry(key, policy.getIfPresentQuietly(ik(key))))
                .collect(Collectors.toMap(Entry::getKey, entry ->
                        getOrCreateRefreshOperation(entry.getKey(), vn(entry.getValue()))));
        // settle every refresh individually so that allOf() cannot fail. A single failing key must not discard the
        // values that reloaded successfully: Caffeine applies each refresh in its own whenComplete callback,
        // independently of the aggregated future (LocalLoadingCache.refresh), so a sibling failure never blocks a
        // successful one. The returned future still completes exceptionally, see below.
        CompletableFuture<?>[] settledRefreshOperations = keyToCompletableFutureOfValues.values().stream()
                .map(completableFutureOfValue -> completableFutureOfValue.handle((value, throwable) -> value))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(settledRefreshOperations)
                .thenApplyAsync(ignored -> {
                    // retain the original 'remove if null' semantics (a failed refresh keeps the current value,
                    // just like Caffeine, which does not touch the cache when the reload throws)
                    Map<K, V> keysWithNewValues = new HashMap<>();
                    Set<K> keysWithNullValues = new HashSet<>();
                    RuntimeException failure = null;
                    for (Entry<K, CompletableFuture<@Nullable V>> entry : keyToCompletableFutureOfValues.entrySet()) {
                        try {
                            // already completed (allOf above awaited the settled futures), so this cannot block
                            V newValue = entry.getValue().join();
                            if (nonNull(newValue)) {
                                keysWithNewValues.put(entry.getKey(), newValue);
                            } else {
                                keysWithNullValues.add(entry.getKey());
                            }
                        } catch (CompletionException | CancellationException e) {
                            // keep the first failure, mirroring allOf(), and report it once every key was processed
                            failure = isNull(failure)
                                    ? e
                                    : failure;
                        }
                    }
                    synchronizationLock.runLocked(() -> {
                        cache.putAll(cacheManager.putAllDistributedRefresh(im(keysWithNewValues)));
                        cache.invalidateAll(cacheManager.invalidateAllDistributedRefresh(iks(keysWithNullValues)));
                    });
                    if (nonNull(failure)) {
                        // propagate as before, but only after the successful subset has been applied and distributed
                        throw failure;
                    }
                    return unmodifiableMap(keysWithNewValues);
                }, executor);
    }

    @SuppressWarnings("FutureReturnValueIgnored") // the callback below is attached for its side effects only
    private CompletableFuture<@Nullable V> getOrCreateRefreshOperation(K key, @Nullable V oldValue) {
        AtomicReference<@Nullable CompletableFuture<@Nullable V>> createdRefreshOperation = new AtomicReference<>();
        // retain the original 'only one concurrent refresh operation per key' semantics
        CompletableFuture<@Nullable V> refreshOperation =
                refreshOperations.compute(key, (k, existingRefreshOperation) -> {
                    if (isNull(existingRefreshOperation) || existingRefreshOperation.isDone()) {
                        // retain the original 'load if null, reload if not null' semantics
                        // an explicit target type for the suppliers, because the nullable value type of the
                        // operation would otherwise be lost while inferring it from an implicit lambda
                        InternalUtils.FailableSupplier<CompletableFuture<@Nullable V>> refreshSupplier =
                                isNull(oldValue)
                                        ? () -> cacheLoader.asyncLoadDelegated(key, executor)
                                        : () -> cacheLoader.asyncReloadDelegated(key, oldValue, executor);
                        CompletableFuture<@Nullable V> newRefreshOperation =
                                getFailable(refreshSupplier, CompletionException::new);
                        createdRefreshOperation.set(newRefreshOperation);
                        return newRefreshOperation;
                    } else {
                        return existingRefreshOperation;
                    }
                });
        // intention: retain the original 'log exception and swallow' semantics
        // but strange: exceptions are still thrown, so this behavior is imitated
        // additionally count stats due to custom implementation and clean up completed refresh operations.
        // the callback is attached only once, to the newly created operation (not on every coalesced call), so stats
        // are not over-counted; the two-arg remove ensures only this operation is removed and a newer in-flight
        // refresh for the same key is never dropped.
        // it has to be attached outside the mapping function above, because a same-thread executor completes the
        // reload before the callback is even attached and then runs it inline: removing from the map while its own
        // mapping function is still running makes that removal fail, and the failure lands in the callback's
        // (discarded) dependent future instead of anywhere visible, silently leaking an entry per refreshed key
        CompletableFuture<@Nullable V> newRefreshOperation = createdRefreshOperation.get();
        if (nonNull(newRefreshOperation)) {
            newRefreshOperation.whenCompleteAsync((v, e) -> {
                if (isNull(e)) {
                    statsCounter.recordLoadSuccess(1);
                } else {
                    statsCounter.recordLoadFailure(1);
                    logger.log(Level.WARNING,
                            format("Exception thrown during refresh for %s", key), e);
                }
                refreshOperations.remove(key, newRefreshOperation);
            }, executor);
        }
        return refreshOperation;
    }
}
