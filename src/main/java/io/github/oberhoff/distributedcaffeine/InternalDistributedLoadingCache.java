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
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.github.oberhoff.distributedcaffeine.InternalKey.ik;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.iks;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.im;
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
    private final ConcurrentMap<K, RefreshOperation<V>> refreshOperations;

    @SuppressWarnings("NotNullFieldNotInitialized")
    private Logger logger;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalCacheLoader<K, V> cacheLoader;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Executor executor;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalDistributedLoadingCache() {
        this.refreshOperations = new ConcurrentHashMap<>();
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        super.initialize(instanceRegistry);
        this.logger = instanceRegistry.getLogger();
        this.cacheLoader = requireNonNull(instanceRegistry.getCacheLoader());
        this.executor = instanceRegistry.getExecutor();
    }

    // a cache loader resolving to null means that nothing is cached, so the result is nullable. Caffeine documents
    // the same for its own method ("or null if the computed value is null") but cannot express it for a value type
    // that is not nullable, and making it one would declare every other method of this cache as holding nullable
    // values, which none of them do
    @Override
    public @Nullable V get(K key) {
        requireNonNull(key);
        // loaded outside the synchronization lock, see getOrLoad
        return getOrLoad(key, cacheLoader::loadDelegated, cacheManager::putAllDistributedLoaded);
    }

    @Override
    public Map<K, V> getAll(Iterable<? extends K> keys) {
        Set<K> keySet = requireNonNullIterable(keys);
        // loaded outside the synchronization lock, see getAllOrLoad
        return getAllOrLoad(keySet, cacheLoader::loadAllDelegated, cacheManager::putAllDistributedLoaded);
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
        Map<K, RefreshOperation<V>> keyToRefreshOperations = keySet.stream()
                .collect(Collectors.toMap(Function.identity(), this::getOrCreateRefreshOperation));
        // settle every refresh individually so that allOf() cannot fail. A single failing key must not discard the
        // values that reloaded successfully: Caffeine applies each refresh in its own whenComplete callback,
        // independently of the aggregated future (LocalLoadingCache.refresh), so a sibling failure never blocks a
        // successful one. The returned future still completes exceptionally, see below.
        CompletableFuture<?>[] settledRefreshOperations = keyToRefreshOperations.values().stream()
                .map(refreshOperation -> refreshOperation.future().handle((value, throwable) -> value))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(settledRefreshOperations)
                .thenApplyAsync(ignored -> applyRefreshes(keyToRefreshOperations), executor);
    }

    // retain the original 'remove if null' semantics (a failed refresh keeps the current value, just like Caffeine,
    // which does not touch the cache when the reload throws)
    private Map<K, V> applyRefreshes(Map<K, RefreshOperation<V>> keyToRefreshOperations) {
        Map<K, V> keysWithNewValues = new HashMap<>();
        Set<K> keysWithNullValues = new HashSet<>();
        RuntimeException failure = null;
        for (Entry<K, RefreshOperation<V>> entry : keyToRefreshOperations.entrySet()) {
            try {
                // already completed (allOf above awaited the settled futures), so this cannot block
                V newValue = entry.getValue().future().join();
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
        synchronizationLock.runLocked(() ->
                applyRefreshesLocked(keyToRefreshOperations, keysWithNewValues, keysWithNullValues));
        if (nonNull(failure)) {
            // propagate as before, but only after the successful subset has been applied and distributed
            throw failure;
        }
        // what was reloaded, whether or not it was applied - just like Caffeine, whose refresh completes with the
        // reloaded value even where it discards it
        return unmodifiableMap(keysWithNewValues);
    }

    // only where nothing overtook the refresh while it was reloading, see isApplicable()
    private void applyRefreshesLocked(Map<K, RefreshOperation<V>> keyToRefreshOperations,
                                      Map<K, V> keysWithNewValues, Set<K> keysWithNullValues) {
        Map<K, V> applicableNewValues = new HashMap<>();
        keysWithNewValues.forEach((key, newValue) -> {
            if (isApplicable(key, requireNonNull(keyToRefreshOperations.get(key)))) {
                applicableNewValues.put(key, newValue);
            } else {
                notifyDiscarded(ik(key), newValue);
            }
        });
        Set<K> applicableNullValues = keysWithNullValues.stream()
                .filter(key -> isApplicable(key, requireNonNull(keyToRefreshOperations.get(key))))
                .collect(Collectors.toSet());
        cache.putAll(cacheManager.putAllDistributedRefresh(im(applicableNewValues)));
        cache.invalidateAll(cacheManager.invalidateAllDistributedRefresh(iks(applicableNullValues)));
    }

    // Whether the result of a refresh may be applied, which is decided the way Caffeine decides it for its own
    // refreshes: only where the refresh still owns the key and the value it reloaded from is still the one held.
    // A write of any kind takes the ownership away (see InternalCacheManager.inFlight), which covers what no
    // comparison of values could - an invalidation of a key that was not held when the refresh started. Comparing
    // the value covers what happens to the key without a write - an eviction, an expiry, a reconciliation - and is
    // a comparison of identities, because a value written anew is a new instance even where it is equal.
    // Called under the synchronization lock, which every write takes as well, so that nothing can come in between
    // deciding and applying. The refresh gives up the key either way
    @SuppressWarnings("ReferenceEquality") // the identity is the point, see above
    private boolean isApplicable(K key, RefreshOperation<V> refreshOperation) {
        return cacheManager.releaseInFlight(ik(key), refreshOperation.token())
                && policy.getIfPresentQuietly(ik(key)) == refreshOperation.oldValue();
    }

    private RefreshOperation<V> getOrCreateRefreshOperation(K key) {
        AtomicReference<@Nullable RefreshOperation<V>> createdRefreshOperation = new AtomicReference<>();
        // retain the original 'only one concurrent refresh operation per key' semantics
        RefreshOperation<V> refreshOperation =
                refreshOperations.compute(key, (k, existingRefreshOperation) -> {
                    if (isNull(existingRefreshOperation) || existingRefreshOperation.future().isDone()) {
                        RefreshOperation<V> newRefreshOperation =
                                createRefreshOperation(key, existingRefreshOperation);
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
        RefreshOperation<V> newRefreshOperation = createdRefreshOperation.get();
        if (nonNull(newRefreshOperation)) {
            attachCompletion(key, newRefreshOperation);
        }
        return refreshOperation;
    }

    private RefreshOperation<V> createRefreshOperation(K key, @Nullable RefreshOperation<V> existingRefreshOperation) {
        // one done but still waiting to be applied is overtaken by this one, just like in Caffeine, where a refresh
        // started anew owns the key from then on
        if (nonNull(existingRefreshOperation)) {
            cacheManager.releaseInFlight(ik(key), existingRefreshOperation.token());
        }
        // what the reload goes on is the value held when it starts, so that is what applying its result is measured
        // against
        InternalValue<V> oldValue = policy.getIfPresentQuietly(ik(key));
        // registered ahead of reloading rather than after it, because a same-thread executor reloads right away and
        // a write made meanwhile would otherwise go unnoticed
        Object token = new Object();
        cacheManager.registerInFlight(ik(key), token);
        // retain the original 'load if null, reload if not null' semantics
        // an explicit target type for the suppliers, because the nullable value type of the operation would
        // otherwise be lost while inferring it from an implicit lambda
        V reloadFrom = vn(oldValue);
        InternalUtils.FailableSupplier<CompletableFuture<@Nullable V>> refreshSupplier = isNull(reloadFrom)
                ? () -> cacheLoader.asyncLoadDelegated(key, executor)
                : () -> cacheLoader.asyncReloadDelegated(key, reloadFrom, executor);
        try {
            return new RefreshOperation<>(token, getFailable(refreshSupplier, CompletionException::new), oldValue);
        } catch (RuntimeException e) {
            cacheManager.releaseInFlight(ik(key), token);
            throw e;
        }
    }

    @SuppressWarnings("FutureReturnValueIgnored") // the callback is attached for its side effects only
    private void attachCompletion(K key, RefreshOperation<V> refreshOperation) {
        refreshOperation.future().whenCompleteAsync((v, e) -> {
            if (isNull(e)) {
                statsCounter.recordLoadSuccess(1);
            } else {
                statsCounter.recordLoadFailure(1);
                logger.log(Level.WARNING,
                        format("Exception thrown during refresh for %s", key), e);
                // nothing is applied for a failed refresh, so nothing else gives up the key for it
                cacheManager.releaseInFlight(ik(key), refreshOperation.token());
            }
            refreshOperations.remove(key, refreshOperation);
        }, executor);
    }

    // a refresh in flight: the token it owns the key by while reloading, and the value it reloads from
    private record RefreshOperation<V>(Object token, CompletableFuture<@Nullable V> future,
                                       @Nullable InternalValue<V> oldValue) {
    }
}
