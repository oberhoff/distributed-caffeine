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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.InternalKey.ik;
import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.iks;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.im;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.m;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullIterable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullMap;
import static io.github.oberhoff.distributedcaffeine.InternalValue.iv;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static io.github.oberhoff.distributedcaffeine.InternalValue.vn;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalDistributedCache<K, V> implements DistributedCache<K, V>, InternalInitializable<K, V> {

    @SuppressWarnings("NotNullFieldNotInitialized")
    protected InternalInstanceRegistry<K, V> instanceRegistry;
    @SuppressWarnings("NotNullFieldNotInitialized")
    protected Cache<InternalKey<K>, InternalValue<V>> cache;
    @SuppressWarnings("NotNullFieldNotInitialized")
    protected Policy<InternalKey<K>, InternalValue<V>> policy;
    @SuppressWarnings("NotNullFieldNotInitialized")
    protected InternalCacheManager<K, V> cacheManager;
    @SuppressWarnings("NotNullFieldNotInitialized")
    protected InternalSynchronizationLock synchronizationLock;
    @SuppressWarnings("NotNullFieldNotInitialized")
    protected EvictedEntryPersistenceConfigurer evictedEntryPersistenceConfigurer;
    @SuppressWarnings("NotNullFieldNotInitialized")
    protected StatsCounter statsCounter;
    @SuppressWarnings("NotNullFieldNotInitialized")
    protected InternalDistributedStatsCounter distributedStatsCounter;
    @SuppressWarnings("NotNullFieldNotInitialized")
    protected InternalRemovalListener<K, V> removalListener;

    // which load each thread is waiting for, across all cache instances, to tell a cycle of loads waiting for each
    // other (see startWaiting). Guarded by itself
    private static final Map<Thread, LoadOperation<?>> WAITING_THREADS = new HashMap<>();

    // the loads in flight by key, so that concurrent misses of one key share the first one's load (see getOrLoad)
    private final ConcurrentMap<K, LoadOperation<V>> loadOperations;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalDistributedCache() {
        this.loadOperations = new ConcurrentHashMap<>();
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.instanceRegistry = instanceRegistry;
        this.cache = instanceRegistry.getCache();
        this.policy = instanceRegistry.getCache().policy();
        this.cacheManager = instanceRegistry.getCacheManager();
        this.synchronizationLock = instanceRegistry.getSynchronizationLock();
        this.evictedEntryPersistenceConfigurer = instanceRegistry.getEvictedEntryPersistenceConfigurer();
        this.statsCounter = instanceRegistry.getStatsCounter();
        this.distributedStatsCounter = instanceRegistry.getDistributedStatsCounter();
        this.removalListener = instanceRegistry.getRemovalListener();
    }

    @Override
    public @Nullable V getIfPresent(K key) {
        requireNonNull(key);
        return vn(cache.getIfPresent(ik(key)));
    }

    @Override
    public Map<K, V> getAllPresent(Iterable<? extends K> keys) {
        Set<K> keySet = requireNonNullIterable(keys);
        return m(cache.getAllPresent(iks(keySet)));
    }

    // a mapping function resolving to null is accepted here (as it is by ConcurrentMap.computeIfAbsent) and means
    // that nothing is cached, so the result is nullable. Caffeine documents the same for its own method ("or null if
    // the computed value is null") but cannot express it for a value type that is not nullable, and making it one
    // would declare every other method of this cache as holding nullable values, which none of them do
    @Override
    public @Nullable V get(K key, Function<? super K, ? extends @Nullable V> mappingFunction) {
        requireNonNull(key);
        requireNonNull(mappingFunction);
        return getOrLoad(key, mappingKey -> {
            // the order the cache loader strategy states, applied to the entry point a cache without a cache
            // loader has instead: the mapping function obtains only what the store could not supply
            V value = evictedEntryPersistenceConfigurer.hasMappingFunctionStrategy()
                    ? cacheManager.loadFromStore(mappingKey)
                    : null;
            return nonNull(value)
                    ? value
                    : mappingFunction.apply(k(mappingKey));
        }, cacheManager::putAllDistributed);
    }

    @Override
    public Map<K, V> getAll(Iterable<? extends K> keys, Function<? super Set<? extends K>,
            ? extends Map<? extends K, ? extends V>> mappingFunction) {
        Set<K> keySet = requireNonNullIterable(keys);
        requireNonNull(mappingFunction);
        return getAllOrLoad(keySet, mappingKeys -> {
            Map<K, V> keyToValue = new HashMap<>();
            Set<K> keysToMap = new HashSet<>(mappingKeys);
            if (evictedEntryPersistenceConfigurer.hasMappingFunctionStrategy()) {
                keyToValue.putAll(cacheManager.loadAllFromStore(iks(mappingKeys)));
                keysToMap.removeAll(keyToValue.keySet());
            }
            // the mapping function is not applied at all once the store supplied every key, which is what Caffeine
            // does with the keys it found itself as well - it only asks for what is missing
            if (!keysToMap.isEmpty()) {
                keyToValue.putAll(mappingFunction.apply(unmodifiableSet(keysToMap)));
            }
            return keyToValue;
        }, cacheManager::putAllDistributed);
    }

    // Loading outside the synchronization lock, which a load used to hold for as long as the cache loader or the
    // mapping function took - so every miss stalled every other operation of this cache instance, writes and arriving
    // cache entries alike: with eight concurrent misses on a loader taking 100 ms, writes dropped from 702 to 4 per
    // second. Caffeine's own get() cannot do it: its mapping function runs inside Caffeine's lock of the key, and
    // taking the synchronization lock from there to publish deadlocks with arriving cache entries, which hold the
    // synchronization lock while they need Caffeine's lock of the key to be applied.
    // So the load happens here, keeping Caffeine's semantics: a hit is read as Caffeine reads it (counting it,
    // recording the access, triggering refreshAfterWrite), concurrent misses of a key wait for the one load, and when
    // that one fails or comes back empty the waiting ones load themselves - just like the ones blocked in Caffeine's
    // computeIfAbsent would, and counting a hit for the ones that waited, as Caffeine does.
    // What comes back is applied only where nothing overtook the load (see isApplicable)
    protected @Nullable V getOrLoad(K key, Load<K, V> load, Publish<K, V> publish) {
        InternalKey<K> internalKey = ik(key);
        while (true) {
            // Read the way Caffeine reads a hit - counting it, recording the access for eviction and expiry, and
            // triggering refreshAfterWrite, none of which a quiet read does - wherever no load of the key is in
            // flight, which is nearly always: a miss then means this call loads, so counting it as one is right.
            // Where a load is in flight, this call is about to wait for it, and one waiting counts a hit once the wait
            // is over - as Caffeine counts it, where the waiting one finds the value inside computeIfAbsent - so it is
            // only looked up quietly, unless the value is there already. Two calls missing at the very same moment
            // may both count a miss, of which Caffeine would have counted one as the waiting one's hit
            boolean missCounted = false;
            if (!loadOperations.containsKey(key) || nonNull(policy.getIfPresentQuietly(internalKey))) {
                InternalValue<V> present = cache.getIfPresent(internalKey);
                if (nonNull(present)) {
                    return v(present);
                }
                missCounted = true;
            }
            LoadOperation<V> loadOperation = new LoadOperation<>(Thread.currentThread(), new CompletableFuture<>());
            LoadOperation<V> existingLoadOperation = loadOperations.putIfAbsent(key, loadOperation);
            if (isNull(existingLoadOperation)) {
                countMiss(missCounted);
                return loadShared(key, loadOperation, load, publish);
            }
            if (!startWaiting(key, existingLoadOperation)) {
                // waiting would close a cycle, so this one loads by itself instead of sharing the load, see
                // startWaiting. Whichever of the two loads is applied first stays, the other one is discarded as
                // overtaken by it
                countMiss(missCounted);
                return loadAndApply(internalKey, load, publish);
            }
            V value = awaitShared(existingLoadOperation, missCounted);
            if (nonNull(value)) {
                return value;
            }
        }
    }

    // the load the ones missing the key meanwhile wait for, which they have to hear of however it ends - an Error
    // included, since they would otherwise wait for it forever
    @SuppressWarnings("java:S1181")
    private @Nullable V loadShared(K key, LoadOperation<V> loadOperation, Load<K, V> load, Publish<K, V> publish) {
        try {
            V value = loadAndApply(ik(key), load, publish);
            loadOperation.future().complete(value);
            return value;
        } catch (RuntimeException | Error e) {
            loadOperation.future().completeExceptionally(e);
            throw e;
        } finally {
            loadOperations.remove(key, loadOperation);
        }
    }

    // what a load of another thread came back with, or nothing where it failed or came back empty - in which case
    // the waiting one loads anew, see getOrLoad
    private @Nullable V awaitShared(LoadOperation<V> loadOperation, boolean missCounted) {
        try {
            V value = loadOperation.future().join();
            // a hit, unless counted as a miss already, see getOrLoad
            if (nonNull(value) && !missCounted) {
                statsCounter.recordHits(1);
            }
            return value;
        } catch (CompletionException | CancellationException e) {
            return null;
        } finally {
            stopWaiting();
        }
    }

    private void countMiss(boolean missCounted) {
        if (!missCounted) {
            statsCounter.recordMisses(1);
        }
    }

    // Whether to wait for a load of another thread. Waiting would never end where that load is itself waiting -
    // directly or through the loads of other threads - for a load of this very thread: one loader asking for a key
    // whose loader asks for the first key, at the same time on two threads. The synchronization lock used to rule
    // that out by letting a single thread load at a time, reentrantly, so this keeps it working rather than hanging
    // (as Caffeine may, which leaves cycles across keys to its documentation) or failing: the thread closing the cycle
    // loads by itself, at the cost of loading that key twice. Only a load asking for the very key this thread is
    // loading already is reported, the way Caffeine reports a recursive computation of a key - loading it again would
    // ask for it again, endlessly.
    // Checking and registering happen in one step, so that of two threads about to close a cycle the second one sees
    // the first
    private static boolean startWaiting(Object key, LoadOperation<?> loadOperation) {
        Thread currentThread = Thread.currentThread();
        if (loadOperation.loader().equals(currentThread)) {
            throw new IllegalStateException(format("Recursive load of %s", key));
        }
        synchronized (WAITING_THREADS) {
            Thread loader = loadOperation.loader();
            // bounded by the number of waiting threads, which no cycle can exceed - none can form, see above
            for (int hops = 0; nonNull(loader) && hops <= WAITING_THREADS.size(); hops++) {
                if (loader.equals(currentThread)) {
                    return false;
                }
                LoadOperation<?> awaited = WAITING_THREADS.get(loader);
                loader = nonNull(awaited) ? awaited.loader() : null;
            }
            WAITING_THREADS.put(currentThread, loadOperation);
            return true;
        }
    }

    private static void stopWaiting() {
        synchronized (WAITING_THREADS) {
            WAITING_THREADS.remove(Thread.currentThread());
        }
    }

    private @Nullable V loadAndApply(InternalKey<K> key, Load<K, V> load, Publish<K, V> publish) {
        Object token = new Object();
        cacheManager.registerInFlight(key, token);
        long startTime = System.nanoTime();
        boolean success = false;
        try {
            V value;
            try {
                value = load.load(key);
            } catch (Exception e) {
                throw toUnchecked(e);
            }
            if (nonNull(value)) {
                InternalValue<V> internalValue = iv(value);
                synchronizationLock.runLocked(() -> {
                    if (isApplicable(key, token)) {
                        cache.putAll(publish.publish(Map.of(key, internalValue)));
                    } else {
                        notifyDiscarded(key, value);
                    }
                });
                success = true;
            }
            return value;
        } finally {
            // a no-op where applying released it already
            cacheManager.releaseInFlight(key, token);
            // after applying, so that a failing publish counts as the failed load it was before
            long loadTime = System.nanoTime() - startTime;
            if (success) {
                statsCounter.recordLoadSuccess(loadTime);
            } else {
                statsCounter.recordLoadFailure(loadTime);
            }
        }
    }

    // The same for a bulk load, which keeps the semantics of Caffeine's own bulk load: it waits for nothing and is
    // waited for by nothing, so it loads whatever is missing when it starts, and what comes back - the keys beyond the
    // ones asked for included - is cached and distributed even where it replaces what was written meanwhile, just like
    // Caffeine replaces it (LocalManualCache.bulkLoad). Only where synchronization started anew in between is nothing
    // of it applied: what it rests on was read before synchronizing settled the content anew, the same reason a
    // single load gives up its token then (see InternalCacheManager.renewActivation)
    protected Map<K, V> getAllOrLoad(Set<K> keys, BulkLoad<K, V> bulkLoad, Publish<K, V> publish) {
        Map<K, V> found = m(cache.getAllPresent(iks(keys)));
        Set<K> keysToLoad = new LinkedHashSet<>();
        keys.forEach(key -> {
            if (!found.containsKey(key)) {
                keysToLoad.add(key);
            }
        });
        if (keysToLoad.isEmpty()) {
            return found;
        }
        String activationId = cacheManager.getActivationId();
        long startTime = System.nanoTime();
        boolean success = false;
        try {
            Map<? extends K, ? extends V> loaded;
            try {
                loaded = bulkLoad.loadAll(unmodifiableSet(keysToLoad));
            } catch (Exception e) {
                throw toUnchecked(e);
            }
            Map<InternalKey<K>, InternalValue<V>> loadedValues = new HashMap<>();
            loaded.forEach((key, value) -> loadedValues.put(ik(requireNonNull(key)), iv(requireNonNull(value))));
            synchronizationLock.runLocked(() -> applyBulk(loadedValues, activationId, publish));
            success = !loadedValues.isEmpty();
            return resultOf(keys, found, loadedValues);
        } finally {
            long loadTime = System.nanoTime() - startTime;
            if (success) {
                statsCounter.recordLoadSuccess(loadTime);
            } else {
                statsCounter.recordLoadFailure(loadTime);
            }
        }
    }

    private void applyBulk(Map<InternalKey<K>, InternalValue<V>> loadedValues, @Nullable String activationId,
                           Publish<K, V> publish) {
        if (Objects.equals(activationId, cacheManager.getActivationId())) {
            if (!loadedValues.isEmpty()) {
                cache.putAll(publish.publish(loadedValues));
            }
        } else {
            loadedValues.forEach((key, value) -> notifyDiscarded(key, v(value)));
        }
    }

    // in the order requested, and only the keys requested, just like Caffeine
    private Map<K, V> resultOf(Set<K> keys, Map<K, V> found, Map<InternalKey<K>, InternalValue<V>> loadedValues) {
        Map<K, V> result = new LinkedHashMap<>();
        keys.forEach(key -> {
            V value = found.containsKey(key)
                    ? found.get(key)
                    : vn(loadedValues.get(ik(key)));
            if (nonNull(value)) {
                result.put(key, value);
            }
        });
        return unmodifiableMap(result);
    }

    // Whether what was loaded for an absent key may be applied: only where the load still holds its token, so that
    // no write overtook it, and the key is still absent - a load never replaces a value, it only fills a gap.
    // Called under the synchronization lock, which every write takes as well, so that nothing can come in between
    // deciding and applying. The load gives up its token either way
    private boolean isApplicable(InternalKey<K> key, Object token) {
        return cacheManager.releaseInFlight(key, token) && isNull(policy.getIfPresentQuietly(key));
    }

    // Reports a loaded or refreshed value that was discarded because a write overtook it (see
    // InternalRemovalListener.notifyDiscarded): REPLACED where the key holds another value now, EXPLICIT where it is
    // gone - which is what Caffeine reports once the overtaking write has replaced or removed it. Nothing where the
    // key holds this very value, which another call applied already - Caffeine reports no removal for a refresh that
    // reloaded the instance held either. Called under the synchronization lock, where what overtook it is settled
    @SuppressWarnings("ReferenceEquality") // the identity is the point, see above
    protected void notifyDiscarded(InternalKey<K> key, V value) {
        InternalValue<V> present = policy.getIfPresentQuietly(key);
        if (isNull(present)) {
            distributedStatsCounter.recordDiscardedLoad();
            removalListener.notifyDiscarded(k(key), value, RemovalCause.EXPLICIT);
        } else if (v(present) != value) {
            distributedStatsCounter.recordDiscardedLoad();
            removalListener.notifyDiscarded(k(key), value, RemovalCause.REPLACED);
        }
    }

    // as Caffeine converts what a cache loader throws (Caffeine.toUnchecked)
    private static RuntimeException toUnchecked(Exception e) {
        if (e instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new CompletionException(e);
    }

    // A load as either kind of cache instance supplies it: a cache loader, which may throw checked exceptions, or a
    // mapping function, which cannot.
    // java:S112 - declared to throw what Caffeine's CacheLoader is declared to throw, since that is what it passes on
    // and Caffeine.toUnchecked is what it is converted by (see toUnchecked)
    @SuppressWarnings("java:S112")
            // Handed the key instance what is loaded is applied with, which caches its hash, so that reading the store and
            // publishing compute it once
    interface Load<K, V> {
        @Nullable V load(InternalKey<K> key) throws Exception;
    }

    // java:S112 - see above. java:S1452 - returning what CacheLoader.loadAll and a bulk mapping function are declared
    // to return, which is what is handed over here unchanged
    @SuppressWarnings({"java:S112", "java:S1452"})
    interface BulkLoad<K, V> {
        Map<? extends K, ? extends V> loadAll(Set<K> keys) throws Exception;
    }

    // How what was loaded is distributed, which differs by the status it is published with.
    // java:S1452 - returning what the publishing methods of InternalCacheManager return, which it is a reference to
    @SuppressWarnings("java:S1452")
    interface Publish<K, V> {
        Map<? extends InternalKey<K>, ? extends InternalValue<V>> publish(Map<InternalKey<K>, InternalValue<V>> map);
    }

    // a load in flight: the thread loading, and what the ones waiting for it get
    private record LoadOperation<V>(Thread loader, CompletableFuture<@Nullable V> future) {
    }

    @Override
    public void put(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        synchronizationLock.runLocked(() ->
                cache.put(ik(key), cacheManager.putDistributed(ik(key), iv(value))));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        requireNonNullMap(map);
        synchronizationLock.runLocked(() ->
                cache.putAll(cacheManager.putAllDistributed(im(map))));
    }

    @Override
    public void invalidate(K key) {
        requireNonNull(key);
        synchronizationLock.runLocked(() ->
                cache.invalidate(cacheManager.invalidateDistributed(ik(key))));
    }

    @Override
    public void invalidateAll(Iterable<? extends K> keys) {
        Set<K> keySet = requireNonNullIterable(keys);
        synchronizationLock.runLocked(() ->
                cache.invalidateAll(cacheManager.invalidateAllDistributed(iks(keySet))));
    }

    @Override
    public void invalidateAll() {
        synchronizationLock.runLocked(() -> {
            cacheManager.invalidateAllDistributed();
            cache.invalidateAll();
        });
    }

    @Override
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    @Override
    public CacheStats stats() {
        return cache.stats();
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        return instanceRegistry.initialize(new InternalConcurrentMap<>());
    }

    @Override
    public Policy<K, V> policy() {
        return instanceRegistry.initialize(new InternalPolicy<>());
    }

    @Override
    public DistributedPolicy<K, V> distributedPolicy() {
        return instanceRegistry.initialize(new InternalDistributedPolicy<>());
    }
}
