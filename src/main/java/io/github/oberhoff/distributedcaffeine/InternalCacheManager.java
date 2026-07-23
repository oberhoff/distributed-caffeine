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
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.ExtendedPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.adapter.Retriever;

import java.lang.ref.WeakReference;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalKey.ik;
import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.entry;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.runFailable;
import static io.github.oberhoff.distributedcaffeine.InternalValue.iv;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_LOADED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED_AFTER_WRITE;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("java:S1452")
class InternalCacheManager<K, V> implements InternalLazyInitializer<K, V>, Retriever<K, V> {

    private final AtomicBoolean isActivated;
    private final SecureRandom secureRandom;
    private final ConcurrentMap<K, Meta<V>> currentCacheEntries;

    private Cache<InternalKey<K>, InternalValue<V>> cache;
    private Policy<InternalKey<K>, InternalValue<V>> policy;
    private DistributionMode distributionMode;
    private Repository<K, V> repository;
    private ExtendedPersistenceConfigurer extendedPersistenceConfigurer;
    private InternalSynchronizationLock synchronizationLock;
    private InternalHasher<K> hasher;
    private Executor executor;

    InternalCacheManager() {
        this.isActivated = new AtomicBoolean(false);
        this.secureRandom = new SecureRandom();
        this.currentCacheEntries = new ConcurrentHashMap<>();
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.cache = instanceRegistry.getCache();
        this.policy = instanceRegistry.getCache().policy();
        this.distributionMode = instanceRegistry.getDistributionMode();
        this.repository = instanceRegistry.getAdapter().getRepository();
        this.extendedPersistenceConfigurer = instanceRegistry.getExtendedPersistenceConfigurer();
        this.synchronizationLock = instanceRegistry.getSynchronizationLock();
        this.hasher = instanceRegistry.getHasher();
        this.executor = instanceRegistry.getExecutor();
    }

    void activate() {
        isActivated.set(true);
    }

    void deactivate() {
        isActivated.set(false);
        currentCacheEntries.clear();
    }

    boolean isActivated() {
        return isActivated.get();
    }

    InternalValue<V> putDistributed(InternalKey<K> key, InternalValue<V> value) {
        putAllDistributed(Map.of(key, value));
        return value;
    }

    Map<? extends InternalKey<K>, ? extends InternalValue<V>> putAllDistributed(
            Map<? extends InternalKey<K>, ? extends InternalValue<V>> map) {
        publishCacheEntries(map, CACHED, true);
        return map;
    }

    InternalValue<V> putDistributedLoaded(InternalKey<K> key, InternalValue<V> value) {
        putAllDistributedLoaded(Map.of(key, value));
        return value;
    }

    Map<? extends InternalKey<K>, ? extends InternalValue<V>> putAllDistributedLoaded(
            Map<? extends InternalKey<K>, ? extends InternalValue<V>> map) {
        publishCacheEntries(map, CACHED_LOADED, true);
        return map;
    }

    Map<? extends InternalKey<K>, ? extends InternalValue<V>> putAllDistributedRefresh(
            Map<? extends InternalKey<K>, ? extends InternalValue<V>> map) {
        publishCacheEntries(map, CACHED_REFRESHED, true);
        return map;
    }

    InternalValue<V> putDistributedRefreshAfterWrite(InternalKey<K> key, InternalValue<V> newValue,
                                                     InternalValue<V> oldValue) {
        // special handling (activated, async, old value, not managed, no cache change)
        if (isActivated()) {
            if (distributionMode.isPopulationConsidered()) {
                CompletableFuture.runAsync(() ->
                                publishCacheEntries(Map.of(key, newValue), CACHED_REFRESHED_AFTER_WRITE, false),
                        executor);
                // return old value which does not change the cache and does not trigger any listeners
                return oldValue;
            } else {
                return newValue;
            }
        } else {
            return newValue;
        }
    }

    InternalKey<K> invalidateDistributed(InternalKey<K> key) {
        invalidateAllDistributed(Set.of(key));
        return key;
    }

    Set<InternalKey<K>> invalidateAllDistributed(Set<InternalKey<K>> keys) {
        Map<InternalKey<K>, InternalValue<V>> map = new HashMap<>(); // allow null values
        keys.forEach(key -> map.put(key, null));
        publishCacheEntries(map, INVALIDATED, true);
        return keys;
    }

    Set<InternalKey<K>> invalidateAllDistributedRefresh(Set<InternalKey<K>> keys) {
        Map<InternalKey<K>, InternalValue<V>> map = new HashMap<>(); // allow null values
        keys.forEach(key -> map.put(key, null));
        publishCacheEntries(map, INVALIDATED_REFRESHED, true);
        return keys;
    }

    InternalValue<V> invalidateDistributedRefreshAfterWrite(InternalKey<K> key, InternalValue<V> oldValue) {
        // special handling (activated, async, old value, not managed, no cache change)
        if (isActivated()) {
            if (distributionMode.isInvalidationConsidered()) {
                Map<InternalKey<K>, InternalValue<V>> map = new HashMap<>(); // allow null values
                map.put(key, null);
                CompletableFuture.runAsync(() ->
                                publishCacheEntries(map, INVALIDATED_REFRESHED_AFTER_WRITE, false),
                        executor);
                // return old value which does not change the cache and does not trigger any listeners
                return oldValue;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    @SuppressWarnings("java:S3776")
    void evictDistributed(InternalKey<K> key, InternalValue<V> value, RemovalCause removalCause) {
        // special handling (activated, eviction support, async, not managed, cache change)
        if (isActivated() && (removalCause.equals(RemovalCause.SIZE) || removalCause.equals(RemovalCause.EXPIRED))) {
            Status status;
            if (extendedPersistenceConfigurer.isConfigured()) {
                status = removalCause.equals(RemovalCause.SIZE)
                        ? EVICTED_SIZE_EXTENDED
                        : EVICTED_TIME_EXTENDED;
            } else {
                status = removalCause.equals(RemovalCause.SIZE)
                        ? EVICTED_SIZE
                        : EVICTED_TIME;
            }
            CompletableFuture.runAsync(() -> {
                // special handling of cache change
                if (distributionMode.isPopulationConsidered() && !distributionMode.isEvictionConsidered()) {
                    currentCacheEntries.compute(k(key), (k, meta) ->
                            isNull(meta) || (meta.getStatus().isCached() && value == meta.getValue())
                                    ? null
                                    : meta);
                }
                publishCacheEntries(Map.of(key, value), status, false);
            }, executor);
        }
    }

    private void publishCacheEntries(Map<? extends InternalKey<K>, ? extends InternalValue<V>> map, Status status, boolean manage) {
        // extended persistence should work regardless of the distribution mode
        if (isActivated() && (status.isConsideredBy(distributionMode) || status.isEvictedExtended())) {
            if (manage) {
                synchronizationLock.ensureLock();
            }
            Set<CacheEntry<K, V>> cacheEntries = map.entrySet().stream()
                    // do not distribute invalidation if value is already absent
                    .filter(entry -> !(status.isInvalidated() && isNull(policy.getIfPresentQuietly(entry.getKey()))))
                    .map(entry -> CacheEntry.of(
                            null, // TODO discriminator
                            hasher.getHash(k(entry.getKey())),
                            manage ? secureRandom.nextInt() : null,
                            (K) k(entry.getKey()),
                            (V) v(entry.getValue()),
                            status,
                            Instant.now()))
                    .collect(toSet());
            if (!cacheEntries.isEmpty()) {
                runFailable(() -> repository.upsertCacheEntries(cacheEntries));
                if (manage) {
                    currentCacheEntries.putAll(cacheEntries.stream()
                            .map(cacheEntry -> entry(cacheEntry.getKey(), Meta.of(cacheEntry)))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
                }
            }
        }
    }

    @Override
    @SuppressWarnings("java:S3776")
    public void retrieveCacheEntries(Collection<CacheEntry<K, V>> cacheEntries) {
        if (isActivated()) {
            synchronizationLock.runLocked(() -> {
                Map<InternalKey<K>, InternalValue<V>> toAdd = new HashMap<>();
                Set<InternalKey<K>> toRemove = new HashSet<>();
                cacheEntries.stream()
                        .filter(cacheEntry -> cacheEntry.getStatus().isConsideredBy(distributionMode))
                        .forEach(cacheEntry -> {
                            InternalKey<K> key = ik(cacheEntry.getKey());
                            Meta<V> meta = currentCacheEntries.put(k(key), Meta.of(cacheEntry));
                            if (isNull(meta) || isNull(meta.getOperation())
                                    || !meta.getOperation().equals(cacheEntry.getOperation())) {
                                if (cacheEntry.isCached()) {
                                    InternalValue<V> value = iv(cacheEntry.getValue());
                                    toAdd.put(key, value);
                                } else {
                                    // only remove from cache if value is present
                                    if (nonNull(policy.getIfPresentQuietly(key))) {
                                        toRemove.add(key);
                                    }
                                }
                            }
                        });
                cache.putAll(toAdd);
                cache.invalidateAll(toRemove);
            });
        }
    }

    void synchronizeCacheEntries() {
        if (isActivated() && distributionMode.isPopulationConsidered()) {
            synchronizationLock.ensureLock();
            // TODO discriminator
            Set<CacheEntry<K, V>> cacheEntries = new HashSet<>();
            try (Stream<CacheEntry<K, V>> cacheEntryStream = getFailable(() -> repository.streamCacheEntries(
                    null,
                    null,
                    CACHED_GROUP,
                    null,
                    true))) {
                cacheEntryStream
                        // TODO
                        .filter(cacheEntry -> !currentCacheEntries.containsKey(cacheEntry.getKey()))
                        .forEach(cacheEntries::add);
            }
            retrieveCacheEntries(cacheEntries);
        }
    }

    void cleanup(Duration shortLivingDuration) {
        if (isActivated()) {
            cache.cleanUp();
            // TODO check in tests
            currentCacheEntries.values().removeIf(meta ->
                    !meta.getStatus().isCached()
                            && meta.getTimestamp().isBefore(Instant.now().minus(shortLivingDuration)));
        }
    }

    private static class Meta<V> {

        private final Status status;
        private final Integer operation;
        private final WeakReference<InternalValue<V>> value;
        private final Instant timestamp;

        private Meta(Integer operation, Status status, InternalValue<V> value) {
            this.operation = operation;
            this.status = status;
            this.value = new WeakReference<>(value);
            this.timestamp = Instant.now();
        }

        private Integer getOperation() {
            return operation;
        }

        private Status getStatus() {
            return status;
        }

        private Instant getTimestamp() {
            return timestamp;
        }

        public InternalValue<V> getValue() {
            return value.get();
        }

        private static <V> Meta<V> of(CacheEntry<?, V> cacheEntry) {
            return new Meta<>(cacheEntry.getOperation(), cacheEntry.getStatus(), iv(cacheEntry.getValue()));
        }
    }
}
