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
import org.bson.types.ObjectId;

import java.time.Duration;
import java.util.ArrayList;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED_LOADED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_SIZE;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_SIZE_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_TIME;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_TIME_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.INVALIDATED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.INVALIDATED_REFRESHED_AFTER_WRITE;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("java:S1452")
class InternalCacheManager<K, V> implements InternalLazyInitializer<K, V> {

    private final AtomicBoolean isActivated;
    private final Map<K, InternalCacheDocument<K, V>> latest;
    private final Map<K, InternalCacheDocument<K, V>> buffer;
    private final Map<K, Set<InternalCacheDocument<K, V>>> balance;

    private Cache<K, V> cache;
    private Policy<K, V> policy;
    private DistributionMode distributionMode;
    private ExtendedPersistenceConfigurer extendedPersistenceConfigurer;
    private InternalMongoRepository<K, V> mongoRepository;
    private InternalMaintenanceWorker<K, V> maintenanceWorker;
    private InternalSynchronizationLock synchronizationLock;
    private Executor executor;
    private Long origin;
    private boolean hasEvictionPolicyByTime;

    InternalCacheManager() {
        this.isActivated = new AtomicBoolean(false);
        this.latest = new ConcurrentHashMap<>();
        this.buffer = new ConcurrentHashMap<>();
        this.balance = new ConcurrentHashMap<>();
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.cache = distributedCaffeine.getCache();
        this.policy = distributedCaffeine.getCache().policy();
        this.distributionMode = distributedCaffeine.getDistributionMode();
        this.extendedPersistenceConfigurer = distributedCaffeine.getExtendedPersistenceConfigurer();
        this.mongoRepository = distributedCaffeine.getMongoRepository();
        this.maintenanceWorker = distributedCaffeine.getMaintenanceWorker();
        this.synchronizationLock = distributedCaffeine.getSynchronizationLock();
        this.executor = distributedCaffeine.getExecutor();
        this.origin = distributedCaffeine.getOrigin();
        this.hasEvictionPolicyByTime = Stream
                .of(policy.expireAfterAccess(), policy.expireAfterWrite(), policy.expireVariably())
                .anyMatch(Optional::isPresent);
    }

    void activate() {
        isActivated.set(true);
    }

    void deactivate() {
        isActivated.set(false);
        latest.clear();
        buffer.clear();
        balance.clear();
    }

    boolean isActivated() {
        return isActivated.get();
    }

    V putDistributed(K key, V value) {
        putAllDistributed(Map.of(key, value));
        return value;
    }

    Map<? extends K, ? extends V> putAllDistributed(Map<? extends K, ? extends V> map) {
        manageOutboundInsert(map, CACHED, true);
        return map;
    }

    V putDistributedLoaded(K key, V value) {
        putAllDistributedLoaded(Map.of(key, value));
        return value;
    }

    Map<? extends K, ? extends V> putAllDistributedLoaded(Map<? extends K, ? extends V> map) {
        manageOutboundInsert(map, CACHED_LOADED, true);
        return map;
    }

    Map<? extends K, ? extends V> putAllDistributedRefresh(Map<? extends K, ? extends V> map) {
        manageOutboundInsert(map, CACHED_REFRESHED, true);
        return map;
    }

    V putDistributedRefreshAfterWrite(K key, V newValue, V oldValue) {
        // special handling (activated, old value, origin independence)
        if (isActivated()) {
            if (distributionMode.isPopulationConsidered()) {
                manageOutboundInsert(Map.of(key, newValue), CACHED_REFRESHED_AFTER_WRITE, false);
                // return old value which does not change the cache and does not trigger the removal listeners
                return oldValue;
            } else {
                return newValue;
            }
        } else {
            return newValue;
        }
    }

    K invalidateDistributed(K key) {
        invalidateAllDistributed(Set.of(key));
        return key;
    }

    Set<K> invalidateAllDistributed(Set<K> keys) {
        Map<K, V> map = new HashMap<>(); // allow null values
        keys.forEach(key -> map.put(key, null));
        manageOutboundInsert(map, INVALIDATED, true);
        return keys;
    }

    Set<K> invalidateAllDistributedRefresh(Set<K> keys) {
        Map<K, V> map = new HashMap<>(); // allow null values
        keys.forEach(key -> map.put(key, null));
        manageOutboundInsert(map, INVALIDATED_REFRESHED, true);
        return keys;
    }

    V invalidateDistributedRefreshAfterWrite(K key, V oldValue) {
        // special handling (activated, old value, origin independence)
        if (isActivated()) {
            if (distributionMode.isInvalidationConsidered()) {
                Map<K, V> map = new HashMap<>(); // allow null values
                map.put(key, null);
                manageOutboundInsert(map, INVALIDATED_REFRESHED_AFTER_WRITE, false);
                // return old value which does not change the cache and does not trigger the removal listener
                return oldValue;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    @SuppressWarnings("java:S3776")
    void evictDistributed(K key, V value, RemovalCause removalCause) {
        // special handling (activated, eviction support, async, conditional replacement, origin independence)
        if (isActivated() && (removalCause.equals(RemovalCause.SIZE) || removalCause.equals(RemovalCause.EXPIRED))) {
            // run asynchronous
            CompletableFuture.runAsync(() -> {
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
                // manage replacement if populations are distributed, but evictions are not distributed
                if (distributionMode.isPopulationConsidered() && !distributionMode.isEvictionConsidered()) {
                    Optional.ofNullable(latest.get(key))
                            .filter(cacheDocument -> cacheDocument.getValue() == value)
                            .ifPresent(cacheDocument -> {
                                latest.values().remove(cacheDocument);
                                maintenanceWorker.queueReplacement(cacheDocument);
                            });
                }
                manageOutboundInsert(Map.of(key, value), status, false);
            }, executor);
        }
    }

    void synchronizeCacheFromStore() {
        if (distributionMode.isPopulationConsidered()) {
            mongoRepository.consumeCacheDocumentsGroupedByKeyInReverseOrder(null, stream ->
                    stream.forEach(cacheDocuments -> {
                        // newest cache entry must be treated in the same way as a distributed inbound insert
                        cacheDocuments.stream()
                                .findFirst()
                                .filter(InternalCacheDocument::isCached)
                                .ifPresent(cacheDocument ->
                                        manageInboundInsert(cacheDocument, false));
                        // correct any inconsistencies (if any) in relation to (not yet) stale cache entries
                        cacheDocuments.stream()
                                .skip(1)
                                .forEach(maintenanceWorker::queueReplacement);
                    }));
            // remove cache entries which are not managed (e.g., if synchronization is started after it was stopped)
            synchronizationLock.runLocked(() -> {
                if (isActivated()) {
                    Set<K> keys = latest.entrySet().stream()
                            .filter(entry -> entry.getValue().isCached())
                            .map(Entry::getKey)
                            .collect(toSet());
                    cache.asMap().keySet().removeIf(key -> !keys.contains(key));
                }
            });
        }
    }

    void manageInboundFailure(ObjectId objectId) {
        if (isActivated() && distributionMode.isPopulationConsidered()) {
            InternalCacheDocument<K, V> cacheDocument = new InternalCacheDocument<K, V>().setId(objectId);
            removeFromBalance(cacheDocument);
        }
    }

    void manageCleanUp(Duration outdatedDuration) {
        if (isActivated()) {
            // remove if not cached but outdated
            Predicate<InternalCacheDocument<K, V>> outdated = cacheDocument ->
                    !cacheDocument.isCached() && cacheDocument.isOlderThan(outdatedDuration);
            latest.values().removeIf(outdated);
            // trigger pending time-based eviction explicitly (to be more prompt)
            if (distributionMode.isEvictionConsidered() && hasEvictionPolicyByTime) {
                cache.cleanUp();
                // workaround as cleanUp() does not seem to trigger all pending time-based evictions reliably
                synchronizationLock.runLocked(() -> latest.values().stream()
                        .filter(InternalCacheDocument::isCached)
                        // preserve weakened key while streaming
                        .map(cacheDocument -> cacheDocument.setKey(cacheDocument.getKey()))
                        .filter(cacheDocument -> nonNull(cacheDocument.getKey()))
                        .filter(cacheDocument -> isNull(policy.getIfPresentQuietly(cacheDocument.getKey())))
                        .forEach(cacheDocument -> {
                            // this should trigger a pending eviction
                            cache.invalidate(cacheDocument.getKey());
                            // weaken the key again
                            cacheDocument.weakened();
                        }));
            }
        }
    }

    @SuppressWarnings("java:S3776")
    void manageInboundInsert(InternalCacheDocument<K, V> inboundCacheDocument, boolean eventBased) {
        if (isActivated() && inboundCacheDocument.getStatus().isConsideredBy(distributionMode)) {
            boolean manage = distributionMode.isPopulationConsidered();
            if (manage) {
                synchronizationLock.runLocked(() -> {
                    Optional<InternalCacheDocument<K, V>> outboundCacheDocument = removeFromBalance(inboundCacheDocument);
                    if (isInBalance(inboundCacheDocument.getKey())) { // in balance
                        InternalCacheDocument<K, V> latestCacheDocument = latest.get(inboundCacheDocument.getKey());
                        List<InternalCacheDocument<K, V>> sortedCacheDocuments = Stream.of(latestCacheDocument,
                                        inboundCacheDocument, buffer.remove(inboundCacheDocument.getKey()))
                                .filter(Objects::nonNull)
                                .distinct()
                                .sorted(Comparator.reverseOrder())
                                .collect(toCollection(ArrayList::new));
                        InternalCacheDocument<K, V> newestCacheDocument = sortedCacheDocuments.remove(0);
                        if (!newestCacheDocument.equals(latestCacheDocument)) {
                            outboundCacheDocument
                                    .filter(newestCacheDocument::equals)
                                    // do not commit to cache if already done by outbound insert
                                    .ifPresentOrElse(originalCacheDocument -> newestCacheDocument
                                                    // keep original key and value instances of outbound insert
                                                    .setKey(originalCacheDocument.getKey())
                                                    .setValue(originalCacheDocument.getValue()),
                                            () -> commitCacheInbound(newestCacheDocument));
                            latest.put(newestCacheDocument.getKey(), newestCacheDocument.weakened());
                        }
                        // reduce redundant replacements across cache instances
                        if (!eventBased || newestCacheDocument.hasSameOrigin(origin)) {
                            sortedCacheDocuments.forEach(maintenanceWorker::queueReplacement);
                        }
                    } else { // not in balance
                        if (inboundCacheDocument.isNewer(buffer.get(inboundCacheDocument.getKey()))) {
                            Optional.ofNullable(buffer.put(inboundCacheDocument.getKey(), inboundCacheDocument))
                                    // reduce redundant replacements across cache instances
                                    .filter(bufferedCacheDocument -> inboundCacheDocument.hasSameOrigin(origin))
                                    .ifPresent(maintenanceWorker::queueReplacement);
                        } else {
                            // reduce redundant replacements across cache instances
                            if (inboundCacheDocument.hasSameOrigin(origin)) {
                                maintenanceWorker.queueReplacement(inboundCacheDocument);
                            }
                        }
                    }
                });
            } else {
                if (!inboundCacheDocument.hasSameOrigin(origin) || inboundCacheDocument.isOriginIndependent()) {
                    commitCacheInbound(inboundCacheDocument);
                }
            }
        }
    }

    private void manageOutboundInsert(Map<? extends K, ? extends V> map, Status status, boolean manage) {
        // extended persistence should work regardless of the distribution mode
        if (isActivated() && (status.isConsideredBy(distributionMode) || status.isEvictedExtended())) {
            // only manage if distributed population is supported
            manage = manage && distributionMode.isPopulationConsidered();
            if (manage) {
                synchronizationLock.ensureLock();
                commitCacheOutbound(map, status).forEach(this::addToBalance);
            } else {
                commitCacheOutbound(map, status);
            }
        }
    }

    private Set<InternalCacheDocument<K, V>> commitCacheOutbound(Map<? extends K, ? extends V> map, Status status) {
        Map<? extends K, ? extends V> filteredMap = map.entrySet().stream()
                // do not distribute invalidation if value is already absent
                .filter(entry -> !(status.isInvalidated() && isNull(policy.getIfPresentQuietly(entry.getKey()))))
                .collect(HashMap::new, (hashMap, entry) -> // allow null values
                        hashMap.put(entry.getKey(), entry.getValue()), HashMap::putAll);
        Set<InternalCacheDocument<K, V>> outboundCacheDocuments = mongoRepository.insert(filteredMap, status);
        maintenanceWorker.queueActivity(outboundCacheDocuments);
        return outboundCacheDocuments;
    }

    private void commitCacheInbound(InternalCacheDocument<K, V> cacheDocument) {
        if (cacheDocument.isCached()) {
            cache.put(cacheDocument.getKey(), cacheDocument.getValue());
        } else if ((cacheDocument.isInvalidated() || cacheDocument.isEvicted())
                // only invalidate cache if value is present
                && nonNull(policy.getIfPresentQuietly(cacheDocument.getKey()))) {
            cache.invalidate(cacheDocument.getKey());
        }
    }

    private boolean isInBalance(K key) {
        return !balance.containsKey(key);
    }

    private void addToBalance(InternalCacheDocument<K, V> cacheDocument) {
        balance.compute(cacheDocument.getKey(), (key, value) -> {
            if (isNull(value)) {
                return new HashSet<>(Set.of(cacheDocument));
            } else {
                value.add(cacheDocument);
                return value;
            }
        });
    }

    private Optional<InternalCacheDocument<K, V>> removeFromBalance(InternalCacheDocument<K, V> cacheDocument) {
        Set<InternalCacheDocument<K, V>> cacheDocuments = nonNull(cacheDocument.getKey())
                ? balance.getOrDefault(cacheDocument.getKey(), Set.of())
                : balance.values().stream()
                .filter(value -> value.contains(cacheDocument))
                .findFirst()
                .orElseGet(Set::of);
        Optional<InternalCacheDocument<K, V>> optionalCacheDocument = cacheDocuments.stream()
                .filter(cacheDocument::equals)
                .findFirst();
        optionalCacheDocument.ifPresent(presentCacheDocument -> {
            cacheDocuments.remove(presentCacheDocument);
            if (cacheDocuments.isEmpty()) {
                balance.remove(presentCacheDocument.getKey());
            }
        });
        return optionalCacheDocument;
    }
}
