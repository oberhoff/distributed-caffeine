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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.LazyInitializer;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.INVALIDATED;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

class InternalCacheManager<K, V> implements LazyInitializer<K, V> {

    private final AtomicBoolean isActivated;
    private final Map<K, InternalCacheDocument<K, V>> latest;
    private final Map<K, InternalCacheDocument<K, V>> buffer;
    private final Map<K, List<InternalCacheDocument<K, V>>> balance;
    private final Set<ObjectId> ignore;

    private Cache<K, V> cache;
    private Policy<K, V> policy;
    private InternalSynchronizationLock synchronizationLock;
    private InternalMongoRepository<K, V> mongoRepository;
    private InternalMaintenanceWorker<K, V> maintenanceWorker;
    private String origin;

    InternalCacheManager() {
        this.isActivated = new AtomicBoolean(false);
        this.latest = new ConcurrentHashMap<>();
        this.buffer = new ConcurrentHashMap<>();
        this.balance = new ConcurrentHashMap<>();
        this.ignore = ConcurrentHashMap.newKeySet();
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.cache = distributedCaffeine.getCache();
        this.policy = distributedCaffeine.getCache().policy();
        this.synchronizationLock = distributedCaffeine.getSynchronizationLock();
        this.mongoRepository = distributedCaffeine.getMongoRepository();
        this.maintenanceWorker = distributedCaffeine.getMaintenanceWorker();
        this.origin = distributedCaffeine.getObjectIdGenerator().getOrigin();
    }

    void activate() {
        isActivated.set(true);
    }

    void deactivate() {
        isActivated.set(false);
        latest.clear();
        buffer.clear();
        balance.clear();
        ignore.clear();
    }

    boolean isActivated() {
        return isActivated.get();
    }

    void manageOutboundInsert(Map<? extends K, ? extends V> map, Status status,
                              boolean manage, boolean originConscious) {
        if (isActivated()) {
            if (manage) {
                synchronizationLock.ensure();
                commitCacheOutbound(map, status).forEach(this::addToBalance);
            } else {
                commitCacheOutbound(map, status).stream()
                        .filter(cacheDocument -> !originConscious)
                        .map(InternalCacheDocument::getId)
                        .forEach(ignore::add);
            }
        }
    }

    void manageInboundInsert(InternalCacheDocument<K, V> inboundCacheDocument, boolean manage, boolean isChangeStream) {
        if (isActivated()) {
            boolean ignored = ignore.remove(inboundCacheDocument.getId());
            if (manage) {
                synchronizationLock.ensure();
                Optional<InternalCacheDocument<K, V>> outboundCacheDocument = removeFromBalance(inboundCacheDocument);
                if (isInBalance(inboundCacheDocument.getKey())) { // in balance
                    InternalCacheDocument<K, V> latestCacheDocument = latest.get(inboundCacheDocument.getKey());
                    List<InternalCacheDocument<K, V>> sortedCacheDocuments = Stream.of(latestCacheDocument,
                                    inboundCacheDocument, buffer.remove(inboundCacheDocument.getKey()))
                            .filter(Objects::nonNull)
                            .distinct()
                            .sorted(Comparator.reverseOrder())
                            .collect(Collectors.toCollection(ArrayList::new));
                    InternalCacheDocument<K, V> newestCacheDocument = sortedCacheDocuments.remove(0);
                    if (!newestCacheDocument.equals(latestCacheDocument)) {
                        outboundCacheDocument
                                .filter(newestCacheDocument::equals)
                                // do not commit to cache if already done by outbound insert
                                .ifPresentOrElse(originalCacheDocument -> {
                                    // keep original key and value instances of outbound insert
                                    newestCacheDocument
                                            .setKey(originalCacheDocument.getKey())
                                            .setValue(originalCacheDocument.getValue());
                                }, () -> commitCacheInbound(newestCacheDocument));
                        latest.put(newestCacheDocument.getKey(), newestCacheDocument.weakened());
                    }
                    // reduce redundant replacements across cache instances
                    if (!isChangeStream || newestCacheDocument.hasOrigin(origin)) {
                        sortedCacheDocuments.forEach(this::manageReplacement);
                    }
                } else { // not in balance
                    if (inboundCacheDocument.isNewer(buffer.get(inboundCacheDocument.getKey()))) {
                        Optional.ofNullable(buffer.put(inboundCacheDocument.getKey(), inboundCacheDocument))
                                // reduce redundant replacements across cache instances
                                .filter(bufferedCacheDocument -> inboundCacheDocument.hasOrigin(origin))
                                .ifPresent(this::manageReplacement);
                    } else {
                        manageReplacement(inboundCacheDocument);
                    }
                }
            } else {
                if (ignored || !inboundCacheDocument.hasOrigin(origin)) {
                    commitCacheInbound(inboundCacheDocument);
                }
            }
        }
    }

    void manageInboundUpdate(InternalCacheDocument<K, V> inboundCacheDocument) {
        if (isActivated()) {
            synchronizationLock.ensure();
            Optional.ofNullable(latest.get(inboundCacheDocument.getKey()))
                    .filter(inboundCacheDocument::equals)
                    .filter(InternalCacheDocument::isCached)
                    .filter(latestCacheDocument -> inboundCacheDocument.isCached())
                    .filter(latestCacheDocument -> !latestCacheDocument.isNewer(inboundCacheDocument))
                    .ifPresent(latestCacheDocument -> {
                        cache.put(inboundCacheDocument.getKey(), inboundCacheDocument.getValue());
                        latest.put(inboundCacheDocument.getKey(), inboundCacheDocument.weakened());
                    });
        }
    }

    void manageInboundDelete(ObjectId objectId) {
        if (isActivated()) {
            synchronizationLock.ensure();
            InternalCacheDocument<K, V> cacheDocument = new InternalCacheDocument<K, V>().setId(objectId);
            latest.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(cacheDocument))
                    .findFirst()
                    // map and add missing data (weakened on purpose)
                    .map(entry -> entry.getValue().setKey(entry.getKey()))
                    .ifPresent(latestCacheDocument -> {
                        if (latestCacheDocument.isCached()) {
                            cache.invalidate(latestCacheDocument.getKey());
                        }
                        latest.remove(latestCacheDocument.getKey(), latestCacheDocument);
                        // speed up removeFromBalance()
                        cacheDocument.setKey(latestCacheDocument.getKey());
                    });
            buffer.entrySet().stream()
                    .filter(entry -> entry.getValue()
                            .getId().equals(objectId))
                    .findFirst()
                    .map(Entry::getValue)
                    .ifPresent(bufferedCacheDocument -> {
                        buffer.remove(bufferedCacheDocument.getKey(), bufferedCacheDocument);
                        // speed up removeFromBalance()
                        cacheDocument.setKey(bufferedCacheDocument.getKey());
                    });
            removeFromBalance(cacheDocument);
            ignore.remove(objectId);
        }
    }

    void manageSynchronization() {
        if (isActivated()) {
            synchronizationLock.ensure();
            Set<K> keys = latest.entrySet().stream()
                    .filter(entry -> entry.getValue().isCached()
                            || entry.getValue().isOrphaned())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
            cache.asMap().keySet().removeIf(key -> !keys.contains(key));
        }
    }

    void manageReplacement(InternalCacheDocument<K, V> cacheDocument) {
        if (isActivated()) {
            // no lock required
            if (cacheDocument.getStatus() == CACHED) {
                maintenanceWorker.queueToBeOrphaned(cacheDocument.getId());
            }
        }
    }

    void manageReplacement(K key, V value, RemovalCause removalCause) {
        if (isActivated()) {
            boolean locked = synchronizationLock.tryLock();
            try {
                // distribution of reference-based evictions is not supported (yet)
                if (removalCause != RemovalCause.COLLECTED) {
                    Stream.concat(Optional.ofNullable(balance.get(key)).stream().flatMap(Collection::stream),
                                    Stream.of(buffer.get(key), latest.get(key)))
                            .filter(Objects::nonNull)
                            .filter(cacheDocument -> cacheDocument.getValue() == value)
                            .findFirst()
                            .ifPresent(this::manageReplacement);
                }
            } finally {
                synchronizationLock.tryUnlock(locked);
            }
        }
    }

    void manageInboundFailure(ObjectId objectId) {
        if (isActivated()) {
            boolean locked = synchronizationLock.tryLock();
            try {
                InternalCacheDocument<K, V> cacheDocument = new InternalCacheDocument<K, V>().setId(objectId);
                if (cacheDocument.hasOrigin(origin)) {
                    removeFromBalance(cacheDocument);
                }
            } finally {
                synchronizationLock.tryUnlock(locked);
            }
        }
    }

    private Set<InternalCacheDocument<K, V>> commitCacheOutbound(Map<? extends K, ? extends V> map, Status status) {
        Map<? extends K, ? extends V> filteredMap = map.entrySet().stream()
                // do not distribute invalidation if value is already absent
                .filter(entry -> status != INVALIDATED || nonNull(policy.getIfPresentQuietly(entry.getKey())))
                .collect(HashMap::new, (hashMap, entry) -> // allows null values
                        hashMap.put(entry.getKey(), entry.getValue()), HashMap::putAll);
        return mongoRepository.insert(filteredMap, status);
    }

    private void commitCacheInbound(InternalCacheDocument<K, V> cacheDocument) {
        if (cacheDocument.isCached() || cacheDocument.isOrphaned()) {
            cache.put(cacheDocument.getKey(), cacheDocument.getValue());
        } else if ((cacheDocument.isInvalidated() || cacheDocument.isEvicted() || cacheDocument.isExtended())
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
                return new ArrayList<>(List.of(cacheDocument));
            } else {
                value.add(cacheDocument);
                return value;
            }
        });
    }

    private Optional<InternalCacheDocument<K, V>> removeFromBalance(InternalCacheDocument<K, V> cacheDocument) {
        List<InternalCacheDocument<K, V>> cacheDocuments = nonNull(cacheDocument.getKey())
                ? balance.getOrDefault(cacheDocument.getKey(), List.of())
                : balance.values().stream()
                .filter(value -> value.contains(cacheDocument))
                .findFirst()
                .orElseGet(List::of);
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
