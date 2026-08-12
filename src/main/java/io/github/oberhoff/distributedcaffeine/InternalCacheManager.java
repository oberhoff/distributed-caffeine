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
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalKey.ik;
import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.runFailable;
import static io.github.oberhoff.distributedcaffeine.InternalValue.iv;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Command.INVALIDATE_ALL;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_LOADED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.COMMAND;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_EXTENDED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED_AFTER_WRITE;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("java:S1452")
class InternalCacheManager<K, V> implements InternalInitializable<K, V>, Retriever<K, V> {

    private final AtomicBoolean isActivated;
    // an operation is this identifier followed by a counter, which makes it identify a single write (as the random
    // value it replaces did) and additionally order that write against the other ones issued here. The identifier is
    // renewed with every activation on purpose: ordering is only meaningful within one of them, because it compares
    // a local mutation against an event this very activation published. Renewing it lets a stamp left behind by an
    // earlier activation simply not match, instead of being compared against a counter unrelated to it
    private final AtomicReference<String> operationId;
    private final AtomicLong operationCounter;

    private Logger logger;
    private String identifier;
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
        this.operationId = new AtomicReference<>("");
        this.operationCounter = new AtomicLong();
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.logger = instanceRegistry.getLogger();
        this.identifier = instanceRegistry.getAdapter().getIdentifier();
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
        operationId.set(Long.toHexString(ThreadLocalRandom.current().nextLong()));
        isActivated.set(true);
    }

    void deactivate() {
        isActivated.set(false);
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
                publishCacheEntriesAsync(Map.of(key, newValue), CACHED_REFRESHED_AFTER_WRITE);
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

    // Invalidating all cache entries cannot be expressed as a set of keys: the calling cache instance can only
    // enumerate what it holds itself, and without population being distributed nothing else knows what the others
    // hold. So instead of one cache entry per key, a command is written, and every cache instance decides from its own
    // content what it removes when that arrives (see retrieveCacheEntries). Where population is distributed the store
    // keeps a record of what is cached, which has to go as well - otherwise a reactivation reads it back and extended
    // persistence reloads from it, undoing what was just invalidated
    void invalidateAllDistributed() {
        if (isActivated() && COMMAND.isConsideredBy(distributionMode)) {
            synchronizationLock.ensureLock();
            if (distributionMode.isPopulationConsidered()) {
                Set<Status> statuses = new HashSet<>(CACHED_GROUP);
                if (extendedPersistenceConfigurer.isConfigured()) {
                    statuses.addAll(EVICTED_EXTENDED_GROUP);
                }
                // transitions what is there and writes nothing for what is not, which also means it cannot resurrect
                // a key as invalidated that no longer exists. Ahead of the cache entry below, so that a population
                // following this operation cannot be overwritten by it afterwards
                runFailable(() -> repository.updateStatusOfCacheEntries(null, statuses, null, INVALIDATED));
            }
            runFailable(() -> repository.upsertCacheEntries(List.of(CacheEntry.of(
                    INVALIDATE_ALL.toString(),
                    // stamped like any other operation of this cache instance, which is what keeps whatever it does
                    // after this from being undone once this arrives back here
                    nextOperation(),
                    null,
                    null,
                    COMMAND,
                    Instant.now()))));
        }
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
                publishCacheEntriesAsync(map, INVALIDATED_REFRESHED_AFTER_WRITE);
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
            // of the three asynchronous publishers this is the one that cannot be made good later: the entry is
            // already gone from the cache, and nothing reads it again to notice and retry. A lost eviction leaves
            // the other instances serving what this one dropped and, with extended persistence configured, leaves
            // the entry CACHED in the store instead of evicted - so it is never pruned and comes back on the next
            // restart. See the TODO on publishCacheEntriesAsync
            publishCacheEntriesAsync(Map.of(key, value), status);
        }
    }

    // the three callers below publish outside the synchronization lock because they run where taking it would
    // deadlock with Caffeine's internal lock. Whatever the returned future carries is therefore the only trace a
    // failure leaves, and dropping it hides a store that is refusing writes: the distribution is simply lost, while
    // locally everything looks like it succeeded
    // TODO logging makes such a failure visible but does not make the instances converge again. Retrying is not
    // enough on its own, because upsertCacheEntries() writes the status unconditionally, so a delayed retry can
    // overwrite a newer CACHED write for the same key with a stale EVICTED one. Letting the data store drive the
    // correction (as invalidate-on-prune does for extended persistence) is the more promising direction
    private void stampOperations(Map<? extends InternalKey<K>, ? extends InternalValue<V>> map) {
        map.values().stream()
                .filter(Objects::nonNull)
                .forEach(value -> value.setOperation(nextOperation()));
    }

    private String nextOperation() {
        return operationId.get() + ":" + operationCounter.incrementAndGet();
    }

    // An operation identifies a single write (self-echo filter) and, through the identifier it begins with, orders
    // that write against the other ones this activation issued. Where it comes from differs by what is published:
    // a populated or evicted cache entry takes the one its value was stamped with before publishing was considered
    // at all, because that value stays in this cache either way and the stamp is what protects it. An invalidated
    // one has no value to have been stamped and only needs an operation on the cache entry written for it.
    private String operationOf(@Nullable InternalValue<V> value) {
        return nonNull(value) ? value.getOperation() : nextOperation();
    }

    // whether the cache entry held here was written by this cache instance after it published the operation the
    // arriving one carries. Only comparable within one activation, so a stamp of an earlier one never matches and
    // the arriving cache entry is applied as it would have been before
    private boolean isSupersededLocally(@Nullable String arriving, @Nullable String held) {
        if (isNull(arriving) || isNull(held)) {
            return false;
        }
        String prefix = operationId.get() + ":";
        if (!arriving.startsWith(prefix) || !held.startsWith(prefix)) {
            return false;
        }
        return Long.parseLong(held.substring(prefix.length()))
                > Long.parseLong(arriving.substring(prefix.length()));
    }

    private void publishCacheEntriesAsync(Map<? extends InternalKey<K>, ? extends InternalValue<V>> map,
                                          Status status) {
        // Deliberately without an operation, so that what is published here arrives like a change of any other cache
        // instance. Carrying one would order it against the later operations of this cache instance and let this one
        // skip it - which for an eviction means keeping a cache entry the store no longer has as cached, because the
        // eviction is written whenever the executor gets around to it and can land after the population following
        // it. Every other cache instance then drops the cache entry while this one keeps it, and nothing reads the
        // store again to notice. Losing the population everywhere is wrong too, but at least it is not a divergence
        // between the instances - see the corresponding disabled test
        CompletableFuture.runAsync(() -> publishCacheEntries(map, status, false), executor)
                .exceptionally(throwable -> {
                    logger.log(Level.WARNING, format("Distributing %s for %s failed for cache at '%s'",
                            status, map.keySet(), identifier), throwable);
                    return null;
                });
    }

    private void publishCacheEntries(Map<? extends InternalKey<K>, ? extends InternalValue<V>> map, Status status,
                                     boolean manage) {
        // Stamped ahead of everything below, because whether a cache entry is published says nothing about whether
        // the value handed over here stays in this cache: without population being distributed nothing is published
        // for a population at all, and that is exactly where a removal by this very cache instance must not be
        // allowed to undo it. Only for the managed operations though, which are the ones taking place right here -
        // the asynchronous ones are stamped by publishCacheEntriesAsync when they take place, not when they are
        // finally published
        if (manage) {
            stampOperations(map);
        }
        // extended persistence should work regardless of the distribution mode
        if (isActivated() && (status.isConsideredBy(distributionMode) || status.isEvictedExtended())) {
            if (manage) {
                synchronizationLock.ensureLock();
            }
            // deliberately without a check of whether this cache instance holds the key: whether it does says
            // nothing about the other ones, which may well be serving it, so skipping the write here would leave
            // them doing so indefinitely. Filtering by what is held belongs on the receiving side (see
            // retrieveCacheEntries), where it is a fact rather than a guess
            List<CacheEntry<K, V>> cacheEntries = map.entrySet().stream()
                    .map(entry -> {
                        InternalValue<V> value = entry.getValue();
                        return CacheEntry.of(
                                // memoizing overload: reuses the hash cached on the key instance (e.g. stamped when
                                // the entry was put/loaded/retrieved) instead of recomputing it under the lock
                                hasher.getHash(entry.getKey()),
                                manage ? operationOf(value) : null,
                                k(entry.getKey()),
                                v(value),
                                status,
                                Instant.now());
                    })
                    // toList (not a set): entries are unique per key, so no dedup is needed and this avoids
                    // computing CacheEntry hashCode/equals on the write path
                    .toList();
            if (!cacheEntries.isEmpty()) {
                runFailable(() -> repository.upsertCacheEntries(cacheEntries));
            }
        }
    }

    @Override
    public void retrieveCacheEntries(Collection<CacheEntry<K, V>> cacheEntries) {
        // no filtering by discriminator here: a retriever belongs to exactly one cache, and the adapter handing over
        // these cache entries is scoped to that cache's discriminator - so whatever arrives is already its own
        retrieveCacheEntries(cacheEntries.stream());
    }

    @SuppressWarnings("java:S3776")
    private void retrieveCacheEntries(Stream<CacheEntry<K, V>> cacheEntries) {
        if (isActivated()) {
            synchronizationLock.runLocked(() -> {
                Map<InternalKey<K>, InternalValue<V>> toAdd = new HashMap<>();
                Set<InternalKey<K>> toRemove = new HashSet<>();
                cacheEntries
                        .filter(cacheEntry -> cacheEntry.getStatus().isConsideredBy(distributionMode))
                        .forEach(cacheEntry -> {
                            // A command belongs to no key, so it is handled ahead of everything below, which all works
                            // with one. Dispatched by the name it carries as its hash, and one that is not known here
                            // is skipped rather than treated as a cache entry - which is what allows a command to be
                            // added without every cache instance already understanding it
                            if (cacheEntry.isCommand()) {
                                if (INVALIDATE_ALL.toString().equals(cacheEntry.getHash())) {
                                    // What it removes is not something the cache instance publishing it could know, so
                                    // it is decided here, from what this one holds at the moment it arrives. The
                                    // ordering guard is the same as for a single key, only applied to each of them:
                                    // what this cache instance has written since publishing it stays, everything else
                                    // goes. A command of another one carries an operation of its own, so nothing is
                                    // superseded by it and the cache is emptied entirely, which is what following it
                                    // means here
                                    cache.asMap().forEach((presentKey, present) -> {
                                        if (!isSupersededLocally(cacheEntry.getOperation(), present.getOperation())) {
                                            toRemove.add(presentKey);
                                        }
                                    });
                                }
                                return;
                            }
                            // propagate the store's hash onto the key so it is never recomputed for this entry
                            // (e.g. when it is later evicted or re-published from this instance)
                            InternalKey<K> key = ik(requireNonNull(cacheEntry.getKey()))
                                    .setHash(cacheEntry.getHash());
                            if (cacheEntry.isCached()) {
                                InternalValue<V> present = policy.getIfPresentQuietly(key);
                                String operation = cacheEntry.getOperation();
                                // Left as it is when the arriving cache entry is the echo of the very write behind
                                // the one held here (self-echo filter), and when this cache instance has written
                                // this key again since publishing it - the same ordering guard as below, which here
                                // keeps an insertion delivered as it once was from reinstating a value already
                                // replaced by a later action of this very cache instance.
                                // The store still backs the entry either way, so it has to survive a stale sweep
                                // even though it is not written again. Clearing the mark here rather than only via
                                // toAdd matters because an entry that is in sync when synchronization stops always
                                // takes this branch: both publishing and retrieving stamp the local value with the
                                // very operation held in the store
                                if (nonNull(present)
                                        && (isSupersededLocally(operation, present.getOperation())
                                        || (nonNull(operation) && operation.equals(present.getOperation())))) {
                                    present.setStale(false);
                                } else {
                                    toAdd.put(key, iv(cacheEntry.getValue()).setOperation(operation));
                                }
                            } else {
                                // only remove from cache if value is present - and only if it was not written here
                                // after the arriving cache entry was published, which would mean undoing a later
                                // action of this very cache instance with an earlier one of its own
                                InternalValue<V> present = policy.getIfPresentQuietly(key);
                                if (nonNull(present)
                                        && !isSupersededLocally(cacheEntry.getOperation(), present.getOperation())) {
                                    toRemove.add(key);
                                }
                            }
                        });
                cache.putAll(toAdd);
                cache.invalidateAll(toRemove);
            });
        }
    }

    // while synchronization was stopped the cache kept serving locally, so local writes never reached the data store
    // and changes made elsewhere never arrived. Retrieving below only ever adds what the store holds, which would
    // leave entries the store no longer backs in place to be served as if they were still valid. Every entry present
    // up front is therefore marked as stale and anything the store still knows clears that mark again, so that only
    // what is left marked has to be removed afterwards. Marking happens in place, which keeps the cache readable
    // throughout instead of replacing it with an empty one that answers every read with a miss
    void synchronizeCacheEntries() {
        if (isActivated()) {
            synchronizationLock.ensureLock();
            cache.asMap().values()
                    .forEach(value -> value.setStale(true));
            if (distributionMode.isPopulationConsidered()) {
                // process the store cursor directly instead of buffering it into a set first (avoids a second full
                // copy in memory and the needless CacheEntry hashCode/equals a set would compute)
                try (Stream<CacheEntry<K, V>> cacheEntryStream = getFailable(() -> repository.streamCacheEntries(
                        null,
                        CACHED_GROUP,
                        null,
                        true))) {
                    retrieveCacheEntries(cacheEntryStream);
                }
            }
            // without population being considered nothing clears the marks, so everything present is dropped - the
            // same outcome as before, where a restart always continued with an empty cache
            cache.asMap().values()
                    .removeIf(InternalValue::isStale);
        }
    }

    void cleanup() {
        if (isActivated()) {
            cache.cleanUp();
        }
    }
}
