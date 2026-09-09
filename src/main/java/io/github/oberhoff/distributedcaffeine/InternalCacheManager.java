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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.CachedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.Publisher;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.adapter.Receiver;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Instant;
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
import static io.github.oberhoff.distributedcaffeine.InternalMaintenanceWorker.DISTRIBUTION_DURATION;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireRepository;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.runFailable;
import static io.github.oberhoff.distributedcaffeine.InternalValue.iv;
import static io.github.oberhoff.distributedcaffeine.InternalValue.vn;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Command.INVALIDATE_ALL;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_LOADED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.COMMAND;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_RETAINED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE_RETAINED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME_RETAINED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED_AFTER_WRITE;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

@SuppressWarnings({"java:S1452"})
class InternalCacheManager<K, V> implements InternalInitializable<K, V>, Receiver<K, V> {

    // a safety bound on the operations kept below, for a cache instance writing faster than the records expire.
    // Dropping the oldest of them is safe, see the field
    private static final int OPERATIONS_MAXIMUM_SIZE = 100_000;

    private final AtomicBoolean isActivated;
    // the identifier of the activation this cache instance is in, renewed with every one of them. It says which
    // activation a value is content of (see InternalValue), and an operation is it followed by a counter, which
    // makes that operation recognisable as one this very activation published and tells it apart from what any
    // other cache instance publishes. Renewing it is what makes both work: an operation of an earlier activation is
    // none of the publishes tracked below any more, so it simply does not match instead of being compared against a
    // counter unrelated to it - and everything held from before stops being of the current activation without a
    // single value having to be touched
    private final AtomicReference<@Nullable String> activationId;
    private final AtomicLong operationCounter;
    // The counter of the last operation this cache instance carried out for a key, whether or not anything was
    // published for it. Remembering it here rather than on the value is the whole point: a value is a lossy record
    // of what happened to a key - invalidating removes it, an eviction removes it, a cache entry of another cache
    // instance overwrites it - and every one of those leaves this cache instance unable to tell whether one of its
    // own cache entries coming back has been overtaken since.
    // Kept for as long as such a cache entry can still be delivered, and bounded on top of that, because losing a
    // record is not unsafe - it only leaves the key as unguarded as it was before
    private final Cache<InternalKey<K>, Long> operations;
    // the same for the command written by invalidating all: it belongs to no key, so what it says covers every one
    // of them and a single counter is all it needs
    private final AtomicLong commandOperation;

    @SuppressWarnings("NotNullFieldNotInitialized")
    private Logger logger;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private String identifier;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Cache<InternalKey<K>, InternalValue<V>> cache;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Policy<InternalKey<K>, InternalValue<V>> policy;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private DistributionMode distributionMode;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Publisher<K, V> publisher;
    private @Nullable Repository<K, V> repository;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private CachedEntryPersistenceConfigurer cachedEntryPersistenceConfigurer;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private EvictedEntryPersistenceConfigurer evictedEntryPersistenceConfigurer;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalSynchronizationLock synchronizationLock;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalHasher<K> hasher;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Executor executor;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalCacheManager() {
        this.isActivated = new AtomicBoolean();
        this.activationId = new AtomicReference<>();
        this.operationCounter = new AtomicLong();
        this.operations = Caffeine.newBuilder()
                .expireAfterWrite(DISTRIBUTION_DURATION)
                .maximumSize(OPERATIONS_MAXIMUM_SIZE)
                .build();
        this.commandOperation = new AtomicLong();
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.logger = instanceRegistry.getLogger();
        this.identifier = instanceRegistry.getAdapter().getIdentifier();
        this.cache = instanceRegistry.getCache();
        this.policy = instanceRegistry.getCache().policy();
        this.distributionMode = instanceRegistry.getDistributionMode();
        this.publisher = instanceRegistry.getAdapter().getPublisher();
        this.repository = instanceRegistry.getAdapter().getRepository().orElse(null);
        this.cachedEntryPersistenceConfigurer = instanceRegistry.getCachedEntryPersistenceConfigurer();
        this.evictedEntryPersistenceConfigurer = instanceRegistry.getEvictedEntryPersistenceConfigurer();
        this.synchronizationLock = instanceRegistry.getSynchronizationLock();
        this.hasher = instanceRegistry.getHasher();
        this.executor = instanceRegistry.getExecutor();
    }

    void activate() {
        // Renewing the identifier is what invalidates everything held from before, without touching a single value:
        // none of it is of this activation any more, so nothing published from here on can rest on it and
        // synchronizing keeps only what the data store confirms. Which also reaches what no pass over the cache
        // could - a value already evicted, whose eviction is reported asynchronously and would otherwise be
        // distributed as if it had taken place after starting again
        activationId.set(Long.toHexString(ThreadLocalRandom.current().nextLong()));
        // nothing published before this activation is one of its publishes, so none of it may hold an arriving
        // cache entry back any more
        operations.invalidateAll();
        commandOperation.set(0);
        isActivated.set(true);
    }

    void deactivate() {
        isActivated.set(false);
    }

    // whether this value is content of the current activation, meaning this cache instance wrote or received it
    // while taking part in synchronization. Only such a value may have a change to it distributed, and only such a
    // value survives synchronizing
    boolean hasCurrentActivationId(InternalValue<V> value) {
        String currentActivationId = activationId.get();
        return nonNull(currentActivationId) && currentActivationId.equals(value.getActivationId());
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
        // handling activationId in loader
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
        Map<InternalKey<K>, @Nullable InternalValue<V>> map = new HashMap<>(); // allow null values
        keys.forEach(key -> map.put(key, null));
        publishCacheEntries(map, INVALIDATED, true);
        return keys;
    }

    // Invalidating all cache entries cannot be expressed as a set of keys: the calling cache instance can only
    // enumerate what it holds itself, and without population being distributed nothing else knows what the others
    // hold. So instead of one cache entry per key, a command is written, and every cache instance decides from its own
    // content what it removes when that arrives (see receiveCacheEntries). Where population is distributed the store
    // keeps a record of what is cached, which has to go as well - otherwise a reactivation reads it back and a
    // retained evicted cache entry is reloaded from it, undoing what was just invalidated
    void invalidateAllDistributed() {
        if (isActivated() && COMMAND.isConsideredBy(distributionMode)) {
            synchronizationLock.ensureLock();
            Repository<K, V> retaining = repository;
            if (distributionMode.isPopulationConsidered() && nonNull(retaining)) {
                Set<Status> statuses = new HashSet<>(CACHED_GROUP);
                if (evictedEntryPersistenceConfigurer.isConfigured()) {
                    statuses.addAll(EVICTED_RETAINED_GROUP);
                }
                // transitions what is there and writes nothing for what is not, which also means it cannot resurrect
                // a key as invalidated that no longer exists. Ahead of the cache entry below, so that a population
                // following this operation cannot be overwritten by it afterwards
                runFailable(() -> retaining.updateStatusOfCacheEntries(null, statuses, null, INVALIDATED));
            }
            // Minted and remembered like any other publish of this cache instance, only for all keys at once
            // rather than for one: what it does covers every one of them, which is what keeps whatever this cache
            // instance writes after it from being undone once this arrives back here
            long counter = operationCounter.incrementAndGet();
            commandOperation.set(counter);
            String operation = activationId.get() + ":" + counter;
            runFailable(() -> publisher.publishCacheEntries(List.of(CacheEntry.of(
                    INVALIDATE_ALL.toString(),
                    operation,
                    null,
                    null,
                    COMMAND,
                    Instant.now()))));
        }
    }

    Set<InternalKey<K>> invalidateAllDistributedRefresh(Set<InternalKey<K>> keys) {
        Map<InternalKey<K>, @Nullable InternalValue<V>> map = new HashMap<>(); // allow null values
        keys.forEach(key -> map.put(key, null));
        publishCacheEntries(map, INVALIDATED_REFRESHED, true);
        return keys;
    }

    @Nullable InternalValue<V> invalidateDistributedRefreshAfterWrite(InternalKey<K> key, InternalValue<V> oldValue) {
        // special handling (activated, async, old value, not managed, no cache change)
        // handling activationId in loader
        if (isActivated()) {
            if (distributionMode.isInvalidationConsidered()) {
                Map<InternalKey<K>, @Nullable InternalValue<V>> map = new HashMap<>(); // allow null values
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
    // special handling (activated, eviction support, async, not managed, cache change).
    // Every condition for distributing an eviction is here rather than partly at the listener reporting it, so
    // that what reaches the underlying store and what does not can be read in one place
    void evictDistributed(@Nullable InternalKey<K> key, @Nullable InternalValue<V> value,
                          RemovalCause removalCause) {
        // a reference that was collected is reported without the key or the value it had, so there is nothing to
        // distribute for it
        if (isNull(key) || isNull(value)) {
            return;
        }
        // An eviction is reported asynchronously, so it can arrive once this cache instance counts as activated
        // again although it took place while it did not - which the value says, because it carries the activation
        // it became content of, and only that activation's content is this cache instance's to distribute
        if (!isActivated() || !hasCurrentActivationId(value)) {
            return;
        }
        // the two causes an eviction is distributed for; the collected one is already out above, and every other
        // cause is a removal this cache instance has distributed itself where it decided on it
        if (!removalCause.equals(RemovalCause.SIZE) && !removalCause.equals(RemovalCause.EXPIRED)) {
            return;
        }
        Status status;
        if (evictedEntryPersistenceConfigurer.isConfigured()) {
            status = removalCause.equals(RemovalCause.SIZE)
                    ? EVICTED_SIZE_RETAINED
                    : EVICTED_TIME_RETAINED;
        } else {
            status = removalCause.equals(RemovalCause.SIZE)
                    ? EVICTED_SIZE
                    : EVICTED_TIME;
        }
        // of the three asynchronous publishers this is the one that cannot be made good later: the entry is
        // already gone from the cache, and nothing reads it again to notice and retry. A lost eviction leaves
        // the other instances serving what this one dropped and, where evicted cache entries are retained,
        // leaves the entry CACHED in the store instead of evicted - so it is never pruned and comes back on
        // the next restart. See the TODO on publishCacheEntriesAsync
        publishCacheEntriesAsync(Map.of(key, value), status);
    }

    // An operation identifies a single write of this cache instance and is remembered for the key it belongs to,
    // which is what a cache entry of its own delivered later is measured against. Deliberately for every managed
    // write and not only for the ones something is published for: whether a cache entry is published says nothing
    // about whether the value handed over here stays in this cache - without population being distributed nothing
    // is published for a population at all, and that is exactly where a removal by this very cache instance must
    // not be allowed to undo it.
    // The activation identifier goes on alongside, but only while this cache instance takes part in
    // synchronization: what it writes while stopped is content of no activation, so nothing is distributed for it
    // afterwards and synchronizing removes it unless the store turns out to back it
    private void stampOperations(Map<? extends InternalKey<K>, ? extends @Nullable InternalValue<V>> map) {
        String currentActivationId = isActivated()
                ? activationId.get()
                : null;
        map.forEach((key, value) -> {
            long counter = operationCounter.incrementAndGet();
            operations.put(key, counter);
            if (nonNull(value)) {
                value.setOperation(activationId.get() + ":" + counter)
                        .setActivationId(currentActivationId);
            }
        });
    }

    // the operation stamped above: on the value where there is one, and reconstructed from what was remembered for
    // the key where there is not, which is the case for an invalidation - it has no value to have been stamped
    private @Nullable String operationOf(InternalKey<K> key, @Nullable InternalValue<V> value) {
        if (nonNull(value)) {
            return value.getOperation();
        }
        Long counter = operations.getIfPresent(key);
        return nonNull(counter)
                ? activationId.get() + ":" + counter
                : null;
    }

    // the counter an operation ends with, if this cache instance issued it in the activation it is in now. Only its
    // own operations say anything here: one of another cache instance is not comparable to what is remembered
    // above, and one of an earlier activation belongs to publishes no longer remembered at all
    private @Nullable Long ownCounterOf(@Nullable String operation) {
        String currentActivationId = activationId.get();
        if (isNull(operation) || isNull(currentActivationId)) {
            return null;
        }
        String prefix = currentActivationId + ":";
        return operation.startsWith(prefix)
                ? Long.valueOf(operation.substring(prefix.length()))
                : null;
    }

    // Whether an arriving cache entry may be applied here.
    // What another cache instance published is always applied, which is what following the one order every cache
    // instance observes amounts to: whatever is delivered last for a key is what all of them end up holding. It is
    // deliberately applied even where it undoes a write this cache instance has just made and not seen come back
    // yet - that write is delivered in its turn and reinstated then, whereas holding the arriving cache entry back
    // would discard it for good, since nothing delivers it a second time.
    // What this cache instance published itself is applied only where it is still the last thing it published for
    // the key, so that an echo delivered after a later write of its own - or after an invalidation, which leaves no
    // value behind at all - cannot put back what that write or that invalidation already replaced. Which is also
    // why a key it no longer holds because Caffeine evicted it or it expired keeps being restored: nothing was
    // published for it afterwards, so what the data store still backs is applied and this cache instance catches up
    private boolean isApplicable(InternalKey<K> key, @Nullable Long ownCounter) {
        if (isNull(ownCounter)) {
            return true;
        }
        long counter = ownCounter;
        Long lastOperation = operations.getIfPresent(key);
        // no record left for the key - expired or dropped by the bound above - so there is nothing saying this was
        // overtaken and it is applied as it would have been before any of this was remembered. Which is the only
        // safe way round: holding it back on a record that is merely gone would lose it for good, since nothing
        // delivers it a second time
        return (isNull(lastOperation) || lastOperation == counter) && commandOperation.get() < counter;
    }

    // What invalidating all removes is not something the cache instance publishing it could know, so it is decided
    // here, from what this one holds at the moment the command arrives: what it published for a key after the
    // command stays, everything else goes. For a command of another cache instance, none of whose operations are
    // comparable here, that is the whole content, and following it means exactly that.
    // Collected before removing, unlike everywhere else here, because what is iterated over is the content of the
    // cache itself and that cannot be removed from while doing so
    // whether what arrives is the echo of the very write the value held here came from. Purely an identity check
    // and no statement about the order: a write of another cache instance may well have landed in between, and
    // then this is not its echo any more and the value has to be applied after all
    private boolean isEchoOf(InternalValue<V> present, @Nullable String operation) {
        return nonNull(operation) && operation.equals(present.getOperation());
    }

    private void receiveInvalidateAll(@Nullable String operation) {
        Long ownCounter = ownCounterOf(operation);
        if (nonNull(ownCounter) && commandOperation.get() != ownCounter) {
            // a command of this cache instance overtaken by a later one of its own, which has already done more
            // than this one would
            return;
        }
        long floor = nonNull(ownCounter) ? ownCounter : Long.MAX_VALUE;
        Set<InternalKey<K>> toRemove = new HashSet<>();
        cache.asMap().keySet().forEach(presentKey -> {
            Long lastOperation = operations.getIfPresent(presentKey);
            if (isNull(lastOperation) || lastOperation < floor) {
                toRemove.add(presentKey);
            }
        });
        cache.invalidateAll(toRemove);
    }

    // the three callers of this method publish outside the synchronization lock because they run where taking it
    // would deadlock with Caffeine's internal lock. It is also the one way a publish can be decided while this cache
    // instance takes part in synchronization and be carried out when it no longer does, so every caller declines an
    // entry the data store has not confirmed before it gets here (see the pointers on the three of them).
    // Whatever the returned future carries is the only trace a failure leaves, and dropping it hides a store that is
    // refusing writes: the distribution is simply lost, while locally everything looks like it succeeded
    // TODO logging makes such a failure visible but does not make the instances converge again. Retrying is not
    // enough on its own, because upsertCacheEntries() writes the status unconditionally, so a delayed retry can
    // overwrite a newer CACHED write for the same key with a stale EVICTED one. Letting the data store drive the
    // correction (as invalidate-on-prune does for persistence of evicted entries) is the more promising direction
    private void publishCacheEntriesAsync(Map<? extends InternalKey<K>, ? extends @Nullable InternalValue<V>> map,
                                          Status status) {
        // Deliberately without an operation, so that what is published here arrives like a change of any other cache
        // instance. Carrying one would order it against the later operations of this cache instance and let this one
        // skip it - which for an eviction means keeping a cache entry the store no longer has as cached, because the
        // eviction is written whenever the executor gets around to it and can land after the population following
        // it. Every other cache instance then drops the cache entry while this one keeps it, and nothing reads the
        // store again to notice. Losing the population everywhere is wrong too, but at least it is not a divergence
        // between the instances - see the corresponding disabled test
        CompletableFuture.runAsync(() -> {
                    if (isActivated()) { // just if the future runs late
                        publishCacheEntries(map, status, false);
                    }
                }, executor)
                .exceptionally(throwable -> {
                    logger.log(Level.WARNING, format("Distributing %s for %s failed for cache at '%s'",
                            status, map.keySet(), identifier), throwable);
                    return null;
                });
    }

    private void publishCacheEntries(Map<? extends InternalKey<K>, ? extends @Nullable InternalValue<V>> map,
                                     Status status, boolean manage) {
        // Marked ahead of everything below, because whether a cache entry is published says nothing about whether
        // the value handed over here stays in this cache: without population being distributed nothing is published
        // for a population at all, and the value still belongs to this activation. Only for the managed operations
        // though, which are the ones taking place right here - the asynchronous ones are marked by the listener and
        // the loader deciding them, not when they are finally published
        if (manage) {
            stampOperations(map);
        }
        // persistence of evicted entries should work regardless of the distribution mode
        if (isActivated() && (status.isConsideredBy(distributionMode) || status.isEvictedRetained())) {
            if (manage) {
                synchronizationLock.ensureLock();
            }
            // deliberately without a check of whether this cache instance holds the key: whether it does says
            // nothing about the other ones, which may well be serving it, so skipping the write here would leave
            // them doing so indefinitely. Filtering by what is held belongs on the receiving side (see
            // receiveCacheEntries), where it is a fact rather than a guess
            List<CacheEntry<K, V>> cacheEntries = map.entrySet().stream()
                    .map(entry -> {
                        InternalValue<V> value = entry.getValue();
                        return CacheEntry.of(
                                // memoizing overload: reuses the hash cached on the key instance (e.g. stamped when
                                // the entry was put/loaded/received) instead of recomputing it under the lock
                                hasher.getHash(entry.getKey()),
                                manage ? operationOf(entry.getKey(), value) : null,
                                k(entry.getKey()),
                                vn(value),
                                status,
                                Instant.now());
                    })
                    // toList (not a set): entries are unique per key, so no dedup is needed and this avoids
                    // computing CacheEntry hashCode/equals on the write path
                    .toList();
            if (!cacheEntries.isEmpty()) {
                runFailable(() -> publisher.publishCacheEntries(cacheEntries));
            }
        }
    }

    // Where arriving cache entries come from, which is what decides whether one that is already held may be left
    // as it is instead of being written again.
    // A change the adapter delivers arrives on its own, so leaving it alone is both safe and worth it - writing an
    // equal value would report a removal for it and restart what Caffeine measures from a write.
    // Reading the whole data store back is the opposite: it writes over a cache that is already at its maximum and
    // therefore evicts while it runs, and Caffeine hands a value to a reader while its removal is still under way.
    // A value left alone can be gone a moment later, taking the mark with it - and since nothing was written for
    // the key either, it is simply missing while the data store goes on backing it, with no cache instance holding
    // it and nothing reading the store again to notice
    private enum Arrival {
        DELIVERED,
        READ_BACK
    }

    @Override
    public void receiveCacheEntries(List<CacheEntry<K, V>> cacheEntries) {
        // no filtering by discriminator here: a receiver belongs to exactly one cache, and the adapter handing over
        // these cache entries is scoped to that cache's discriminator - so whatever arrives is already its own
        receiveCacheEntries(cacheEntries.stream(), Arrival.DELIVERED);
    }

    private void receiveCacheEntries(Stream<CacheEntry<K, V>> cacheEntries, Arrival arrival) {
        if (isActivated()) {
            // Applied one at a time rather than collected and applied afterwards, so that every cache entry is
            // decided against what the ones handed over before it have already done. Collecting decides all of them
            // against the content held before any of them was applied, which for two changes of one key means the
            // outcome is picked by which of the two of them a separate insertion and removal ends up in rather than
            // by which of them came last. Caffeine applies its own bulk operations one entry at a time as well, so
            // there is nothing to be gained by collecting either
            synchronizationLock.runLocked(() -> cacheEntries
                    .filter(cacheEntry -> cacheEntry.getStatus().isConsideredBy(distributionMode))
                    .forEach(cacheEntry -> receiveCacheEntry(cacheEntry, arrival)));
        }
    }

    private void receiveCacheEntry(CacheEntry<K, V> cacheEntry, Arrival arrival) {
        // A command belongs to no key, so it is handled ahead of everything below, which all works with one.
        // Dispatched by the name it carries as its hash, and one that is not known here is skipped rather than
        // treated as a cache entry - which is what allows a command to be added without every cache instance
        // already understanding it
        if (cacheEntry.isCommand()) {
            if (INVALIDATE_ALL.toString().equals(cacheEntry.getHash())) {
                receiveInvalidateAll(cacheEntry.getOperation());
            }
            return;
        }
        // propagate the store's hash onto the key so it is never recomputed for this entry
        // (e.g. when it is later evicted or re-published from this instance).
        // Only a command carries no key, and those returned above - a cache entry without one
        // contradicts its own status, which no annotation can express here
        InternalKey<K> key = ik(requireNonNull(cacheEntry.getKey()))
                .setHash(cacheEntry.getHash());
        // whether this is one of the publishes of this cache instance coming back, and which of them
        Long ownCounter = ownCounterOf(cacheEntry.getOperation());
        if (!isApplicable(key, ownCounter)) {
            return;
        }
        if (cacheEntry.isCached()) {
            InternalValue<V> present = policy.getIfPresentQuietly(key);
            String operation = cacheEntry.getOperation();
            // Left as it is where it is already held and nothing evicts alongside (see Arrival). The mark is
            // refreshed either way, because the data store backs the entry and it has to survive a stale sweep
            // even where nothing is written for it - an entry that is in sync when synchronization stops always
            // takes this branch
            if (arrival == Arrival.DELIVERED && nonNull(present) && isEchoOf(present, operation)) {
                present.setActivationId(activationId.get());
            } else {
                // a cached status always comes with a value - only invalidated and evicted ones carry none,
                // which no annotation can express here either
                cache.put(key, iv(requireNonNull(cacheEntry.getValue()))
                        .setOperation(operation)
                        .setActivationId(activationId.get()));
            }
        } else {
            // a no-op where the key is not held, which is the ordinary case for an invalidation of this cache
            // instance coming back: it removed the value when it was issued
            cache.invalidate(key);
        }
    }

    // while synchronization was stopped the cache kept serving locally, so local writes never reached the data store
    // and changes made elsewhere never arrived. Receiving below only ever adds what the store holds, which would
    // leave entries the store no longer backs in place to be served as if they were still valid. Everything present
    // is marked by then (stopping and activating do that), so what the store still knows clears its mark again and
    // only what stays marked is removed here - in place, which keeps the cache readable throughout instead of
    // replacing it with an empty one that answers every read with a miss
    void synchronizeCacheEntries() {
        if (isActivated()) {
            synchronizationLock.ensureLock();
            // without a synchronization strategy nothing is read back, which is also why nothing was retained: the
            // marks below are then left in place for everything, so the cache is emptied rather than reconciled
            if (cachedEntryPersistenceConfigurer.hasInitialSynchronizationStrategy()
                    && distributionMode.isPopulationConsidered()) {
                // process the store cursor directly instead of buffering it into a set first (avoids a second full
                // copy in memory and the needless CacheEntry hashCode/equals a set would compute)
                Repository<K, V> retaining = requireRepository(repository, identifier);
                try (Stream<CacheEntry<K, V>> cacheEntryStream = getFailable(() -> retaining.streamCacheEntries(
                        null,
                        CACHED_GROUP,
                        true))) {
                    receiveCacheEntries(cacheEntryStream, Arrival.READ_BACK);
                }
            }
            // without population being considered nothing clears the marks, so everything present is dropped - the
            // same outcome as before, where a restart always continued with an empty cache
            cache.asMap().values()
                    .removeIf(value -> !hasCurrentActivationId(value));
        }
    }

    void cleanup() {
        if (isActivated()) {
            cache.cleanUp();
        }
    }
}
