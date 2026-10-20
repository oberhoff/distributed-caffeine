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
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.CachedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.InternalUtils.FailableRunnable;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.Publisher;
import io.github.oberhoff.distributedcaffeine.adapter.Receiver;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import static io.github.oberhoff.distributedcaffeine.adapter.Repository.Order.ASCENDING;
import static io.github.oberhoff.distributedcaffeine.adapter.Repository.Order.UNORDERED;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings({"java:S1452"})
class InternalCacheManager<K, V> implements InternalInitializable<K, V>, Receiver<K, V> {

    // the most one asynchronous publish writes at once, so that a backlog goes out in writes of a bounded size
    private static final int MAXIMUM_ASYNCHRONOUS_BATCH = 1_000;

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
    // The loads and refreshes in flight for a key, as a token of each one that started. They read outside the
    // synchronization lock, so whatever happens to the key meanwhile is newer than what they read, and their result
    // may only be applied where nothing did. Every write to the key - of this cache instance or of another one -
    // therefore takes the tokens away, the same way Caffeine discards its own refreshes on every write, and one that
    // no longer holds its token applies nothing. Kept here because this is where every one of those writes passes,
    // including an invalidation of a key that is not held, which leaves nothing else behind to tell from.
    // Several per key, because a load and a refresh of the same key do not wait for each other
    private final ConcurrentMap<InternalKey<K>, Set<Object>> inFlight;

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
    private InternalStoreGuard storeGuard;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalDistributedStatsCounter distributedStatsCounter;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalHasher<K> hasher;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Executor executor;
    // What the asynchronous publishers hand over, in the order they did, and whether a drain is under way to write
    // it - both guarded by the queue itself (see publishCacheEntriesAsync)
    private final Deque<PendingPublish<K, V>> pendingPublishes;
    private boolean draining;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalCacheManager() {
        this.isActivated = new AtomicBoolean();
        this.activationId = new AtomicReference<>();
        this.operationCounter = new AtomicLong();
        this.operations = Caffeine.newBuilder()
                .expireAfterWrite(DISTRIBUTION_DURATION)
                .build();
        this.commandOperation = new AtomicLong();
        this.inFlight = new ConcurrentHashMap<>();
        this.pendingPublishes = new ArrayDeque<>();
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
        this.storeGuard = instanceRegistry.getStoreGuard();
        this.distributedStatsCounter = instanceRegistry.getDistributedStatsCounter();
        this.hasher = instanceRegistry.getHasher();
        this.executor = instanceRegistry.getExecutor();
    }

    void activate() {
        renewActivation();
        isActivated.set(true);
    }

    // Renewing the identifier is what invalidates everything held from before, without touching a single value:
    // none of it is of this activation any more, so nothing published from here on can rest on it and
    // synchronizing keeps only what the data store confirms. Which also reaches what no pass over the cache
    // could - a value already evicted, whose eviction is reported asynchronously and would otherwise be
    // distributed as if it had taken place after starting again.
    // Separate from activating because recovering from a loss of synchronization needs exactly this and nothing
    // else: the cache instance is already activated by then, so reporting it activated again would say something
    // that is not true of it and is not meant by it
    private void renewActivation() {
        activationId.set(Long.toHexString(ThreadLocalRandom.current().nextLong()));
        // nothing published before this activation is one of its publishes, so none of it may hold an arriving
        // cache entry back any more
        operations.invalidateAll();
        commandOperation.set(0);
        // nor may a load or refresh still in flight from before apply what it read: what it rests on was read before
        // synchronizing settles the content anew, and with the cache loader strategy that is the store's content
        // from before reconciling
        overtakeAll();
    }

    void deactivate() {
        isActivated.set(false);
    }

    // whether this value is content of anything but the current activation, meaning this cache instance neither
    // wrote nor received it while taking part in synchronization. Only content of the current activation may have
    // a change to it distributed, and only such content survives synchronizing - which is what every caller here
    // asks about, so the question is put the way they ask it rather than inverted at each of them
    boolean isOutsideCurrentActivation(InternalValue<V> value) {
        String currentActivationId = activationId.get();
        return isNull(currentActivationId) || !currentActivationId.equals(value.getActivationId());
    }

    boolean isActivated() {
        return isActivated.get();
    }

    // Reloading retained evicted cache entries, for whichever loading strategy asked for it: a cache loader is not
    // the only way into a cache instance, so this lives here rather than with the one that happens to be optional.
    // Which strategy applies is decided by the caller, because what it falls back to when the store comes up empty
    // is the caller's as well
    @Nullable V loadFromStore(InternalKey<K> key) {
        return loadAllFromStore(Set.of(key)).get(k(key));
    }

    Map<K, V> loadAllFromStore(Set<? extends InternalKey<K>> keys) {
        // the memoizing overload caches each hash on its key instance, so a subsequent publish that reuses the same
        // instance (the publish of what was loaded, or refreshAfterWrite) does not recompute it
        Set<String> hashes = keys.stream()
                .map(hasher::getHash)
                .collect(toSet());
        Repository<K, V> retaining = requireRepository(repository, identifier);
        // Guarded, and this is the half that would otherwise never be counted: it runs before anything is
        // published, so a store that is down fails here and the publish that would have reported it is never
        // reached. Left unguarded, a cache that reads the store on every miss would pay the driver's timeout for
        // every one of them, for as long as the outage lasts, while holding the synchronization lock
        Map<K, V> found;
        try (Stream<CacheEntry<K, V>> cacheEntryStream = getFailable(() -> storeGuard.getGuarded(identifier,
                () -> retaining.streamCacheEntries(
                        hashes,
                        EVICTED_RETAINED_GROUP,
                        UNORDERED)))) {
            //noinspection NullableProblems
            found = cacheEntryStream
                    .filter(cacheEntry -> nonNull(cacheEntry.getValue()))
                    .collect(toMap(CacheEntry::getKey, CacheEntry::getValue));
        } catch (RuntimeException e) {
            // per key like a hit or a miss, so that the three of them add up to what was looked up
            distributedStatsCounter.recordStoreLookupFailure(keys.size());
            throw e;
        }
        distributedStatsCounter.recordStoreLookup(found.size(), keys.size() - found.size());
        return found;
    }

    InternalValue<V> putDistributed(InternalKey<K> key, InternalValue<V> value) {
        putAllDistributed(Map.of(key, value));
        return value;
    }

    Map<? extends InternalKey<K>, ? extends InternalValue<V>> putAllDistributed(
            Map<? extends InternalKey<K>, ? extends InternalValue<V>> map) {
        publishCacheEntries(map, CACHED);
        return map;
    }

    Map<? extends InternalKey<K>, ? extends InternalValue<V>> putAllDistributedLoaded(
            Map<? extends InternalKey<K>, ? extends InternalValue<V>> map) {
        publishCacheEntries(map, CACHED_LOADED);
        return map;
    }

    Map<? extends InternalKey<K>, ? extends InternalValue<V>> putAllDistributedRefresh(
            Map<? extends InternalKey<K>, ? extends InternalValue<V>> map) {
        publishCacheEntries(map, CACHED_REFRESHED);
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
        publishCacheEntries(map, INVALIDATED);
        return keys;
    }

    // Invalidating all cache entries cannot be expressed as a set of keys: the calling cache instance can only
    // enumerate what it holds itself, and without population being distributed nothing else knows what the others
    // hold. So instead of one cache entry per key, a command is written, and every cache instance decides from its own
    // content what it removes when that arrives (see receiveCacheEntries). What the store retains has to go as well:
    // where population is distributed it keeps a record of what is cached, which a reactivation would read back,
    // and where evicted cache entries are retained - in every distribution mode, because an eviction carries the
    // value of the cache instance handing it over rather than one population distributed - the next miss would
    // reload one of them. Either way, what was just invalidated would come back
    void invalidateAllDistributed() {
        // whether or not anything is distributed for it, every key is written by it
        overtakeAll();
        if (isActivated() && COMMAND.isConsideredBy(distributionMode)) {
            synchronizationLock.ensureExclusiveLock();
            // Minted and remembered like any other publish of this cache instance, only for all keys at once
            // rather than for one: what it does covers every one of them, which is what keeps whatever this cache
            // instance writes after it from being undone once this arrives back here.
            // Minted ahead of marking what the store holds invalidated, so that the marks carry it as well: they are
            // delivered like any other change, and late - after this cache instance wrote a key again - they would
            // otherwise arrive as an invalidation of another cache instance and undo that write. Carrying the
            // operation, they are the echo of this command instead, which a later write is never undone by.
            // Remembered only once the store took them, as before, so that a failing store leaves nothing behind
            long counter = operationCounter.incrementAndGet();
            String operation = activationId.get() + ":" + counter;
            Repository<K, V> retaining = repository;
            Set<Status> statuses = new HashSet<>();
            if (distributionMode.isPopulationConsidered()) {
                statuses.addAll(CACHED_GROUP);
            }
            if (evictedEntryPersistenceConfigurer.isConfigured()) {
                statuses.addAll(EVICTED_RETAINED_GROUP);
            }
            if (!statuses.isEmpty() && nonNull(retaining)) {
                // transitions what is there and writes nothing for what is not, which also means it cannot resurrect
                // a key as invalidated that no longer exists. Ahead of the cache entry below, so that a population
                // following this operation cannot be overwritten by it afterwards.
                // Guarded like the publish it precedes, and for a reason that only shows from here: it runs first,
                // so a store that is down fails on this one and the publish that would have reported it is never
                // reached - leaving the sweep paying the driver's timeout under the lock, every time, forever
                writeGuarded(() -> retaining.updateStatusOfCacheEntries(null, statuses, null, operation,
                        INVALIDATED), true);
            }
            commandOperation.set(counter);
            publishGuarded(true, List.of(CacheEntry.of(
                    INVALIDATE_ALL.toString(),
                    operation,
                    null,
                    null,
                    COMMAND,
                    Instant.now())));
        }
    }

    Set<InternalKey<K>> invalidateAllDistributedRefresh(Set<InternalKey<K>> keys) {
        Map<InternalKey<K>, @Nullable InternalValue<V>> map = new HashMap<>(); // allow null values
        keys.forEach(key -> map.put(key, null));
        publishCacheEntries(map, INVALIDATED_REFRESHED);
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
        if (!isActivated() || isOutsideCurrentActivation(value)) {
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
            overtake(key);
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
        // a command of this cache instance took the tokens away when it was issued already, and anything
        // started since is newer than it
        if (isNull(ownCounter)) {
            overtakeAll();
        }
        Set<InternalKey<K>> toRemove = new HashSet<>();
        cache.asMap().keySet().forEach(presentKey -> {
            Long lastOperation = operations.getIfPresent(presentKey);
            if (isNull(lastOperation) || lastOperation < floor) {
                toRemove.add(presentKey);
            }
        });
        cache.invalidateAll(toRemove);
        // what is removed rather than the command itself, so that it adds up with the transitions of the same
        // command arriving for single keys (see invalidateAllDistributed): whichever comes first removes a value,
        // and the other one finds nothing left to count
        if (isNull(ownCounter)) {
            distributedStatsCounter.recordRemoteRemovals(toRemove.size());
        }
    }

    // The three callers of this method publish outside the synchronization lock because they run where taking it
    // would deadlock with Caffeine's internal lock. It is also the one way a publish can be decided while this cache
    // instance takes part in synchronization and be carried out when it no longer does, so every caller declines an
    // entry the data store has not confirmed before it gets here (see the pointers on the three of them).
    // Handed over to the one drain below rather than written by a task of its own each: a task per cache entry
    // holds a connection of the store per cache entry, so under load they take every connection there is and the
    // writes of the cache instance itself wait behind them, and fail once waiting for a connection times out. The
    // drain holds one, and what is handed over while it writes goes out with its next write - nothing waits for a
    // batch to fill, so a cache entry is written as soon as the one write ahead of it is done.
    // In the order handed over, and filtered here by the same rule a publish applies, so that nothing waits in line
    // that would not be written anyway
    private void publishCacheEntriesAsync(Map<? extends InternalKey<K>, ? extends @Nullable InternalValue<V>> map,
                                          Status status) {
        // persistence of evicted entries should work regardless of the distribution mode
        if (!status.isConsideredBy(distributionMode) && !status.isEvictedRetained()) {
            return;
        }
        boolean startDraining;
        synchronized (pendingPublishes) {
            map.forEach((key, value) -> pendingPublishes.add(new PendingPublish<>(key, value, status)));
            startDraining = !draining;
            draining = true;
        }
        if (startDraining) {
            try {
                executor.execute(this::drainPendingPublishes);
            } catch (RuntimeException e) {
                // left in line for the next hand-over to start draining
                synchronized (pendingPublishes) {
                    draining = false;
                }
                throw e;
            }
        }
    }

    // Writes what is in line, a batch at a time, until nothing is left. A batch takes every key once at most: a
    // store may apply the writes of one batch in any order - the MongoDB adapter writes them unordered - and two of
    // the same key would then land the wrong way round as often as not, so the second one starts the next batch
    private void drainPendingPublishes() {
        boolean drained = false;
        try {
            while (!drained) {
                List<PendingPublish<K, V>> batch = new ArrayList<>();
                synchronized (pendingPublishes) {
                    Set<InternalKey<K>> keys = new HashSet<>();
                    while (batch.size() < MAXIMUM_ASYNCHRONOUS_BATCH && !pendingPublishes.isEmpty()
                            && keys.add(pendingPublishes.getFirst().key())) {
                        batch.add(pendingPublishes.removeFirst());
                    }
                    if (batch.isEmpty()) {
                        draining = false;
                        drained = true;
                    }
                }
                if (!drained) {
                    publishPending(batch);
                }
            }
        } finally {
            // whatever ended it otherwise, the next hand-over starts draining again
            if (!drained) {
                synchronized (pendingPublishes) {
                    draining = false;
                }
            }
        }
    }

    // Deliberately without an operation, so that what is published here arrives like a change of any other cache
    // instance. Carrying one would order it against the later operations of this cache instance and let this one
    // skip it - which for an eviction means keeping a cache entry the store no longer has as cached, because the
    // eviction is written whenever the drain gets around to it and can land after the population following it.
    // Every other cache instance then drops the cache entry while this one keeps it, and nothing reads the store
    // again to notice. Losing the population everywhere is wrong too, but at least it is not a divergence between
    // the instances - see the corresponding disabled test.
    // A failure is logged rather than thrown, because nobody waits for this write to throw to
    // TODO logging makes such a failure visible but does not make the instances converge again. Retrying is not
    // enough on its own, because upsertCacheEntries() writes the status unconditionally, so a delayed retry can
    // overwrite a newer CACHED write for the same key with a stale EVICTED one. Letting the data store drive the
    // correction (as invalidate-on-prune does for persistence of evicted entries) is the more promising direction
    private void publishPending(List<PendingPublish<K, V>> batch) {
        // just if the cache instance stopped since it was handed over
        if (!isActivated()) {
            return;
        }
        List<CacheEntry<K, V>> cacheEntries = batch.stream()
                .map(pending -> CacheEntry.of(hasher.getHash(pending.key()), null, k(pending.key()),
                        vn(pending.value()), pending.status(), Instant.now()))
                .toList();
        try {
            publishGuarded(false, cacheEntries);
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, format("Distributing %s for %s failed for cache at '%s'",
                    batch.stream().map(PendingPublish::status).collect(toCollection(LinkedHashSet::new)),
                    batch.stream().map(PendingPublish::key).toList(), identifier), e);
        }
    }

    // Marked ahead of everything below, because whether a cache entry is published says nothing about whether the
    // value handed over here stays in this cache: without population being distributed nothing is published for a
    // population at all, and the value still belongs to this activation. These are the operations taking place
    // right here - the asynchronous ones are marked by the listener and the loader deciding them, not when they are
    // finally published
    private void publishCacheEntries(Map<? extends InternalKey<K>, ? extends @Nullable InternalValue<V>> map,
                                     Status status) {
        stampOperations(map);
        // persistence of evicted entries should work regardless of the distribution mode
        if (isActivated() && (status.isConsideredBy(distributionMode) || status.isEvictedRetained())) {
            synchronizationLock.ensureLock(map.keySet());
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
                                operationOf(entry.getKey(), value),
                                k(entry.getKey()),
                                vn(value),
                                status,
                                Instant.now());
                    })
                    // toList (not a set): entries are unique per key, so no dedup is needed and this avoids
                    // computing CacheEntry hashCode/equals on the write path
                    .toList();
            if (!cacheEntries.isEmpty()) {
                try {
                    publishGuarded(true, cacheEntries);
                } catch (RuntimeException e) {
                    // A write that failed may have reached the data store or not, so what was remembered for it
                    // says nothing any more about which of the writes of this cache instance comes back last - and
                    // where it is not the one the data store ends up with, it would hold back the one that is. Like a
                    // write of its own that failed in doubt and landed, followed by another one that failed without
                    // landing: the first one is delivered, the second never, and remembering the second discards the
                    // first for good, while every other cache instance applies it. Forgotten instead, which is the
                    // safe way round for the same reason a record that is merely gone is (see isApplicable)
                    map.keySet().forEach(operations::invalidate);
                    throw e;
                }
            }
        }
    }

    // Refused rather than attempted while the store is being left alone, and reported as the store failing
    // would be reported: what a caller can do about it is the same either way, so it is not made into a failure
    // of its own that every caller would have to learn about
    // Through runFailable like the plain publish it replaces, so that a caller sees a store failure wrapped
    // exactly as it was wrapped before anything stood in front of it
    private void publishGuarded(boolean synchronous, List<CacheEntry<K, V>> cacheEntries) {
        writeGuarded(() -> publisher.publishCacheEntries(cacheEntries), synchronous);
    }

    // Every write of this cache instance to the store comes through here, which is what makes it the one place to
    // count them. Only a synchronous one is timed: a caller waits for it, under the stripes of its keys, so its time
    // is both what a write costs whoever made it and how long those stripes were taken up by it - neither of which
    // holds for one of the drain, which nobody waits for and which runs alongside them. Synchronous writes to
    // different keys overlap, so their times add up to something meaningful per write only, not per unit of time -
    // which is why DistributedStats offers them as an average per write and nothing else. Timed including a refusal, which costs next to nothing, as a load
    // failing fast does in Caffeine
    private void writeGuarded(FailableRunnable write, boolean synchronous) {
        long startTime = System.nanoTime();
        boolean success = false;
        try {
            runFailable(() -> storeGuard.runGuarded(identifier, write));
            success = true;
        } finally {
            if (success) {
                distributedStatsCounter.recordStoreWriteSuccess();
            } else {
                distributedStatsCounter.recordStoreWriteFailure(synchronous);
            }
            if (synchronous) {
                distributedStatsCounter.recordSynchronousStoreWrite(System.nanoTime() - startTime);
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
        if (!isActivated()) {
            return;
        }
        // Applied under the scope of what they concern: an entry is a key operation, a command is a whole-cache one.
        // A command therefore splits what is handed over into runs of entries, applied in turn with the command
        // between them, and a run is applied one stripe at a time rather than one entry at a time: taking a fair
        // stripe once per entry would hand over one entry per store round-trip of a writer waiting on it (see
        // InternalSynchronizationLock). Within a stripe the entries keep their order, which is what the order of
        // the entries of one key rests on - entries of different keys do not depend on each other's order, while a
        // command does on everything around it, which is why it is never part of a run
        List<Map.Entry<InternalKey<K>, CacheEntry<K, V>>> run = new ArrayList<>();
        for (CacheEntry<K, V> cacheEntry : cacheEntries) {
            if (cacheEntry.getStatus().isConsideredBy(distributionMode)) {
                if (cacheEntry.isCommand()) {
                    applyRun(run);
                    run.clear();
                    synchronizationLock.runLocked(() -> receiveCacheEntry(cacheEntry, Arrival.DELIVERED));
                } else {
                    InternalKey<K> key = ik(requireNonNull(cacheEntry.getKey()))
                            .setHash(cacheEntry.getHash());
                    run.add(Map.entry(key, cacheEntry));
                }
            }
        }
        applyRun(run);
    }

    private void applyRun(List<Map.Entry<InternalKey<K>, CacheEntry<K, V>>> run) {
        if (!run.isEmpty()) {
            synchronizationLock.runLockedPerStripe(run, Map.Entry::getKey,
                    entry -> applyCacheEntry(entry.getValue(), entry.getKey(), Arrival.DELIVERED));
        }
    }

    @Override
    public void receiveSynchronizationRestart() {
        // What the adapter reports is a fact about itself - receiving restarted - and what that means is this
        // side's to decide, because only here is it known what a cache entry missed in the meantime would cost.
        // Reconciling instead of trusting what the resumed stream delivers, because a cache entry missed while
        // synchronization was interrupted leaves nothing behind that a replay could report: the event carrying it
        // is simply not delivered. Renewing the identifier marks everything held from before and synchronizing
        // then keeps only what the data store confirms - the same two steps activating takes, and for the same
        // reason, rather than a second mechanism beside them.
        // Both the check and the recovery happen under the lock, so that deactivating in between cannot be
        // overtaken by what follows it
        synchronizationLock.runLocked(() -> {
            if (isActivated()) {
                long retainedBefore = cache.estimatedSize();
                renewActivation();
                synchronizeCacheEntries();
                distributedStatsCounter.recordReconciliation();
                // Reported because what it costs is visible to whoever uses this cache and explains nothing about
                // itself: without persistence of cached entries the data store confirms none of them, so the cache
                // is left empty and every read misses until it fills again. Logged after the fact so that the
                // counts are what actually happened rather than what was about to be attempted.
                // Counted as before and after rather than as a share of what was held, because reconciling both
                // drops and restores: what the data store backs but this cache instance missed comes back with it,
                // so it can hold more afterwards than it did before
                logger.log(Level.WARNING, format("Synchronization was interrupted for cache at '%s', so the cache "
                                + "was reconciled against the underlying store, dropping what it does not confirm "
                                + "and restoring what it backs (%d before, %d after)",
                        identifier, retainedBefore, cache.estimatedSize()));
            }
        });
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
        applyCacheEntry(cacheEntry, key, arrival);
    }

    @SuppressWarnings("java:S3776")
    private void applyCacheEntry(CacheEntry<K, V> cacheEntry, InternalKey<K> key, Arrival arrival) {
        // whether this is one of the publishes of this cache instance coming back, and which of them
        Long ownCounter = ownCounterOf(cacheEntry.getOperation());
        if (!isApplicable(key, ownCounter)) {
            return;
        }
        // only a write of another cache instance is news here: one of this cache instance took the tokens away
        // when it was issued, and taking away one started since would discard a load nothing overtook. Nor is a
        // cache entry without any operation, which only the store writes itself - transitioning statuses for
        // invalidating all, which the command following it takes care of, or pruning, which is no write of the
        // value at all. Taking the tokens away for those discarded a refresh started right after invalidating all,
        // whenever the transition arrived while it was reloading
        boolean isOfAnother = isNull(ownCounter) && nonNull(cacheEntry.getOperation());
        if (isOfAnother) {
            overtake(key);
        }
        // what another cache instance did, as it arrives - not what reading the data store back restores, which
        // is this cache instance catching up rather than another one changing anything
        boolean isRemote = isOfAnother && arrival == Arrival.DELIVERED;
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
                if (isRemote) {
                    distributedStatsCounter.recordRemoteUpdate();
                }
            }
        } else {
            // counted only where something is removed: every cache instance receives every invalidation, and one
            // of a key not held here changes nothing here
            if (isRemote && nonNull(policy.getIfPresentQuietly(key))) {
                distributedStatsCounter.recordRemoteRemovals(1);
            }
            // a no-op where the key is not held, which is the ordinary case for an invalidation of this cache
            // instance coming back: it removed the value when it was issued
            cache.invalidate(key);
        }
    }

    // whenever synchronization was not in effect the cache kept serving locally while changes made elsewhere did
    // not arrive - either because it was stopped, in which case local writes never reached the data store either,
    // or because watching failed, in which case they did and only the inbound half was missing. Receiving below
    // only ever adds what the store holds, which would leave entries the store no longer backs in place to be
    // served as if they were still valid. Everything present is marked by then (stopping, activating and
    // recovering from a loss of synchronization all do that), so what the store still knows clears its mark again
    // and only what stays marked is removed here - in place, which keeps the cache readable throughout instead of
    // replacing it with an empty one that answers every read with a miss
    void synchronizeCacheEntries() {
        if (isActivated()) {
            synchronizationLock.ensureExclusiveLock();
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
                        ASCENDING))) {
                    // the exclusive side of the scope is held (activation and reconciliation take it), so applying
                    // the entries directly is what keeps this one whole-cache operation rather than one per key
                    cacheEntryStream
                            .filter(cacheEntry -> cacheEntry.getStatus().isConsideredBy(distributionMode))
                            .forEach(cacheEntry -> receiveCacheEntry(cacheEntry, Arrival.READ_BACK));
                }
            }
            // without population being considered nothing clears the marks, so everything present is dropped - the
            // same outcome as before, where a restart always continued with an empty cache
            cache.asMap().values()
                    .removeIf(this::isOutsideCurrentActivation);
        }
    }

    // registers a load or refresh now in flight for the key, see inFlight
    void registerInFlight(InternalKey<K> key, Object token) {
        // in one atomic step with creating the set, for the reason given below
        inFlight.compute(key, (k, tokens) -> {
            Set<Object> held = isNull(tokens) ? ConcurrentHashMap.newKeySet() : tokens;
            held.add(token);
            return held;
        });
    }

    // whether the load or refresh still held its token, which it gives up either way. Only one that still did may
    // apply its result. Removing the emptied set happens in the same atomic step as the check, so that a token
    // registered concurrently never ends up in a set that is no longer mapped
    boolean releaseInFlight(InternalKey<K> key, Object token) {
        boolean[] held = new boolean[1];
        inFlight.computeIfPresent(key, (k, tokens) -> {
            held[0] = tokens.remove(token);
            return tokens.isEmpty() ? null : tokens;
        });
        return held[0];
    }

    // the activation in effect, to tell whether synchronization started anew while something read outside the
    // synchronization lock (see renewActivation)
    @Nullable String getActivationId() {
        return activationId.get();
    }

    private void overtake(InternalKey<K> key) {
        inFlight.remove(key);
    }

    private void overtakeAll() {
        inFlight.clear();
    }

    void cleanup() {
        if (isActivated()) {
            cache.cleanUp();
        }
    }

    // a cache entry handed over for an asynchronous publish, with the status it is published with
    private record PendingPublish<K, V>(InternalKey<K> key, @Nullable InternalValue<V> value, Status status) {
    }
}
