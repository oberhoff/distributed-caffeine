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

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.oberhoff.distributedcaffeine.InternalStatsCounter.Counter;
import io.github.oberhoff.distributedcaffeine.InternalStoreGuard.Operation;
import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.Expected;
import io.github.oberhoff.distributedcaffeine.spi.ExpectedEntry;
import io.github.oberhoff.distributedcaffeine.spi.ReadResult;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * The core of one cache instance: L1 (a Caffeine {@code AsyncCache} of {@link InternalKey} to {@link InternalValue}),
 * the store, and the paths between them. The facades ({@code DistributedCache} and friends) only translate.
 * <p>
 * Synchronous loads run in the caller's thread, as in Caffeine's synchronous caches, but outside Caffeine's lock: on a
 * miss the caller puts its own incomplete future into L1 (atomic and cheap), then loads and completes it; other
 * callers for the key wait on that future. Loads Caffeine triggers itself (refresh, {@code refreshAfterWrite}) go
 * through the internal {@link AsyncCacheLoader} on the store executor.
 * <p>
 * A load reads the store first and calls the source only on a miss; its result is written conditionally on what it
 * read (ordering rule 6): {@code ifMissing} after reading nothing, {@code ifVersion} after reading a tombstone or an
 * unreadable value, so that an invalidation in between makes the write conflict and the load starts over.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
// the core is the cache's one place of coordination (ordering rules, loads, messages, maintenance), so its size,
// its dependencies and the complexity of its steps are deliberate; see VISION.md
@SuppressWarnings({"java:S6539", "java:S3776", "java:S107"})
final class InternalCore<K, V> {

    private static final System.Logger LOGGER = System.getLogger("io.github.oberhoff.distributedcaffeine");
    private static final long RESUME_MIN_MILLIS = 250;
    private static final long RESUME_MAX_MILLIS = 30_000;

    // the hashes this thread is loading as their owner, to detect a loader requesting its own key; emptied after each
    // load, and kept per thread to spare an allocation per load
    @SuppressWarnings("java:S5164")
    private static final ThreadLocal<Set<InternalKey<?>>> LOADING = ThreadLocal.withInitial(HashSet::new);
    private static final int ATTEMPTS = 3;
    // the epoch of values loaded from the source alone while the store did not answer: never trusted, so the first
    // read with the store back checks them
    private static final int UNCONFIRMED = -1;
    private static final int REVALIDATION_BATCH = 1_000;
    private static final long REVALIDATION_PAUSE_MILLIS = 10;

    @SuppressWarnings("java:S112") // as Caffeine's loaders: any exception
    interface SingleLoader<K, V> {
        @Nullable V load(K key) throws Exception;
    }

    @SuppressWarnings({"java:S112", "java:S1452"}) // as Caffeine's loaders: any exception, any subtypes
    interface BulkLoader<K, V> {
        Map<? extends K, ? extends V> loadAll(Set<? extends K> keys) throws Exception;
    }

    @SuppressWarnings("java:S112") // as Caffeine's loaders: any exception
    interface Reloader<K, V> {
        @Nullable V reload(K key, V oldValue) throws Exception;
    }

    // the user's loaders in their asynchronous forms (AsyncCacheLoader, the mapping functions of AsyncCache): a load
    // waits for their futures without holding a thread
    @SuppressWarnings({"java:S112", "java:S1452"}) // as Caffeine's loaders: any exception, any subtypes
    interface AsyncSingleLoader<K, V> {
        CompletableFuture<? extends @Nullable V> load(K key) throws Exception;
    }

    @SuppressWarnings({"java:S112", "java:S1452"}) // as Caffeine's loaders: any exception, any subtypes
    interface AsyncBulkLoader<K, V> {
        CompletableFuture<? extends Map<? extends K, ? extends V>> loadAll(Set<? extends K> keys) throws Exception;
    }

    @SuppressWarnings({"java:S112", "java:S1452"}) // as Caffeine's loaders: any exception, any subtypes
    interface AsyncReloader<K, V> {
        CompletableFuture<? extends @Nullable V> reload(K key, V oldValue) throws Exception;
    }

    /**
     * The loaders of a loading cache: the synchronous forms for the synchronous paths (which run in the caller's
     * thread), the asynchronous forms, if the cache was built asynchronously, for the asynchronous paths and the
     * loads Caffeine runs itself.
     */
    record Loaders<K, V>(SingleLoader<K, V> single, Reloader<K, V> reloader,
                         @Nullable AsyncSingleLoader<K, V> asyncSingle, @Nullable AsyncBulkLoader<K, V> asyncBulk,
                         @Nullable AsyncReloader<K, V> asyncReloader) {
    }

    // runs a step in the current thread: the synchronous paths chain their steps like the asynchronous ones
    private static final Executor INLINE = Runnable::run;

    // a decision on the current value of a key (null: none); returning null removes it, returning the current value
    // unchanged writes nothing (unless the update always writes)
    interface Remapping<K, V> {
        @Nullable V apply(K key, @Nullable V current);
    }

    // what an atomic update found and left
    record Update<V>(@Nullable V oldValue, @Nullable V newValue) {
    }

    private final Store store;
    private final InternalHasher<K> hasher;
    private final InternalSerializer<K> keySerializer;
    private final InternalSerializer<V> valueSerializer;
    private final Duration maximumAge;
    private final Executor storeExecutor;
    private volatile @Nullable Duration refreshAfterWrite;
    private final Runnable onClose;
    private final AtomicInteger epoch = new AtomicInteger();
    private final AtomicInteger revalidations = new AtomicInteger();
    // the highest cut-off this instance knows of (the stamp of the last invalidateAll: every read reports it, and so do
    // its own calls and the INVALIDATED_ALL messages): what an operation based on a held value compares with
    private final AtomicLong knownCutoff = new AtomicLong();
    private final AsyncCache<InternalKey<K>, InternalValue<V>> l1;
    private final @Nullable SingleLoader<K, V> loader;
    private final @Nullable AsyncSingleLoader<K, V> asyncLoader;
    private final @Nullable AsyncBulkLoader<K, V> asyncBulkLoader;
    private final DistributionMode distributionMode;
    // recent loads that found nothing, by hash, if absent caching is on
    private final @Nullable Cache<String, Boolean> absent;
    // the synchronous loads in flight by the thread running them, and the load each waiting thread waits for (see
    // awaitLoad)
    private final Map<InternalKey<K>, Thread> loadOwners = new ConcurrentHashMap<>();
    private final Map<Thread, InternalKey<K>> waitingFor = new ConcurrentHashMap<>();
    // ordering rule 4: the highest version announced for a key while its load was in flight
    private final Map<String, Long> minimumVersions = new ConcurrentHashMap<>();
    // ordering rule 4 for writes: the writes in flight per hash, and the highest version announced meanwhile, so
    // that a write committing to L1 after a newer write elsewhere was announced does not keep its older value
    private final Map<String, Integer> writing = new ConcurrentHashMap<>();
    private final Map<String, Long> writeMinimums = new ConcurrentHashMap<>();
    // null without distribution (DistributionMode.NONE): no subscription at all
    private final @Nullable InternalDelivery delivery;
    private final @Nullable ChangeSubscription subscription;
    private final InternalStatsCounter stats;
    // Caffeine's stats counter of L1, for what Caffeine counts but the core does not run through it
    private final InternalCaffeineStats caffeineStats;
    private final InternalStoreGuard guard;
    private final long maximumSize;
    private final Duration maintenanceInterval;
    private volatile boolean subscriptionUp = true;
    // null without distribution, like the subscription
    private final @Nullable InternalHeartbeat heartbeat;
    private final Duration heartbeatInterval;
    private final AtomicReference<@Nullable ScheduledFuture<?>> maintenanceTask = new AtomicReference<>();
    private final AtomicReference<@Nullable ScheduledFuture<?>> heartbeatTask = new AtomicReference<>();
    // supervising the subscription: whether resume attempts are under way, the losses reported so far (a loss during
    // an attempt calls for another), the wait before the next attempt, and its task
    private final AtomicBoolean resuming = new AtomicBoolean();
    private final AtomicLong losses = new AtomicLong();
    private volatile long resumeDelayMillis = RESUME_MIN_MILLIS;
    private final AtomicReference<@Nullable ScheduledFuture<?>> resumeTask = new AtomicReference<>();
    private volatile boolean closed;

    InternalCore(Store store, Caffeine<InternalKey<K>, InternalValue<V>> caffeine, InternalHasher<K> hasher,
                 InternalSerializer<K> keySerializer, InternalSerializer<V> valueSerializer, Duration maximumAge,
                 Executor storeExecutor, @Nullable Duration refreshAfterWrite, @Nullable Loaders<K, V> loaders,
                 DistributionMode distributionMode,
                 @Nullable Duration absentCachingExpiry, long maximumSize, Duration maintenanceInterval,
                 Duration heartbeatInterval, InternalStatsCounter stats, Runnable onClose) {
        this.stats = requireNonNull(stats);
        this.guard = new InternalStoreGuard(stats);
        this.store = requireNonNull(store);
        this.hasher = requireNonNull(hasher);
        this.keySerializer = requireNonNull(keySerializer);
        this.valueSerializer = requireNonNull(valueSerializer);
        this.maximumAge = requireNonNull(maximumAge);
        this.storeExecutor = requireNonNull(storeExecutor);
        this.loader = loaders == null ? null : loaders.single();
        this.onClose = requireNonNull(onClose);
        this.refreshAfterWrite = refreshAfterWrite;
        this.l1 = loaders == null ? caffeine.buildAsync() : caffeine.buildAsync(new Loader(loaders));
        this.caffeineStats = CaffeineConfigurer.takeCreatedStatsCounter();
        this.asyncLoader = loaders == null ? null : loaders.asyncSingle();
        this.asyncBulkLoader = loaders == null ? null : asyncBulkLoader(loaders);
        this.distributionMode = requireNonNull(distributionMode);
        this.maximumSize = maximumSize;
        this.maintenanceInterval = requireNonNull(maintenanceInterval);
        this.heartbeatInterval = requireNonNull(heartbeatInterval);
        this.absent = absentCachingExpiry == null ? null
                : Caffeine.newBuilder().expireAfterWrite(absentCachingExpiry).maximumSize(100_000).build();
        // subscribed before the first read, so every change after it reaches this instance
        if (distributionMode == DistributionMode.NONE) {
            this.delivery = null;
            this.subscription = null;
            this.heartbeat = null;
        } else if (store instanceof ChangeFeed feed) {
            this.heartbeat = new InternalHeartbeat(this::beat, this::silenced, this::gap);
            this.delivery = new InternalDelivery(storeExecutor, this::apply, this::gap, this::lost);
            this.subscription = feed.subscribe(delivery);
            if (losses.get() > 0) {
                superviseResume(); // lost while subscribing, before the subscription could be resumed
            }
        } else {
            throw new IllegalArgumentException("a store without a change feed supports only DistributionMode.NONE");
        }
        scheduleMaintenance();
        scheduleHeartbeat();
    }

    // keys and lifecycle

    InternalKey<K> wrap(K key) {
        checkOpen();
        return hasher.wrap(key);
    }

    void checkOpen() {
        if (closed) {
            throw new IllegalStateException("The cache is closed");
        }
    }

    void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (AtomicReference<@Nullable ScheduledFuture<?>> scheduled : List.of(maintenanceTask, heartbeatTask,
                resumeTask)) {
            ScheduledFuture<?> task = scheduled.get();
            if (task != null) {
                task.cancel(false);
            }
        }
        try {
            if (delivery != null) {
                delivery.close();
            }
            if (subscription != null) {
                subscription.close();
            }
            store.close();
        } finally {
            l1.synchronous().invalidateAll();
            onClose.run();
        }
    }

    AsyncCache<InternalKey<K>, InternalValue<V>> l1() {
        return l1;
    }

    DistributedStats stats() {
        return stats.snapshot();
    }

    // whether absent caching remembers the key (tests)
    boolean isAbsent(K key) {
        return absent != null && absent.getIfPresent(wrap(key).hash()) != null;
    }

    // nothing delivered left to process and no background revalidation running (tests)
    boolean isQuiescent() {
        return (delivery == null || delivery.isIdle()) && revalidations.get() == 0;
    }

    // reads

    /**
     * Returns the held value if its load has completed, otherwise reads it through the store (never the loader), as
     * Caffeine's {@code getIfPresent} answers without loading. A load in flight answers {@code null}, as in Caffeine.
     */
    @Nullable V getIfPresent(K key) {
        return getIfPresent(key, true);
    }

    /**
     * {@code getIfPresent}, counted in Caffeine's stats or not: {@code asMap().get} is not, as in Caffeine.
     */
    @Nullable V getIfPresent(K key, boolean recordStats) {
        InternalKey<K> internalKey = wrap(key);
        // looked at quietly first, so that Caffeine's stats count a held key here and any other once, in readThrough
        CompletableFuture<InternalValue<V>> held = null;
        if (l1.asMap().containsKey(internalKey)) {
            held = recordStats ? l1.getIfPresent(internalKey) : l1.asMap().get(internalKey);
        }
        if (held != null) {
            if (!held.isDone()) {
                // a load in flight here: its value may already be in the store, seen by a reader before this one,
                // so the store answers (Caffeine answers null, its load being invisible until it completes); counted
                // already
                return readThrough(List.of(internalKey), false).get(internalKey);
            }
            if (held.isCompletedExceptionally()) {
                return null;
            }
            InternalValue<V> confirmed = confirmOrDrop(internalKey, held);
            if (confirmed != null) {
                return confirmed.value();
            }
        }
        return readThrough(List.of(internalKey), recordStats).get(internalKey);
    }

    /**
     * {@code getIfPresent} for keys not held: one store read, and what it finds is held. Counted in Caffeine's stats
     * as a hit or a miss per key, never as a load (Caffeine's {@code getIfPresent} never loads). The keys are writes
     * in flight meanwhile (the write registry), so a message announced between the read and holding its result
     * drops it again; a load in flight for a key is left alone. A store that does not answer is a miss.
     */
    private Map<InternalKey<K>, V> readThrough(List<InternalKey<K>> internalKeys) {
        return readThrough(internalKeys, true);
    }

    private Map<InternalKey<K>, V> readThrough(List<InternalKey<K>> internalKeys, boolean recordStats) {
        Map<InternalKey<K>, V> found = new LinkedHashMap<>();
        List<InternalKey<K>> toRead = new ArrayList<>();
        for (InternalKey<K> internalKey : internalKeys) {
            if (absent != null && absent.getIfPresent(internalKey.hash()) != null) {
                stats.increment(Counter.ABSENT_HIT);
            } else {
                toRead.add(internalKey);
            }
        }
        if (!toRead.isEmpty()) {
            int readEpoch = epoch.get();
            toRead.forEach(this::beginWrite);
            try {
                ReadResult read = readRows(hashes(toRead));
                Map<String, Row> rows = read.rows();
                for (InternalKey<K> internalKey : toRead) {
                    Row row = rows.get(internalKey.hash());
                    V value = valueOf(row);
                    if (row == null || value == null) {
                        stats.increment(Counter.STORE_MISS);
                        if (row != null) {
                            // no value at the row's version: L1 may be behind it (a removal in flight here, or a row
                            // hidden at its value's version by an invalidateAll here not yet applied to L1), and no
                            // later read here may answer older
                            dropOlder(internalKey, row.version() + 1);
                        }
                        continue;
                    }
                    stats.increment(Counter.STORE_HIT);
                    hold(internalKey, new InternalValue<>(value, row.version(), readEpoch, read.cutoff()));
                    found.put(internalKey, value);
                    if (needsRefresh(row.age())) {
                        refreshLater(internalKey);
                    }
                }
            } catch (StoreException e) {
                // a miss, as Caffeine's getIfPresent answers without loading
            } finally {
                toRead.forEach(this::endWrite);
            }
        }
        if (recordStats) {
            countLookups(internalKeys);
        }
        return found;
    }

    // counts each key once in Caffeine's stats: a hit if held now, a miss otherwise
    @SuppressWarnings({"CheckReturnValue", "FutureReturnValueIgnored", "ResultOfMethodCallIgnored"}) // for the stats
    private void countLookups(List<InternalKey<K>> internalKeys) {
        internalKeys.forEach(l1::getIfPresent);
    }

    // holds a value read from the store: unless a completed value at least as new is held, or a load is in flight
    private void hold(InternalKey<K> internalKey, InternalValue<V> value) {
        install(internalKey, value, true, false);
    }

    /**
     * Puts a value into L1 under the version rule (ordering rules 2 and 3): a completed value at least as new is
     * kept, an older, failed or empty one replaced; a missing key is added only if {@code addIfMissing}, a load in
     * flight replaced only if {@code overInFlight}. Compare-and-swap on {@code putIfAbsent} / {@code replace}, retried
     * on a race, instead of {@code compute}: on an {@code AsyncCache}'s map Caffeine counts a {@code compute} as a
     * load in its stats (measured), and a put or a held store value is not a load. Nothing is put if a removal or
     * message newer than the value came while it was in flight (the write registry's minimum): under the registry's
     * lock of the key, so that a removal of this instance either is seen here or finds the value in L1, never leaving
     * a reader a window in which a value older than it is held.
     */
    private void install(InternalKey<K> internalKey, InternalValue<V> value, boolean addIfMissing,
                         boolean overInFlight) {
        writing.compute(internalKey.hash(), (hash, count) -> {
            Long minimum = writeMinimums.get(hash);
            if (minimum == null || value.version() >= minimum) {
                installNow(internalKey, value, addIfMissing, overInFlight);
            }
            return count;
        });
    }

    @SuppressWarnings("ReferenceEquality") // the same value instance put again replaces itself silently
    private void installNow(InternalKey<K> internalKey, InternalValue<V> value, boolean addIfMissing,
                            boolean overInFlight) {
        value.bind(internalKey);
        CompletableFuture<InternalValue<V>> fresh = CompletableFuture.completedFuture(value);
        while (true) {
            CompletableFuture<InternalValue<V>> current = l1.asMap().get(internalKey);
            InternalValue<V> currentValue = completedValue(current);
            if (currentValue != null) {
                value.bindLike(currentValue); // a lookup key (population): the held value knows the key
            }
            if (current == null) {
                if (!addIfMissing || l1.asMap().putIfAbsent(internalKey, fresh) == null) {
                    return;
                }
                continue;
            }
            if (!current.isDone()) {
                if (!overInFlight) {
                    return;
                }
            } else if (currentValue != null && currentValue.version() >= value.version()) {
                return;
            }
            InternalValue<V> byItself = currentValue != null && currentValue.value() == value.value()
                    ? currentValue : null;
            if (byItself != null) {
                byItself.replacementSilent(true);
            }
            if (l1.asMap().replace(internalKey, current, fresh)) {
                return;
            }
            if (byItself != null) {
                byItself.replacementSilent(false);
            }
        }
    }

    @Nullable V get(K key, SingleLoader<K, V> singleLoader) {
        InternalValue<V> value = getOrLoad(wrap(key), requireNonNull(singleLoader));
        return value == null ? null : value.value();
    }

    @Nullable V get(K key) {
        return get(key, requireNonNull(loader, "not a loading cache"));
    }

    Map<K, V> getAllPresent(Iterable<? extends K> keys) {
        checkOpen();
        List<InternalKey<K>> requested = wrapAll(keys);
        confirmOrDropAll(requested);
        Map<K, V> result = new LinkedHashMap<>();
        List<InternalKey<K>> missing = new ArrayList<>();
        for (InternalKey<K> internalKey : requested) {
            // as in getIfPresent: a miss is counted once, by the read through the store
            CompletableFuture<InternalValue<V>> held = l1.asMap().containsKey(internalKey)
                    ? l1.getIfPresent(internalKey) : null;
            if (held == null) {
                missing.add(internalKey);
            } else {
                InternalValue<V> value = completedValue(held);
                if (value != null) {
                    result.put(internalKey.key(), value.value());
                }
            }
        }
        if (!missing.isEmpty()) {
            Map<InternalKey<K>, V> found = readThrough(missing);
            Map<K, V> ordered = new LinkedHashMap<>();
            for (InternalKey<K> internalKey : requested) {
                if (result.containsKey(internalKey.key())) {
                    ordered.put(internalKey.key(), result.get(internalKey.key()));
                } else if (found.containsKey(internalKey)) {
                    ordered.put(internalKey.key(), found.get(internalKey));
                }
            }
            return ordered;
        }
        return result;
    }

    Map<K, V> getAll(Iterable<? extends K> keys, BulkLoader<K, V> bulkLoader) {
        checkOpen();
        List<InternalKey<K>> requested = wrapAll(keys);
        confirmOrDropAll(requested);
        Map<InternalKey<K>, InternalValue<V>> values = getAllOrLoad(requested, requireNonNull(bulkLoader));
        Map<K, V> result = new LinkedHashMap<>();
        for (InternalKey<K> internalKey : requested) {
            InternalValue<V> value = values.get(internalKey);
            if (value != null) {
                result.put(internalKey.key(), value.value());
            }
        }
        return result;
    }

    @Nullable StoredValue<V> getFromStore(K key) {
        InternalKey<K> internalKey = wrap(key);
        Row row = readRows(List.of(internalKey.hash())).rows().get(internalKey.hash());
        return row == null || row.state() != State.VALUE ? null
                : new StoredValue<>(valueSerializer.deserialize(requireNonNull(row.value())), row.age());
    }

    Map<K, StoredValue<V>> getAllFromStore(Iterable<? extends K> keys) {
        checkOpen();
        List<InternalKey<K>> requested = wrapAll(keys);
        Map<String, Row> rows = readRows(hashes(requested)).rows();
        Map<K, StoredValue<V>> result = new LinkedHashMap<>();
        for (InternalKey<K> internalKey : requested) {
            Row row = rows.get(internalKey.hash());
            if (row != null && row.state() == State.VALUE) {
                result.put(internalKey.key(),
                        new StoredValue<>(valueSerializer.deserialize(requireNonNull(row.value())), row.age()));
            }
        }
        return result;
    }

    // writes

    void put(K key, V value) {
        InternalKey<K> internalKey = wrap(key);
        requireNonNull(value, "value cannot be null");
        // taken before the write: an invalidateAll() landing in between leaves the value untrusted
        int writeEpoch = epoch.get();
        beginWrite(internalKey);
        try {
            long version = requireNonNull(write(List.of(internalKey), List.of(entry(internalKey, value)))
                    .get(internalKey.hash()));
            commit(internalKey, new InternalValue<>(value, version, writeEpoch));
        } finally {
            endWrite(internalKey);
        }
    }

    private void forgetAbsence(String hash) {
        if (absent != null) {
            absent.invalidate(hash);
        }
    }

    void putAll(Map<? extends K, ? extends V> map) {
        checkOpen();
        Map<InternalKey<K>, V> values = new LinkedHashMap<>();
        map.forEach((key, value) -> values.put(hasher.wrap(requireNonNull(key)), requireNonNull(value)));
        if (values.isEmpty()) {
            return;
        }
        List<Entry> entries = new ArrayList<>();
        values.forEach((internalKey, value) -> entries.add(entry(internalKey, value)));
        int writeEpoch = epoch.get();
        values.keySet().forEach(this::beginWrite);
        try {
            Map<String, Long> versions = write(new ArrayList<>(values.keySet()), entries);
            values.forEach((internalKey, value) ->
                    commit(internalKey, new InternalValue<>(value, requireNonNull(versions.get(internalKey.hash())),
                            writeEpoch)));
        } finally {
            values.keySet().forEach(this::endWrite);
        }
    }

    /**
     * The user's remapping function of {@code compute}, {@code computeIfPresent} or {@code merge}, counted in Caffeine's
     * stats as Caffeine counts it: a load, failed if it returns {@code null} or throws. Each run counts (it may run
     * more than once after a conflict).
     */
    @SuppressWarnings("java:S1181") // an error is recorded as a failed load too, then rethrown
    <A, B extends @Nullable Object> BiFunction<A, B, @Nullable V> countedAsLoad(
            BiFunction<? super A, ? super B, ? extends @Nullable V> remappingFunction) {
        return (first, second) -> {
            long start = System.nanoTime();
            V value;
            try {
                value = remappingFunction.apply(first, second);
            } catch (RuntimeException | Error e) {
                caffeineStats.recordLoadFailure(System.nanoTime() - start);
                throw e;
            }
            if (value == null) {
                caffeineStats.recordLoadFailure(System.nanoTime() - start);
            } else {
                caffeineStats.recordLoadSuccess(System.nanoTime() - start);
            }
            return value;
        };
    }

    /**
     * The atomic {@code asMap()} methods: reads the current value through L1 (if trusted) and L2, decides, writes
     * conditionally on the version read (a removal as a conditional tombstone), and on a conflict decides again on
     * what the conflict returned. Atomic across instances; the remapping may run more than once.
     */
    Update<V> update(K key, Remapping<K, V> remapping) {
        return update(key, remapping, false);
    }

    // alwaysWrite: a value equal to the current one is written too (asMap().put renews the row), and a removal writes
    // a tombstone even where there is no value (asMap().remove(k) moves the version, so that a load in flight
    // conflicts, as Caffeine's remove discards a load in flight)
    @SuppressWarnings("ReferenceEquality") // the same instance returned leaves the value unchanged, as in Caffeine
    Update<V> update(K key, Remapping<K, V> remapping, boolean alwaysWrite) {
        InternalKey<K> internalKey = wrap(key);
        String hash = internalKey.hash();
        beginWrite(internalKey);
        try {
            Current<V> current = heldOrRead(internalKey);
            boolean missingConflicted = false;
            while (true) {
                V newValue = remapping.apply(internalKey.key(), current.value());
                if (current.held() && (newValue == null || (newValue == current.value() && !alwaysWrite))) {
                    // decided on a held value, but not to write one: nothing would check it (a removal's tombstone
                    // is conditioned on the version only), so checked by a read. The same version under the same
                    // cut-off confirms the decision (the remapping, counted as a load, does not run again; the held
                    // instance kept, as Caffeine tells an unchanged value by identity); otherwise decided again
                    Current<V> read = read(internalKey);
                    if (read.version() != current.version() || read.cutoff() != current.cutoff()) {
                        current = read;
                        continue;
                    }
                    current = new Current<>(current.value(), read.version(), read.cutoff(), read.epoch(), false);
                }
                if (newValue == current.value() && !alwaysWrite) {
                    // nothing to write, but L1 may be behind what the read found (a write in flight here): brought up
                    // to it, so that no later read here answers older
                    catchUp(internalKey, current);
                    return new Update<>(current.value(), newValue);
                }
                try {
                    if (newValue == null) {
                        long expected = current.version() == 0 ? ExpectedEntry.MISSING : current.version();
                        // a row the read did not show and no value: a value too old to count, invalidated as it is
                        Long version = missingConflicted && current.version() == 0
                                ? guard.write(() -> store.invalidate(List.of(hash))).get(hash)
                                : guard.write(() -> store.invalidateIfUnchanged(List.of(new Expected(hash, expected))))
                                .get(hash);
                        stats.increment(Counter.CONDITIONAL_WRITE);
                        if (version == null) {
                            stats.increment(Counter.CONFLICT);
                        }
                        if (version != null) {
                            dropOlder(internalKey, version);
                            return new Update<>(current.value(), null);
                        }
                        missingConflicted = current.version() == 0;
                        current = read(internalKey);
                    } else {
                        forgetAbsence(hash);
                        Entry entry = entry(internalKey, newValue);
                        WriteResult result = writeConditionally(current.version() == 0
                                ? ExpectedEntry.ifMissing(entry, current.cutoff())
                                : ExpectedEntry.ifVersion(entry, current.version(), current.cutoff()));
                        if (result.written()) {
                            // at the epoch of the read: after an invalidateAll here since, the value (hidden in the
                            // store) is checked on its next read
                            commit(internalKey, new InternalValue<>(newValue, result.version(), current.epoch(),
                                    current.cutoff()));
                            return new Update<>(current.value(), newValue);
                        }
                        // read again, for the cut-off of what the next decision is based on; except a value too old to
                        // count, which a read omits: decided on as no value, at its version, under the highest cut-off
                        // known (the epoch first: an invalidateAll here raises the cut-off before the epoch). A
                        // conflict at the version decided on is a refused basis (Store#checksBasis): read
                        current = result.current() == null && result.version() > 0
                                && result.version() != current.version() ? tooOld(result.version()) : read(internalKey);
                    }
                } catch (StoreException e) {
                    if (e.isOutcomeUnknown()) {
                        l1.synchronous().invalidate(internalKey);
                    }
                    throw e;
                }
            }
        } finally {
            endWrite(internalKey);
        }
    }

    // the hot path (VISION.md, "The decision's cut-off on every row", step 2): a trusted held value whose basis is
    // known, on a store that refuses a write decided under an older cut-off, is decided on as it is; a write is then
    // one store operation, refused if a write elsewhere (the version) or an invalidateAll not heard of yet (the basis)
    // came since. Otherwise from the store, whose read reports the cut-off the write then carries as its basis
    private Current<V> heldOrRead(InternalKey<K> internalKey) {
        if (store.checksBasis()) {
            int heldEpoch = epoch.get();
            InternalValue<V> held = completedValue(l1.asMap().get(internalKey));
            if (held != null && held.basis() != InternalValue.UNKNOWN_BASIS && isTrusted(held)) {
                return new Current<>(held.value(), held.version(), held.basis(), heldEpoch, true);
            }
        }
        return read(internalKey);
    }

    private Current<V> read(InternalKey<K> internalKey) {
        int readEpoch = epoch.get();
        ReadResult read = readRows(List.of(internalKey.hash()));
        Row row = read.rows().get(internalKey.hash());
        if (row == null) {
            return new Current<>(null, 0, read.cutoff(), readEpoch, false);
        }
        return new Current<>(valueOf(row), row.version(), read.cutoff(), readEpoch, false);
    }

    private void catchUp(InternalKey<K> internalKey, Current<V> current) {
        V value = current.value();
        if (value != null) {
            hold(internalKey, new InternalValue<>(value, current.version(), current.epoch(), current.cutoff()));
        } else if (current.version() > 0) {
            // no value at that version (a hidden row reads as a tombstone at its value's version): held at it is stale
            dropOlder(internalKey, current.version() + 1);
        }
    }

    private Current<V> tooOld(long version) {
        int readEpoch = epoch.get();
        return new Current<>(null, version, knownCutoff.get(), readEpoch, false);
    }

    // what an update decides on, held or read from the store (version 0: no row; a version without a value: a
    // tombstone, an expired row, an unreadable value, or a value too old to count), the cut-off it is known visible
    // under (the write's basis), the epoch before it was looked at (the written value's in L1), and whether it is held
    private record Current<V>(@Nullable V value, long version, long cutoff, int epoch, boolean held) {
    }

    private void beginWrite(InternalKey<K> internalKey) {
        writing.merge(internalKey.hash(), 1, Integer::sum);
    }

    // after the commit to L1: a newer version announced while the write was in flight drops the committed value
    private void endWrite(InternalKey<K> internalKey) {
        writing.compute(internalKey.hash(), (hash, count) -> {
            Long minimum = writeMinimums.get(hash);
            if (minimum != null) {
                removeIfOlder(internalKey, minimum);
            }
            if (count == null || count == 1) {
                writeMinimums.remove(hash);
                return null;
            }
            return count - 1;
        });
    }

    /**
     * {@code asMap().containsKey}: L1 if held and trusted; otherwise an existence check in the store (versions only,
     * nothing is held), which also confirms or drops an unconfirmed copy. A store that does not answer counts as a
     * miss, as for {@code getIfPresent}.
     */
    boolean containsKey(K key) {
        InternalKey<K> internalKey = wrap(key);
        CompletableFuture<InternalValue<V>> held = l1.asMap().get(internalKey);
        if (held != null && completedValue(held) != null && confirmOrDrop(internalKey, held) != null) {
            return true;
        }
        if (absent != null && absent.getIfPresent(internalKey.hash()) != null) {
            return false;
        }
        try {
            return readVisibleVersions(List.of(internalKey.hash()))
                    .containsKey(internalKey.hash());
        } catch (StoreException e) {
            return false;
        }
    }

    // policy().refreshAfterWrite().setRefreshesAfter(...) changes it for L1, so refresh rule 2 follows
    void refreshAfterWrite(Duration refreshAfterWrite) {
        this.refreshAfterWrite = refreshAfterWrite;
    }

    @Nullable Duration refreshAfterWrite() {
        return refreshAfterWrite;
    }

    void invalidate(K key) {
        InternalKey<K> internalKey = wrap(key);
        try {
            dropOlder(internalKey, requireNonNull(guard.write(() -> store.invalidate(List.of(internalKey.hash())))
                    .get(internalKey.hash())));
        } finally {
            // dropped locally whatever the store answered: dropping is always safe
            l1.synchronous().invalidate(internalKey);
        }
    }

    void invalidateAll(Iterable<? extends K> keys) {
        checkOpen();
        List<InternalKey<K>> internalKeys = wrapAll(keys);
        if (internalKeys.isEmpty()) {
            return;
        }
        try {
            Map<String, Long> versions = guard.write(() -> store.invalidate(hashes(internalKeys)));
            internalKeys.forEach(internalKey -> dropOlder(internalKey,
                    requireNonNull(versions.get(internalKey.hash()))));
        } finally {
            l1.synchronous().invalidateAll(internalKeys);
        }
    }

    void invalidateAll() {
        checkOpen();
        try {
            // once: a write decided before it, whenever it commits, is hidden by its basis (VISION.md, "The decision's
            // cut-off on every row")
            knownCutoff.accumulateAndGet(guard.write(store::invalidateAll), Math::max);
        } finally {
            // a load started before completes with an older epoch, so it is checked on its next read
            epoch.incrementAndGet();
            l1.synchronous().invalidateAll();
        }
    }

    // an unconditional write; with an unknown outcome the keys are dropped from L1 (ordering rule 5)
    private Map<String, Long> write(List<InternalKey<K>> internalKeys, List<Entry> entries) {
        internalKeys.forEach(internalKey -> forgetAbsence(internalKey.hash()));
        try {
            Map<String, Long> versions = guard.put(() -> store.put(entries));
            stats.add(Counter.PUT, entries.size());
            return versions;
        } catch (StoreException e) {
            if (e.isOutcomeUnknown()) {
                l1.synchronous().invalidateAll(internalKeys);
            }
            throw e;
        }
    }


    // puts the value into L1 unless L1 holds a completed value at least as new (ordering rule 3)
    private void commit(InternalKey<K> internalKey, InternalValue<V> value) {
        install(internalKey, value, true, true);
    }

    // single key: the owner of the load runs it in its own thread, outside Caffeine's lock

    // a future of L1 completes with null where nothing was found (see joinHeld)
    @SuppressWarnings({"unchecked", "java:S2589"})
    private @Nullable InternalValue<V> getOrLoad(InternalKey<K> internalKey, @Nullable SingleLoader<K, V> singleLoader) {
        @Nullable CompletableFuture<InternalValue<V>>[] owned = new CompletableFuture[1];
        CompletableFuture<InternalValue<V>> future = l1.get(internalKey, (key, executor) -> {
            CompletableFuture<InternalValue<V>> created = new CompletableFuture<>();
            owned[0] = created;
            return created;
        });
        CompletableFuture<InternalValue<V>> created = owned[0];
        if (created == null) {
            if (!future.isDone()) {
                if (LOADING.get().contains(internalKey)) {
                    throw new IllegalStateException("Recursive load of " + internalKey);
                }
                return awaitLoad(internalKey, future, singleLoader);
            }
            InternalValue<V> value = joinHeld(future);
            if (value == null) {
                return null;
            }
            InternalValue<V> confirmed = confirmOrDrop(internalKey, future);
            return confirmed != null ? confirmed : getOrLoad(internalKey, singleLoader);
        }
        // every step inline: the load has completed when runLoad returns
        Thread current = Thread.currentThread();
        loadOwners.put(internalKey, current);
        try {
            runLoad(internalKey, created, singleLoader == null ? null : inline(singleLoader), INLINE);
        } finally {
            loadOwners.remove(internalKey, current);
        }
        return joinHeld(future);
    }

    // waits for another thread's load of the key, unless that load (through what its owner waits for, and so on)
    // waits for this thread: waiting would then close a cycle and hang every thread in it, so the source is asked
    // here instead, and the answer neither held nor written: the owner's load holds and writes its own (the store has
    // no value, or the owner's load would not be waiting)
    private @Nullable InternalValue<V> awaitLoad(InternalKey<K> internalKey, CompletableFuture<InternalValue<V>> future,
                                                 @Nullable SingleLoader<K, V> singleLoader) {
        Thread current = Thread.currentThread();
        waitingFor.put(current, internalKey);
        try {
            if (closesCycle(internalKey, current)) {
                V value = singleLoader == null ? null
                        : join(call(() -> inline(singleLoader).load(internalKey.key())));
                return value == null ? null : new InternalValue<>(value, 1, UNCONFIRMED);
            }
            return joinHeld(future);
        } finally {
            waitingFor.remove(current, internalKey);
        }
    }

    // whether the owners of the loads waited for, followed from the key, lead back to the given thread
    private boolean closesCycle(InternalKey<K> internalKey, Thread current) {
        InternalKey<K> key = internalKey;
        for (int hops = 0; hops <= waitingFor.size(); hops++) {
            Thread owner = loadOwners.get(key);
            if (owner == null) {
                return false;
            }
            if (owner.equals(current)) {
                return true;
            }
            key = waitingFor.get(owner);
            if (key == null) {
                return false;
            }
        }
        return false;
    }

    // the owner's part of a load: its steps in the caller's thread for the synchronous caches (INLINE), on the store
    // executor for the asynchronous ones
    // the callback completing the owned future is attached for its effect; whether L1 still holds that very future
    @SuppressWarnings({"FutureReturnValueIgnored", "ReferenceEquality"})
    private void runLoad(InternalKey<K> internalKey, CompletableFuture<InternalValue<V>> owned,
                         @Nullable AsyncSingleLoader<K, V> singleLoader, Executor steps) {
        Set<InternalKey<?>> loading = LOADING.get();
        loading.add(internalKey);
        CompletableFuture<LoadResult<V>> result;
        try {
            result = load(internalKey, singleLoader, owned, steps);
        } finally {
            loading.remove(internalKey);
        }
        result.whenComplete((loaded, failure) -> {
            if (failure != null) {
                owned.completeExceptionally(cause(failure));
            } else {
                if (loaded.value == null) {
                    completeAbsent(owned);
                } else {
                    owned.complete(loaded.value.bind(internalKey));
                }
                if (!loaded.cacheable) {
                    l1.asMap().remove(internalKey, owned);
                } else if (loaded.value != null && l1.asMap().get(internalKey) != owned) {
                    // replaced while in flight (a write's commit replaces a load): the loaded value may be the newer
                    // one, read from the store after that write's; held under the version rule, a removal kept
                    install(internalKey, loaded.value, false, false);
                }
                if (loaded.cacheable && loaded.value != null && needsRefresh(loaded.age)) {
                    refreshLater(internalKey);
                }
            }
        });
    }

    // asynchronous reads: a trusted hit or a load in flight is answered with L1's future at once; a miss loads with
    // its store steps on the store executor and waits for the user's future without holding a thread; an untrusted
    // value is checked on the store executor

    CompletableFuture<@Nullable V> getAsync(K key, @Nullable AsyncSingleLoader<K, V> singleLoader) {
        return unwrap(getOrLoadAsync(wrap(key), singleLoader));
    }

    // AsyncCache.getIfPresent: a held future at once, otherwise the synchronous path on the store executor
    CompletableFuture<@Nullable V> getIfPresentAsync(K key) {
        InternalKey<K> internalKey = wrap(key);
        if (l1.asMap().containsKey(internalKey)) {
            CompletableFuture<InternalValue<V>> held = l1.getIfPresent(internalKey);
            if (held != null) {
                if (!held.isDone()) {
                    return unwrap(held);
                }
                InternalValue<V> value = held.isCompletedExceptionally() ? null : held.join();
                if (value != null && (isTrusted(value) || guard.isOpen())) {
                    return unwrap(held);
                }
                if (value != null) {
                    return supply(() -> {
                        InternalValue<V> confirmed = confirmOrDrop(internalKey, held);
                        return confirmed != null ? confirmed.value()
                                : readThrough(List.of(internalKey)).get(internalKey);
                    });
                }
            }
        }
        return supply(() -> readThrough(List.of(internalKey)).get(internalKey));
    }

    CompletableFuture<@Nullable V> getAsync(K key) {
        return getAsync(key, requireNonNull(asyncLoader, "not an asynchronous loading cache"));
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<InternalValue<V>> getOrLoadAsync(InternalKey<K> internalKey,
                                                               @Nullable AsyncSingleLoader<K, V> singleLoader) {
        @Nullable CompletableFuture<InternalValue<V>>[] owned = new CompletableFuture[1];
        CompletableFuture<InternalValue<V>> future = l1.get(internalKey, (key, executor) -> {
            CompletableFuture<InternalValue<V>> created = new CompletableFuture<>();
            owned[0] = created;
            return created;
        });
        CompletableFuture<InternalValue<V>> created = owned[0];
        if (created != null) {
            runLoad(internalKey, created, singleLoader, storeExecutor);
            return future;
        }
        if (!future.isDone() || future.isCompletedExceptionally()) {
            return future;
        }
        InternalValue<V> value = completedValue(future);
        if (value == null || isTrusted(value) || guard.isOpen()) {
            return future;
        }
        return this.<@Nullable InternalValue<V>>supply(() -> confirmOrDrop(internalKey, future))
                .thenCompose(confirmed -> confirmed != null ? CompletableFuture.completedFuture(confirmed)
                        : getOrLoadAsync(internalKey, singleLoader));
    }

    static <V> CompletableFuture<@Nullable V> unwrap(CompletableFuture<InternalValue<V>> future) {
        return future.thenApply(InternalValue::valueOrNull);
    }

    CompletableFuture<Map<K, V>> getAllAsync(Iterable<? extends K> keys) {
        return getAllAsync(keys, requireNonNull(asyncBulkLoader, "not an asynchronous loading cache"));
    }

    /**
     * {@code AsyncCache.getAll}: answered at once if every key is held, completed and trusted; otherwise untrusted
     * held values are checked on the store executor, keys in flight are waited for without a thread (Caffeine's
     * {@code getAll}), and the rest are loaded in bulk like single keys. Without a bulk loader, only what is present
     * is returned.
     */
    CompletableFuture<Map<K, V>> getAllAsync(Iterable<? extends K> keys, @Nullable AsyncBulkLoader<K, V> bulkLoader) {
        checkOpen();
        List<InternalKey<K>> requested = wrapAll(keys);
        Map<K, V> present = new LinkedHashMap<>();
        boolean untrusted = false;
        boolean complete = true;
        for (InternalKey<K> internalKey : requested) {
            CompletableFuture<InternalValue<V>> held = l1.asMap().get(internalKey);
            InternalValue<V> value = held == null || !held.isDone() || held.isCompletedExceptionally() ? null
                    : held.join();
            if (value == null) {
                complete = false;
            } else if (!isTrusted(value)) {
                untrusted = true;
                complete = false;
            } else {
                present.put(internalKey.key(), value.value());
            }
        }
        if (complete) {
            caffeineStats.recordHits(present.size()); // not through Caffeine, which counts each key held as a hit
            return CompletableFuture.completedFuture(present);
        }
        if (bulkLoader == null) {
            List<K> userKeys = requested.stream().map(InternalKey::key).toList();
            return supply(() -> getAllPresent(userKeys));
        }
        CompletableFuture<@Nullable Void> checked = untrusted && !guard.isOpen()
                ? this.<@Nullable Void>supply(() -> {
                    confirmOrDropAll(requested);
                    return null;
                })
                : CompletableFuture.completedFuture(null);
        return checked.thenCompose(ignored -> getAllOrLoad(requested, bulkLoader, storeExecutor)).thenApply(values -> {
            Map<K, V> result = new LinkedHashMap<>();
            for (InternalKey<K> internalKey : requested) {
                InternalValue<V> value = values.get(internalKey);
                if (value != null) {
                    result.put(internalKey.key(), value.value());
                }
            }
            return result;
        });
    }

    /**
     * {@code AsyncCache.put(key, future)}: the future is held at once (readers wait on it, messages treat it as a
     * load in flight); when it completes, the value is written to L2 unconditionally and the held future completes
     * with its version. A failed future or {@code null} is removed, as in Caffeine, where the put has replaced the
     * previous value at once: so it writes a tombstone, and the previous value is gone everywhere. Two puts whose
     * futures complete in reverse order behave like two concurrent puts: the store's order decides.
     */
    // the identical instance is the point (see below); the callback writing the value is attached for its effect,
    // and an error fails the held future too
    @SuppressWarnings({"ReferenceEquality", "FutureReturnValueIgnored", "java:S1181"})
    void putAsync(K key, CompletableFuture<? extends V> valueFuture) {
        InternalKey<K> internalKey = wrap(key);
        requireNonNull(valueFuture, "valueFuture cannot be null");
        CompletableFuture<InternalValue<V>> owned = new CompletableFuture<>();
        // Caffeine counts no load for a future completed when put, but the held one is completed later
        boolean completedWhenPut = completedValue(valueFuture) != null;
        CompletableFuture<InternalValue<V>> previous = l1.asMap().get(internalKey);
        InternalValue<V> replaced = completedWhenPut && previous != null && previous.isDone()
                && !previous.isCompletedExceptionally() ? previous.join() : null;
        if (replaced != null && replaced.value() == valueFuture.join()) {
            // Caffeine reports no replacement of a completed future by one completed with the identical instance
            replaced.replacementSilent(true);
        }
        l1.put(internalKey, owned);
        valueFuture.whenComplete((value, failure) -> {
            if (failure != null || value == null) {
                storeExecutor.execute(() -> {
                    try {
                        guard.write(() -> store.invalidate(List.of(internalKey.hash())));
                    } catch (RuntimeException e) {
                        // the held future is removed anyway; the previous value may survive elsewhere
                    } finally {
                        if (failure != null) {
                            owned.completeExceptionally(failure);
                        } else {
                            completeAbsent(owned);
                        }
                    }
                });
            } else {
                // no write registry needed: the held future is in flight until it completes, so a message meanwhile
                // sets the minimum version it must reach (ordering rule 4)
                storeExecutor.execute(() -> {
                    int writeEpoch = epoch.get();
                    try {
                        long version = requireNonNull(write(List.of(internalKey), List.of(entry(internalKey, value)))
                                .get(internalKey.hash()));
                        if (completedWhenPut) {
                            caffeineStats.skipLoadSuccess();
                        }
                        owned.complete(new InternalValue<V>(value, version, writeEpoch).bind(internalKey));
                    } catch (RuntimeException | Error e) {
                        owned.completeExceptionally(e);
                    }
                });
            }
        });
    }

    /**
     * A load: reads the store, calls the source on a miss, writes conditionally on what it read; starts over after
     * an invalidation in between, at most ATTEMPTS times, after which the loaded value is returned but not cached.
     * The store steps run on {@code steps} (INLINE: in the current thread), the source is a future, so no thread
     * waits for it; with a synchronous loader (an already completed future) and INLINE steps, everything runs in the
     * caller's thread, as before.
     *
     * @param owned this load's future in L1, if it is the owner of one (see rememberAbsence)
     */
    private CompletableFuture<LoadResult<V>> load(InternalKey<K> internalKey,
                                                  @Nullable AsyncSingleLoader<K, V> singleLoader,
                                                  @Nullable CompletableFuture<InternalValue<V>> owned, Executor steps) {
        return load(internalKey, singleLoader, owned, steps, false);
    }

    // fromSource: a refresh (refresh rule 1), which reads the store only for the version to write on
    private CompletableFuture<LoadResult<V>> load(InternalKey<K> internalKey,
                                                  @Nullable AsyncSingleLoader<K, V> singleLoader,
                                                  @Nullable CompletableFuture<InternalValue<V>> owned, Executor steps,
                                                  boolean fromSource) {
        String hash = internalKey.hash();
        int loadEpoch = epoch.get();
        if (absent != null && absent.getIfPresent(hash) != null) {
            stats.increment(Counter.ABSENT_HIT);
            return CompletableFuture.completedFuture(LoadResult.absent());
        }
        return on(steps, () -> {
            try {
                ReadResult read = readRows(List.of(hash));
                return new Read(read.rows().get(hash), read.cutoff(), false);
            } catch (StoreException e) {
                return new Read(null, 0, true);
            }
        }).thenCompose(read -> {
            if (read.storeDown()) {
                return sourceOnly(internalKey, singleLoader);
            }
            Row row = read.row();
            V found = fromSource ? null : valueOf(row);
            if (row != null && found != null) {
                stats.increment(Counter.STORE_HIT);
                return CompletableFuture.completedFuture(
                        LoadResult.cached(new InternalValue<>(found, row.version(), loadEpoch, read.cutoff()),
                                row.age()));
            }
            if (!fromSource) {
                stats.increment(Counter.STORE_MISS);
            }
            return attempt(internalKey, singleLoader, owned, steps, loadEpoch, row, read.cutoff(), 0, null, 0,
                    fromSource);
        });
    }

    private record Read(@Nullable Row row, long cutoff, boolean storeDown) {
    }

    // one attempt: the source, then the conditional write on what was read (the row of the previous conflict)
    private CompletableFuture<LoadResult<V>> attempt(InternalKey<K> internalKey,
                                                     @Nullable AsyncSingleLoader<K, V> singleLoader,
                                                     @Nullable CompletableFuture<InternalValue<V>> owned,
                                                     Executor steps, int loadEpoch, @Nullable Row row, long cutoff,
                                                     int attempt, @Nullable V lastLoaded, long lastVersion,
                                                     boolean fromSource) {
        if (attempt == ATTEMPTS) {
            return CompletableFuture.completedFuture(
                    LoadResult.uncached(new InternalValue<>(requireNonNull(lastLoaded), lastVersion, epoch.get())));
        }
        if (singleLoader == null) {
            return CompletableFuture.completedFuture(LoadResult.absent());
        }
        long seen = row == null ? 0 : row.version();
        stats.increment(Counter.SOURCE_LOAD);
        return call(() -> singleLoader.load(internalKey.key())).thenComposeAsync(loaded -> {
            if (loaded == null) {
                if (fromSource && seen != 0) {
                    // a refresh finding nothing at the source removes the value, as Caffeine's refresh does
                    guard.write(() -> store.invalidateIfUnchanged(List.of(new Expected(internalKey.hash(), seen))));
                    stats.increment(Counter.CONDITIONAL_WRITE);
                }
                rememberAbsence(internalKey, owned, loadEpoch);
                return CompletableFuture.completedFuture(LoadResult.absent());
            }
            Written<V> written = writeLoaded(internalKey, loaded, seen, cutoff, loadEpoch);
            if (written.result() != null) {
                return CompletableFuture.completedFuture(written.result());
            }
            // starting over: read again, for the row and the cut-off the next decision is based on (a conflict's row
            // reports no cut-off, and one known before may be behind: the write would then be hidden for nothing)
            int againEpoch = epoch.get();
            ReadResult again;
            try {
                again = readRows(List.of(internalKey.hash()));
            } catch (StoreException e) {
                return CompletableFuture.completedFuture(
                        LoadResult.cached(new InternalValue<>(loaded, 1, UNCONFIRMED), Duration.ZERO));
            }
            Row current = again.rows().get(internalKey.hash());
            V found = valueOf(current);
            if (current != null && found != null) {
                // a newer value landed meanwhile: it wins
                return CompletableFuture.completedFuture(
                        LoadResult.cached(new InternalValue<>(found, current.version(), againEpoch, again.cutoff()),
                                current.age()));
            }
            return attempt(internalKey, singleLoader, owned, steps, againEpoch, current, again.cutoff(),
                    attempt + 1, loaded, written.version(), fromSource);
        }, steps);
    }

    // the outcome of writing a loaded value: a result, or the row of a conflict to start over from
    private record Written<V>(@Nullable LoadResult<V> result, @Nullable Row current, long version) {
    }

    // the store step after the source: writes conditionally on the version seen (ordering rule 6), with the cut-off
    // the read reported as its basis; held at the epoch before the read
    private Written<V> writeLoaded(InternalKey<K> internalKey, V loaded, long seen, long cutoff, int loadEpoch) {
        Entry entry = entry(internalKey, loaded);
        WriteResult result;
        try {
            result = writeConditionally(seen == 0 ? ExpectedEntry.ifMissing(entry, cutoff)
                    : ExpectedEntry.ifVersion(entry, seen, cutoff));
        } catch (StoreException e) {
            return new Written<>(LoadResult.cached(new InternalValue<>(loaded, 1, UNCONFIRMED), Duration.ZERO), null, 0);
        }
        if (result.written()) {
            return new Written<>(LoadResult.cached(new InternalValue<>(loaded, result.version(), loadEpoch, cutoff),
                    Duration.ZERO), null, 0);
        }
        Row current = result.current();
        if (current == null && result.version() > 0 && result.version() != seen) {
            // a value too old to count: nothing was invalidated, so the loaded value may replace it
            try {
                result = writeConditionally(ExpectedEntry.ifVersion(entry, result.version(), cutoff));
            } catch (StoreException e) {
                return new Written<>(LoadResult.cached(new InternalValue<>(loaded, 1, UNCONFIRMED), Duration.ZERO),
                        null, 0);
            }
            if (result.written()) {
                return new Written<>(LoadResult.cached(new InternalValue<>(loaded, result.version(), loadEpoch,
                        cutoff), Duration.ZERO), null, 0);
            }
            current = result.current();
        }
        if (current != null && current.state() == State.VALUE && current.visible(cutoff)
                && current.version() != seen) {
            V value = valueOf(current);
            if (value != null) {
                // a newer value landed meanwhile: it wins (a conflict's row comes unfiltered: one decided before the
                // cut-off this load read is hidden, and the load starts over; so does one at the version seen, its
                // basis refused)
                return new Written<>(LoadResult.cached(new InternalValue<>(value, current.version(), loadEpoch,
                        cutoff), current.age()), null, 0);
            }
        }
        // invalidated (or expired) meanwhile: what was loaded may be outdated, so load again
        return new Written<>(null, current, Math.max(1, result.version()));
    }

    // a load found nothing, neither in the store nor at the source. Recorded first, then checked against messages
    // that arrived during the load (a message records its version before it forgets the absence), so no outdated
    // absence survives a write elsewhere. Messages find a load in flight only through its future in L1, so nothing is
    // recorded where that future was removed meanwhile - invalidate, invalidateAll - a write may have been announced
    // unseen. Nothing either where Caffeine runs the load itself, whose future is not the load's own to check, or where
    // a gap passed during the load (the epoch moved on): its lost messages may have announced a write
    @SuppressWarnings("ReferenceEquality") // whether this very load's future is still held
    private void rememberAbsence(InternalKey<K> internalKey, @Nullable CompletableFuture<InternalValue<V>> owned,
                                 int loadEpoch) {
        if (absent == null || owned == null) {
            return;
        }
        String hash = internalKey.hash();
        absent.put(hash, Boolean.TRUE);
        if (minimumVersions.containsKey(hash) || l1.asMap().get(internalKey) != owned || epoch.get() != loadEpoch) {
            absent.invalidate(hash);
        }
    }

    // the store does not answer: misses go to the source alone, and the result is held but never trusted, so the
    // first read with the store back checks it (it carries the lowest version, so any message drops it)
    private CompletableFuture<LoadResult<V>> sourceOnly(InternalKey<K> internalKey,
                                                        @Nullable AsyncSingleLoader<K, V> singleLoader) {
        if (singleLoader == null) {
            return CompletableFuture.completedFuture(LoadResult.absent());
        }
        stats.increment(Counter.SOURCE_LOAD);
        return call(() -> singleLoader.load(internalKey.key())).thenApply(loaded -> loaded == null ? LoadResult.absent()
                : LoadResult.cached(new InternalValue<>(loaded, 1, UNCONFIRMED), Duration.ZERO));
    }

    // a conditional write of a value, with the cut-off it was decided under as its basis: if an invalidateAll moved
    // the cut-off meanwhile, the written value is hidden for every reader (VISION.md, "The decision's cut-off on every
    // row"), and the writer counts as before it
    private WriteResult writeConditionally(ExpectedEntry expected) {
        WriteResult result = requireNonNull(putIfUnchanged(List.of(expected)).get(expected.entry().hash()));
        stats.increment(Counter.CONDITIONAL_WRITE);
        if (!result.written()) {
            stats.increment(Counter.CONFLICT);
        }
        return result;
    }

    // a conditional write through the guard; one that landed with its outcome unknown after an invalidateAll is hidden
    // by its basis like any other
    private Map<String, WriteResult> putIfUnchanged(List<ExpectedEntry> writes) {
        return guard.write(() -> store.putIfUnchanged(writes, maximumAge));
    }

    // the cut-off now, from the store (a read of no keys); the highest known if the store does not answer, whose
    // write then fails as well
    private long currentCutoff() {
        try {
            return readRows(List.of()).cutoff();
        } catch (StoreException e) {
            return knownCutoff.get();
        }
    }

    // a read through the guard; rows not visible under the cut-off (written at or before it, or decided before it)
    // count as missing: shown as tombstones with their version, so that a conditional write can replace them
    private ReadResult readRows(Collection<String> hashes) {
        ReadResult read = guard.read(() -> store.read(hashes, maximumAge));
        long cutoff = read.cutoff();
        knownCutoff.accumulateAndGet(cutoff, Math::max);
        if (read.rows().values().stream().allMatch(row -> row.visible(cutoff))) {
            return read;
        }
        Map<String, Row> rows = new LinkedHashMap<>();
        read.rows().forEach((hash, row) -> rows.put(hash, row.visible(cutoff) ? row
                : new Row(hash, row.key(), null, State.TOMBSTONE, row.version(), row.age(), row.stamp(), row.basis())));
        return new ReadResult(rows, cutoff);
    }

    // the versions of value rows visible under the cut-off
    private Map<String, Long> readVisibleVersions(Collection<String> hashes) {
        VersionsResult read = guard.read(() -> store.readVersions(hashes, maximumAge));
        long cutoff = read.cutoff();
        knownCutoff.accumulateAndGet(cutoff, Math::max);
        if (cutoff == 0) {
            return read.versions();
        }
        Map<String, Long> versions = new LinkedHashMap<>();
        read.versions().forEach((hash, version) -> {
            if (Row.visible(requireNonNull(read.stamps().get(hash)), requireNonNull(read.bases().get(hash)), cutoff)) {
                versions.put(hash, version);
            }
        });
        return versions;
    }

    private boolean needsRefresh(Duration age) {
        Duration refreshAfter = refreshAfterWrite; // read once: the policy may change it meanwhile
        return refreshAfter != null && age.compareTo(refreshAfter) >= 0;
    }

    // refresh rule 2: a value read from the store that is at least R old is refreshed at once, in the background
    @SuppressWarnings("unchecked")
    private void refreshLater(InternalKey<K> internalKey) {
        if (l1 instanceof AsyncLoadingCache<?, ?> loading) {
            ((AsyncLoadingCache<InternalKey<K>, InternalValue<V>>) loading).synchronous().refresh(internalKey);
        }
    }

    // several keys

    private Map<InternalKey<K>, InternalValue<V>> getAllOrLoad(List<InternalKey<K>> internalKeys,
                                                               BulkLoader<K, V> bulkLoader) {
        // every step inline: the load has completed when getAllOrLoad returns its future
        return join(getAllOrLoad(internalKeys, inline(bulkLoader), INLINE));
    }

    @SuppressWarnings({"unchecked", "FutureReturnValueIgnored"}) // the callback completes the owned future
    private CompletableFuture<Map<InternalKey<K>, InternalValue<V>>> getAllOrLoad(List<InternalKey<K>> internalKeys,
                                                                                AsyncBulkLoader<K, V> bulkLoader,
                                                                                Executor steps) {
        Set<InternalKey<K>>[] missing = new Set[1];
        @Nullable CompletableFuture<Map<InternalKey<K>, InternalValue<V>>>[] owned = new CompletableFuture[1];
        CompletableFuture<Map<InternalKey<K>, InternalValue<V>>> future = l1.getAll(internalKeys, (keys, executor) -> {
            missing[0] = new LinkedHashSet<>(keys);
            CompletableFuture<Map<InternalKey<K>, InternalValue<V>>> created = new CompletableFuture<>();
            owned[0] = created;
            return created;
        });
        CompletableFuture<Map<InternalKey<K>, InternalValue<V>>> created = owned[0];
        if (created != null) {
            loadAll(missing[0], bulkLoader, steps).whenComplete((loaded, failure) -> {
                if (failure != null) {
                    created.completeExceptionally(cause(failure));
                } else {
                    loaded.forEach((internalKey, value) -> value.bind(internalKey));
                    created.complete(loaded);
                }
            });
        }
        return future;
    }

    // one read for all keys, the source for the rest, one conditional write; keys that conflict start over one by
    // one. Extra keys the source returns unasked were never read, so they are written only where no row exists (a
    // tombstone or an expired row means the key was touched meanwhile, so the source value may be outdated), and held
    // by the core itself under the write registry rather than handed to Caffeine, which would insert them without the
    // version rule and unseen by messages announced during the load. Steps as for single keys
    private CompletableFuture<Map<InternalKey<K>, InternalValue<V>>> loadAll(Set<InternalKey<K>> internalKeys,
                                                                           @Nullable AsyncBulkLoader<K, V> bulkLoader,
                                                                           Executor steps) {
        return on(steps, () -> readAll(internalKeys)).thenCompose(read -> {
            if (read == null) {
                return sourceOnly(internalKeys, bulkLoader);
            }
            if (read.toLoad().isEmpty() || bulkLoader == null) {
                return CompletableFuture.completedFuture(read.result());
            }
            Map<K, InternalKey<K>> byKey = new LinkedHashMap<>();
            read.toLoad().keySet().forEach(internalKey -> byKey.put(internalKey.key(), internalKey));
            stats.add(Counter.SOURCE_LOAD, byKey.size());
            return call(() -> bulkLoader.loadAll(byKey.keySet())).thenComposeAsync(loaded -> {
                List<InternalKey<K>> conflicting = writeAll(internalKeys, read,
                        byKey, requireNonNull(loaded, "loadAll returned null"));
                if (conflicting.isEmpty()) {
                    return CompletableFuture.completedFuture(read.result());
                }
                AsyncSingleLoader<K, V> single = key -> call(() -> bulkLoader.loadAll(Set.of(key)))
                        .thenApply(values -> values.get(key));
                List<CompletableFuture<Void>> retries = conflicting.stream().map(internalKey ->
                    load(internalKey, single, null, steps, false).thenAccept(retried -> {
                        if (retried.cacheable && retried.value != null) {
                            synchronized (read.result()) {
                                read.result().put(internalKey, retried.value);
                            }
                        }
                    })).toList();
                return CompletableFuture.allOf(retries.toArray(new CompletableFuture[0])).thenApply(done -> read.result());
            }, steps);
        });
    }

    // what one read found: the values found, the version seen per key still to load, the cut-off it reported (the
    // writes' basis), and the epoch before it (theirs in L1)
    private record BulkRead<K, V>(Map<InternalKey<K>, InternalValue<V>> result, Map<InternalKey<K>, Long> toLoad,
                                  long cutoff, int epoch) {
    }

    // the store step before the source; null if the store does not answer
    private @Nullable BulkRead<K, V> readAll(Set<InternalKey<K>> internalKeys) {
        int readEpoch = epoch.get();
        ReadResult read;
        try {
            read = readRows(hashes(internalKeys));
        } catch (StoreException e) {
            return null;
        }
        Map<String, Row> rows = read.rows();
        Map<InternalKey<K>, InternalValue<V>> result = new LinkedHashMap<>();
        Map<InternalKey<K>, Long> toLoad = new LinkedHashMap<>();
        for (InternalKey<K> internalKey : internalKeys) {
            Row row = rows.get(internalKey.hash());
            V value = valueOf(row);
            if (row != null && value != null) {
                result.put(internalKey, new InternalValue<>(value, row.version(), readEpoch, read.cutoff()));
            } else {
                toLoad.put(internalKey, row == null ? 0L : row.version());
            }
        }
        stats.add(Counter.STORE_HIT, result.size());
        stats.add(Counter.STORE_MISS, toLoad.size());
        return new BulkRead<>(result, toLoad, read.cutoff(), readEpoch);
    }

    // the store step after the source: one conditional write; fills the result and returns the keys to start over
    private List<InternalKey<K>> writeAll(Set<InternalKey<K>> internalKeys, BulkRead<K, V> read,
                                          Map<K, InternalKey<K>> byKey, Map<? extends K, ? extends V> loaded) {
        Map<InternalKey<K>, InternalValue<V>> result = read.result();
        Map<InternalKey<K>, Long> toLoad = read.toLoad();
        List<ExpectedEntry> writes = new ArrayList<>();
        List<InternalKey<K>> extras = new ArrayList<>();
        Map<String, InternalKey<K>> written = new LinkedHashMap<>();
        Map<String, V> values = new LinkedHashMap<>();
        loaded.forEach((@Nullable K key, @Nullable V value) -> {
            if (key == null || value == null) {
                return;
            }
            InternalKey<K> internalKey = byKey.containsKey(key) ? byKey.get(key) : hasher.wrap(key);
            if (internalKeys.contains(internalKey) && !toLoad.containsKey(internalKey)) {
                return; // already answered by the store
            }
            Entry entry = entry(internalKey, value);
            Long seen = toLoad.get(internalKey);
            if (seen == null) {
                extras.add(internalKey);
            }
            writes.add(seen == null || seen == 0 ? ExpectedEntry.ifMissing(entry, read.cutoff())
                    : ExpectedEntry.ifVersion(entry, seen, read.cutoff()));
            written.put(internalKey.hash(), internalKey);
            values.put(internalKey.hash(), value);
        });
        // the extras are writes in flight until they are held (see endWrite)
        int writeEpoch = read.epoch();
        extras.forEach(this::beginWrite);
        Map<String, WriteResult> results;
        try {
            try {
                results = writes.isEmpty() ? Map.of() : putIfUnchanged(writes);
            } catch (StoreException e) {
                values.forEach((hash, value) -> {
                    InternalKey<K> internalKey = written.get(hash);
                    if (toLoad.containsKey(internalKey)) {
                        result.put(internalKey, new InternalValue<>(value, 1, UNCONFIRMED));
                    }
                });
                return List.of();
            }
            stats.add(Counter.CONDITIONAL_WRITE, results.size());
            results.forEach((hash, writeResult) -> {
                if (writeResult.written()) {
                    InternalKey<K> internalKey = requireNonNull(written.get(hash));
                    if (!toLoad.containsKey(internalKey)) {
                        hold(internalKey, new InternalValue<>(requireNonNull(values.get(hash)), writeResult.version(),
                                writeEpoch, read.cutoff()));
                    }
                } else {
                    stats.increment(Counter.CONFLICT);
                }
            });
        } finally {
            extras.forEach(this::endWrite);
        }
        List<InternalKey<K>> conflicting = new ArrayList<>();
        results.forEach((hash, writeResult) -> {
            InternalKey<K> internalKey = written.get(hash);
            boolean requested = toLoad.containsKey(internalKey);
            if (writeResult.written() && requested) {
                result.put(internalKey, new InternalValue<>(requireNonNull(values.get(hash)), writeResult.version(),
                        writeEpoch, read.cutoff()));
            } else if (!writeResult.written() && requested) {
                Row current = writeResult.current();
                // a newer value, not the one seen (its basis refused) nor one hidden under the cut-off read
                V value = current != null && current.visible(read.cutoff())
                        && current.version() != requireNonNull(toLoad.get(internalKey)) ? valueOf(current) : null;
                if (current != null && value != null) {
                    result.put(internalKey, new InternalValue<>(value, current.version(), writeEpoch, read.cutoff()));
                } else {
                    conflicting.add(internalKey);
                }
            }
        });
        return conflicting;
    }

    // messages (ordering rules 2 and 4): applied only to held values older than the announced version

    @SuppressWarnings("FutureReturnValueIgnored") // the check of a load in flight is attached for its effect
    private void apply(List<Change> messages) {
        // read once per batch and key, whatever the number of messages for it (duplicates, several writes)
        Set<String> toRead = new LinkedHashSet<>();
        for (Change message : messages) {
            if (InternalHeartbeat.HASH.equals(message.hash())) {
                if (heartbeat != null) {
                    heartbeat.received(message.version());
                }
                continue;
            }
            stats.increment(switch (message.kind()) {
                case WRITTEN -> Counter.WRITTEN_MESSAGE;
                case INVALIDATED -> Counter.INVALIDATED_MESSAGE;
                case EXPIRED -> Counter.EXPIRED_MESSAGE;
                case INVALIDATED_ALL -> Counter.INVALIDATED_ALL_MESSAGE;
            });
            if (message.kind() == Change.Kind.INVALIDATED_ALL) {
                knownCutoff.accumulateAndGet(message.version(), Math::max);
                epoch.incrementAndGet();
                l1.synchronous().invalidateAll();
                continue;
            }
            String hash = requireNonNull(message.hash());
            long version = message.version();
            // before L1 is looked at, so that a write in flight either sees the minimum or has committed already
            boolean writeInFlight = writing.computeIfPresent(hash, (key, count) -> {
                writeMinimums.merge(key, version, Math::max);
                return count;
            }) != null;
            InternalKey<K> lookup = InternalKey.lookup(hash);
            CompletableFuture<InternalValue<V>> held = l1.asMap().get(lookup);
            if (held != null && !held.isDone()) {
                // a load in flight: it must reach this version, checked when it completes
                minimumVersions.merge(hash, version, Math::max);
                held.whenComplete((value, failure) -> checkMinimumVersion(lookup, held));
            } else if (held != null && !writeInFlight) {
                // with a write in flight, decided when it commits (endWrite applies the version rule): its own echo,
                // often faster than the commit to L1, then costs neither a drop nor a population read
                if (!isOlder(held, version)) {
                    stats.increment(Counter.IGNORED_MESSAGE);
                } else if (message.kind() == Change.Kind.WRITTEN && distributionMode == DistributionMode.UPDATE) {
                    stats.increment(toRead.add(hash) ? Counter.REPLACED : Counter.IGNORED_MESSAGE);
                } else {
                    stats.increment(switch (message.kind()) {
                        case INVALIDATED -> Counter.INVALIDATED;
                        case EXPIRED -> Counter.EXPIRED;
                        default -> Counter.REPLACED;
                    });
                    removeIfOlder(lookup, version);
                }
            }
            if (message.kind() == Change.Kind.WRITTEN) {
                // after the minimum version above, so that a load recording an absence meanwhile sees one or the other
                forgetAbsence(hash);
            }
        }
        if (!toRead.isEmpty()) {
            populate(new ArrayList<>(toRead));
        }
    }

    // population: holders read the new rows in one query and take what they hold, under the version rule
    private void populate(List<String> hashes) {
        stats.add(Counter.POPULATION_READ, hashes.size());
        // the epoch before the read: a value read before an invalidateAll here is held untrusted
        int readEpoch = epoch.get();
        ReadResult read;
        try {
            read = readRows(hashes);
        } catch (StoreException e) {
            hashes.forEach(hash -> removeIfOlder(InternalKey.lookup(hash), Long.MAX_VALUE));
            return;
        }
        for (String hash : hashes) {
            InternalKey<K> lookup = InternalKey.lookup(hash);
            Row row = read.rows().get(hash);
            V value = valueOf(row);
            if (row == null || value == null) {
                removeIfOlder(lookup, row == null ? Long.MAX_VALUE : row.version());
            } else {
                install(lookup, new InternalValue<>(value, row.version(), readEpoch, read.cutoff()), false, false);
            }
        }
    }

    private void checkMinimumVersion(InternalKey<K> lookup, CompletableFuture<InternalValue<V>> completed) {
        Long minimum = minimumVersions.get(lookup.hash());
        if (minimum == null) {
            return;
        }
        if (isOlder(completed, minimum)) {
            l1.asMap().remove(lookup, completed);
        }
        minimumVersions.remove(lookup.hash(), minimum);
    }

    // what L1 may hold for a key is at least the given version, as a message would be applied: a removal by this
    // instance at its tombstone's, or a read that found none at its row's; also a write in flight here that commits
    // an older value after it drops that value (endWrite), since without a value nothing in L1 holds the version
    // (and without distribution no message would ever correct it). A load in flight is dropped too, as Caffeine's
    // remove discards it after it completed: its waiters still get its value, and its owner holds it only if the key
    // is still there (see runLoad)
    private void dropOlder(InternalKey<K> internalKey, long version) {
        writing.compute(internalKey.hash(), (hash, count) -> {
            if (count != null) {
                writeMinimums.merge(hash, version, Math::max);
            }
            CompletableFuture<InternalValue<V>> current = l1.asMap().get(internalKey);
            while (current != null && (!current.isDone() || isOlder(current, version))
                    && !l1.asMap().remove(internalKey, current)) {
                current = l1.asMap().get(internalKey);
            }
            return count;
        });
    }

    // compare-and-swap like install, for the same reason
    private void removeIfOlder(InternalKey<K> lookup, long version) {
        CompletableFuture<InternalValue<V>> current = l1.asMap().get(lookup);
        while (current != null && isOlder(current, version) && !l1.asMap().remove(lookup, current)) {
            current = l1.asMap().get(lookup);
        }
    }

    // a completed value older than the version; a failed or empty future counts as older, a load in flight does not
    private static <V> boolean isOlder(CompletableFuture<InternalValue<V>> future, long version) {
        if (!future.isDone()) {
            return false;
        }
        if (future.isCompletedExceptionally()) {
            return true;
        }
        InternalValue<V> value = completedValue(future);
        return value == null || value.version() < version;
    }

    // continuity lost, delivery continues: messages may have been missed. Everything held becomes unconfirmed (the
    // epoch moves on) and is checked against the store on its next read and by a background pass. The subscription
    // is up again here, so every change after the check arrives as a message
    private void gap() {
        stats.increment(Counter.GAP);
        // the epoch first: a load recording an absence after the set was cleared then sees the new epoch
        int revalidating = epoch.incrementAndGet();
        if (absent != null) {
            absent.invalidateAll();
        }
        subscriptionUp = true;
        revalidations.incrementAndGet();
        storeExecutor.execute(() -> {
            try {
                revalidateAll(revalidating);
            } finally {
                revalidations.decrementAndGet();
            }
        });
    }

    // delivery stopped: until it is resumed, every access checks the held version against the store
    private void lost() {
        stats.increment(Counter.INTERRUPTION);
        subscriptionUp = false;
        superviseResume();
    }

    // supervising the subscription (VISION.md, "Supervision"): resume() at once, then with backoff (from 250 ms,
    // doubling, at most 30 s, with jitter) until delivery is back. Every resume is a gap: what was written meanwhile
    // may not be delivered, so what is held is checked against the store
    private void superviseResume() {
        losses.incrementAndGet();
        if (!closed && subscription != null && resuming.compareAndSet(false, true)) {
            resumeDelayMillis = RESUME_MIN_MILLIS;
            execute(this::attemptResume);
        }
    }

    private void attemptResume() {
        ChangeSubscription current = subscription;
        InternalDelivery queue = delivery;
        if (closed || current == null || queue == null) {
            resuming.set(false);
            return;
        }
        long seen = losses.get();
        try {
            current.resume();
        } catch (RuntimeException e) {
            long delay = resumeDelayMillis;
            resumeDelayMillis = Math.min(RESUME_MAX_MILLIS, delay * 2);
            LOGGER.log(System.Logger.Level.WARNING, "Resuming the change feed failed; trying again in " + delay
                    + " ms", e);
            if (!closed) {
                resumeTask.set(InternalMaintenance.schedule(() -> execute(this::attemptResume),
                        Duration.ofMillis(delay)));
            }
            return;
        }
        LOGGER.log(System.Logger.Level.INFO, "The change feed resumed; revalidating what is held");
        queue.onGap();
        resuming.set(false);
        if (losses.get() != seen) {
            // lost again while resuming: the resumed delivery may be the lost one
            superviseResume();
        }
    }

    // on the store executor; a closed executor ends the supervision with the cache
    private void execute(Runnable task) {
        try {
            storeExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            resuming.set(false);
        }
    }

    // a beat missed: the feed may deliver nothing, without having said so. Re-established like a lost one; trusted
    // again only after a gap
    private void silenced() {
        stats.increment(Counter.INTERRUPTION);
        superviseResume();
    }

    // the feed delivers, as far as this instance knows: neither reported interrupted nor silent
    private boolean feedUp() {
        return subscriptionUp && (heartbeat == null || !heartbeat.isSilent());
    }

    private boolean isTrusted(InternalValue<V> value) {
        return feedUp() && value.epoch() == epoch.get();
    }

    // revalidation on read: a held value not trusted is checked against the store (its version only); kept and
    // confirmed if the store holds the same version, dropped otherwise (null: the caller treats it as a miss). While
    // the store does not answer, it is served unconfirmed
    @SuppressWarnings("java:S2589") // a future of L1 completes with null where nothing was found (see joinHeld)
    private @Nullable InternalValue<V> confirmOrDrop(InternalKey<K> internalKey, CompletableFuture<InternalValue<V>> held) {
        InternalValue<V> value = joinHeld(held);
        if (value == null || isTrusted(value)) {
            return value;
        }
        // also while the guard is open: it refuses at once, or lets this check through as its probe, which is how
        // a read notices the store is back (otherwise only maintenance did, up to its interval later, serving a value
        // the store no longer had meanwhile: found by the failover test that loses a write)
        int current = epoch.get();
        Map<String, Long> versions;
        try {
            versions = readVisibleVersions(List.of(internalKey.hash()));
        } catch (StoreException e) {
            return value;
        }
        Long stored = versions.get(internalKey.hash());
        stats.increment(Counter.REVALIDATED);
        if (stored != null && stored == value.version()) {
            stats.increment(Counter.REVALIDATION_KEPT);
            value.confirm(current);
            return value;
        }
        stats.increment(Counter.REVALIDATION_DROPPED);
        l1.asMap().remove(internalKey, held);
        return null;
    }

    // the same for several keys, in one query; afterwards the dropped ones are missing from L1
    private void confirmOrDropAll(List<InternalKey<K>> internalKeys) {
        Map<InternalKey<K>, CompletableFuture<InternalValue<V>>> toCheck = new LinkedHashMap<>();
        for (InternalKey<K> internalKey : internalKeys) {
            CompletableFuture<InternalValue<V>> held = l1.asMap().get(internalKey);
            InternalValue<V> value = completedValue(held);
            if (held != null && value != null && !isTrusted(value)) {
                toCheck.put(internalKey, held);
            }
        }
        if (toCheck.isEmpty() || guard.isOpen()) {
            return;
        }
        check(toCheck, epoch.get());
    }

    private void check(Map<InternalKey<K>, CompletableFuture<InternalValue<V>>> toCheck, int confirmingEpoch) {
        Map<String, Long> versions;
        try {
            versions = readVisibleVersions(hashes(toCheck.keySet()));
        } catch (StoreException e) {
            return;
        }
        stats.add(Counter.REVALIDATED, toCheck.size());
        toCheck.forEach((internalKey, held) -> {
            Long stored = versions.get(internalKey.hash());
            InternalValue<V> value = held.join();
            if (stored != null && stored == value.version()) {
                stats.increment(Counter.REVALIDATION_KEPT);
                value.confirm(confirmingEpoch);
            } else {
                stats.increment(Counter.REVALIDATION_DROPPED);
                l1.asMap().remove(internalKey, held);
            }
        });
    }

    // the background pass after a gap: checks everything held in batches, rate-limited so that it does not load a
    // store that has just recovered; stops when a newer gap starts a pass of its own, or the store stops answering
    private void revalidateAll(int revalidating) {
        try {
            revalidateBatches(revalidating);
        } catch (RuntimeException e) {
            // e.g. the store closed meanwhile: the next read of each value checks it anyway
        }
    }

    @SuppressWarnings("BusyWait") // the pause between batches is the rate limit, not a wait for a condition
    private void revalidateBatches(int revalidating) {
        List<InternalKey<K>> held = new ArrayList<>(l1.asMap().keySet());
        for (int from = 0; from < held.size(); from += REVALIDATION_BATCH) {
            if (closed || epoch.get() != revalidating || guard.isOpen()) {
                return;
            }
            Map<InternalKey<K>, CompletableFuture<InternalValue<V>>> batch = new LinkedHashMap<>();
            for (InternalKey<K> internalKey : held.subList(from, Math.min(held.size(), from + REVALIDATION_BATCH))) {
                CompletableFuture<InternalValue<V>> future = l1.asMap().get(internalKey);
                InternalValue<V> value = completedValue(future);
                if (future != null && value != null && value.epoch() < revalidating) {
                    batch.put(internalKey, future);
                }
            }
            if (!batch.isEmpty()) {
                check(batch, revalidating);
            }
            try {
                Thread.sleep(REVALIDATION_PAUSE_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    SynchronizationState state() {
        if (closed) {
            return SynchronizationState.CLOSED;
        }
        return guard.isOpen() || !feedUp() ? SynchronizationState.DEGRADED : SynchronizationState.SYNCHRONIZED;
    }

    // maintenance: expiry (announced), pruning to size and of old tombstones (silent). Calls the store directly, not
    // through the guard: it is how the guard learns that the store answers again
    private void scheduleMaintenance() {
        if (!closed) {
            maintenanceTask.set(InternalMaintenance.schedule(() -> storeExecutor.execute(this::maintain),
                    maintenanceInterval));
        }
    }

    private void maintain() {
        if (closed) {
            return;
        }
        try {
            InternalStoreGuard.retrying(() -> store.expire(maximumAge), Operation.WRITE);
            InternalStoreGuard.retrying(() -> store.pruneToSize(maximumSize), Operation.WRITE);
            InternalStoreGuard.retrying(() -> store.pruneTombstones(InternalMaintenance.TOMBSTONE_RETENTION),
                    Operation.WRITE);
            guard.success();
        } catch (StoreException e) {
            guard.failure();
        } catch (RuntimeException e) {
            // keeps maintenance alive; the next run tries again
        } finally {
            scheduleMaintenance();
        }
    }

    // the heartbeat of the change feed: on the store executor, every interval (with jitter), only with distribution
    private void scheduleHeartbeat() {
        if (!closed && heartbeat != null) {
            heartbeatTask.set(InternalMaintenance.schedule(() -> storeExecutor.execute(this::heartbeat),
                    heartbeatInterval));
        }
    }

    private void heartbeat() {
        if (closed || heartbeat == null) {
            return;
        }
        try {
            heartbeat.tick();
        } finally {
            scheduleHeartbeat();
        }
    }

    // one beat: the reserved key invalidated, directly (not counted as the user's write); none while the guard is open
    private long beat() {
        if (guard.isOpen()) {
            return 0;
        }
        return requireNonNull(store.invalidate(List.of(InternalHeartbeat.HASH)).get(InternalHeartbeat.HASH));
    }

    private CompletableFuture<Map<InternalKey<K>, InternalValue<V>>> sourceOnly(Set<InternalKey<K>> internalKeys,
                                                                              @Nullable AsyncBulkLoader<K, V> bulkLoader) {
        if (bulkLoader == null) {
            return CompletableFuture.completedFuture(new LinkedHashMap<>());
        }
        Map<K, InternalKey<K>> byKey = new LinkedHashMap<>();
        internalKeys.forEach(internalKey -> byKey.put(internalKey.key(), internalKey));
        stats.add(Counter.SOURCE_LOAD, byKey.size());
        return call(() -> bulkLoader.loadAll(byKey.keySet())).thenApply(loaded -> {
            Map<InternalKey<K>, InternalValue<V>> result = new LinkedHashMap<>();
            requireNonNull(loaded, "loadAll returned null").forEach((@Nullable K key, @Nullable V value) -> {
                InternalKey<K> internalKey = key == null ? null : byKey.get(key);
                if (internalKey != null && value != null) {
                    result.put(internalKey, new InternalValue<>(value, 1, UNCONFIRMED));
                }
            });
            return result;
        });
    }

    // the loader Caffeine calls itself: refresh, refreshAfterWrite, and loads of absent keys on refresh. Its store
    // steps run on the store executor; the user's code as the cache was built: the asynchronous loader's futures,
    // or the synchronous loader on the store executor. It does not override asyncLoadAll, so Caffeine never loads
    // through it in bulk: refreshAll refreshes key by key, and every getAll passes its own function

    private final class Loader implements AsyncCacheLoader<InternalKey<K>, InternalValue<V>> {

        private final AsyncSingleLoader<K, V> singleLoader;
        private final AsyncReloader<K, V> reloader;

        Loader(Loaders<K, V> loaders) {
            AsyncSingleLoader<K, V> asyncSingle = loaders.asyncSingle();
            if (asyncSingle != null) {
                this.singleLoader = asyncSingle;
                this.reloader = requireNonNull(loaders.asyncReloader());
            } else {
                // reached from a store step, so they run on the store executor already
                this.singleLoader = inline(loaders.single());
                Reloader<K, V> reload = loaders.reloader();
                this.reloader = (key, oldValue) -> supply(() -> reload.reload(key, oldValue));
            }
        }

        @Override
        public CompletableFuture<? extends InternalValue<V>> asyncLoad(InternalKey<K> key, Executor executor) {
            // only a refresh of a key not held gets here (every other load passes its own function to Caffeine)
            return load(key, singleLoader, null, storeExecutor, true)
                    .thenApply(result -> result.cacheable && result.value != null ? result.value.bind(key) : null);
        }

        // a refresh goes to the source; its result is written conditionally on the held version, and a conflict
        // takes the newer value instead (or drops the key if it was invalidated). The result is committed to L1 here,
        // as a write in flight, as put does: Caffeine installs a refresh only over the value it started from, which
        // the write's own echo would otherwise drop first (found by the parity test); Caffeine then discards its copy
        // as overtaken, silently
        @Override
        public CompletableFuture<? extends InternalValue<V>> asyncReload(InternalKey<K> key, InternalValue<V> oldValue,
                                                                        Executor executor) {
            stats.increment(Counter.SOURCE_RELOAD);
            // the cut-off read from the store before the source, the write's basis (one remembered may be behind:
            // the write would then be hidden for nothing); the epoch before it, the value's in L1
            int reloadEpoch = epoch.get();
            return CompletableFuture.supplyAsync(InternalCore.this::currentCutoff, storeExecutor).thenCompose(cutoff ->
                    call(() -> reloader.reload(key.key(), oldValue.value())).thenApplyAsync(reloaded -> {
                if (reloaded == null) {
                    Map<String, Long> removed = guard.write(() ->
                            store.invalidateIfUnchanged(List.of(new Expected(key.hash(), oldValue.version()))));
                    stats.increment(Counter.CONDITIONAL_WRITE);
                    if (removed.isEmpty()) {
                        stats.increment(Counter.CONFLICT);
                    }
                    return null;
                }
                beginWrite(key);
                try {
                    WriteResult result = writeConditionally(ExpectedEntry.ifVersion(entry(key, reloaded),
                            oldValue.version(), cutoff));
                    InternalValue<V> value;
                    if (result.written()) {
                        value = new InternalValue<>(reloaded, result.version(), reloadEpoch, cutoff);
                    } else {
                        Row current = result.current();
                        V found = current != null && current.visible(cutoff)
                                && current.version() != oldValue.version() ? valueOf(current) : null;
                        value = current == null || found == null ? null
                                : new InternalValue<>(found, current.version(), reloadEpoch, cutoff);
                    }
                    if (value == null) {
                        return null;
                    }
                    commit(key, value);
                    CompletableFuture<InternalValue<V>> held = l1.asMap().get(key);
                    if (held == null || !held.isDone() || held.isCompletedExceptionally() || held.join() != value) {
                        // not held (a newer value was): Caffeine discards it, with a REPLACED event to silence
                        value.replacementSilent(true);
                    }
                    // held: Caffeine finds the identical instance and discards nothing
                    return value;
                } finally {
                    endWrite(key);
                }
            }, storeExecutor));
        }
    }

    @SuppressWarnings("java:S112") // a step may throw anything, which fails its future
    interface Work<T extends @Nullable Object> {
        T run() throws Exception;
    }

    @SuppressWarnings({"java:S112", "java:S1452"}) // as Caffeine's loaders: any exception, any subtypes
    interface Call<T extends @Nullable Object> {
        CompletableFuture<? extends T> run() throws Exception;
    }

    <T extends @Nullable Object> CompletableFuture<T> supply(Work<T> work) {
        return on(storeExecutor, work);
    }

    // a step on the executor (INLINE: in the current thread); its exception fails the future
    @SuppressWarnings("java:S1181") // an error fails the future too, rather than leaving its waiters hanging
    private static <T extends @Nullable Object> CompletableFuture<T> on(Executor executor, Work<T> work) {
        CompletableFuture<T> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                future.complete(work.run());
            } catch (Exception | Error e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    // the user's future: an exception thrown instead of returning one, or no future at all, fails it
    @SuppressWarnings("java:S1181") // an error fails the future too, rather than leaving its waiters hanging
    private static <T extends @Nullable Object> CompletableFuture<T> call(Call<T> call) {
        try {
            return requireNonNull(call.run(), "the loader returned no future").thenApply(value -> value);
        } catch (Exception | Error e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // a synchronous loader as an already completed future, run where it is called
    static <K, V> AsyncSingleLoader<K, V> inline(SingleLoader<K, V> singleLoader) {
        return key -> CompletableFuture.completedFuture(singleLoader.load(key));
    }

    static <K, V> AsyncBulkLoader<K, V> inline(BulkLoader<K, V> bulkLoader) {
        return keys -> CompletableFuture.completedFuture(bulkLoader.loadAll(keys));
    }

    // an asynchronous loader without bulk loading: one future per key
    static <K, V> AsyncBulkLoader<K, V> asyncPerKey(AsyncSingleLoader<K, V> singleLoader) {
        return keys -> {
            Map<K, CompletableFuture<? extends V>> futures = new LinkedHashMap<>();
            for (K key : keys) {
                futures.put(key, call(() -> singleLoader.load(key)));
            }
            return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).thenApply(done -> {
                Map<K, V> result = new LinkedHashMap<>();
                futures.forEach((key, future) -> {
                    V value = completedValue(future);
                    if (value != null) {
                        result.put(key, value);
                    }
                });
                return result;
            });
        };
    }

    static Throwable cause(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null ? failure.getCause() : failure;
    }

    static <K, V> BulkLoader<K, V> perKey(SingleLoader<K, V> singleLoader) {
        return keys -> {
            Map<K, V> result = new LinkedHashMap<>();
            for (K key : keys) {
                V value = singleLoader.load(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        };
    }

    // helpers

    private Entry entry(InternalKey<K> internalKey, V value) {
        return new Entry(internalKey.hash(), keySerializer.serialize(internalKey.key()), valueSerializer.serialize(value));
    }

    // the value a row holds for this instance: none for a tombstone, an expired row, or an unreadable value
    private @Nullable V valueOf(@Nullable Row row) {
        if (row == null || row.state() != State.VALUE) {
            return null;
        }
        return deserializeOrNull(row);
    }

    // an unreadable value counts as a miss (see Serialization); the version rule then replaces it
    private @Nullable V deserializeOrNull(Row row) {
        try {
            return valueSerializer.deserialize(requireNonNull(row.value()));
        } catch (InternalSerializer.SerializationException e) {
            stats.increment(Counter.UNREADABLE_ROW);
            return null;
        }
    }

    // the asynchronous bulk loader: the user's, or one future per key; none without an asynchronous loader
    private static <K, V> @Nullable AsyncBulkLoader<K, V> asyncBulkLoader(Loaders<K, V> loaders) {
        AsyncSingleLoader<K, V> single = loaders.asyncSingle();
        if (single == null) {
            return null;
        }
        AsyncBulkLoader<K, V> bulk = loaders.asyncBulk();
        return bulk != null ? bulk : asyncPerKey(single);
    }

    private List<InternalKey<K>> wrapAll(Iterable<? extends K> keys) {
        requireNonNull(keys, "keys cannot be null");
        return StreamSupport.stream(keys.spliterator(), false)
                .map(key -> hasher.wrap(requireNonNull(key, "keys cannot contain null")))
                .distinct()
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private static <T> List<String> hashes(Collection<InternalKey<T>> internalKeys) {
        return internalKeys.stream().map(InternalKey::hash).toList();
    }

    // waits for a user's future in a loader: its own exception, not a wrapper; interruptible
    @SuppressWarnings("java:S112") // rethrows the loader's own exception, whatever it is
    static <T> T await(CompletableFuture<T> future) throws Exception {
        try {
            return future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception exception) {
                throw exception;
            }
            if (e.getCause() instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    // the value of a completed future of L1, or null: in flight, failed, or completed with null (nothing found), which
    // Caffeine's types cannot express for a value type that is not nullable
    static <T> @Nullable T completedValue(@Nullable CompletableFuture<? extends T> future) {
        return future != null && future.isDone() && !future.isCompletedExceptionally() ? future.join() : null;
    }

    // completes a future of L1 with null, which Caffeine takes as nothing found but its types cannot express for a
    // value type that is not nullable
    private static void completeAbsent(CompletableFuture<?> future) {
        future.complete(null);
    }

    // join for a future of L1, which completes with null where nothing was found
    @SuppressWarnings("DataFlowIssue") // the null Caffeine's types cannot express, see above
    private static <T> @Nullable T joinHeld(CompletableFuture<T> future) {
        return join(future);
    }

    static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (e.getCause() instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    private record LoadResult<V>(@Nullable InternalValue<V> value, boolean cacheable, Duration age) {

        static <V> LoadResult<V> cached(InternalValue<V> value, Duration age) {
            return new LoadResult<>(value, true, age);
        }

        static <V> LoadResult<V> uncached(InternalValue<V> value) {
            return new LoadResult<>(value, false, Duration.ZERO);
        }

        static <V> LoadResult<V> absent() {
            return new LoadResult<>(null, true, Duration.ZERO);
        }
    }
}
