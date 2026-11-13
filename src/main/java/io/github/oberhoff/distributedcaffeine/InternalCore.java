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
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;
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
 * <p>
 * The ordering rules the code refers to by number (VISION.md, "Ordering", in the terms of the versions the core now
 * proposes and the store enforces, see {@link InternalVersion}):
 * <ol>
 *     <li>Every value in L1 carries the version of its row: the one its write applied at, or the one read.</li>
 *     <li>A message or a load result applies only above the version held, which covers messages out of order,
 *     duplicates and the writer's own echo (the same version, so ignored).</li>
 *     <li>Local writes commit to L1 under the same comparison. A write is registered by hash before it goes to the
 *     store (the write registry): a message arriving before the commit records its version, and a value older than
 *     it is dropped after the commit; otherwise a key not held yet would keep the older value for good.</li>
 *     <li>A load overtaken by a message is discarded: a message for a key with a load in flight records the version
 *     the load's result must reach.</li>
 *     <li>A write with an unknown outcome drops the key from L1; if it landed, the next read finds it in the
 *     store.</li>
 *     <li>Writes not decided on a read ({@code put}, {@code invalidate}) apply above the stored version; the results
 *     of loads and refreshes only if the row still has the version they read, or is still missing, so that an older
 *     source value never overwrites a newer write that landed meanwhile.</li>
 * </ol>
 * The refresh rules: (1) a refresh always calls the source, never takes the store's value, and writes under rule 6;
 * (2) a value read from the store that is at least R old ({@code refreshAfterWrite}) is served and refreshed at once,
 * since Caffeine counts R from the value's insertion into L1. A value younger than R when read is therefore refreshed
 * by an age of at most 2R.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Andreas Oberhoff
 */
// the core is the cache's one place of coordination (ordering rules, loads, messages, maintenance), so its size,
// its dependencies and the complexity of its steps are deliberate; see VISION.md
@SuppressWarnings({"java:S6539", "java:S3776", "java:S107"})
final class InternalCore<K, V> {

    private static final System.Logger LOGGER = System.getLogger(DistributedCaffeine.class.getPackageName());
    private static final long RESUME_MIN_MILLIS = 250;
    private static final long RESUME_MAX_MILLIS = 30_000;
    // a cache started without its store: how often it warns that the store is still not reached, and how long it
    // waits after a check of the store failed (another layout, a missing right) before it tries again
    private static final Duration NOT_REACHED_WARNING_INTERVAL = Duration.ofMinutes(5);
    private static final Duration FAILED_CHECK_RETRY = Duration.ofMinutes(5);

    // the hashes this thread is loading as their owner, to detect a loader requesting its own key; emptied after each
    // load, and kept per thread to spare an allocation per load
    @SuppressWarnings("java:S5164")
    private static final ThreadLocal<Set<InternalKey<?>>> LOADING = ThreadLocal.withInitial(HashSet::new);
    // how often a load is attempted before it returns its value uncached (see load); the builder may raise it for the
    // linearizability tests (DistributedCaffeine#withLoadAttempts)
    static final int LOAD_ATTEMPTS = 3;
    // refusals of an advancing write by larger versions before it fails: each is another writer's progress
    private static final int ADVANCE_ATTEMPTS = 100;
    // what writeConditionally returns for a write not applied (versions are positive)
    private static final long REFUSED = -1;
    // the epoch of values loaded from the source alone while the store did not answer: never trusted, so the first
    // read with the store back checks them
    private static final int UNCONFIRMED = -1;
    private static final int REVALIDATION_BATCH = 1_000;
    private static final long REVALIDATION_PAUSE_MILLIS = 10;
    // how many absences negative caching remembers per instance: a bound on memory only (about 150 bytes each, so some
    // 15 MB when full), not on correctness. Caffeine evicts the least useful (one-off probes before keys asked for
    // again and again), and a forgotten absence costs a load, never a wrong answer; so not a setting
    private static final int NEGATIVE_CACHING_MAXIMUM_SIZE = 100_000;

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

    // how a cache started without its store connects it (VISION.md, "Starting without the store"): connect creates the
    // store and makes it ready for distribution (DistributedCaffeine#connect), throwing a retryable StoreException
    // while the store is not reached and anything else for a check that failed; cause is why build() did not reach it;
    // unbounded tells that the copies have neither a maximum age nor an expiry, so that those loaded meanwhile live
    // until the store is reached (said in the warnings)
    record Connector(Supplier<Connected> connect, StoreException cause, boolean unbounded) {
    }

    // a store connected, and how long size pruning spares a row for its kind of change source
    // (DistributedCaffeine#pruneGrace)
    record Connected(Store store, Duration pruneGrace) {
    }

    // what an atomic update found and left
    record Update<V>(@Nullable V oldValue, @Nullable V newValue) {

        // the key removed: nothing new
        static <V> Update<V> removed(@Nullable V oldValue) {
            return new Update<>(oldValue, null);
        }
    }

    private final Store store;
    private final InternalHasher<K> hasher;
    private final InternalSerializer<K> keySerializer;
    private final InternalSerializer<V> valueSerializer;
    // the cache's name, for its log messages: where it lives (Adapter#toString)
    private final String name;
    // the store's maximum age and size: as built, or as set since through the distributed policy (maximumAge,
    // maximumSize), read where used; whether each was bounded when built (only then can it be set)
    private volatile Duration maximumAge;
    private final boolean ageBounded;
    // the maximum age for the store only (StoreConfigurer#withMaximumAgeForStoreOnly): a value expires in the store,
    // but its copies stay here
    private final boolean maximumAgeForStoreOnly;
    // L1's expiry, if it is this library's (CaffeineConfigurer#caffeine): the policy shows its fixed durations
    private final @Nullable InternalExpiry<K, V> expiry;
    private final Executor storeExecutor;
    private final int loadAttempts;
    private volatile @Nullable Duration refreshAfterWrite;
    private final Runnable onClose;
    private final AtomicInteger epoch = new AtomicInteger();
    // the version of the latest control row (invalidateAll) this instance applied, its own or announced: a message of
    // the control row at or below it is one already applied (a poll reports every change within its overlap again,
    // a feed may deliver twice, its own write comes back), and clearing again would drop what was decided after it
    private final AtomicLong controlVersion = new AtomicLong(Long.MIN_VALUE);
    // the time, sleeping, scheduling and randomness (InternalEnvironment): real, or a test's simulation
    private final InternalEnvironment environment;
    // this instance's clock: every version it proposes, larger than every one it has seen (InternalVersion)
    private final InternalVersion clock;
    // when the last warning of a clock ahead was logged, at most one a minute
    private final AtomicLong clockAheadWarned = new AtomicLong();
    private final AtomicLong unreadableWarned = new AtomicLong();
    // when a failed write of AsyncCache.put was last logged (asyncWriteLost)
    private final AtomicLong asyncWriteLostWarned = new AtomicLong();
    // the atomic map operations (compute, replace, merge and their kin) against this instance's own invalidateAll,
    // atomic per key as in Caffeine: they hold the shared side from their decision to their commit, invalidateAll the
    // exclusive side while it writes the control row and clears L1. Otherwise one decided on a value from before the
    // control row and written after it would survive the invalidateAll (VISION.md, "Target design: invalidateAll by
    // the store's time, hidden on read"). An invalidateAll elsewhere, not heard of yet, is the race accepted
    private final InternalClearLock clearing = new InternalClearLock();
    private final AtomicInteger revalidations = new AtomicInteger();
    private final AsyncCache<InternalKey<K>, InternalValue<V>> l1;
    private final @Nullable SingleLoader<K, V> loader;
    private final @Nullable AsyncSingleLoader<K, V> asyncLoader;
    private final @Nullable AsyncBulkLoader<K, V> asyncBulkLoader;
    private final DistributionMode distributionMode;
    // recent loads that found nothing, by hash, if negative caching is on
    private final @Nullable Cache<String, Boolean> absences;
    // the synchronous loads in flight by the thread running them, and the load each waiting thread waits for, see
    // awaitLoad
    private final Map<InternalKey<K>, Thread> loadOwners = new ConcurrentHashMap<>();
    private final Map<Thread, InternalKey<K>> waitingFor = new ConcurrentHashMap<>();
    // the reservations (ordering rule 4; VISION.md, "Design study: L1 by invalidation and reservation"): per hash, the
    // operations in flight that will put into L1 (loads, writes, reads of the store, refreshes) and the least version
    // L1 must hold when they do, raised by every message and removal meanwhile. An install checks it under the key's
    // entry, and the last operation to end drops what is held below it
    private final Map<String, Reservation> reservations = new ConcurrentHashMap<>();
    // null without distribution (DistributionMode.NONE): no subscription at all
    private final @Nullable InternalDelivery delivery;
    private final @Nullable ChangeSubscription subscription;
    private final InternalStatsCounter stats;
    // Caffeine's stats counter of L1, for what Caffeine counts but the core does not run through it
    private final InternalCaffeineStats caffeineStats;
    private final InternalStoreGuard guard;
    private volatile long maximumSize;
    private final boolean sizeBounded;
    private final Duration maintenanceInterval;
    // how long a row is spared from size pruning at least (see DistributedCaffeine#pruneGrace): so that a load's write
    // on "missing" does not apply over a newer row pruned while the load ran (unless the load took longer), and,
    // polled, a write is never deleted before a poll could see it
    private volatile Duration pruneGrace;
    private volatile boolean subscriptionUp = true;
    // null without distribution, like the subscription
    private final @Nullable InternalHeartbeat heartbeat;
    private final Duration heartbeatInterval;
    // the maintenance runs that failed in a row, and whether that was logged (maintenanceFailed); runs follow one
    // another, each on the store executor
    private final AtomicInteger maintenanceFailures = new AtomicInteger();
    private volatile boolean maintenanceFailing;
    private final AtomicReference<@Nullable ScheduledFuture<?>> maintenanceTask = new AtomicReference<>();
    private final AtomicReference<@Nullable ScheduledFuture<?>> heartbeatTask = new AtomicReference<>();
    // supervising the subscription: whether resume attempts are under way, the losses reported so far (a loss during
    // an attempt calls for another), the wait before the next attempt, and its task
    private final AtomicBoolean resuming = new AtomicBoolean();
    private final AtomicLong losses = new AtomicLong();
    private volatile long resumeDelayMillis = RESUME_MIN_MILLIS;
    private final AtomicReference<@Nullable ScheduledFuture<?>> resumeTask = new AtomicReference<>();
    // a cache started without its store (VISION.md, "Starting without the store"): the stand-in until the store is
    // connected, how to connect it, the wait before the next attempt (doubling as resumes do), its task, and when the
    // store was last warned of as not reached; null for a cache whose store answered at build()
    private final @Nullable InternalPendingStore pending;
    private final @Nullable Connector connector;
    private volatile long connectDelayMillis = RESUME_MIN_MILLIS;
    private final AtomicReference<@Nullable ScheduledFuture<?>> connectTask = new AtomicReference<>();
    private volatile long notReachedWarned;
    private volatile boolean closed;

    @SuppressWarnings("java:S2637") // l1 is assigned in the try below, before anything can fail after it
    InternalCore(String name, Store store, Caffeine<InternalKey<K>, InternalValue<V>> caffeine,
                 @Nullable InternalExpiry<K, V> expiry, InternalHasher<K> hasher, InternalSerializer<K> keySerializer,
                 InternalSerializer<V> valueSerializer,
                 Duration maximumAge,
                 boolean maximumAgeForStoreOnly, Executor storeExecutor, @Nullable Duration refreshAfterWrite,
                 @Nullable Loaders<K, V> loaders,
                 DistributionMode distributionMode,
                 @Nullable Duration negativeCachingExpiry, long maximumSize, Duration maintenanceInterval,
                 Duration pruneGrace, Duration heartbeatInterval, int loadAttempts, InternalStatsCounter stats,
                 Runnable onClose, @Nullable Connector connector, InternalEnvironment environment) {
        this.environment = requireNonNull(environment);
        this.name = requireNonNull(name);
        this.clock = new InternalVersion(environment, this::clockAhead);
        this.loadAttempts = loadAttempts;
        this.stats = requireNonNull(stats);
        this.guard = new InternalStoreGuard(name, stats, environment);
        // started without its store: refused at once until connected, before the subscription below reports its loss
        this.pending = store instanceof InternalPendingStore standIn ? standIn : null;
        this.connector = connector;
        if (pending != null) {
            requireNonNull(connector, "a cache started without its store needs a connector");
            guard.openWithoutStore();
        }
        this.store = requireNonNull(store);
        this.hasher = requireNonNull(hasher);
        this.keySerializer = requireNonNull(keySerializer);
        this.valueSerializer = requireNonNull(valueSerializer);
        this.maximumAge = requireNonNull(maximumAge);
        this.ageBounded = !maximumAge.equals(StoreConfigurer.UNBOUNDED_AGE);
        this.maximumAgeForStoreOnly = maximumAgeForStoreOnly;
        this.expiry = expiry;
        this.storeExecutor = requireNonNull(storeExecutor);
        this.loader = loaders == null ? null : loaders.single();
        this.onClose = requireNonNull(onClose);
        this.refreshAfterWrite = refreshAfterWrite;
        try {
            this.l1 = loaders == null ? caffeine.buildAsync() : caffeine.buildAsync(new Loader(loaders));
        } finally {
            // taken even if the build failed after Caffeine created it: left on this thread, a later build here
            // without stats would take a stranger's counter
            this.caffeineStats = CaffeineConfigurer.takeCreatedStatsCounter();
        }
        this.asyncLoader = loaders == null ? null : loaders.asyncSingle();
        this.asyncBulkLoader = loaders == null ? null : asyncBulkLoader(loaders);
        this.distributionMode = requireNonNull(distributionMode);
        this.maximumSize = maximumSize;
        this.sizeBounded = maximumSize != StoreConfigurer.UNBOUNDED_SIZE;
        this.maintenanceInterval = requireNonNull(maintenanceInterval);
        this.pruneGrace = requireNonNull(pruneGrace);
        this.heartbeatInterval = requireNonNull(heartbeatInterval);
        // on the store executor and the environment's time, as the rest of the core: Caffeine's defaults (the common
        // pool, System.nanoTime) would escape a simulation (found by InternalInterleavingTest: a thread of the common
        // pool outside its control)
        this.absences = negativeCachingExpiry == null ? null
                : Caffeine.newBuilder().expireAfterWrite(negativeCachingExpiry)
                .maximumSize(NEGATIVE_CACHING_MAXIMUM_SIZE).executor(storeExecutor).ticker(environment::nanoTime)
                .build();
        // subscribed before the first read, so every change after it reaches this instance
        if (distributionMode == DistributionMode.NONE) {
            this.delivery = null;
            this.subscription = null;
            this.heartbeat = null;
        } else if (store instanceof ChangeFeed feed) {
            this.heartbeat = new InternalHeartbeat(clock, this::beat, this::silenced, this::gap);
            this.delivery = new InternalDelivery(name, storeExecutor, this::apply, this::gap, this::lost);
            this.subscription = feed.subscribe(delivery);
            if (pending != null) {
                // started without its store: no delivery before the connect, which the supervision's resume subscribes
                subscriptionUp = false;
                superviseResume();
            } else if (losses.get() > 0) {
                superviseResume(); // lost while subscribing, before the subscription could be resumed
            }
        } else {
            throw new IllegalArgumentException("a store without a change feed supports only DistributionMode.NONE");
        }
        scheduleMaintenance();
        scheduleHeartbeat();
        if (connector != null) {
            startWithoutStore(connector);
        }
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
                resumeTask, connectTask)) {
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
            // emptied so that a closed instance someone still references holds no memory; unseen by the user's
            // removal listener, as these are no removals in the cache's sense (else a listener acting on removals
            // would act on every copy at shutdown). A load still in flight completes unmarked and may be reported
            for (CompletableFuture<InternalValue<V>> copy : l1.asMap().values()) {
                InternalValue<V> value = completedValue(copy);
                if (value != null) {
                    value.removalSilent();
                }
            }
            l1.synchronous().invalidateAll();
            onClose.run();
        }
    }

    @Nullable InternalExpiry<K, V> expiry() {
        return expiry;
    }

    AsyncCache<InternalKey<K>, InternalValue<V>> l1() {
        return l1;
    }

    DistributedStats stats() {
        return stats.snapshot();
    }

    // whether negative caching remembers the key (tests)
    boolean remembersAbsence(K key) {
        return absences != null && absences.getIfPresent(wrap(key).hash()) != null;
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
            if (absenceTrusted(internalKey.hash())) {
                stats.increment(Counter.NEGATIVE_HIT);
            } else {
                toRead.add(internalKey);
            }
        }
        if (!toRead.isEmpty()) {
            int readEpoch = epoch.get();
            toRead.forEach(this::reserve);
            try {
                Map<String, Row> rows = readRows(hashes(toRead));
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
                    if (remembered(internalKey.hash())) {
                        forgetAbsence(internalKey.hash()); // written elsewhere while no message could tell
                    }
                    hold(internalKey, new InternalValue<>(value, row.version(), readEpoch, row.age()));
                    found.put(internalKey, value);
                    if (needsRefresh(row.age())) {
                        refreshLater(internalKey);
                    }
                }
            } catch (StoreException e) {
                // a miss, as Caffeine's getIfPresent answers without loading
            } finally {
                toRead.forEach(this::release);
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
        reservations.compute(internalKey.hash(), (hash, reservation) -> {
            long minimum = reservation == null ? Reservation.NONE : reservation.minimum();
            if (minimum == Reservation.NONE || InternalVersion.atLeast(value.version(), minimum)) {
                installNow(internalKey, value, addIfMissing, overInFlight);
            }
            return reservation;
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
            } else if (currentValue != null && InternalVersion.atLeast(currentValue.version(), value.version())) {
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
        List<InternalKey<K>> inFlight = new ArrayList<>();
        for (InternalKey<K> internalKey : requested) {
            // as in getIfPresent: a miss is counted once, by the read through the store
            CompletableFuture<InternalValue<V>> held = l1.asMap().containsKey(internalKey)
                    ? l1.getIfPresent(internalKey) : null;
            if (held == null) {
                missing.add(internalKey);
            } else if (!held.isDone()) {
                // as in getIfPresent: a load in flight here, whose value may already be in the store; counted already
                inFlight.add(internalKey);
            } else {
                InternalValue<V> value = completedValue(held);
                if (value != null) {
                    result.put(internalKey.key(), value.value());
                }
            }
        }
        if (!missing.isEmpty() || !inFlight.isEmpty()) {
            // one read for both; only the keys not held are counted, after it, as readThrough counts them
            List<InternalKey<K>> toRead = new ArrayList<>(missing);
            toRead.addAll(inFlight);
            Map<InternalKey<K>, V> found = readThrough(toRead, false);
            countLookups(missing);
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

    // a value this instance cannot read counts as none, as for every read (see deserializeOrNull)
    @Nullable StoredValue<V> getFromStore(K key) {
        InternalKey<K> internalKey = wrap(key);
        Row row = readRows(List.of(internalKey.hash())).get(internalKey.hash());
        V value = valueOf(row);
        return row == null || value == null ? null : new StoredValue<>(value, shown(row.age()));
    }

    // an age as the caller sees it: never negative, though a store reports one for a row written after the read's
    // instant (Store#read), where only the comparison within the read needs it
    private static Duration shown(Duration age) {
        return age.isNegative() ? Duration.ZERO : age;
    }

    Map<K, StoredValue<V>> getAllFromStore(Iterable<? extends K> keys) {
        checkOpen();
        List<InternalKey<K>> requested = wrapAll(keys);
        Map<String, Row> rows = readRows(hashes(requested));
        Map<K, StoredValue<V>> result = new LinkedHashMap<>();
        for (InternalKey<K> internalKey : requested) {
            Row row = rows.get(internalKey.hash());
            V value = valueOf(row);
            if (row != null && value != null) {
                result.put(internalKey.key(), new StoredValue<>(value, shown(row.age())));
            }
        }
        return result;
    }

    // writes

    // a put reads nothing, by requirement: one round trip, a write that applies above whatever version is stored
    // (ordering rule 6), so that a racing write is ordered by the store rather than by a read first
    void put(K key, V value) {
        InternalKey<K> internalKey = wrap(key);
        requireNonNull(value, "value cannot be null");
        // taken before the write: an invalidateAll() landing in between leaves the value untrusted
        int writeEpoch = epoch.get();
        reserve(internalKey);
        try {
            long version = requireNonNull(putValues(Map.of(internalKey, value)).get(internalKey.hash()));
            commit(internalKey, new InternalValue<>(value, version, writeEpoch));
        } finally {
            release(internalKey);
        }
    }

    // negative caching: whether the key is remembered absent. Answered from as it is only while the feed delivers
    // (absenceTrusted), since only a message ends an absence on a write elsewhere: the trust of a held value. Otherwise
    // the store is read first, and a value found there wins; while the store does not answer, the absence is served
    // unconfirmed, as a held value is
    private boolean remembered(String hash) {
        return absences != null && absences.getIfPresent(hash) != null;
    }

    private boolean absenceTrusted(String hash) {
        return remembered(hash) && feedUp();
    }

    private void forgetAbsence(String hash) {
        if (absences != null) {
            absences.invalidate(hash);
        }
    }

    void putAll(Map<? extends K, ? extends V> map) {
        checkOpen();
        Map<InternalKey<K>, V> values = new LinkedHashMap<>();
        map.forEach((key, value) -> values.put(hasher.wrap(requireNonNull(key)), requireNonNull(value)));
        if (values.isEmpty()) {
            return;
        }
        int writeEpoch = epoch.get();
        values.keySet().forEach(this::reserve);
        try {
            Map<String, Long> versions = putValues(values);
            values.forEach((internalKey, value) ->
                    commit(internalKey, new InternalValue<>(value, requireNonNull(versions.get(internalKey.hash())),
                            writeEpoch)));
        } finally {
            values.keySet().forEach(this::release);
        }
    }

    /**
     * The user's remapping function of {@code compute}, {@code computeIfPresent} or {@code merge}, counted in
     * Caffeine's stats as Caffeine counts it: a load, failed if it returns {@code null} or throws. Each run counts (it
     * may run more than once after a conflict).
     */
    @SuppressWarnings("java:S1181") // an error is recorded as a failed load too, then rethrown
    <A, B extends @Nullable Object> BiFunction<A, B, @Nullable V> countedAsLoad(
            BiFunction<? super A, ? super B, ? extends @Nullable V> remappingFunction) {
        return (first, second) -> {
            long start = environment.nanoTime();
            V value;
            try {
                value = remappingFunction.apply(first, second);
            } catch (RuntimeException | Error e) {
                caffeineStats.recordLoadFailure(environment.nanoTime() - start);
                throw e;
            }
            if (value == null) {
                caffeineStats.recordLoadFailure(environment.nanoTime() - start);
            } else {
                caffeineStats.recordLoadSuccess(environment.nanoTime() - start);
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
        clearing.lockShared();
        reserve(internalKey);
        try {
            // while the store does not answer, a held copy is decided on as a read serves it, unconfirmed: a decision
            // to write nothing (putIfAbsent on a present key, a replace or remove whose value does not match) is
            // answered from it, as computeIfAbsent and get answer; a decision to write throws, as every write then.
            // Without a held copy, nothing can be told about the key, and the call throws
            Current<V> current;
            @Nullable StoreException unanswered = null;
            try {
                current = heldOrRead(internalKey);
            } catch (StoreException e) {
                InternalValue<V> held = completedValue(l1.asMap().get(internalKey));
                if (held == null) {
                    throw e;
                }
                current = new Current<>(held.value(), held.version(), held.epoch(), true);
                unanswered = e;
            }
            while (true) {
                V newValue = remapping.apply(internalKey.key(), current.value());
                if (unanswered != null) {
                    // not even a write conditioned on the held version: an untrusted copy may predate an
                    // invalidateAll, which leaves the versions of the rows it hides as they are
                    if (newValue == current.value() && !alwaysWrite) {
                        return new Update<>(current.value(), newValue);
                    }
                    throw failed(internalKey, newValue, unanswered);
                }
                if (current.held() && (newValue == null || (newValue == current.value() && !alwaysWrite))) {
                    // decided on a held value, but not to write one: nothing would check it, so checked by a read.
                    // The same version and content confirm the decision (the remapping, counted as a load, does not
                    // run again; the held instance kept, as Caffeine tells an unchanged value by identity); otherwise
                    // decided again. The version alone does not tell (see stillStored). While the store does not
                    // answer, a decision to write nothing stands unconfirmed
                    Current<V> read;
                    try {
                        read = read(internalKey);
                    } catch (StoreException e) {
                        if (newValue == current.value() && !alwaysWrite) {
                            return new Update<>(current.value(), newValue);
                        }
                        // the decision left to confirm here is a removal
                        throw failed(internalKey, null, e);
                    }
                    if (read.version() != current.version() || read.value() == null
                            || !sameContent(read.value(), current.value())) {
                        // written elsewhere since, or hidden by an invalidateAll (read at the same version, no value)
                        current = read;
                        continue;
                    }
                    current = new Current<>(current.value(), read.version(), read.epoch(), false, read.age());
                }
                if (newValue == current.value() && !alwaysWrite) {
                    // nothing to write, but L1 may be behind what the read found (a write in flight here): brought up
                    // to it, so that no later read here answers older
                    catchUp(internalKey, current);
                    return new Update<>(current.value(), newValue);
                }
                try {
                    long version = current.version() == 0 ? clock.fresh()
                            : clock.next(current.version());
                    if (newValue == null) {
                        Write tombstone = Write.tombstone(hash, version);
                        long applied = decide(current.version() == 0 ? tombstone.ifMissing()
                                : tombstone.ifVersion(current.version()));
                        if (applied != REFUSED) {
                            dropOlder(internalKey, applied);
                            return Update.removed(current.value());
                        }
                    } else {
                        forgetAbsence(hash);
                        Write write = valueWrite(internalKey, newValue, version);
                        long applied = decide(current.version() == 0 ? write.ifMissing()
                                : write.ifVersion(current.version()));
                        if (applied != REFUSED) {
                            // at the epoch of the read: after an invalidateAll here since, the value is checked on
                            // its next read
                            commit(internalKey, new InternalValue<>(newValue, applied, current.epoch()));
                            return new Update<>(current.value(), newValue);
                        }
                    }
                    // a write elsewhere since what was decided on: read again, for the row the next decision is
                    // based on. A value too old to count reads as expired at its version, and a row hidden by an
                    // invalidateAll as a tombstone, so the next write is conditioned on it
                    current = read(internalKey);
                } catch (StoreException e) {
                    throw failed(internalKey, newValue, e);
                }
            }
        } finally {
            release(internalKey);
            clearing.unlockShared();
        }
    }

    // an update the store did not take: a removal drops the key locally whatever the store answered, as invalidate
    // does, so that this instance no longer serves what the caller removed; a value written with an unknown outcome
    // drops it too (at no version known: anything held goes, and nothing in flight installs). A value refused for
    // sure leaves the copy held, as the store still holds what it shows
    private StoreException failed(InternalKey<K> internalKey, @Nullable V newValue, StoreException e) {
        if (newValue == null || e.isOutcomeUnknown()) {
            dropOlder(internalKey, InternalVersion.ANYTHING);
        }
        return e;
    }

    // the hot path: a trusted held value is decided on as it is, and the write, conditional on its version, is one
    // store operation, refused if a write elsewhere came since. Otherwise from the store. An invalidateAll elsewhere
    // not heard of yet is not seen by the version (it leaves the row as it is): the decision then counts as before it,
    // the race accepted for loads and updates in flight (VISION.md, "Target design: invalidateAll by the store's time")
    // With the maximum age for the store only, always read: most held copies outlive their rows, and a write
    // conditioned on the held version would be refused and decided again, the remapping run twice
    private Current<V> heldOrRead(InternalKey<K> internalKey) {
        int heldEpoch = epoch.get();
        InternalValue<V> held = completedValue(l1.asMap().get(internalKey));
        if (held != null && !maximumAgeForStoreOnly && isTrusted(held)) {
            return new Current<>(held.value(), held.version(), heldEpoch, true);
        }
        return read(internalKey);
    }

    private Current<V> read(InternalKey<K> internalKey) {
        int readEpoch = epoch.get();
        Row row = readRows(List.of(internalKey.hash())).get(internalKey.hash());
        if (maximumAgeForStoreOnly && (row == null || row.state() == State.EXPIRED)) {
            // with the maximum age for the store only, a row the store let go of (expired, pruned to size, or an
            // expired row cleaned up) is not a removal: a trusted copy held here is still the latest value, at the
            // row's version (0: none), so that the write is conditioned on what the store holds. Sound because a
            // trusted holder has heard every write older than about two heartbeat intervals with their jitter, and the
            // store lets go of no row younger: a row expires a maximum age after its last write (at least five
            // intervals), size pruning spares three intervals (DistributedCaffeine#pruneGrace), and tombstones are kept
            // five minutes (at least five intervals, since an interval is at most a minute). A removal leaves a
            // tombstone, and a row hidden by an invalidateAll reads as one; neither is taken for the held value. A row
            // deleted by an invalidateAll not heard of yet is the race accepted for updates in flight (see heldOrRead)
            InternalValue<V> held = completedValue(l1.asMap().get(internalKey));
            if (held != null && held.epoch() == readEpoch && isTrusted(held)) {
                return new Current<>(held.value(), row == null ? 0 : row.version(), readEpoch, false);
            }
        }
        if (row == null) {
            return new Current<>(null, 0, readEpoch, false);
        }
        return new Current<>(valueOf(row), row.version(), readEpoch, false, row.age());
    }

    private void catchUp(InternalKey<K> internalKey, Current<V> current) {
        V value = current.value();
        if (value != null) {
            hold(internalKey, new InternalValue<>(value, current.version(), current.epoch(), current.age()));
        } else if (current.version() > 0) {
            // no value at that version (a hidden row reads as a tombstone at its value's version): held at it is stale
            dropOlder(internalKey, current.version() + 1);
        }
    }

    // what an update decides on, held or read from the store (version 0: no row; a version without a value: a
    // tombstone, an expired row or a value too old to count, a row hidden by an invalidateAll, or an unreadable
    // value), the epoch before it was looked at (the written value's in L1), whether it is held, and the age of its row
    // as read (none for a held value, which keeps its own expiry)
    private record Current<V>(@Nullable V value, long version, int epoch, boolean held, Duration age) {

        Current(@Nullable V value, long version, int epoch, boolean held) {
            this(value, version, epoch, held, Duration.ZERO);
        }
    }

    // before an operation that will put into L1 reads or writes the store (or, for a load, right after its future
    // entered L1): every message and removal from now on raises the key's minimum
    private void reserve(InternalKey<K> internalKey) {
        reservations.merge(internalKey.hash(), Reservation.ONE, Reservation::joined);
    }

    // after the operation put into L1 (or failed): what L1 holds below the minimum raised meanwhile is dropped, a
    // completed load's value included; the reservation ends with the last operation
    private void release(InternalKey<K> internalKey) {
        reservations.compute(internalKey.hash(), (hash, reservation) -> {
            if (reservation == null) {
                return null;
            }
            if (reservation.minimum() != Reservation.NONE) {
                removeIfOlder(internalKey, reservation.minimum());
            }
            return reservation.holders() == 1 ? null : reservation.left();
        });
    }

    // a write of AsyncCache.put that failed, value or removal: its caller has returned and cannot be told, so it is
    // logged, at most once a minute (an outage fails every one)
    private void asyncWriteLost(RuntimeException e) {
        long now = environment.nanoTime();
        long last = asyncWriteLostWarned.get();
        if ((last == 0 || now - last > TimeUnit.MINUTES.toNanos(1)) && asyncWriteLostWarned.compareAndSet(last, now)) {
            LOGGER.log(System.Logger.Level.WARNING, name + ": A write of AsyncCache.put failed and was dropped (its "
                    + "caller cannot be told; logged at most once a minute): " + e.getMessage());
        }
    }

    // a version seen more than a minute ahead of this instance's clock (InternalVersion): counted, and logged at most
    // once a minute
    private void clockAhead(long aheadMillis) {
        stats.increment(Counter.CLOCK_AHEAD);
        long now = environment.nanoTime();
        long last = clockAheadWarned.get();
        if ((last == 0 || now - last > TimeUnit.MINUTES.toNanos(1)) && clockAheadWarned.compareAndSet(last, now)) {
            LOGGER.log(System.Logger.Level.WARNING, name + ": A version written by a clock "
                    + Duration.ofMillis(aheadMillis) + " ahead of this instance's was seen (check the clocks of the "
                    + "instances and the store servers)"
                    + (aheadMillis > InternalVersion.FOLLOW_LIMIT_MILLIS
                    ? "; more than a day: not followed, a write on such a key goes just above it"
                    : "; followed: the versions here move ahead with it") + " (distributedPolicy().stats()"
                    + ".clockAheadCount())");
        }
    }

    // runs a task on the store executor; one it refuses (shut down, or bounded and full) runs on a thread of its own
    // instead, never in the caller's thread, which may hold Caffeine's lock of a key: the releases these tasks carry
    // must run, or a reservation would leak
    private void onStoreExecutor(Runnable task) {
        try {
            storeExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            Thread fallback = new Thread(task, "distributed-caffeine-fallback");
            fallback.setDaemon(true);
            fallback.start();
        }
    }

    // the number of keys reserved: zero whenever no operation runs (tests check it, a leak would show)
    int reservedKeys() {
        return reservations.size();
    }

    // a reservation: the operations holding it, and the least version L1 must hold for the key when they install
    // (NONE: nothing came meanwhile). An entry lives while an operation on the key is in flight: every reserve is
    // released on every path (finally, or the completion of a future, on the store executor or, refused there, on a
    // thread of its own), so the registry is empty whenever nothing runs; the one bound is a user's loader future
    // that never completes, whose key stays reserved for as long as Caffeine holds that future in L1 anyway
    private record Reservation(int holders, long minimum) {

        static final long NONE = 0;
        static final Reservation ONE = new Reservation(1, NONE);

        Reservation joined(Reservation other) {
            return new Reservation(holders + other.holders, minimum);
        }

        Reservation left() {
            return new Reservation(holders - 1, minimum);
        }

        Reservation raised(long version) {
            return new Reservation(holders, minimum == NONE ? version : InternalVersion.atLeastBoth(minimum, version));
        }
    }

    /**
     * {@code asMap().containsKey}: L1 if held and trusted; otherwise an existence check in the store (versions only,
     * nothing is held), which also confirms or drops an unconfirmed copy. A store that does not answer counts as a
     * miss, as for {@code getIfPresent}. A value this instance cannot read counts as present here, unlike for
     * {@code get}: knowing it would mean reading the value, which the existence check avoids (README, "Unreadable
     * values").
     */
    boolean containsKey(K key) {
        InternalKey<K> internalKey = wrap(key);
        CompletableFuture<InternalValue<V>> held = l1.asMap().get(internalKey);
        if (held != null && completedValue(held) != null && confirmOrDrop(internalKey, held) != null) {
            return true;
        }
        if (absenceTrusted(internalKey.hash())) {
            return false;
        }
        try {
            boolean found = readVisibleVersions(List.of(internalKey.hash())).containsKey(internalKey.hash());
            if (found && remembered(internalKey.hash())) {
                forgetAbsence(internalKey.hash()); // written elsewhere while no message could tell
            }
            return found;
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

    @SuppressWarnings("java:S1181") // whatever fails, the key is dropped locally before it is rethrown
    void invalidate(K key) {
        InternalKey<K> internalKey = wrap(key);
        Map<String, Write> tombstone = new LinkedHashMap<>(Map.of(internalKey.hash(),
                Write.tombstone(internalKey.hash(), clock.fresh())));
        try {
            advance(tombstone, false);
        } catch (RuntimeException | Error e) {
            // dropped locally whatever the store answered, at no version known: anything held goes, and nothing in
            // flight installs (dropOlder at ANYTHING)
            dropOlder(internalKey, InternalVersion.ANYTHING);
            throw e;
        }
        // only what is older than the tombstone: a value written after it (another write here, committed while this
        // one ran) stays. Removing it too left a gap in which a write between the two installed its older value, held
        // stale from then on (found by Lincheck, stress)
        dropOlder(internalKey, requireNonNull(tombstone.get(internalKey.hash())).version());
    }

    @SuppressWarnings("java:S1181") // whatever fails, the keys are dropped locally before it is rethrown
    void invalidateAll(Iterable<? extends K> keys) {
        checkOpen();
        List<InternalKey<K>> internalKeys = wrapAll(keys);
        if (internalKeys.isEmpty()) {
            return;
        }
        Map<String, Write> tombstones = new LinkedHashMap<>();
        internalKeys.forEach(internalKey -> tombstones.put(internalKey.hash(),
                Write.tombstone(internalKey.hash(), clock.fresh())));
        try {
            advance(tombstones, false);
        } catch (RuntimeException | Error e) {
            // as for one key (invalidate): at no version known, anything held goes and nothing in flight installs
            internalKeys.forEach(internalKey -> dropOlder(internalKey, InternalVersion.ANYTHING));
            throw e;
        }
        // only what is older than each tombstone, as for one key (invalidate)
        internalKeys.forEach(internalKey -> dropOlder(internalKey,
                requireNonNull(tombstones.get(internalKey.hash())).version()));
    }

    void invalidateAll() {
        checkOpen();
        Write control = Write.value(Store.CONTROL_HASH, controlBytes(keySerializer.format()),
                controlBytes(valueSerializer.format()), clock.fresh());
        clearing.lockExclusive();
        try {
            // one row, whatever the size of the cache: every row written at or before it counts as missing from now
            // on, and maintenance deletes those rows (VISION.md, "Target design: invalidateAll by the store's time,
            // hidden on read"). The atomic map operations of this instance wait for it (clearing); a load in flight
            // that read before it and commits after it survives it, as in Caffeine, and so does whatever another
            // instance decided before hearing of it, as in the other distributed caches
            Map<String, Write> written = new LinkedHashMap<>(Map.of(Store.CONTROL_HASH, control));
            advance(written, false);
            // its own echo then clears nothing again (at the version the store applied it at)
            controlVersion.accumulateAndGet(requireNonNull(written.get(Store.CONTROL_HASH)).version(), Math::max);
        } finally {
            // a load started before completes with an older epoch, so it is checked on its next read
            epoch.incrementAndGet();
            l1.synchronous().invalidateAll();
            clearing.unlockExclusive();
            distrustWithinAllowance();
        }
        // a put after this call returns must be written after the control row by the store's clock, also where that is
        // a wall clock stepping back a little or counting in milliseconds: waited out here, so that no put pays for it
        // (in nanoseconds: a clock counting in microseconds allows for less than a millisecond, which is still more
        // than nothing)
        long allowance = store.clockAllowance().toNanos();
        if (allowance > 0) {
            try {
                environment.sleep(Duration.ofNanos(allowance));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // the writes right after an invalidateAll applied here (its own, or announced): a write committed after the control
    // row but within the store clock's allowance of it can carry the same write time (a clock counting in
    // milliseconds) or an earlier one (a clock stepping back), and the store hides it like a row written before the
    // control row, while the instance that wrote or loaded it holds its value, for good (found by the property test on
    // the mongodb profile). The allowance after invalidateAll returns covers only writes after it. So the epoch moves
    // once more, one allowance after this instance applied it: everything installed meanwhile, by an operation that
    // began before then, is untrusted and checked against the store on its next read (one row read), a hidden row
    // dropped. An operation beginning later commits more than the allowance after the control row (applied here no
    // earlier than it was written) and is stamped after it. A clock without allowance (monotonic) needs none of this
    private void distrustWithinAllowance() {
        Duration allowance = store.clockAllowance();
        if (!allowance.isZero() && !allowance.isNegative()) {
            InternalMaintenance.scheduleOnce(environment, epoch::incrementAndGet, allowance);
        }
    }

    // the key and value of the control row: valid in every format (JSON's null), never read
    private static byte[] controlBytes(StoreSettings.Format format) {
        return format == StoreSettings.Format.JSON ? "null".getBytes(StandardCharsets.UTF_8) : new byte[0];
    }

    // the store step of put and putAll (VISION.md, "Target design: monotonic versions"): every key advances, at a
    // version larger than every one this instance has seen, so that one write is enough unless a larger version is
    // stored already (see advance). With an unknown outcome the keys are dropped from L1 (ordering rule 5). Returns
    // the version written per hash
    private Map<String, Long> putValues(Map<InternalKey<K>, V> values) {
        Map<String, Write> writes = new LinkedHashMap<>();
        values.forEach((internalKey, value) -> {
            forgetAbsence(internalKey.hash());
            writes.put(internalKey.hash(), valueWrite(internalKey, value, clock.fresh()));
        });
        try {
            advance(writes, true);
            stats.add(Counter.PUT, writes.size());
        } catch (StoreException e) {
            if (e.isOutcomeUnknown()) {
                // at no version known: anything held goes, and nothing in flight installs (see invalidate)
                values.keySet().forEach(internalKey -> dropOlder(internalKey, InternalVersion.ANYTHING));
            }
            throw e;
        }
        Map<String, Long> versions = new LinkedHashMap<>();
        writes.forEach((hash, write) -> versions.put(hash, write.version()));
        return versions;
    }

    /**
     * Writes that advance (puts, tombstones, the control row), until each is applied: a refusal means the store holds
     * a version at least as large already, written by a writer whose clock is ahead or that raced this one. The row is
     * read (which moves the clock past its version), and the write proposed again, above; a row at exactly the
     * write's version is the write itself, applied by an attempt whose answer was lost. Each refusal is another
     * writer's progress, so it ends; bounded anyway, as a store failure. Replaces each write in the map by the one
     * applied.
     *
     * @param put whether through the guard as a put (not retried with an unknown outcome) or as a write
     */
    // the key and the value are nullable arrays (byte @Nullable []), which Sonar does not read
    @SuppressWarnings("java:S4449")
    private void advance(Map<String, Write> writes, boolean put) {
        Collection<Write> remaining = new ArrayList<>(writes.values());
        for (int attempt = 1; !remaining.isEmpty(); attempt++) {
            if (attempt > ADVANCE_ATTEMPTS) {
                throw new StoreException("A write was refused " + ADVANCE_ATTEMPTS + " times, each time by a newer "
                        + "version of the key written concurrently; it can be tried again", null, false, true);
            }
            Collection<Write> attempted = remaining;
            WriteResult result = put ? guard.put(() -> store.write(attempted))
                    : guard.write(() -> store.write(attempted));
            // applied at the version the store reports: its own, where it assigns one (VISION.md, "Target design:
            // versions applied as well as each store can")
            for (Write write : attempted) {
                if (!result.isRefused(write) && result.version(write) != write.version()) {
                    writes.put(write.hash(), write.at(result.version(write)));
                }
            }
            Set<String> refused = result.refused();
            if (refused.isEmpty()) {
                return;
            }
            stats.add(Counter.VERSION_REFUSAL, refused.size());
            Map<String, Row> rows = readRows(refused);
            List<Write> again = new ArrayList<>();
            for (String hash : refused) {
                Write write = requireNonNull(writes.get(hash));
                Row row = rows.get(hash);
                if (row != null && row.version() == write.version()
                        && (row.state() == State.TOMBSTONE) == write.isTombstone()
                        && Arrays.equals(row.value(), write.value())) {
                    // this write landed after all (its first attempt, its outcome unknown, was retried). The content
                    // decides, not the version alone: two instances can propose the same version (the same
                    // millisecond and counter, and node bits drawn alike, one pair in 1,024), the second refused as
                    // not larger; taken by the version alone, its put was lost (found by Lincheck with two instances).
                    // The same content at the same version is the same state, whoever wrote it
                    continue;
                }
                // above the row read, also where it lies too far ahead for the clock to follow it
                Write above = new Write(hash, write.key(), write.value(),
                        row == null ? clock.fresh() : clock.next(row.version()), Write.ADVANCE);
                writes.put(hash, above);
                again.add(above);
            }
            remaining = again;
        }
    }

    // puts the value into L1 unless L1 holds a completed value at least as new (ordering rule 3)
    private void commit(InternalKey<K> internalKey, InternalValue<V> value) {
        install(internalKey, value, true, true);
    }

    // single key: the owner of the load runs it in its own thread, outside Caffeine's lock

    // a future of L1 completes with null where nothing was found (see joinHeld)
    @SuppressWarnings({"unchecked", "java:S2589", "java:S2583"})
    private @Nullable InternalValue<V> getOrLoad(InternalKey<K> internalKey,
                                                 @Nullable SingleLoader<K, V> singleLoader) {
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
                return value == null ? null : new InternalValue<>(value, InternalVersion.UNCONFIRMED, UNCONFIRMED);
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
    // the callback completing the owned future is attached for its effect; whether L1 still holds that very future;
    // whatever fails, the reservation is released before it is rethrown
    @SuppressWarnings({"FutureReturnValueIgnored", "ReferenceEquality", "java:S1181"})
    private void runLoad(InternalKey<K> internalKey, CompletableFuture<InternalValue<V>> owned,
                         @Nullable AsyncSingleLoader<K, V> singleLoader, Executor steps) {
        Set<InternalKey<?>> loading = LOADING.get();
        loading.add(internalKey);
        // reserved until the owned future completed and its value is held: a message meanwhile raises the minimum it
        // must reach, checked at the release
        reserve(internalKey);
        CompletableFuture<LoadResult<V>> result;
        try {
            result = load(internalKey, singleLoader, owned, steps);
        } catch (RuntimeException | Error e) {
            release(internalKey);
            throw e;
        } finally {
            loading.remove(internalKey);
        }
        result.whenComplete((loaded, failure) -> {
            try {
                completeLoad(internalKey, owned, loaded, failure);
            } finally {
                release(internalKey);
            }
        });
    }

    // the owner completes its future in L1 with what the load found
    @SuppressWarnings("ReferenceEquality") // whether L1 still holds this very future
    private void completeLoad(InternalKey<K> internalKey, CompletableFuture<InternalValue<V>> owned,
                              @Nullable LoadResult<V> result, @Nullable Throwable failure) {
        if (failure != null) {
            owned.completeExceptionally(cause(failure));
            return;
        }
        LoadResult<V> loaded = requireNonNull(result);
        if (loaded.value == null) {
            completeAbsent(owned);
        } else {
            owned.complete(loaded.value.bind(internalKey));
        }
        if (!loaded.cacheable) {
            l1.asMap().remove(internalKey, owned);
        } else if (loaded.value != null && l1.asMap().get(internalKey) != owned) {
            // replaced while in flight (a write's commit replaces a load): the loaded value may be the newer one, read
            // from the store after that write's; held under the version rule, a removal kept
            install(internalKey, loaded.value, false, false);
        }
        if (loaded.cacheable && loaded.value != null && needsRefresh(loaded.age)) {
            refreshLater(internalKey);
        }
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
                // checked also while the guard is open, as its probe (see confirmOrDrop)
                if (value != null && isTrusted(value)) {
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
        // checked also while the guard is open, as its probe (see confirmOrDrop)
        if (value == null || isTrusted(value)) {
            return future;
        }
        return this.<@Nullable InternalValue<V>>supply(() -> confirmOrDrop(internalKey, future))
                .thenCompose(confirmed -> confirmed != null ? completedWith(confirmed)
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
        CompletableFuture<@Nullable Void> checked = untrusted
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
    void putAsync(K key, CompletableFuture<? extends @Nullable V> valueFuture) {
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
        // not reserved while the future is pending: a message or a removal meanwhile drops it from L1 (a future in
        // flight not reserved, see apply and dropOlder), so nothing outdated is held, and a future never completed
        // holds no reservation. Reserved for the store work once it completed, released whatever happens
        l1.put(internalKey, owned);
        valueFuture.whenComplete((value, failure) -> {
            if (failure != null || value == null) {
                onStoreExecutor(() -> {
                    reserve(internalKey);
                    try {
                        advance(new LinkedHashMap<>(Map.of(internalKey.hash(),
                                Write.tombstone(internalKey.hash(), clock.fresh()))), false);
                    } catch (RuntimeException e) {
                        // the held future is removed anyway; the previous value may survive elsewhere, and here once
                        // the store is read again
                        asyncWriteLost(e);
                    } finally {
                        if (failure != null) {
                            owned.completeExceptionally(failure);
                        } else {
                            completeAbsent(owned);
                        }
                        release(internalKey);
                    }
                });
            } else {
                onStoreExecutor(() -> {
                    reserve(internalKey);
                    int writeEpoch = epoch.get();
                    try {
                        long version = requireNonNull(putValues(Map.of(internalKey, value))
                                .get(internalKey.hash()));
                        if (completedWhenPut) {
                            caffeineStats.skipLoadSuccess();
                        }
                        owned.complete(new InternalValue<>(value, version, writeEpoch).bind(internalKey));
                    } catch (StoreException e) {
                        // a write that failed, which the caller of put cannot be told: the key is absent here, as on
                        // the other instances (or as written, if the outcome is unknown: read from the store), so
                        // that readers waiting on it do not fail for a write, but load
                        asyncWriteLost(e);
                        completeAbsent(owned);
                    } catch (RuntimeException | Error e) {
                        owned.completeExceptionally(e);
                    } finally {
                        release(internalKey);
                    }
                });
            }
        });
    }

    /**
     * A load: reads the store, calls the source on a miss, writes conditionally on what it read; starts over after
     * an invalidation in between, at most loadAttempts times (three), after which the loaded value is returned but not
     * cached. The condition is what keeps a slow load from writing a value outdated by an invalidation during it (the
     * stale set); the bound keeps a load under a storm of invalidations from calling the source without end. Returning
     * the value uncached is what Caffeine's AsyncCache, Hazelcast's and Infinispan's near caches and Redis's
     * client-side caching do after the first invalidation (VISION.md, "The uncached load"); counted
     * (UNCACHED_LOAD), as it shows a key removed about as often as it loads.
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
        boolean remembered = false;
        if (fromSource) {
            // a refresh always asks the source (refresh rule 1), whose answer supersedes the absence
            forgetAbsence(hash);
        } else {
            remembered = remembered(hash);
            if (remembered && feedUp()) {
                stats.increment(Counter.NEGATIVE_HIT);
                return CompletableFuture.completedFuture(LoadResult.absent());
            }
        }
        boolean checked = remembered; // an absence while the feed is down: checked against the store, as a held value
        return on(steps, () -> {
            try {
                return new Read(readRows(List.of(hash)).get(hash), false);
            } catch (StoreException e) {
                return new Read(null, true);
            }
        }).thenCompose(read -> {
            if (read.storeDown()) {
                if (checked) {
                    // served unconfirmed, as a held value is while the store does not answer
                    stats.increment(Counter.NEGATIVE_HIT);
                    return CompletableFuture.completedFuture(LoadResult.absent());
                }
                return sourceOnly(internalKey, singleLoader, owned, loadEpoch);
            }
            Row row = read.row();
            V found = fromSource ? null : valueOf(row);
            if (row != null && found != null) {
                if (checked) {
                    forgetAbsence(hash); // written elsewhere while no message could tell
                }
                stats.increment(Counter.STORE_HIT);
                return CompletableFuture.completedFuture(
                        LoadResult.cached(new InternalValue<>(found, row.version(), loadEpoch, row.age()),
                                row.age()));
            }
            if (checked) {
                // the store holds no value either: the absence stands
                stats.increment(Counter.NEGATIVE_HIT);
                return CompletableFuture.completedFuture(LoadResult.absent());
            }
            if (!fromSource) {
                stats.increment(Counter.STORE_MISS);
            }
            return attempt(internalKey, singleLoader, owned, steps, loadEpoch, row, 0, null, 0, fromSource);
        });
    }

    private record Read(@Nullable Row row, boolean storeDown) {
    }

    // one attempt: the source, then the conditional write on what was read (the row of the previous conflict)
    private CompletableFuture<LoadResult<V>> attempt(InternalKey<K> internalKey,
                                                     @Nullable AsyncSingleLoader<K, V> singleLoader,
                                                     @Nullable CompletableFuture<InternalValue<V>> owned,
                                                     Executor steps, int loadEpoch, @Nullable Row row, int attempt,
                                                     @Nullable V lastLoaded, long lastVersion, boolean fromSource) {
        if (attempt == loadAttempts) {
            stats.increment(Counter.UNCACHED_LOAD);
            return CompletableFuture.completedFuture(
                    LoadResult.uncached(valueAt(requireNonNull(lastLoaded), lastVersion, epoch.get())));
        }
        if (singleLoader == null) {
            return CompletableFuture.completedFuture(LoadResult.absent());
        }
        long seen = row == null ? 0 : row.version();
        stats.increment(Counter.SOURCE_LOAD);
        return call(() -> singleLoader.load(internalKey.key())).thenComposeAsync(loaded -> {
            if (loaded == null) {
                if (fromSource && seen != 0) {
                    // a refresh finding nothing at the source removes the value, as Caffeine's refresh does; while
                    // the store does not answer, nothing is held here anyway (see asyncReload)
                    try {
                        guard.write(() -> store.write(List.of(Write.tombstone(internalKey.hash(),
                                clock.next(seen)).ifVersion(seen))));
                        stats.increment(Counter.CONDITIONAL_WRITE);
                    } catch (StoreException e) {
                        // absent here; the other instances keep theirs until the store is back
                    }
                }
                rememberAbsence(internalKey, owned, loadEpoch);
                return CompletableFuture.completedFuture(LoadResult.absent());
            }
            Written<V> written = writeLoaded(internalKey, loaded, seen, loadEpoch);
            LoadResult<V> result = written.result();
            if (result != null) {
                return CompletableFuture.completedFuture(result);
            }
            // starting over: read again, for the row the next decision is based on (a conflict reports no row)
            int againEpoch = epoch.get();
            Map<String, Row> again;
            try {
                again = readRows(List.of(internalKey.hash()));
            } catch (StoreException e) {
                return CompletableFuture.completedFuture(
                        LoadResult.cached(valueAt(loaded, InternalVersion.UNCONFIRMED, UNCONFIRMED),
                                Duration.ZERO));
            }
            Row current = again.get(internalKey.hash());
            V found = valueOf(current);
            if (current != null && found != null) {
                // a newer value landed meanwhile: it wins
                return CompletableFuture.completedFuture(
                        LoadResult.cached(new InternalValue<>(found, current.version(), againEpoch, current.age()),
                                current.age()));
            }
            return attempt(internalKey, singleLoader, owned, steps, againEpoch, current, attempt + 1, loaded,
                    written.version(), fromSource);
        }, steps);
    }

    // the outcome of writing a loaded value: a result, or none after a conflict, to start over; and the version tried
    private record Written<V>(@Nullable LoadResult<V> result, long version) {
    }

    // the store step after the source: writes conditionally on the version seen (ordering rule 6), at a version above
    // it; held at the epoch before the read. A conflict means a write or an
    // invalidation elsewhere since the read: what was loaded may be outdated, so the load starts over
    private Written<V> writeLoaded(InternalKey<K> internalKey, V loaded, long seen, int loadEpoch) {
        long version = seen == 0 ? clock.fresh() : clock.next(seen);
        Write write = valueWrite(internalKey, loaded, version);
        long applied;
        try {
            applied = writeConditionally(seen == 0 ? write.ifMissing() : write.ifVersion(seen));
        } catch (StoreException e) {
            return new Written<>(LoadResult.cached(new InternalValue<>(loaded, InternalVersion.UNCONFIRMED,
                    UNCONFIRMED), Duration.ZERO), version);
        }
        if (applied != REFUSED) {
            return new Written<>(LoadResult.cached(new InternalValue<>(loaded, applied, loadEpoch),
                    Duration.ZERO), applied);
        }
        return new Written<>(null, version);
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
        if (absences == null || owned == null) {
            return;
        }
        String hash = internalKey.hash();
        absences.put(hash, Boolean.TRUE);
        Reservation reservation = reservations.get(hash);
        if ((reservation != null && reservation.minimum() != Reservation.NONE) || l1.asMap().get(internalKey) != owned
                || epoch.get() != loadEpoch) {
            absences.invalidate(hash);
        }
    }

    // the store does not answer: misses go to the source alone, and the result is held but never trusted, so the
    // first read with the store back checks it (it carries the lowest version, so any message drops it). An absence is
    // remembered as after a store read: served while the feed delivers, checked against the store otherwise, and
    // cleared by the gap the feed reports when it resumes
    private CompletableFuture<LoadResult<V>> sourceOnly(InternalKey<K> internalKey,
                                                        @Nullable AsyncSingleLoader<K, V> singleLoader,
                                                        @Nullable CompletableFuture<InternalValue<V>> owned,
                                                        int loadEpoch) {
        if (singleLoader == null) {
            return CompletableFuture.completedFuture(LoadResult.absent());
        }
        stats.increment(Counter.SOURCE_LOAD);
        return call(() -> singleLoader.load(internalKey.key())).thenApply(loaded -> {
            if (loaded == null) {
                rememberAbsence(internalKey, owned, loadEpoch);
                return LoadResult.absent();
            }
            return LoadResult.cached(valueAt(loaded, InternalVersion.UNCONFIRMED, UNCONFIRMED), Duration.ZERO);
        });
    }

    // a write decided on a version: conditioned on it, or on no row where nothing is known (no row read, or a value
    // no store confirmed: UNCONFIRMED is 0, and a row found then means the decision was not based on it)
    private static Write basedOn(Write write, long version) {
        return version == 0 ? write.ifMissing() : write.ifVersion(version);
    }

    // the conditional write of an atomic map method. Repeated like any conditional write, but a repetition after an
    // attempt whose outcome was unknown that is refused may be refused by that attempt's own row: decided again on it,
    // the remapping would be applied twice (merge adding twice). So the row is read: at the write's version with its
    // content, it is the write itself (as in advance), which counts as applied there; otherwise the call throws, its
    // outcome unknown, as a put's. That happens only where the store applied a version of its own (PostgreSQL, Redis,
    // while MongoDB mostly takes the one proposed) or another writer raced the lost answer. Two instances proposing the
    // same version and content (see advance) would read as one write here, one remapping lost: accepted, as it needs a
    // lost answer and a race in the same millisecond. A load's or a refresh's write needs none of this: deciding again
    // on its own row returns the value it wrote
    private long decide(Write write) {
        boolean[] unknown = {false};
        WriteResult result = guard.write(() -> store.write(List.of(write)), () -> unknown[0] = true);
        stats.increment(Counter.CONDITIONAL_WRITE);
        if (!result.isRefused(write)) {
            return result.version(write);
        }
        if (unknown[0]) {
            Row row = readRows(List.of(write.hash())).get(write.hash());
            if (row != null && row.version() == write.version()
                    && (row.state() == State.TOMBSTONE) == write.isTombstone()
                    && Arrays.equals(row.value(), write.value())) {
                return write.version();
            }
            stats.increment(Counter.UNKNOWN_OUTCOME);
            throw new StoreException("The store's answer to a write was lost, and repeating the write was refused, by "
                    + "the write itself or by a concurrent one: its outcome is unknown", null, true, false);
        }
        stats.increment(Counter.CONFLICT);
        return REFUSED;
    }

    // a conditional write through the guard; returns the version it was applied at (the store's, where it assigns
    // one), or REFUSED
    private long writeConditionally(Write write) {
        WriteResult result = guard.write(() -> store.write(List.of(write)));
        stats.increment(Counter.CONDITIONAL_WRITE);
        if (result.isRefused(write)) {
            stats.increment(Counter.CONFLICT);
            return REFUSED;
        }
        return result.version(write);
    }

    // a read through the guard, the control row read with it in the same batch (VISION.md, "Target design:
    // invalidateAll by the store's time, hidden on read"): a row at least as old as the control row was written at or
    // before the last invalidateAll and counts as missing, shown as a tombstone at its version, so that a conditional
    // write can replace it. The ages of one read are measured at the same instant, so they compare like the times of
    // the writes
    private Map<String, Row> readRows(Collection<String> hashes) {
        Map<String, Row> read = guard.read(() -> store.read(withControl(hashes), maximumAge));
        Row control = read.get(Store.CONTROL_HASH);
        Map<String, Row> rows = new LinkedHashMap<>();
        read.forEach((hash, row) -> {
            clock.observe(row.version());
            if (!hash.equals(Store.CONTROL_HASH)) {
                rows.put(hash, control == null || row.age().compareTo(control.age()) < 0 ? row
                        : new Row(hash, row.key(), null, State.TOMBSTONE, row.version(), row.age()));
            }
        });
        return rows;
    }

    // the versions of value rows not hidden by the control row; a control row older than the maximum age is not
    // reported, but neither is any row it hides, which is older still
    private Map<String, Long> readVisibleVersions(Collection<String> hashes) {
        VersionsResult read = guard.read(() -> store.readVersions(withControl(hashes), maximumAge));
        Duration control = read.ages().get(Store.CONTROL_HASH);
        Map<String, Long> versions = new LinkedHashMap<>();
        read.versions().forEach((hash, version) -> {
            clock.observe(version);
            if (!hash.equals(Store.CONTROL_HASH)
                    && (control == null || requireNonNull(read.ages().get(hash)).compareTo(control) < 0)) {
                versions.put(hash, version);
            }
        });
        return versions;
    }

    private static List<String> withControl(Collection<String> hashes) {
        List<String> withControl = new ArrayList<>(hashes.size() + 1);
        withControl.addAll(hashes);
        withControl.add(Store.CONTROL_HASH);
        return withControl;
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
        BulkOutcome<K, V> outcome = new BulkOutcome<>(ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(),
                ConcurrentHashMap.newKeySet());
        @Nullable CompletableFuture<Map<InternalKey<K>, InternalValue<V>>>[] owned = new CompletableFuture[1];
        CompletableFuture<Map<InternalKey<K>, InternalValue<V>>> future = l1.getAll(internalKeys, (keys, executor) -> {
            missing[0] = new LinkedHashSet<>(keys);
            CompletableFuture<Map<InternalKey<K>, InternalValue<V>>> created = new CompletableFuture<>();
            owned[0] = created;
            return created;
        });
        CompletableFuture<Map<InternalKey<K>, InternalValue<V>>> created = owned[0];
        if (created != null) {
            // reserved until the futures in L1 completed, as for a single load (see runLoad)
            Set<InternalKey<K>> loading = missing[0];
            loading.forEach(this::reserve);
            // each key's own future in L1, which Caffeine put there before asking for the bulk one: what an absence
            // is checked against (rememberAbsence), recorded before Caffeine completes and removes them
            int loadEpoch = epoch.get();
            Map<InternalKey<K>, CompletableFuture<InternalValue<V>>> keyFutures = new HashMap<>();
            if (absences != null) {
                loading.forEach(internalKey -> {
                    CompletableFuture<InternalValue<V>> keyFuture = l1.asMap().get(internalKey);
                    if (keyFuture != null) {
                        keyFutures.put(internalKey, keyFuture);
                    }
                });
            }
            loadAll(loading, bulkLoader, steps, outcome).whenComplete((loaded, failure) -> {
                try {
                    if (failure != null) {
                        created.completeExceptionally(cause(failure));
                    } else {
                        outcome.absent().forEach(internalKey ->
                                rememberAbsence(internalKey, keyFutures.get(internalKey), loadEpoch));
                        loaded.forEach((internalKey, value) -> value.bind(internalKey));
                        outcome.uncached().forEach((internalKey, value) -> value.bind(internalKey));
                        created.complete(loaded);
                    }
                } finally {
                    loading.forEach(this::release);
                }
            });
        }
        return future.thenApply(values -> {
            // refresh rule 2, as for a single load (see completeLoad): once Caffeine holds the values, since a refresh
            // of a key still in flight would only join its load
            outcome.stale().forEach(internalKey -> {
                if (values.containsKey(internalKey)) {
                    refreshLater(internalKey);
                }
            });
            // a key whose load gave up is answered as a single get answers it, with its value left uncached: added
            // to a copy, as Caffeine installs every entry of the bulk result in L1
            if (outcome.uncached().isEmpty()) {
                return values;
            }
            Map<InternalKey<K>, InternalValue<V>> answered = new HashMap<>(values);
            answered.putAll(outcome.uncached());
            return answered;
        });
    }

    // what a bulk load leaves to its caller besides the values to hold: the keys neither the store nor the source has
    // a value for, to be remembered (negative caching); the keys whose loads gave up after repeated removals (see
    // attempt), with the last value loaded; and the keys whose values read from the store are due for a refresh
    private record BulkOutcome<K, V>(Set<InternalKey<K>> absent, Map<InternalKey<K>, InternalValue<V>> uncached,
                                     Set<InternalKey<K>> stale) {
    }

    // one read for all keys, the source for the rest, one conditional write; keys that conflict start over one by
    // one. Extra keys the source returns unasked were never read, so they are written only where no row exists (a
    // tombstone or an expired row means the key was touched meanwhile, so the source value may be outdated), and held
    // by the core itself under the write registry rather than handed to Caffeine, which would insert them without the
    // version rule and unseen by messages announced during the load. Steps as for single keys
    // outcome: filled for the caller (see BulkOutcome). Absences are answered as for single keys (see load): trusted
    // ones at once, others after the store
    private CompletableFuture<Map<InternalKey<K>, InternalValue<V>>> loadAll(Set<InternalKey<K>> requested,
                                                                           @Nullable AsyncBulkLoader<K, V> bulkLoader,
                                                                           Executor steps,
                                                                           BulkOutcome<K, V> outcome) {
        Set<InternalKey<K>> internalKeys = new LinkedHashSet<>();
        Set<InternalKey<K>> checked = new HashSet<>();
        for (InternalKey<K> internalKey : requested) {
            if (absenceTrusted(internalKey.hash())) {
                stats.increment(Counter.NEGATIVE_HIT);
                continue;
            }
            if (remembered(internalKey.hash())) {
                checked.add(internalKey);
            }
            internalKeys.add(internalKey);
        }
        if (internalKeys.isEmpty()) {
            return CompletableFuture.completedFuture(new LinkedHashMap<>());
        }
        return on(steps, () -> readAll(internalKeys, checked, outcome)).thenCompose(read -> {
            if (read == null) {
                // served unconfirmed, as held values are while the store does not answer
                stats.add(Counter.NEGATIVE_HIT, checked.size());
                Set<InternalKey<K>> unchecked = new LinkedHashSet<>(internalKeys);
                unchecked.removeAll(checked);
                return unchecked.isEmpty() ? CompletableFuture.completedFuture(new LinkedHashMap<>())
                        : sourceOnly(unchecked, bulkLoader, outcome.absent());
            }
            if (read.toLoad().isEmpty() || bulkLoader == null) {
                return CompletableFuture.completedFuture(read.result());
            }
            Map<K, InternalKey<K>> byKey = new LinkedHashMap<>();
            read.toLoad().keySet().forEach(internalKey -> byKey.put(internalKey.key(), internalKey));
            stats.add(Counter.SOURCE_LOAD, byKey.size());
            return call(() -> bulkLoader.loadAll(byKey.keySet())).thenComposeAsync(loaded -> {
                Map<? extends @Nullable K, ? extends @Nullable V> found =
                        requireNonNull(loaded, "loadAll returned null");
                read.toLoad().keySet().forEach(internalKey -> {
                    if (found.get(internalKey.key()) == null) {
                        outcome.absent().add(internalKey);
                    }
                });
                List<InternalKey<K>> conflicting = writeAll(internalKeys, read, byKey, found);
                if (conflicting.isEmpty()) {
                    return CompletableFuture.completedFuture(read.result());
                }
                AsyncSingleLoader<K, V> single = key -> call(() -> bulkLoader.loadAll(Set.of(key)))
                        .thenApply(values -> values.get(key));
                List<CompletableFuture<Void>> retries = conflicting.stream().map(internalKey ->
                    load(internalKey, single, null, steps, false).thenAccept(retried -> {
                        if (retried.value == null) {
                            return;
                        }
                        if (retried.cacheable) {
                            synchronized (read.result()) {
                                read.result().put(internalKey, retried.value);
                            }
                            if (needsRefresh(retried.age)) {
                                outcome.stale().add(internalKey);
                            }
                        } else {
                            outcome.uncached().put(internalKey, retried.value);
                        }
                    })).toList();
                return CompletableFuture.allOf(retries.toArray(new CompletableFuture[0]))
                        .thenApply(done -> read.result());
            }, steps);
        });
    }

    // what one read found: the values found, the version seen per key still to load, and the epoch before it (theirs
    // in L1)
    private record BulkRead<K, V>(Map<InternalKey<K>, InternalValue<V>> result, Map<InternalKey<K>, Long> toLoad,
                                  int epoch) {
    }

    // the store step before the source; null if the store does not answer. checked: keys remembered absent while the
    // feed is down, loaded by nobody: a value in the store ends the absence, none confirms it. A value found that is
    // due for a refresh is added to the outcome's stale keys
    private @Nullable BulkRead<K, V> readAll(Set<InternalKey<K>> internalKeys, Set<InternalKey<K>> checked,
                                             BulkOutcome<K, V> outcome) {
        int readEpoch = epoch.get();
        Map<String, Row> rows;
        try {
            rows = readRows(hashes(internalKeys));
        } catch (StoreException e) {
            return null;
        }
        Map<InternalKey<K>, InternalValue<V>> result = new LinkedHashMap<>();
        Map<InternalKey<K>, Long> toLoad = new LinkedHashMap<>();
        for (InternalKey<K> internalKey : internalKeys) {
            Row row = rows.get(internalKey.hash());
            V value = valueOf(row);
            if (row != null && value != null) {
                if (checked.contains(internalKey)) {
                    forgetAbsence(internalKey.hash()); // written elsewhere while no message could tell
                }
                result.put(internalKey, new InternalValue<>(value, row.version(), readEpoch, row.age()));
                if (needsRefresh(row.age())) {
                    outcome.stale().add(internalKey);
                }
            } else if (checked.contains(internalKey)) {
                stats.increment(Counter.NEGATIVE_HIT);
            } else {
                toLoad.put(internalKey, row == null ? 0L : row.version());
            }
        }
        stats.add(Counter.STORE_HIT, result.size());
        stats.add(Counter.STORE_MISS, toLoad.size());
        return new BulkRead<>(result, toLoad, readEpoch);
    }

    // the store step after the source: one conditional write; fills the result and returns the keys to start over
    private List<InternalKey<K>> writeAll(Set<InternalKey<K>> internalKeys, BulkRead<K, V> read,
                                          Map<K, InternalKey<K>> byKey,
                                          Map<? extends @Nullable K, ? extends @Nullable V> loaded) {
        Map<InternalKey<K>, InternalValue<V>> result = read.result();
        Map<InternalKey<K>, Long> toLoad = read.toLoad();
        List<Write> writes = new ArrayList<>();
        List<InternalKey<K>> extras = new ArrayList<>();
        Map<String, InternalKey<K>> written = new LinkedHashMap<>();
        Map<String, V> values = new LinkedHashMap<>();
        Map<String, Long> versions = new LinkedHashMap<>();
        // the user's map: a null key or value in it is skipped
        loaded.forEach((key, value) -> {
            if (key == null || value == null) {
                return;
            }
            InternalKey<K> internalKey = byKey.containsKey(key) ? byKey.get(key) : hasher.wrap(key);
            if (internalKeys.contains(internalKey) && !toLoad.containsKey(internalKey)) {
                return; // already answered by the store
            }
            Long seen = toLoad.get(internalKey);
            if (seen == null) {
                extras.add(internalKey);
            }
            long version = seen == null || seen == 0 ? clock.fresh() : clock.next(seen);
            Write write = valueWrite(internalKey, value, version);
            writes.add(seen == null || seen == 0 ? write.ifMissing() : write.ifVersion(seen));
            written.put(internalKey.hash(), internalKey);
            values.put(internalKey.hash(), value);
            versions.put(internalKey.hash(), version);
        });
        // the extras are writes in flight until they are held (see release)
        int writeEpoch = read.epoch();
        extras.forEach(this::reserve);
        Set<String> conflicts;
        try {
            try {
                WriteResult outcome = writes.isEmpty() ? WriteResult.allApplied()
                        : guard.write(() -> store.write(writes));
                conflicts = outcome.refused();
                // at the versions the store reports
                writes.forEach(write -> versions.put(write.hash(), outcome.version(write)));
            } catch (StoreException e) {
                values.forEach((hash, value) -> {
                    InternalKey<K> internalKey = written.get(hash);
                    if (toLoad.containsKey(internalKey)) {
                        result.put(internalKey, new InternalValue<>(value, InternalVersion.UNCONFIRMED, UNCONFIRMED));
                    }
                });
                return List.of();
            }
            stats.add(Counter.CONDITIONAL_WRITE, writes.size());
            stats.add(Counter.CONFLICT, conflicts.size());
            written.forEach((hash, internalKey) -> {
                if (!conflicts.contains(hash) && !toLoad.containsKey(internalKey)) {
                    hold(internalKey, new InternalValue<>(requireNonNull(values.get(hash)),
                            requireNonNull(versions.get(hash)), writeEpoch));
                }
            });
        } finally {
            extras.forEach(this::release);
        }
        // a requested key that conflicted was written or invalidated elsewhere since the read: loaded again on its own,
        // which reads it first
        List<InternalKey<K>> conflicting = new ArrayList<>();
        written.forEach((hash, internalKey) -> {
            if (!toLoad.containsKey(internalKey)) {
                return;
            }
            if (conflicts.contains(hash)) {
                conflicting.add(internalKey);
            } else {
                result.put(internalKey, new InternalValue<>(requireNonNull(values.get(hash)),
                        requireNonNull(versions.get(hash)), writeEpoch));
            }
        });
        return conflicting;
    }

    // messages (ordering rules 2 and 4): applied only to held values older than the announced version; a message
    // below the held version makes the store decide (see apply)

    // with the maximum age for the store only, an expiry is the end of the maximum age in the store, not a change:
    // counted and ignored, the copies stay (see read for the atomic methods)
    private boolean ignoredExpiry(Change message) {
        if (!maximumAgeForStoreOnly || message.kind() != Change.Kind.EXPIRED) {
            return false;
        }
        stats.increment(Counter.EXPIRED_MESSAGE);
        stats.increment(Counter.IGNORED_MESSAGE);
        return true;
    }

    // a message of the heartbeat or of the control row, applied: false for a message of a key
    private boolean appliedAsSignal(Change message) {
        if (InternalHeartbeat.HASH.equals(message.hash())) {
            if (heartbeat != null) {
                heartbeat.received(message.version());
            }
            return true;
        }
        if (!Store.CONTROL_HASH.equals(message.hash())) {
            return false;
        }
        // the control row written: an invalidateAll, here or elsewhere; once per version. Safe to skip one applied
        // already: whatever L1 holds now was decided after that control row was written (a message of an earlier
        // write is older than what is held, and a read hides the rows written before it)
        if (controlVersion.getAndAccumulate(message.version(), Math::max) >= message.version()) {
            stats.increment(Counter.IGNORED_MESSAGE);
            return true;
        }
        stats.increment(Counter.INVALIDATED_ALL_MESSAGE);
        // the epoch too, not only the clear: a read through the store (getIfPresent) installs after it read, with no
        // future in L1 the clear would remove, so a value it read before the cut-off would stay trusted (found by
        // InternalInterleavingTest, whose scenario of loads alone did not need it)
        epoch.incrementAndGet();
        l1.synchronous().invalidateAll();
        distrustWithinAllowance();
        return true;
    }

    @SuppressWarnings("FutureReturnValueIgnored") // the check of a load in flight is attached for its effect
    private void apply(List<Change> messages) {
        // read once per batch and key, whatever the number of messages for it (duplicates, several writes)
        Set<String> toRead = new LinkedHashSet<>();
        Map<InternalKey<K>, CompletableFuture<InternalValue<V>>> toCheck = new LinkedHashMap<>();
        for (Change message : messages) {
            clock.observe(message.version());
            if (appliedAsSignal(message) || ignoredExpiry(message)) {
                continue;
            }
            stats.increment(switch (message.kind()) {
                case WRITTEN -> Counter.WRITTEN_MESSAGE;
                case INVALIDATED -> Counter.INVALIDATED_MESSAGE;
                case EXPIRED -> Counter.EXPIRED_MESSAGE;
            });
            String hash = message.hash();
            long version = message.version();
            // before L1 is looked at, so that an operation in flight either sees the minimum or has installed already
            boolean reserved = reservations.computeIfPresent(hash, (key, reservation) -> reservation.raised(version))
                    != null;
            InternalKey<K> lookup = InternalKey.lookup(hash);
            CompletableFuture<InternalValue<V>> held = l1.asMap().get(lookup);
            if (held != null && !held.isDone()) {
                if (!reserved) {
                    // a load in flight not reserved yet (it reserves right after its future entered L1): dropped, its
                    // waiters still get its value, and its owner does not hold it
                    l1.asMap().remove(lookup, held);
                }
                // reserved: checked when its owner releases it
            } else if (held != null && !reserved) {
                // with an operation in flight, decided when it ends (release applies the version rule): its own echo,
                // often faster than the commit to L1, then costs neither a drop nor a population read
                if (!isOlder(held, version)) {
                    stats.increment(Counter.IGNORED_MESSAGE);
                    if (isNewer(held, version)) {
                        // a message below the held version: usually an earlier write announced late (an echo of
                        // this instance's own, reordered). But a row deleted without a tombstone (evicted, pruned
                        // to size) loses its version, and a write after that applies at whatever its writer
                        // proposed, possibly below what is held here (the same millisecond, the clocks' skew):
                        // ignored, it would leave this value held for good. The store decides by the version it
                        // holds now: the same is kept, any other dropped (VISION.md, "In-memory store profiles",
                        // dimension 6)
                        toCheck.put(lookup, held);
                    }
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
        // while the store does not answer, a check would fail: the value stays, as any held one does then
        if (!toCheck.isEmpty() && !guard.isOpen()) {
            check(toCheck, epoch.get());
        }
    }

    // population: holders read the new rows in one query and take what they hold, under the version rule
    private void populate(List<String> hashes) {
        stats.add(Counter.POPULATION_READ, hashes.size());
        // the epoch before the read: a value read before an invalidateAll here is held untrusted
        int readEpoch = epoch.get();
        Map<String, Row> read;
        try {
            read = readRows(hashes);
        } catch (StoreException e) {
            hashes.forEach(hash -> removeIfOlder(InternalKey.lookup(hash), InternalVersion.ANYTHING));
            return;
        }
        for (String hash : hashes) {
            InternalKey<K> lookup = InternalKey.lookup(hash);
            Row row = read.get(hash);
            V value = valueOf(row);
            if (row == null || value == null) {
                removeIfOlder(lookup, row == null ? InternalVersion.ANYTHING : row.version());
            } else {
                install(lookup, new InternalValue<>(value, row.version(), readEpoch, row.age()), false, false);
            }
        }
    }

    // what L1 may hold for a key is at least the given version, as a message would be applied: a removal by this
    // instance at its tombstone's, or a read that found none at its row's; also a write in flight here that commits
    // an older value after it drops that value (release), since without a value nothing in L1 holds the version
    // (and without distribution no message would ever correct it). A load in flight is dropped too, as Caffeine's
    // remove discards it after it completed: its waiters still get its value, and its owner holds it only if the key
    // is still there (see runLoad)
    private void dropOlder(InternalKey<K> internalKey, long version) {
        reservations.compute(internalKey.hash(), (hash, reservation) -> {
            CompletableFuture<InternalValue<V>> current = l1.asMap().get(internalKey);
            while (current != null && (!current.isDone() || isOlder(current, version))
                    && !l1.asMap().remove(internalKey, current)) {
                current = l1.asMap().get(internalKey);
            }
            return reservation == null ? null : reservation.raised(version);
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
        return value == null || !InternalVersion.atLeast(value.version(), version);
    }

    // a completed value newer than the version: one of a write after it, or of one applied below a value held
    // elsewhere (see apply)
    private static <V> boolean isNewer(CompletableFuture<InternalValue<V>> future, long version) {
        InternalValue<V> value = completedValue(future);
        return value != null && value.version() > version;
    }

    // continuity lost, delivery continues: messages may have been missed. Everything held becomes unconfirmed (the
    // epoch moves on) and is checked against the store on its next read and by a background pass. The subscription
    // is up again here, so every change after the check arrives as a message
    private void gap() {
        stats.increment(Counter.GAP);
        // the epoch first: a load recording an absence after the set was cleared then sees the new epoch
        int revalidating = epoch.incrementAndGet();
        if (absences != null) {
            absences.invalidateAll();
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
        // counted once in effect: whoever sees the count sees the cache degraded
        subscriptionUp = false;
        stats.increment(Counter.INTERRUPTION);
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
        // a cache started without its store: its feed cannot resume before the connect, which says so itself
        boolean connectedBefore = pending == null || pending.isConnected();
        try {
            current.resume();
        } catch (RuntimeException e) {
            long delay = resumeDelayMillis;
            resumeDelayMillis = Math.min(RESUME_MAX_MILLIS, delay * 2);
            // the cause with its stack trace on the first failed attempt of an outage (the delay still the least), the
            // attempts after it a line each: an outage of minutes logged a trace every 30 s (found: the deterministic
            // property test spent seconds printing them)
            String failed = name + ": Resuming the change feed failed; trying again in " + delay + " ms";
            if (connectedBefore && delay == RESUME_MIN_MILLIS) {
                LOGGER.log(System.Logger.Level.WARNING, failed, e);
            } else if (connectedBefore) {
                LOGGER.log(System.Logger.Level.WARNING, failed + ": " + e.getMessage());
            }
            if (!closed) {
                resumeTask.set(InternalMaintenance.schedule(environment, () -> execute(this::attemptResume),
                        Duration.ofMillis(delay)));
                if (!connectedBefore && requireNonNull(pending).isConnected()) {
                    resumeNow(); // connected while this attempt failed: at once rather than after the backoff
                }
            }
            return;
        }
        LOGGER.log(System.Logger.Level.INFO, name + ": The change feed resumed");
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
        LOGGER.log(System.Logger.Level.WARNING, name + ": The change feed seems silent (no heartbeat arrived in time); "
                + "it is re-established, and until then every read checks its copy against the store");
        superviseResume();
    }

    // the feed delivers, as far as this instance knows: neither reported interrupted nor silent, nor, where it is
    // polled, its latest poll failed. A failed poll misses nothing yet (the next one covers its window, and a loss is
    // reported only past the grace period), but changes made meanwhile arrive only once a poll succeeds: until then,
    // as for a feed interrupted, every read checks its copy (README, "Distribution interrupted"); otherwise copies
    // stayed trusted until the heartbeat noticed the silence, two intervals later
    private boolean feedUp() {
        return subscriptionUp && !(subscription instanceof InternalPollingStore.PollStatus polls && polls.isFailing())
                && (heartbeat == null || !heartbeat.isSilent());
    }

    private boolean isTrusted(InternalValue<V> value) {
        return feedUp() && value.epoch() == epoch.get();
    }

    // revalidation on read: a held value not trusted is checked against the store; kept and confirmed if the store
    // still has it (stillStored), dropped otherwise (null: the caller treats it as a miss). While the store does not
    // answer, it is served unconfirmed
    @SuppressWarnings("java:S2589") // a future of L1 completes with null where nothing was found (see joinHeld)
    private @Nullable InternalValue<V> confirmOrDrop(InternalKey<K> internalKey,
                                                     CompletableFuture<InternalValue<V>> held) {
        InternalValue<V> value = joinHeld(held);
        if (value == null || isTrusted(value)) {
            return value;
        }
        // also while the guard is open: it refuses at once, or lets this check through as its probe, which is how
        // a read notices the store is back (otherwise only maintenance did, up to its interval later, serving a value
        // the store no longer had meanwhile: found by the failover test that loses a write)
        int current = epoch.get();
        Map<String, Row> rows;
        try {
            rows = readRows(List.of(internalKey.hash()));
        } catch (StoreException e) {
            return value;
        }
        if (stillStored(rows.get(internalKey.hash()), value)) {
            stats.increment(Counter.REVALIDATION_KEPT);
            value.confirm(current);
            return value;
        }
        stats.increment(Counter.REVALIDATION_DROPPED);
        l1.asMap().remove(internalKey, held);
        return null;
    }

    // whether the store still has the held value: a value row at its version, holding the same bytes. The version alone
    // does not tell: a failover that lost a write lets a store assigning versions (Redis, MongoDB), onto a clock no
    // further than the old primary's, give a later write the lost write's very version, which a holder of the lost
    // value would take for its own (found 2026-11-10, InternalStoreProfileTest). Compared serialized: as the store
    // holds it, or else as the serializer writes what it reads, since a store holding JSON natively returns it in a
    // form of its own (PostgreSQL's jsonb, MongoDB's documents: spacing, key order). Never by equals, which an entity
    // may answer by its id alone; a serializer not stable byte for byte, or a map whose keys the store reordered, only
    // costs a reload
    private boolean stillStored(@Nullable Row row, InternalValue<V> held) {
        if (row == null || row.state() != State.VALUE || row.version() != held.version()) {
            return false;
        }
        byte[] stored = requireNonNull(row.value());
        try {
            byte[] serialized = valueSerializer.serialize(held.value());
            return Arrays.equals(stored, serialized)
                    || Arrays.equals(valueSerializer.serialize(valueSerializer.deserialize(stored)), serialized);
        } catch (InternalSerializer.SerializationException e) {
            return false;
        }
    }

    // whether a value read and a value held are the same, serialized as the store holds them (see stillStored)
    private boolean sameContent(@Nullable V read, @Nullable V held) {
        if (read == null || held == null) {
            return false;
        }
        try {
            return Arrays.equals(valueSerializer.serialize(read), valueSerializer.serialize(held));
        } catch (InternalSerializer.SerializationException e) {
            return false;
        }
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
        // also while the guard is open, as its probe (see confirmOrDrop)
        if (toCheck.isEmpty()) {
            return;
        }
        check(toCheck, epoch.get());
    }

    private void check(Map<InternalKey<K>, CompletableFuture<InternalValue<V>>> toCheck, int confirmingEpoch) {
        Map<String, Row> rows;
        try {
            rows = readRows(hashes(toCheck.keySet()));
        } catch (StoreException e) {
            return;
        }
        toCheck.forEach((internalKey, held) -> {
            InternalValue<V> value = held.join();
            if (stillStored(rows.get(internalKey.hash()), value)) {
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
                environment.sleep(Duration.ofMillis(REVALIDATION_PAUSE_MILLIS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    // whether the cache was built with a maximum age, so that it can be set (DistributedPolicy.StoreMaximumAge)
    boolean hasMaximumAge() {
        return ageBounded;
    }

    Duration maximumAge() {
        return maximumAge;
    }

    // the store's maximum age for this instance from now on: its reads (a row at least as old counts as missing), its
    // maintenance (which expires such rows in the store, announced) and, unless the age is for the store only, the
    // copies it holds (InternalExpiry#reapply). Instances may disagree while a change reaches them all: every atomic
    // decision is conditioned on the row's version, which a read of a row too old reports as well (Store#read), and
    // with the age for the store only, every instance's own age is at least five heartbeat intervals, all that read()
    // relies on. The shortest age expires the values for every instance, at the cost of loads
    void maximumAge(Duration maximumAge) {
        requireNonNull(maximumAge, "maximumAge cannot be null");
        if (maximumAge.isNegative() || maximumAge.isZero()) {
            throw new IllegalArgumentException("maximumAge must be positive");
        }
        // as StoreConfigurer#withMaximumAge does
        Duration age = maximumAge.compareTo(StoreConfigurer.UNBOUNDED_AGE) > 0 ? StoreConfigurer.UNBOUNDED_AGE
                : maximumAge;
        if (maximumAgeForStoreOnly) {
            Duration minimum = InternalMaintenance.storeOnlyMinimumAge(heartbeatInterval);
            if (age.compareTo(minimum) < 0) {
                throw new IllegalArgumentException("withMaximumAgeForStoreOnly() requires a maximum age of at least "
                        + "five heartbeat intervals (" + minimum + "), not " + age);
            }
        }
        this.maximumAge = age;
        // L1's expiry is this library's whenever the copies are bounded by the age (CaffeineConfigurer#caffeine), with
        // the maximum age for the store only those no store confirmed
        if (expiry != null) {
            if (maximumAgeForStoreOnly) {
                expiry.unconfirmedMaximumAge(InternalExpiry.nanos(age));
            } else {
                expiry.maximumAge(InternalExpiry.nanos(age));
            }
            expiry.reapply(l1.synchronous().policy());
        }
    }

    // whether the cache was built with a maximum size, so that it can be set (DistributedPolicy.StoreMaximumSize)
    boolean hasMaximumSize() {
        return sizeBounded;
    }

    long maximumSize() {
        return maximumSize;
    }

    // the store's maximum size for this instance's maintenance from now on; instances that disagree prune to the
    // smallest
    void maximumSize(long maximumSize) {
        if (maximumSize <= 0) {
            throw new IllegalArgumentException("maximumSize must be positive");
        }
        this.maximumSize = maximumSize;
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
            maintenanceTask.set(InternalMaintenance.schedule(environment, () -> storeExecutor.execute(this::maintain),
                    maintenanceInterval));
        }
    }

    private void maintain() {
        // one version for every row expired in this run, larger than every one seen (rows of a larger one are left)
        maintain(0, clock.fresh());
    }

    // one batch of a run's phase (expiry, pruning to size, pruning tombstones), at most the phase's limit
    // (InternalMaintenance): a batch that reached it is followed by the next after a pause as long as it took, so that
    // a large backlog is worked off at most half the time and other store work goes in between; a shorter one moves on
    // to the next phase. A failure ends the run, the next one starts over at its interval
    private void maintain(int phase, long expiredVersion) {
        if (closed) {
            return;
        }
        int next = phase;
        Duration pause = Duration.ZERO;
        try {
            for (; next < 3; next++) {
                int current = next;
                int limit = current == 0 ? InternalMaintenance.EXPIRY_BATCH : InternalMaintenance.PRUNE_BATCH;
                long start = environment.nanoTime();
                // a bound not set has nothing to remove; each batch with the bound as set now
                Duration age = maximumAge;
                long size = maximumSize;
                int changed = guard.retrying(() -> switch (current) {
                    case 0 -> age.equals(StoreConfigurer.UNBOUNDED_AGE) ? 0
                            : store.expire(age, expiredVersion, limit);
                    case 1 -> size == StoreConfigurer.UNBOUNDED_SIZE ? 0
                            : store.pruneToSize(size, pruneGrace, limit);
                    default -> store.pruneTombstones(InternalMaintenance.TOMBSTONE_RETENTION, limit);
                }, Operation.WRITE);
                guard.success();
                if (current < 2) { // tombstones are no values, so their pruning is not counted
                    stats.add(current == 0 ? Counter.MAINTENANCE_EXPIRED : Counter.MAINTENANCE_PRUNED, changed);
                }
                if (changed >= limit) {
                    pause = Duration.ofNanos(environment.nanoTime() - start);
                    break;
                }
            }
            if (next == 3) {
                maintenanceFailures.set(0);
                if (maintenanceFailing) {
                    maintenanceFailing = false;
                    LOGGER.log(System.Logger.Level.INFO, name + ": Maintenance of the store succeeded again");
                }
            }
        } catch (StoreException e) {
            guard.failure(e);
            maintenanceFailed(e, InternalStoreGuard.isUnexpected(e));
            next = 3;
        } catch (RuntimeException e) {
            // keeps maintenance alive; the next run tries again
            maintenanceFailed(e, true);
            next = 3;
        }
        if (next < 3) {
            int phaseLeft = next;
            if (!closed) {
                maintenanceTask.set(InternalMaintenance.scheduleAfter(environment,
                        () -> storeExecutor.execute(() -> maintain(phaseLeft, expiredVersion)), pause));
            }
        } else {
            scheduleMaintenance();
        }
    }

    // a run failed: a failure that recurs every run (a batch always timing out, a missing right to delete) would leave
    // expiry and pruning undone without a trace, so it is logged once as many runs in a row failed as the store guard
    // needs to count the store down, and then not until a run succeeds again. Not while the store is not answering
    // at all, which the guard logs (on an instance without other calls, maintenance's failures are what it counts);
    // an unexpected failure at once, with its stack trace
    private void maintenanceFailed(RuntimeException e, boolean unexpected) {
        int failedRuns = maintenanceFailures.incrementAndGet();
        if (maintenanceFailing
                || (!unexpected && (failedRuns < InternalStoreGuard.THRESHOLD || guard.isOpen()))) {
            return;
        }
        maintenanceFailing = true;
        String message = name + ": Maintenance of the store (expiry, pruning) failed; it is tried again every "
                + "maintenance interval: " + e.getMessage();
        if (unexpected) {
            LOGGER.log(System.Logger.Level.WARNING, message, e);
        } else {
            LOGGER.log(System.Logger.Level.WARNING, message);
        }
    }

    // starting without the store (VISION.md, "Starting without the store"): build() did not reach it, so the cache
    // starts as in an outage (the guard open: misses load from the source, writes fail) and connects in the background,
    // with a backoff as resumes have; the connect closes the guard and resumes the feed, a gap that checks what was
    // loaded meanwhile
    private void startWithoutStore(Connector connecting) {
        notReachedWarned = environment.nanoTime();
        LOGGER.log(System.Logger.Level.WARNING, name + ": The store is not available ("
                + connecting.cause().getMessage() + "); the cache starts without it and connects in the background. "
                + "Until then misses load from the source, writes fail, and its state is DEGRADED"
                + unboundedNote(connecting)
                + " (StoreConfigurer#withFailFast() fails the build instead)");
        scheduleConnect(Duration.ofMillis(RESUME_MIN_MILLIS));
    }

    private static String unboundedNote(Connector connecting) {
        return connecting.unbounded() ? "; with neither a maximum age nor an expiry, the values loaded meanwhile are "
                + "kept until it is reached" : "";
    }

    private void scheduleConnect(Duration delay) {
        if (!closed) {
            connectTask.set(InternalMaintenance.scheduleAfter(environment, () -> {
                try {
                    storeExecutor.execute(this::attemptConnect);
                } catch (RejectedExecutionException e) {
                    // a closed executor ends the attempts with the cache
                }
            }, delay));
        }
    }

    private void attemptConnect() {
        InternalPendingStore standIn = pending;
        Connector connecting = connector;
        if (closed || standIn == null || connecting == null) {
            return;
        }
        Connected connected;
        try {
            connected = connecting.connect().get();
        } catch (StoreException e) {
            if (!e.isRetryable()) {
                failedCheck(e);
                return;
            }
            notReached(e, connecting);
            // one attempt at a time (each schedules the next), as for resumes
            long delay = connectDelayMillis;
            connectDelayMillis = Math.min(RESUME_MAX_MILLIS, delay * 2);
            scheduleConnect(Duration.ofMillis(jittered(delay)));
            return;
        } catch (RuntimeException e) {
            failedCheck(e);
            return;
        }
        // set before the stand-in passes calls on: nothing was pruned before, as maintenance failed until now
        pruneGrace = connected.pruneGrace();
        standIn.connect(connected.store());
        if (closed) {
            return; // the stand-in closed the store, or the close closes it
        }
        guard.connected();
        LOGGER.log(System.Logger.Level.INFO, name + ": The store was reached; the cache started without it is "
                + "connected, and what it loaded meanwhile is checked against the store");
        resumeNow();
    }

    // the backoff's wait with jitter (±20 %), so that instances started together do not try in step
    private long jittered(long delay) {
        return delay * 4 / 5 + environment.random(Math.max(1, delay * 2 / 5));
    }

    // not reached again: warned of every few minutes, not at every attempt
    private void notReached(StoreException e, Connector connecting) {
        long now = environment.nanoTime();
        if (now - notReachedWarned >= NOT_REACHED_WARNING_INTERVAL.toNanos()) {
            notReachedWarned = now;
            LOGGER.log(System.Logger.Level.WARNING, name + ": The store has still not been available since the cache "
                    + "started (" + e.getMessage() + "); misses load from the source, writes fail"
                    + unboundedNote(connecting));
        }
    }

    // the store answered, but the cache cannot use it (another layout, a missing right, a store that cannot distribute
    // as configured): an error found only now, which no restart repairs, so logged as one at every attempt; tried again
    // after a while, as a right can be granted while the cache runs. The reason is not exposed beyond DEGRADED: only
    // a human can act on it (VISION.md, "Starting without the store")
    private void failedCheck(RuntimeException e) {
        String message = name + ": The store was reached, but the cache cannot use it (" + e.getMessage() + "); it "
                + "stays DEGRADED, loading misses from the source, and tries again in " + FAILED_CHECK_RETRY.toMinutes()
                + " minutes";
        // a configuration the store refused, or one the core refused at DistributedCaffeine#distributing, is logged
        // without a stack trace, and anything else, a bug, with one
        if (e instanceof StoreException || e instanceof IllegalStateException) {
            LOGGER.log(System.Logger.Level.ERROR, message);
        } else {
            LOGGER.log(System.Logger.Level.ERROR, message, e);
        }
        scheduleConnect(FAILED_CHECK_RETRY);
    }

    // the feed resumed now rather than after the supervision's backoff, which kept failing before the connect: the
    // attempt scheduled is taken over; an attempt in progress calls this again once it scheduled the next
    private void resumeNow() {
        ScheduledFuture<?> scheduled = resumeTask.get();
        if (subscription != null && scheduled != null && scheduled.cancel(false)) {
            execute(this::attemptResume);
        }
    }

    // the heartbeat of the change feed: on the store executor, every interval (with jitter), only with distribution
    private void scheduleHeartbeat() {
        if (!closed && heartbeat != null) {
            heartbeatTask.set(InternalMaintenance.schedule(environment, () -> storeExecutor.execute(this::heartbeat),
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

    // one beat: the reserved key invalidated at the beat's version, directly (not counted as the user's write); none
    // while the guard is open
    private long beat(long version) {
        if (guard.isOpen()) {
            return 0;
        }
        Write beat = Write.tombstone(InternalHeartbeat.HASH, version);
        WriteResult result = store.write(List.of(beat));
        // at the version the store applied it at (its own, where it assigns one): announced at that one. Refused, a
        // larger one is stored, whose announcement counts as well
        return result.isRefused(beat) ? version : result.version(beat);
    }

    private CompletableFuture<Map<InternalKey<K>, InternalValue<V>>> sourceOnly(
            Set<InternalKey<K>> internalKeys, @Nullable AsyncBulkLoader<K, V> bulkLoader, Set<InternalKey<K>> absent) {
        if (bulkLoader == null) {
            return CompletableFuture.completedFuture(new LinkedHashMap<>());
        }
        Map<K, InternalKey<K>> byKey = new LinkedHashMap<>();
        internalKeys.forEach(internalKey -> byKey.put(internalKey.key(), internalKey));
        stats.add(Counter.SOURCE_LOAD, byKey.size());
        return call(() -> bulkLoader.loadAll(byKey.keySet())).thenApply(loaded -> {
            Map<InternalKey<K>, InternalValue<V>> result = new LinkedHashMap<>();
            // the user's map: a null key or value in it is skipped
            Map<? extends @Nullable K, ? extends @Nullable V> found = requireNonNull(loaded, "loadAll returned null");
            found.forEach((key, value) -> {
                InternalKey<K> internalKey = key == null ? null : byKey.get(key);
                if (internalKey != null && value != null) {
                    result.put(internalKey, valueAt(value, InternalVersion.UNCONFIRMED, UNCONFIRMED));
                }
            });
            internalKeys.forEach(internalKey -> {
                if (!result.containsKey(internalKey)) {
                    absent.add(internalKey);
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
        @SuppressWarnings("FutureReturnValueIgnored") // the release is attached for its effect
        public CompletableFuture<? extends InternalValue<V>> asyncLoad(InternalKey<K> key, Executor executor) {
            // only a refresh of a key not held gets here (every other load passes its own function to Caffeine);
            // reserved until the future Caffeine holds completed (see runLoad). Reserved on the store executor, not
            // here: Caffeine calls this under its lock of the key, and the registry takes its own lock before L1's
            // (install, dropOlder, release), so reserving here deadlocked (found by the property test; the locks are
            // ConcurrentHashMap bins, so even two different keys could). A message before the reservation drops this
            // future from L1 (dropOlder), and the store is read only after it, so nothing is missed meanwhile
            AtomicBoolean reserved = new AtomicBoolean();
            CompletableFuture<@Nullable InternalValue<V>> loaded = CompletableFuture.runAsync(() -> {
                reserve(key);
                reserved.set(true);
            }, storeExecutor).thenCompose(ignored -> load(key, singleLoader, null, INLINE, true))
                    .thenApply(result -> result.cacheable && result.value != null ? result.value.bind(key) : null);
            // on the very future Caffeine holds, so that it is complete when the release checks it (a chained one would
            // complete after the release); on the store executor, as the future may complete before this attaches,
            // and the release would then run here, under Caffeine's lock (the same deadlock, found the same way).
            // Released whatever the executor does (onStoreExecutor), or the reservation would leak
            loaded.whenComplete((value, failure) -> {
                if (reserved.get()) {
                    onStoreExecutor(() -> release(key));
                }
            });
            return heldFuture(loaded);
        }

        // a refresh goes to the source; its result is written conditionally on the held version, at a version above it,
        // and a conflict takes what the store holds now instead, read again (or drops the key if it
        // holds no value). The result is committed to L1 here,
        // as a write in flight, as put does: Caffeine installs a refresh only over the value it started from, which
        // the write's own echo would otherwise drop first (found by the parity test); Caffeine then discards its copy
        // as overtaken, silently.
        // While the store does not answer (or lost the answer of the write), the refresh degrades as a load does
        // (sourceOnly, writeLoaded): the source's answer is not thrown away but returned for Caffeine to install over
        // the value it started from, held but never trusted (the first read with the store back checks it), and null
        // removes the key here only. Failing instead would keep the old value with its timer, so that every read past
        // refreshAfterWrite called the source again, each time in vain
        @Override
        public CompletableFuture<? extends InternalValue<V>> asyncReload(InternalKey<K> key, InternalValue<V> oldValue,
                                                                        Executor executor) {
            stats.increment(Counter.SOURCE_RELOAD);
            // the epoch before the source, the value's in L1
            int reloadEpoch = epoch.get();
            return heldFuture(call(() -> reloader.reload(key.key(), oldValue.value())).thenApplyAsync(reloaded -> {
                if (reloaded == null) {
                    try {
                        writeConditionally(basedOn(Write.tombstone(key.hash(), clock.next(oldValue.version())),
                                oldValue.version()));
                    } catch (StoreException e) {
                        // removed here only; the other instances keep theirs until the store is back
                    }
                    return null;
                }
                reserve(key);
                try {
                    long version = clock.next(oldValue.version());
                    InternalValue<V> value;
                    long applied;
                    try {
                        applied = writeConditionally(basedOn(valueWrite(key, reloaded, version), oldValue.version()));
                    } catch (StoreException e) {
                        return valueAt(reloaded, InternalVersion.UNCONFIRMED, UNCONFIRMED).bind(key);
                    }
                    if (applied != REFUSED) {
                        value = valueAt(reloaded, applied, reloadEpoch);
                    } else {
                        // written or invalidated elsewhere since: the refresh answers what the store holds now, the
                        // newer value or none (none as well when the store stops answering here: the value held is
                        // outdated either way)
                        Current<V> current;
                        try {
                            current = read(key);
                        } catch (StoreException e) {
                            return null;
                        }
                        V found = current.value();
                        if (found == null) {
                            return null;
                        }
                        value = new InternalValue<>(found, current.version(), current.epoch(), current.age());
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
                    release(key);
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
            Map<K, CompletableFuture<? extends @Nullable V>> futures = new LinkedHashMap<>();
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

    // an unconditional write of a value at the given version; conditioned by the caller where it is decided on a read
    private Write valueWrite(InternalKey<K> internalKey, V value, long version) {
        return Write.value(internalKey.hash(), keySerializer.serialize(internalKey.key()),
                valueSerializer.serialize(value), version);
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
            unreadable(e);
            return null;
        }
    }

    // a stored value this instance cannot deserialize: counted, and logged at most once a minute, so that instances
    // whose serializers or value classes differ are noticed also without stats (each such read becomes a load);
    // expected for a while during a rolling update that changes them
    private void unreadable(InternalSerializer.SerializationException e) {
        long now = environment.nanoTime();
        long last = unreadableWarned.get();
        if ((last == 0 || now - last > TimeUnit.MINUTES.toNanos(1)) && unreadableWarned.compareAndSet(last, now)) {
            Throwable cause = e.getCause();
            LOGGER.log(System.Logger.Level.WARNING, name + ": A stored value could not be read and counts as a miss, "
                    + "so it is loaded again: " + e.getMessage() + (cause == null ? "" : " (" + cause + ")")
                    + ". The instances may use different serializers or value classes, which is expected during a "
                    + "rolling update that changes them (distributedPolicy().stats().unreadableRowCount())");
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
    static <T extends @Nullable Object> T await(CompletableFuture<T> future) throws Exception {
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
    static <T> @Nullable T completedValue(@Nullable CompletableFuture<? extends @Nullable T> future) {
        return future != null && future.isDone() && !future.isCompletedExceptionally() ? future.join() : null;
    }

    // completes a future of L1 with null, which Caffeine takes as nothing found but its types cannot express for a
    // value type that is not nullable
    private static void completeAbsent(CompletableFuture<?> future) {
        future.complete(null);
    }

    // a future that may complete with null (nothing found), handed to Caffeine as one of its values, which its types
    // cannot express for a value type that is not nullable; the very same instance
    @SuppressWarnings("NullAway") // the null Caffeine's types cannot express, see above
    private static <T> CompletableFuture<T> heldFuture(CompletableFuture<@Nullable T> future) {
        return future;
    }

    // a completed future of a value of this cache's type, from a variable typed nullable but checked (see valueAt)
    private CompletableFuture<InternalValue<V>> completedWith(InternalValue<V> value) {
        return CompletableFuture.completedFuture(value);
    }

    // a value of this cache's type from a variable typed nullable but checked (the value type, not the variable's,
    // goes into the type argument)
    private InternalValue<V> valueAt(V value, long version, int epoch) {
        return new InternalValue<>(value, version, epoch);
    }

    // join for a future of L1, which completes with null where nothing was found
    @SuppressWarnings("DataFlowIssue") // the null Caffeine's types cannot express, see above
    private static <T> @Nullable T joinHeld(CompletableFuture<T> future) {
        return join(future);
    }

    static <T extends @Nullable Object> T join(CompletableFuture<T> future) {
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

        // a row written after the read's instant has a negative age (Store#read): as young as can be
        static <V> LoadResult<V> cached(InternalValue<V> value, Duration age) {
            return new LoadResult<>(value, true, shown(age));
        }

        static <V> LoadResult<V> uncached(InternalValue<V> value) {
            return new LoadResult<>(value, false, Duration.ZERO);
        }

        static <V> LoadResult<V> absent() {
            return new LoadResult<>(null, true, Duration.ZERO);
        }
    }
}
