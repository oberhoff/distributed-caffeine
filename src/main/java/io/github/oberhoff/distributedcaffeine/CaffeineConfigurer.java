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
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.ConcurrentStatsCounter;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Caffeine's builder methods for this instance's own copies, one to one, typed with the cache's key and value types.
 * Each keeps its meaning and validation from {@link Caffeine} and applies to this instance only: the expiries and
 * bounds decide which copies this instance keeps and for how long, while the store's maximum age decides how long a
 * value is valid ({@link StoreConfigurer#withMaximumAge(Duration)}). A copy that goes sends the instance back to the
 * store, not to the source. Except: {@link #weakKeys()}, {@link #weakValues()} and {@link #softValues()} are rejected.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public final class CaffeineConfigurer<K, V> {

    // the stats counter Caffeine created last on this thread: Caffeine creates it while building, and the core records
    // into it what Caffeine counts itself but the core does not run through Caffeine (the remappings of compute)
    private static final ThreadLocal<@Nullable InternalCaffeineStats> CREATED_STATS_COUNTER = new ThreadLocal<>();

    // the real builder of the local level, of InternalKey<K> to InternalValue<V>; raw, because Caffeine's typed
    // callback methods narrow its type parameters, which this class has fixed from the start
    private final Caffeine caffeine = Caffeine.newBuilder();
    private boolean recordingStats;
    private @Nullable Executor executor;
    private @Nullable Duration refreshAfterWrite;
    // the user's expiry of a local copy, applied at build bounded by the store's maximum age (see caffeine(Duration)),
    // and checked at once against a builder of its own, so that a wrong combination fails where it is set, as in
    // Caffeine
    private final Caffeine<Object, Object> checked = Caffeine.newBuilder();
    private @Nullable Duration expireAfterWrite;
    private @Nullable Duration expireAfterAccess;
    private @Nullable Expiry<? super K, ? super V> expiry;
    // the ticker of the local level, which the emulated fixed expiry reads for the policy (Caffeine's default with an
    // expiry: the system's)
    private Ticker ticker = Ticker.systemTicker();
    // the expiry applied to the builder (once, so that the builder can build again; reset for every build), if any
    private @Nullable InternalExpiry<K, V> internalExpiry;
    private boolean expiryApplied;

    CaffeineConfigurer() {
    }

    /**
     * See {@link Caffeine#initialCapacity(int)}.
     *
     * @param initialCapacity the minimum total size for the internal data structures
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> initialCapacity(int initialCapacity) {
        caffeine.initialCapacity(initialCapacity);
        return this;
    }

    /**
     * See {@link Caffeine#executor(Executor)}.
     * <p>
     * <b>Distributed Caffeine:</b> Runs the loader, listeners and refreshes; the store calls run on the store's
     * executor ({@code StoreConfigurer.withExecutor}).
     *
     * @param executor the executor
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> executor(Executor executor) {
        caffeine.executor(executor);
        this.executor = executor;
        return this;
    }

    /**
     * See {@link Caffeine#scheduler(Scheduler)}.
     *
     * @param scheduler the scheduler
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> scheduler(Scheduler scheduler) {
        caffeine.scheduler(scheduler);
        return this;
    }

    /**
     * See {@link Caffeine#maximumSize(long)}.
     * <p>
     * <b>Distributed Caffeine:</b> Bounds this instance's copies; the store has its own maximum size.
     *
     * @param maximumSize the maximum number of entries this instance holds
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> maximumSize(long maximumSize) {
        caffeine.maximumSize(maximumSize);
        return this;
    }

    /**
     * See {@link Caffeine#maximumWeight(long)}.
     * <p>
     * <b>Distributed Caffeine:</b> Bounds the weight of this instance's copies; the store has its own maximum size.
     *
     * @param maximumWeight the maximum total weight of the entries this instance holds
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> maximumWeight(long maximumWeight) {
        caffeine.maximumWeight(maximumWeight);
        return this;
    }

    /**
     * See {@link Caffeine#weigher(Weigher)}.
     *
     * @param weigher the weigher
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> weigher(Weigher<? super K, ? super V> weigher) {
        requireNonNull(weigher);
        caffeine.weigher((Weigher<InternalKey<K>, InternalValue<V>>) (key, value) ->
                weigher.weigh(key.key(), value.value()));
        return this;
    }

    /**
     * See {@link Caffeine#weakKeys()}.
     * <p>
     * <b>Distributed Caffeine:</b> Rejected: keys are matched by hash across instances, not by identity.
     *
     * @return never
     * @throws UnsupportedOperationException always
     */
    @SuppressWarnings({"DoNotCallSuggester", "UnusedReturnValue"})
    public CaffeineConfigurer<K, V> weakKeys() {
        throw new UnsupportedOperationException("weakKeys() is not supported by distributed caches: keys are "
                + "identified by their hash across instances, while weak keys compare by identity");
    }

    /**
     * See {@link Caffeine#weakValues()}.
     * <p>
     * <b>Distributed Caffeine:</b> Rejected: values are held in a wrapper the garbage collector would reclaim at once.
     *
     * @return never
     * @throws UnsupportedOperationException always
     */
    @SuppressWarnings("DoNotCallSuggester")
    public CaffeineConfigurer<K, V> weakValues() {
        throw new UnsupportedOperationException("weakValues() is not supported by distributed caches: values are "
                + "held in a wrapper that only the cache references");
    }

    /**
     * See {@link Caffeine#softValues()}.
     * <p>
     * <b>Distributed Caffeine:</b> Rejected: values are held in a wrapper the garbage collector would reclaim under
     * memory pressure.
     *
     * @return never
     * @throws UnsupportedOperationException always
     */
    @SuppressWarnings("DoNotCallSuggester")
    public CaffeineConfigurer<K, V> softValues() {
        throw new UnsupportedOperationException("softValues() is not supported by distributed caches: values are "
                + "held in a wrapper that only the cache references");
    }

    /**
     * See {@link Caffeine#expireAfterWrite(Duration)}.
     * <p>
     * <b>Distributed Caffeine:</b> Governs this instance's copies only: an expired copy is read from the store again,
     * where the value stays valid until the store's maximum age. With distribution, it bounds how long a lost change
     * can go unnoticed; with {@link DistributionMode#NONE}, how long the other instances' writes stay unseen. A
     * duration longer than the store's maximum age fails the build, as the copies would outlive their values' validity
     * (not with {@link StoreConfigurer#withMaximumAgeForStoreOnly()}). Whatever the duration, a copy goes at the
     * maximum age after its value's write in the store, sooner for a value already old when read. With a maximum age,
     * the local level expires each copy by a variable expiry to do so; {@code policy()} still offers the fixed
     * durations as Caffeine does. At runtime, where instances change one after another, a duration longer than the
     * maximum age is lowered instead, also one set through {@code policy().expireAfterWrite()}:
     * {@code getExpiresAfter()} returns it lowered, and a longer maximum age set later
     * ({@link DistributedPolicy#storeMaximumAge()}) lifts it again, up to the duration set.
     *
     * @param duration the duration
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> expireAfterWrite(Duration duration) {
        checked.expireAfterWrite(duration);
        expireAfterWrite = duration;
        return this;
    }

    /**
     * See {@link Caffeine#expireAfterWrite(long, TimeUnit)}.
     * <p>
     * <b>Distributed Caffeine:</b> Governs this instance's copies only: an expired copy is read from the store again,
     * where the value stays valid until the store's maximum age. With distribution, it bounds how long a lost change
     * can go unnoticed; with {@link DistributionMode#NONE}, how long the other instances' writes stay unseen. A
     * duration longer than the store's maximum age fails the build, as the copies would outlive their values' validity
     * (not with {@link StoreConfigurer#withMaximumAgeForStoreOnly()}). Whatever the duration, a copy goes at the
     * maximum age after its value's write in the store, sooner for a value already old when read. With a maximum age,
     * the local level expires each copy by a variable expiry to do so; {@code policy()} still offers the fixed
     * durations as Caffeine does. At runtime, where instances change one after another, a duration longer than the
     * maximum age is lowered instead, also one set through {@code policy().expireAfterWrite()}:
     * {@code getExpiresAfter()} returns it lowered, and a longer maximum age set later
     * ({@link DistributedPolicy#storeMaximumAge()}) lifts it again, up to the duration set.
     *
     * @param duration the duration
     * @param unit     its unit
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> expireAfterWrite(long duration, TimeUnit unit) {
        checked.expireAfterWrite(duration, unit);
        expireAfterWrite = Duration.ofNanos(unit.toNanos(duration));
        return this;
    }

    /**
     * See {@link Caffeine#expireAfterAccess(Duration)}.
     * <p>
     * <b>Distributed Caffeine:</b> Governs this instance's copies only, counting the accesses on this instance: an
     * idle copy is read from the store again. However often it is accessed, a copy still expires at the store's
     * maximum age after its value's write, so a duration longer than the maximum age, which would never apply, fails
     * the build (not with {@link StoreConfigurer#withMaximumAgeForStoreOnly()}).
     *
     * @param duration the duration
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> expireAfterAccess(Duration duration) {
        checked.expireAfterAccess(duration);
        expireAfterAccess = duration;
        return this;
    }

    /**
     * See {@link Caffeine#expireAfterAccess(long, TimeUnit)}.
     * <p>
     * <b>Distributed Caffeine:</b> Governs this instance's copies only, counting the accesses on this instance: an
     * idle copy is read from the store again. However often it is accessed, a copy still expires at the store's
     * maximum age after its value's write, so a duration longer than the maximum age, which would never apply, fails
     * the build (not with {@link StoreConfigurer#withMaximumAgeForStoreOnly()}).
     *
     * @param duration the duration
     * @param unit     its unit
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> expireAfterAccess(long duration, TimeUnit unit) {
        checked.expireAfterAccess(duration, unit);
        expireAfterAccess = Duration.ofNanos(unit.toNanos(duration));
        return this;
    }

    /**
     * See {@link Caffeine#expireAfter(Expiry)}.
     * <p>
     * <b>Distributed Caffeine:</b> Governs this instance's copies only, so it decides how long a copy is kept, not how
     * long its value is valid: an expired copy is read from the store again, where the value stays until the store's
     * maximum age. For a value with a validity of its own, treat an invalid value read as missing and invalidate it.
     * The methods are called on each instance's own install, update and read of a copy: {@code expireAfterCreate} also
     * for a value read from the store, written long before. A duration longer than what is left of the store's maximum
     * age since the value's write is lowered to it, also one set through {@code policy().expireVariably()}, so no copy
     * outlives its value's validity (not with {@link StoreConfigurer#withMaximumAgeForStoreOnly()}).
     *
     * @param expiry the expiry
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> expireAfter(Expiry<? super K, ? super V> expiry) {
        checked.expireAfter(requireNonNull(expiry));
        this.expiry = expiry;
        return this;
    }

    /**
     * See {@link Caffeine#refreshAfterWrite(Duration)}.
     * <p>
     * <b>Distributed Caffeine:</b> Requires a loading cache, as in Caffeine. The duration counts from when this
     * instance got the value; a value read from the store that is already older is refreshed at once. So a value is
     * refreshed by an age of at most twice the duration. A refresh always calls the loader, and when several instances
     * refresh a key at once, the first result written wins.
     *
     * @param duration the duration
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> refreshAfterWrite(Duration duration) {
        caffeine.refreshAfterWrite(duration);
        refreshAfterWrite = duration;
        return this;
    }

    /**
     * See {@link Caffeine#refreshAfterWrite(long, TimeUnit)}.
     * <p>
     * <b>Distributed Caffeine:</b> Requires a loading cache, as in Caffeine. The duration counts from when this
     * instance got the value; a value read from the store that is already older is refreshed at once. So a value is
     * refreshed by an age of at most twice the duration. A refresh always calls the loader, and when several instances
     * refresh a key at once, the first result written wins.
     *
     * @param duration the duration
     * @param unit     its unit
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> refreshAfterWrite(long duration, TimeUnit unit) {
        caffeine.refreshAfterWrite(duration, unit);
        refreshAfterWrite = Duration.ofNanos(unit.toNanos(duration));
        return this;
    }

    /**
     * See {@link Caffeine#ticker(Ticker)}.
     * <p>
     * <b>Distributed Caffeine:</b> Drives this instance's expiry and refresh only; ages in the store follow the store's
     * clock.
     *
     * @param ticker the ticker
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> ticker(Ticker ticker) {
        caffeine.ticker(ticker);
        this.ticker = ticker;
        return this;
    }

    /**
     * See {@link Caffeine#evictionListener(RemovalListener)}.
     * <p>
     * <b>Distributed Caffeine:</b> Notified for the copies this instance evicts or expires, also at the store's maximum
     * age, which applies even without an expiry configured. A change made elsewhere is reported to the removal listener
     * only.
     *
     * @param evictionListener the listener
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> evictionListener(RemovalListener<? super K, ? super V> evictionListener) {
        caffeine.evictionListener(unwrapping(requireNonNull(evictionListener)));
        return this;
    }

    /**
     * See {@link Caffeine#removalListener(RemovalListener)}.
     * <p>
     * <b>Distributed Caffeine:</b> Also notified when a change elsewhere removes or replaces an entry this instance
     * holds; not for the entries {@code close()} drops.
     *
     * @param removalListener the listener
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> removalListener(RemovalListener<? super K, ? super V> removalListener) {
        caffeine.removalListener(unwrapping(requireNonNull(removalListener)));
        return this;
    }

    /**
     * See {@link Caffeine#recordStats()}.
     * <p>
     * <b>Distributed Caffeine:</b> Also records the distributed stats ({@code distributedPolicy().stats()}).
     *
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> recordStats() {
        caffeine.recordStats(capturing(ConcurrentStatsCounter::new));
        recordingStats = true;
        return this;
    }

    /**
     * See {@link Caffeine#recordStats(Supplier)}.
     * <p>
     * <b>Distributed Caffeine:</b> Also records the distributed stats ({@code distributedPolicy().stats()}).
     *
     * @param statsCounterSupplier the supplier of the stats counter
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> recordStats(Supplier<? extends StatsCounter> statsCounterSupplier) {
        requireNonNull(statsCounterSupplier, "statsCounterSupplier cannot be null");
        caffeine.recordStats(capturing(statsCounterSupplier));
        recordingStats = true;
        return this;
    }

    @Override
    public String toString() {
        return caffeine.toString();
    }

    private static Supplier<StatsCounter> capturing(Supplier<? extends StatsCounter> statsCounterSupplier) {
        return () -> {
            InternalCaffeineStats statsCounter = new InternalCaffeineStats(
                    requireNonNull(statsCounterSupplier.get(), "the stats counter supplier returned null"));
            CREATED_STATS_COUNTER.set(statsCounter);
            return statsCounter;
        };
    }

    // the stats counter of the cache this thread has just built, or a disabled one
    static InternalCaffeineStats takeCreatedStatsCounter() {
        InternalCaffeineStats statsCounter = CREATED_STATS_COUNTER.get();
        CREATED_STATS_COUNTER.remove();
        return statsCounter != null ? statsCounter : new InternalCaffeineStats(StatsCounter.disabledStatsCounter());
    }

    // the builder of the local level, every copy expiring at the store's maximum age after its row's write at the
    // latest: the user's expiry lowered to it, or that age by default. A copy changed or expired in the store is
    // dropped by its message; where that message is lost without a gap reported (a feed losing messages silently), or
    // there are no messages (DistributionMode.NONE), this bounds how long it is served (VISION.md, "In-memory store
    // profiles", dimension 7). Counted from the write, not the install, so a copy of a row read when already old does
    // not live up to twice the maximum age: a variable expiry (InternalExpiry), which Caffeine's fixed expiry cannot
    // be, as its duration is one for all copies. Without a bound (no maximum age, or one for the store only),
    // Caffeine's own fixed expiry, as configured. Applied once, so that the builder can build again
    Caffeine<InternalKey<K>, InternalValue<V>> caffeine(Duration maximumAge) {
        if (!expiryApplied) {
            applyExpiry(maximumAge);
            expiryApplied = true;
        } else if (internalExpiry != null) {
            internalExpiry.reset();
        }
        return caffeine;
    }

    // the user's Expiry, the fixed durations emulated under a maximum age, or else Caffeine's own fixed expiry
    private void applyExpiry(Duration maximumAge) {
        if (expiry != null) {
            internalExpiry = new InternalExpiry<>(expiry, maximumAge, ticker);
            caffeine.expireAfter(internalExpiry);
        } else if (!maximumAge.equals(StoreConfigurer.UNBOUNDED_AGE)) {
            // expireAfterWrite at the maximum age without one set, as the policy shows it, also after the maximum age
            // changed (lowered to it otherwise)
            internalExpiry = new InternalExpiry<>(maximumAge, ticker,
                    expireAfterWrite == null ? Long.MAX_VALUE : InternalExpiry.nanos(expireAfterWrite),
                    expireAfterAccess == null ? InternalExpiry.NONE : InternalExpiry.nanos(expireAfterAccess));
            caffeine.expireAfter(internalExpiry);
        } else {
            caffeine.expireAfterWrite(expireAfterWrite == null ? maximumAge : expireAfterWrite);
            if (expireAfterAccess != null) {
                caffeine.expireAfterAccess(expireAfterAccess);
            }
        }
    }

    // the expiry of the local level built last, if it is one of this library's (see caffeine(Duration))
    @Nullable InternalExpiry<K, V> internalExpiry() {
        return internalExpiry;
    }

    // the executor Caffeine runs the user's code on; the asynchronous loaders and mapping functions run there too
    Executor executor() {
        return executor != null ? executor : ForkJoinPool.commonPool();
    }

    boolean isRecordingStats() {
        return recordingStats;
    }

    boolean isRefreshing() {
        return refreshAfterWrite != null;
    }

    @Nullable Duration refreshAfterWrite() {
        return refreshAfterWrite;
    }

    @Nullable Duration expireAfterWrite() {
        return expireAfterWrite;
    }

    @Nullable Duration expireAfterAccess() {
        return expireAfterAccess;
    }

    private static <K, V> RemovalListener<InternalKey<K>, InternalValue<V>> unwrapping(
            RemovalListener<? super K, ? super V> listener) {
        return (key, value, cause) -> {
            // Caffeine reports no replacement by the identical instance (an asynchronous cache: if both futures are
            // complete), but L1 replaces the wrapper for the new version; and a refresh result the core installed
            // itself is discarded by Caffeine as overtaken: both are marked
            if (cause == RemovalCause.REPLACED && value != null && value.isReplacementSilent()) {
                return;
            }
            if (value != null && value.isRemovalSilent()) {
                return; // emptied by close()
            }
            Object userKey = userKey(key, value);
            if (key != null && userKey == null) {
                return; // not resolvable (a value never bound); not expected
            }
            listener.onRemoval((K) userKey, InternalValue.valueOrNull(value), cause);
        };
    }

    // removed through a message, Caffeine's unbounded cache reports the lookup key: the value knows the key
    private static @Nullable Object userKey(@Nullable InternalKey<?> key, @Nullable InternalValue<?> value) {
        if (key == null) {
            return null;
        }
        if (key.hasKey()) {
            return key.key();
        }
        return value == null ? null : value.key();
    }
}
