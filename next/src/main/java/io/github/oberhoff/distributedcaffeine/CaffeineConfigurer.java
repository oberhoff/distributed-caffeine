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
 * Caffeine's builder methods for the cache instance's local level, one to one, typed with the cache's key and value
 * types. Every setting keeps the meaning it has in Caffeine and applies to this instance only; see Caffeine's
 * {@link Caffeine} for each method's contract, which is passed through unchanged (including its validation, such as
 * a setting made twice).
 * <p>
 * Exceptions: {@link #weakKeys()}, {@link #weakValues()} and {@link #softValues()} are rejected, because keys are
 * identified by hash across instances, not by identity, and the local level holds values in a wrapper only it
 * references.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
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
    // the user's expiry of a local copy, applied at build capped at the store's maximum age (see caffeine(Duration)),
    // and checked at once against a builder of its own, so that a wrong combination fails where it is set, as in
    // Caffeine
    private final Caffeine<Object, Object> checked = Caffeine.newBuilder();
    private @Nullable Duration expireAfterWrite;
    private @Nullable Expiry<? super K, ? super V> expiry;
    // whether the expiry was applied to the builder (once, so that the builder can build again)
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
     * See {@link Caffeine#executor(Executor)}. Runs the user's loader, listeners and refreshes, as in Caffeine; the
     * library's store I/O runs on its own store executor.
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
     * See {@link Caffeine#maximumSize(long)}. Bounds this instance's local level; the store has a bound of its own.
     *
     * @param maximumSize the maximum number of entries held locally
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> maximumSize(long maximumSize) {
        caffeine.maximumSize(maximumSize);
        return this;
    }

    /**
     * See {@link Caffeine#maximumWeight(long)}.
     *
     * @param maximumWeight the maximum total weight of the entries held locally
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
     * Rejected: keys are identified by hash across instances, not by identity.
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
     * Rejected: the local level holds values in a wrapper only it references, which the collector would take at once.
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
     * Rejected, as {@link #weakValues()}.
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
     * See {@link Caffeine#expireAfterWrite(Duration)}. How long this instance keeps its local copy, at most the
     * store's maximum age (a longer duration is lowered to it): no copy outlives the store's maximum age, also where a
     * message dropping it was lost.
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
     *
     * @param duration the duration
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> expireAfterAccess(Duration duration) {
        checked.expireAfterAccess(duration);
        caffeine.expireAfterAccess(duration);
        return this;
    }

    /**
     * See {@link Caffeine#expireAfterAccess(long, TimeUnit)}.
     *
     * @param duration the duration
     * @param unit     its unit
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> expireAfterAccess(long duration, TimeUnit unit) {
        checked.expireAfterAccess(duration, unit);
        caffeine.expireAfterAccess(duration, unit);
        return this;
    }

    /**
     * See {@link Caffeine#expireAfter(Expiry)}. Every duration it returns is at most the store's maximum age (a
     * longer one is lowered to it), as for {@link #expireAfterWrite(Duration)}.
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
     * See {@link Caffeine#refreshAfterWrite(Duration)}. Requires a loading cache, as in Caffeine.
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
     * See {@link Caffeine#ticker(Ticker)}. Drives the local level's expiry and refresh only; ages in the store come
     * from the store's clock.
     *
     * @param ticker the ticker
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> ticker(Ticker ticker) {
        caffeine.ticker(ticker);
        return this;
    }

    /**
     * See {@link Caffeine#evictionListener(RemovalListener)}.
     *
     * @param evictionListener the listener
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> evictionListener(RemovalListener<? super K, ? super V> evictionListener) {
        caffeine.evictionListener(unwrapping(requireNonNull(evictionListener)));
        return this;
    }

    /**
     * See {@link Caffeine#removalListener(RemovalListener)}. Also notified when a change elsewhere removes or replaces
     * a held entry (see the removal causes in the documentation).
     *
     * @param removalListener the listener
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> removalListener(RemovalListener<? super K, ? super V> removalListener) {
        caffeine.removalListener(unwrapping(requireNonNull(removalListener)));
        return this;
    }

    /**
     * See {@link Caffeine#recordStats()}. Also enables the distribution stats.
     *
     * @return this configurer
     */
    public CaffeineConfigurer<K, V> recordStats() {
        caffeine.recordStats(capturing(ConcurrentStatsCounter::new));
        recordingStats = true;
        return this;
    }

    /**
     * See {@link Caffeine#recordStats(Supplier)}. Also enables the distribution stats.
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

    // the builder of the local level, every copy expiring at the store's maximum age at the latest: the user's
    // expiry lowered to it, or that age by default. A copy changed or expired in the store is dropped by its message;
    // where that message is lost without a gap reported (a feed losing messages silently), or there are no messages
    // (DistributionMode.NONE), this bounds how long it is served (VISION.md, "In-memory store profiles", dimension
    // 7). Counted from the copy's install, so a copy of a row read when already old lives up to twice the maximum age
    // after the write. Applied once, so that the builder can build again
    Caffeine<InternalKey<K>, InternalValue<V>> caffeine(Duration maximumAge) {
        if (!expiryApplied) {
            if (expiry != null) {
                caffeine.expireAfter(new InternalExpiry<>(expiry, maximumAge));
            } else {
                caffeine.expireAfterWrite(expireAfterWrite == null || expireAfterWrite.compareTo(maximumAge) > 0
                        ? maximumAge : expireAfterWrite);
            }
            expiryApplied = true;
        }
        return caffeine;
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

    private static <K, V> RemovalListener<InternalKey<K>, InternalValue<V>> unwrapping(
            RemovalListener<? super K, ? super V> listener) {
        return (key, value, cause) -> {
            // Caffeine reports no replacement by the identical instance (an asynchronous cache: if both futures are
            // complete), but L1 replaces the wrapper for the new version; and a refresh result the core installed
            // itself is discarded by Caffeine as overtaken: both are marked
            if (cause == RemovalCause.REPLACED && value != null && value.isReplacementSilent()) {
                return;
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
