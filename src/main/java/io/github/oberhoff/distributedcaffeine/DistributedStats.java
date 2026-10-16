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

import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.Configurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer.LoadingStrategy;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.PersistenceConfigurer;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Statistics about the distribution of this cache instance, complementing
 * {@link com.github.benmanes.caffeine.cache.Cache#stats()}.
 * <p>
 * An immutable snapshot of counters that only increase, recorded per cache instance only if
 * {@link com.github.benmanes.caffeine.cache.Caffeine#recordStats()} is enabled (otherwise all are zero).
 *
 * @author Andreas Oberhoff
 */
public final class DistributedStats {

    private static final DistributedStats EMPTY = new DistributedStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    private final long storeHitCount;
    private final long storeMissCount;
    private final long storeFailureCount;
    private final long storeWriteSuccessCount;
    private final long storeWriteFailureCount;
    private final long asynchronousStoreWriteFailureCount;
    private final long synchronousStoreWriteCount;
    private final long totalSynchronousStoreWriteTime;
    private final long storeRejectionCount;
    private final long remoteUpdateCount;
    private final long remoteRemovalCount;
    private final long discardedLoadCount;
    private final long reconciliationCount;

    @SuppressWarnings("java:S107") // one per counter, and package-private so that the order is never a user's concern
    DistributedStats(long storeHitCount, long storeMissCount, long storeFailureCount,
                     long storeWriteSuccessCount, long storeWriteFailureCount, long asynchronousStoreWriteFailureCount,
                     long synchronousStoreWriteCount, long totalSynchronousStoreWriteTime,
                     long storeRejectionCount, long remoteUpdateCount, long remoteRemovalCount,
                     long discardedLoadCount, long reconciliationCount) {
        this.storeHitCount = requireNonNegative(storeHitCount);
        this.storeMissCount = requireNonNegative(storeMissCount);
        this.storeFailureCount = requireNonNegative(storeFailureCount);
        this.storeWriteSuccessCount = requireNonNegative(storeWriteSuccessCount);
        this.storeWriteFailureCount = requireNonNegative(storeWriteFailureCount);
        this.asynchronousStoreWriteFailureCount = requireNonNegative(asynchronousStoreWriteFailureCount);
        this.synchronousStoreWriteCount = requireNonNegative(synchronousStoreWriteCount);
        this.totalSynchronousStoreWriteTime = requireNonNegative(totalSynchronousStoreWriteTime);
        this.storeRejectionCount = requireNonNegative(storeRejectionCount);
        this.remoteUpdateCount = requireNonNegative(remoteUpdateCount);
        this.remoteRemovalCount = requireNonNegative(remoteRemovalCount);
        this.discardedLoadCount = requireNonNegative(discardedLoadCount);
        this.reconciliationCount = requireNonNegative(reconciliationCount);
    }

    /**
     * Returns statistics with all counts being zero.
     *
     * @return statistics with all counts being zero
     */
    public static DistributedStats empty() {
        return EMPTY;
    }

    /**
     * Returns the number of keys found in the underlying store before loading, so that neither the cache loader nor
     * the mapping function was called.
     * <p>
     * <b>Note:</b> Keys are only looked up in the underlying store before loading if persistence of evicted entries
     * is configured (see {@link PersistenceConfigurer#withEvictedEntries(Configurer)}) with at least one loading
     * strategy (see {@link EvictedEntryPersistenceConfigurer#withLoadingStrategies(LoadingStrategy...)}).
     *
     * @return the number of keys found in the underlying store
     */
    public long storeHitCount() {
        return storeHitCount;
    }

    /**
     * Returns the number of keys not found in the underlying store before loading, so that the cache loader or the
     * mapping function was called.
     * <p>
     * <b>Note:</b> Keys are only looked up in the underlying store before loading if persistence of evicted entries
     * is configured (see {@link PersistenceConfigurer#withEvictedEntries(Configurer)}) with at least one loading
     * strategy (see {@link EvictedEntryPersistenceConfigurer#withLoadingStrategies(LoadingStrategy...)}).
     *
     * @return the number of keys not found in the underlying store
     */
    public long storeMissCount() {
        return storeMissCount;
    }

    /**
     * Returns the number of keys whose lookup in the underlying store before loading failed (including refused
     * lookups, see {@link #storeRejectionCount()}).
     * <p>
     * <b>Note:</b> Keys are only looked up in the underlying store before loading if persistence of evicted entries
     * is configured (see {@link PersistenceConfigurer#withEvictedEntries(Configurer)}) with at least one loading
     * strategy (see {@link EvictedEntryPersistenceConfigurer#withLoadingStrategies(LoadingStrategy...)}).
     *
     * @return the number of keys whose lookup in the underlying store failed
     */
    public long storeFailureCount() {
        return storeFailureCount;
    }

    /**
     * Returns the number of keys looked up in the underlying store before loading, i.e. hits, misses and failures.
     * <p>
     * <b>Note:</b> Keys are only looked up in the underlying store before loading if persistence of evicted entries
     * is configured (see {@link PersistenceConfigurer#withEvictedEntries(Configurer)}) with at least one loading
     * strategy (see {@link EvictedEntryPersistenceConfigurer#withLoadingStrategies(LoadingStrategy...)}).
     *
     * @return the number of keys looked up in the underlying store
     */
    public long storeRequestCount() {
        return saturatedAdd(saturatedAdd(storeHitCount, storeMissCount), storeFailureCount);
    }

    /**
     * Returns the ratio of {@link #storeHitCount()} to {@link #storeRequestCount()}, or {@code 1.0} if there were no
     * requests (as for Caffeine's hit rate).
     * <p>
     * <b>Note:</b> Keys are only looked up in the underlying store before loading if persistence of evicted entries
     * is configured (see {@link PersistenceConfigurer#withEvictedEntries(Configurer)}) with at least one loading
     * strategy (see {@link EvictedEntryPersistenceConfigurer#withLoadingStrategies(LoadingStrategy...)}).
     *
     * @return the ratio of keys found in the underlying store to keys looked up there
     */
    public double storeHitRate() {
        long requestCount = storeRequestCount();
        return (requestCount == 0) ? 1.0 : (double) storeHitCount / requestCount;
    }

    /**
     * Returns the number of successful writes to the underlying store (synchronous and asynchronous), each of which
     * may carry the changes of several keys.
     *
     * @return the number of successful writes to the underlying store
     */
    public long storeWriteSuccessCount() {
        return storeWriteSuccessCount;
    }

    /**
     * Returns the number of failed writes to the underlying store (synchronous and asynchronous, including refused
     * writes, see {@link #storeRejectionCount()}).
     *
     * @return the number of failed writes to the underlying store
     */
    public long storeWriteFailureCount() {
        return storeWriteFailureCount;
    }

    /**
     * Returns the number of failed asynchronous writes to the underlying store, which are included in
     * {@link #storeWriteFailureCount()}. Unlike a failed synchronous write, nobody is told about such a failure (it is
     * only logged), and the change is lost: other cache instances may go on serving a value this one evicted or
     * refreshed, until it is written again or expires there.
     *
     * @return the number of failed asynchronous writes to the underlying store
     */
    public long asynchronousStoreWriteFailureCount() {
        return asynchronousStoreWriteFailureCount;
    }

    /**
     * Returns the number of writes to the underlying store, i.e. successes and failures.
     *
     * @return the number of writes to the underlying store
     */
    public long storeWriteCount() {
        return saturatedAdd(storeWriteSuccessCount, storeWriteFailureCount);
    }

    /**
     * Returns the number of synchronous writes to the underlying store, successful or not, which is what
     * {@link #totalSynchronousStoreWriteTime()} is divided by.
     *
     * @return the number of synchronous writes to the underlying store
     */
    public long synchronousStoreWriteCount() {
        return synchronousStoreWriteCount;
    }

    /**
     * Returns the total number of nanoseconds spent on synchronous writes to the underlying store, meant to be
     * compared between two snapshots (see {@link #minus(DistributedStats)}): its difference divided by that of
     * {@link #synchronousStoreWriteCount()} is the average time a caller waited for a write in between, and divided
     * by the time in between it is the share of that time spent writing. Synchronous writes of a cache instance are
     * made one at a time, so a share approaching {@code 1.0} means that the underlying store limits them.
     *
     * @return the total number of nanoseconds spent on synchronous writes to the underlying store
     */
    public long totalSynchronousStoreWriteTime() {
        return totalSynchronousStoreWriteTime;
    }

    /**
     * Returns the average number of nanoseconds per synchronous write to the underlying store over the lifetime of
     * this cache instance, or {@code 0.0} if there were none. For the average over a period of time, see
     * {@link #totalSynchronousStoreWriteTime()}.
     *
     * @return the average number of nanoseconds per synchronous write to the underlying store
     */
    public double averageSynchronousStoreWriteTime() {
        return (synchronousStoreWriteCount == 0)
                ? 0.0
                : (double) totalSynchronousStoreWriteTime / synchronousStoreWriteCount;
    }

    /**
     * Returns the number of calls to the underlying store refused without contacting it, because the preceding ones
     * failed, so that the store is left alone for a short time (see
     * {@link DistributedPolicy.SynchronizationState#DEGRADED}). Refused calls are included in
     * {@link #storeFailureCount()} (per key) and {@link #storeWriteFailureCount()}.
     *
     * @return the number of calls to the underlying store refused without contacting it
     */
    public long storeRejectionCount() {
        return storeRejectionCount;
    }

    /**
     * Returns the number of values put into this cache instance because another cache instance changed them.
     *
     * @return the number of values put because another cache instance changed them
     */
    public long remoteUpdateCount() {
        return remoteUpdateCount;
    }

    /**
     * Returns the number of values removed from this cache instance because another cache instance invalidated or
     * evicted them (depending on the {@link DistributionMode}). Only values actually held here are counted, so
     * invalidating all values counts each value removed.
     *
     * @return the number of values removed because another cache instance invalidated or evicted them
     */
    public long remoteRemovalCount() {
        return remoteRemovalCount;
    }

    /**
     * Returns the number of loaded or refreshed values discarded, because their key was written while loading (by this
     * or another cache instance). They are reported to the removal listener as well.
     *
     * @return the number of loaded or refreshed values discarded
     */
    public long discardedLoadCount() {
        return discardedLoadCount;
    }

    /**
     * Returns the number of times this cache instance was reconciled against the underlying store, because receiving
     * the changes of other cache instances was interrupted (e.g. by a failover). Reconciling drops values the store
     * does not confirm.
     *
     * @return the number of times this cache instance was reconciled against the underlying store
     */
    public long reconciliationCount() {
        return reconciliationCount;
    }

    /**
     * Returns the difference of these statistics and the given ones, with each count being at least zero.
     *
     * @param other the statistics to subtract
     * @return the difference of these statistics and the given ones
     */
    public DistributedStats minus(DistributedStats other) {
        return new DistributedStats(
                Math.max(0, storeHitCount - other.storeHitCount),
                Math.max(0, storeMissCount - other.storeMissCount),
                Math.max(0, storeFailureCount - other.storeFailureCount),
                Math.max(0, storeWriteSuccessCount - other.storeWriteSuccessCount),
                Math.max(0, storeWriteFailureCount - other.storeWriteFailureCount),
                Math.max(0, asynchronousStoreWriteFailureCount - other.asynchronousStoreWriteFailureCount),
                Math.max(0, synchronousStoreWriteCount - other.synchronousStoreWriteCount),
                Math.max(0, totalSynchronousStoreWriteTime - other.totalSynchronousStoreWriteTime),
                Math.max(0, storeRejectionCount - other.storeRejectionCount),
                Math.max(0, remoteUpdateCount - other.remoteUpdateCount),
                Math.max(0, remoteRemovalCount - other.remoteRemovalCount),
                Math.max(0, discardedLoadCount - other.discardedLoadCount),
                Math.max(0, reconciliationCount - other.reconciliationCount));
    }

    /**
     * Returns the sum of these statistics and the given ones, with each count saturating at {@link Long#MAX_VALUE}.
     *
     * @param other the statistics to add
     * @return the sum of these statistics and the given ones
     */
    public DistributedStats plus(DistributedStats other) {
        return new DistributedStats(
                saturatedAdd(storeHitCount, other.storeHitCount),
                saturatedAdd(storeMissCount, other.storeMissCount),
                saturatedAdd(storeFailureCount, other.storeFailureCount),
                saturatedAdd(storeWriteSuccessCount, other.storeWriteSuccessCount),
                saturatedAdd(storeWriteFailureCount, other.storeWriteFailureCount),
                saturatedAdd(asynchronousStoreWriteFailureCount, other.asynchronousStoreWriteFailureCount),
                saturatedAdd(synchronousStoreWriteCount, other.synchronousStoreWriteCount),
                saturatedAdd(totalSynchronousStoreWriteTime, other.totalSynchronousStoreWriteTime),
                saturatedAdd(storeRejectionCount, other.storeRejectionCount),
                saturatedAdd(remoteUpdateCount, other.remoteUpdateCount),
                saturatedAdd(remoteRemovalCount, other.remoteRemovalCount),
                saturatedAdd(discardedLoadCount, other.discardedLoadCount),
                saturatedAdd(reconciliationCount, other.reconciliationCount));
    }

    private static long requireNonNegative(long count) {
        if (count < 0) {
            throw new IllegalArgumentException("A count cannot be negative");
        }
        return count;
    }

    // as in Caffeine's CacheStats: an overflow sticks at the maximum rather than wrapping around into the negative
    private static long saturatedAdd(long a, long b) {
        long naiveSum = a + b;
        if (((a ^ b) < 0) || ((a ^ naiveSum) >= 0)) {
            return naiveSum;
        }
        return Long.MAX_VALUE + ((naiveSum >>> (Long.SIZE - 1)) ^ 1);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DistributedStats other)) {
            return false;
        }
        return storeHitCount == other.storeHitCount
                && storeMissCount == other.storeMissCount
                && storeFailureCount == other.storeFailureCount
                && storeWriteSuccessCount == other.storeWriteSuccessCount
                && storeWriteFailureCount == other.storeWriteFailureCount
                && asynchronousStoreWriteFailureCount == other.asynchronousStoreWriteFailureCount
                && synchronousStoreWriteCount == other.synchronousStoreWriteCount
                && totalSynchronousStoreWriteTime == other.totalSynchronousStoreWriteTime
                && storeRejectionCount == other.storeRejectionCount
                && remoteUpdateCount == other.remoteUpdateCount
                && remoteRemovalCount == other.remoteRemovalCount
                && discardedLoadCount == other.discardedLoadCount
                && reconciliationCount == other.reconciliationCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(storeHitCount, storeMissCount, storeFailureCount, storeWriteSuccessCount,
                storeWriteFailureCount, asynchronousStoreWriteFailureCount, synchronousStoreWriteCount,
                totalSynchronousStoreWriteTime, storeRejectionCount, remoteUpdateCount, remoteRemovalCount,
                discardedLoadCount, reconciliationCount);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + "storeHitCount=" + storeHitCount
                + ", storeMissCount=" + storeMissCount
                + ", storeFailureCount=" + storeFailureCount
                + ", storeWriteSuccessCount=" + storeWriteSuccessCount
                + ", storeWriteFailureCount=" + storeWriteFailureCount
                + ", asynchronousStoreWriteFailureCount=" + asynchronousStoreWriteFailureCount
                + ", synchronousStoreWriteCount=" + synchronousStoreWriteCount
                + ", totalSynchronousStoreWriteTime=" + totalSynchronousStoreWriteTime
                + ", storeRejectionCount=" + storeRejectionCount
                + ", remoteUpdateCount=" + remoteUpdateCount
                + ", remoteRemovalCount=" + remoteRemovalCount
                + ", discardedLoadCount=" + discardedLoadCount
                + ", reconciliationCount=" + reconciliationCount
                + "}";
    }
}
