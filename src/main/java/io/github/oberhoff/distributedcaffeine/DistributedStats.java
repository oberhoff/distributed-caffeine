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

import io.github.oberhoff.distributedcaffeine.InternalStatsCounter.Counter;
import org.jspecify.annotations.Nullable;

import java.util.EnumMap;
import java.util.Map;

/**
 * A snapshot of one cache instance's store and synchronization activity, from {@code distributedPolicy().stats()};
 * recorded only if the cache records stats ({@code recordStats()}). Caffeine's own stats keep their meaning: a value
 * found in the store counts there as a miss and a successful load, so their hit rate is the rate of local hits. The
 * share of requests served without calling the source adds the store hits ({@link #storeHitCount()}):
 * <pre>{@code
 * (cache.stats().hitCount() + cache.distributedPolicy().stats().storeHitCount())
 *         / (double) cache.stats().requestCount()
 * }</pre>
 * A message is a change made by any instance, as the store announces it.
 * <p>
 * The counts only grow: the difference of two snapshots ({@link #minus}) is the activity in between, and the sum
 * ({@link #plus}) combines instances.
 *
 * @author Andreas Oberhoff
 */
public final class DistributedStats {

    private static final DistributedStats EMPTY = new DistributedStats(new EnumMap<>(Counter.class));

    // by counter; a counter missing counts zero
    private final Map<Counter, Long> values;

    DistributedStats(Map<Counter, Long> values) {
        this.values = values.isEmpty() ? new EnumMap<>(Counter.class) : new EnumMap<>(values);
    }

    /**
     * Returns a snapshot with every count zero.
     *
     * @return the empty snapshot
     */
    public static DistributedStats empty() {
        return EMPTY;
    }

    /**
     * Returns the number of read calls that reached the store (values or versions).
     *
     * @return the count
     */
    public long storeReadCount() {
        return value(Counter.STORE_READ);
    }

    /**
     * Returns the time spent in those read calls, in nanoseconds; divide its difference between two snapshots by
     * the difference of {@link #storeReadCount()} for an average.
     *
     * @return the time in nanoseconds
     */
    public long totalStoreReadTime() {
        return value(Counter.STORE_READ_TIME);
    }

    /**
     * Returns the number of keys a load found in the store, so that the source was not called.
     *
     * @return the count
     */
    public long storeHitCount() {
        return value(Counter.STORE_HIT);
    }

    /**
     * Returns the number of keys a load did not find in the store (missing, invalidated, expired, too old or
     * unreadable).
     *
     * @return the count
     */
    public long storeMissCount() {
        return value(Counter.STORE_MISS);
    }

    /**
     * Returns the number of keys loaded from the source (the loader or mapping function); unlike Caffeine's load
     * count, which includes the store hits.
     *
     * @return the count
     */
    public long sourceLoadCount() {
        return value(Counter.SOURCE_LOAD);
    }

    /**
     * Returns the number of keys reloaded from the source (refresh, refreshAfterWrite).
     *
     * @return the count
     */
    public long sourceReloadCount() {
        return value(Counter.SOURCE_RELOAD);
    }

    /**
     * Returns the number of write calls that reached the store.
     *
     * @return the count
     */
    public long storeWriteCount() {
        return value(Counter.STORE_WRITE);
    }

    /**
     * Returns the time spent in those write calls, in nanoseconds; divide its difference between two snapshots
     * by the difference of {@link #storeWriteCount()} for an average.
     *
     * @return the time in nanoseconds
     */
    public long totalStoreWriteTime() {
        return value(Counter.STORE_WRITE_TIME);
    }

    /**
     * Returns the number of keys written unconditionally ({@code put}, {@code putAll}).
     *
     * @return the count
     */
    public long putCount() {
        return value(Counter.PUT);
    }

    /**
     * Returns the number of keys written or removed conditionally on the version read (loads, refreshes, the
     * atomic {@code asMap()} methods).
     *
     * @return the count
     */
    public long conditionalWriteCount() {
        return value(Counter.CONDITIONAL_WRITE);
    }

    /**
     * Returns the number of conditional writes and removals that found another version, so that a newer value
     * won or the decision was made again.
     *
     * @return the count
     */
    public long conflictCount() {
        return value(Counter.CONFLICT);
    }

    /**
     * Returns the number of store calls that failed.
     *
     * @return the count
     */
    public long storeFailureCount() {
        return value(Counter.STORE_FAILURE);
    }

    /**
     * Returns the number of failed write calls whose outcome is unknown (the keys were dropped locally).
     *
     * @return the count
     */
    public long unknownOutcomeCount() {
        return value(Counter.UNKNOWN_OUTCOME);
    }

    /**
     * Returns the number of store calls refused at once because the store had failed repeatedly; such calls fail
     * without waiting until a probe finds the store answering again.
     *
     * @return the count
     */
    public long refusedCount() {
        return value(Counter.REFUSED);
    }

    /**
     * Returns how often repeated failures made this instance stop calling the store for a while.
     *
     * @return the count
     */
    public long guardOpenCount() {
        return value(Counter.GUARD_OPEN);
    }

    /**
     * Returns the number of messages received for keys written anywhere, this instance included.
     *
     * @return the count
     */
    public long writtenMessageCount() {
        return value(Counter.WRITTEN_MESSAGE);
    }

    /**
     * Returns the number of messages received for keys invalidated anywhere.
     *
     * @return the count
     */
    public long invalidatedMessageCount() {
        return value(Counter.INVALIDATED_MESSAGE);
    }

    /**
     * Returns the number of messages received for keys that reached the maximum age.
     *
     * @return the count
     */
    public long expiredMessageCount() {
        return value(Counter.EXPIRED_MESSAGE);
    }

    /**
     * Returns the number of messages received for {@code invalidateAll()} anywhere.
     *
     * @return the count
     */
    public long invalidatedAllMessageCount() {
        return value(Counter.INVALIDATED_ALL_MESSAGE);
    }

    /**
     * Returns the number of messages for a held key that were not newer than the copy held, such as this instance's own
     * writes coming back.
     *
     * @return the count
     */
    public long ignoredMessageCount() {
        return value(Counter.IGNORED_MESSAGE);
    }

    /**
     * Returns the number of copies dropped, or with {@code DistributionMode.UPDATE} replaced, because the key was
     * written elsewhere.
     *
     * @return the count
     */
    public long replacedCount() {
        return value(Counter.REPLACED);
    }

    /**
     * Returns the number of held values dropped because the key was invalidated elsewhere.
     *
     * @return the count
     */
    public long invalidatedCount() {
        return value(Counter.INVALIDATED);
    }

    /**
     * Returns the number of held values dropped because the key reached the maximum age in the store.
     *
     * @return the count
     */
    public long expiredCount() {
        return value(Counter.EXPIRED);
    }

    /**
     * Returns the number of keys read from the store to replace a copy written elsewhere
     * ({@code DistributionMode.UPDATE}).
     *
     * @return the count
     */
    public long populationReadCount() {
        return value(Counter.POPULATION_READ);
    }

    /**
     * Returns how often messages may have been missed, so that the copies held were checked against the store.
     *
     * @return the count
     */
    public long gapCount() {
        return value(Counter.GAP);
    }

    /**
     * Returns how often the arrival of messages was interrupted.
     *
     * @return the count
     */
    public long interruptionCount() {
        return value(Counter.INTERRUPTION);
    }

    /**
     * Returns the number of held values checked against the store, after a gap or while delivery was interrupted:
     * the kept ones plus the dropped ones.
     *
     * @return the count
     */
    public long revalidatedCount() {
        return value(Counter.REVALIDATION_KEPT) + value(Counter.REVALIDATION_DROPPED);
    }

    /**
     * Returns of those, the values the store still held, in the same version and with the same content.
     *
     * @return the count
     */
    public long revalidationKeptCount() {
        return value(Counter.REVALIDATION_KEPT);
    }

    /**
     * Returns of those, the values dropped because the store held another version, other content or none.
     *
     * @return the count
     */
    public long revalidationDroppedCount() {
        return value(Counter.REVALIDATION_DROPPED);
    }

    /**
     * Returns the number of rows whose value could not be deserialized (treated as a miss).
     *
     * @return the count
     */
    public long unreadableRowCount() {
        return value(Counter.UNREADABLE_ROW);
    }

    /**
     * Returns the number of loads answered by negative caching without the store or the source.
     *
     * @return the count
     */
    public long negativeHitCount() {
        return value(Counter.NEGATIVE_HIT);
    }

    /**
     * Returns the number of unconditional writes ({@code put}, {@code invalidate}) the store refused because it held a
     * newer version, written by a racing write or by an instance whose clock runs ahead; each was written again, at one
     * more round trip. A measure of contention and clock skew.
     *
     * @return the count
     */
    public long versionRefusalCount() {
        return value(Counter.VERSION_REFUSAL);
    }

    /**
     * Returns the number of loads that returned their value without caching it, because the key was removed elsewhere
     * during each of three attempts in a row. Rare unless a key is removed about as often as its load takes.
     *
     * @return the count
     */
    public long uncachedLoadCount() {
        return value(Counter.UNCACHED_LOAD);
    }

    /**
     * Returns the number of versions seen whose time lay more than a minute ahead of this instance's clock, written by
     * an instance or a store server whose clock runs ahead. Correctness does not depend on it; a steady count shows a
     * clock to fix.
     *
     * @return the count
     */
    public long clockAheadCount() {
        return value(Counter.CLOCK_AHEAD);
    }

    /**
     * Returns the number of values this instance's maintenance expired in the store at the maximum age (announced to
     * every instance, see {@link #expiredMessageCount()}).
     *
     * @return the count
     */
    public long maintenanceExpiredCount() {
        return value(Counter.MAINTENANCE_EXPIRED);
    }

    /**
     * Returns the number of values this instance's maintenance removed from the store to keep it within its maximum
     * size, without notifying the instances, which may keep their copies. A steady count means the maximum size is
     * smaller than the working set, so values are loaded from the source again.
     *
     * @return the count
     */
    public long maintenancePrunedCount() {
        return value(Counter.MAINTENANCE_PRUNED);
    }

    /**
     * Returns the sum of this snapshot and another, saturating at {@code Long.MAX_VALUE}.
     *
     * @param other the other snapshot
     * @return the sum
     */
    public DistributedStats plus(DistributedStats other) {
        Map<Counter, Long> sum = new EnumMap<>(Counter.class);
        for (Counter counter : Counter.values()) {
            long a = value(counter);
            long b = other.value(counter);
            long result = a + b;
            sum.put(counter, ((a ^ result) & (b ^ result)) < 0 ? Long.MAX_VALUE : result);
        }
        return new DistributedStats(sum);
    }

    /**
     * Returns the difference of this snapshot and an earlier one, never below zero.
     *
     * @param other the earlier snapshot
     * @return the difference
     */
    public DistributedStats minus(DistributedStats other) {
        Map<Counter, Long> difference = new EnumMap<>(Counter.class);
        for (Counter counter : Counter.values()) {
            difference.put(counter, Math.max(0, value(counter) - other.value(counter)));
        }
        return new DistributedStats(difference);
    }

    private long value(Counter counter) {
        return values.getOrDefault(counter, 0L);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (!(o instanceof DistributedStats other)) {
            return false;
        }
        for (Counter counter : Counter.values()) {
            if (value(counter) != other.value(counter)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (Counter counter : Counter.values()) {
            hash = 31 * hash + Long.hashCode(value(counter));
        }
        return hash;
    }

    @Override
    public String toString() {
        return "DistributedStats{" + "storeReadCount=" + value(Counter.STORE_READ) + ", "
                + "totalStoreReadTime=" + value(Counter.STORE_READ_TIME) + ", "
                + "storeHitCount=" + value(Counter.STORE_HIT) + ", "
                + "storeMissCount=" + value(Counter.STORE_MISS) + ", "
                + "sourceLoadCount=" + value(Counter.SOURCE_LOAD) + ", "
                + "sourceReloadCount=" + value(Counter.SOURCE_RELOAD) + ", "
                + "storeWriteCount=" + value(Counter.STORE_WRITE) + ", "
                + "totalStoreWriteTime=" + value(Counter.STORE_WRITE_TIME) + ", "
                + "putCount=" + value(Counter.PUT) + ", "
                + "conditionalWriteCount=" + value(Counter.CONDITIONAL_WRITE) + ", "
                + "conflictCount=" + value(Counter.CONFLICT) + ", "
                + "storeFailureCount=" + value(Counter.STORE_FAILURE) + ", "
                + "unknownOutcomeCount=" + value(Counter.UNKNOWN_OUTCOME) + ", "
                + "refusedCount=" + value(Counter.REFUSED) + ", "
                + "guardOpenCount=" + value(Counter.GUARD_OPEN) + ", "
                + "writtenMessageCount=" + value(Counter.WRITTEN_MESSAGE) + ", "
                + "invalidatedMessageCount=" + value(Counter.INVALIDATED_MESSAGE) + ", "
                + "expiredMessageCount=" + value(Counter.EXPIRED_MESSAGE) + ", "
                + "invalidatedAllMessageCount=" + value(Counter.INVALIDATED_ALL_MESSAGE) + ", "
                + "ignoredMessageCount=" + value(Counter.IGNORED_MESSAGE) + ", "
                + "replacedCount=" + value(Counter.REPLACED) + ", "
                + "invalidatedCount=" + value(Counter.INVALIDATED) + ", "
                + "expiredCount=" + value(Counter.EXPIRED) + ", "
                + "populationReadCount=" + value(Counter.POPULATION_READ) + ", "
                + "gapCount=" + value(Counter.GAP) + ", "
                + "interruptionCount=" + value(Counter.INTERRUPTION) + ", "
                + "revalidatedCount=" + revalidatedCount() + ", "
                + "revalidationKeptCount=" + value(Counter.REVALIDATION_KEPT) + ", "
                + "revalidationDroppedCount=" + value(Counter.REVALIDATION_DROPPED) + ", "
                + "unreadableRowCount=" + value(Counter.UNREADABLE_ROW) + ", "
                + "negativeHitCount=" + value(Counter.NEGATIVE_HIT) + ", "
                + "versionRefusalCount=" + value(Counter.VERSION_REFUSAL) + ", "
                + "uncachedLoadCount=" + value(Counter.UNCACHED_LOAD) + ", "
                + "clockAheadCount=" + value(Counter.CLOCK_AHEAD) + ", "
                + "maintenanceExpiredCount=" + value(Counter.MAINTENANCE_EXPIRED) + ", "
                + "maintenancePrunedCount=" + value(Counter.MAINTENANCE_PRUNED) + "}";
    }
}
