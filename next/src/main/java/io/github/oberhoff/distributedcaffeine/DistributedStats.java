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

import org.jspecify.annotations.Nullable;

import java.util.Arrays;

/**
 * A snapshot of what one cache instance did on the distributed side, from {@code distributedPolicy().stats()};
 * counted only if the cache records stats ({@code recordStats()}), as Caffeine's {@code stats()}. Caffeine's stats
 * keep their meaning: a value found in the store counts there as a miss and a successful load.
 * <p>
 * The counts only grow; the difference of two snapshots ({@link #minus}) gives the activity in between, the sum
 * ({@link #plus}) combines instances.
 */
public final class DistributedStats {

    private static final DistributedStats EMPTY = new DistributedStats(new long[34]);

    private final long[] values;

    DistributedStats(long[] values) {
        if (values.length != 34) {
            throw new IllegalArgumentException("expected 34 values");
        }
        this.values = values.clone();
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
        return values[0];
    }

    /**
     * Returns the time spent in those read calls, in nanoseconds; divide its difference between two snapshots by
     * the difference of {@link #storeReadCount()} for an average.
     *
     * @return the time in nanoseconds
     */
    public long totalStoreReadTime() {
        return values[1];
    }

    /**
     * Returns the number of keys a load found in the store, so that the source was not called.
     *
     * @return the count
     */
    public long storeHitCount() {
        return values[2];
    }

    /**
     * Returns the number of keys a load did not find in the store (missing, invalidated, expired, too old or
     * unreadable).
     *
     * @return the count
     */
    public long storeMissCount() {
        return values[3];
    }

    /**
     * Returns the number of keys loaded from the source (the loader or mapping function); unlike Caffeine's load
     * count, which includes the store hits.
     *
     * @return the count
     */
    public long sourceLoadCount() {
        return values[4];
    }

    /**
     * Returns the number of keys reloaded from the source (refresh, refreshAfterWrite).
     *
     * @return the count
     */
    public long sourceReloadCount() {
        return values[5];
    }

    /**
     * Returns the number of write calls that reached the store.
     *
     * @return the count
     */
    public long storeWriteCount() {
        return values[6];
    }

    /**
     * Returns the time spent in those write calls, in nanoseconds; divide its difference between two snapshots
     * by the difference of {@link #storeWriteCount()} for an average.
     *
     * @return the time in nanoseconds
     */
    public long totalStoreWriteTime() {
        return values[7];
    }

    /**
     * Returns the number of keys written unconditionally ({@code put}, {@code putAll}).
     *
     * @return the count
     */
    public long putCount() {
        return values[8];
    }

    /**
     * Returns the number of keys written or removed conditionally on the version read (loads, refreshes, the
     * atomic {@code asMap()} methods).
     *
     * @return the count
     */
    public long conditionalWriteCount() {
        return values[9];
    }

    /**
     * Returns the number of conditional writes and removals that found another version, so that a newer value
     * won or the decision was made again.
     *
     * @return the count
     */
    public long conflictCount() {
        return values[10];
    }

    /**
     * Returns the number of store calls that failed.
     *
     * @return the count
     */
    public long storeFailureCount() {
        return values[11];
    }

    /**
     * Returns the number of failed write calls whose outcome is unknown (the keys were dropped locally).
     *
     * @return the count
     */
    public long unknownOutcomeCount() {
        return values[12];
    }

    /**
     * Returns the number of store calls refused at once while the store guard was open.
     *
     * @return the count
     */
    public long refusedCount() {
        return values[13];
    }

    /**
     * Returns how often the store guard opened.
     *
     * @return the count
     */
    public long guardOpenCount() {
        return values[14];
    }

    /**
     * Returns the number of messages received for keys written anywhere, this instance included.
     *
     * @return the count
     */
    public long writtenMessageCount() {
        return values[15];
    }

    /**
     * Returns the number of messages received for keys invalidated anywhere.
     *
     * @return the count
     */
    public long invalidatedMessageCount() {
        return values[16];
    }

    /**
     * Returns the number of messages received for keys that reached the maximum age.
     *
     * @return the count
     */
    public long expiredMessageCount() {
        return values[17];
    }

    /**
     * Returns the number of messages received for {@code invalidateAll()} anywhere.
     *
     * @return the count
     */
    public long invalidatedAllMessageCount() {
        return values[18];
    }

    /**
     * Returns the number of messages for a held key whose version was not newer (own echoes, duplicates, delayed
     * messages).
     *
     * @return the count
     */
    public long ignoredMessageCount() {
        return values[19];
    }

    /**
     * Returns the number of held values dropped or, with population, replaced because the key was written
     * elsewhere.
     *
     * @return the count
     */
    public long replacedCount() {
        return values[20];
    }

    /**
     * Returns the number of held values dropped because the key was invalidated elsewhere.
     *
     * @return the count
     */
    public long invalidatedCount() {
        return values[21];
    }

    /**
     * Returns the number of held values dropped because the key reached the maximum age in the store.
     *
     * @return the count
     */
    public long expiredCount() {
        return values[22];
    }

    /**
     * Returns the number of keys read from the store to replace a held value written elsewhere (population).
     *
     * @return the count
     */
    public long populationReadCount() {
        return values[23];
    }

    /**
     * Returns how often messages may have been missed, so that what was held is checked against the store.
     *
     * @return the count
     */
    public long gapCount() {
        return values[24];
    }

    /**
     * Returns how often the delivery of messages was interrupted.
     *
     * @return the count
     */
    public long interruptionCount() {
        return values[25];
    }

    /**
     * Returns the number of held values whose version was checked against the store, after a gap or while
     * delivery was interrupted.
     *
     * @return the count
     */
    public long revalidatedCount() {
        return values[26];
    }

    /**
     * Returns of those, the values the store still held in the same version.
     *
     * @return the count
     */
    public long revalidationKeptCount() {
        return values[27];
    }

    /**
     * Returns of those, the values dropped because the store held another version or none.
     *
     * @return the count
     */
    public long revalidationDroppedCount() {
        return values[28];
    }

    /**
     * Returns the number of rows whose value could not be deserialized (treated as a miss).
     *
     * @return the count
     */
    public long unreadableRowCount() {
        return values[29];
    }

    /**
     * Returns the number of loads answered by absent caching without the store or the source.
     *
     * @return the count
     */
    public long absentHitCount() {
        return values[30];
    }

    /**
     * Returns the number of writes not decided on a read (a {@code put}, an {@code invalidate}) that the store refused
     * because it held a larger version already, written by an instance whose clock is ahead or by a racing write; each
     * was proposed again, above it, at the cost of one more round trip. A measure of clock skew and contention.
     *
     * @return the count
     */
    public long versionRefusalCount() {
        return values[31];
    }

    /**
     * Returns the number of loads that returned their value without caching it: the key was invalidated or removed on
     * some instance during each of three attempts in a row (each such removal means the loaded value may be outdated,
     * so the load writes only if nothing happened in between, and otherwise loads again). The caller gets the value it
     * loaded last; neither this instance nor the store keeps it. Rare unless a key is removed about as often as its
     * load takes; a steady count shows such a key.
     *
     * @return the count
     */
    public long uncachedLoadCount() {
        return values[32];
    }

    /**
     * Returns the number of versions seen (in an announcement, a read, a refused write) whose time lay more than a
     * minute ahead of this instance's clock: written by an instance or a store server whose clock runs ahead. Up to a
     * day ahead this instance follows it (its later versions lie above it); beyond, it does not, and only a write on
     * such a key goes above it. Correctness does not depend on it; a steady count shows a clock to fix.
     *
     * @return the count
     */
    public long clockAheadCount() {
        return values[33];
    }

    /**
     * Returns the sum of this snapshot and another, saturating at {@code Long.MAX_VALUE}.
     *
     * @param other the other snapshot
     * @return the sum
     */
    public DistributedStats plus(DistributedStats other) {
        long[] sum = new long[34];
        for (int i = 0; i < values.length; i++) {
            long result = values[i] + other.values[i];
            sum[i] = ((values[i] ^ result) & (other.values[i] ^ result)) < 0 ? Long.MAX_VALUE : result;
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
        long[] difference = new long[34];
        for (int i = 0; i < values.length; i++) {
            difference[i] = Math.max(0, values[i] - other.values[i]);
        }
        return new DistributedStats(difference);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        return o instanceof DistributedStats other && Arrays.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return "DistributedStats{" + "storeReadCount=" + values[0] + ", "
                + "totalStoreReadTime=" + values[1] + ", "
                + "storeHitCount=" + values[2] + ", "
                + "storeMissCount=" + values[3] + ", "
                + "sourceLoadCount=" + values[4] + ", "
                + "sourceReloadCount=" + values[5] + ", "
                + "storeWriteCount=" + values[6] + ", "
                + "totalStoreWriteTime=" + values[7] + ", "
                + "putCount=" + values[8] + ", "
                + "conditionalWriteCount=" + values[9] + ", "
                + "conflictCount=" + values[10] + ", "
                + "storeFailureCount=" + values[11] + ", "
                + "unknownOutcomeCount=" + values[12] + ", "
                + "refusedCount=" + values[13] + ", "
                + "guardOpenCount=" + values[14] + ", "
                + "writtenMessageCount=" + values[15] + ", "
                + "invalidatedMessageCount=" + values[16] + ", "
                + "expiredMessageCount=" + values[17] + ", "
                + "invalidatedAllMessageCount=" + values[18] + ", "
                + "ignoredMessageCount=" + values[19] + ", "
                + "replacedCount=" + values[20] + ", "
                + "invalidatedCount=" + values[21] + ", "
                + "expiredCount=" + values[22] + ", "
                + "populationReadCount=" + values[23] + ", "
                + "gapCount=" + values[24] + ", "
                + "interruptionCount=" + values[25] + ", "
                + "revalidatedCount=" + values[26] + ", "
                + "revalidationKeptCount=" + values[27] + ", "
                + "revalidationDroppedCount=" + values[28] + ", "
                + "unreadableRowCount=" + values[29] + ", "
                + "absentHitCount=" + values[30] + ", "
                + "versionRefusalCount=" + values[31] + ", "
                + "uncachedLoadCount=" + values[32] + ", "
                + "clockAheadCount=" + values[33] + "}";
    }
}
