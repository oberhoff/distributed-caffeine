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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * Versions as the core proposes them (VISION.md, "Target design: monotonic versions, proposed by the core, enforced by
 * the store"): a hybrid logical clock, one per cache instance, packed into one positive {@code long} ordered as a
 * number: milliseconds since 2020 (43 bits, until 2298), a logical counter (10 bits) and a node id drawn at random per
 * instance (10 bits), so that two instances rarely propose the same version at the same time (an equal version is
 * refused by the store like a smaller one, and proposed again). One per instance, not per JVM: versions are compared
 * only within one key of one store, so a clock shared with other caches adds nothing, and two instances of one cache
 * in one JVM (tests, benchmarks) then order their versions as two JVMs do, through messages and reads, instead of
 * hiding that case behind a common clock. The base cannot move with time: stored versions are compared for as long
 * as their rows live, and a later base would make every new version smaller than the stored ones. A clock before 2020
 * counts as 2020, one beyond the range as its end, so that no clock, however wrong, overflows into the sign.
 * <ul>
 *     <li>Every version seen (a message, a read, a refusal) moves the clock forward ({@link #observe}), so that every
 *     version proposed afterwards ({@link #fresh}) is larger than every version seen; one more than a minute ahead
 *     of this clock is reported, one more than a day ahead is not followed (a clock grossly wrong would otherwise
 *     carry every instance towards the end of the range), and a write on it goes just above it ({@link #next}).</li>
 *     <li>The store enforces the order: a write not decided on a read applies only above the stored version, so a
 *     key's version only grows and commit order equals version order. The clocks decide how often a write is refused
 *     (and proposed again, above), never what a reader sees.</li>
 *     <li>{@link #UNCONFIRMED} is below every version a store issues (0; every version is positive) and equal to none:
 *     a value loaded from the source alone while the store did not answer, which any message or read replaces, and
 *     which no stored version confirms (a store numbering its own versions starts at 1). {@link #ANYTHING} is above every version: what L1 must drop
 *     whatever it holds.</li>
 * </ul>
 */
final class InternalVersion {

    private static final int NODE_BITS = 10;
    private static final int LOGICAL_BITS = 10;
    private static final long NODE_MASK = (1L << NODE_BITS) - 1;
    // one logical tick: the counter sits above the node bits; past 1,023 ticks in a millisecond it carries into time
    private static final long TICK = 1L << NODE_BITS;
    private static final long EPOCH_MILLIS = 1_577_836_800_000L; // 2020-01-01T00:00:00Z
    private static final long MAX_MILLIS = (1L << (63 - LOGICAL_BITS - NODE_BITS)) - 1;
    private static final int TIME_SHIFT = LOGICAL_BITS + NODE_BITS;

    /**
     * How far ahead of this clock a version seen may be before it is reported (VISION.md, "A bound on clocks far
     * ahead"): the writer's clock, another instance's or a store server's, runs noticeably ahead.
     */
    static final long WARN_AHEAD_MILLIS = 60_000;

    /**
     * How far ahead of this clock a version seen may be and still be followed: beyond, the clock stays where it is, so
     * that a clock grossly wrong (another year) cannot carry every instance towards the end of the range. A write on a
     * row so far ahead still succeeds, at a version just above it ({@link #next}).
     */
    static final long FOLLOW_LIMIT_MILLIS = 24 * 60 * 60_000L;

    /**
     * The version of a value no store confirmed: below every version a store issues, and equal to none.
     */
    static final long UNCONFIRMED = 0;

    /**
     * Above every version: what L1 must drop whatever it holds.
     */
    static final long ANYTHING = Long.MAX_VALUE;

    private final long node = ThreadLocalRandom.current().nextLong(1L << NODE_BITS);
    // the clock without the node: the largest of the last version proposed and every version seen (but those too far
    // ahead)
    private final AtomicLong clock = new AtomicLong();
    // told how far ahead (milliseconds) a version seen is, when beyond WARN_AHEAD_MILLIS
    private final LongConsumer ahead;

    InternalVersion() {
        this(ignored -> {
        });
    }

    InternalVersion(LongConsumer ahead) {
        this.ahead = ahead;
    }

    // a version larger than every one proposed or seen before by this clock
    long fresh() {
        long physical = nowMillis() << TIME_SHIFT;
        return clock.updateAndGet(last -> Math.max(physical, inRange(last + TICK))) | node;
    }

    // a version just above the given one, the clock left where it is: for a row too far ahead to follow
    long above(long version) {
        return inRange((version & ~NODE_MASK) + TICK) | node;
    }

    private static long nowMillis() {
        return Math.min(MAX_MILLIS, Math.max(0, System.currentTimeMillis() - EPOCH_MILLIS));
    }

    // the end of the range (2298) reached only by a clock or a stored version that far in the future: an error, not a
    // version repeated (each write would then be refused until its attempts ran out)
    private static long inRange(long version) {
        if (version < 0) {
            throw new IllegalStateException("The versions reached the end of their range (the year 2298): a clock or a "
                    + "stored version lies that far in the future");
        }
        return version;
    }

    // a version seen: what is proposed afterwards is larger. Read first: nearly every version seen is behind the clock,
    // so most reads and messages leave it untouched
    void observe(long version) {
        if (version == ANYTHING) {
            return;
        }
        long seen = version & ~NODE_MASK;
        if (seen > clock.get()) {
            long aheadMillis = (seen >>> TIME_SHIFT) - nowMillis();
            if (aheadMillis > WARN_AHEAD_MILLIS) {
                ahead.accept(aheadMillis);
                if (aheadMillis > FOLLOW_LIMIT_MILLIS) {
                    return;
                }
            }
            clock.accumulateAndGet(seen, Math::max);
        }
    }

    // a version larger than the given one (and than every one followed): what a write decided on it carries
    long next(long version) {
        observe(version);
        long fresh = fresh();
        // above it whatever the clock did: a version too far ahead is not followed, but a write on it must succeed
        return fresh > version ? fresh : above(version);
    }

    // whether the first version is at least as new as the second
    static boolean atLeast(long version, long other) {
        if (other == ANYTHING) {
            return false;
        }
        // ANYTHING, the largest long, is at least as new as every other version
        return version >= other;
    }

    // whether the first version is newer than the second
    static boolean newer(long version, long other) {
        return version != other && atLeast(version, other);
    }

    // the newer of both, ANYTHING absorbing; for merging what a key must reach
    static long atLeastBoth(long version, long other) {
        return atLeast(version, other) ? version : other;
    }
}
