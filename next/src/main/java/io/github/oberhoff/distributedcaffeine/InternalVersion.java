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

/**
 * Versions as the core chooses them (VISION.md, "Target design: a thin SPI, versions in the core"): an incarnation
 * and a counter, packed into one positive {@code long}, so that stores, messages and the polling feed carry one
 * number as before. The store only compares versions for equality; the order is the core's:
 * <ul>
 *     <li>A write on what was read continues its incarnation ({@link #next}): the counter plus one.</li>
 *     <li>A write on nothing known (an insert, an unconditional put, a tombstone, an expiry) starts a new one
 *     ({@link #fresh}): random, so that no two writers pick the same one for a key, except with a chance of about
 *     2<sup>-39</sup>.</li>
 *     <li>Two versions of one incarnation are ordered by their counters; of two incarnations, the order is unknown,
 *     and L1 acts conservatively: whatever cannot be shown to be at least as new is dropped.</li>
 *     <li>Incarnation 0 is below every other: a value loaded from the source alone while the store did not answer
 *     ({@link #UNCONFIRMED}), which any message or read replaces.</li>
 * </ul>
 * {@link #ANYTHING} is newer than every version: what L1 must drop whatever it holds for a key.
 */
final class InternalVersion {

    static final int COUNTER_BITS = 24;
    private static final long COUNTER_MASK = (1L << COUNTER_BITS) - 1;
    // 39 bits, so that the packed version stays positive and below ANYTHING
    private static final long INCARNATIONS = (1L << 39) - 1;

    /**
     * The version of a value no store confirmed: incarnation 0, below every other version.
     */
    static final long UNCONFIRMED = 1;

    /**
     * Newer than every version: what L1 must drop whatever it holds.
     */
    static final long ANYTHING = Long.MAX_VALUE;

    private InternalVersion() {
    }

    // a new incarnation, at counter 1; never incarnation 0
    static long fresh() {
        long incarnation = 1 + ThreadLocalRandom.current().nextLong(INCARNATIONS - 1);
        return (incarnation << COUNTER_BITS) | 1;
    }

    // the version after this one, in its incarnation; a new incarnation once the counter is exhausted
    static long next(long version) {
        return (version & COUNTER_MASK) == COUNTER_MASK ? fresh() : version + 1;
    }

    static long incarnation(long version) {
        return version >>> COUNTER_BITS;
    }

    // whether the first version is known to be at least as new as the second
    static boolean atLeast(long version, long other) {
        if (other == ANYTHING) {
            return false;
        }
        if (version == ANYTHING) {
            return true;
        }
        long incarnation = incarnation(version);
        long otherIncarnation = incarnation(other);
        if (otherIncarnation == 0) {
            return incarnation != 0 || version >= other;
        }
        return incarnation == otherIncarnation && version >= other;
    }

    // whether the first version is known to be newer than the second
    static boolean newer(long version, long other) {
        return version != other && atLeast(version, other);
    }

    // the least version known to be at least as new as both: the newer of one incarnation, otherwise ANYTHING (a
    // value must then be dropped whichever it holds); for merging what a key must reach
    static long atLeastBoth(long version, long other) {
        if (atLeast(version, other)) {
            return version;
        }
        return atLeast(other, version) ? other : ANYTHING;
    }
}
