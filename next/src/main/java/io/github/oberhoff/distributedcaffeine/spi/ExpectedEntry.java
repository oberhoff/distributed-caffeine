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
package io.github.oberhoff.distributedcaffeine.spi;

import static java.util.Objects.requireNonNull;

/**
 * A conditional write: the entry is written only if the stored row still matches the expectation (ordering rule 6).
 *
 * @param entry           the entry to write
 * @param expectedVersion the version the row must still have, or {@link #MISSING} if no row may exist
 * @param createdVersion  for {@link #MISSING}: the version of the row created, the store's time of the read that found
 *                        no row ({@link ReadResult#time()}); {@code 0} otherwise
 */
public record ExpectedEntry(Entry entry, long expectedVersion, long createdVersion) {

    /**
     * The expectation that no row exists for the key at all, not even a tombstone: what a load writes after reading
     * nothing, so that an invalidation in between (which leaves a tombstone) makes the write conflict.
     */
    public static final long MISSING = -1L;

    public ExpectedEntry {
        requireNonNull(entry, "entry cannot be null");
        if (expectedVersion != MISSING && expectedVersion <= 0) {
            throw new IllegalArgumentException("expectedVersion must be a version or MISSING");
        }
        if ((expectedVersion == MISSING) != (createdVersion > 0)) {
            throw new IllegalArgumentException("createdVersion must be positive exactly for MISSING");
        }
    }

    /**
     * Writes the entry only if the row still has the given version (whatever its state).
     *
     * @param entry   the entry to write
     * @param version the version the row must still have
     * @return the conditional write
     */
    public static ExpectedEntry ifVersion(Entry entry, long version) {
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        return new ExpectedEntry(entry, version, 0);
    }

    /**
     * Writes the entry only if no row exists for the key, not even a tombstone, creating the row with the given
     * version: the store's time of the read that found no row. A value read from the source after that read but
     * written after an {@code invalidateAll} then carries a version below the sweep's time, which tells readers it is
     * invalidated (the sweep cannot reach a row that did not exist).
     *
     * @param entry    the entry to write
     * @param readTime the store's time of the read that found no row ({@link ReadResult#time()})
     * @return the conditional write
     */
    public static ExpectedEntry ifMissing(Entry entry, long readTime) {
        if (readTime <= 0) {
            throw new IllegalArgumentException("readTime must be positive");
        }
        return new ExpectedEntry(entry, MISSING, readTime);
    }

    /**
     * Returns whether no row may exist.
     *
     * @return {@code true} if the expectation is {@link #MISSING}
     */
    public boolean expectsMissing() {
        return expectedVersion == MISSING;
    }
}
