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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A conditional invalidation of one key ({@link Store#invalidateIfUnchanged}): a tombstone is written only if the
 * stored row still has the expected version, or, for {@link #MISSING}, only if no row exists.
 *
 * @param hash            the hash of the key
 * @param expectedVersion the version the row must still have, or {@link #MISSING} if no row may exist
 */
public record Expected(String hash, long expectedVersion) {

    /**
     * The expectation that no row exists for the key at all, not even a tombstone.
     */
    public static final long MISSING = ExpectedEntry.MISSING;

    public Expected {
        requireNonNull(hash, "hash cannot be null");
        if (expectedVersion != MISSING && expectedVersion <= 0) {
            throw new IllegalArgumentException("expectedVersion must be a version or MISSING");
        }
    }

    /**
     * Invalidates the key only if its row still has the given version (whatever its state).
     *
     * @param hash    the hash of the key
     * @param version the version the row must still have
     * @return the expectation
     */
    public static Expected ifVersion(String hash, long version) {
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        return new Expected(hash, version);
    }

    /**
     * Invalidates the key only if no row exists for it, not even a tombstone ({@code remove(k)} on a key without a
     * value, which must still move the version so that a load in flight conflicts).
     *
     * @param hash the hash of the key
     * @return the expectation
     */
    public static Expected ifMissing(String hash) {
        return new Expected(hash, MISSING);
    }

    /**
     * Returns whether no row may exist.
     *
     * @return {@code true} if the expectation is {@link #MISSING}
     */
    public boolean expectsMissing() {
        return expectedVersion == MISSING;
    }

    /**
     * Returns the expectations as a map from hash to expected version, in their order (for implementations).
     *
     * @param expected the expectations
     * @return the expected versions, by hash
     */
    public static Map<String, Long> byHash(Collection<Expected> expected) {
        Map<String, Long> byHash = new LinkedHashMap<>();
        expected.forEach(expectation -> byHash.put(expectation.hash(), expectation.expectedVersion()));
        return byHash;
    }
}
