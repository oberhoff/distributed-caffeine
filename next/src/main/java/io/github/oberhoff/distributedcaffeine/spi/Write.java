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

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * One key's write ({@link Store#write}): a value or a tombstone, at the version the core chose for it, under a
 * condition on the version the stored row has now. The store compares versions and stores the given one, nothing else;
 * a row's version only ever grows (VISION.md, "Target design: monotonic versions, proposed by the core, enforced by
 * the store").
 *
 * @param hash     the hash of the key
 * @param key      the serialized key, in the cache's key format; {@code null} for a tombstone, which keeps the key the
 *                 row has (none for a row that does not exist yet)
 * @param value    the serialized value, in the cache's value format; {@code null} for a tombstone
 * @param version  the version the row gets: positive, unique for the key
 * @param expected the condition: {@link #ADVANCE} (only if no row exists or its version is smaller than this write's),
 *                 {@link #MISSING} (no row may exist, not even a tombstone), or the version the row must still have,
 *                 whatever its state
 */
@SuppressWarnings("ArrayRecordComponent") // equals, hashCode and toString below compare and show the contents
public record Write(String hash, byte @Nullable [] key, byte @Nullable [] value, long version, long expected) {

    /**
     * The condition of a write not decided on a read (a {@code put}, a tombstone, the control row): it applies only if
     * no row exists or the row's version is smaller than the write's, so that the version only grows.
     */
    public static final long ADVANCE = 0L;

    /**
     * The condition that no row exists for the key at all, not even a tombstone: what a load writes after reading
     * nothing, so that an invalidation in between (which leaves a tombstone) makes the write conflict.
     */
    public static final long MISSING = -1L;

    public Write {
        requireNonNull(hash, "hash cannot be null");
        if ((key == null) != (value == null)) {
            throw new IllegalArgumentException("key and value must both be present (a value) or both absent "
                    + "(a tombstone)");
        }
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        if (expected < MISSING) {
            throw new IllegalArgumentException("expected must be a version, ADVANCE or MISSING");
        }
    }

    /**
     * Writes a value, advancing the row's version.
     *
     * @param hash    the hash of the key
     * @param key     the serialized key
     * @param value   the serialized value
     * @param version the version the row gets
     * @return the write
     */
    public static Write value(String hash, byte[] key, byte[] value, long version) {
        return new Write(hash, requireNonNull(key, "key cannot be null"), requireNonNull(value, "value cannot be null"),
                version, ADVANCE);
    }

    /**
     * Writes a tombstone, advancing the row's version, creating one where no row exists, so that the version reaches
     * loads in flight.
     *
     * @param hash    the hash of the key
     * @param version the version the tombstone gets
     * @return the write
     */
    public static Write tombstone(String hash, long version) {
        return new Write(hash, null, null, version, ADVANCE);
    }

    /**
     * Returns this write, conditioned on the row still having the given version (whatever its state).
     *
     * @param expectedVersion the version the row must still have
     * @return the conditional write
     */
    public Write ifVersion(long expectedVersion) {
        if (expectedVersion <= 0) {
            throw new IllegalArgumentException("expectedVersion must be positive");
        }
        return new Write(hash, key, value, version, expectedVersion);
    }

    /**
     * Returns this write, conditioned on no row existing for the key, not even a tombstone.
     *
     * @return the conditional write
     */
    public Write ifMissing() {
        return new Write(hash, key, value, version, MISSING);
    }

    /**
     * Returns this write at another version, its condition kept: as applied where the store assigned the version
     * ({@link WriteResult}).
     *
     * @param version the version
     * @return the write at that version
     */
    public Write at(long version) {
        return new Write(hash, key, value, version, expected);
    }

    /**
     * Returns whether this write writes a tombstone.
     *
     * @return {@code true} for a tombstone, {@code false} for a value
     */
    public boolean isTombstone() {
        return value == null;
    }

    /**
     * Returns whether this write advances the version (not decided on a read).
     *
     * @return {@code true} if {@link #expected()} is {@link #ADVANCE}
     */
    public boolean isAdvance() {
        return expected == ADVANCE;
    }

    /**
     * Returns whether no row may exist.
     *
     * @return {@code true} if {@link #expected()} is {@link #MISSING}
     */
    public boolean expectsMissing() {
        return expected == MISSING;
    }

    @Override
    public boolean equals(@Nullable Object object) {
        return object instanceof Write that
                && hash.equals(that.hash)
                && Arrays.equals(key, that.key)
                && Arrays.equals(value, that.value)
                && version == that.version
                && expected == that.expected;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, Arrays.hashCode(key), Arrays.hashCode(value), version, expected);
    }

    @Override
    public String toString() {
        return "Write[hash=" + hash + ", " + (value == null ? "tombstone" : "value of " + value.length + " bytes")
                + ", version=" + version + ", expected=" + (expected == ADVANCE ? "advance" : expected == MISSING ? "missing"
                : String.valueOf(expected)) + "]";
    }
}
