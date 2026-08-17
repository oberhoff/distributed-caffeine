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
package io.github.oberhoff.distributedcaffeine.adapter;

import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;

import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.alignTimestamp;
import static java.util.Objects.requireNonNull;

/**
 * Class representing the metadata of a cache entry (field values must meet certain conditions).
 *
 * @author Andreas Oberhoff
 */
public final class CacheEntryMetadata {

    private final String hash;
    private final @Nullable String operation;
    private final Status status;
    private final Instant timestamp;

    private CacheEntryMetadata(String hash, @Nullable String operation, Status status, Instant timestamp) {
        requireNonNull(hash, "hash cannot be null");
        requireNonNull(status, "status cannot be null");
        requireNonNull(timestamp, "timestamp cannot be null");

        this.hash = hash;
        this.operation = operation;
        this.status = status;
        this.timestamp = timestamp;
    }

    /**
     * Returns the metadata of a cache entry defined by the specified parameters.
     *
     * @param hash      the hash (or the name of the command if status is {@link Status#COMMAND}, never {@code null})
     * @param operation the operation identifier (can be {@code null})
     * @param status    the status (never {@code null})
     * @param timestamp the timestamp (never {@code null})
     * @return the new metadata of a cache entry
     * @throws NullPointerException if a field value does not meet the conditions of the metadata of a cache entry
     */
    public static CacheEntryMetadata of(String hash, @Nullable String operation, Status status, Instant timestamp) {
        return new CacheEntryMetadata(hash, operation, status, timestamp);
    }

    /**
     * Returns the hash of the cache entry (or the name of the command if status is {@link Status#COMMAND}, never
     * {@code null}).
     *
     * @return the hash (or the name of the command if status is {@link Status#COMMAND}, never {@code null})
     */
    public String getHash() {
        return hash;
    }

    /**
     * Returns the operation identifier of the cache entry (can be {@code null}).
     *
     * @return the operation identifier (can be {@code null})
     */
    public @Nullable String getOperation() {
        return operation;
    }

    /**
     * Returns the status of the cache entry (never {@code null}).
     *
     * @return the status (never {@code null})
     */
    public Status getStatus() {
        return status;
    }

    /**
     * Returns the timestamp of the cache entry (never {@code null}).
     *
     * @return the timestamp (never {@code null})
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(@Nullable Object object) {
        if (object == this) return true;
        if (object == null || object.getClass() != getClass()) return false;
        CacheEntryMetadata that = (CacheEntryMetadata) object;
        return Objects.equals(this.hash, that.hash)
                && Objects.equals(this.operation, that.operation)
                && Objects.equals(this.status, that.status)
                && Objects.equals(alignTimestamp(this.timestamp), alignTimestamp(that.timestamp));
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, operation, status, alignTimestamp(timestamp));
    }

    @Override
    public String toString() {
        return "%s{%s=%s, %s=%s, %s=%s, %s=%s}".formatted(getClass().getSimpleName(),
                Field.HASH, hash,
                Field.OPERATION, operation,
                Field.STATUS, status,
                Field.TIMESTAMP, alignTimestamp(timestamp));
    }
}
