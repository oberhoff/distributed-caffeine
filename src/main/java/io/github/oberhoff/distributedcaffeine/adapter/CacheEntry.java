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

import io.github.oberhoff.distributedcaffeine.DistributionMode;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Interface representing a cache entry containing key and value along with some metadata.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@NullMarked
public interface CacheEntry<K, V> {

    /**
     * Fields of a cache entry used to store it in an underlying store, along with
     * {@link Repository#DISCRIMINATOR_FIELD}.
     *
     * @author Andreas Oberhoff
     */
    @NullMarked
    enum Field {

        /**
         * Field used to store the hash of a cache entry (or the name of the command if status is
         * {@link Status#COMMAND}), never {@code null}.
         */
        HASH,

        /**
         * Field used to store an internal operation identifier, can be {@code null}.
         */
        OPERATION,

        /**
         * Field used to store the key of a cache entry (or {@code null} if status is {@link Status#COMMAND}).
         */
        KEY,

        /**
         * Field used to store the value of a cache entry, can be {@code null}.
         */
        VALUE,

        /**
         * Field used to store the status of a cache entry, never {@code null}.
         */
        STATUS,

        /**
         * Field used to store the timestamp of a cache entry, never {@code null}.
         */
        TIMESTAMP;

        private final String value;

        Field() {
            this.value = name().toLowerCase();
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /**
     * Statuses of cache entries resulting from different types of cache operations (population, invalidation,
     * eviction).
     *
     * @author Andreas Oberhoff
     */
    @NullMarked
    enum Status {

        /**
         * Status of a cache entry that was populated manually.
         */
        CACHED,

        /**
         * Status of a cache entry that was populated by a cache loader.
         */
        CACHED_LOADED,

        /**
         * Status of a cache entry that was populated by a refresh.
         */
        CACHED_REFRESHED,

        /**
         * Status of a cache entry that was populated by a refresh after write.
         */
        CACHED_REFRESHED_AFTER_WRITE,

        /**
         * Status of a cache entry that was invalidated manually.
         */
        INVALIDATED,

        /**
         * Status of a cache entry that was invalidated by a refresh.
         */
        INVALIDATED_REFRESHED,

        /**
         * Status of a cache entry that was invalidated by a refresh after write.
         */
        INVALIDATED_REFRESHED_AFTER_WRITE,

        /**
         * Status of a cache entry that was evicted by size.
         */
        EVICTED_SIZE,

        /**
         * Status of a cache entry that was evicted by time.
         */
        EVICTED_TIME,

        /**
         * Status of a cache entry that was evicted by size while extended persistence was configured.
         */
        EVICTED_SIZE_EXTENDED,

        /**
         * Status of a cache entry that was evicted by time while extended persistence was configured.
         */
        EVICTED_TIME_EXTENDED,

        /**
         * Status of a cache entry that carries a command instead of belonging to a key.
         */
        COMMAND;

        /**
         * Group of statuses representing populated cache entries.
         */
        public static final Set<Status> CACHED_GROUP =
                Set.of(CACHED, CACHED_LOADED, CACHED_REFRESHED, CACHED_REFRESHED_AFTER_WRITE);

        /**
         * Group of statuses representing invalidated cache entries.
         */
        public static final Set<Status> INVALIDATED_GROUP =
                Set.of(INVALIDATED, INVALIDATED_REFRESHED, INVALIDATED_REFRESHED_AFTER_WRITE);

        /**
         * Group of statuses representing evicted cache entries regardless of whether extended persistence was
         * configured or not.
         */
        public static final Set<Status> EVICTED_GROUP =
                Set.of(EVICTED_SIZE, EVICTED_TIME, EVICTED_SIZE_EXTENDED, EVICTED_TIME_EXTENDED);

        /**
         * Group of statuses representing evicted cache entries while extended persistence was configured.
         */
        public static final Set<Status> EVICTED_EXTENDED_GROUP =
                Set.of(EVICTED_SIZE_EXTENDED, EVICTED_TIME_EXTENDED);

        /**
         * Group of statuses representing invalidated and evicted cache entries while extended persistence was not
         * configured, along with cache entries carrying a command.
         */
        public static final Set<Status> SHORT_LIVING_GROUP =
                Set.of(INVALIDATED, INVALIDATED_REFRESHED, INVALIDATED_REFRESHED_AFTER_WRITE,
                        EVICTED_SIZE, EVICTED_TIME, COMMAND);

        private final String value;

        Status() {
            this.value = name().toLowerCase();
        }

        /**
         * Indicates whether a cache entry was populated or not.
         *
         * @return {@code true} if cache entry was populated, otherwise {@code false}
         */
        public boolean isCached() {
            return isMemberOf(CACHED_GROUP);
        }

        /**
         * Indicates whether a cache entry was invalidated or not.
         *
         * @return {@code true} if cache entry was invalidated, otherwise {@code false}
         */
        public boolean isInvalidated() {
            return isMemberOf(INVALIDATED_GROUP);
        }

        /**
         * Indicates whether a cache entry was evicted or not regardless of whether extended persistence was configured
         * or not.
         *
         * @return {@code true} if cache entry was evicted, otherwise {@code false}
         */
        public boolean isEvicted() {
            return isMemberOf(EVICTED_GROUP);
        }

        /**
         * Indicates whether a cache entry was evicted or not while extended persistence was not configured.
         *
         * @return {@code true} if cache entry was evicted, otherwise {@code false}
         */
        public boolean isEvictedExtended() {
            return isMemberOf(EVICTED_EXTENDED_GROUP);
        }

        /**
         * Indicates whether a cache entry carries a command instead of belonging to a key or not.
         *
         * @return {@code true} if cache entry carries a command, otherwise {@code false}
         */
        public boolean isCommand() {
            return this == COMMAND;
        }

        /**
         * Indicates whether a status of a cache entry is considered by a specified {@link DistributionMode}.
         *
         * @param distributionMode the distribution mode
         * @return {@code true} if status is considered by the distribution mode, otherwise {@code false}
         */
        public boolean isConsideredBy(DistributionMode distributionMode) {
            if (isCached()) {
                return distributionMode.isPopulationConsidered();
            } else if (isInvalidated()) {
                return distributionMode.isInvalidationConsidered();
            } else if (isEvicted()) {
                return distributionMode.isEvictionConsidered();
            } else {
                return isCommand();
            }
        }

        private boolean isMemberOf(Set<Status> statuses) {
            return statuses.contains(this);
        }

        @Override
        public String toString() {
            return value;
        }

        /**
         * Returns the status represented by the specified string value.
         *
         * @param value string value of the status
         * @return the status
         */
        public static Status of(String value) {
            return Stream.of(Status.values())
                    .filter(status -> status.value.equals(value))
                    .findFirst()
                    .orElseThrow();
        }
    }

    /**
     * Commands a cache entry can carry instead of belonging to a key.
     *
     * @author Andreas Oberhoff
     */
    @NullMarked
    enum Command {

        /**
         * Command representing an 'invalidate all' operation.
         */
        INVALIDATE_ALL;

        private final String value;

        Command() {
            this.value = name().toLowerCase();
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /**
     * Returns the hash of the cache entry (or the name of the command if status is {@link Status#COMMAND}).
     *
     * @return the hash (or the name of the command if status is {@link Status#COMMAND})
     */
    String getHash();

    /**
     * Returns the operation identifier of the cache entry.
     *
     * @return the operation identifier
     */
    @Nullable String getOperation();

    /**
     * Returns the key of the cache entry (or {@code null} if status is {@link Status#COMMAND}).
     *
     * @return the key (or {@code null} if status is {@link Status#COMMAND})
     */
    @Nullable K getKey();

    /**
     * Returns the value of the cache entry.
     *
     * @return the value
     */
    @Nullable V getValue();

    /**
     * Returns the status of the cache entry.
     *
     * @return the status
     */
    Status getStatus();

    /**
     * Returns the timestamp of the cache entry.
     *
     * @return the timestamp
     */
    Instant getTimestamp();

    /**
     * Indicates whether the cache entry was populated or not.
     *
     * @return {@code true} if cache entry was populated, otherwise {@code false}
     */
    default boolean isCached() {
        return getStatus().isCached();
    }

    /**
     * Indicates whether the cache entry was invalidated or not.
     *
     * @return {@code true} if cache entry was invalidated, otherwise {@code false}
     */
    default boolean isInvalidated() {
        return getStatus().isInvalidated();
    }

    /**
     * Indicates whether the cache entry was evicted or not regardless of whether extended persistence was configured or
     * not.
     *
     * @return {@code true} if cache entry was evicted, otherwise {@code false}
     */
    default boolean isEvicted() {
        return getStatus().isEvicted();
    }

    /**
     * Indicates whether the cache entry was evicted or not while extended persistence was not configured.
     *
     * @return {@code true} if cache entry was evicted, otherwise {@code false}
     */
    default boolean isEvictedExtended() {
        return getStatus().isEvictedExtended();
    }

    /**
     * Indicates whether the cache entry carries a command instead of belonging to a key or not.
     *
     * @return {@code true} if cache entry carries a command, otherwise {@code false}
     */
    default boolean isCommand() {
        return getStatus().isCommand();
    }

    /**
     * Returns a cache entry defined by the specified parameters.
     *
     * @param hash      the hash (or the name of the command if status is {@link Status#COMMAND})
     * @param operation the operation identifier
     * @param key       the key (or {@code null} if status is {@link Status#COMMAND})
     * @param value     the value
     * @param status    the status
     * @param timestamp the timestamp
     * @param <K>       the key type of the cache
     * @param <V>       the value type of the cache
     * @return the cache entry
     */
    static <K, V> CacheEntry<K, V> of(String hash, @Nullable String operation, @Nullable K key, @Nullable V value,
                                      Status status, Instant timestamp) {
        requireNonNull(hash, "hash cannot be null");
        requireNonNull(status, "status cannot be null");
        requireNonNull(timestamp, "timestamp cannot be null");
        if (!status.isCommand()) {
            requireNonNull(key, "key cannot be null");
        }

        return new CacheEntry<>() {

            @Override
            public String getHash() {
                return hash;
            }

            @Override
            public @Nullable String getOperation() {
                return operation;
            }

            @Override
            public @Nullable K getKey() {
                return key;
            }

            @Override
            public @Nullable V getValue() {
                return value;
            }

            @Override
            public Status getStatus() {
                return status;
            }

            @Override
            public Instant getTimestamp() {
                return timestamp;
            }

            @Override
            public boolean equals(@Nullable Object object) {
                if (this == object) return true;
                if (object == null || getClass() != object.getClass()) return false;
                CacheEntry<?, ?> that = (CacheEntry<?, ?>) object;
                return Objects.equals(this.getHash(), that.getHash())
                        && Objects.equals(this.getOperation(), that.getOperation())
                        && Objects.equals(this.getKey(), that.getKey())
                        && Objects.equals(this.getValue(), that.getValue())
                        && Objects.equals(this.getStatus(), that.getStatus())
                        && Objects.equals(alignTimestamp(this.getTimestamp()), alignTimestamp(that.getTimestamp()));
            }

            @Override
            public int hashCode() {
                return Objects.hash(getHash(), getOperation(), getKey(), getValue(), getStatus(),
                        alignTimestamp(timestamp));
            }

            @Override
            public String toString() {
                return "CacheEntry{hash=%s, operation=%s, key=%s, value=%s, status=%s, timestamp=%s}"
                        .formatted(getHash(), getOperation(), getKey(), getValue(), getStatus(),
                                alignTimestamp(getTimestamp()));
            }

            private Instant alignTimestamp(Instant timestamp) {
                return timestamp.truncatedTo(ChronoUnit.MILLIS);
            }
        };
    }
}
