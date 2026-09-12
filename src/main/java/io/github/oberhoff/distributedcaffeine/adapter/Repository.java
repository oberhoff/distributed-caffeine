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

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Set;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;

/**
 * Interface representing a repository that manages persistence of cache entries using an underlying store.
 * <p>
 * <b>Note:</b> Every operation must implicitly be restricted to a discriminator set via
 * {@link DiscriminatorAware#setDiscriminator(String)} and stored in a
 * {@link DiscriminatorAware#DISCRIMINATOR_FIELD}, along with fields from {@link CacheEntry.Field}.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@SuppressWarnings({"RedundantThrows", "java:S112"})
public interface Repository<K, V> extends Publisher<K, V> {

    /**
     * Returns a (optionally ordered) stream of cache entries from the underlying store that match the specified
     * parameters.
     * <p>
     * <b>Note:</b> Parameters expect conditional handling, see details below (filtering by discriminator must be
     * implicit).
     * <p>
     * <b>Note:</b> A cache entry that could not be read for whatever reason (e.g. deserialization fails or field values
     * do not meet the conditions of a cache entry) should be skipped and logged instead of breaking the stream
     * exceptionally.
     *
     * @param hashes              the hashes to filter by ({@code null} means to omit this filter)
     * @param statuses            the statuses to filter by ({@code null} means to omit this filter)
     * @param orderByTimestampAsc {@code true} if returned cache entries should be ordered ascending by timestamp,
     *                            otherwise {@code false} (order does not matter)
     * @return a stream of cache entries
     * @throws Exception if streaming fails
     */
    Stream<CacheEntry<K, V>> streamCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                                boolean orderByTimestampAsc) throws Exception;

    /**
     * Returns a (optionally ordered) stream of metadata of cache entries from the underlying store that match the
     * specified parameters.
     * <p>
     * <b>Note:</b> Parameters expect conditional handling, see details below (filtering by discriminator must be
     * implicit).
     * <p>
     * <b>Note:</b> Metadata of a cache entry that could not be read for whatever reason (e.g. field values do not meet
     * the conditions of the metadata of a cache entry) should be skipped and logged instead of breaking the stream
     * exceptionally.
     *
     * @param hashes              the hashes to filter by ({@code null} means to omit this filter)
     * @param statuses            the statuses to filter by ({@code null} means to omit this filter)
     * @param orderByTimestampAsc {@code true} if returned metadata of cache entries should be ordered ascending by
     *                            timestamp, otherwise {@code false} (order does not matter)
     * @return a stream of metadata of cache entries
     * @throws Exception if streaming fails
     */
    Stream<CacheEntryMetadata> streamCacheEntryMetadata(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                                        boolean orderByTimestampAsc) throws Exception;

    /**
     * Updates status of cache entries from the underlying store that match the specified parameters. The operation
     * field must be set to {@code null} and the timestamp field must be set to a current value for updated cache
     * entries.
     * <p>
     * <b>Note:</b> Parameters expect conditional handling, see details below (filtering by discriminator must be
     * implicit).
     *
     * @param hashes    the hashes to filter by ({@code null} means to omit this filter)
     * @param statuses  the statuses to filter by ({@code null} means to omit this filter)
     * @param olderThan the timestamp to filter (older cache entries) by ({@code null} means to omit this filter)
     * @param newStatus the new status to update
     * @throws Exception if updating fails
     */
    void updateStatusOfCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                    @Nullable Instant olderThan, Status newStatus) throws Exception;

    /**
     * Deletes cache entries from the underlying store that match the specified parameters.
     * <p>
     * <b>Note:</b> Parameters expect conditional handling, see details below (filtering by discriminator must be
     * implicit).
     *
     * @param hashes    the hashes to filter by ({@code null} means to omit this filter)
     * @param statuses  the statuses to filter by ({@code null} means to omit this filter)
     * @param olderThan the timestamp to filter (older cache entries) by ({@code null} means to omit this filter)
     * @throws Exception if deleting fails
     */
    void deleteCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                            @Nullable Instant olderThan) throws Exception;

    /**
     * Counts cache entries in the underlying store that match the specified parameters.
     * <p>
     * <b>Note:</b> Parameters expect conditional handling, see details below (filtering by discriminator must be
     * implicit).
     *
     * @param statuses the statuses to filter by ({@code null} means to omit this filter)
     * @return the count
     * @throws Exception if counting fails
     */
    long countCacheEntries(@Nullable Set<Status> statuses) throws Exception;
}
