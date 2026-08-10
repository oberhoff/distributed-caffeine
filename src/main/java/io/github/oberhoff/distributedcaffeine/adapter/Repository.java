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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;

/**
 * Interface representing a repository that manages distributed synchronization between cache instances using an
 * underlying store.
 * <p>
 * <b>Attention:</b> A repository represents the view of exactly one cache instance on an underlying store, not the
 * store as a whole. Every operation below is implicitly scoped to the discriminator set via
 * {@link DiscriminatorAware#setDiscriminator(String)} and must neither read nor write cache entries belonging to
 * another one.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@NullMarked
@SuppressWarnings("java:S112")
public interface Repository<K, V> extends IdentifierAware, DiscriminatorAware, SerializerAware<K, V> {

    /**
     * Canonical name of the field a discriminator is stored in, offered to implementations whose underlying store
     * names its fields. Unlike the fields of a {@link CacheEntry}, a discriminator is not part of a cache entry but
     * scopes a repository as a whole.
     */
    String DISCRIMINATOR_FIELD = "discriminator";

    /**
     * Discriminator used by caches that do not choose one of their own. It is an ordinary discriminator in every
     * respect, so caches using it are related to each other and separate from caches using any other.
     */
    String DEFAULT_DISCRIMINATOR = "default";

    /**
     * Upserts cache entries into the underlying store, stamping the discriminator of this repository on each of them.
     * <p>
     * <b>Note:</b> The unique identifier for an upsert is the combination of the discriminator and the hash of a cache
     * entry (neither of them ever {@code null}). The uniqueness must be ensured by the underlying store (e.g. by an
     * index on both fields).
     *
     * @param cacheEntries the cache entries to store
     * @throws Exception if upserting fails
     */
    void upsertCacheEntries(Collection<CacheEntry<K, V>> cacheEntries) throws Exception;

    /**
     * Returns a (optionally ordered) stream of cache entries from the underlying store that match the specified
     * parameters.
     * <p>
     * <b>Note:</b> Filtering by the discriminator of this repository applies in addition to the parameters below, so
     * that cache entries belonging to other caches are never returned. Parameters expect conditional handling, see
     * details below.
     *
     * @param hashes              the hashes to filter by ({@code null} means to omit this filter)
     * @param statuses            the statuses to filter by ({@code null} means to omit this filter)
     * @param fields              the fields to return within the cache entry ({@code null} means to return all fields)
     * @param orderByTimestampAsc {@code true} if returned cache entries should be ordered ascending by timestamp,
     *                            {@code false} otherwise (order does not matter)
     * @return a stream of cache entries
     * @throws Exception if streaming fails
     */
    Stream<CacheEntry<K, V>> streamCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                                @Nullable Set<Field> fields,
                                                boolean orderByTimestampAsc) throws Exception;

    /**
     * Updates status of cache entries from the underlying store that match the specified parameters. The operation
     * field must be set to {@code null} and the timestamp field must be set to a current value for updated cache
     * entries.
     * <p>
     * <b>Note:</b> Filtering by the discriminator of this repository applies in addition to the parameters below, so
     * that cache entries belonging to other caches are never updated. Parameters expect conditional handling, see
     * details below.
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
     * <b>Note:</b> Filtering by the discriminator of this repository applies in addition to the parameters below, so
     * that cache entries belonging to other caches are never deleted. Parameters expect conditional handling, see
     * details below.
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
     * <b>Note:</b> Filtering by the discriminator of this repository applies in addition to the parameters below, so
     * that cache entries belonging to other caches are never counted. Parameters expect conditional handling, see
     * details below.
     *
     * @param statuses the statuses to filter by ({@code null} means to omit this filter)
     * @return the count
     * @throws Exception if counting fails
     */
    long countCacheEntries(@Nullable Set<Status> statuses) throws Exception;
}
