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

import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.CachedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.SerializersConfigurer;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import org.jspecify.annotations.Nullable;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireRepository;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullIterable;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_RETAINED_GROUP;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("java:S1450")
class InternalDistributedPolicy<K, V> implements DistributedPolicy<K, V>, InternalInitializable<K, V> {

    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalInstanceRegistry<K, V> instanceRegistry;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Adapter<K, V> adapter;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private SerializersConfigurer<K, V> serializersConfigurer;
    private @Nullable Repository<K, V> repository;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalHasher<K> hasher;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private CachedEntryPersistenceConfigurer cachedEntryPersistenceConfigurer;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private EvictedEntryPersistenceConfigurer evictedEntryPersistenceConfigurer;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalDistributedPolicy() {
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.instanceRegistry = instanceRegistry;
        this.adapter = instanceRegistry.getAdapter();
        this.serializersConfigurer = instanceRegistry.getSerializersConfigurer();
        this.repository = instanceRegistry.getAdapter().getRepository().orElse(null);
        this.hasher = instanceRegistry.getHasher();
        this.cachedEntryPersistenceConfigurer = instanceRegistry.getCachedEntryPersistenceConfigurer();
        this.evictedEntryPersistenceConfigurer = instanceRegistry.getEvictedEntryPersistenceConfigurer();
    }

    @Override
    public Adapter<K, V> getAdapter() {
        return adapter;
    }

    @Override
    public void startSynchronization() {
        instanceRegistry.activate();
    }

    @Override
    public void stopSynchronization() {
        instanceRegistry.deactivate();
    }

    @Override
    public Serializer<K, ?> getKeySerializer() {
        return serializersConfigurer.getKeySerializer();
    }

    @Override
    public Serializer<V, ?> getValueSerializer() {
        return serializersConfigurer.getValueSerializer();
    }

    @Override
    @SuppressWarnings("java:S2638")
    public @Nullable CacheEntry<K, V> getFromStore(K key, boolean includeEvicted) {
        requireNonNull(key);
        return getAllFromStore(Set.of(key), includeEvicted).stream()
                .filter(cacheEntry -> key.equals(cacheEntry.getKey()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public Set<CacheEntry<K, V>> getAllFromStore(Iterable<? extends K> keys, boolean includeEvicted) {
        Set<K> keySet = requireNonNullIterable(keys);
        // what a persistence tier retains, not what a write leaves behind until it is swept: cached entries
        // are written for distribution whether or not persistence is configured for them, so without it they would
        // be visible here for the distribution duration alone and turn up empty afterwards
        Set<Status> statuses = new HashSet<>();
        if (cachedEntryPersistenceConfigurer.isConfigured()) {
            statuses.addAll(CACHED_GROUP);
        }
        if (includeEvicted && evictedEntryPersistenceConfigurer.isConfigured()) {
            statuses.addAll(EVICTED_RETAINED_GROUP);
        }
        if (statuses.isEmpty()) {
            return Set.of();
        }
        Repository<K, V> retaining = requireRepository(repository, adapter.getIdentifier());
        try (Stream<CacheEntry<K, V>> cacheEntryStream = getFailable(() -> retaining.streamCacheEntries(
                hasher.getHashes(keySet),
                statuses,
                false))) {
            return cacheEntryStream
                    .filter(cacheEntry -> nonNull(cacheEntry.getValue()))
                    .collect(Collectors.toSet());
        }
    }
}
