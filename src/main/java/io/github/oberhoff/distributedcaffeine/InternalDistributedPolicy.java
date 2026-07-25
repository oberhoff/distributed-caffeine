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

import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.SerializersConfigurer;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import org.jspecify.annotations.NonNull;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullIterable;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_EXTENDED_GROUP;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("java:S1450")
class InternalDistributedPolicy<K, V> implements DistributedPolicy<K, V>, InternalLazyInitializer<K, V> {

    private InternalInstanceRegistry<K, V> instanceRegistry;
    private Adapter<K, V> adapter;
    private SerializersConfigurer<K, V> serializersConfigurer;
    private Repository<K, V> repository;
    private InternalHasher<K> hasher;

    InternalDistributedPolicy() {
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.instanceRegistry = instanceRegistry;
        this.adapter = instanceRegistry.getAdapter();
        this.serializersConfigurer = instanceRegistry.getSerializersConfigurer();
        this.repository = instanceRegistry.getAdapter().getRepository();
        this.hasher = instanceRegistry.getHasher();
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
    public CacheEntry<@NonNull K, @NonNull V> getFromStore(K key, boolean includeEvicted) {
        requireNonNull(key);
        return getAllFromStore(Set.of(key), includeEvicted).stream()
                .filter(cacheEntry -> key.equals(cacheEntry.getKey()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public Set<CacheEntry<K, V>> getAllFromStore(Iterable<? extends K> keys, boolean includeEvicted) {
        Set<K> keySet = requireNonNullIterable(keys);
        Set<Status> statuses = new HashSet<>(CACHED_GROUP);
        if (includeEvicted) {
            statuses.addAll(EVICTED_EXTENDED_GROUP);
        }
        // TODO discriminator
        try (Stream<CacheEntry<K, V>> cacheEntryStream = getFailable(() -> repository.streamCacheEntries(
                null,
                hasher.getHashes(keySet),
                statuses,
                null,
                false))) {
            return cacheEntryStream
                    .collect(Collectors.toSet());
        }
    }
}
