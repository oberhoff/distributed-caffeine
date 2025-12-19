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

import com.mongodb.client.MongoCollection;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.SerializersConfigurer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import org.bson.Document;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullIterable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class InternalDistributedPolicy<K, V> implements DistributedPolicy<K, V>, InternalLazyInitializer<K, V> {

    private DistributedCaffeine<K, V> distributedCaffeine;
    private SerializersConfigurer<K, V> serializersConfigurer;
    private InternalMongoRepository<K, V> mongoRepository;

    InternalDistributedPolicy() {
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.distributedCaffeine = distributedCaffeine;
        this.serializersConfigurer = distributedCaffeine.getSerializersConfigurer();
        this.mongoRepository = distributedCaffeine.getMongoRepository();
    }

    @Override
    public MongoCollection<Document> getMongoCollection() {
        return distributedCaffeine.getMongoCollection();
    }

    @Override
    public void startSynchronization() {
        distributedCaffeine.activate();
    }

    @Override
    public void stopSynchronization() {
        distributedCaffeine.deactivate();
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
    public CacheEntry<@NonNull K, @NonNull V> getFromMongo(K key, boolean includeEvicted) {
        requireNonNull(key);
        return getDistributed(key, includeEvicted);
    }

    @Override
    public List<CacheEntry<K, V>> getAllFromMongo(Iterable<? extends K> keys, boolean includeEvicted) {
        Set<K> keySet = requireNonNullIterable(keys);
        return getAllDistributed(keySet, includeEvicted);
    }

    private CacheEntry<K, V> getDistributed(K key, boolean includeEvicted) {
        return getAllDistributed(Set.of(key), includeEvicted).stream()
                .filter(cacheEntry -> cacheEntry.getKey().equals(key))
                .findFirst()
                .orElse(null);
    }

    private List<CacheEntry<K, V>> getAllDistributed(Set<? extends K> keys, boolean includeEvicted) {
        List<CacheEntry<K, V>> cacheEntries = new ArrayList<>();
        mongoRepository.consumeCacheDocumentsGroupedByKeyInReverseOrder(keys, stream ->
                stream.forEach(cacheDocuments -> cacheDocuments.stream()
                        .findFirst()
                        .filter(cacheDocument -> cacheDocument.isCached()
                                || (includeEvicted && cacheDocument.isEvictedExtended()))
                        .map(this::toCacheEntry)
                        .ifPresent(cacheEntries::add)));
        return cacheEntries;
    }

    private CacheEntry<K, V> toCacheEntry(InternalCacheDocument<K, V> cacheDocument) {
        return new CacheEntry<>() {
            @Override
            public String getId() {
                return cacheDocument.getId().toString();
            }

            @Override
            public K getKey() {
                return cacheDocument.getKey();
            }

            @Override
            public V getValue() {
                return cacheDocument.getValue();
            }

            @Override
            public boolean isEvicted() {
                return cacheDocument.isEvicted();
            }

            @Override
            public boolean equals(Object object) {
                if (this == object) return true;
                if (object == null || getClass() != object.getClass()) return false;
                CacheEntry<?, ?> that = (CacheEntry<?, ?>) object;
                return Objects.equals(this.getKey(), that.getKey())
                        && Objects.equals(this.getValue(), that.getValue());
            }

            @Override
            public int hashCode() {
                return Objects.hash(getKey(), getValue());
            }

            @Override
            public String toString() {
                return format("CacheEntry{id=%s, key=%s, value=%s, isEvicted=%s}",
                        getId(), getKey(), getValue(), isEvicted());
            }
        };
    }
}
