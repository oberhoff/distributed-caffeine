/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.LazyInitializer;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.CACHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.EVICTED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.HASH;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.KEY;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.STATUS;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.TOUCHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.VALUE;
import static java.util.Objects.requireNonNull;

class InternalDistributedPolicy<K, V> implements DistributedPolicy<K, V>, LazyInitializer<K, V> {

    private DistributedCaffeine<K, V> distributedCaffeine;
    private InternalMongoRepository<K, V> mongoRepository;

    InternalDistributedPolicy() {
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.distributedCaffeine = distributedCaffeine;
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
    public CacheEntry<K, V> getFromMongo(K key, boolean includeEvicted) {
        return getDistributed(key, includeEvicted);
    }

    @Override
    public List<CacheEntry<K, V>> getAllFromMongo(Iterable<? extends K> keys, boolean includeEvicted) {
        Set<K> keySet = StreamSupport.stream(keys.spliterator(), false)
                .collect(Collectors.toSet());
        return getAllDistributed(keySet, includeEvicted);
    }

    private CacheEntry<K, V> getDistributed(K key, boolean includeEvicted) {
        return getAllDistributed(Set.of(key), includeEvicted).stream()
                .filter(cacheEntry -> cacheEntry.getKey().equals(key))
                .findFirst()
                .orElse(null);
    }

    private List<CacheEntry<K, V>> getAllDistributed(Set<? extends K> keys, boolean includeEvicted) {
        List<Integer> hashes = keys.stream()
                .map(key -> requireNonNull(key, "key cannot be null"))
                .map(Objects::hashCode)
                .collect(Collectors.toList());
        Bson filter = Filters.in(HASH, hashes);
        Bson projection = Projections.include(KEY, VALUE, STATUS, TOUCHED);
        List<CacheEntry<K, V>> result = new ArrayList<>();
        try (Stream<InternalCacheDocument<K, V>> cacheDocumentStream =
                     mongoRepository.streamCacheDocuments(filter, projection)) {
            // retain "hashCode -> bucket -> equals" semantic
            cacheDocumentStream
                    .filter(cacheDocument -> keys.contains(cacheDocument.getKey()))
                    .collect(Collectors.groupingBy(InternalCacheDocument::getKey))
                    .forEach((key, cacheDocuments) -> cacheDocuments.stream()
                            .max(Comparator.naturalOrder())
                            .filter(cacheDocument ->
                                    CACHED.equals(cacheDocument.getStatus())
                                            || (includeEvicted && EVICTED.equals(cacheDocument.getStatus())))
                            .ifPresent(result::add));
        }
        return result;
    }
}
