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

import com.github.benmanes.caffeine.cache.Policy;
import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

class InternalPolicy<K, V> implements Policy<K, V>, InternalLazyInitializer<K, V> {

    private DistributedCaffeine<K, V> distributedCaffeine;
    private Policy<K, V> policy;

    InternalPolicy() {
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.distributedCaffeine = distributedCaffeine;
        this.policy = distributedCaffeine.getCache().policy();
    }

    @Override
    public boolean isRecordingStats() {
        return policy.isRecordingStats();
    }

    @Override
    public V getIfPresentQuietly(K key) {
        return policy.getIfPresentQuietly(key);
    }

    @Override
    public CacheEntry<@NonNull K, @NonNull V> getEntryIfPresentQuietly(K key) {
        return policy.getEntryIfPresentQuietly(key);
    }

    @Override
    public Map<K, CompletableFuture<V>> refreshes() {
        return policy.refreshes();
    }

    @Override
    public Optional<Eviction<K, V>> eviction() {
        return policy.eviction();
    }

    @Override
    public Optional<FixedExpiration<K, V>> expireAfterAccess() {
        return policy.expireAfterAccess();
    }

    @Override
    public Optional<FixedExpiration<K, V>> expireAfterWrite() {
        return policy.expireAfterWrite();
    }

    @Override
    public Optional<VarExpiration<K, V>> expireVariably() {
        return policy.expireVariably()
                .map(varExpiration -> new InternalExpiration<>(distributedCaffeine, policy, varExpiration));
    }

    @Override
    public Optional<FixedRefresh<K, V>> refreshAfterWrite() {
        return policy.refreshAfterWrite();
    }

    static class InternalExpiration<K, V> implements VarExpiration<K, V> {

        private final Policy<K, V> policy;
        private final VarExpiration<K, V> varExpiration;
        private final InternalCacheManager<K, V> cacheManager;
        private final InternalSynchronizationLock synchronizationLock;

        InternalExpiration(DistributedCaffeine<K, V> distributedCaffeine,
                           Policy<K, V> policy,
                           VarExpiration<K, V> varExpiration) {
            this.policy = policy;
            this.varExpiration = varExpiration;
            this.cacheManager = distributedCaffeine.getCacheManager();
            this.synchronizationLock = distributedCaffeine.getSynchronizationLock();
        }

        @Override
        public V put(K key, V value, long duration, TimeUnit unit) {
            requireNonNull(key);
            requireNonNull(value);
            requireNonNull(unit);
            return synchronizationLock.getLocked(() ->
                    varExpiration.put(key, cacheManager.putDistributed(key, value), duration, unit));
        }

        @Override
        public V putIfAbsent(K key, V value, long duration, TimeUnit unit) {
            requireNonNull(key);
            requireNonNull(value);
            requireNonNull(unit);
            V oldValue = policy.getIfPresentQuietly(key);
            if (isNull(oldValue)) {
                return put(key, value, duration, unit); // implicit distribution
            } else {
                return oldValue;
            }
        }

        @Override
        public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction,
                         Duration duration) {
            requireNonNull(key);
            requireNonNull(remappingFunction);
            requireNonNull(duration);
            BiFunction<K, V, V> distributedRemapping = (remappingKey, remappingValue) -> {
                V value = remappingFunction.apply(remappingKey, remappingValue);
                if (isNull(value)) {
                    cacheManager.invalidateDistributed(remappingKey);
                } else {
                    cacheManager.putDistributed(remappingKey, value);
                }
                return value;
            };
            return synchronizationLock.getLocked(() ->
                    varExpiration.compute(key, distributedRemapping, duration));
        }

        @Override
        public OptionalLong getExpiresAfter(K key, TimeUnit unit) {
            return varExpiration.getExpiresAfter(key, unit);
        }

        @Override
        public void setExpiresAfter(K key, long duration, TimeUnit unit) {
            varExpiration.setExpiresAfter(key, duration, unit);
        }

        @Override
        public Map<K, V> oldest(int limit) {
            return varExpiration.oldest(limit);
        }

        @Override
        public <T> T oldest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            return varExpiration.oldest(mappingFunction);
        }

        @Override
        public Map<K, V> youngest(int limit) {
            return varExpiration.youngest(limit);
        }

        @Override
        public <T> T youngest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            return varExpiration.youngest(mappingFunction);
        }
    }
}
