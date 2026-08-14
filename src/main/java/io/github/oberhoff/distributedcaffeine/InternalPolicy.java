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

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalKey.ik;
import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.m;
import static io.github.oberhoff.distributedcaffeine.InternalValue.iv;
import static io.github.oberhoff.distributedcaffeine.InternalValue.ivn;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static io.github.oberhoff.distributedcaffeine.InternalValue.vn;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableMap;

class InternalPolicy<K, V> implements Policy<K, V>, InternalInitializable<K, V> {

    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalInstanceRegistry<K, V> instanceRegistry;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Policy<InternalKey<K>, InternalValue<V>> policy;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalPolicy() {
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.instanceRegistry = instanceRegistry;
        this.policy = instanceRegistry.getCache().policy();
    }

    @Override
    public boolean isRecordingStats() {
        return policy.isRecordingStats();
    }

    @Override
    public @Nullable V getIfPresentQuietly(K key) {
        return vn(policy.getIfPresentQuietly(ik(key)));
    }

    @Override
    @SuppressWarnings("java:S2638")
    public @Nullable CacheEntry<K, V> getEntryIfPresentQuietly(K key) {
        CacheEntry<InternalKey<K>, InternalValue<V>> cacheEntry = policy.getEntryIfPresentQuietly(ik(key));
        return isNull(cacheEntry)
                ? null
                : createCacheEntry(cacheEntry);
    }

    @Override
    public Map<K, CompletableFuture<V>> refreshes() {
        return policy.refreshes().entrySet().stream()
                .collect(toUnmodifiableMap(
                        entry -> k(entry.getKey()),
                        entry -> entry.getValue().thenApply(InternalValue::v)));
    }

    @Override
    public Optional<Eviction<K, V>> eviction() {
        //noinspection Convert2Diamond
        return policy.eviction()
                .map(eviction -> new Eviction<K, V>() {

                    @Override
                    public boolean isWeighted() {
                        return eviction.isWeighted();
                    }

                    @Override
                    public OptionalInt weightOf(K key) {
                        return eviction.weightOf(ik(key));
                    }

                    @Override
                    public OptionalLong weightedSize() {
                        return eviction.weightedSize();
                    }

                    @Override
                    public long getMaximum() {
                        return eviction.getMaximum();
                    }

                    @Override
                    public void setMaximum(long maximum) {
                        eviction.setMaximum(maximum);
                    }

                    @Override
                    public Map<K, V> coldest(int limit) {
                        return m(eviction.coldest(limit));
                    }

                    @Override
                    public Map<K, V> hottest(int limit) {
                        return m(eviction.hottest(limit));
                    }
                });
    }

    @Override
    public Optional<FixedExpiration<K, V>> expireAfterAccess() {
        return policy.expireAfterAccess()
                .map(InternalPolicy::createFixedExpiration);
    }

    @Override
    public Optional<FixedExpiration<K, V>> expireAfterWrite() {
        return policy.expireAfterWrite().map(
                InternalPolicy::createFixedExpiration);
    }

    @Override
    public Optional<VarExpiration<K, V>> expireVariably() {
        return policy.expireVariably()
                .map(varExpiration -> instanceRegistry.initialize(new InternalExpiration<>(varExpiration)));
    }

    @Override
    public Optional<FixedRefresh<K, V>> refreshAfterWrite() {
        //noinspection Convert2Diamond
        return policy.refreshAfterWrite()
                .map(fixedRefresh -> new FixedRefresh<K, V>() {
                    @Override
                    public OptionalLong ageOf(K key, TimeUnit unit) {
                        return fixedRefresh.ageOf(ik(key), unit);
                    }

                    @Override
                    public long getRefreshesAfter(TimeUnit unit) {
                        return fixedRefresh.getRefreshesAfter(unit);
                    }

                    @Override
                    public void setRefreshesAfter(long duration, TimeUnit unit) {
                        fixedRefresh.setRefreshesAfter(duration, unit);
                    }
                });
    }

    static class InternalExpiration<K, V> implements VarExpiration<K, V>, InternalInitializable<K, V> {

        private final VarExpiration<InternalKey<K>, InternalValue<V>> varExpiration;

        @SuppressWarnings("NotNullFieldNotInitialized")
        private Policy<InternalKey<K>, InternalValue<V>> policy;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private InternalCacheManager<K, V> cacheManager;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private InternalSynchronizationLock synchronizationLock;

        @SuppressWarnings({"java:S2637", "NullAway.Init"})
        InternalExpiration(VarExpiration<InternalKey<K>, InternalValue<V>> varExpiration) {
            this.varExpiration = varExpiration;
            // see also initialize()
        }

        @Override
        public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
            this.policy = instanceRegistry.getCache().policy();
            this.cacheManager = instanceRegistry.getCacheManager();
            this.synchronizationLock = instanceRegistry.getSynchronizationLock();
        }

        @Override
        public @Nullable V put(K key, V value, long duration, TimeUnit unit) {
            requireNonNull(key);
            requireNonNull(value);
            requireNonNull(unit);
            return synchronizationLock.getLockedOrNull(() ->
                    vn(varExpiration.put(ik(key), cacheManager.putDistributed(ik(key), iv(value)), duration, unit)));
        }

        @Override
        public @Nullable V putIfAbsent(K key, V value, long duration, TimeUnit unit) {
            requireNonNull(key);
            requireNonNull(value);
            requireNonNull(unit);
            return synchronizationLock.getLockedOrNull(() -> {
                V oldValue = vn(policy.getIfPresentQuietly(ik(key)));
                return isNull(oldValue)
                        ? put(key, value, duration, unit) // implicit distribution
                        : oldValue;
            });
        }

        @Override
        @SuppressWarnings("NullableProblems")
        public @Nullable V compute(K key, BiFunction<? super K, ? super @Nullable V, ? extends @Nullable V> remappingFunction,
                                   Duration duration) {
            requireNonNull(key);
            requireNonNull(remappingFunction);
            requireNonNull(duration);
            BiFunction<InternalKey<K>, @Nullable InternalValue<V>, @Nullable InternalValue<V>> distributedRemapping =
                    (remappingKey, remappingValue) -> {
                        InternalValue<V> value = ivn(remappingFunction
                                .apply(k(remappingKey), vn(remappingValue)));
                        if (isNull(value)) {
                            cacheManager.invalidateDistributed(remappingKey);
                        } else {
                            cacheManager.putDistributed(remappingKey, value);
                        }
                        return value;
                    };
            return synchronizationLock.getLockedOrNull(() ->
                    vn(varExpiration.compute(ik(key), distributedRemapping, duration)));
        }

        @Override
        public OptionalLong getExpiresAfter(K key, TimeUnit unit) {
            return varExpiration.getExpiresAfter(ik(key), unit);
        }

        @Override
        public void setExpiresAfter(K key, long duration, TimeUnit unit) {
            varExpiration.setExpiresAfter(ik(key), duration, unit);
        }

        @Override
        public Map<K, V> oldest(int limit) {
            return m(varExpiration.oldest(limit));
        }

        @Override
        public <T extends @Nullable Object> T oldest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            Function<Stream<CacheEntry<InternalKey<K>, InternalValue<V>>>, T> internalMappingFunction =
                    stream -> mappingFunction.apply(stream
                            .map(InternalPolicy::createCacheEntry));
            return varExpiration.oldest(internalMappingFunction);
        }

        @Override
        public Map<K, V> youngest(int limit) {
            return m(varExpiration.youngest(limit));
        }

        @Override
        public <T extends @Nullable Object> T youngest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            Function<Stream<CacheEntry<InternalKey<K>, InternalValue<V>>>, T> internalMappingFunction =
                    stream -> mappingFunction.apply(stream
                            .map(InternalPolicy::createCacheEntry));
            return varExpiration.youngest(internalMappingFunction);
        }
    }

    private static <K, V> CacheEntry<K, V> createCacheEntry(CacheEntry<InternalKey<K>, InternalValue<V>> entry) {
        //noinspection Convert2Diamond
        return new CacheEntry<K, V>() {

            @Override
            public K getKey() {
                return k(entry.getKey());
            }

            @Override
            public V getValue() {
                return v(entry.getValue());
            }

            @Override
            public V setValue(V value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int weight() {
                return entry.weight();
            }

            @Override
            public long expiresAt() {
                return entry.expiresAt();
            }

            @Override
            public long refreshableAt() {
                return entry.refreshableAt();
            }

            @Override
            public long snapshotAt() {
                return entry.snapshotAt();
            }
        };
    }

    private static <K, V> FixedExpiration<K, V> createFixedExpiration(FixedExpiration<InternalKey<K>, InternalValue<V>> fixedExpiration) {
        //noinspection Convert2Diamond
        return new FixedExpiration<K, V>() {

            @Override
            public OptionalLong ageOf(K key, TimeUnit unit) {
                return fixedExpiration.ageOf(ik(key), unit);
            }

            @Override
            public long getExpiresAfter(TimeUnit unit) {
                return fixedExpiration.getExpiresAfter(unit);
            }

            @Override
            public void setExpiresAfter(long duration, TimeUnit unit) {
                fixedExpiration.setExpiresAfter(duration, unit);
            }

            @Override
            public Map<K, V> oldest(int limit) {
                return m(fixedExpiration.oldest(limit));
            }

            @Override
            public Map<K, V> youngest(int limit) {
                return m(fixedExpiration.youngest(limit));
            }
        };
    }
}
