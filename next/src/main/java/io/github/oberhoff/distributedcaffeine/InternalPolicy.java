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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * {@code policy()}: L1's Caffeine policy, translated from {@link InternalKey} / {@link InternalValue} to the user's
 * keys and values (see "Reading and writing methods" in VISION.md). Everything is local to this instance, as in
 * Caffeine: quiet reads, refreshes in flight, sizes, durations, and the coldest/hottest/oldest/youngest snapshots.
 * The writes of {@link Policy.VarExpiration} go through the {@code asMap()} methods (through L2); their duration
 * applies to this instance's copy only.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
final class InternalPolicy<K, V> implements Policy<K, V> {

    private final InternalCore<K, V> core;
    private final InternalConcurrentMap<K, V> map;
    private final Policy<InternalKey<K>, InternalValue<V>> policy;

    InternalPolicy(InternalCore<K, V> core, InternalConcurrentMap<K, V> map) {
        this.core = core;
        this.map = map;
        this.policy = core.l1().synchronous().policy();
    }

    @Override
    public boolean isRecordingStats() {
        return policy.isRecordingStats();
    }

    @Override
    public @Nullable V getIfPresentQuietly(K key) {
        return InternalValue.valueOrNull(policy.getIfPresentQuietly(core.wrap(key)));
    }

    @Override
    public @Nullable CacheEntry<K, V> getEntryIfPresentQuietly(K key) {
        CacheEntry<InternalKey<K>, InternalValue<V>> entry = policy.getEntryIfPresentQuietly(core.wrap(key));
        return entry == null ? null : new Entry<>(entry);
    }

    @Override
    public Map<K, CompletableFuture<V>> refreshes() {
        core.checkOpen();
        Map<K, CompletableFuture<V>> refreshes = new LinkedHashMap<>();
        policy.refreshes().forEach((internalKey, future) ->
                refreshes.put(internalKey.key(), InternalValue.valuesOf(future)));
        return Collections.unmodifiableMap(refreshes);
    }

    @Override
    public Optional<Eviction<K, V>> eviction() {
        return policy.eviction().map(EvictionView::new);
    }

    @Override
    public Optional<FixedExpiration<K, V>> expireAfterAccess() {
        return policy.expireAfterAccess().map(FixedExpirationView::new);
    }

    @Override
    public Optional<FixedExpiration<K, V>> expireAfterWrite() {
        return policy.expireAfterWrite().map(FixedExpirationView::new);
    }

    @Override
    public Optional<VarExpiration<K, V>> expireVariably() {
        return policy.expireVariably().map(VarExpirationView::new);
    }

    @Override
    public Optional<FixedRefresh<K, V>> refreshAfterWrite() {
        return policy.refreshAfterWrite().map(FixedRefreshView::new);
    }

    // translation

    private static <K, V> Map<K, V> unwrap(Map<InternalKey<K>, InternalValue<V>> snapshot) {
        Map<K, V> result = new LinkedHashMap<>();
        snapshot.forEach((internalKey, value) -> result.put(internalKey.key(), value.value()));
        return Collections.unmodifiableMap(result);
    }

    private static <K, V, T extends @Nullable Object> Function<Stream<CacheEntry<InternalKey<K>, InternalValue<V>>>, T>
            unwrap(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
        requireNonNull(mappingFunction, "mappingFunction cannot be null");
        return stream -> mappingFunction.apply(stream.map(Entry::new));
    }

    private final class EvictionView implements Eviction<K, V> {

        private final Eviction<InternalKey<K>, InternalValue<V>> eviction;

        EvictionView(Eviction<InternalKey<K>, InternalValue<V>> eviction) {
            this.eviction = eviction;
        }

        @Override
        public boolean isWeighted() {
            return eviction.isWeighted();
        }

        @Override
        public OptionalInt weightOf(K key) {
            return eviction.weightOf(core.wrap(key));
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
            return unwrap(eviction.coldest(limit));
        }

        @Override
        public Map<K, V> coldestWeighted(long weightLimit) {
            return unwrap(eviction.coldestWeighted(weightLimit));
        }

        @Override
        public <T extends @Nullable Object> T coldest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            return eviction.coldest(unwrap(mappingFunction));
        }

        @Override
        public Map<K, V> hottest(int limit) {
            return unwrap(eviction.hottest(limit));
        }

        @Override
        public Map<K, V> hottestWeighted(long weightLimit) {
            return unwrap(eviction.hottestWeighted(weightLimit));
        }

        @Override
        public <T extends @Nullable Object> T hottest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            return eviction.hottest(unwrap(mappingFunction));
        }
    }

    private final class FixedExpirationView implements FixedExpiration<K, V> {

        private final FixedExpiration<InternalKey<K>, InternalValue<V>> expiration;

        FixedExpirationView(FixedExpiration<InternalKey<K>, InternalValue<V>> expiration) {
            this.expiration = expiration;
        }

        @Override
        public OptionalLong ageOf(K key, TimeUnit unit) {
            return expiration.ageOf(core.wrap(key), unit);
        }

        @Override
        public long getExpiresAfter(TimeUnit unit) {
            return expiration.getExpiresAfter(unit);
        }

        @Override
        public void setExpiresAfter(long duration, TimeUnit unit) {
            expiration.setExpiresAfter(duration, unit);
        }

        @Override
        public Map<K, V> oldest(int limit) {
            return unwrap(expiration.oldest(limit));
        }

        @Override
        public <T extends @Nullable Object> T oldest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            return expiration.oldest(unwrap(mappingFunction));
        }

        @Override
        public Map<K, V> youngest(int limit) {
            return unwrap(expiration.youngest(limit));
        }

        @Override
        public <T extends @Nullable Object> T youngest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            return expiration.youngest(unwrap(mappingFunction));
        }
    }

    private final class VarExpirationView implements VarExpiration<K, V> {

        private final VarExpiration<InternalKey<K>, InternalValue<V>> expiration;

        VarExpirationView(VarExpiration<InternalKey<K>, InternalValue<V>> expiration) {
            this.expiration = expiration;
        }

        @Override
        public OptionalLong getExpiresAfter(K key, TimeUnit unit) {
            return expiration.getExpiresAfter(core.wrap(key), unit);
        }

        @Override
        public void setExpiresAfter(K key, long duration, TimeUnit unit) {
            expiration.setExpiresAfter(core.wrap(key), duration, unit);
        }

        @Override
        public @Nullable V putIfAbsent(K key, V value, long duration, TimeUnit unit) {
            V previous = map.putIfAbsent(key, value);
            if (previous == null) {
                setExpiresAfter(key, duration, unit);
            }
            return previous;
        }

        @Override
        public @Nullable V put(K key, V value, long duration, TimeUnit unit) {
            V previous = map.put(key, value);
            setExpiresAfter(key, duration, unit);
            return previous;
        }

        @Override
        public @Nullable V compute(K key,
                                   BiFunction<? super K, ? super @Nullable V, ? extends @Nullable V> remappingFunction,
                                   Duration duration) {
            requireNonNull(duration, "duration cannot be null");
            V value = map.compute(key, remappingFunction);
            if (value != null) {
                setExpiresAfter(key, duration);
            }
            return value;
        }

        @Override
        public Map<K, V> oldest(int limit) {
            return unwrap(expiration.oldest(limit));
        }

        @Override
        public <T extends @Nullable Object> T oldest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            return expiration.oldest(unwrap(mappingFunction));
        }

        @Override
        public Map<K, V> youngest(int limit) {
            return unwrap(expiration.youngest(limit));
        }

        @Override
        public <T extends @Nullable Object> T youngest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            return expiration.youngest(unwrap(mappingFunction));
        }
    }

    private final class FixedRefreshView implements FixedRefresh<K, V> {

        private final FixedRefresh<InternalKey<K>, InternalValue<V>> refresh;

        FixedRefreshView(FixedRefresh<InternalKey<K>, InternalValue<V>> refresh) {
            this.refresh = refresh;
        }

        @Override
        public OptionalLong ageOf(K key, TimeUnit unit) {
            return refresh.ageOf(core.wrap(key), unit);
        }

        @Override
        public long getRefreshesAfter(TimeUnit unit) {
            return refresh.getRefreshesAfter(unit);
        }

        @Override
        public void setRefreshesAfter(long duration, TimeUnit unit) {
            refresh.setRefreshesAfter(duration, unit);
            core.refreshAfterWrite(refresh.getRefreshesAfter());
        }
    }

    // a snapshot of an L1 entry, unwrapped; immutable like Caffeine's
    private record Entry<K, V>(K key, V value, int weight, long expiresAt, long refreshableAt, long snapshotAt)
            implements CacheEntry<K, V> {

        Entry(CacheEntry<InternalKey<K>, InternalValue<V>> entry) {
            this(entry.getKey().key(), entry.getValue().value(), entry.weight(), entry.expiresAt(),
                    entry.refreshableAt(), entry.snapshotAt());
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(@Nullable Object o) {
            return o instanceof Map.Entry<?, ?> e && key.equals(e.getKey()) && value.equals(e.getValue());
        }

        @Override
        public int hashCode() {
            return key.hashCode() ^ value.hashCode();
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }
}
