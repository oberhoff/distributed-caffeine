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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
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
 * @author Andreas Oberhoff
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

    // with a maximum age, L1 expires variably even where the user set fixed durations (InternalExpiry): those are
    // shown as Caffeine shows them, emulated, and the variable expiry only where the user set one

    @Override
    public Optional<FixedExpiration<K, V>> expireAfterAccess() {
        InternalExpiry<K, V> emulated = emulated();
        if (emulated == null) {
            return policy.expireAfterAccess().map(FixedExpirationView::new);
        }
        return emulated.afterAccess() == InternalExpiry.NONE ? Optional.empty()
                : Optional.of(new EmulatedExpirationView(emulated, true));
    }

    @Override
    public Optional<FixedExpiration<K, V>> expireAfterWrite() {
        InternalExpiry<K, V> emulated = emulated();
        return emulated == null ? policy.expireAfterWrite().map(FixedExpirationView::new)
                : Optional.of(new EmulatedExpirationView(emulated, false));
    }

    @Override
    public Optional<VarExpiration<K, V>> expireVariably() {
        return emulated() != null ? Optional.empty() : policy.expireVariably().map(VarExpirationView::new);
    }

    // the expiry emulating fixed durations, if L1 has one
    private @Nullable InternalExpiry<K, V> emulated() {
        InternalExpiry<K, V> expiry = core.expiry();
        return expiry == null || expiry.isVariable() ? null : expiry;
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

        // at most until the copy must go, as the store's maximum age bounds every duration of the user's Expiry
        @Override
        public void setExpiresAfter(K key, long duration, TimeUnit unit) {
            InternalKey<K> internalKey = core.wrap(key);
            InternalExpiry<K, V> expiry = core.expiry();
            InternalValue<V> held = expiry == null ? null : policy.getIfPresentQuietly(internalKey);
            if (expiry == null || held == null || duration < 0) {
                expiration.setExpiresAfter(internalKey, duration, unit); // Caffeine rejects a negative one
                return;
            }
            expiration.setExpiresAfter(internalKey,
                    expiry.capped(held, expiry.now(), saturatedNanos(duration, unit)), TimeUnit.NANOSECONDS);
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

    // Caffeine's fixed policy of expireAfterWrite (or of expireAfterAccess), emulated over L1's variable expiry: its
    // duration as set, the ages since the install (or the last read), and the order by them. Setting the duration
    // applies it to the held copies at once, as Caffeine's fixed policy does, each still bounded by the store's maximum
    // age (InternalExpiry#left)
    private final class EmulatedExpirationView implements FixedExpiration<K, V> {

        private final InternalExpiry<K, V> expiry;
        private final boolean access;

        EmulatedExpirationView(InternalExpiry<K, V> expiry, boolean access) {
            this.expiry = expiry;
            this.access = access;
        }

        @Override
        public OptionalLong ageOf(K key, TimeUnit unit) {
            requireNonNull(unit, "unit cannot be null");
            InternalValue<V> held = policy.getIfPresentQuietly(core.wrap(key));
            return held == null ? OptionalLong.empty()
                    : OptionalLong.of(unit.convert(expiry.now() - since(held), TimeUnit.NANOSECONDS));
        }

        @Override
        public long getExpiresAfter(TimeUnit unit) {
            return unit.convert(access ? expiry.afterAccess() : expiry.afterWrite(), TimeUnit.NANOSECONDS);
        }

        @Override
        public void setExpiresAfter(long duration, TimeUnit unit) {
            if (duration < 0) {
                throw new IllegalArgumentException("duration cannot be negative: " + duration + " " + unit);
            }
            long nanos = saturatedNanos(duration, unit);
            if (access) {
                expiry.afterAccess(nanos);
            } else {
                expiry.afterWrite(nanos);
            }
            expiry.reapply(policy);
        }

        @Override
        public Map<K, V> oldest(int limit) {
            return oldest(stream -> unwrapped(stream.limit(limit)));
        }

        @Override
        public <T extends @Nullable Object> T oldest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            return mapped(Comparator.naturalOrder(), mappingFunction);
        }

        @Override
        public Map<K, V> youngest(int limit) {
            return youngest(stream -> unwrapped(stream.limit(limit)));
        }

        @Override
        public <T extends @Nullable Object> T youngest(Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            return mapped(Comparator.reverseOrder(), mappingFunction);
        }

        private <T extends @Nullable Object> T mapped(Comparator<Long> order,
                                                      Function<Stream<CacheEntry<K, V>>, T> mappingFunction) {
            requireNonNull(mappingFunction, "mappingFunction cannot be null");
            return mappingFunction.apply(ordered(order));
        }

        private long since(InternalValue<V> value) {
            return access ? value.accessedAt() : value.installedAt();
        }

        // the held copies by install (or last read): the times taken first, as a read may move them while sorting
        private Stream<CacheEntry<K, V>> ordered(Comparator<Long> order) {
            List<Timed<K, V>> timed = policy.expireVariably().orElseThrow().oldest(stream -> stream
                    .map(entry -> new Timed<>(entry, since(entry.getValue()))).toList());
            return timed.stream().sorted(Comparator.comparing(Timed::time, order))
                    .map(entry -> new Entry<>(entry.entry()));
        }
    }

    private record Timed<K, V>(CacheEntry<InternalKey<K>, InternalValue<V>> entry, long time) {
    }

    private static <K, V> Map<K, V> unwrapped(Stream<CacheEntry<K, V>> entries) {
        Map<K, V> result = new LinkedHashMap<>();
        entries.forEach(entry -> result.put(entry.getKey(), entry.getValue()));
        return Collections.unmodifiableMap(result);
    }

    // as Caffeine converts a duration with its unit, saturated
    private static long saturatedNanos(long duration, TimeUnit unit) {
        return requireNonNull(unit, "unit cannot be null").toNanos(duration);
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
