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

import com.github.benmanes.caffeine.cache.AsyncCache;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A Caffeine {@link AsyncCache} whose instances share a store. Each instance holds its own copies, reads a key from the
 * store when it holds none, and drops or updates its copies when the key changes elsewhere. Behaves as Caffeine's
 * cache, except where a method documents otherwise. A call that needs the store and cannot reach it, or a write whose
 * outcome is unknown, throws the unchecked {@link io.github.oberhoff.distributedcaffeine.spi.StoreException}.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Andreas Oberhoff
 */
public interface DistributedAsyncCache<K, V> extends AsyncCache<K, V>, AutoCloseable {

    /**
     * Returns the distributed side of this cache: its synchronization state, its stats and direct reads of the store.
     *
     * @return the distributed policy
     */
    DistributedPolicy<K, V> distributedPolicy();

    /**
     * Returns a future of the value associated with the key, without calling the loader.
     * <p>
     * <b>Distributed Caffeine:</b> Unlike Caffeine's, the future is never {@code null}. If this instance holds no
     * usable copy, the store is read: the future completes with the value found there, which this instance keeps, or
     * with {@code null} if there is none or the store does not answer. To look at this instance's copy only, use {@code
     * synchronous().policy().getIfPresentQuietly(key)}.
     *
     * @param key the key whose associated value is to be returned
     * @return a future of the value, completing with {@code null} if the cache contains no value for the key
     * @throws NullPointerException if the specified key is null
     */
    // whether the value is absent is known only when the future completes, with null. Caffeine's futures complete
    // with null where nothing is found, which its types cannot express for a value type that is not nullable
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    CompletableFuture<@Nullable V> getIfPresent(K key);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> A computed value is cached only if no instance wrote or removed the key during the
     * computation. If one wrote it, that value is returned instead; if one removed it, the value is computed again.
     * After three such removals in a row, the last computed value is returned without being cached ({@code
     * distributedPolicy().stats().uncachedLoadCount()}).
     */
    // Caffeine's futures complete with null where nothing is found, which its types cannot express for a value type
    // that is not nullable
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    CompletableFuture<@Nullable V> get(K key, Function<? super K, ? extends @Nullable V> mappingFunction);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> A computed value is cached only if no instance wrote or removed the key during the
     * computation. If one wrote it, that value is returned instead; if one removed it, the value is computed again.
     * After three such removals in a row, the last computed value is returned without being cached ({@code
     * distributedPolicy().stats().uncachedLoadCount()}).
     */
    // Caffeine's futures complete with null where nothing is found, which its types cannot express for a value type
    // that is not nullable
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    CompletableFuture<@Nullable V> get(K key, BiFunction<? super K, ? super Executor,
            ? extends CompletableFuture<? extends @Nullable V>> mappingFunction);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> The keys this instance holds no usable copy of are read from the store in one batch;
     * only those missing there are computed. A computed value is cached only if no instance wrote or removed its key
     * during the computation. If one wrote it, that value is returned instead; if one removed it, the key is computed
     * again on its own, up to three times. If a removal meets each of these too, the last computed value is returned
     * without being cached ({@code distributedPolicy().stats().uncachedLoadCount()}).
     */
    @Override
    CompletableFuture<Map<K, V>> getAll(Iterable<? extends K> keys,
            Function<? super Set<? extends K>, ? extends Map<? extends K, ? extends V>> mappingFunction);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> The keys this instance holds no usable copy of are read from the store in one batch;
     * only those missing there are computed. A computed value is cached only if no instance wrote or removed its key
     * during the computation. If one wrote it, that value is returned instead; if one removed it, the key is computed
     * again on its own, up to three times. If a removal meets each of these too, the last computed value is returned
     * without being cached ({@code distributedPolicy().stats().uncachedLoadCount()}).
     */
    @Override
    CompletableFuture<Map<K, V>> getAll(Iterable<? extends K> keys, BiFunction<? super Set<? extends K>,
            ? super Executor, ? extends CompletableFuture<? extends Map<? extends K, ? extends V>>> mappingFunction);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> The value is written to the store once the future completes. If that write fails, no
     * caller can be told: the key is left absent here, and the failure is logged.
     */
    @Override
    void put(K key, CompletableFuture<? extends @Nullable V> valueFuture);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> Lookups ({@code get}, {@code getOrDefault}, {@code containsKey}) read the store on a
     * local miss before they return; {@code get} returns the future of a load still in flight, or else a completed one.
     * Writes and removals, also through the key, value and entry sets, go to the store and, with distribution, to every
     * instance; the atomic methods ({@code compute}, {@code merge}, ...) are atomic across instances. Sizes and
     * iteration cover only the copies of this instance.
     */
    @Override
    ConcurrentMap<K, CompletableFuture<V>> asMap();

    @Override
    DistributedCache<K, V> synchronous();

    /**
     * Closes this cache instance and releases its connections to the store. The local copies are dropped without
     * notifying the removal listener. Afterwards every operation throws {@link IllegalStateException}, except
     * {@code distributedPolicy().getSynchronizationState()} and {@code distributedPolicy().stats()}. Idempotent.
     */
    @Override
    void close();
}
