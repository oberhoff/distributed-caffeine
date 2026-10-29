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

import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

// The asynchronous facade: translates Caffeine's AsyncCache methods to the core. User functions run on Caffeine's
// executor, as in Caffeine; the store executor waits for them.
class InternalDistributedAsyncCache<K, V> implements DistributedAsyncCache<K, V> {

    private static final String MAPPING_FUNCTION_CANNOT_BE_NULL = "mappingFunction cannot be null";

    final InternalCore<K, V> core;
    final Executor executor;
    private final DistributedCache<K, V> synchronous;
    private final ConcurrentMap<K, CompletableFuture<V>> map;

    InternalDistributedAsyncCache(InternalCore<K, V> core, Executor executor) {
        this(core, executor, new InternalDistributedCache<>(core));
    }

    InternalDistributedAsyncCache(InternalCore<K, V> core, Executor executor, DistributedCache<K, V> synchronous) {
        this.core = core;
        this.executor = executor;
        this.synchronous = synchronous;
        this.map = new InternalAsyncConcurrentMap<>(core, synchronous.asMap());
    }

    @Override
    public CompletableFuture<@Nullable V> getIfPresent(K key) {
        return core.getIfPresentAsync(key);
    }

    @Override
    public CompletableFuture<@Nullable V> get(K key, Function<? super K, ? extends @Nullable V> mappingFunction) {
        requireNonNull(mappingFunction, MAPPING_FUNCTION_CANNOT_BE_NULL);
        return core.getAsync(key, k -> CompletableFuture.supplyAsync(() -> mappingFunction.apply(k), executor));
    }

    @Override
    public CompletableFuture<@Nullable V> get(K key, BiFunction<? super K, ? super Executor,
            ? extends CompletableFuture<? extends @Nullable V>> mappingFunction) {
        requireNonNull(mappingFunction, MAPPING_FUNCTION_CANNOT_BE_NULL);
        return core.getAsync(key, k -> mappingFunction.apply(k, executor));
    }

    @Override
    public CompletableFuture<Map<K, V>> getAll(Iterable<? extends K> keys,
                                               Function<? super Set<? extends K>, ? extends Map<? extends K, ? extends V>>
                                                       mappingFunction) {
        requireNonNull(mappingFunction, MAPPING_FUNCTION_CANNOT_BE_NULL);
        return core.getAllAsync(keys, missing -> CompletableFuture.supplyAsync(() -> mappingFunction.apply(missing),
                executor));
    }

    @Override
    public CompletableFuture<Map<K, V>> getAll(Iterable<? extends K> keys,
                                               BiFunction<? super Set<? extends K>, ? super Executor,
                                                       ? extends CompletableFuture<? extends Map<? extends K, ? extends V>>>
                                                       mappingFunction) {
        requireNonNull(mappingFunction, MAPPING_FUNCTION_CANNOT_BE_NULL);
        return core.getAllAsync(keys, missing -> mappingFunction.apply(missing, executor));
    }

    @Override
    public void put(K key, CompletableFuture<? extends @Nullable V> valueFuture) {
        core.putAsync(key, valueFuture);
    }

    @Override
    public ConcurrentMap<K, CompletableFuture<V>> asMap() {
        core.checkOpen();
        return map;
    }

    @Override
    public DistributedCache<K, V> synchronous() {
        return synchronous;
    }

    @Override
    public DistributedPolicy<K, V> distributedPolicy() {
        return synchronous.distributedPolicy();
    }

    @Override
    public void close() {
        core.close();
    }
}
