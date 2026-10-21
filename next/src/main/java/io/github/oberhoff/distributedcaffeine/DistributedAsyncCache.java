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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A Caffeine {@link AsyncCache} whose instances share a store; see {@link DistributedCache}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface DistributedAsyncCache<K, V> extends AsyncCache<K, V>, AutoCloseable {

    /**
     * Returns access to the distributed side of this cache.
     *
     * @return the distributed policy
     */
    DistributedPolicy<K, V> distributedPolicy();

    // never null, unlike Caffeine's: a miss in the local level is read through the store, so whether the value is
    // absent is known only when the future completes, with null.
    // Caffeine's futures complete with null where nothing is found, which its types cannot express for a value type
    // that is not nullable
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    CompletableFuture<@Nullable V> getIfPresent(K key);

    // Caffeine's futures complete with null where nothing is found, which its types cannot express for a value type
    // that is not nullable
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    CompletableFuture<@Nullable V> get(K key, Function<? super K, ? extends @Nullable V> mappingFunction);

    // Caffeine's futures complete with null where nothing is found, which its types cannot express for a value type
    // that is not nullable
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    CompletableFuture<@Nullable V> get(K key, BiFunction<? super K, ? super Executor,
            ? extends CompletableFuture<? extends @Nullable V>> mappingFunction);

    @Override
    DistributedCache<K, V> synchronous();

    /**
     * Closes this cache instance, as {@link DistributedCache#close()}.
     */
    @Override
    void close();
}
