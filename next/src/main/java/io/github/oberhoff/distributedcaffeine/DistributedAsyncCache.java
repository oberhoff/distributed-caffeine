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

    /**
     * Returns a future of the value associated with the key, without calling the loader.
     * <p>
     * Unlike Caffeine's, the returned future is never {@code null}. If this instance does not hold the key, or holds
     * a copy it has to confirm first (after missed announcements), the store is read: the future completes with the
     * value found there, which this instance holds afterwards, or with {@code null} if there is none or the store
     * does not answer. A load in flight on this instance is waited for. To look at this instance's copies only,
     * without reading the store, use {@code synchronous().policy().getIfPresentQuietly(key)}.
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
