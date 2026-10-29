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

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * A Caffeine {@link LoadingCache} whose instances share a store; see {@link DistributedCache}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface DistributedLoadingCache<K, V> extends DistributedCache<K, V>, LoadingCache<K, V> {

    /**
     * {@inheritDoc}
     * <p>
     * A loaded value is cached only if the key was neither written nor removed on any instance while it loaded;
     * otherwise a value written meanwhile is returned instead, and after a removal the key is loaded again. After three
     * attempts in a row each met by a removal, the last loaded value is returned without being cached, as Caffeine's
     * asynchronous cache does with a load an invalidation discards
     * ({@code distributedPolicy().stats().uncachedLoadCount()}).
     */
    // a loader resolving to null caches nothing.
    // Caffeine documents a null result ("or null if the computed value is null") but cannot express it for a value
    // type that is not nullable; declaring the value type nullable would declare every other method nullable too
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    @Nullable V get(K key);

    // completes with null where the reload resolved to null; see get(K) for the types
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    CompletableFuture<@Nullable V> refresh(K key);
}
