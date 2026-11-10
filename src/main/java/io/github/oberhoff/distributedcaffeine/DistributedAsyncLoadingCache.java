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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A Caffeine {@link AsyncLoadingCache} whose instances share a store. Each instance holds its own copies, reads a key
 * from the store when it holds none, and drops or updates its copies when the key changes elsewhere. Behaves as
 * Caffeine's cache, except where a method documents otherwise. A call that needs the store and cannot reach it, or a
 * write whose outcome is unknown, throws the unchecked {@link
 * io.github.oberhoff.distributedcaffeine.spi.StoreException}.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Andreas Oberhoff
 */
public interface DistributedAsyncLoadingCache<K, V> extends DistributedAsyncCache<K, V>, AsyncLoadingCache<K, V> {

    @Override
    DistributedLoadingCache<K, V> synchronous();

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> A loaded value is cached only if the key was neither written nor removed on any
     * instance while it loaded; otherwise a value written meanwhile is returned instead, and after a removal the key is
     * loaded again. After three attempts in a row each met by a removal, the last loaded value is returned without
     * being cached, as Caffeine's asynchronous cache does with a load an invalidation discards ({@code
     * distributedPolicy().stats().uncachedLoadCount()}).
     */
    // Caffeine's futures complete with null where nothing is found, which its types cannot express for a value type
    // that is not nullable
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    CompletableFuture<@Nullable V> get(K key);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> The keys this instance holds no usable copy of are read from the store in one batch;
     * only those missing there are loaded. A loaded value is cached only if no instance wrote or removed its key while
     * it loaded. If one wrote it, that value is returned instead; if one removed it, the key is loaded again on its
     * own, up to three times. If a removal meets each of these too, the last loaded value is returned without being
     * cached ({@code distributedPolicy().stats().uncachedLoadCount()}).
     */
    @Override
    CompletableFuture<Map<K, V>> getAll(Iterable<? extends K> keys);
}
