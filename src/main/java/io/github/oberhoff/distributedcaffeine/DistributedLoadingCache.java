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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A Caffeine {@link LoadingCache} whose instances share a store. Each instance holds its own copies, reads a key from
 * the store when it holds none, and drops or updates its copies when the key changes elsewhere. Behaves as Caffeine's
 * cache, except where a method documents otherwise. A call that needs the store and cannot reach it, or a write whose
 * outcome is unknown, throws the unchecked {@link io.github.oberhoff.distributedcaffeine.spi.StoreException}.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Andreas Oberhoff
 */
public interface DistributedLoadingCache<K, V> extends DistributedCache<K, V>, LoadingCache<K, V> {

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> A loaded value is cached only if the key was neither written nor removed on any
     * instance while it loaded; otherwise a value written meanwhile is returned instead, and after a removal the key is
     * loaded again. After three attempts in a row each met by a removal, the last loaded value is returned without
     * being cached, as Caffeine's asynchronous cache does with a load an invalidation discards ({@code
     * distributedPolicy().stats().uncachedLoadCount()}).
     */
    // a loader resolving to null caches nothing.
    // Caffeine documents a null result ("or null if the computed value is null") but cannot express it for a value
    // type that is not nullable; declaring the value type nullable would declare every other method nullable too
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    @Nullable V get(K key);

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
    Map<K, V> getAll(Iterable<? extends K> keys);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> A refresh always calls the loader, even where the store holds a value. Its result is
     * written to the store only if no instance wrote or removed the key since this instance got the value it refreshes;
     * otherwise the future completes with what the store holds now, or with {@code null} if the key was removed. While
     * the store does not answer, the result is kept locally without being written.
     */
    // completes with null where the reload resolved to null; see get(K) for the types
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    CompletableFuture<@Nullable V> refresh(K key);

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> The keys are refreshed one by one. Each refresh calls the loader, even where the
     * store holds a value. Its result is written to the store only if no instance wrote or removed the key since this
     * instance got the value it refreshes; otherwise the key gets what the store holds now, or none if it was removed.
     * While the store does not answer, the results are kept locally without being written.
     */
    @Override
    CompletableFuture<Map<K, V>> refreshAll(Iterable<? extends K> keys);
}
