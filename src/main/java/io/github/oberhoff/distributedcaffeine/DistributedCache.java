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

import com.github.benmanes.caffeine.cache.Cache;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.function.Function;

/**
 * A Caffeine {@link Cache} whose instances share a store. Each instance holds its own copies, reads a key from the
 * store when it holds none, and drops or updates its copies when the key changes elsewhere. Behaves as Caffeine's
 * cache, except where a method documents otherwise.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Andreas Oberhoff
 */
public interface DistributedCache<K, V> extends Cache<K, V>, AutoCloseable {

    /**
     * Returns the distributed side of this cache: its synchronization state, its stats and direct reads of the store.
     *
     * @return the distributed policy
     */
    DistributedPolicy<K, V> distributedPolicy();

    /**
     * {@inheritDoc}
     * <p>
     * If this instance holds no usable copy, the store is read, and a value found there is returned and kept by this
     * instance. The loader is never called. If the store does not answer, the key counts as absent. To look at this
     * instance's copy only, use {@code policy().getIfPresentQuietly(key)}.
     */
    @Override
    @Nullable V getIfPresent(K key);

    /**
     * {@inheritDoc}
     * <p>
     * As {@link #getIfPresent}: the keys this instance holds no usable copy of are read from the store, in one batch.
     */
    @Override
    Map<K, V> getAllPresent(Iterable<? extends K> keys);

    /**
     * {@inheritDoc}
     * <p>
     * A computed value is cached only if no instance wrote or removed the key during the computation. If one wrote it,
     * that value is returned instead; if one removed it, the value is computed again. After three such removals in a
     * row, the last computed value is returned without being cached
     * ({@code distributedPolicy().stats().uncachedLoadCount()}).
     */
    // a mapping function resolving to null is accepted (as by ConcurrentMap.computeIfAbsent) and caches nothing.
    // Caffeine documents a null result ("or null if the computed value is null") but cannot express it for a value
    // type that is not nullable; declaring the value type nullable would declare every other method nullable too
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    @Nullable V get(K key, Function<? super K, ? extends @Nullable V> mappingFunction);

    /**
     * Closes this cache instance and releases its connections to the store. Afterwards every operation throws
     * {@link IllegalStateException}, except {@code distributedPolicy().getSynchronizationState()}. Idempotent.
     */
    @Override
    void close();
}
