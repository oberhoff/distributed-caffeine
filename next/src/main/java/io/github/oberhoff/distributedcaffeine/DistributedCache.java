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

import java.util.function.Function;

/**
 * A Caffeine {@link Cache} whose instances share a store: it contains what the store holds, reads a key through the
 * store when this instance does not hold it, and keeps every instance's copies in step through the store's
 * announcements. Behaves as Caffeine's cache, with the documented deviations.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface DistributedCache<K, V> extends Cache<K, V>, AutoCloseable {

    /**
     * Returns access to the distributed side of this cache: its synchronization state, its stats and the store.
     *
     * @return the distributed policy
     */
    DistributedPolicy<K, V> distributedPolicy();

    // a mapping function resolving to null is accepted (as by ConcurrentMap.computeIfAbsent) and caches nothing.
    // Caffeine documents a null result ("or null if the computed value is null") but cannot express it for a value
    // type that is not nullable; declaring the value type nullable would declare every other method nullable too
    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    @Nullable V get(K key, Function<? super K, ? extends @Nullable V> mappingFunction);

    /**
     * Closes this cache instance: unsubscribes, stops its maintenance and releases shared connections when it was
     * their last user. Afterwards every operation throws {@link IllegalStateException}, except
     * {@code distributedPolicy().getSynchronizationState()}. Idempotent.
     */
    @Override
    void close();
}
