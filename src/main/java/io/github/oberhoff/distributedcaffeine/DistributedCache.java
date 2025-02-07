/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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

/**
 * Interface representing a cache instance. Cache entries are manually added and are stored in the cache until either
 * invalidated or evicted.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public interface DistributedCache<K, V> extends Cache<K, V> {

    /**
     * Returns an access point for inspecting and performing low-level operations on the cache instance.
     *
     * @return the {@link DistributedPolicy} as an access point
     */
    DistributedPolicy<K, V> distributedPolicy();
}
