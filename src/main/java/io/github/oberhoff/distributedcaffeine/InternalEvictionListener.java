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

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

class InternalEvictionListener<K, V> implements RemovalListener<K, V>, InternalLazyInitializer<K, V> {

    private final RemovalListener<K, V> evictionListener;

    private InternalCacheManager<K, V> cacheManager;

    InternalEvictionListener(RemovalListener<K, V> evictionListener) {
        this.evictionListener = evictionListener;
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.cacheManager = distributedCaffeine.getCacheManager();
    }

    @Override
    public void onRemoval(K key, V value, RemovalCause removalCause) {
        // special handling, no lock required
        cacheManager.evictDistributed(key, value, removalCause);
        evictionListener.onRemoval(key, value, removalCause);
    }
}
