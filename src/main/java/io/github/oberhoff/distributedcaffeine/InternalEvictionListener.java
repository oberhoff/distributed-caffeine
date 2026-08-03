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

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.jspecify.annotations.Nullable;

import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalEvictionListener<K, V> implements RemovalListener<InternalKey<K>, InternalValue<V>>,
        InternalInitializable<K, V> {

    private final RemovalListener<K, V> evictionListener;

    private InternalCacheManager<K, V> cacheManager;

    InternalEvictionListener(RemovalListener<K, V> evictionListener) {
        this.evictionListener = requireNonNull(evictionListener);
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.cacheManager = instanceRegistry.getCacheManager();
    }

    @Override
    public void onRemoval(@Nullable InternalKey<K> key, @Nullable InternalValue<V> value, RemovalCause removalCause) {
        // a stale entry is one the data store has not confirmed since synchronization was (re)started, and it keeps
        // occupying the size budget until the sweep removes it. Distributing its eviction would hand the cluster a
        // value this instance is in the middle of discarding, and reporting it would announce an eviction for an
        // entry that only still exists because reconciling with the store has not caught up yet
        if (nonNull(value) && value.isStale()) {
            return;
        }
        // special handling, no lock required
        cacheManager.evictDistributed(key, value, removalCause);
        evictionListener.onRemoval(k(key), v(value), removalCause);
    }
}
