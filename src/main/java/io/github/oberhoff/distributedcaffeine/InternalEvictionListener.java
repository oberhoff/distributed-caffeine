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

import static io.github.oberhoff.distributedcaffeine.InternalKey.kn;
import static io.github.oberhoff.distributedcaffeine.InternalValue.vn;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalEvictionListener<K, V> implements RemovalListener<InternalKey<K>, InternalValue<V>>,
        InternalInitializable<K, V> {

    private final RemovalListener<K, V> evictionListener;

    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalCacheManager<K, V> cacheManager;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
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
        // an eviction is reported asynchronously, so it can arrive once this cache instance counts as activated
        // again although it took place while it did not - which the value says, because it carries the activation it
        // became content of and only that activation's content is this cache instance's to distribute. Reporting it
        // below is another matter
        if (nonNull(key) && nonNull(value) && cacheManager.hasCurrentActivationId(value)) {
            // special handling, no lock required
            cacheManager.evictDistributed(key, value, removalCause);
        }
        evictionListener.onRemoval(kn(key), vn(value), removalCause);
    }
}
