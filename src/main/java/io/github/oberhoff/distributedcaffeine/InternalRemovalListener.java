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

class InternalRemovalListener<K, V> implements RemovalListener<InternalKey<K>, InternalValue<V>>,
        InternalInitializable<K, V> {

    private final RemovalListener<K, V> removalListener;

    InternalRemovalListener(RemovalListener<K, V> removalListener) {
        this.removalListener = requireNonNull(removalListener);
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        // noop
    }

    @Override
    public void onRemoval(@Nullable InternalKey<K> key, @Nullable InternalValue<V> value, RemovalCause removalCause) {
        // a stale entry is one the data store has not confirmed since synchronization was (re)started, so it is being
        // removed (or replaced) by reconciling with the store rather than by anything done to the cache. Reporting it
        // would announce removals for entries the application never removed
        if (nonNull(value) && value.isStale()) {
            return;
        }
        removalListener.onRemoval(kn(key), vn(value), removalCause);
    }
}
