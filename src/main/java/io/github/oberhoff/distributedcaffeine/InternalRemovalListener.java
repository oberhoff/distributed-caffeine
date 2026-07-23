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
import static java.util.Objects.requireNonNull;

class InternalRemovalListener<K, V> implements RemovalListener<InternalKey<K>, InternalValue<V>>,
        InternalLazyInitializer<K, V> {

    private final RemovalListener<K, V> removalListener;

    InternalRemovalListener(RemovalListener<K, V> removalListener) {
        this.removalListener = requireNonNull(removalListener);
        // see also initialize()
    }

    InternalRemovalListener<K, V> neutralize() {
        // noop
        return this;
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        // noop
    }

    @Override
    public void onRemoval(@Nullable InternalKey<K> key, @Nullable InternalValue<V> value, RemovalCause removalCause) {
        removalListener.onRemoval(k(key), v(value), removalCause);
    }
}
