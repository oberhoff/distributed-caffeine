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
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.LazyInitializer;
import org.checkerframework.checker.nullness.qual.Nullable;

class InternalRemovalListener<K, V> implements RemovalListener<K, V>, LazyInitializer<K, V> {

    private final RemovalListener<K, V> removalListener;

    InternalRemovalListener(RemovalListener<K, V> removalListener) {
        this.removalListener = removalListener;
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
    }

    @Override
    public void onRemoval(@Nullable K key, @Nullable V value, RemovalCause removalCause) {
        removalListener.onRemoval(key, value, removalCause);
    }
}
