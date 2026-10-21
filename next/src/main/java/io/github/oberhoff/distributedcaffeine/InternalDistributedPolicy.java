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

import org.jspecify.annotations.Nullable;

import java.util.Map;

final class InternalDistributedPolicy<K, V> implements DistributedPolicy<K, V> {

    private final InternalCore<K, V> core;

    InternalDistributedPolicy(InternalCore<K, V> core) {
        this.core = core;
    }

    @Override
    public SynchronizationState getSynchronizationState() {
        return core.state();
    }

    @Override
    public @Nullable StoredValue<V> getFromStore(K key) {
        return core.getFromStore(key);
    }

    @Override
    public Map<K, StoredValue<V>> getAllFromStore(Iterable<? extends K> keys) {
        return core.getAllFromStore(keys);
    }

    @Override
    public DistributedStats stats() {
        return core.stats();
    }
}
