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

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * @author Andreas Oberhoff
 */
final class InternalDistributedPolicy<K, V> implements DistributedPolicy<K, V> {

    private final InternalCore<K, V> core;
    // present if the cache was built with the bound
    private final @Nullable StoreMaximumAge maximumAge;
    private final @Nullable StoreMaximumSize maximumSize;

    InternalDistributedPolicy(InternalCore<K, V> core) {
        this.core = core;
        this.maximumAge = core.hasMaximumAge() ? new MaximumAgeView() : null;
        this.maximumSize = core.hasMaximumSize() ? new MaximumSizeView() : null;
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

    @Override
    public Optional<StoreMaximumAge> storeMaximumAge() {
        return Optional.ofNullable(maximumAge);
    }

    @Override
    public Optional<StoreMaximumSize> storeMaximumSize() {
        return Optional.ofNullable(maximumSize);
    }

    private final class MaximumAgeView implements StoreMaximumAge {

        @Override
        public Duration getMaximumAge() {
            return core.maximumAge();
        }

        @Override
        public void setMaximumAge(Duration maximumAge) {
            core.maximumAge(maximumAge);
        }
    }

    private final class MaximumSizeView implements StoreMaximumSize {

        @Override
        public long getMaximumSize() {
            return core.maximumSize();
        }

        @Override
        public void setMaximumSize(long maximumSize) {
            core.maximumSize(maximumSize);
        }
    }
}
