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

import static java.util.Objects.requireNonNull;

/**
 * @author Andreas Oberhoff
 */
final class InternalDistributedPolicy<K, V> implements DistributedPolicy<K, V> {

    private final InternalCore<K, V> core;
    private final Store<K, V> store;

    InternalDistributedPolicy(InternalCore<K, V> core) {
        this.core = core;
        this.store = new StoreView();
    }

    @Override
    public SynchronizationState synchronizationState() {
        return core.state();
    }

    @Override
    public Store<K, V> store() {
        return store;
    }

    private final class StoreView implements Store<K, V> {

        // present if the cache was built with the bound
        private final @Nullable MaximumAge maximumAge = core.hasMaximumAge() ? new MaximumAgeView() : null;
        private final @Nullable MaximumAgePerEntry<K, V> perEntry = core.hasMaximumAgePerEntry()
                ? (key, value, age) -> core.put(key, value, requireNonNull(age, "maximumAge cannot be null"))
                : null;
        private final @Nullable MaximumSize maximumSize = core.hasMaximumSize() ? new MaximumSizeView() : null;

        @Override
        public @Nullable StoredValue<V> get(K key) {
            return core.getFromStore(key);
        }

        @Override
        public Map<K, StoredValue<V>> getAll(Iterable<? extends K> keys) {
            return core.getAllFromStore(keys);
        }

        @Override
        public Optional<MaximumAge> maximumAge() {
            return Optional.ofNullable(maximumAge);
        }

        @Override
        public Optional<MaximumAgePerEntry<K, V>> maximumAgePerEntry() {
            return Optional.ofNullable(perEntry);
        }

        @Override
        public Optional<MaximumSize> maximumSize() {
            return Optional.ofNullable(maximumSize);
        }

        @Override
        public Mark mark() {
            return core.markStore();
        }

        @Override
        public void invalidateAllBeforeMark(Mark mark) {
            core.invalidateAllBeforeMark(mark);
        }

        @Override
        public void invalidateAllOlderThan(Duration olderThan) {
            core.invalidateAllOlderThan(olderThan);
        }
    }

    private final class MaximumAgeView implements MaximumAge {

        @Override
        public Duration getMaximumAge() {
            return core.maximumAge();
        }

        @Override
        public void setMaximumAge(Duration maximumAge) {
            core.maximumAge(maximumAge);
        }
    }

    private final class MaximumSizeView implements MaximumSize {

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
