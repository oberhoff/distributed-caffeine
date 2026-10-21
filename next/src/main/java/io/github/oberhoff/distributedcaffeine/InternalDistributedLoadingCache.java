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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

// The synchronous loading facade.
final class InternalDistributedLoadingCache<K, V> extends InternalDistributedCache<K, V>
        implements DistributedLoadingCache<K, V> {

    private final InternalCore.BulkLoader<K, V> bulkLoader;

    InternalDistributedLoadingCache(InternalCore<K, V> core, InternalCore.BulkLoader<K, V> bulkLoader) {
        super(core);
        this.bulkLoader = bulkLoader;
    }

    @Override
    public @Nullable V get(K key) {
        return core.get(key);
    }

    @Override
    public Map<K, V> getAll(Iterable<? extends K> keys) {
        return core.getAll(keys, bulkLoader);
    }

    @Override
    public CompletableFuture<@Nullable V> refresh(K key) {
        InternalKey<K> internalKey = core.wrap(key);
        return InternalCore.unwrap(loading().synchronous().refresh(internalKey));
    }

    @Override
    public CompletableFuture<Map<K, V>> refreshAll(Iterable<? extends K> keys) {
        requireNonNull(keys, "keys cannot be null");
        core.checkOpen();
        List<InternalKey<K>> internalKeys = new java.util.ArrayList<>();
        keys.forEach(key -> internalKeys.add(core.wrap(key)));
        return loading().synchronous().refreshAll(internalKeys).thenApply(values -> {
            Map<K, V> result = new LinkedHashMap<>();
            values.forEach((internalKey, value) -> result.put(internalKey.key(), value.value()));
            return result;
        });
    }

    private AsyncLoadingCache<InternalKey<K>, InternalValue<V>> loading() {
        return (AsyncLoadingCache<InternalKey<K>, InternalValue<V>>) core.l1();
    }
}
