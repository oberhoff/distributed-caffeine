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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

// The asynchronous loading facade.
final class InternalDistributedAsyncLoadingCache<K, V> extends InternalDistributedAsyncCache<K, V>
        implements DistributedAsyncLoadingCache<K, V> {

    InternalDistributedAsyncLoadingCache(InternalCore<K, V> core, InternalCore.BulkLoader<K, V> bulkLoader,
                                         Executor executor) {
        // the synchronous view loads in the caller's thread, with the synchronous forms of the loader
        super(core, executor, new InternalDistributedLoadingCache<>(core, bulkLoader));
    }

    @Override
    public CompletableFuture<@Nullable V> get(K key) {
        return core.getAsync(key);
    }

    @Override
    public CompletableFuture<Map<K, V>> getAll(Iterable<? extends K> keys) {
        return core.getAllAsync(keys);
    }

    @Override
    public DistributedLoadingCache<K, V> synchronous() {
        return (DistributedLoadingCache<K, V>) super.synchronous();
    }
}
