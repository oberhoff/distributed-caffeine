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

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.jspecify.annotations.NonNull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

class InternalDistributedLoadingCache<K, V> extends InternalDistributedCache<K, V>
        implements DistributedLoadingCache<K, V> {

    @Override
    public V get(@NonNull K key) {
        // locking is required here because strange deadlocks can occur if the cache is populated concurrently
        synchronizationLock.lock();
        try {
            return ((LoadingCache<K, V>) cache).get(key);
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public @NonNull Map<K, V> getAll(@NonNull Iterable<? extends K> keys) {
        // locking is required here because strange deadlocks can occur if the cache is populated concurrently
        synchronizationLock.lock();
        try {
            return ((LoadingCache<K, V>) cache).getAll(keys);
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public @NonNull CompletableFuture<V> refresh(@NonNull K key) {
        // locking is required here because strange deadlocks can occur if the cache is populated concurrently
        synchronizationLock.lock();
        try {
            return ((LoadingCache<K, V>) cache).refresh(key);
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public @NonNull CompletableFuture<Map<K, V>> refreshAll(@NonNull Iterable<? extends K> keys) {
        // locking is required here because strange deadlocks can occur if the cache is populated concurrently
        synchronizationLock.lock();
        try {
            return ((LoadingCache<K, V>) cache).refreshAll(keys);
        } finally {
            synchronizationLock.unlock();
        }
    }
}
