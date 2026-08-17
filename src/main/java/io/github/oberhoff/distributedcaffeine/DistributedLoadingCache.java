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

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface representing a distributed loading cache instance. Cache entries are added manually or loaded automatically
 * and remain in the cache until either invalidated or evicted.
 * <p>
 * <b>Note:</b> Invalidations are distributed to other cache instances independently of what this cache instance holds.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public interface DistributedLoadingCache<K, V> extends DistributedCache<K, V>, LoadingCache<K, V> {

    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    @Nullable V get(K key);

    @Override
    Map<K, V> getAll(Iterable<? extends K> keys);

    @Override
    @SuppressWarnings({"java:S2638", "NullAway"})
    CompletableFuture<@Nullable V> refresh(K key);

    @Override
    CompletableFuture<Map<K, V>> refreshAll(Iterable<? extends K> keys);
}
