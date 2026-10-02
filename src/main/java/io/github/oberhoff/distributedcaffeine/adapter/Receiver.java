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
package io.github.oberhoff.distributedcaffeine.adapter;

import java.util.List;

/**
 * Interface representing a receiver that manages distributed synchronization between cache instances.
 * <p>
 * <b>Note:</b> Inbound cache operations are supposed to be passed on as cache entries by the {@link Adapter} using
 * the receiver. Beside them, it takes what the adapter reports about its own receiving, which is a matter of the
 * adapter's state rather than of any cache entry.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public interface Receiver<K, V> {

    /**
     * Receives cache entries originating from the underlying store (in the order the underlying store produced them).
     *
     * @param cacheEntries the cache entries to be received
     */
    void receiveCacheEntries(List<CacheEntry<K, V>> cacheEntries);

    /**
     * Receives the information that inbound synchronization was interrupted and has restarted, so that cache
     * entries of the underlying store may have been missed in the meantime.
     * <p>
     * <b>Note:</b> This is supposed to be called by the {@link Adapter} whenever it resumes receiving after a
     * failure, but not when it starts receiving for the first time. Whether anything was actually missed cannot be
     * told apart from nothing having happened, so what is reported is the possibility rather than the fact.
     */
    void receiveSynchronizationRestart();
}
