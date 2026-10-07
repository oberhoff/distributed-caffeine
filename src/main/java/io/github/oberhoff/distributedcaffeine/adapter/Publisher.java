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

import java.util.Collection;

/**
 * Interface representing a publisher that manages distributed synchronization between cache instances.
 * <p>
 * <b>Note:</b> Published cache entries must reach all cache instances in one and the same order, not merely in
 * order per key. Invalidating all cache entries is what requires it: it is applied to whatever a receiving cache
 * instance holds at the moment it arrives, so its position relative to another cache instance's publication decides
 * whether that cache entry survives it, and two cache instances observing the two in opposite orders stay different
 * from then on.
 * <p>
 * <b>Note:</b> One order over everything published follows from there being a single point at which publications
 * are serialized, which is what the underlying store is wherever it distributes them as well. An adapter that cannot
 * offer such a point has to make up for it rather than leave it out: whenever it may have missed or reordered
 * anything, it reports so using {@link Receiver#receiveSynchronizationRestart()}, which has the receiving cache
 * instance reconcile against the underlying store instead of carrying on with content that may disagree with the
 * other ones.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@SuppressWarnings("java:S112")
public interface Publisher<K, V> extends IdentifierAware, DiscriminatorAware, SerializerAware<K, V> {

    /**
     * Publishes cache entries for distributed synchronization between cache instances.
     * <p>
     * <b>Note:</b> If persistence is supported, discriminators should be handled in accordance with the associated
     * {@link Repository}.
     *
     * @param cacheEntries the cache entries to publish
     * @throws Exception if publishing fails
     */
    void publishCacheEntries(Collection<CacheEntry<K, V>> cacheEntries) throws Exception;
}
