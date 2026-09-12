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
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@SuppressWarnings({"RedundantThrows", "java:S112"})
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
