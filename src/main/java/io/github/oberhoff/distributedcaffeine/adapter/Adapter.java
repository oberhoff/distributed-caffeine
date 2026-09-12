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

import java.util.Optional;

/**
 * Interface representing an adapter that manages distributed synchronization between cache instances and,
 * optionally, persistence of cache entries.
 * <p>
 * <b>Note:</b> An adapter instance belongs to exactly one cache instance and cannot be shared between them.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public interface Adapter<K, V> extends StateAware, SerializerAware<K, V>, ReceiverAware<K, V> {

    /**
     * Returns the publisher of this adapter
     *
     * @return the publisher
     */
    Publisher<K, V> getPublisher();

    /**
     * Returns the repository of this adapter, or an empty optional if persistence is not supported.
     *
     * @return the repository, or an empty optional if persistence is not supported
     */
    Optional<Repository<K, V>> getRepository();

    /**
     * Returns the identifier of this adapter
     *
     * @return the identifier
     */
    String getIdentifier();

    /**
     * Returns the discriminator of this adapter
     *
     * @return the discriminator
     */
    @SuppressWarnings("unused")
    String getDiscriminator();
}
