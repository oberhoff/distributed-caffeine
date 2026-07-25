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

import org.jspecify.annotations.NullMarked;

/**
 * Interface representing an adapter that manages distributed synchronization between cache instances using an
 * underlying store.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@NullMarked
public interface Adapter<K, V> extends StateAware, SerializerAware<K, V>, RetrieverAware<K, V> {

    /**
     * Returns the repository used by this adapter
     *
     * @return the repository
     */
    Repository<K, V> getRepository();

    /**
     * Returns the identifier of this adapter
     *
     * @return the identifier
     */
    String getIdentifier();
}
