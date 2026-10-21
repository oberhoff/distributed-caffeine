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
package io.github.oberhoff.distributedcaffeine.spi;

/**
 * What a user passes to {@code DistributedCaffeine.newBuilder}, and what provides the store of each cache instance:
 * it describes where one cache lives (client, database, table or collection, discriminator) and is used for one cache
 * only. It is typed with the cache's key and value types so that the builder infers them; the types are not used by
 * the adapter itself, which only handles bytes.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 */
@SuppressWarnings("unused") // the types are there for the builder to infer, see above
public interface Adapter<K, V> {

    /**
     * Creates the store of a cache instance. Checks the connection, the schema marker and the shape of the table,
     * creating them where allowed, and fails fast otherwise.
     *
     * @param settings what the cache configured for its store: the formats and the operation timeout
     * @return the store
     * @throws StoreException if the store cannot be used
     */
    Store createStore(StoreSettings settings);
}
