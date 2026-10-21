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
 * What an adapter provides to the core: the store of a cache instance. An adapter describes where one cache lives
 * (client, database, table or collection, discriminator) and is used for one cache only.
 */
public interface StoreFactory {

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
