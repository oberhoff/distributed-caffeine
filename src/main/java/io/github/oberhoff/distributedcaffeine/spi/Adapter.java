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

import org.jspecify.annotations.Nullable;

/**
 * What a user passes to {@code DistributedCaffeine.newBuilder}, and what provides the store of each cache instance:
 * it describes where one cache lives (client, database, table or collection, discriminator) and is used for one cache
 * only. It is typed with the cache's key and value types so that the builder infers them; the types are not used by
 * the adapter itself, which only handles bytes. Its {@code toString()} names where the cache lives, as the cache's
 * log messages and errors begin with it: the product, the place and the discriminator (such as
 * {@code MongoDB orders.cache/default}), never connection details, which may carry credentials.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
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

    /**
     * Returns the client this adapter reaches its store through, where caches may share it (a driver's client, a
     * {@code DataSource}). Caches whose adapters return the same one (by {@code equals}) do not each wait for a store
     * that does not answer: after a {@link #createStore(StoreSettings) creation} that waited at least a tenth of the
     * operation timeout and failed as not reached ({@link StoreException#isRetryable() retryable}), as where the
     * network drops packets, creations through the same client within one operation timeout fail at once, as not
     * reached, without asking the store. So caches built one after another during such an outage start without their
     * store within about one operation timeout in all, instead of one each. A creation that reaches the store, or that
     * the store answers with any other failure, ends it; a creation failing fast (the connection refused, the host
     * unknown) does not start it.
     *
     * @return the client, or {@code null} (the default) where the adapter shares none, and every creation asks the
     * store
     */
    default @Nullable Object client() {
        return null;
    }
}
