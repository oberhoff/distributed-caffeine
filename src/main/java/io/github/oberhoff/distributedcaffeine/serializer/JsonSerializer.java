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
package io.github.oberhoff.distributedcaffeine.serializer;

/**
 * A serializer whose values are JSON, held by the store as text or in its own JSON type ({@link #storeAsNativeJson()}).
 *
 * @param <T> the type of the object to serialize
 * @author Andreas Oberhoff
 */
public interface JsonSerializer<T> extends StringSerializer<T> {

    /**
     * Indicates whether values are stored in the store's own JSON type, where it has one, so that they can be queried
     * there; otherwise, and on a store without one, as text. Reading them back is the same either way:
     * <table>
     *     <caption>What each store does</caption>
     *     <tr><th>Store</th><th>{@code true}</th><th>{@code false}</th></tr>
     *     <tr><td>MongoDB</td><td>a BSON document (queryable)</td><td>a string</td></tr>
     *     <tr><td>PostgreSQL</td><td>{@code jsonb} (queryable)</td><td>{@code text}</td></tr>
     *     <tr><td>Redis (also Valkey, Dragonfly, Kvrocks)</td><td colspan="2">UTF-8 text either way (a JSON type
     *     would only serve queries on the server, which a cache does not make)</td></tr>
     *     <tr><td>MariaDB, MySQL</td><td colspan="2">UTF-8 text either way, as bytes</td></tr>
     * </table>
     *
     * @return {@code true} for the store's own JSON type, {@code false} for text
     */
    boolean storeAsNativeJson();
}
