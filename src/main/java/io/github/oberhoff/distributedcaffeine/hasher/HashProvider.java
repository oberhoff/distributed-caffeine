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
package io.github.oberhoff.distributedcaffeine.hasher;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * Computes the hash of a key, for key types that are neither built in ({@link String}, {@link Long},
 * {@link Integer}, {@link UUID} and {@link Enum enum}, see {@link Hasher#builtInHash}) nor implement
 * {@link Hashable}. Keys that are equal must have the same hash.
 *
 * @param <K> the key type of the cache
 * @author Andreas Oberhoff
 */
@FunctionalInterface
public interface HashProvider<K> {

    /**
     * Returns the hash of the key, computed with a fresh hasher from the supplier: put the values that
     * {@code equals} compares, then return {@link Hasher#getHash()}.
     *
     * @param key    the key
     * @param hasher the supplier of a fresh hasher
     * @return the hash
     */
    Hash getHash(K key, Supplier<Hasher> hasher);
}
