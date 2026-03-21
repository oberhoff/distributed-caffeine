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

import org.jspecify.annotations.NullMarked;

import java.util.function.Supplier;

/**
 * Interface representing a provider that computes a hash for a key using the supplied hasher.
 *
 * @param <K> the key type of the cache
 * @author Andreas Oberhoff
 */
@NullMarked
@FunctionalInterface
public interface HashProvider<K> {

    /**
     * Returns a hash for the specified key using the specified  hasher for computing.
     *
     * @param hasher the hasher used to compute the cache
     * @param key    the key to compute a hash for
     * @return the hash
     */
    String getHash(Supplier<Hasher> hasher, K key);
}
