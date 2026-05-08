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
package io.github.oberhoff.distributedcaffeine;

import io.github.oberhoff.distributedcaffeine.hasher.HashProvider;
import io.github.oberhoff.distributedcaffeine.hasher.Hashable;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;

import java.util.Set;

import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toSet;

class InternalHasher<K> {

    private final HashProvider<K> hashProvider;

    InternalHasher(HashProvider<K> hashProvider) {
        this.hashProvider = hashProvider;
    }

    String getHash(K key) {
        if (nonNull(hashProvider)) {
            return hashProvider.getHash(Hasher::new, key);
        } else {
            if (key instanceof Hashable hashable) {
                return hashable.getHash(Hasher::new);
            } else {
                throw new IllegalStateException(
                        "No %s configured and key of type %s does not implement %s interface".formatted(
                                HashProvider.class.getSimpleName(),
                                key.getClass().getSimpleName(),
                                Hashable.class.getSimpleName()));
            }
        }
    }

    Set<String> getHashes(Set<? extends K> keys) {
        return keys.stream()
                .map(this::getHash)
                .collect(toSet());
    }
}
