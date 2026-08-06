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
import java.util.UUID;

import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toSet;

class InternalHasher<K> {

    private final HashProvider<K> hashProvider;

    InternalHasher(HashProvider<K> hashProvider) {
        this.hashProvider = hashProvider;
    }

    // memoizing variant: reuse the hash cached on the key (or the one propagated from a store entry), otherwise
    // compute it once and cache it on the key instance for subsequent hashings
    String getHash(InternalKey<K> key) {
        String hash = key.getHash();
        return nonNull(hash)
                ? hash
                : key.setHash(getHash(k(key))).getHash();
    }

    String getHash(K key) {
        if (nonNull(hashProvider)) {
            return hashProvider.getHash(key, Hasher::new);
        } else {
            if (key instanceof Hashable hashable) {
                return hashable.getHash(Hasher::new);
            } else if (key instanceof String s) {
                return new Hasher().putString(s).getHash();
            } else if (key instanceof Long l) {
                return new Hasher().putLong(l).getHash();
            } else if (key instanceof Integer i) {
                return new Hasher().putInt(i).getHash();
            } else if (key instanceof UUID u) {
                return new Hasher().putUUID(u).getHash();
            } else {
                throw new IllegalStateException(
                        "Keys of type %s are not hashable out of the box (only %s, %s, %s and %s are), "
                                .concat("keys have to implement the %s interface or a %s has to be specified.")
                                .formatted(
                                        key.getClass().getSimpleName(),
                                        String.class.getSimpleName(),
                                        Long.class.getSimpleName(),
                                        Integer.class.getSimpleName(),
                                        UUID.class.getSimpleName(),
                                        Hashable.class.getSimpleName(),
                                        HashProvider.class.getSimpleName()));
            }
        }
    }

    Set<String> getHashes(Set<? extends K> keys) {
        return keys.stream()
                .map(this::getHash)
                .collect(toSet());
    }
}
