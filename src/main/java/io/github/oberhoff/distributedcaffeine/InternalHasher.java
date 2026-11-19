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

import com.dynatrace.hash4j.hashing.HashValue128;
import io.github.oberhoff.distributedcaffeine.hasher.HashProvider;
import io.github.oberhoff.distributedcaffeine.hasher.Hashable;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a user's key into an {@link InternalKey} with its hash, as in 0.7.x: a configured
 * {@link HashProvider} first, then a key implementing {@link Hashable}, then the built-in key types of
 * {@link Hasher#putBuiltIn}.
 * <p>
 * Built-in key types are hashed by {@link Hasher#hashBuiltIn}, exactly as {@code Hasher.putBuiltIn(key, new Hasher())}
 * followed by {@code getHash()} would, but with the bits read directly, so no string is created on the hit path.
 *
 * @param <K> the key type
 * @author Andreas Oberhoff
 */
final class InternalHasher<K> {

    // the prefix of the library's own rows (the heartbeat's key, the stores' control rows); the built-in hashes are hex
    static final String RESERVED = "#";

    private final @Nullable HashProvider<K> hashProvider;

    InternalHasher(@Nullable HashProvider<K> hashProvider) {
        this.hashProvider = hashProvider;
    }

    InternalKey<K> wrap(K key) {
        requireNonNull(key, "key cannot be null");
        if (hashProvider != null) {
            return InternalKey.ofHash(key, requireHash(hashProvider.getHash(key, Hasher::new), key));
        }
        if (key instanceof Hashable hashable) {
            return InternalKey.ofHash(key, requireHash(hashable.getHash(Hasher::new), key));
        }
        HashValue128 value = Hasher.hashBuiltIn(key);
        if (value == null) {
            throw new IllegalStateException(("Keys of type %s cannot be hashed out of the box (only String, Long, "
                    + "Integer, UUID and enums can): implement Hashable in the key class, or set withHashProvider(...) "
                    + "on the builder").formatted(key.getClass().getSimpleName()));
        }
        // Hasher.getHash() formats toByteArray(), which holds the least significant half first, then the most
        // significant one, each little-endian; read as two big-endian halves of the hex string, they are reversed
        return InternalKey.ofBits(key, Long.reverseBytes(value.getLeastSignificantBits()),
                Long.reverseBytes(value.getMostSignificantBits()));
    }

    private static String requireHash(@Nullable String hash, Object key) {
        if (hash == null || hash.isEmpty()) {
            throw new IllegalStateException("The hash of key " + key + " cannot be null or empty");
        }
        if (hash.startsWith(RESERVED)) {
            throw new IllegalStateException("The hash of key " + key + " is reserved: " + hash);
        }
        return hash;
    }
}
