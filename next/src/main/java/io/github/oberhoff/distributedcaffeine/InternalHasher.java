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

import com.dynatrace.hash4j.hashing.HashStream128;
import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hashing;
import io.github.oberhoff.distributedcaffeine.hasher.HashProvider;
import io.github.oberhoff.distributedcaffeine.hasher.Hashable;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;
import org.jspecify.annotations.Nullable;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a user's key into an {@link InternalKey} with its hash, as in the current generation: a configured
 * {@link HashProvider} first, then a key implementing {@link Hashable}, then the built-in key types {@code String},
 * {@code Long}, {@code Integer} and {@code UUID}.
 * <p>
 * Built-in key types are hashed exactly as {@code new Hasher().putString(key).getHash()} (and so on) would, through
 * the same hash4j stream calls, but on a stream reused per thread and with the bits read directly, so no string is
 * created on the hit path.
 *
 * @param <K> the key type
 */
final class InternalHasher<K> {

    // one reusable stream per thread, reset before each use: hashing allocates nothing
    @SuppressWarnings("java:S5164")
    private static final ThreadLocal<HashStream128> STREAM = ThreadLocal.withInitial(() -> Hashing.xxh3_128().hashStream());

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
        HashStream128 stream = STREAM.get().reset();
        if (key instanceof String string) {
            stream.putString(string);
        } else if (key instanceof Long longValue) {
            stream.putLong(longValue);
        } else if (key instanceof Integer integerValue) {
            stream.putInt(integerValue);
        } else if (key instanceof UUID uuid) {
            stream.putUUID(uuid);
        } else {
            throw new IllegalStateException(("Keys of type %s are not hashable out of the box (only String, Long, "
                    + "Integer and UUID are), keys have to implement the Hashable interface or a HashProvider has to "
                    + "be specified.").formatted(key.getClass().getSimpleName()));
        }
        HashValue128 value = stream.get();
        // Hasher.getHash() formats toByteArray(), which holds the least significant half first, then the most
        // significant one, each little-endian; read as two big-endian halves of the hex string, they are reversed
        return InternalKey.ofBits(key, Long.reverseBytes(value.getLeastSignificantBits()),
                Long.reverseBytes(value.getMostSignificantBits()));
    }

    private static String requireHash(@Nullable String hash, Object key) {
        if (hash == null || hash.isEmpty()) {
            throw new IllegalStateException("The hash of key " + key + " cannot be null or empty");
        }
        if (InternalHeartbeat.HASH.equals(hash)) {
            throw new IllegalStateException("The hash of key " + key + " is reserved: " + hash);
        }
        return hash;
    }
}
