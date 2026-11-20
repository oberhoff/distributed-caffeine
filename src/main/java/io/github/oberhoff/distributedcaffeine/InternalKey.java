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

import io.github.oberhoff.distributedcaffeine.hasher.Hash;
import org.jspecify.annotations.Nullable;

import java.util.HexFormat;

import static java.util.Objects.requireNonNull;

/**
 * A key in L1, compared by the hash of the user's key rather than by the key itself, so that a message, which carries
 * only the hash, finds the held entry directly.
 * <p>
 * Every key's hash is a {@link Hash}, whose two longs the key holds and compares; its stored form, 32 lowercase hex
 * characters ({@link Hash#toHex()}), is made once, on demand, for the store and messages. The longs are copied rather
 * than the {@code Hash} kept: the JIT then allocates no {@code Hash} on a hit (escape analysis), and a held key is one
 * object, not two. A lookup by such a string
 * parses it back into the {@code Hash}; a lookup by another string (one of the library's own rows, {@code #...}) is
 * held and compared as the string. Which form a lookup takes follows from the string alone, so every instance decides
 * it the same way, and a 32-hex string never equals any other string, so the two forms are never compared with each
 * other.
 *
 * @param <K> the key type
 * @author Andreas Oberhoff
 */
final class InternalKey<K> {

    private final @Nullable K key;
    // the hash's bits, both 0 for a lookup by another string
    private final long mostSignificantBits;
    private final long leastSignificantBits;
    // the string of a lookup by another string than a hash's stored form, otherwise null
    private final @Nullable String text;
    // the stored form of the hash, made on demand
    private @Nullable String hexString;

    private InternalKey(@Nullable K key, long mostSignificantBits, long leastSignificantBits, @Nullable String text,
            @Nullable String hexString) {
        this.key = key;
        this.mostSignificantBits = mostSignificantBits;
        this.leastSignificantBits = leastSignificantBits;
        this.text = text;
        this.hexString = hexString;
    }

    /**
     * Wraps a key with its hash.
     *
     * @param key  the user's key
     * @param hash the hash
     * @return the internal key
     */
    static <K> InternalKey<K> of(K key, Hash hash) {
        return new InternalKey<>(requireNonNull(key), hash.mostSignificantBits(), hash.leastSignificantBits(), null,
                null);
    }

    /**
     * Builds a key for looking up by hash alone (from a message or a row); it holds no user key.
     *
     * @param hash the hash
     * @return the lookup key
     */
    static <K> InternalKey<K> lookup(String hash) {
        requireNonNull(hash, "hash cannot be null");
        if (hash.isEmpty()) {
            throw new IllegalArgumentException("hash cannot be empty");
        }
        if (isStoredForm(hash)) {
            // the inverse of Hash.toHex: each half's 8 bytes little-endian, the least significant half first
            return new InternalKey<>(null, Long.reverseBytes(HexFormat.fromHexDigitsToLong(hash, 16, 32)),
                    Long.reverseBytes(HexFormat.fromHexDigitsToLong(hash, 0, 16)), null, hash);
        }
        return new InternalKey<>(null, 0L, 0L, hash, null);
    }

    // exactly what Hash.toHex() produces: 32 lowercase hex characters
    private static boolean isStoredForm(String hash) {
        if (hash.length() != 32) {
            return false;
        }
        for (int i = 0; i < 32; i++) {
            char c = hash.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                return false;
            }
        }
        return true;
    }

    // false for a lookup key (from a message), which holds the hash only
    boolean hasKey() {
        return key != null;
    }

    /**
     * Returns the user's key.
     *
     * @return the key
     * @throws IllegalStateException for a lookup key
     */
    K key() {
        if (key == null) {
            throw new IllegalStateException("A lookup key holds no user key");
        }
        return key;
    }

    /**
     * Returns the hash string, as stored and announced.
     *
     * @return the hash
     */
    String hash() {
        if (text != null) {
            return text;
        }
        String result = hexString;
        if (result == null) {
            result = new Hash(mostSignificantBits, leastSignificantBits).toHex();
            hexString = result;
        }
        return result;
    }

    // whether the key holds a hash's bits, rather than another string
    boolean isCompact() {
        return text == null;
    }

    @Override
    public boolean equals(@Nullable Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof InternalKey<?> that)) {
            return false;
        }
        if (text == null) {
            return that.text == null && mostSignificantBits == that.mostSignificantBits
                    && leastSignificantBits == that.leastSignificantBits;
        }
        return text.equals(that.text);
    }

    @Override
    public int hashCode() {
        // XXH3 spreads every bit, so the least significant half is as good as both
        return text == null ? Long.hashCode(leastSignificantBits) : text.hashCode();
    }

    @Override
    public String toString() {
        return key == null ? "lookup(" + hash() + ")" : String.valueOf(key);
    }
}
