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

import org.jspecify.annotations.Nullable;

import java.util.HexFormat;

import static java.util.Objects.requireNonNull;

/**
 * A key in L1, compared by the hash of the user's key rather than by the key itself, so that a message, which carries
 * only the hash, finds the held entry directly.
 * <p>
 * Identity is conceptually the hash string, as in the current generation. A hash of 32 lowercase hex characters (what
 * {@code Hasher.getHash()} produces) is held compactly as 128 bits; any other string a custom {@code Hashable} or
 * {@code HashProvider} returns is held and compared as the string. Which form a key takes follows from the hash
 * string alone, so every instance decides it the same way, and a 32-hex string never equals any other string, so
 * the two forms are never compared with each other.
 * <p>
 * The 128 bits are the two halves of the hex string, each read as a big-endian number, which keeps a compact key
 * built from bits ({@link #ofBits}) equal to one parsed from the same hash's hex string ({@link #ofHash}).
 *
 * @param <K> the key type
 */
final class InternalKey<K> {

    private static final HexFormat HEX = HexFormat.of();

    private final @Nullable K key;
    private final long first;
    private final long second;
    // the hash if it is not 32 lowercase hex characters, otherwise null
    private final @Nullable String text;
    // the hex string of a compact hash, produced on demand
    private @Nullable String hexString;

    private InternalKey(@Nullable K key, long first, long second, @Nullable String text, @Nullable String hexString) {
        this.key = key;
        this.first = first;
        this.second = second;
        this.text = text;
        this.hexString = hexString;
    }

    /**
     * Wraps a key with its hash in compact form.
     *
     * @param key    the user's key
     * @param first  the first half of the hash, as the first 16 hex characters read big-endian
     * @param second the second half
     * @return the internal key
     */
    static <K> InternalKey<K> ofBits(K key, long first, long second) {
        return new InternalKey<>(requireNonNull(key), first, second, null, null);
    }

    /**
     * Wraps a key with the hash a custom {@code Hashable} or {@code HashProvider} returned.
     *
     * @param key  the user's key
     * @param hash the hash
     * @return the internal key
     */
    static <K> InternalKey<K> ofHash(K key, String hash) {
        return fromHash(requireNonNull(key), hash);
    }

    /**
     * Builds a key for looking up by hash alone (from a message or a row); it holds no user key.
     *
     * @param hash the hash
     * @return the lookup key
     */
    static <K> InternalKey<K> lookup(String hash) {
        return fromHash(null, hash);
    }

    private static <K> InternalKey<K> fromHash(@Nullable K key, String hash) {
        requireNonNull(hash, "hash cannot be null");
        if (hash.isEmpty()) {
            throw new IllegalArgumentException("hash cannot be empty");
        }
        if (isCompact(hash)) {
            return new InternalKey<>(key, HexFormat.fromHexDigitsToLong(hash, 0, 16),
                    HexFormat.fromHexDigitsToLong(hash, 16, 32), null, hash);
        }
        return new InternalKey<>(key, 0L, 0L, hash, null);
    }

    // exactly what Hasher.getHash() produces: 32 lowercase hex characters
    private static boolean isCompact(String hash) {
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

    /**
     * Returns the user's key.
     *
     * @return the key
     * @throws IllegalStateException for a lookup key
     */
    // false for a lookup key (from a message), which holds the hash only
    boolean hasKey() {
        return key != null;
    }

    K key() {
        if (key == null) {
            throw new IllegalStateException("a lookup key holds no user key");
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
            result = HEX.toHexDigits(first) + HEX.toHexDigits(second);
            hexString = result;
        }
        return result;
    }

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
            return that.text == null && first == that.first && second == that.second;
        }
        return text.equals(that.text);
    }

    @Override
    public int hashCode() {
        return text == null ? Long.hashCode(second) : text.hashCode();
    }

    @Override
    public String toString() {
        return key == null ? "lookup(" + hash() + ")" : String.valueOf(key);
    }
}
