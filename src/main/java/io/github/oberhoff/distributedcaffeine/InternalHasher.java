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

import com.dynatrace.hash4j.hashing.Hashing;
import io.github.oberhoff.distributedcaffeine.hasher.Hash;
import io.github.oberhoff.distributedcaffeine.hasher.HashProvider;
import io.github.oberhoff.distributedcaffeine.hasher.Hashable;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import org.jspecify.annotations.Nullable;

import java.security.CodeSource;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a user's key into an {@link InternalKey} with its hash: from a configured {@link HashProvider} first, then
 * from a key implementing {@link Hashable}, then by the built-in kinds of {@link Hasher#builtInHash}. Each gives a
 * {@link Hash}, whose bits are read directly, so no string is created on the hit path. Building one checks once per
 * JVM that the hash library resolved at runtime hashes as the stored format says ({@link #verify}).
 *
 * @param <K> the key type
 * @author Andreas Oberhoff
 */
final class InternalHasher<K> {

    // set once the known answers came out right in this JVM
    private static volatile boolean verified;

    private final @Nullable HashProvider<K> hashProvider;

    InternalHasher(@Nullable HashProvider<K> hashProvider) {
        verify();
        this.hashProvider = hashProvider;
    }

    InternalKey<K> wrap(K key) {
        requireNonNull(key, "key cannot be null");
        @Nullable Hash value;
        if (hashProvider != null) {
            value = requireHash(hashProvider.getHash(key, Hasher::new), "The hash provider", key);
        } else if (key instanceof Hashable hashable) {
            value = requireHash(hashable.getHash(Hasher::new), "The key's Hashable", key);
        } else {
            value = Hasher.builtInHash(key);
        }
        if (value == null) {
            throw new IllegalStateException(("Keys of type %s cannot be hashed out of the box (only String, Long, "
                    + "Integer, UUID and enums can): implement Hashable in the key class, or set withHashProvider(...) "
                    + "on the builder").formatted(key.getClass().getName()));
        }
        return InternalKey.of(key, value);
    }

    private static Hash requireHash(@Nullable Hash hash, String source, Object key) {
        if (hash == null) {
            throw new IllegalStateException(source + " returned no hash for key " + key);
        }
        return hash;
    }

    /**
     * Checks once per JVM that the hash library resolved at runtime gives the known answers of this format. An
     * application's build may resolve another version of hash4j than this release's (Maven takes the nearest
     * declaration, Gradle the highest); one hashing otherwise would identify every key differently from instances
     * hashing as specified, so that each would serve values the other changed. Every hash goes through hash4j's XXH3
     * by {@link Hasher}'s public methods, so the known answers are computed by them.
     *
     * @throws IllegalStateException if an answer differs
     */
    static void verify() {
        if (verified) {
            return;
        }
        for (KnownAnswer answer : KnownAnswer.ALL) {
            String computed = answer.compute();
            if (!answer.expected.equals(computed)) {
                throw new IllegalStateException(("The hash library resolved at runtime (hash4j from %s) computes "
                        + "other hashes than the stored hash format %s (%s gave %s, expected %s): use "
                        + "com.dynatrace.hash4j:hash4j in the version this release of distributed-caffeine depends on")
                        .formatted(hash4jLocation(), Store.HASH_FORMAT, answer.input, computed, answer.expected));
            }
        }
        verified = true;
    }

    // hash4j's jar records no version (no manifest entry, no pom.properties), but its location usually names it
    static String hash4jLocation() {
        try {
            CodeSource source = Hashing.class.getProtectionDomain().getCodeSource();
            return source == null ? "an unknown location" : String.valueOf(source.getLocation());
        } catch (SecurityException e) {
            return "an unknown location";
        }
    }

    // the known answers: every built-in kind, and strings across XXH3's length classes (0, 1–3, 4–8, 9–16, 17–128,
    // 129–240 and longer bytes; a char is two), and Hasher's common put methods, each alone and in a chain (the format
    // of Hashables and hash providers): all of them hashed by the XXH3 of the hash4j version resolved
    enum KnownAnswer {
        EMPTY("\"\"", "c37e2c9a3b59df2bce3d5ec5aabb4f09"),
        ONE_CHAR("\"a\"", "b1124599dc2197feaa39dcdb6d51294a"),
        FOUR_CHARS("\"abcd\"", "d0bcadd0e21145116c4279353a6e052f"),
        EIGHT_CHARS("\"abcdefgh\"", "1932e8d48c634adc63e97dd57547c0fe"),
        SIXTY_CHARS("60 chars", "3875c9fdfee7c238be48b0eaf8b6edf5"),
        HUNDRED_CHARS("100 chars", "dadc8c980803e5bc168a7e50c7c3786d"),
        THOUSAND_CHARS("1000 chars", "a6a3b5969fad136958b5338afa5e80d2"),
        LONG("42L", "0387d14ddb5d64810c117d02a808ffe6"),
        INTEGER("42", "de5c0bb42cec701d5a117066e4816da5"),
        UUID("a UUID", "9b7bf5b03e3c3046c568df0bf3105b4f"),
        ENUM("an enum constant", "1cb7d14fc30c37d7c19d5a04df02b2e9"),
        HASHER("a Hasher's chain", "3c2c3225e53e2ecbf08387a8b94f159a"),
        PUT_STRING("putString", "d06583ab4499c6660f0974e919d2576b"),
        PUT_STRING_EMPTY("putString, empty", "650b2237c763e5db73c3e0d76e81332a"),
        PUT_STRING_LONG("putString, 1000 chars", "93f86c195cf2bef10c426d8c76ca84e7"),
        PUT_STRING_UNICODE("putString, beyond Latin-1", "052107653f5c326c14b84a566dc388b0"),
        PUT_LONG("putLong", "9b718b324763de1288020fbbf0dd7c5e"),
        PUT_INT("putInt", "a643a813a29855dea71ba195f749941b"),
        PUT_UUID("putUUID", "0726f9f6e5a484efd4ff85d40bfeccd4"),
        PUT_BOOLEAN("putBoolean", "ebce86ebd2f92ee105558391445a0251"),
        PUT_BYTE_ARRAY("putByteArray", "500e1bf76169d9dc5f00a89469579f9c"),
        PUT_BYTES("putBytes", "3b73ae32769bceebd4b8c4cb88eb77ac"),
        PUT_DOUBLE("putDouble", "2e4a1bc917862c7a8108edb8ca1238c7"),
        PUT_CHAR("putChar", "adadd7dba20787a6d074cf94fa604b73"),
        PUT_UNORDERED("putUnorderedIterable", "588f2def333d28da1e5a30f822700fdd");

        static final List<KnownAnswer> ALL = List.of(values());
        private static final String DIGITS = "0123456789";

        final String input;
        final String expected;

        KnownAnswer(String input, String expected) {
            this.input = input;
            this.expected = expected;
        }

        String compute() {
            return switch (this) {
                case EMPTY -> requireNonNull(Hasher.builtInHash("")).toHex();
                case ONE_CHAR -> requireNonNull(Hasher.builtInHash("a")).toHex();
                case FOUR_CHARS -> requireNonNull(Hasher.builtInHash("abcd")).toHex();
                case EIGHT_CHARS -> requireNonNull(Hasher.builtInHash("abcdefgh")).toHex();
                case SIXTY_CHARS -> requireNonNull(Hasher.builtInHash(DIGITS.repeat(6))).toHex();
                case HUNDRED_CHARS -> requireNonNull(Hasher.builtInHash(DIGITS.repeat(10))).toHex();
                case THOUSAND_CHARS -> requireNonNull(Hasher.builtInHash(DIGITS.repeat(100))).toHex();
                case LONG -> requireNonNull(Hasher.builtInHash(42L)).toHex();
                case INTEGER -> requireNonNull(Hasher.builtInHash(42)).toHex();
                case UUID -> requireNonNull(Hasher.builtInHash(new UUID(0x0123456789ABCDEFL, 0xFEDCBA9876543210L)))
                        .toHex();
                case ENUM -> requireNonNull(Hasher.builtInHash(Thread.State.NEW)).toHex();
                case HASHER -> new Hasher().putString("abcde").putLong(42L).putInt(7).putBoolean(true)
                        .putUUID(new UUID(1, 2)).getHash().toHex();
                case PUT_STRING -> new Hasher().putString("abcdefghijklmnopqrs").getHash().toHex();
                case PUT_STRING_EMPTY -> new Hasher().putString("").getHash().toHex();
                case PUT_STRING_LONG -> new Hasher().putString("x".repeat(1000)).getHash().toHex();
                case PUT_STRING_UNICODE -> new Hasher().putString("ü€𝄞").getHash().toHex();
                case PUT_LONG -> new Hasher().putLong(Long.MIN_VALUE).getHash().toHex();
                case PUT_INT -> new Hasher().putInt(-7).getHash().toHex();
                case PUT_UUID -> new Hasher().putUUID(new UUID(0x0123456789ABCDEFL, 0xFEDCBA9876543210L)).getHash()
                        .toHex();
                case PUT_BOOLEAN -> new Hasher().putBoolean(true).getHash().toHex();
                case PUT_BYTE_ARRAY -> new Hasher().putByteArray(new byte[]{1, 2, 3}).getHash().toHex();
                case PUT_BYTES -> new Hasher().putBytes(new byte[]{1, 2, 3}).getHash().toHex();
                case PUT_DOUBLE -> new Hasher().putDouble(3.25).getHash().toHex();
                case PUT_CHAR -> new Hasher().putChar('x').getHash().toHex();
                case PUT_UNORDERED -> new Hasher()
                        .putUnorderedIterable(List.of("b", "a", "c"), (value, hasher) -> hasher.putString(value))
                        .getHash().toHex();
            };
        }
    }
}
