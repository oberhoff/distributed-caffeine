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

import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hashing;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The stored hash format of built-in kinds ({@link Hasher#builtInHash}), against an oracle of its own: every kind's
 * bytes written out by hand and hashed with XXH3-128 under a seed recomputed from the kind's name, so that a change of
 * the format, ours or a hash4j upgrade's, fails here rather than in a rollout.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class HasherBuiltInTest {

    enum Status {
        ACTIVE,
        // a constant with a body of its own: an anonymous subclass, hashed by its declaring class all the same
        CLOSED {
            @Override
            public String toString() {
                return "closed";
            }
        }
    }

    enum Role {
        ACTIVE
    }

    // the seed of a kind: the 64-bit XXH3 hash of "distributed-caffeine:" and its name, over its chars
    private static long seed(String kind) {
        return Hashing.xxh3_64().hashCharsToLong("distributed-caffeine:" + kind);
    }

    private static Hash oracle(String kind, byte[] bytes) {
        HashValue128 value = Hashing.xxh3_128(seed(kind)).hashBytesTo128Bits(bytes);
        return new Hash(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static byte[] chars(String string) {
        return string.getBytes(StandardCharsets.UTF_16LE);
    }

    private static byte[] littleEndian(long... values) {
        ByteBuffer buffer = ByteBuffer.allocate(8 * values.length).order(ByteOrder.LITTLE_ENDIAN);
        for (long value : values) {
            buffer.putLong(value);
        }
        return buffer.array();
    }

    private static Hash builtIn(Object value) {
        return requireNonNull(Hasher.builtInHash(value));
    }

    @Test
    void every_kind_hashes_its_bytes_under_its_seed() {
        UUID uuid = new UUID(0x0123456789ABCDEFL, 0xFEDCBA9876543210L);
        byte[] integer = new byte[4];
        System.arraycopy(littleEndian(-7), 0, integer, 0, 4);

        for (String string : new String[]{"", "a", "abcdefghijklmnopqrs", "ü€𝄞", "x".repeat(70_000)}) {
            assertThat(builtIn(string)).isEqualTo(oracle("String", chars(string)));
        }
        for (long value : new long[]{0, -1, 42, Long.MIN_VALUE, Long.MAX_VALUE}) {
            assertThat(builtIn(value)).isEqualTo(oracle("Long", littleEndian(value)));
        }
        assertThat(builtIn(-7)).isEqualTo(oracle("Integer", integer));
        assertThat(builtIn(uuid)).isEqualTo(oracle("UUID",
                littleEndian(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())));
        assertThat(builtIn(Status.CLOSED)).isEqualTo(oracle("Enum",
                chars(Status.class.getName() + "\u0000CLOSED")));
    }

    @Test
    void a_hash_is_stored_as_the_hex_of_its_bytes_least_significant_half_first() {
        Hash hash = new Hash(0x0011223344556677L, 0x8899AABBCCDDEEFFL);
        HashValue128 hash4j = new HashValue128(0x0011223344556677L, 0x8899AABBCCDDEEFFL);

        assertThat(hash.toHex()).isEqualTo("ffeeddccbbaa99887766554433221100").isEqualTo(hash.toString());
        assertThat(hash.toBytes()).as("as hash4j's").isEqualTo(hash4j.toByteArray());
    }

    @Test
    void the_seeds_are_the_hashes_of_the_kinds_names() {
        // the literals in Hasher's table, recomputed
        assertThat(seed("String")).isEqualTo(0xE68419576BCD7641L);
        assertThat(seed("Long")).isEqualTo(0x20C220B12413CD8CL);
        assertThat(seed("Integer")).isEqualTo(0x14C32D217F455375L);
        assertThat(seed("UUID")).isEqualTo(0x147950E84C3FFAFFL);
        assertThat(seed("Enum")).isEqualTo(0x9BA907CB5ADD779EL);
    }

    @Test
    void values_of_different_kinds_never_share_a_hash() {
        // each pair hashed alike before this format: the same bytes in the stream's encoding
        List<Object> values = List.of("", 0, "ab", 0x0000000200620061L, Status.ACTIVE, Role.ACTIVE, "ACTIVE",
                new UUID(0, 0), 0L);
        Set<Hash> hashes = new HashSet<>();
        for (Object value : values) {
            hashes.add(builtIn(value));
        }

        assertThat(hashes).hasSize(values.size());
        assertThat(builtIn("abcde").toHex()).as("a Hashable putting the same string: unseeded, another function")
                .isNotEqualTo(new Hasher().putString("abcde").getHash().toHex());
    }

    @Test
    void other_kinds_are_not_built_in() {
        assertThat(Hasher.builtInHash(LocalDate.of(2026, 11, 19))).isNull();
        assertThat(Hasher.builtInHash(new Object())).isNull();
    }
}
