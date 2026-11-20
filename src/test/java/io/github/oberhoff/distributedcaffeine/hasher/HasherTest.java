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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The format of custom hashes ({@link Hashable}, {@link HashProvider}): what {@link Hasher}'s put methods compute,
 * hash4j's stream encoding, pinned by known answers. A change, ours or a hash4j upgrade's, would change every stored
 * custom hash, and fails here rather than in a rollout.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class HasherTest {

    static Stream<Arguments> knownAnswers() {
        return Stream.of(
                answer("putString", h -> h.putString("abcdefghijklmnopqrs"), "d06583ab4499c6660f0974e919d2576b"),
                answer("putString, empty", h -> h.putString(""), "650b2237c763e5db73c3e0d76e81332a"),
                answer("putString, 1000 chars", h -> h.putString("x".repeat(1000)),
                        "93f86c195cf2bef10c426d8c76ca84e7"),
                answer("putString, beyond Latin-1", h -> h.putString("ü€𝄞"), "052107653f5c326c14b84a566dc388b0"),
                answer("putLong", h -> h.putLong(Long.MIN_VALUE), "9b718b324763de1288020fbbf0dd7c5e"),
                answer("putInt", h -> h.putInt(-7), "a643a813a29855dea71ba195f749941b"),
                answer("putUUID", h -> h.putUUID(new UUID(0x0123456789ABCDEFL, 0xFEDCBA9876543210L)),
                        "0726f9f6e5a484efd4ff85d40bfeccd4"),
                answer("putBoolean", h -> h.putBoolean(true), "ebce86ebd2f92ee105558391445a0251"),
                answer("putByteArray", h -> h.putByteArray(new byte[]{1, 2, 3}), "500e1bf76169d9dc5f00a89469579f9c"),
                answer("putBytes", h -> h.putBytes(new byte[]{1, 2, 3}), "3b73ae32769bceebd4b8c4cb88eb77ac"),
                answer("putDouble", h -> h.putDouble(3.25), "2e4a1bc917862c7a8108edb8ca1238c7"),
                answer("putChar", h -> h.putChar('x'), "adadd7dba20787a6d074cf94fa604b73"),
                answer("putUnorderedIterable, in another order",
                        h -> h.putUnorderedIterable(List.of("c", "a", "b"), (value, hasher) -> hasher.putString(value)),
                        "588f2def333d28da1e5a30f822700fdd"),
                answer("a chain", h -> h.putString("abcde").putLong(42L).putInt(7),
                        "10faeacd006b8d17e4c2ed6a08d61c5b"));
    }

    private static Arguments answer(String name, UnaryOperator<Hasher> puts, String expected) {
        return Arguments.of(name, puts, expected);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("knownAnswers")
    void a_custom_hash_keeps_its_value(String name, UnaryOperator<Hasher> puts, String expected) {
        assertThat(puts.apply(new Hasher()).getHash().toHex()).isEqualTo(expected);
    }

    @Test
    void put_hash_puts_the_hash_s_bytes() {
        for (Object value : new Object[]{"a", 42L, 7, new UUID(1, 2), Thread.State.NEW}) {
            Hash builtIn = requireNonNull(Hasher.builtInHash(value));

            assertThat(new Hasher().putHash(builtIn).getHash()).as("%s", value)
                    .isEqualTo(new Hasher().putBytes(builtIn.toBytes()).getHash());
        }
    }

    @Test
    void other_kinds_have_no_built_in_hash() {
        assertThat(Hasher.builtInHash(LocalDate.of(2026, 11, 19))).isNull();
    }

    @Test
    void a_hash_leaves_the_hasher_as_it_was() {
        // a short value, and one beyond the hasher's own buffer, which then hashes through a stream
        for (String value : new String[]{"short", "x".repeat(10_000)}) {
            Hasher hasher = new Hasher().putString(value);
            Hash first = hasher.getHash();

            assertThat(hasher.getHash()).as("again").isEqualTo(first);
            assertThat(hasher.putLong(7L).getHash()).as("with a value more")
                    .isEqualTo(new Hasher().putString(value).putLong(7L).getHash());
        }
    }

    @Test
    void nothing_put_is_refused() {
        Hasher hasher = new Hasher();

        assertThatThrownBy(hasher::getHash).isInstanceOf(IllegalStateException.class);
    }
}
