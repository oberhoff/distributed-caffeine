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
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDate;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * @author Andreas Oberhoff
 */
@NullMarked
class HasherTest {

    enum Status {
        ACTIVE
    }

    private static final List<Object> BUILT_IN = List.of("", "user:000123:profile", "ü€𝄞", 42L, Long.MIN_VALUE, -7,
            new UUID(1, 2), Status.ACTIVE);

    private static String hex(HashValue128 value) {
        return HexFormat.of().formatHex(value.toByteArray());
    }

    private static String viaHasher(Object key) {
        Hasher hasher = new Hasher();
        assertThat(Hasher.putBuiltIn(key, hasher)).isTrue();
        return hasher.getHash();
    }

    // known answers: the stored hashes, literal, so that a change of the algorithm or of hash4j's encoding (a hash4j
    // upgrade among them) fails here rather than in a rollout, where old and new instances would no longer share keys
    static Stream<Arguments> knownAnswers() {
        return Stream.of(
                Arguments.of("", "650b2237c763e5db73c3e0d76e81332a"),
                Arguments.of("a", "e2f5d71d292c65b50b0f6908876ef87a"),
                Arguments.of("user:000123:profile", "9142f90dc13669fe2e41af2187cca801"),
                Arguments.of("ü€𝄞", "052107653f5c326c14b84a566dc388b0"),
                Arguments.of("x".repeat(1000), "93f86c195cf2bef10c426d8c76ca84e7"),
                Arguments.of(0L, "b26556039db4c345547d14dc998a0a2c"),
                Arguments.of(-1L, "b0f02e4cd349328ba55a4207d2206bdc"),
                Arguments.of(42L, "455a898b4e16c26186fee1552f3eab9b"),
                Arguments.of(Long.MIN_VALUE, "9b718b324763de1288020fbbf0dd7c5e"),
                Arguments.of(0, "650b2237c763e5db73c3e0d76e81332a"),
                Arguments.of(-7, "a643a813a29855dea71ba195f749941b"),
                Arguments.of(Integer.MAX_VALUE, "0c7cecb51b57b33c96052a07876508fd"),
                Arguments.of(new UUID(0, 0), "2d8be206ef23ea0562f8e399959a18e5"),
                Arguments.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                        "efb53fe68f302c45f8504aaf47c7e2c9"),
                Arguments.of(Status.ACTIVE, "2d8ff794cf12db5827efa5cf11b1cd2f"));
    }

    @ParameterizedTest
    @MethodSource("knownAnswers")
    void a_built_in_key_keeps_its_stored_hash(Object key, String expected) {
        HashValue128 value = Hasher.hashBuiltIn(key);

        assertThat(viaHasher(key)).isEqualTo(expected);
        assertThat(value).isNotNull();
        assertThat(hex(value)).isEqualTo(expected);
    }

    @Test
    void a_hasher_built_hash_keeps_its_value() {
        assertThat(new Hasher().putString("order").putLong(42L).putInt(7).getHash())
                .isEqualTo("697d63d1f3aeb3382e23beb84d653eb2");
    }

    @Test
    void the_cache_s_hash_of_a_built_in_key_is_the_hasher_s() {
        for (Object key : BUILT_IN) {
            HashValue128 value = Hasher.hashBuiltIn(key);

            assertThat(value).as("%s", key).isNotNull();
            assertThat(hex(value)).as("%s", key).isEqualTo(viaHasher(key));
        }
    }

    @Test
    void keys_of_other_types_are_not_built_in() {
        Hasher hasher = new Hasher();

        assertThat(Hasher.hashBuiltIn(LocalDate.of(2026, 11, 19))).isNull();
        assertThat(Hasher.putBuiltIn(LocalDate.of(2026, 11, 19), hasher)).isFalse();
    }

    @Test
    void a_virtual_thread_gets_the_same_hash() throws Exception {
        ExecutorService virtualThreads;
        try {
            // Java 21 and later; the tests compile for 17
            virtualThreads = (ExecutorService) Executors.class
                    .getMethod("newVirtualThreadPerTaskExecutor").invoke(null);
        } catch (NoSuchMethodException e) {
            assumeTrue(false, "no virtual threads before Java 21");
            return;
        }
        try {
            for (Object key : BUILT_IN) {
                HashValue128 value = virtualThreads.submit(() -> Hasher.hashBuiltIn(key)).get();

                assertThat(value).isNotNull();
                assertThat(hex(value)).as("%s", key).isEqualTo(viaHasher(key));
            }
        } finally {
            virtualThreads.shutdown();
        }
    }
}
