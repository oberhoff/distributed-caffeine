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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InternalKeyTest {

    private final InternalHasher<Object> hasher = new InternalHasher<>(null);

    static List<Object> builtInKeys() {
        return List.of("", "user:000123:profile", "ü€𝄞", 0L, Long.MIN_VALUE, 42L, 0, -7, Integer.MAX_VALUE,
                new UUID(0, 0), UUID.fromString("123e4567-e89b-12d3-a456-426614174000"));
    }

    private static String hashOf(Object key) {
        Hasher hasher = new Hasher();
        if (key instanceof String string) {
            hasher.putString(string);
        } else if (key instanceof Long value) {
            hasher.putLong(value);
        } else if (key instanceof Integer value) {
            hasher.putInt(value);
        } else {
            hasher.putUUID((UUID) key);
        }
        return hasher.getHash();
    }

    @ParameterizedTest
    @MethodSource("builtInKeys")
    void built_in_keys_get_the_hash_the_hasher_gives_and_equal_the_key_parsed_from_it(Object key) {
        InternalKey<Object> wrapped = hasher.wrap(key);
        String expected = hashOf(key);

        assertThat(wrapped.isCompact()).isTrue();
        assertThat(wrapped.hash()).isEqualTo(expected);
        assertThat(wrapped).isEqualTo(InternalKey.lookup(expected)).hasSameHashCodeAs(InternalKey.lookup(expected));
        assertThat(wrapped.key()).isEqualTo(key);
    }

    @Test
    void equal_keys_are_equal_and_different_keys_are_not() {
        assertThat(hasher.wrap("a")).isEqualTo(hasher.wrap("a")).isNotEqualTo(hasher.wrap("b"));
        assertThat(hasher.wrap(1L)).isNotEqualTo(hasher.wrap(1));
    }

    @Test
    void a_custom_hex_hash_is_compact_and_equals_its_lookup_key() {
        String hash = new Hasher().putString("custom").getHash();
        InternalKey<String> key = InternalKey.ofHash("k", hash);

        assertThat(key.isCompact()).isTrue();
        assertThat(key).isEqualTo(InternalKey.lookup(hash));
        assertThat(key.hash()).isSameAs(hash);
    }

    @Test
    void any_other_custom_hash_is_kept_and_compared_as_the_string() {
        InternalKey<String> key = InternalKey.ofHash("k", "order-42");

        assertThat(key.isCompact()).isFalse();
        assertThat(key.hash()).isEqualTo("order-42");
        assertThat(key).isEqualTo(InternalKey.lookup("order-42")).hasSameHashCodeAs(InternalKey.lookup("order-42"))
                .isNotEqualTo(InternalKey.lookup("order-43"));
    }

    @Test
    void uppercase_hex_is_another_string_and_never_equals_the_lowercase_hash() {
        String lower = new Hasher().putString("x").getHash();
        String upper = lower.toUpperCase(Locale.ROOT);

        assertThat(InternalKey.lookup(upper).isCompact()).isFalse();
        assertThat(InternalKey.lookup(upper)).isNotEqualTo(InternalKey.lookup(lower));
    }

    @Test
    void a_lookup_key_holds_no_user_key() {
        InternalKey<Object> lookup = InternalKey.lookup("order-42");
        assertThatThrownBy(lookup::key).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void empty_hashes_are_rejected() {
        assertThatThrownBy(() -> InternalKey.lookup("")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void a_hash_provider_comes_before_hashable_and_built_in_types() {
        HashProvider<Object> provider = (key, ignored) -> "provided";
        InternalHasher<Object> withProvider = new InternalHasher<>(provider);

        assertThat(withProvider.wrap("s").hash()).isEqualTo("provided");
        assertThat(withProvider.wrap(new Custom("p")).hash()).isEqualTo("provided");
    }

    @Test
    void a_hashable_key_provides_its_own_hash() {
        Custom key = new Custom("c");

        InternalKey<Object> wrapped = hasher.wrap(key);

        assertThat(wrapped.hash()).isEqualTo(new Hasher().putString("c").getHash());
        assertThat(wrapped.isCompact()).isTrue();
    }

    @Test
    void other_key_types_are_rejected_with_a_clear_message() {
        Object unsupported = new Object();
        assertThatThrownBy(() -> hasher.wrap(unsupported))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Object").hasMessageContaining("Hashable").hasMessageContaining("HashProvider");
    }

    @Test
    @SuppressWarnings("DataFlowIssue") // the null hash is the point: it is rejected
    void null_or_empty_custom_hashes_are_rejected() {
        InternalHasher<Object> empty = new InternalHasher<>((key, ignored) -> "");
        InternalHasher<Object> none = new InternalHasher<>((key, ignored) -> null);

        assertThatThrownBy(() -> empty.wrap("k")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> none.wrap("k")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @SuppressWarnings("resource") // shut down below: an ExecutorService is AutoCloseable only from Java 19 on
    void hashing_on_many_threads_gives_the_same_hashes() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<String>> futures = IntStream.range(0, 2_000)
                    .mapToObj(i -> executor.submit(() -> hasher.wrap("key-" + (i % 100)).hash()))
                    .toList();
            for (int i = 0; i < futures.size(); i++) {
                assertThat(futures.get(i).get()).isEqualTo(hashOf("key-" + (i % 100)));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    record Custom(String id) implements Hashable {
        @Override
        public String getHash(Supplier<Hasher> hasher) {
            return hasher.get().putString(id).getHash();
        }
    }
}
