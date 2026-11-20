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
package io.github.oberhoff.distributedcaffeine.integration.spring;

import io.github.oberhoff.distributedcaffeine.hasher.Hash;
import io.github.oberhoff.distributedcaffeine.hasher.HashProvider;
import io.github.oberhoff.distributedcaffeine.hasher.Hashable;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.springframework.cache.interceptor.SimpleKey;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Andreas Oberhoff
 */
@NullMarked
class SimpleKeyHashProviderTest {

    private static final SimpleKeyHashProvider HASHER = new SimpleKeyHashProvider();

    private record Order(String id) implements Hashable {
        @Override
        public Hash getHash(Supplier<Hasher> hasher) {
            return hasher.get().putString("order").putString(id).getHash();
        }
    }

    // a key generator's key of its own that brings its hash
    private static final class OwnKey extends SimpleKey implements Hashable {
        OwnKey(Object... elements) {
            super(elements);
        }

        @Override
        public Hash getHash(Supplier<Hasher> hasher) {
            return hasher.get().putString("own").getHash();
        }
    }

    private enum Status {
        ACTIVE
    }

    private static String hash(Object key) {
        return HASHER.getHash(key, Hasher::new).toHex();
    }

    @Test
    void keys_the_cache_hashes_itself_keep_their_hash() {
        UUID uuid = UUID.randomUUID();
        Order order = new Order("o1");

        for (Object key : new Object[]{"a", 1L, 1, uuid, Status.ACTIVE}) {
            assertThat(hash(key)).as("%s", key).isEqualTo(requireNonNull(Hasher.builtInHash(key)).toHex());
        }
        assertThat(hash(order)).isEqualTo(order.getHash(Hasher::new).toHex());
        assertThat(hash(new OwnKey("a", 1))).as("a hashable SimpleKey keeps its own hash, whatever its params")
                .isEqualTo(new Hasher().putString("own").getHash().toHex())
                .isEqualTo(hash(new OwnKey("b")));
    }

    @Test
    void equal_simple_keys_hash_alike() {
        assertThat(hash(new SimpleKey("a", 1, null, new Order("o2"))))
                .isEqualTo(hash(new SimpleKey("a", 1, null, new Order("o2"))));
        assertThat(hash(SimpleKey.EMPTY)).isEqualTo(hash(new SimpleKey()));
    }

    @Test
    void unequal_simple_keys_hash_apart() {
        List<Object> keys = List.of(
                SimpleKey.EMPTY,
                new SimpleKey((Object) null),
                new SimpleKey(null, null),
                new SimpleKey("a", "b"),
                new SimpleKey("b", "a"),
                new SimpleKey("ab"),
                new SimpleKey("a", ""),
                new SimpleKey("", "a"),
                new SimpleKey("1", 1),
                new SimpleKey(1, 1),
                new SimpleKey(1L, 1),
                new SimpleKey(Status.ACTIVE),
                new SimpleKey("ACTIVE"),
                new SimpleKey(new SimpleKey("a", "b")),
                "a",
                1);
        Set<String> hashes = new HashSet<>();
        for (Object key : keys) {
            hashes.add(hash(key));
        }

        assertThat(hashes).as("every key its own hash").hasSize(keys.size());
    }

    @Test
    void a_part_of_another_type_is_refused_naming_it() {
        SimpleKey key = new SimpleKey("a", LocalDate.of(2026, 11, 19));

        assertThatThrownBy(() -> hash(key)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("java.time.LocalDate").hasMessageContaining("Hashable");
        LocalDate date = LocalDate.of(2026, 11, 19);
        assertThatThrownBy(() -> hash(date)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("java.time.LocalDate");
    }

    @Test
    void the_fallback_hashes_keys_and_parts_of_other_types() {
        HashProvider<Object> dates = (key, hasher) -> hasher.get().putLong(((LocalDate) key).toEpochDay())
                .getHash();
        SimpleKeyHashProvider withDates = new SimpleKeyHashProvider(dates);
        LocalDate date = LocalDate.of(2026, 11, 19);

        assertThat(withDates.getHash(date, Hasher::new)).isEqualTo(dates.getHash(date, Hasher::new));
        assertThat(withDates.getHash(new SimpleKey("a", date), Hasher::new))
                .isEqualTo(withDates.getHash(new SimpleKey("a", LocalDate.of(2026, 11, 19)), Hasher::new))
                .isNotEqualTo(withDates.getHash(new SimpleKey("a", date.plusDays(1)), Hasher::new));
        assertThat(withDates.getHash("a", Hasher::new).toHex()).as("the built-in hashing first").isEqualTo(hash("a"));
    }

    @Test
    void the_hash_of_a_simple_key_stays_the_same_across_releases() {
        // pins the format, part of the stored hashes (SimpleKeyHashProvider's comment): the domain, the number of
        // params, then per param a tag and its data (a string, null, a long, a custom hash); the values from hash4j's
        // stream fed those bytes, an oracle apart from Hasher
        Hashable custom = hasher -> hasher.get().putString("abcde").getHash();

        assertThat(hash(new SimpleKey("a", null, 7L, custom))).isEqualTo("bb0f1795187bb24d549c0dd925097ff1");
        assertThat(hash(SimpleKey.EMPTY)).isEqualTo("9b40bdcfca4036a111e8e65d7d4c1275");
    }
}
