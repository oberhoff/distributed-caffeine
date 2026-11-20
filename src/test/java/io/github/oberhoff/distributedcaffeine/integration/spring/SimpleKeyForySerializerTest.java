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

import io.github.oberhoff.distributedcaffeine.serializer.ForySerializer;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.springframework.cache.interceptor.SimpleKey;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The key serializer the manager sets where none was set: a {@link SimpleKey} by its params, everything else as the
 * default {@link ForySerializer}.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class SimpleKeyForySerializerTest {

    private static final SimpleKeyForySerializer SERIALIZER = new SimpleKeyForySerializer();

    private record Part(String id) {
    }

    @Test
    void a_simple_key_is_read_back_equal() {
        List<SimpleKey> keys = List.of(
                SimpleKey.EMPTY,
                new SimpleKey((Object) null),
                new SimpleKey("a", null, 7L),
                new SimpleKey("a", new SimpleKey(1, "b"), UUID.fromString("00000000-0000-0001-0000-000000000002")),
                new SimpleKey(LocalDate.of(2026, 11, 20), new Part("p")));

        for (SimpleKey key : keys) {
            assertThat(SERIALIZER.deserialize(SERIALIZER.serialize(key))).as("%s", key).isEqualTo(key);
        }
    }

    @Test
    void a_simple_key_is_serialized_by_its_params_not_by_java_serialization() {
        SimpleKey key = new SimpleKey("a", 7L);

        // Fory's default writes a SimpleKey through its Java serialization path
        assertThat(SERIALIZER.serialize(key)).isNotEqualTo(new ForySerializer<>().serialize(key));
    }

    // a key generator's key of its own
    private static final class OwnKey extends SimpleKey {
        OwnKey(@Nullable Object... elements) {
            super(elements);
        }
    }

    @Test
    void a_subclass_is_serialized_by_its_params_too_and_read_back_as_a_simple_key() {
        OwnKey key = new OwnKey("a", new OwnKey(1), null);

        Object read = SERIALIZER.deserialize(SERIALIZER.serialize(key));

        assertThat(SERIALIZER.serialize(key)).isNotEqualTo(new ForySerializer<>().serialize(key));
        assertThat(read).isExactlyInstanceOf(SimpleKey.class).isEqualTo(new SimpleKey("a", new SimpleKey(1), null));
    }

    @Test
    void every_other_type_is_serialized_as_by_the_default() {
        ForySerializer<Object> fory = new ForySerializer<>();

        for (Object value : new Object[]{"a", 7L, 7, LocalDate.of(2026, 11, 20), new Part("q"), List.of("x", "y")}) {
            assertThat(SERIALIZER.serialize(value)).as("%s", value).isEqualTo(fory.serialize(value));
            assertThat(SERIALIZER.deserialize(fory.serialize(value))).as("%s", value).isEqualTo(value);
        }
    }
}
