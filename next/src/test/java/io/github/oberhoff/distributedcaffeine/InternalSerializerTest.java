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

import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.ForySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Format;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InternalSerializerTest {

    record Order(String id) {
    }

    @Test
    void the_format_follows_from_the_serializers_interface() {
        assertThat(InternalSerializer.of(new ForySerializer<>()).format()).isEqualTo(Format.BINARY);
        assertThat(InternalSerializer.of(ByteArraySerializer.ofByteArray()).format()).isEqualTo(Format.BINARY);
        assertThat(InternalSerializer.of(StringSerializer.ofString()).format()).isEqualTo(Format.TEXT);
        assertThat(InternalSerializer.of(new JacksonSerializer<>(Order.class, false)).format()).isEqualTo(Format.TEXT);
        assertThat(InternalSerializer.of(new JacksonSerializer<>(Order.class, true)).format()).isEqualTo(Format.JSON);
    }

    @Test
    void text_travels_as_utf8() {
        InternalSerializer<String> serializer = InternalSerializer.of(StringSerializer.ofString());

        byte[] bytes = serializer.serialize("ü€𝄞");

        assertThat(bytes).isEqualTo("ü€𝄞".getBytes(StandardCharsets.UTF_8));
        assertThat(serializer.deserialize(bytes)).isEqualTo("ü€𝄞");
    }

    @Test
    void json_round_trips() {
        InternalSerializer<Order> serializer = InternalSerializer.of(new JacksonSerializer<>(Order.class, true));

        // a plain id and one JSON has to escape
        for (Order order : List.of(new Order("o1"), new Order("ö \"1\" \\"))) {
            assertThat(serializer.deserialize(serializer.serialize(order))).isEqualTo(order);
        }
    }

    @Test
    void a_serializer_of_no_known_kind_is_rejected() {
        Serializer<String, Object> unknown = new Serializer<>() {
            @Override
            public Object serialize(String object) {
                return object;
            }

            @Override
            public String deserialize(Object value) {
                return (String) value;
            }
        };

        assertThatThrownBy(() -> InternalSerializer.of(unknown)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ByteArraySerializer");
    }

    @Test
    void failures_are_wrapped_and_unreadable_bytes_reported() {
        InternalSerializer<Long> serializer = InternalSerializer.of(StringSerializer.ofLong());

        assertThatThrownBy(() -> serializer.deserialize("not a number".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(InternalSerializer.SerializationException.class)
                .hasCauseInstanceOf(NumberFormatException.class);
    }

    @Test
    void null_results_are_failures() {
        InternalSerializer<String> serializer = InternalSerializer.of(new StringSerializer<>() {
            @Override
            public @Nullable String serialize(String object) {
                return null;
            }

            @Override
            public @Nullable String deserialize(String value) {
                return null;
            }
        });

        assertThatThrownBy(() -> serializer.serialize("x")).isInstanceOf(InternalSerializer.SerializationException.class);
        assertThatThrownBy(() -> serializer.deserialize(new byte[0])).isInstanceOf(InternalSerializer.SerializationException.class);
    }

    @Test
    void the_defaults_are_fory_and_choices_are_made_once() {
        SerializersConfigurer<String, String> defaults = new SerializersConfigurer<>();
        SerializersConfigurer<String, String> chosen = new SerializersConfigurer<String, String>()
                .withKeySerializer(StringSerializer.ofString());

        assertThat(defaults.keySerializer().format()).isEqualTo(Format.BINARY);
        assertThat(defaults.valueSerializer().format()).isEqualTo(Format.BINARY);
        assertThat(chosen.keySerializer().format()).isEqualTo(Format.TEXT);
        StringSerializer<String> again = StringSerializer.ofString();
        assertThatThrownBy(() -> chosen.withKeySerializer(again)).isInstanceOf(IllegalStateException.class);
    }
}
