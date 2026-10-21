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
package io.github.oberhoff.distributedcaffeine.serializer;

import org.junit.jupiter.api.Test;
import tools.jackson.core.type.TypeReference;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SerializersTest {

    record Order(String id, List<String> items) implements Serializable {
    }

    record Other(String name) implements Serializable {
    }

    @Test
    void the_built_in_serializers_store_readably_and_round_trip() throws Exception {
        UUID uuid = UUID.randomUUID();

        assertThat(StringSerializer.ofString().serialize("ü€")).isEqualTo("ü€");
        assertThat(StringSerializer.ofLong().serialize(-42L)).isEqualTo("-42");
        assertThat(StringSerializer.ofLong().deserialize("-42")).isEqualTo(-42L);
        assertThat(StringSerializer.ofInteger().deserialize(StringSerializer.ofInteger().serialize(7))).isEqualTo(7);
        assertThat(StringSerializer.ofUUID().serialize(uuid)).isEqualTo(uuid.toString());
        assertThat(StringSerializer.ofUUID().deserialize(uuid.toString())).isEqualTo(uuid);
    }

    @Test
    void the_byte_array_serializer_copies_both_ways() throws Exception {
        byte[] original = {1, 2, 3};

        byte[] serialized = ByteArraySerializer.ofByteArray().serialize(original);
        original[0] = 9;
        byte[] deserialized = ByteArraySerializer.ofByteArray().deserialize(serialized);
        serialized[1] = 9;

        assertThat(deserialized).containsExactly(1, 2, 3);
    }

    @Test
    void fory_handles_any_class_by_default() {
        ForySerializer<Object> serializer = new ForySerializer<>();
        Order order = new Order("o2", List.of("a", "b"));

        assertThat(serializer.deserialize(serializer.serialize(order))).isEqualTo(order);
    }

    @Test
    void fory_requiring_registration_accepts_only_registered_classes() {
        ForySerializer<Object> serializer = ForySerializer.requiringRegistration(Order.class, Other.class);
        Order order = new Order("o3", List.of("a"));

        assertThat(serializer.deserialize(serializer.serialize(order))).isEqualTo(order);
        ForySerializer<Object> strict = ForySerializer.requiringRegistration(Order.class);
        assertThatThrownBy(() -> strict.serialize(new Other("y"))).isInstanceOf(Exception.class);
    }

    @Test
    void java_serialization_with_an_allow_list_rejects_other_classes() throws Exception {
        JavaObjectSerializer<Object> open = new JavaObjectSerializer<>();
        JavaObjectSerializer<Object> allowing = JavaObjectSerializer.allowing(Order.class);
        Order order = new Order("o4", List.of("a", "b"));

        assertThat(allowing.deserialize(open.serialize(order))).isEqualTo(order);
        byte[] other = open.serialize(new Other("x"));
        assertThatThrownBy(() -> allowing.deserialize(other)).isInstanceOf(java.io.InvalidClassException.class);
        assertThat(open.deserialize(other)).isEqualTo(new Other("x"));
    }

    @Test
    void jackson_handles_generic_types_through_a_type_reference() {
        JacksonSerializer<List<Order>> serializer = new JacksonSerializer<>(new TypeReference<>() {
        }, false);
        List<Order> orders = List.of(new Order("o5", List.of("a")), new Order("o6", List.of()));

        assertThat(serializer.deserialize(serializer.serialize(orders))).isEqualTo(orders);
    }

    @Test
    void jackson_says_how_its_json_is_stored() {
        JacksonSerializer<Order> asText = new JacksonSerializer<>(Order.class, false);
        JacksonSerializer<Order> asJson = new JacksonSerializer<>(Order.class, true);
        Order order = new Order("o1", List.of("a"));

        assertThat(asText.storeAsBinaryJson()).isFalse();
        assertThat(asJson.storeAsBinaryJson()).isTrue();
        assertThat(asJson.deserialize(asJson.serialize(order))).isEqualTo(order);
        assertThat(asJson.serialize(order)).contains("\"id\":\"o1\"");
    }
}
