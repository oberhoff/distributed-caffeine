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

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

// The simple serializers behind StringSerializer.ofString() and friends: stored readably, no dependency.
final class BuiltInSerializers {

    static final StringSerializer<String> STRING = new StringSerializer<>() {
        @Override
        public String serialize(String object) {
            return requireNonNull(object);
        }

        @Override
        public String deserialize(String value) {
            return requireNonNull(value);
        }
    };

    static final StringSerializer<Long> LONG = new StringSerializer<>() {
        @Override
        public String serialize(Long object) {
            return object.toString();
        }

        @Override
        public Long deserialize(String value) {
            return Long.valueOf(value);
        }
    };

    static final StringSerializer<Integer> INTEGER = new StringSerializer<>() {
        @Override
        public String serialize(Integer object) {
            return object.toString();
        }

        @Override
        public Integer deserialize(String value) {
            return Integer.valueOf(value);
        }
    };

    static final StringSerializer<java.util.UUID> UUID = new StringSerializer<>() {
        @Override
        public String serialize(java.util.UUID object) {
            return object.toString();
        }

        @Override
        public java.util.UUID deserialize(String value) {
            return java.util.UUID.fromString(value);
        }
    };

    // a copy each way, so that neither the caller's array nor the stored one can be changed through the other
    static final ByteArraySerializer<byte[]> BYTE_ARRAY = new ByteArraySerializer<>() {
        @Override
        public byte[] serialize(byte[] object) {
            return Arrays.copyOf(object, object.length);
        }

        @Override
        public byte[] deserialize(byte[] value) {
            return Arrays.copyOf(value, value.length);
        }
    };

    private BuiltInSerializers() {
    }
}
