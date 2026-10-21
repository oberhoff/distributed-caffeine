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

/**
 * Interface to use when implementing a serializer with string representation.
 *
 * @param <T> the type of the object to serialize
 * @author Andreas Oberhoff
 */
public interface StringSerializer<T> extends Serializer<T, String> {

    @SuppressWarnings("RedundantThrows")
    @Override
    String serialize(T object) throws Exception;

    @SuppressWarnings("RedundantThrows")
    @Override
    T deserialize(String value) throws Exception;

    /**
     * Returns a serializer storing strings as they are, readable in the store.
     *
     * @return the serializer
     */
    static StringSerializer<String> ofString() {
        return BuiltInSerializers.STRING;
    }

    /**
     * Returns a serializer storing {@code Long}s as decimal text, readable in the store.
     *
     * @return the serializer
     */
    static StringSerializer<Long> ofLong() {
        return BuiltInSerializers.LONG;
    }

    /**
     * Returns a serializer storing {@code Integer}s as decimal text, readable in the store.
     *
     * @return the serializer
     */
    static StringSerializer<Integer> ofInteger() {
        return BuiltInSerializers.INTEGER;
    }

    /**
     * Returns a serializer storing {@code UUID}s in their canonical text form, readable in the store.
     *
     * @return the serializer
     */
    static StringSerializer<java.util.UUID> ofUUID() {
        return BuiltInSerializers.UUID;
    }
}
