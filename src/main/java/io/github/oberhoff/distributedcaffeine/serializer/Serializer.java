/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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
 * This is only a marker interface. If custom serializers are required, they must implement one of the following
 * interfaces instead:
 * <ul>
 *      <li>{@link ByteArraySerializer} for serializing an object to a byte array representation</li>
 *      <li>{@link StringSerializer} for serializing an object to a string representation</li>
 *      <li>{@link JsonSerializer} for serializing an object to a JSON representation (encoded as String or BSON)</li>
 * </ul>
 *
 * @param <T> the type of the object to serialize
 * @param <U> the type of the value to deserialize
 * @author Andreas Oberhoff
 */
public interface Serializer<T, U> {

    /**
     * Serializes an object.
     *
     * @param object the object to be serialized
     * @return the serialized value
     * @throws SerializerException if serialization fails
     */
    U serialize(T object) throws SerializerException;

    /**
     * Deserializes a value.
     *
     * @param value the value to be deserialized
     * @return the deserialized object
     * @throws SerializerException if deserialization fails
     */
    @SuppressWarnings("unused")
    T deserialize(U value) throws SerializerException;
}
