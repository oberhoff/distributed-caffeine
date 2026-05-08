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
package io.github.oberhoff.distributedcaffeine.adapter;

import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.isNull;

/**
 * Interface representing objects that are aware of serializers.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@NullMarked
public interface SerializerAware<K, V> {

    /**
     * Sets the key serializer for this object.
     *
     * @param keySerializer the key serializer to be set
     */
    void setKeySerializer(Serializer<K, ?> keySerializer);

    /**
     * Sets the value serializer for this object.
     *
     * @param valueSerializer the value serializer to be set
     */
    void setValueSerializer(Serializer<V, ?> valueSerializer);

    /**
     * Returns the serialized value of an object using the specified serializer.
     *
     * @param object     the object to be serialized
     * @param serializer the serializer to be used
     * @param <T>        type of the serialized value
     * @return the serialized value
     * @throws Exception if serialization fails
     */
    @SuppressWarnings("unchecked")
    static <T> @Nullable Object serialize(@Nullable T object, Serializer<T, ?> serializer) throws Exception {
        Object serializedObject;
        if (isNull(object)) {
            serializedObject = null;
        } else if (serializer instanceof ByteArraySerializer) {
            ByteArraySerializer<T> byteArraySerializer = (ByteArraySerializer<T>) serializer;
            serializedObject = byteArraySerializer.serialize(object);
        } else if (serializer instanceof JsonSerializer) {
            JsonSerializer<T> jsonSerializer = (JsonSerializer<T>) serializer;
            serializedObject = jsonSerializer.serialize(object);
        } else if (serializer instanceof StringSerializer) {
            StringSerializer<T> stringSerializer = (StringSerializer<T>) serializer;
            serializedObject = stringSerializer.serialize(object);
        } else {
            throw new IllegalStateException("No %s found for serializing object of type %s"
                    .formatted(Serializer.class.getSimpleName(), object.getClass().getSimpleName()));
        }
        return serializedObject;
    }

    /**
     * Returns the deserialized object of an value using the specified serializer.
     *
     * @param value      the value to be deserialized
     * @param serializer the serializer to be used
     * @param <T>        type of the deserialized object
     * @return the deserialized object
     * @throws Exception if deserialization fails
     */
    @SuppressWarnings("unchecked")
    static <T> @Nullable T deserialize(@Nullable Object value, Serializer<T, ?> serializer) throws Exception {
        T deserializedValue;
        if (isNull(value)) {
            deserializedValue = null;
        } else if (serializer instanceof ByteArraySerializer && value instanceof byte[]) {
            ByteArraySerializer<T> byteArraySerializer = (ByteArraySerializer<T>) serializer;
            deserializedValue = byteArraySerializer.deserialize((byte[]) value);
        } else if (serializer instanceof JsonSerializer && value instanceof String) {
            JsonSerializer<T> jsonSerializer = (JsonSerializer<T>) serializer;
            deserializedValue = jsonSerializer.deserialize((String) value);
        } else if (serializer instanceof StringSerializer && value instanceof String) {
            StringSerializer<T> stringSerializer = (StringSerializer<T>) serializer;
            deserializedValue = stringSerializer.deserialize((String) value);
        } else {
            throw new IllegalStateException("No %s found for deserializing value of type %s"
                    .formatted(Serializer.class.getSimpleName(), value.getClass().getSimpleName()));
        }
        return deserializedValue;
    }
}
