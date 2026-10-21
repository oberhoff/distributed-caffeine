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
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Format;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * A user's serializer as the core uses it: objects to bytes and back, plus the {@link Format} the adapter stores the
 * bytes in, derived once from the serializer's interface. Text travels as UTF-8.
 *
 * @param <T> the type of the objects
 */
final class InternalSerializer<T> {

    private final Serializer<T, ?> serializer;
    private final Format format;

    private InternalSerializer(Serializer<T, ?> serializer, Format format) {
        this.serializer = serializer;
        this.format = format;
    }

    static <T> InternalSerializer<T> of(Serializer<T, ?> serializer) {
        requireNonNull(serializer, "serializer cannot be null");
        if (serializer instanceof ByteArraySerializer) {
            return new InternalSerializer<>(serializer, Format.BINARY);
        }
        if (serializer instanceof JsonSerializer<?> json && json.storeAsBinaryJson()) {
            return new InternalSerializer<>(serializer, Format.JSON);
        }
        if (serializer instanceof StringSerializer) {
            return new InternalSerializer<>(serializer, Format.TEXT);
        }
        throw new IllegalArgumentException("Serializer %s must implement %s, %s or %s".formatted(
                serializer.getClass().getName(), ByteArraySerializer.class.getSimpleName(),
                StringSerializer.class.getSimpleName(), JsonSerializer.class.getSimpleName()));
    }

    Format format() {
        return format;
    }

    @SuppressWarnings("unchecked")
    byte[] serialize(T object) {
        requireNonNull(object, "object cannot be null");
        try {
            if (format == Format.BINARY) {
                return requireNonNull(((ByteArraySerializer<T>) serializer).serialize(object), "serialized to null");
            }
            String text = requireNonNull(((StringSerializer<T>) serializer).serialize(object), "serialized to null");
            return text.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new SerializationException("Serializing " + object.getClass().getName() + " failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    T deserialize(byte[] bytes) {
        requireNonNull(bytes, "bytes cannot be null");
        try {
            T object = format == Format.BINARY
                    ? ((ByteArraySerializer<T>) serializer).deserialize(bytes)
                    : ((StringSerializer<T>) serializer).deserialize(new String(bytes, StandardCharsets.UTF_8));
            return requireNonNull(object, "deserialized to null");
        } catch (Exception e) {
            throw new SerializationException("Deserializing " + bytes.length + " bytes in format " + format
                    + " failed", e);
        }
    }

    /**
     * Serializing or deserializing failed; for a stored row this makes it unreadable (treated as a miss).
     */
    static final class SerializationException extends RuntimeException {

        SerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
