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

import org.apache.fory.ThreadSafeFory;
import org.apache.fory.config.ForyBuilder;
import org.apache.fory.config.Language;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Serializes to bytes with <i>Apache Fory</i>; the default for keys and values, as it handles every type without
 * configuration.
 * <p>
 * <b>Attention:</b> by default, any class can be deserialized, so whoever can write to the store can make every
 * instance deserialize classes of their choice. If the store's access cannot be trusted, use
 * {@link #requiringRegistration(Class[])}.
 *
 * @param <T> the type of the object to serialize
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/apache/fory">Apache Fory on GitHub</a>
 */
public class ForySerializer<T> implements ByteArraySerializer<T> {

    private final ThreadSafeFory fory;

    /**
     * Constructs a serializer that accepts any class.
     */
    public ForySerializer() {
        this(new Class<?>[0]);
    }

    /**
     * Constructs a serializer that accepts any class and registers the given ones, which stores them more compactly.
     *
     * @param registerClasses the classes to register, in an order that stays the same across instances
     */
    public ForySerializer(Class<?>... registerClasses) {
        this(new ForyBuilder()
                        .withLanguage(Language.JAVA)
                        .requireClassRegistration(false)
                        .suppressClassRegistrationWarnings(true),
                registerClasses);
    }

    /**
     * Constructs a serializer from a Fory builder of the caller's, registering the given classes.
     *
     * @param foryBuilder     the Fory builder
     * @param registerClasses the classes to register, in an order that stays the same across instances
     */
    public ForySerializer(ForyBuilder foryBuilder, Class<?>... registerClasses) {
        requireNonNull(foryBuilder, "foryBuilder cannot be null");
        requireNonNull(registerClasses, "registerClasses cannot be null");
        this.fory = foryBuilder
                .buildThreadSafeFory();
        Stream.of(registerClasses)
                .forEach(fory::register);
    }

    /**
     * Returns a serializer that deserializes only the given classes, hardened against values written to the store by
     * someone else. Every class of the serialized objects must be listed.
     *
     * @param registerClasses the classes to accept, in an order that stays the same across instances
     * @param <T>             the type of the object to serialize
     * @return the serializer
     */
    public static <T> ForySerializer<T> requiringRegistration(Class<?>... registerClasses) {
        return new ForySerializer<>(new ForyBuilder()
                .withLanguage(Language.JAVA)
                .requireClassRegistration(true), registerClasses);
    }

    @Override
    public byte[] serialize(T object) {
        return fory.serialize(object);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] value) {
        return (T) fory.deserialize(value);
    }
}
