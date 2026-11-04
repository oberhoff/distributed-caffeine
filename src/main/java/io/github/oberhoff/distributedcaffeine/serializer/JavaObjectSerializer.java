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

import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Serializes to bytes with <i>Java Object Serialization</i>; the objects must implement {@link java.io.Serializable}.
 *
 * @param <T> the type of the object to serialize
 * @author Andreas Oberhoff
 * @see <a href="https://docs.oracle.com/en/java/javase/17/docs/specs/serialization/index.html">
 * Java Object Serialization Specification</a>
 */
public class JavaObjectSerializer<T> implements ByteArraySerializer<T> {

    /**
     * Constructs a serializer that accepts any serializable class.
     */
    public JavaObjectSerializer() {
        this.filter = null;
    }

    /**
     * Constructs a serializer that deserializes only what the filter allows (hardening against values written to
     * the store by someone else).
     *
     * @param filter the filter applied to every deserialization
     */
    public JavaObjectSerializer(ObjectInputFilter filter) {
        this.filter = requireNonNull(filter, "filter cannot be null");
    }

    /**
     * Returns a serializer that deserializes only the given classes and the JDK's {@code java.lang}, {@code java.util}
     * and {@code java.time} classes they commonly contain, hardened against values written to the store by someone
     * else.
     *
     * @param allowedClasses the classes to accept
     * @param <T>            the type of the object to serialize
     * @return the serializer
     */
    public static <T> JavaObjectSerializer<T> allowing(Class<?>... allowedClasses) {
        requireNonNull(allowedClasses, "allowedClasses cannot be null");
        String pattern = Arrays.stream(allowedClasses).map(Class::getName).collect(Collectors.joining(";"))
                + ";java.lang.*;java.util.*;java.time.*;!*";
        return new JavaObjectSerializer<>(ObjectInputFilter.Config.createFilter(pattern));
    }

    private final @Nullable ObjectInputFilter filter;

    @Override
    public byte[] serialize(T object) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(object);
            return byteArrayOutputStream.toByteArray();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] value) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            if (filter != null) {
                objectInputStream.setObjectInputFilter(filter);
            }
            return (T) objectInputStream.readObject();
        }
    }
}
