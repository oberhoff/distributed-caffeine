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
import org.apache.fory.logging.LogLevel;
import org.apache.fory.logging.LoggerFactory;
import org.jspecify.annotations.NullMarked;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of a serializer with byte array representation based on <i>Apache Fory</i>.
 *
 * @param <T> the type of the object to serialize
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/apache/fory">Apache Fory on GitHub</a>
 */
@NullMarked
public class ForySerializer<T> implements ByteArraySerializer<T> {

    private final ThreadSafeFory fory;

    static {
        LoggerFactory.setLogLevel(LogLevel.ERROR_LEVEL);
    }

    /**
     * Constructs a serializer with byte array representation based on <i>Apache Fory</i>.
     */
    public ForySerializer() {
        this(new Class<?>[0]);
    }

    /**
     * Constructs a serializer with byte array representation based on <i>Apache Fory</i> along with optional
     * class-based type information.
     *
     * @param registerClasses optional class of the object (with additional classes of nested objects) to serialize
     */
    public ForySerializer(Class<?>... registerClasses) {
        this(new ForyBuilder()
                        .withLanguage(Language.JAVA)
                        .requireClassRegistration(false)
                        .suppressClassRegistrationWarnings(true),
                registerClasses);
    }

    /**
     * Constructs a serializer with byte array representation based on <i>Apache Fory</i> along with a customizable
     * Fory builder and optional class-based type information.
     *
     * @param foryBuilder     customizable Fory builder used to construct Fory instance internally
     * @param registerClasses optional class of the object (with additional classes of nested objects) to serialize
     */
    public ForySerializer(ForyBuilder foryBuilder, Class<?>... registerClasses) {
        requireNonNull(foryBuilder, "foryBuilder cannot be null");
        requireNonNull(registerClasses, "registerClasses cannot be null");
        this.fory = foryBuilder
                .buildThreadSafeForyPool(0, Integer.MAX_VALUE);
        Stream.of(registerClasses)
                .forEach(fory::register);
    }

    @Override
    public byte[] serialize(Object object) {
        return fory.serialize(object);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] value) {
        return (T) fory.deserialize(value);
    }
}
