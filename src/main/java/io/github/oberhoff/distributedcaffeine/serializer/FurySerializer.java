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

import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.Language;
import org.apache.fury.logging.LogLevel;
import org.apache.fury.logging.LoggerFactory;

import java.util.stream.Stream;

/**
 * Implementation of a serializer with byte array representation based on <i>Apache Fury</i>.
 *
 * @param <T> the type of the object to serialize
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/apache/fury">Apache Fury on GitHub</a>
 */
public class FurySerializer<T> implements ByteArraySerializer<T> {

    final private ThreadSafeFury fury;

    static {
        LoggerFactory.setLogLevel(LogLevel.ERROR_LEVEL);
    }

    /**
     * Constructs a serializer with byte array representation based on <i>Apache Fury</i> along with optional
     * {@link Class}-based type information.
     *
     * @param registerClasses optional {@link Class} of the object (with additional classes of nested objects) to
     *                        serialize
     */
    public FurySerializer(Class<?>... registerClasses) {
        this.fury = Fury.builder()
                .withLanguage(Language.JAVA)
                .requireClassRegistration(false)
                .suppressClassRegistrationWarnings(true)
                .buildThreadSafeFury();
        Stream.of(registerClasses)
                .forEach(fury::register);
    }

    @Override
    public byte[] serialize(Object object) throws SerializerException {
        try {
            return fury.serialize(object);
        } catch (Exception e) {
            throw new SerializerException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] value) throws SerializerException {
        try {
            return (T) fury.deserialize(value);
        } catch (Exception e) {
            throw new SerializerException(e);
        }
    }
}
