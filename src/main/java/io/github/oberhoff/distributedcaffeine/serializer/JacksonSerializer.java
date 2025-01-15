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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of a serializer with JSON representation (encoded as String or BSON) based on <i>Jackson</i>.
 *
 * @param <T> the type of the object to serialize
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/FasterXML/jackson">Jackson on GitHub</a>
 */
public class JacksonSerializer<T> implements JsonSerializer<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> typeClass;
    private final TypeReference<T> typeReference;
    private final boolean storeAsBson;

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with {@link Class}-based type
     * information.
     *
     * @param typeClass   the {@link Class} of the object to serialize
     * @param storeAsBson {@code true} for BSON encoding or {@code false} for string encoding
     */
    public JacksonSerializer(Class<? super T> typeClass, boolean storeAsBson) {
        this(new ObjectMapper(), typeClass, storeAsBson);
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with {@link TypeReference}-based
     * type information.
     *
     * @param typeReference the {@link TypeReference} of the object to serialize
     * @param storeAsBson   {@code true} for BSON encoding or {@code false} for string encoding
     */
    public JacksonSerializer(TypeReference<T> typeReference, boolean storeAsBson) {
        this(new ObjectMapper(), typeReference, storeAsBson);
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with a customizable
     * {@link ObjectMapper} and {@link Class}-based type information.
     *
     * @param objectMapper the customized {@link ObjectMapper}
     * @param typeClass    the {@link Class} of the object to serialize
     * @param storeAsBson  {@code true} for BSON encoding or {@code false} for string encoding
     */
    @SuppressWarnings("unchecked")
    public JacksonSerializer(ObjectMapper objectMapper, Class<? super T> typeClass, boolean storeAsBson) {
        this.objectMapper = requireNonNull(objectMapper);
        this.typeClass = (Class<T>) requireNonNull(typeClass);
        this.typeReference = null;
        this.storeAsBson = storeAsBson;
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with a customizable
     * {@link ObjectMapper} and {@link TypeReference}-based type information.
     *
     * @param objectMapper  the customized {@link ObjectMapper}
     * @param typeReference the {@link TypeReference} of the object to serialize
     * @param storeAsBson   {@code true} for BSON encoding or {@code false} for string encoding
     */
    public JacksonSerializer(ObjectMapper objectMapper, TypeReference<T> typeReference, boolean storeAsBson) {
        this.objectMapper = requireNonNull(objectMapper);
        this.typeClass = null;
        this.typeReference = requireNonNull(typeReference);
        this.storeAsBson = storeAsBson;
    }

    @Override
    public String serialize(T object) throws SerializerException {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            throw new SerializerException(e);
        }
    }

    @Override
    public T deserialize(String value) throws SerializerException {
        try {
            if (nonNull(typeClass)) {
                return objectMapper.readValue(value, typeClass);
            } else {
                return objectMapper.readValue(value, typeReference);
            }
        } catch (Exception e) {
            throw new SerializerException(e);
        }
    }

    @Override
    public boolean storeAsBson() {
        return storeAsBson;
    }
}
