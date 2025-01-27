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

import com.fasterxml.jackson.core.JsonProcessingException;
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
    private final boolean storeAsBinaryJson;

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with class-based type
     * information.
     *
     * @param typeClass         the class of the object to serialize
     * @param storeAsBinaryJson {@code true} for BSON encoding or {@code false} for string encoding
     */
    public JacksonSerializer(Class<T> typeClass, boolean storeAsBinaryJson) {
        this(new ObjectMapper(), typeClass, storeAsBinaryJson);
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with reference-based
     * type information.
     *
     * @param typeReference     the type reference of the object to serialize
     * @param storeAsBinaryJson {@code true} for BSON encoding or {@code false} for string encoding
     */
    public JacksonSerializer(TypeReference<T> typeReference, boolean storeAsBinaryJson) {
        this(new ObjectMapper(), typeReference, storeAsBinaryJson);
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with a customizable
     * object mapper and class-based type information.
     *
     * @param objectMapper      the customized object mapper
     * @param typeClass         the class of the object to serialize
     * @param storeAsBinaryJson {@code true} for BSON encoding or {@code false} for string encoding
     */
    public JacksonSerializer(ObjectMapper objectMapper, Class<T> typeClass, boolean storeAsBinaryJson) {
        this.objectMapper = requireNonNull(objectMapper);
        this.typeClass = requireNonNull(typeClass);
        this.typeReference = null;
        this.storeAsBinaryJson = storeAsBinaryJson;
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with a customizable
     * object mapper and reference-based type information.
     *
     * @param objectMapper      the customized object mapper
     * @param typeReference     the type reference of the object to serialize
     * @param storeAsBinaryJson {@code true} for BSON encoding or {@code false} for string encoding
     */
    public JacksonSerializer(ObjectMapper objectMapper, TypeReference<T> typeReference, boolean storeAsBinaryJson) {
        this.objectMapper = requireNonNull(objectMapper);
        this.typeClass = null;
        this.typeReference = requireNonNull(typeReference);
        this.storeAsBinaryJson = storeAsBinaryJson;
    }

    @Override
    public String serialize(T object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }

    @Override
    public T deserialize(String value) throws JsonProcessingException {
        if (nonNull(typeClass)) {
            return objectMapper.readValue(value, typeClass);
        } else {
            return objectMapper.readValue(value, typeReference);
        }
    }

    @Override
    public boolean storeAsBinaryJson() {
        return storeAsBinaryJson;
    }
}
