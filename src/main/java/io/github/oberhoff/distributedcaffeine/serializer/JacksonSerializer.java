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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of a serializer with JSON representation (encoded as String or BSON) based on <i>Jackson</i>.
 *
 * @param <T> the type of the object to serialize
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/FasterXML/jackson">Jackson on GitHub</a>
 */
@NullMarked
public class JacksonSerializer<T> implements JsonSerializer<T> {

    private final JsonMapper jsonMapper;
    private final @Nullable Class<T> typeClass;
    private final @Nullable TypeReference<T> typeReference;
    private final boolean storeAsBinaryJson;

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with class-based type
     * information. If a customized JSON mapper is required, {@link #JacksonSerializer(JsonMapper, Class, boolean)}
     * can be used instead.
     *
     * @param typeClass         the class of the object to serialize
     * @param storeAsBinaryJson {@code true} for BSON encoding or {@code false} for string encoding
     */
    public JacksonSerializer(Class<T> typeClass, boolean storeAsBinaryJson) {
        this(JsonMapper.builder().build(), typeClass, storeAsBinaryJson);
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with reference-based type
     * information. If a customized JSON mapper is required,
     * {@link #JacksonSerializer(JsonMapper, TypeReference, boolean)} can be used instead.
     *
     * @param typeReference     the type reference of the object to serialize
     * @param storeAsBinaryJson {@code true} for BSON encoding or {@code false} for string encoding
     */
    public JacksonSerializer(TypeReference<T> typeReference, boolean storeAsBinaryJson) {
        this(JsonMapper.builder().build(), typeReference, storeAsBinaryJson);
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with a customizable JSON mapper
     * and class-based type information. If a default JSON mapper is sufficient,
     * {@link #JacksonSerializer(Class, boolean)} can be used instead.
     *
     * @param jsonMapper        the customized JSON mapper
     * @param typeClass         the class of the object to serialize
     * @param storeAsBinaryJson {@code true} for BSON encoding or {@code false} for string encoding
     */
    public JacksonSerializer(JsonMapper jsonMapper, Class<T> typeClass, boolean storeAsBinaryJson) {
        this.jsonMapper = requireNonNull(jsonMapper, "jsonMapper cannot be null");
        this.typeClass = requireNonNull(typeClass, "typeClass cannot be null");
        this.typeReference = null;
        this.storeAsBinaryJson = storeAsBinaryJson;
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with a customizable JSON mapper
     * and reference-based type information. If a default JSON mapper is sufficient,
     * {@link #JacksonSerializer(TypeReference, boolean)} can be used instead.
     *
     * @param jsonMapper        the customized JSON mapper
     * @param typeReference     the type reference of the object to serialize
     * @param storeAsBinaryJson {@code true} for BSON encoding or {@code false} for string encoding
     */
    public JacksonSerializer(JsonMapper jsonMapper, TypeReference<T> typeReference, boolean storeAsBinaryJson) {
        this.jsonMapper = requireNonNull(jsonMapper, "jsonMapper cannot be null");
        this.typeClass = null;
        this.typeReference = requireNonNull(typeReference, "typeReference cannot be null");
        this.storeAsBinaryJson = storeAsBinaryJson;
    }

    @Override
    public String serialize(T object) {
        return jsonMapper.writeValueAsString(object);
    }

    @Override
    public T deserialize(String value) {
        if (nonNull(typeClass)) {
            return jsonMapper.readValue(value, typeClass);
        } else {
            return jsonMapper.readValue(value, typeReference);
        }
    }

    @Override
    public boolean storeAsBinaryJson() {
        return storeAsBinaryJson;
    }
}
