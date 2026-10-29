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
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of a serializer with JSON representation (stored as text, or in the store's own JSON type where it
 * has one, see {@link JsonSerializer#storeAsNativeJson()}) based on <i>Jackson</i>.
 *
 * @param <T> the type of the object to serialize
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/FasterXML/jackson">Jackson on GitHub</a>
 */
public class JacksonSerializer<T> implements JsonSerializer<T> {

    private final JsonMapper jsonMapper;
    private final @Nullable Class<T> typeClass;
    private final @Nullable TypeReference<T> typeReference;
    private final boolean storeAsNativeJson;

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with class-based type
     * information. If a customized JSON mapper is required, {@link #JacksonSerializer(JsonMapper, Class, boolean)}
     * can be used instead.
     *
     * @param typeClass         the class of the object to serialize
     * @param storeAsNativeJson whether to store values in the store's own JSON type, where it has one (see
     *                          {@link JsonSerializer#storeAsNativeJson()}), or as text
     */
    public JacksonSerializer(Class<T> typeClass, boolean storeAsNativeJson) {
        this(JsonMapper.builder().build(), typeClass, storeAsNativeJson);
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with reference-based type
     * information. If a customized JSON mapper is required,
     * {@link #JacksonSerializer(JsonMapper, TypeReference, boolean)} can be used instead.
     *
     * @param typeReference     the type reference of the object to serialize
     * @param storeAsNativeJson whether to store values in the store's own JSON type, where it has one (see
     *                          {@link JsonSerializer#storeAsNativeJson()}), or as text
     */
    public JacksonSerializer(TypeReference<T> typeReference, boolean storeAsNativeJson) {
        this(JsonMapper.builder().build(), typeReference, storeAsNativeJson);
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with a customizable JSON mapper
     * and class-based type information. If a default JSON mapper is sufficient,
     * {@link #JacksonSerializer(Class, boolean)} can be used instead.
     *
     * @param jsonMapper        the customized JSON mapper
     * @param typeClass         the class of the object to serialize
     * @param storeAsNativeJson whether to store values in the store's own JSON type, where it has one (see
     *                          {@link JsonSerializer#storeAsNativeJson()}), or as text
     */
    public JacksonSerializer(JsonMapper jsonMapper, Class<T> typeClass, boolean storeAsNativeJson) {
        this.jsonMapper = requireNonNull(jsonMapper, "jsonMapper cannot be null");
        this.typeClass = requireNonNull(typeClass, "typeClass cannot be null");
        this.typeReference = null;
        this.storeAsNativeJson = storeAsNativeJson;
    }

    /**
     * Constructs a serializer with JSON representation based on <i>Jackson</i> along with a customizable JSON mapper
     * and reference-based type information. If a default JSON mapper is sufficient,
     * {@link #JacksonSerializer(TypeReference, boolean)} can be used instead.
     *
     * @param jsonMapper        the customized JSON mapper
     * @param typeReference     the type reference of the object to serialize
     * @param storeAsNativeJson whether to store values in the store's own JSON type, where it has one (see
     *                          {@link JsonSerializer#storeAsNativeJson()}), or as text
     */
    public JacksonSerializer(JsonMapper jsonMapper, TypeReference<T> typeReference, boolean storeAsNativeJson) {
        this.jsonMapper = requireNonNull(jsonMapper, "jsonMapper cannot be null");
        this.typeClass = null;
        this.typeReference = requireNonNull(typeReference, "typeReference cannot be null");
        this.storeAsNativeJson = storeAsNativeJson;
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
    public boolean storeAsNativeJson() {
        return storeAsNativeJson;
    }
}
