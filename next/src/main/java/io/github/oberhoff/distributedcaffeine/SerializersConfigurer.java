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
import io.github.oberhoff.distributedcaffeine.serializer.ForySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * How keys and values are serialized for the store. Both default to {@link ForySerializer}, which handles every type
 * without configuration and stores bytes; {@link StringSerializer#ofString()} and its siblings, or a
 * {@link JsonSerializer}, store them readably. A serializer must implement {@link ByteArraySerializer},
 * {@link StringSerializer} or {@link JsonSerializer}; which one decides how the store holds the result.
 * <p>
 * <b>Attention:</b> values come from a shared store, so with the default whoever can write to the store can make
 * every instance deserialize arbitrary classes. If the store's access cannot be trusted, use
 * {@link ForySerializer#requiringRegistration(Class[])} or another hardened serializer.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class SerializersConfigurer<K, V> {

    private @Nullable InternalSerializer<K> keySerializer;
    private @Nullable InternalSerializer<V> valueSerializer;

    SerializersConfigurer() {
    }

    /**
     * Sets the serializer of keys. Keys are matched by hash; their serialized form is stored for diagnostics only.
     *
     * @param keySerializer the serializer
     * @return this configurer
     */
    public SerializersConfigurer<K, V> withKeySerializer(Serializer<K, ?> keySerializer) {
        if (this.keySerializer != null) {
            throw new IllegalStateException("keySerializer was already set");
        }
        this.keySerializer = InternalSerializer.of(requireNonNull(keySerializer, "keySerializer cannot be null"));
        return this;
    }

    /**
     * Sets the serializer of values.
     *
     * @param valueSerializer the serializer
     * @return this configurer
     */
    public SerializersConfigurer<K, V> withValueSerializer(Serializer<V, ?> valueSerializer) {
        if (this.valueSerializer != null) {
            throw new IllegalStateException("valueSerializer was already set");
        }
        this.valueSerializer = InternalSerializer.of(requireNonNull(valueSerializer, "valueSerializer cannot be null"));
        return this;
    }

    InternalSerializer<K> keySerializer() {
        return keySerializer != null ? keySerializer : InternalSerializer.of(new ForySerializer<>());
    }

    InternalSerializer<V> valueSerializer() {
        return valueSerializer != null ? valueSerializer : InternalSerializer.of(new ForySerializer<>());
    }
}
