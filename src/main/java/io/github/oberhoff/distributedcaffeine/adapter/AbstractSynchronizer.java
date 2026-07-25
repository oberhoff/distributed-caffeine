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
package io.github.oberhoff.distributedcaffeine.adapter;

import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Class to extend when implementing a custom synchronizer that manages distributed synchronization between cache
 * instances using an underlying store.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@NullMarked
public abstract class AbstractSynchronizer<K, V> implements Synchronizer<K, V> {

    /**
     * The identifier to be used by this adapter.
     */
    protected @Nullable String identifier;

    /**
     * The key serializer to be used by this adapter.
     */
    protected @Nullable Serializer<K, ?> keySerializer;

    /**
     * The value serializer to be used by this adapter.
     */
    protected @Nullable Serializer<V, ?> valueSerializer;

    /**
     * The key retriever to be used by this adapter.
     */
    protected @Nullable Retriever<K, V> retriever;

    /**
     * Constructs a new synchronizer.
     */
    protected AbstractSynchronizer() {
        // noop
    }

    @Override
    public void setIdentifier(String identifier) {
        requireNonNull(identifier, "identifier cannot be null");
        this.identifier = identifier;
    }

    @Override
    public void setKeySerializer(Serializer<K, ?> keySerializer) {
        requireNonNull(keySerializer, "keySerializer cannot be null");
        this.keySerializer = keySerializer;
    }

    @Override
    public void setValueSerializer(Serializer<V, ?> valueSerializer) {
        requireNonNull(valueSerializer, "valueSerializer cannot be null");
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void setRetriever(Retriever<K, V> retriever) {
        requireNonNull(retriever, "retriever cannot be null");
        this.retriever = retriever;
    }
}
