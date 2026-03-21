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

import static java.util.Objects.requireNonNull;

/**
 * Class to extend when implementing a custom adapter that manages distributed synchronization between cache instances
 * using an underlying store.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@NullMarked
public abstract class AbstractAdapter<K, V> implements Adapter<K, V> {

    /**
     * The repository to be used by the adapter.
     */
    protected final Repository<K, V> repository;

    /**
     * The synchronizer to be used by this adapter.
     */
    protected final Synchronizer<K, V> synchronizer;

    /**
     * The identifier to be used by this adapter.
     */
    protected final String identifier;

    /**
     * Constructs a new adapter defined by the specified parameters.
     *
     * @param repository   the repository to be used by the adapter
     * @param synchronizer the synchronizer to be used by this adapter
     * @param identifier   the identifier to be used by this adapter
     */
    protected AbstractAdapter(Repository<K, V> repository, Synchronizer<K, V> synchronizer, String identifier) {
        requireNonNull(repository, "repository cannot be null");
        requireNonNull(synchronizer, "synchronizer cannot be null");
        requireNonNull(identifier, "identifier cannot be null");
        this.repository = repository;
        this.synchronizer = synchronizer;
        this.identifier = identifier;
        this.repository.setIdentifier(identifier);
        this.synchronizer.setIdentifier(identifier);
    }

    @Override
    public Repository<K, V> getRepository() {
        return repository;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public void activate() {
        this.synchronizer.activate();
    }

    @Override
    public void deactivate() {
        this.synchronizer.deactivate();
    }

    @Override
    public boolean isActivated() {
        return this.synchronizer.isActivated();
    }

    @Override
    public void setKeySerializer(Serializer<K, ?> keySerializer) {
        requireNonNull(keySerializer, "keySerializer cannot be null");
        this.repository.setKeySerializer(keySerializer);
        this.synchronizer.setKeySerializer(keySerializer);
    }

    @Override
    public void setValueSerializer(Serializer<V, ?> valueSerializer) {
        requireNonNull(valueSerializer, "valueSerializer cannot be null");
        this.repository.setValueSerializer(valueSerializer);
        this.synchronizer.setValueSerializer(valueSerializer);
    }

    @Override
    public void setRetriever(Retriever<K, V> retriever) {
        requireNonNull(retriever, "retriever cannot be null");
        this.synchronizer.setRetriever(retriever);
    }
}
