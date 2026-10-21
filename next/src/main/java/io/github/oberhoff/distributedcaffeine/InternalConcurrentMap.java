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

import org.jspecify.annotations.Nullable;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * The {@code asMap()} view of a synchronous cache (see "Reading and writing methods" in VISION.md). Methods about one
 * key read through L2: {@code get} and {@code getOrDefault} like {@code getIfPresent}, {@code containsKey} as an
 * existence check, {@code computeIfAbsent} like {@code get(key, function)}, and the conditional methods atomically
 * across instances (the remapping may run more than once after a conflict). Methods about the whole map (sizes,
 * iteration, the views, {@code containsValue}) show what this instance holds; removals through the views invalidate
 * globally, and {@code clear()} is the global {@code invalidateAll()}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@SuppressWarnings("java:S2160") // AbstractMap's equals and hashCode compare the entries, as Map requires
final class InternalConcurrentMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V> {

    private static final String VALUE_CANNOT_BE_NULL = "value cannot be null";
    private static final String REMAPPING_FUNCTION_CANNOT_BE_NULL = "remappingFunction cannot be null";

    private final InternalCore<K, V> core;
    private final ConcurrentMap<InternalKey<K>, InternalValue<V>> local;
    private final Set<K> keySetView = new KeySet();
    private final Collection<V> valuesView = new Values();
    private final Set<Map.Entry<K, V>> entrySetView = new EntrySet();

    InternalConcurrentMap(InternalCore<K, V> core) {
        this.core = core;
        this.local = core.l1().synchronous().asMap();
    }

    // one key, through L2

    @Override
    @SuppressWarnings("unchecked")
    public @Nullable V get(Object key) {
        return core.getIfPresent((K) requireNonNull(key), false);
    }

    @Override
    public @Nullable V getOrDefault(Object key, @Nullable V defaultValue) {
        V value = get(key);
        return value != null ? value : defaultValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(Object key) {
        return core.containsKey((K) requireNonNull(key));
    }

    // atomic like the conditional methods, so the previous value is exact across instances
    @Override
    public @Nullable V put(K key, V value) {
        requireNonNull(value, VALUE_CANNOT_BE_NULL);
        return core.update(key, (k, current) -> value, true).oldValue();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        core.putAll(requireNonNull(map, "map cannot be null"));
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nullable V remove(Object key) {
        // always moves the version, also without a value: a load in flight then conflicts
        return core.update((K) requireNonNull(key), (k, current) -> null, true).oldValue();
    }

    @Override
    public @Nullable V putIfAbsent(K key, V value) {
        requireNonNull(value, VALUE_CANNOT_BE_NULL);
        return core.update(key, (k, current) -> current != null ? current : value).oldValue();
    }

    @Override
    public @Nullable V replace(K key, V value) {
        requireNonNull(value, VALUE_CANNOT_BE_NULL);
        return core.update(key, (k, current) -> current != null ? value : null).oldValue();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        requireNonNull(oldValue, "oldValue cannot be null");
        requireNonNull(newValue, "newValue cannot be null");
        V found = core.update(key, (k, current) -> oldValue.equals(current) ? newValue : current).oldValue();
        return oldValue.equals(found);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object key, @Nullable Object value) {
        requireNonNull(key);
        if (value == null) {
            return false;
        }
        V found = core.update((K) key, (k, current) -> value.equals(current) ? null : current).oldValue();
        return value.equals(found);
    }

    @Override
    public @Nullable V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        requireNonNull(mappingFunction, "mappingFunction cannot be null");
        return core.get(key, mappingFunction::apply);
    }

    @Override
    public @Nullable V computeIfPresent(K key,
                                        BiFunction<? super K, ? super V, ? extends @Nullable V> remappingFunction) {
        requireNonNull(remappingFunction, REMAPPING_FUNCTION_CANNOT_BE_NULL);
        BiFunction<K, V, @Nullable V> counted = core.countedAsLoad(remappingFunction);
        return core.update(key, (k, current) -> current == null ? null : counted.apply(k, current)).newValue();
    }

    @Override
    public @Nullable V compute(K key,
                               BiFunction<? super K, ? super @Nullable V, ? extends @Nullable V> remappingFunction) {
        requireNonNull(remappingFunction, REMAPPING_FUNCTION_CANNOT_BE_NULL);
        return core.update(key, core.<K, @Nullable V>countedAsLoad(remappingFunction)::apply).newValue();
    }

    @Override
    public @Nullable V merge(K key, V value,
                             BiFunction<? super V, ? super V, ? extends @Nullable V> remappingFunction) {
        requireNonNull(value, VALUE_CANNOT_BE_NULL);
        requireNonNull(remappingFunction, REMAPPING_FUNCTION_CANNOT_BE_NULL);
        return core.update(key, (k, current) -> current == null ? value
                : core.countedAsLoad(remappingFunction).apply(current, value)).newValue();
    }

    @Override
    public void clear() {
        core.invalidateAll();
    }

    // the whole map, local

    @Override
    public int size() {
        core.checkOpen();
        return local.size();
    }

    @Override
    public boolean isEmpty() {
        core.checkOpen();
        return local.isEmpty();
    }

    @Override
    public boolean containsValue(Object value) {
        requireNonNull(value);
        core.checkOpen();
        for (InternalValue<V> held : local.values()) {
            if (value.equals(held.value())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<K> keySet() {
        return keySetView;
    }

    @Override
    public Collection<V> values() {
        return valuesView;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return entrySetView;
    }

    // iterates what this instance holds; remove() invalidates the last key globally
    private abstract class LocalIterator<T> implements Iterator<T> {

        private final Iterator<Map.Entry<InternalKey<K>, InternalValue<V>>> iterator;
        private @Nullable K last;

        LocalIterator() {
            core.checkOpen();
            this.iterator = local.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            Map.Entry<InternalKey<K>, InternalValue<V>> next = iterator.next();
            last = next.getKey().key();
            return map(next.getKey().key(), next.getValue().value());
        }

        abstract T map(K key, V value);

        @Override
        public void remove() {
            if (last == null) {
                throw new IllegalStateException();
            }
            core.invalidate(last);
            last = null;
        }
    }

    private final class KeySet extends AbstractSet<K> {

        @Override
        public Iterator<K> iterator() {
            return new LocalIterator<K>() {
                @Override
                K map(K key, V value) {
                    return key;
                }
            };
        }

        @Override
        public int size() {
            return InternalConcurrentMap.this.size();
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean contains(Object key) {
            return local.containsKey(core.wrap((K) requireNonNull(key)));
        }

        // true if this instance held the key; invalidated globally either way
        @Override
        @SuppressWarnings("unchecked")
        public boolean remove(Object key) {
            K k = (K) requireNonNull(key);
            boolean held = contains(k);
            core.invalidate(k);
            return held;
        }

        @Override
        public void clear() {
            InternalConcurrentMap.this.clear();
        }
    }

    private final class Values extends AbstractCollection<V> {

        @Override
        public Iterator<V> iterator() {
            return new LocalIterator<V>() {
                @Override
                V map(K key, V value) {
                    return value;
                }
            };
        }

        @Override
        public int size() {
            return InternalConcurrentMap.this.size();
        }

        @Override
        public boolean contains(Object value) {
            return containsValue(value);
        }

        @Override
        public void clear() {
            InternalConcurrentMap.this.clear();
        }
    }

    private final class EntrySet extends AbstractSet<Map.Entry<K, V>> {

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new LocalIterator<Map.Entry<K, V>>() {
                @Override
                Map.Entry<K, V> map(K key, V value) {
                    return new WriteThroughEntry(key, value);
                }
            };
        }

        @Override
        public int size() {
            return InternalConcurrentMap.this.size();
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean contains(Object entry) {
            if (!(entry instanceof Map.Entry<?, ?> e) || e.getKey() == null || e.getValue() == null) {
                return false;
            }
            InternalValue<V> held = local.get(core.wrap((K) e.getKey()));
            return held != null && e.getValue().equals(held.value());
        }

        // conditional on the value, like remove(key, value)
        @Override
        public boolean remove(Object entry) {
            if (!(entry instanceof Map.Entry<?, ?> e) || e.getKey() == null) {
                return false;
            }
            return InternalConcurrentMap.this.remove(e.getKey(), e.getValue());
        }

        @Override
        public void clear() {
            InternalConcurrentMap.this.clear();
        }
    }

    // setValue writes, like put
    private final class WriteThroughEntry extends SimpleEntry<K, V> {

        WriteThroughEntry(K key, V value) {
            super(key, value);
        }

        @Override
        public V setValue(V value) {
            requireNonNull(value, VALUE_CANNOT_BE_NULL);
            core.put(getKey(), value);
            return super.setValue(value);
        }
    }
}
