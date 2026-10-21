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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

/**
 * The {@code asMap()} view of an asynchronous cache: the semantics of the synchronous view (see
 * {@link InternalConcurrentMap}) with futures as values. A future in flight in L1 is returned as it is; otherwise
 * the methods answer as the synchronous view does, waiting for the store where the answer is synchronous (a value or
 * a boolean), and the conditional methods wait for the futures they are given, since the condition is decided in
 * the store. {@code put} and {@code computeIfAbsent} do not wait for their futures. The views, sizes and iteration
 * show this instance's L1, loads in flight included, as in Caffeine.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@SuppressWarnings("java:S2160") // AbstractMap's equals and hashCode compare the entries, as Map requires
final class InternalAsyncConcurrentMap<K, V> extends AbstractMap<K, CompletableFuture<V>>
        implements ConcurrentMap<K, CompletableFuture<V>> {

    private static final String VALUE_FUTURE_CANNOT_BE_NULL = "valueFuture cannot be null";
    private static final String REMAPPING_FUNCTION_CANNOT_BE_NULL = "remappingFunction cannot be null";

    private final InternalCore<K, V> core;
    private final ConcurrentMap<K, V> synchronous;
    private final ConcurrentMap<InternalKey<K>, CompletableFuture<InternalValue<V>>> local;
    private final Set<Map.Entry<K, CompletableFuture<V>>> entrySetView = new EntrySet();
    private final Set<K> keySetView = new KeySet();
    private final Collection<CompletableFuture<V>> valuesView = new Values();

    InternalAsyncConcurrentMap(InternalCore<K, V> core, ConcurrentMap<K, V> synchronous) {
        this.core = core;
        this.synchronous = synchronous;
        this.local = core.l1().asMap();
    }

    // one key

    @Override
    @SuppressWarnings("unchecked")
    public @Nullable CompletableFuture<V> get(Object key) {
        CompletableFuture<V> inFlight = inFlight((K) requireNonNull(key));
        return inFlight != null ? inFlight : completed(synchronous.get(key));
    }

    @Override
    public boolean containsKey(Object key) {
        return synchronous.containsKey(key);
    }

    // does not wait for the future; the previous value is the one seen when called
    @Override
    public @Nullable CompletableFuture<V> put(K key, CompletableFuture<V> valueFuture) {
        requireNonNull(valueFuture, VALUE_FUTURE_CANNOT_BE_NULL);
        CompletableFuture<V> previous = get(key);
        core.putAsync(key, valueFuture);
        return previous;
    }

    @Override
    public void putAll(Map<? extends K, ? extends CompletableFuture<V>> map) {
        requireNonNull(map, "map cannot be null").forEach(core::putAsync);
    }

    @Override
    public @Nullable CompletableFuture<V> remove(Object key) {
        return completed(synchronous.remove(key));
    }

    @Override
    public boolean remove(Object key, @Nullable Object value) {
        Object expected = value instanceof CompletableFuture<?> future ? InternalCore.completedValue(future) : null;
        return expected != null && synchronous.remove(key, expected);
    }

    @Override
    public @Nullable CompletableFuture<V> putIfAbsent(K key, CompletableFuture<V> valueFuture) {
        requireNonNull(valueFuture, VALUE_FUTURE_CANNOT_BE_NULL);
        CompletableFuture<V> inFlight = inFlight(key);
        if (inFlight != null) {
            return inFlight;
        }
        return completed(synchronous.putIfAbsent(key, InternalCore.join(valueFuture)));
    }

    @Override
    public @Nullable CompletableFuture<V> replace(K key, CompletableFuture<V> valueFuture) {
        requireNonNull(valueFuture, VALUE_FUTURE_CANNOT_BE_NULL);
        return completed(synchronous.replace(key, InternalCore.join(valueFuture)));
    }

    @Override
    public boolean replace(K key, CompletableFuture<V> oldValue, CompletableFuture<V> newValue) {
        requireNonNull(oldValue, "oldValue cannot be null");
        requireNonNull(newValue, "newValue cannot be null");
        if (InternalCore.completedValue(oldValue) == null) {
            return false;
        }
        return synchronous.replace(key, oldValue.join(), InternalCore.join(newValue));
    }

    // like AsyncCache.get(key, function): does not wait, and completes with null when the value is absent, which
    // Caffeine's types cannot express for a value type that is not nullable
    @Override
    @SuppressWarnings("NullAway")
    public CompletableFuture<V> computeIfAbsent(K key,
                                                Function<? super K, ? extends CompletableFuture<V>> mappingFunction) {
        requireNonNull(mappingFunction, "mappingFunction cannot be null");
        // a mapping function returning no future means that nothing is cached
        return core.getAsync(key,
                k -> requireNonNullElse(mappingFunction.apply(k), CompletableFuture.completedFuture(null)));
    }

    @Override
    public @Nullable CompletableFuture<V> computeIfPresent(K key, BiFunction<? super K, ? super CompletableFuture<V>,
            ? extends @Nullable CompletableFuture<V>> remappingFunction) {
        requireNonNull(remappingFunction, REMAPPING_FUNCTION_CANNOT_BE_NULL);
        return completed(synchronous.computeIfPresent(key, (k, value) ->
                joinOrNull(remappingFunction.apply(k, CompletableFuture.completedFuture(value)))));
    }

    @Override
    public @Nullable CompletableFuture<V> compute(K key, BiFunction<? super K, ? super @Nullable CompletableFuture<V>,
            ? extends @Nullable CompletableFuture<V>> remappingFunction) {
        requireNonNull(remappingFunction, REMAPPING_FUNCTION_CANNOT_BE_NULL);
        return completed(synchronous.compute(key, (k, value) ->
                joinOrNull(remappingFunction.apply(k, completed(value)))));
    }

    @Override
    public @Nullable CompletableFuture<V> merge(K key, CompletableFuture<V> valueFuture,
                                                BiFunction<? super CompletableFuture<V>, ? super CompletableFuture<V>,
                                                        ? extends @Nullable CompletableFuture<V>> remappingFunction) {
        requireNonNull(valueFuture, VALUE_FUTURE_CANNOT_BE_NULL);
        requireNonNull(remappingFunction, REMAPPING_FUNCTION_CANNOT_BE_NULL);
        return completed(synchronous.merge(key, InternalCore.join(valueFuture), (current, value) ->
                joinOrNull(remappingFunction.apply(CompletableFuture.completedFuture(current),
                        CompletableFuture.completedFuture(value)))));
    }

    @Override
    public void clear() {
        synchronous.clear();
    }

    private @Nullable CompletableFuture<V> inFlight(K key) {
        CompletableFuture<InternalValue<V>> held = local.get(core.wrap(key));
        return held != null && !held.isDone() ? unwrap(held) : null;
    }

    private static <V> @Nullable CompletableFuture<V> completed(@Nullable V value) {
        return value == null ? null : CompletableFuture.completedFuture(value);
    }

    private static <V> @Nullable V joinOrNull(@Nullable CompletableFuture<V> future) {
        return future == null ? null : InternalCore.join(future);
    }

    private static <V> CompletableFuture<V> unwrap(CompletableFuture<InternalValue<V>> future) {
        return InternalValue.valuesOf(future);
    }

    // the whole map, local, loads in flight included

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
        // the futures handed out are derived from L1's, so completed values are compared
        Object expected = value instanceof CompletableFuture<?> future ? InternalCore.completedValue(future) : null;
        if (expected == null) {
            return false;
        }
        return local.values().stream().anyMatch(held -> {
            InternalValue<V> heldValue = InternalCore.completedValue(held);
            return heldValue != null && expected.equals(heldValue.value());
        });
    }

    @Override
    public Set<Map.Entry<K, CompletableFuture<V>>> entrySet() {
        return entrySetView;
    }

    @Override
    public Set<K> keySet() {
        return keySetView;
    }

    @Override
    public Collection<CompletableFuture<V>> values() {
        return valuesView;
    }

    // iterates what this instance holds; remove() invalidates the last key globally
    private abstract class LocalIterator<T> implements Iterator<T> {

        private final Iterator<Map.Entry<InternalKey<K>, CompletableFuture<InternalValue<V>>>> iterator;
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
            Map.Entry<InternalKey<K>, CompletableFuture<InternalValue<V>>> next = iterator.next();
            last = next.getKey().key();
            return map(next.getKey().key(), unwrap(next.getValue()));
        }

        abstract T map(K key, CompletableFuture<V> value);

        @Override
        public void remove() {
            if (last == null) {
                throw new IllegalStateException();
            }
            core.invalidate(last);
            last = null;
        }
    }

    private final class EntrySet extends AbstractSet<Map.Entry<K, CompletableFuture<V>>> {

        @Override
        public Iterator<Map.Entry<K, CompletableFuture<V>>> iterator() {
            return new LocalIterator<Map.Entry<K, CompletableFuture<V>>>() {
                @Override
                Map.Entry<K, CompletableFuture<V>> map(K key, CompletableFuture<V> value) {
                    return new WriteThroughEntry(key, value);
                }
            };
        }

        @Override
        public int size() {
            return InternalAsyncConcurrentMap.this.size();
        }

        @Override
        public boolean remove(Object entry) {
            return entry instanceof Map.Entry<?, ?> e && e.getKey() != null
                    && InternalAsyncConcurrentMap.this.remove(e.getKey(), e.getValue());
        }

        @Override
        public void clear() {
            InternalAsyncConcurrentMap.this.clear();
        }
    }

    private final class KeySet extends AbstractSet<K> {

        @Override
        public Iterator<K> iterator() {
            return new LocalIterator<K>() {
                @Override
                K map(K key, CompletableFuture<V> value) {
                    return key;
                }
            };
        }

        @Override
        public int size() {
            return InternalAsyncConcurrentMap.this.size();
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean contains(Object key) {
            return local.containsKey(core.wrap((K) requireNonNull(key)));
        }

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
            InternalAsyncConcurrentMap.this.clear();
        }
    }

    private final class Values extends AbstractCollection<CompletableFuture<V>> {

        @Override
        public Iterator<CompletableFuture<V>> iterator() {
            return new LocalIterator<CompletableFuture<V>>() {
                @Override
                CompletableFuture<V> map(K key, CompletableFuture<V> value) {
                    return value;
                }
            };
        }

        @Override
        public int size() {
            return InternalAsyncConcurrentMap.this.size();
        }

        @Override
        public void clear() {
            InternalAsyncConcurrentMap.this.clear();
        }
    }

    // setValue puts, like put
    private final class WriteThroughEntry extends SimpleEntry<K, CompletableFuture<V>> {

        WriteThroughEntry(K key, CompletableFuture<V> value) {
            super(key, value);
        }

        @Override
        public CompletableFuture<V> setValue(CompletableFuture<V> value) {
            requireNonNull(value, "value cannot be null");
            core.putAsync(getKey(), value);
            return super.setValue(value);
        }
    }
}
