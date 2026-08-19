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
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.InternalKey.ik;
import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.entry;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.im;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.m;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullMap;
import static io.github.oberhoff.distributedcaffeine.InternalValue.iv;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static io.github.oberhoff.distributedcaffeine.InternalValue.vn;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

class InternalConcurrentMap<K, V> implements ConcurrentMap<K, V>, InternalInitializable<K, V> {

    @SuppressWarnings("NotNullFieldNotInitialized")
    private ConcurrentMap<InternalKey<K>, InternalValue<V>> concurrentMap;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalCacheManager<K, V> cacheManager;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalSynchronizationLock synchronizationLock;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalConcurrentMap() {
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.concurrentMap = instanceRegistry.getCache().asMap();
        this.cacheManager = instanceRegistry.getCacheManager();
        this.synchronizationLock = instanceRegistry.getSynchronizationLock();
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nullable V get(Object key) {
        return vn(concurrentMap.get(ik((K) key)));
    }

    @Override
    public @Nullable V put(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        InternalKey<K> internalKey = ik(key);
        return synchronizationLock.getLockedOrNull(() ->
                vn(concurrentMap.put(internalKey, cacheManager.putDistributed(internalKey, iv(value)))));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        requireNonNullMap(map);
        synchronizationLock.runLocked(() ->
                concurrentMap.putAll(cacheManager.putAllDistributed(im(map))));
    }

    @Override
    public @Nullable V putIfAbsent(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        // atomic check-then-act under the (reentrant) synchronization lock; map values are never null, so a
        // non-null get() already proves presence (no separate containsKey needed)
        return synchronizationLock.getLockedOrNull(() -> {
            V oldValue = get(key);
            return isNull(oldValue)
                    ? put(key, value) // implicit distribution
                    : oldValue;
        });
    }

    @Override
    public @Nullable V replace(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        return synchronizationLock.getLockedOrNull(() ->
                isNull(get(key))
                        ? null
                        : put(key, value)); // implicit distribution
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        requireNonNull(key);
        requireNonNull(oldValue);
        requireNonNull(newValue);
        // implicit distribution
        return TRUE.equals(synchronizationLock.getLockedOrNull(() -> {
            if (Objects.equals(get(key), oldValue)) {
                put(key, newValue); // implicit distribution
                return true;
            }
            return false;
        }));
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nullable V remove(Object key) {
        requireNonNull(key);
        return synchronizationLock.getLockedOrNull(() ->
                vn(concurrentMap.remove(cacheManager.invalidateDistributed(ik((K) key)))));
    }

    @Override
    public boolean remove(Object key, Object value) {
        requireNonNull(key);
        // atomic check-then-act; a null value never matches (map values are never null), so no exception is thrown
        // implicit distribution
        return TRUE.equals(synchronizationLock.getLockedOrNull(() -> {
            V oldValue = get(key);
            if (nonNull(oldValue) && Objects.equals(oldValue, value)) {
                remove(key); // implicit distribution
                return true;
            }
            return false;
        }));
    }

    @Override
    public @Nullable V computeIfAbsent(K key, Function<? super K, ? extends @Nullable V> mappingFunction) {
        requireNonNull(key);
        requireNonNull(mappingFunction);
        // atomic under the (reentrant) synchronization lock; the mapping function is applied at most once, and the
        // resulting change is distributed via put() - the inherited default is a non-atomic CAS-retry that may apply
        // the function multiple times
        return synchronizationLock.getLockedOrNull(() -> {
            V oldValue = get(key);
            if (nonNull(oldValue)) {
                return oldValue;
            }
            V newValue = mappingFunction.apply(key);
            if (nonNull(newValue)) {
                put(key, newValue); // implicit distribution
            }
            return newValue;
        });
    }

    @Override
    public @Nullable V computeIfPresent(K key, BiFunction<? super K, ? super @Nullable V,
            ? extends @Nullable V> remappingFunction) {
        requireNonNull(key);
        requireNonNull(remappingFunction);
        return synchronizationLock.getLockedOrNull(() -> {
            V oldValue = get(key);
            if (isNull(oldValue)) {
                return null;
            }
            V newValue = remappingFunction.apply(key, oldValue);
            if (nonNull(newValue)) {
                put(key, newValue); // implicit distribution
                return newValue;
            }
            remove(key); // implicit distribution
            return null;
        });
    }

    @Override
    public @Nullable V compute(K key, BiFunction<? super K, ? super @Nullable V,
            ? extends @Nullable V> remappingFunction) {
        requireNonNull(key);
        requireNonNull(remappingFunction);
        return synchronizationLock.getLockedOrNull(() -> {
            V oldValue = get(key);
            V newValue = remappingFunction.apply(key, oldValue);
            if (nonNull(newValue)) {
                put(key, newValue); // implicit distribution
                return newValue;
            }
            if (nonNull(oldValue)) {
                remove(key); // implicit distribution
            }
            return null;
        });
    }

    @Override
    public @Nullable V merge(K key, V value, BiFunction<? super @Nullable V, ? super V,
            ? extends @Nullable V> remappingFunction) {
        requireNonNull(key);
        requireNonNull(value);
        requireNonNull(remappingFunction);
        return synchronizationLock.getLockedOrNull(() -> {
            V oldValue = get(key);
            V newValue = isNull(oldValue)
                    ? value
                    : remappingFunction.apply(oldValue, value);
            if (nonNull(newValue)) {
                put(key, newValue); // implicit distribution
            } else {
                // newValue can only be null when the remapping function ran, which implies oldValue was present
                // (value is non-null), so there is always a mapping to remove here
                remove(key); // implicit distribution
            }
            return newValue;
        });
    }

    @Override
    public void clear() {
        synchronizationLock.runLocked(() -> {
            cacheManager.invalidateAllDistributed();
            concurrentMap.clear();
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(Object key) {
        return concurrentMap.containsKey(ik((K) key));
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsValue(Object value) {
        return concurrentMap.containsValue(iv((V) value));
    }

    @Override
    public int size() {
        return concurrentMap.size();
    }

    @Override
    public boolean isEmpty() {
        return concurrentMap.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<K> iterator() {
                return new Iterator<>() {
                    private final Iterator<InternalKey<K>> iterator =
                            concurrentMap.keySet().iterator();
                    private @Nullable InternalKey<K> next;

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public K next() {
                        next = iterator.next();
                        return k(next);
                    }

                    @Override
                    public void remove() {
                        // read into a local, so that what is invalidated is what the guard above checked - and
                        // clear it afterwards, so that a second call fails the guard instead of distributing an
                        // invalidation for an entry that is already gone and only then letting the delegate throw
                        InternalKey<K> current = next;
                        if (isNull(current)) {
                            throw new IllegalStateException();
                        }
                        synchronizationLock.runLocked(() -> {
                            cacheManager.invalidateDistributed(current);
                            iterator.remove();
                        });
                        next = null;
                    }
                };
            }

            // the inherited implementations scan through the iterator above, which is correct but linear, while the
            // map itself answers both directly
            @Override
            public boolean contains(Object key) {
                return InternalConcurrentMap.this.containsKey(key);
            }

            @Override
            public boolean remove(Object key) {
                // atomic check-then-act under the (reentrant) synchronization lock; map values are never null, so a
                // non-null result already proves the key was there. Checking first keeps a key that is not there
                // from being distributed as invalidated, which is what scanning through the iterator did as well
                return TRUE.equals(synchronizationLock.getLockedOrNull(() ->
                        containsKey(key) && nonNull(InternalConcurrentMap.this.remove(key)))); // implicit distribution
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                requireNonNull(c);
                return TRUE.equals(synchronizationLock.getLockedOrNull(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.keySet().stream()
                            .filter(key -> c.contains(k(key)))
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.keySet().removeAll(keys);
                }));
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                requireNonNull(c);
                return TRUE.equals(synchronizationLock.getLockedOrNull(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.keySet().stream()
                            .filter(key -> !c.contains(k(key)))
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.keySet().removeAll(keys);
                }));
            }

            @Override
            public void clear() {
                InternalConcurrentMap.this.clear(); // implicit distribution
            }

            @Override
            public int size() {
                return InternalConcurrentMap.this.size();
            }
        };
    }

    @Override
    public Collection<V> values() {
        return new AbstractCollection<>() {
            @Override
            public Iterator<V> iterator() {
                return new Iterator<>() {
                    private final Iterator<Entry<InternalKey<K>, InternalValue<V>>> iterator =
                            concurrentMap.entrySet().iterator();
                    private @Nullable Entry<InternalKey<K>, InternalValue<V>> next;

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public V next() {
                        next = iterator.next();
                        return v(next.getValue());
                    }

                    @Override
                    @SuppressWarnings("DuplicatedCode")
                    public void remove() {
                        // see the key set iterator above for why this reads into a local and clears afterwards
                        Entry<InternalKey<K>, InternalValue<V>> current = next;
                        if (isNull(current)) {
                            throw new IllegalStateException();
                        }
                        synchronizationLock.runLocked(() -> {
                            cacheManager.invalidateDistributed(current.getKey());
                            iterator.remove();
                        });
                        next = null;
                    }
                };
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                requireNonNull(c);
                return TRUE.equals(synchronizationLock.getLockedOrNull(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> c.contains(v(entry.getValue())))
                            .map(Entry::getKey)
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.keySet().removeAll(keys);
                }));
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                requireNonNull(c);
                return TRUE.equals(synchronizationLock.getLockedOrNull(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> !c.contains(v(entry.getValue())))
                            .map(Entry::getKey)
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.keySet().removeAll(keys);
                }));
            }

            @Override
            public void clear() {
                InternalConcurrentMap.this.clear(); // implicit distribution
            }

            @Override
            public int size() {
                return InternalConcurrentMap.this.size();
            }
        };
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<Entry<K, V>> iterator() {
                return new Iterator<>() {
                    private final Iterator<Entry<InternalKey<K>, InternalValue<V>>> iterator =
                            concurrentMap.entrySet().iterator();
                    private @Nullable Entry<InternalKey<K>, InternalValue<V>> next;

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        next = iterator.next();
                        return new WriteThroughEntry<>(entry(k(next.getKey()), v(next.getValue())),
                                InternalConcurrentMap.this);
                    }

                    @Override
                    @SuppressWarnings("DuplicatedCode")
                    public void remove() {
                        // see the key set iterator above for why this reads into a local and clears afterwards
                        Entry<InternalKey<K>, InternalValue<V>> current = next;
                        if (isNull(current)) {
                            throw new IllegalStateException();
                        }
                        synchronizationLock.runLocked(() -> {
                            cacheManager.invalidateDistributed(current.getKey());
                            iterator.remove();
                        });
                        next = null;
                    }
                };
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                requireNonNull(c);
                return TRUE.equals(synchronizationLock.getLockedOrNull(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> c.contains(entry(k(entry.getKey()), v(entry.getValue()))))
                            .map(Entry::getKey)
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.keySet().removeAll(keys);
                }));
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                requireNonNull(c);
                return TRUE.equals(synchronizationLock.getLockedOrNull(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> !c.contains(entry(k(entry.getKey()), v(entry.getValue()))))
                            .map(Entry::getKey)
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.keySet().removeAll(keys);
                }));
            }

            @Override
            public void clear() {
                InternalConcurrentMap.this.clear(); // implicit distribution
            }

            @Override
            public int size() {
                return InternalConcurrentMap.this.size();
            }
        };
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof Map<?, ?> map && Objects.equals(m(concurrentMap), map);
    }

    @Override
    public int hashCode() {
        // InternalKey/InternalValue delegate hashCode() to the wrapped key/value, so the underlying map already
        // satisfies the Map.hashCode() contract (sum of key.hashCode() ^ value.hashCode()) without unwrapping
        return concurrentMap.hashCode();
    }

    @Override
    public String toString() {
        // render directly in the standard '{key=value, ...}' format without materializing an unwrapped copy
        return concurrentMap.entrySet().stream()
                .map(entry -> k(entry.getKey()) + "=" + v(entry.getValue()))
                .collect(joining(", ", "{", "}"));
    }

    @SuppressWarnings("java:S2160")
    private static final class WriteThroughEntry<K, V> extends SimpleEntry<K, V> {

        private final transient Map<K, V> map;

        private WriteThroughEntry(Entry<? extends K, ? extends V> entry, InternalConcurrentMap<K, V> map) {
            super(entry);
            this.map = map;
        }

        @Override
        public V setValue(V value) {
            V oldValue = getValue();
            map.put(super.getKey(), value); // implicit distribution
            super.setValue(value);
            return oldValue;
        }
    }
}
