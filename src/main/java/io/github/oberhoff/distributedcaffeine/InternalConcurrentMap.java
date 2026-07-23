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

import java.util.AbstractCollection;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static io.github.oberhoff.distributedcaffeine.InternalKey.ik;
import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.entry;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.im;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.m;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullMap;
import static io.github.oberhoff.distributedcaffeine.InternalValue.iv;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

class InternalConcurrentMap<K, V> implements ConcurrentMap<K, V>, InternalLazyInitializer<K, V> {

    private ConcurrentMap<InternalKey<K>, InternalValue<V>> concurrentMap;
    private InternalCacheManager<K, V> cacheManager;
    private InternalSynchronizationLock synchronizationLock;

    InternalConcurrentMap() {
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.concurrentMap = instanceRegistry.getCache().asMap();
        this.cacheManager = instanceRegistry.getCacheManager();
        this.synchronizationLock = instanceRegistry.getSynchronizationLock();
    }

    /*
    // TODO this methods should be atomic:
    computeIfAbsent(K key, Function mappingFunction)
    computeIfPresent(K key, BiFunction remappingFunction)
    compute(K key, BiFunction remappingFunction)
    merge(K key, V value, BiFunction remappingFunction)
    putIfAbsent(K key, V value)
    remove(Object key, Object value)
    replace(K key, V oldValue, V newValue)
    replace(K key, V value)
    */

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        return v(concurrentMap.get(ik((K) key)));
    }

    @Override
    public V put(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        return synchronizationLock.getLocked(() ->
                v(concurrentMap.put(ik(key), cacheManager.putDistributed(ik(key), iv(value)))));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        requireNonNullMap(map);
        synchronizationLock.runLocked(() ->
                concurrentMap.putAll(cacheManager.putAllDistributed(im(map))));
    }

    @Override
    public V putIfAbsent(K key, V value) {
        requireNonNull(key);
        if (!containsKey(key)) {
            return put(key, value); // implicit distribution
        } else {
            return get(key);
        }
    }

    @Override
    public V replace(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        if (containsKey(key)) {
            return put(key, value); // implicit distribution
        } else {
            return null;
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        requireNonNull(key);
        requireNonNull(oldValue);
        requireNonNull(newValue);
        if (containsKey(key) && Objects.equals(get(key), oldValue)) {
            put(key, newValue); // implicit distribution
            return true;
        } else {
            return false;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        requireNonNull(key);
        return synchronizationLock.getLocked(() ->
                v(concurrentMap.remove(cacheManager.invalidateDistributed(ik((K) key)))));
    }

    @Override
    public boolean remove(Object key, Object value) {
        requireNonNull(key);
        if (containsKey(key) && Objects.equals(get(key), value)) {
            remove(key); // implicit distribution
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void clear() {
        synchronizationLock.runLocked(() -> {
            Set<InternalKey<K>> keySet = concurrentMap.keySet();
            cacheManager.invalidateAllDistributed(keySet);
            keySet.clear();
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
            public Iterator<K> iterator() {
                return new Iterator<>() {
                    private final Iterator<InternalKey<K>> iterator =
                            concurrentMap.keySet().iterator();
                    private InternalKey<K> next;

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
                        if (isNull(next)) {
                            throw new IllegalStateException();
                        } else {
                            synchronizationLock.runLocked(() -> {
                                cacheManager.invalidateDistributed(next);
                                iterator.remove();
                            });
                        }
                    }
                };
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                return synchronizationLock.getLocked(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.keySet().stream()
                            .filter(key -> c.contains(ik(key)))
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.keySet().removeAll(c);
                });
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                return synchronizationLock.getLocked(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.keySet().stream()
                            .filter(key -> !c.contains(ik(key)))
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.keySet().retainAll(c);
                });
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
            public Iterator<V> iterator() {
                return new Iterator<>() {
                    private final Iterator<Entry<InternalKey<K>, InternalValue<V>>> iterator =
                            concurrentMap.entrySet().iterator();
                    private Entry<InternalKey<K>, InternalValue<V>> next;

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
                    public void remove() {
                        if (isNull(next)) {
                            throw new IllegalStateException();
                        } else {
                            synchronizationLock.runLocked(() -> {
                                cacheManager.invalidateDistributed(next.getKey());
                                iterator.remove();
                            });
                        }
                    }
                };
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                return synchronizationLock.getLocked(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> c.contains(iv(entry.getValue())))
                            .map(Entry::getKey)
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.values().removeAll(c);
                });
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                return synchronizationLock.getLocked(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> !c.contains(iv(entry.getValue())))
                            .map(Entry::getKey)
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.values().retainAll(c);
                });
            }

            @Override
            public void clear() {
                InternalConcurrentMap.this.clear(); // implicit distribution
            }

            public int size() {
                return InternalConcurrentMap.this.size();
            }
        };
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            public Iterator<Entry<K, V>> iterator() {
                return new Iterator<>() {
                    private final Iterator<Entry<InternalKey<K>, InternalValue<V>>> iterator =
                            concurrentMap.entrySet().iterator();
                    private Entry<InternalKey<K>, InternalValue<V>> next;

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
                    public void remove() {
                        if (isNull(next)) {
                            throw new IllegalStateException();
                        } else {
                            synchronizationLock.runLocked(() -> {
                                cacheManager.invalidateDistributed(next.getKey());
                                iterator.remove();
                            });
                        }
                    }
                };
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                return synchronizationLock.getLocked(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> c.contains(entry(ik(entry.getKey()), iv(entry.getValue()))))
                            .map(Entry::getKey)
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.entrySet().removeAll(c);
                });
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                return synchronizationLock.getLocked(() -> {
                    Set<InternalKey<K>> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> !c.contains(entry(ik(entry.getKey()), iv(entry.getValue()))))
                            .map(Entry::getKey)
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.entrySet().retainAll(c);
                });
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
        return m(concurrentMap).hashCode();
    }

    @Override
    public String toString() {
        return m(concurrentMap).toString();
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
