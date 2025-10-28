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
package io.github.oberhoff.distributedcaffeine;

import org.jspecify.annotations.NonNull;

import java.util.AbstractCollection;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullMap;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

class InternalConcurrentMap<K, V> implements ConcurrentMap<K, V>, InternalLazyInitializer<K, V> {

    private ConcurrentMap<K, V> concurrentMap;
    private InternalCacheManager<K, V> cacheManager;
    private InternalSynchronizationLock synchronizationLock;

    InternalConcurrentMap() {
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.concurrentMap = distributedCaffeine.getCache().asMap();
        this.cacheManager = distributedCaffeine.getCacheManager();
        this.synchronizationLock = distributedCaffeine.getSynchronizationLock();
    }

    @Override
    public V get(Object key) {
        return concurrentMap.get(key);
    }

    @Override
    public V put(K key, V value) {
        requireNonNull(key);
        requireNonNull(value);
        return synchronizationLock.getLocked(() ->
                concurrentMap.put(key, cacheManager.putDistributed(key, value)));
    }

    @Override
    public void putAll(@NonNull Map<? extends K, ? extends V> map) {
        requireNonNullMap(map);
        synchronizationLock.runLocked(() ->
                concurrentMap.putAll(cacheManager.putAllDistributed(map)));
    }

    @Override
    public V putIfAbsent(@NonNull K key, V value) {
        requireNonNull(key);
        if (!containsKey(key)) {
            return put(key, value); // implicit distribution
        } else {
            return get(key);
        }
    }

    @Override
    public V replace(@NonNull K key, @NonNull V value) {
        requireNonNull(key);
        requireNonNull(value);
        if (containsKey(key)) {
            return put(key, value); // implicit distribution
        } else {
            return null;
        }
    }

    @Override
    public boolean replace(@NonNull K key, @NonNull V oldValue, @NonNull V newValue) {
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
                concurrentMap.remove(cacheManager.invalidateDistributed((K) key)));
    }

    @Override
    public boolean remove(@NonNull Object key, Object value) {
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
            Set<K> keySet = concurrentMap.keySet();
            cacheManager.invalidateAllDistributed(keySet);
            keySet.clear();
        });
    }

    @Override
    public boolean containsKey(Object key) {
        return concurrentMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return concurrentMap.containsValue(value);
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
    public @NonNull Set<K> keySet() {
        return new AbstractSet<>() {
            public @NonNull Iterator<K> iterator() {
                return new Iterator<>() {
                    private final Iterator<K> iterator = concurrentMap.keySet().iterator();
                    private K next;

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public K next() {
                        next = iterator.next();
                        return next;
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
                    Set<K> keys = concurrentMap.keySet().stream()
                            .filter(c::contains)
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.keySet().removeAll(c);
                });
            }

            @Override
            public boolean retainAll(@NonNull Collection<?> c) {
                return synchronizationLock.getLocked(() -> {
                    Set<K> keys = concurrentMap.keySet().stream()
                            .filter(key -> !c.contains(key))
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
    public @NonNull Collection<V> values() {
        return new AbstractCollection<>() {
            public @NonNull Iterator<V> iterator() {
                return new Iterator<>() {
                    private final Iterator<Entry<K, V>> iterator = concurrentMap.entrySet().iterator();
                    private Entry<K, V> next;

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public V next() {
                        next = iterator.next();
                        return next.getValue();
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
            public boolean removeAll(@NonNull Collection<?> c) {
                return synchronizationLock.getLocked(() -> {
                    Set<K> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> c.contains(entry.getValue()))
                            .map(Entry::getKey)
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.values().removeAll(c);
                });
            }

            @Override
            public boolean retainAll(@NonNull Collection<?> c) {
                return synchronizationLock.getLocked(() -> {
                    Set<K> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> !c.contains(entry.getValue()))
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
    public @NonNull Set<Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            public @NonNull Iterator<Entry<K, V>> iterator() {
                return new Iterator<>() {
                    private final Iterator<Entry<K, V>> iterator = concurrentMap.entrySet().iterator();
                    private Entry<K, V> next;

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        next = new WriteThroughEntry<>(iterator.next(), InternalConcurrentMap.this);
                        return next;
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
                    Set<K> keys = concurrentMap.entrySet().stream()
                            .filter(c::contains)
                            .map(Entry::getKey)
                            .collect(toSet());
                    cacheManager.invalidateAllDistributed(keys);
                    return concurrentMap.entrySet().removeAll(c);
                });
            }

            @Override
            public boolean retainAll(@NonNull Collection<?> c) {
                return synchronizationLock.getLocked(() -> {
                    Set<K> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> !c.contains(entry))
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
        return object instanceof Map && concurrentMap.equals(object);
    }

    @Override
    public int hashCode() {
        return concurrentMap.hashCode();
    }

    @Override
    public String toString() {
        return concurrentMap.toString();
    }

    @SuppressWarnings("squid:S2160")
    static final class WriteThroughEntry<K, V> extends SimpleEntry<K, V> {

        private final transient Map<K, V> map;

        public WriteThroughEntry(Entry<? extends K, ? extends V> entry, Map<K, V> map) {
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
