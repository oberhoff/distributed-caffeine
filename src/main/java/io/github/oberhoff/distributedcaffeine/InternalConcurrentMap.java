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

import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.LazyInitializer;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.AbstractCollection;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

class InternalConcurrentMap<K, V> implements ConcurrentMap<K, V>, LazyInitializer<K, V> {

    private DistributedCaffeine<K, V> distributedCaffeine;
    private ConcurrentMap<K, V> concurrentMap;
    private InternalSynchronizationLock synchronizationLock;

    InternalConcurrentMap() {
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.distributedCaffeine = distributedCaffeine;
        this.concurrentMap = distributedCaffeine.getCache().asMap();
        this.synchronizationLock = distributedCaffeine.getSynchronizationLock();
    }

    @Override
    public V get(Object key) {
        return concurrentMap.get(key);
    }

    @Override
    public V put(K key, V value) {
        synchronizationLock.lock();
        try {
            return concurrentMap.put(key, distributedCaffeine.putDistributed(key, value));
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public void putAll(@NonNull Map<? extends K, ? extends V> map) {
        synchronizationLock.lock();
        try {
            concurrentMap.putAll(distributedCaffeine.putAllDistributed(map));
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public V putIfAbsent(@NonNull K key, V value) {
        if (!containsKey(key)) {
            return put(key, value); // implicit distribution
        } else {
            return get(key);
        }
    }

    @Override
    public boolean replace(@NonNull K key, @NonNull V oldValue, @NonNull V newValue) {
        if (containsKey(key) && Objects.equals(get(key), oldValue)) {
            put(key, newValue); // implicit distribution
            return true;
        } else {
            return false;
        }
    }

    @Override
    public V replace(@NonNull K key, @NonNull V value) {
        if (containsKey(key)) {
            return put(key, value); // implicit distribution
        } else {
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        synchronizationLock.lock();
        try {
            return concurrentMap.remove(distributedCaffeine.invalidateDistributed((K) key));
        } finally {
            synchronizationLock.unlock();
        }
    }

    @Override
    public boolean remove(@NonNull Object key, Object value) {
        if (containsKey(key) && Objects.equals(get(key), value)) {
            remove(key); // implicit distribution
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void clear() {
        synchronizationLock.lock();
        try {
            distributedCaffeine.invalidateAllDistributed(concurrentMap.keySet());
            concurrentMap.clear();
        } finally {
            synchronizationLock.unlock();
        }
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
                    private final Iterator<Entry<K, V>> iterator = concurrentMap.entrySet().iterator();
                    private Entry<K, V> next;

                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    public K next() {
                        next = new WriteThroughEntry<>(iterator.next(), InternalConcurrentMap.this);
                        return next.getKey();
                    }

                    @Override
                    public void remove() {
                        synchronizationLock.lock();
                        try {
                            distributedCaffeine.invalidateDistributed(next.getKey());
                            iterator.remove();
                        } finally {
                            synchronizationLock.unlock();
                        }
                    }
                };
            }

            public int size() {
                return InternalConcurrentMap.this.size();
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                synchronizationLock.lock();
                try {
                    Set<K> keys = concurrentMap.keySet().stream()
                            .filter(c::contains)
                            .collect(Collectors.toSet());
                    distributedCaffeine.invalidateAllDistributed(keys);
                    return concurrentMap.keySet().removeAll(c);
                } finally {
                    synchronizationLock.unlock();
                }
            }

            @Override
            public boolean retainAll(@NonNull Collection<?> c) {
                synchronizationLock.lock();
                try {
                    Set<K> keys = concurrentMap.keySet().stream()
                            .filter(key -> !c.contains(key))
                            .collect(Collectors.toSet());
                    distributedCaffeine.invalidateAllDistributed(keys);
                    return concurrentMap.keySet().retainAll(c);
                } finally {
                    synchronizationLock.unlock();
                }
            }

            @Override
            public void clear() {
                InternalConcurrentMap.this.clear(); // implicit distribution
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

                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    public V next() {
                        next = new WriteThroughEntry<>(iterator.next(), InternalConcurrentMap.this);
                        return next.getValue();
                    }

                    @Override
                    public void remove() {
                        synchronizationLock.lock();
                        try {
                            distributedCaffeine.invalidateDistributed(next.getKey());
                            iterator.remove();
                        } finally {
                            synchronizationLock.unlock();
                        }
                    }
                };
            }

            public int size() {
                return InternalConcurrentMap.this.size();
            }

            @Override
            public boolean removeAll(@NonNull Collection<?> c) {
                synchronizationLock.lock();
                try {
                    Set<K> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> c.contains(entry.getValue()))
                            .map(Entry::getKey)
                            .collect(Collectors.toSet());
                    distributedCaffeine.invalidateAllDistributed(keys);
                    return concurrentMap.values().removeAll(c);
                } finally {
                    synchronizationLock.unlock();
                }
            }

            @Override
            public boolean retainAll(@NonNull Collection<?> c) {
                synchronizationLock.lock();
                try {
                    Set<K> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> !c.contains(entry.getValue()))
                            .map(Entry::getKey)
                            .collect(Collectors.toSet());
                    distributedCaffeine.invalidateAllDistributed(keys);
                    return concurrentMap.values().retainAll(c);
                } finally {
                    synchronizationLock.unlock();
                }
            }

            @Override
            public void clear() {
                InternalConcurrentMap.this.clear(); // implicit distribution
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

                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    public Entry<K, V> next() {
                        next = new WriteThroughEntry<>(iterator.next(), InternalConcurrentMap.this);
                        return next;
                    }

                    @Override
                    public void remove() {
                        synchronizationLock.lock();
                        try {
                            distributedCaffeine.invalidateDistributed(next.getKey());
                            iterator.remove();
                        } finally {
                            synchronizationLock.unlock();
                        }
                    }
                };
            }

            public int size() {
                return InternalConcurrentMap.this.size();
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                synchronizationLock.lock();
                try {
                    Set<K> keys = concurrentMap.entrySet().stream()
                            .filter(c::contains)
                            .map(Entry::getKey)
                            .collect(Collectors.toSet());
                    distributedCaffeine.invalidateAllDistributed(keys);
                    return concurrentMap.entrySet().removeAll(c);
                } finally {
                    synchronizationLock.unlock();
                }
            }

            @Override
            public boolean retainAll(@NonNull Collection<?> c) {
                synchronizationLock.lock();
                try {
                    Set<K> keys = concurrentMap.entrySet().stream()
                            .filter(entry -> !c.contains(entry))
                            .map(Entry::getKey)
                            .collect(Collectors.toSet());
                    distributedCaffeine.invalidateAllDistributed(keys);
                    return concurrentMap.entrySet().retainAll(c);
                } finally {
                    synchronizationLock.unlock();
                }
            }

            @Override
            public void clear() {
                InternalConcurrentMap.this.clear(); // implicit distribution
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalConcurrentMap<?, ?> that = (InternalConcurrentMap<?, ?>) o;
        return concurrentMap.equals(that.concurrentMap);
    }

    @Override
    public int hashCode() {
        return concurrentMap.hashCode();
    }

    @Override
    public String toString() {
        return concurrentMap.toString();
    }

    static final class WriteThroughEntry<K, V> extends SimpleEntry<K, V> {

        private final Map<K, V> map;

        public WriteThroughEntry(Entry<? extends K, ? extends V> entry, Map<K, V> map) {
            super(entry);
            this.map = map;
        }

        @Override
        public V setValue(V value) {
            V oldValue = getValue();
            super.setValue(value);
            map.put(super.getKey(), value);
            return oldValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            WriteThroughEntry<?, ?> that = (WriteThroughEntry<?, ?>) o;
            return Objects.equals(getKey(), that.getKey())
                    && Objects.equals(getValue(), that.getValue());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getKey(), getValue());
        }
    }
}
