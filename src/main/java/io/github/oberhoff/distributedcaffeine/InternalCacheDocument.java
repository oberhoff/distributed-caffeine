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

import org.bson.types.ObjectId;
import org.jspecify.annotations.NonNull;

import java.lang.ref.WeakReference;
import java.util.Comparator;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.isNull;

class InternalCacheDocument<K, V> implements DistributedPolicy.CacheEntry<K, V>, Comparable<InternalCacheDocument<K, V>> {

    static final String ID = "_id";
    static final String HASH = "hash";
    static final String KEY = "key";
    static final String VALUE = "value";
    static final String STATUS = "status";
    static final String TOUCHED = "touched";
    static final String EXPIRES = "expires";

    static final String CACHED = "cached";
    static final String INVALIDATED = "invalidated";
    static final String EVICTED = "evicted";
    static final String ORPHANED = "orphaned";

    private ObjectId id;
    private Integer hash;
    private K key;
    private WeakReference<K> weakKey;
    private V value;
    private WeakReference<V> weakValue;
    private String status;
    private Date touched;
    private Date expires;

    @Override
    public ObjectId getId() {
        return id;
    }

    InternalCacheDocument<K, V> setId(ObjectId id) {
        this.id = id;
        return this;
    }

    @SuppressWarnings("unused")
    Integer getHash() {
        return hash;
    }

    InternalCacheDocument<K, V> setHash(Integer hash) {
        this.hash = hash;
        return this;
    }

    @Override
    public K getKey() {
        return Stream.of(key, Optional.ofNullable(weakKey)
                        .map(WeakReference::get)
                        .orElse(null))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    InternalCacheDocument<K, V> setKey(K key) {
        this.key = key;
        weakKey = new WeakReference<>(key);
        return this;
    }

    @Override
    public V getValue() {
        return Stream.of(value, Optional.ofNullable(weakValue)
                        .map(WeakReference::get)
                        .orElse(null))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    InternalCacheDocument<K, V> setValue(V value) {
        this.value = value;
        weakValue = new WeakReference<>(value);
        return this;
    }

    @Override
    public String getStatus() {
        return status;
    }

    InternalCacheDocument<K, V> setStatus(String status) {
        this.status = status;
        return this;
    }

    @Override
    public Date getTouched() {
        return touched;
    }

    InternalCacheDocument<K, V> setTouched(Date touched) {
        this.touched = touched;
        return this;
    }

    @SuppressWarnings("unused")
    Date getExpires() {
        return expires;
    }

    InternalCacheDocument<K, V> setExpires(Date expires) {
        this.expires = expires;
        return this;
    }

    boolean isCached() {
        return CACHED.equals(status);
    }

    boolean isInvalidated() {
        return INVALIDATED.equals(status);
    }

    @Override
    public boolean isEvicted() {
        return EVICTED.equals(status);
    }

    boolean isOrphaned() {
        return ORPHANED.equals(status);
    }

    boolean isNewer(InternalCacheDocument<K, V> cacheDocument) {
        return isNull(cacheDocument) || compareTo(cacheDocument) > 0;
    }

    boolean hasOrigin(String origin) {
        return id.toHexString().substring(8, 18).equals(origin);
    }

    InternalCacheDocument<K, V> weakened() {
        this.key = null;
        this.value = null;
        return this;
    }

    @Override
    public int compareTo(@NonNull InternalCacheDocument<K, V> that) {
        // unique object id is used as tie-breaker if touched is equal
        return Comparator.<InternalCacheDocument<K, V>, Date>comparing(InternalCacheDocument::getTouched)
                .thenComparing(InternalCacheDocument::getId)
                .compare(this, that);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalCacheDocument<?, ?> that = (InternalCacheDocument<?, ?>) o;
        return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return format("CacheDocument{id=%s, hash=%s, key=%s, value=%s, status='%s', touched=%s, expires=%s}",
                id, hash, key, value, status, touched, expires);
    }
}
