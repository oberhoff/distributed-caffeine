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
import java.time.Instant;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EXTENDED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.ORPHANED;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.extractOrigin;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullOnCondition;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

class InternalCacheDocument<K, V> implements Comparable<InternalCacheDocument<K, V>> {

    enum Field {

        _ID,
        HASH,
        KEY,
        VALUE,
        STATUS,
        TOUCHED,
        EXPIRES;

        private final String value;

        Field() {
            this.value = name().toLowerCase();
        }

        @Override
        public String toString() {
            return value;
        }
    }

    enum Status {

        CACHED,
        INVALIDATED,
        EVICTED,
        EXTENDED,
        ORPHANED;

        private final String value;

        Status() {
            this.value = name().toLowerCase();
        }

        @Override
        public String toString() {
            return value;
        }

        boolean matches(DistributionMode distributionMode) {
            if (this == CACHED || this == ORPHANED) {
                return distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                        || distributionMode == POPULATION_AND_INVALIDATION;
            } else if (this == INVALIDATED) {
                return distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                        || distributionMode == POPULATION_AND_INVALIDATION
                        || distributionMode == INVALIDATION_AND_EVICTION
                        || distributionMode == INVALIDATION;
            } else if (this == EVICTED || this == EXTENDED) {
                return distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                        || distributionMode == INVALIDATION_AND_EVICTION;
            } else {
                return false;
            }
        }

        static Status of(String value) {
            return Stream.of(values())
                    .filter(status -> status.toString().equals(value))
                    .findFirst()
                    .orElse(null);
        }
    }

    private ObjectId id;
    private Integer hash;
    private K key;
    private WeakReference<K> weakKey;
    private V value;
    private WeakReference<V> weakValue;
    private Status status;
    private Instant touched;
    private Instant expires;

    ObjectId getId() {
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

    K getKey() {
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

    V getValue() {
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


    Status getStatus() {
        return status;
    }

    InternalCacheDocument<K, V> setStatus(Status status) {
        this.status = status;
        return this;
    }

    Instant getTouched() {
        return touched;
    }

    InternalCacheDocument<K, V> setTouched(Instant touched) {
        this.touched = touched;
        return this;
    }

    Instant getExpires() {
        return expires;
    }

    InternalCacheDocument<K, V> setExpires(Instant expires) {
        this.expires = expires;
        return this;
    }

    boolean isCached() {
        return status == CACHED;
    }

    boolean isInvalidated() {
        return status == INVALIDATED;
    }

    boolean isEvicted() {
        return status == EVICTED;
    }

    boolean isExtended() {
        return status == EXTENDED;
    }

    boolean isOrphaned() {
        return status == ORPHANED;
    }

    boolean isNewer(InternalCacheDocument<K, V> cacheDocument) {
        return isNull(cacheDocument) || compareTo(cacheDocument) > 0;
    }

    boolean hasOrigin(String origin) {
        return extractOrigin(id).equals(origin);
    }

    InternalCacheDocument<K, V> validate() {
        requireNonNull(id, "id cannot be null");
        requireNonNull(hash, "hash cannot be null");
        requireNonNull(key, "key cannot be null");
        requireNonNullOnCondition(status != INVALIDATED, value, "value cannot be null");
        requireNonNull(status, "status cannot be null");
        requireNonNull(touched, "touched cannot be null");
        requireNonNullOnCondition(status != CACHED, expires, "expires cannot be null");
        return this;
    }

    InternalCacheDocument<K, V> weakened() {
        this.key = null;
        this.value = null;
        return this;
    }

    @Override
    public int compareTo(@NonNull InternalCacheDocument<K, V> that) {
        return Comparator.<InternalCacheDocument<K, V>, Instant>comparing(InternalCacheDocument::getTouched)
                // unique object id is used as tie-breaker if touched is equal
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
