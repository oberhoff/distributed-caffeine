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
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.EXPIRES;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.HASH;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.KEY;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.ORIGIN;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.STALE;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.STATUS;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.TOUCHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.VALUE;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field._ID;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireNonNullOnCondition;
import static java.lang.String.format;
import static java.util.Objects.isNull;

class InternalCacheDocument<K, V> implements Comparable<InternalCacheDocument<K, V>> {

    @SuppressWarnings("squid:S115")
    enum Field {

        _ID,
        DISCRIMINATOR,
        ORIGIN,
        HASH,
        KEY,
        VALUE,
        STATUS,
        STALE,
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
        CACHED_REFRESHED,
        CACHED_REFRESHED_AFTER_WRITE,
        INVALIDATED,
        INVALIDATED_REFRESHED,
        INVALIDATED_REFRESHED_AFTER_WRITE,
        EVICTED_SIZE,
        EVICTED_TIME,
        EVICTED_SIZE_EXTENDED,
        EVICTED_TIME_EXTENDED;

        static final Status[] CACHED_GROUP = new Status[]{CACHED, CACHED_REFRESHED, CACHED_REFRESHED_AFTER_WRITE};
        static final Status[] INVALIDATED_GROUP = new Status[]{INVALIDATED, INVALIDATED_REFRESHED,
                INVALIDATED_REFRESHED_AFTER_WRITE};
        static final Status[] EVICTED_GROUP = new Status[]{EVICTED_SIZE, EVICTED_TIME, EVICTED_SIZE_EXTENDED,
                EVICTED_TIME_EXTENDED};
        static final Status[] EVICTED_EXTENDED_GROUP = new Status[]{EVICTED_SIZE_EXTENDED, EVICTED_TIME_EXTENDED};

        private final String value;

        Status() {
            this.value = name().toLowerCase();
        }

        boolean isCached() {
            return isMemberOf(CACHED_GROUP);
        }

        boolean isInvalidated() {
            return isMemberOf(INVALIDATED_GROUP);
        }

        boolean isEvicted() {
            return isMemberOf(EVICTED_GROUP);
        }

        boolean isEvictedExtended() {
            return isMemberOf(EVICTED_EXTENDED_GROUP);
        }

        boolean isConsideredBy(DistributionMode distributionMode) {
            if (isCached()) {
                return distributionMode.isPopulationConsidered();
            } else if (isInvalidated()) {
                return distributionMode.isInvalidationConsidered();
            } else if (isEvicted()) {
                return distributionMode.isEvictionConsidered();
            } else {
                return false;
            }
        }

        private boolean isMemberOf(Status[] statuses) {
            // for-loop is fastest
            for (Status status : statuses) {
                if (status.equals(this)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return value;
        }

        static Status of(String value) {
            return Stream.of(Status.values())
                    .filter(status -> status.value.equals(value))
                    .findFirst()
                    .orElse(null); // fails on validation
        }
    }

    private ObjectId id;
    private Integer hash;
    private String discriminator;
    private Long origin;
    private K key;
    private WeakReference<K> weakKey;
    private V value;
    private WeakReference<V> weakValue;
    private Status status;
    private Boolean stale;
    private Instant touched;
    private Instant expires;

    private final Instant constructed = Instant.now();

    ObjectId getId() {
        return id;
    }

    InternalCacheDocument<K, V> setId(ObjectId id) {
        this.id = id;
        return this;
    }

    public Long getOrigin() {
        return origin;
    }

    public String getDiscriminator() {
        return discriminator;
    }

    public InternalCacheDocument<K, V> setDiscriminator(String discriminator) {
        this.discriminator = discriminator;
        return this;
    }

    public InternalCacheDocument<K, V> setOrigin(Long origin) {
        this.origin = origin;
        return this;
    }

    Integer getHash() {
        return hash;
    }

    InternalCacheDocument<K, V> setHash(Integer hash) {
        this.hash = hash;
        return this;
    }

    K getKey() {
        return Optional.ofNullable(key)
                .or(() -> Optional.ofNullable(weakKey)
                        .map(WeakReference::get))
                .orElse(null);
    }

    InternalCacheDocument<K, V> setKey(K key) {
        this.key = key;
        weakKey = new WeakReference<>(key);
        return this;
    }

    V getValue() {
        return Optional.ofNullable(value)
                .or(() -> Optional.ofNullable(weakValue)
                        .map(WeakReference::get))
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

    public Boolean isStale() {
        return stale;
    }

    public InternalCacheDocument<K, V> setStale(Boolean stale) {
        this.stale = stale;
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
        return status.isCached();
    }

    boolean isInvalidated() {
        return status.isInvalidated();
    }

    boolean isEvicted() {
        return status.isEvicted();
    }

    boolean isEvictedExtended() {
        return status.isEvictedExtended();
    }

    boolean isNewer(InternalCacheDocument<K, V> cacheDocument) {
        return isNull(cacheDocument) || compareTo(cacheDocument) > 0;
    }

    boolean isOlderThan(Duration duration) {
        return constructed.plus(duration).isBefore(Instant.now());
    }

    boolean isOriginIndependent() {
        return isEvicted()
                || status.equals(CACHED_REFRESHED_AFTER_WRITE)
                || status.equals(Status.INVALIDATED_REFRESHED_AFTER_WRITE);
    }

    boolean hasSameOrigin(Long origin) {
        return this.origin.equals(origin);
    }

    InternalCacheDocument<K, V> weakened() {
        this.key = null;
        this.value = null;
        return this;
    }

    InternalCacheDocument<K, V> validate(Field... fields) {
        Set<Field> validationFields = fields.length == 0
                ? Set.of(Field.values())
                : Set.of(fields);
        requireNonNullOnCondition(validationFields.contains(_ID), id, "id cannot be null");
        // discriminator can be null
        requireNonNullOnCondition(validationFields.contains(ORIGIN), origin, "origin cannot be null");
        requireNonNullOnCondition(validationFields.contains(HASH), hash, "hash cannot be null");
        requireNonNullOnCondition(validationFields.contains(KEY), key, "key cannot be null");
        requireNonNullOnCondition(validationFields.contains(VALUE) && !status.isInvalidated(), value,
                "value cannot be null");
        requireNonNullOnCondition(validationFields.contains(STATUS), status, "status cannot be null");
        requireNonNullOnCondition(validationFields.contains(STALE), stale, "stale cannot be null");
        requireNonNullOnCondition(validationFields.contains(TOUCHED), touched, "touched cannot be null");
        requireNonNullOnCondition(validationFields.contains(EXPIRES) && !status.isCached(), expires,
                "expires cannot be null");
        return this;
    }

    @Override
    public int compareTo(@NonNull InternalCacheDocument<K, V> that) {
        return Comparator.<InternalCacheDocument<K, V>, Instant>comparing(InternalCacheDocument::getTouched)
                // unique id is used as tie-breaker if touched is equal
                .thenComparing(InternalCacheDocument::getId)
                .compare(this, that);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        InternalCacheDocument<?, ?> that = (InternalCacheDocument<?, ?>) object;
        return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return format("CacheDocument{id=%s, origin=%s hash=%s, key=%s, value=%s, "
                        .concat("status=%s, stale=%s touched=%s, expires=%s}"),
                getId(), getOrigin(), getHash(), getKey(), getValue(),
                getStatus(), isStale(), getTouched(), getExpires());
    }
}
