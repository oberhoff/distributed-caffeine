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

import com.github.benmanes.caffeine.cache.Expiry;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

// The user's Expiry, on the local level's wrapped keys and values, every duration at most what is left of the store's
// maximum age since the copy was installed (CaffeineConfigurer#caffeine): a value created or replaced may live that
// long at most, a read never lengthens it beyond.
final class InternalExpiry<K, V> implements Expiry<InternalKey<K>, InternalValue<V>> {

    private final Expiry<? super K, ? super V> expiry;
    private final long maximumAge;

    InternalExpiry(Expiry<? super K, ? super V> expiry, Duration maximumAge) {
        this.expiry = requireNonNull(expiry);
        this.maximumAge = maximumAge.toNanos();
    }

    @Override
    public long expireAfterCreate(InternalKey<K> key, InternalValue<V> value, long currentTime) {
        return installed(value, currentTime, expiry.expireAfterCreate(key.key(), value.value(), currentTime));
    }

    @Override
    public long expireAfterUpdate(InternalKey<K> key, InternalValue<V> value, long currentTime, long currentDuration) {
        return installed(value, currentTime,
                expiry.expireAfterUpdate(key.key(), value.value(), currentTime, currentDuration));
    }

    @Override
    public long expireAfterRead(InternalKey<K> key, InternalValue<V> value, long currentTime, long currentDuration) {
        long left = Math.max(0, value.latestExpiry() - currentTime);
        return Math.min(left, expiry.expireAfterRead(key.key(), value.value(), currentTime, currentDuration));
    }

    // a copy installed now: it goes at the store's maximum age at the latest
    private long installed(InternalValue<V> value, long currentTime, long duration) {
        long latest = currentTime + maximumAge;
        value.latestExpiry(latest < currentTime ? Long.MAX_VALUE : latest); // saturated
        return Math.min(duration, maximumAge);
    }
}
