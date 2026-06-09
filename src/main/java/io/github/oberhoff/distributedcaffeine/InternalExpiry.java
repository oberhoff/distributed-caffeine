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

import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static java.util.Objects.requireNonNull;

class InternalExpiry<K, V> implements Expiry<InternalKey<K>, InternalValue<V>> {

    private final Expiry<K, V> expiry;

    InternalExpiry(Expiry<K, V> expiry) {
        this.expiry = requireNonNull(expiry);
    }

    @Override
    public long expireAfterCreate(InternalKey<K> key, InternalValue<V> value, long currentTime) {
        return expiry.expireAfterCreate(k(key), v(value), currentTime);
    }

    @Override
    public long expireAfterUpdate(InternalKey<K> key, InternalValue<V> value, long currentTime, long currentDuration) {
        return expiry.expireAfterUpdate(k(key), v(value), currentTime, currentDuration);
    }

    @Override
    public long expireAfterRead(InternalKey<K> key, InternalValue<V> value, long currentTime, long currentDuration) {
        return expiry.expireAfterRead(k(key), v(value), currentTime, currentDuration);
    }
}
