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
import com.github.benmanes.caffeine.cache.Ticker;
import org.jspecify.annotations.Nullable;

import java.time.Duration;

/**
 * The expiry of the local level (CaffeineConfigurer#caffeine), on its wrapped keys and values: the user's Expiry, or
 * else the fixed durations of expireAfterWrite and expireAfterAccess, which Caffeine cannot combine with a variable
 * expiry and which are therefore emulated here (InternalPolicy offers them as Caffeine's fixed policies would). Either
 * way, a copy goes at the store's maximum age after the write of the row it was read from at the latest: its install
 * plus what was left of that age then (InternalValue#age). Counted from the install alone, a copy of a row read when
 * already old would live up to twice the maximum age after the write, and with DistributionMode.NONE, or a message lost
 * unnoticed, that copy is served all along. A read never lengthens a copy beyond. The bound is late by the time between
 * the store's read and the install, a round trip at most.
 *
 * @author Andreas Oberhoff
 */
final class InternalExpiry<K, V> implements Expiry<InternalKey<K>, InternalValue<V>> {

    // no fixed duration set
    static final long NONE = -1;

    private final @Nullable Expiry<? super K, ? super V> expiry;
    private final long maximumAge;
    private final Ticker ticker;
    private final long configuredAfterWrite;
    private final long configuredAfterAccess;
    // the fixed durations, changed through InternalPolicy (as Caffeine's fixed policies change theirs) and reset for
    // every cache built (see reset)
    private volatile long afterWrite;
    private volatile long afterAccess;

    // the user's Expiry
    InternalExpiry(Expiry<? super K, ? super V> expiry, Duration maximumAge, Ticker ticker) {
        this(expiry, maximumAge, ticker, NONE, NONE);
    }

    // the fixed durations (expireAfterAccess NONE: not set), expireAfterWrite lowered to the maximum age
    InternalExpiry(Duration maximumAge, Ticker ticker, long afterWrite, long afterAccess) {
        this(null, maximumAge, ticker, afterWrite, afterAccess);
    }

    private InternalExpiry(@Nullable Expiry<? super K, ? super V> expiry, Duration maximumAge, Ticker ticker,
                           long afterWrite, long afterAccess) {
        this.expiry = expiry;
        this.maximumAge = maximumAge.toNanos();
        this.ticker = ticker;
        this.configuredAfterWrite = afterWrite == NONE ? NONE : Math.min(afterWrite, this.maximumAge);
        this.configuredAfterAccess = afterAccess;
        reset();
    }

    // a cache built (again) starts with the durations configured, as one Caffeine builds does
    void reset() {
        afterWrite = configuredAfterWrite;
        afterAccess = configuredAfterAccess;
    }

    // whether the user's Expiry decides, rather than the fixed durations
    boolean isVariable() {
        return expiry != null;
    }

    long afterWrite() {
        return afterWrite;
    }

    long afterAccess() {
        return afterAccess;
    }

    // as Caffeine's FixedExpiration#setExpiresAfter: the held copies follow it (see InternalPolicy); expireAfterWrite
    // lowered to the maximum age, as configured
    void afterWrite(long nanos) {
        afterWrite = Math.min(nanos, maximumAge);
    }

    void afterAccess(long nanos) {
        afterAccess = nanos;
    }

    long now() {
        return ticker.read();
    }

    @Override
    public long expireAfterCreate(InternalKey<K> key, InternalValue<V> value, long currentTime) {
        installed(value, currentTime);
        return expiry == null ? left(value, currentTime)
                : capped(value, currentTime, expiry.expireAfterCreate(key.key(), value.value(), currentTime));
    }

    @Override
    public long expireAfterUpdate(InternalKey<K> key, InternalValue<V> value, long currentTime, long currentDuration) {
        installed(value, currentTime);
        return expiry == null ? left(value, currentTime) : capped(value, currentTime,
                expiry.expireAfterUpdate(key.key(), value.value(), currentTime, currentDuration));
    }

    @Override
    public long expireAfterRead(InternalKey<K> key, InternalValue<V> value, long currentTime, long currentDuration) {
        if (expiry != null) {
            return capped(value, currentTime,
                    expiry.expireAfterRead(key.key(), value.value(), currentTime, currentDuration));
        }
        if (afterAccess != NONE) {
            value.accessed(currentTime);
        }
        return left(value, currentTime);
    }

    // a duration from now, at most until the copy must go
    long capped(InternalValue<V> value, long currentTime, long duration) {
        return Math.min(duration, from(currentTime, value.latestExpiry()));
    }

    // what is left of a copy under the fixed durations, from now: Caffeine's expireAfterWrite counts from the install,
    // its expireAfterAccess from the last read or install, and the earlier one applies
    long left(InternalValue<V> value, long currentTime) {
        long until = value.latestExpiry();
        long write = afterWrite;
        if (write != NONE) {
            until = Math.min(until, plus(value.installedAt(), write));
        }
        long access = afterAccess;
        if (access != NONE) {
            until = Math.min(until, plus(value.accessedAt(), access));
        }
        return from(currentTime, until);
    }

    // a copy installed now: it goes at the store's maximum age after its row's write at the latest
    private void installed(InternalValue<V> value, long currentTime) {
        value.installed(currentTime);
        value.latestExpiry(plus(currentTime, Math.max(0, maximumAge - value.age())));
    }

    // the nanoseconds from now until then (none if passed); a saturated time stays unbounded, as Caffeine's ticker may
    // read negative, which would overflow the difference
    private static long from(long currentTime, long until) {
        return until == Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0, until - currentTime);
    }

    // a duration in nanoseconds, saturated as Caffeine does
    static long nanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return duration.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
    }

    // saturated, as an unbounded maximum age is Long.MAX_VALUE nanoseconds
    private static long plus(long ticks, long nanos) {
        long sum = ticks + nanos;
        return sum < ticks ? Long.MAX_VALUE : sum;
    }
}
