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
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.Policy.CacheEntry;
import com.github.benmanes.caffeine.cache.Policy.VarExpiration;
import com.github.benmanes.caffeine.cache.Ticker;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

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
    private final long configuredMaximumAge;
    private final long configuredUnconfirmedMaximumAge;
    private final Ticker ticker;
    private final long configuredAfterWrite;
    private final long configuredAfterAccess;
    // the store's maximum age, changed through the distributed policy (InternalCore#maximumAge), and the fixed
    // durations, changed through InternalPolicy (as Caffeine's fixed policies change theirs); all reset for every cache
    // built (see reset). expireAfterWrite is kept as set and lowered to the maximum age where it is read, so that a
    // longer maximum age set later lifts it again, up to what was set
    private volatile long maximumAge;
    // the store's maximum age for copies no store confirmed (InternalVersion.UNCONFIRMED: loaded from the source while
    // the store did not answer): the same as maximumAge, except with the maximum age for the store only, which leaves
    // the other copies unbounded by it. Their staleness is bounded by messages, which a copy no store confirmed was
    // never checked against; a cache that does not reach its store, started without it, would hold them for good
    // otherwise (VISION.md, "Starting without the store", design step 5)
    private volatile long unconfirmedMaximumAge;
    private volatile long afterWrite;
    private volatile long afterAccess;

    // the user's Expiry
    InternalExpiry(Expiry<? super K, ? super V> expiry, Duration maximumAge, Duration unconfirmedMaximumAge,
                   Ticker ticker) {
        this(expiry, maximumAge, unconfirmedMaximumAge, ticker, NONE, NONE);
    }

    // the fixed durations (expireAfterAccess NONE: not set), expireAfterWrite lowered to the maximum age
    // (Long.MAX_VALUE: the maximum age, following it)
    InternalExpiry(Duration maximumAge, Duration unconfirmedMaximumAge, Ticker ticker, long afterWrite,
                   long afterAccess) {
        this(null, maximumAge, unconfirmedMaximumAge, ticker, afterWrite, afterAccess);
    }

    private InternalExpiry(@Nullable Expiry<? super K, ? super V> expiry, Duration maximumAge,
                           Duration unconfirmedMaximumAge, Ticker ticker, long afterWrite, long afterAccess) {
        this.expiry = expiry;
        this.configuredMaximumAge = maximumAge.toNanos();
        this.configuredUnconfirmedMaximumAge = unconfirmedMaximumAge.toNanos();
        this.ticker = ticker;
        this.configuredAfterWrite = afterWrite;
        this.configuredAfterAccess = afterAccess;
        reset();
    }

    // a cache built (again) starts with the durations configured, as one Caffeine builds does
    void reset() {
        maximumAge = configuredMaximumAge;
        unconfirmedMaximumAge = configuredUnconfirmedMaximumAge;
        afterWrite = configuredAfterWrite;
        afterAccess = configuredAfterAccess;
    }

    // whether the user's Expiry decides, rather than the fixed durations
    boolean isVariable() {
        return expiry != null;
    }

    // expireAfterWrite as it applies: lowered to the maximum age
    long afterWrite() {
        long write = afterWrite;
        return write == NONE ? NONE : Math.min(write, maximumAge);
    }

    long afterAccess() {
        return afterAccess;
    }

    // as Caffeine's FixedExpiration#setExpiresAfter: the held copies follow it (reapply)
    void afterWrite(long nanos) {
        afterWrite = nanos;
    }

    void afterAccess(long nanos) {
        afterAccess = nanos;
    }

    // the store's maximum age set at runtime (InternalCore#maximumAge): the held copies follow it (reapply)
    void maximumAge(long nanos) {
        maximumAge = nanos;
        unconfirmedMaximumAge = nanos;
    }

    // the same with the maximum age for the store only: only the copies no store confirmed follow it
    void unconfirmedMaximumAge(long nanos) {
        unconfirmedMaximumAge = nanos;
    }

    // every held copy's expiry computed again, after a duration or the maximum age changed, as Caffeine's fixed policy
    // applies a new duration to the held copies at once. Each computation reads the durations and the age as they are
    // then, so concurrent changes end with the last one applied. A copy replaced meanwhile computed its own already,
    // but this may have overwritten it with its predecessor's: done again for the new one. Under the user's Expiry,
    // whose durations cannot be computed again, a copy keeps what is left of it, lowered to its new latest expiry: a
    // longer maximum age lengthens no copy held, only those installed after
    void reapply(Policy<InternalKey<K>, InternalValue<V>> policy) {
        VarExpiration<InternalKey<K>, InternalValue<V>> variable = policy.expireVariably().orElseThrow();
        for (InternalKey<K> internalKey : variable.oldest(stream -> stream.map(CacheEntry::getKey).toList())) {
            InternalValue<V> held = policy.getIfPresentQuietly(internalKey);
            while (held != null) {
                long currentTime = now();
                if (expiry == null) {
                    variable.setExpiresAfter(internalKey, left(held, currentTime), TimeUnit.NANOSECONDS);
                } else {
                    OptionalLong remaining = variable.getExpiresAfter(internalKey, TimeUnit.NANOSECONDS);
                    if (remaining.isPresent()) {
                        variable.setExpiresAfter(internalKey, capped(held, currentTime, remaining.getAsLong()),
                                TimeUnit.NANOSECONDS);
                    }
                }
                InternalValue<V> now = policy.getIfPresentQuietly(internalKey);
                held = now == held ? null : now;
            }
        }
    }

    long now() {
        return ticker.read();
    }

    @Override
    public long expireAfterCreate(InternalKey<K> key, InternalValue<V> value, long currentTime) {
        value.installed(currentTime);
        return expiry == null ? left(value, currentTime)
                : capped(value, currentTime, expiry.expireAfterCreate(key.key(), value.value(), currentTime));
    }

    @Override
    public long expireAfterUpdate(InternalKey<K> key, InternalValue<V> value, long currentTime, long currentDuration) {
        value.installed(currentTime);
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
        return Math.min(duration, from(currentTime, latestExpiry(value)));
    }

    // what is left of a copy under the fixed durations, from now: Caffeine's expireAfterWrite counts from the install,
    // its expireAfterAccess from the last read or install, and the earlier one applies
    long left(InternalValue<V> value, long currentTime) {
        long until = latestExpiry(value);
        long write = afterWrite();
        if (write != NONE) {
            until = Math.min(until, plus(value.installedAt(), write));
        }
        long access = afterAccess;
        if (access != NONE) {
            until = Math.min(until, plus(value.accessedAt(), access));
        }
        return from(currentTime, until);
    }

    // when a copy must go at the latest: the store's maximum age after its row's write, that is its install plus what
    // was left of the age then. Computed from the age as it is now, not kept from the install, so that a maximum age
    // set later applies to the copies held too
    private long latestExpiry(InternalValue<V> value) {
        long age = value.version() == InternalVersion.UNCONFIRMED ? Math.min(maximumAge, unconfirmedMaximumAge)
                : maximumAge;
        return plus(value.installedAt(), Math.max(0, age - value.age()));
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
