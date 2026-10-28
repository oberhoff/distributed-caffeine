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

import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * A value in L1, with the version the store assigned to its write (compared under the ordering rules) and the epoch
 * at which it was last confirmed (see "States and recovery": a held value is trusted only if its epoch is current).
 * <p>
 * The value and version never change; confirming only moves the epoch forward, in place, because replacing the
 * value in Caffeine would count as a write and restart its expiry.
 *
 * @param <V> the value type
 */
final class InternalValue<V> {

    private final V value;
    private final long version;
    private volatile int epoch;
    // no REPLACED event for this wrapper: replaced in L1 by a wrapper of the identical value instance (as in Caffeine),
    // or a refresh result installed by the core, which Caffeine then discards as overtaken
    private volatile boolean replacementSilent;
    // the user key of the entry holding this value: Caffeine's unbounded cache reports a removal with the key the
    // caller passed, which for a message is a lookup key holding the hash only
    private final AtomicReference<@Nullable Object> key = new AtomicReference<>();

    InternalValue(V value, long version, int epoch) {
        this.value = requireNonNull(value, "value cannot be null");
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        this.version = version;
        this.epoch = epoch;
    }

    // the user's value of a value that may be absent
    static <V> @Nullable V valueOrNull(@Nullable InternalValue<V> value) {
        return value == null ? null : value.value();
    }

    // a future of L1 as the user's: completing with null where nothing was found, as Caffeine's futures do, which its
    // types cannot express for a value type that is not nullable
    @SuppressWarnings("DataFlowIssue") // the null Caffeine's types cannot express, see above
    static <V> CompletableFuture<V> valuesOf(CompletableFuture<InternalValue<V>> future) {
        return future.thenApply(InternalValue::valueOrNull);
    }

    V value() {
        return value;
    }

    long version() {
        return version;
    }

    int epoch() {
        return epoch;
    }

    /**
     * Marks the value as confirmed at the given epoch; never moves the epoch backwards.
     *
     * @param confirmedEpoch the epoch at which the store confirmed the version
     */
    void confirm(int confirmedEpoch) {
        synchronized (this) {
            if (confirmedEpoch > epoch) {
                epoch = confirmedEpoch;
            }
        }
    }

    // binds the user key of the entry this value is held under; a lookup key binds nothing
    InternalValue<V> bind(InternalKey<?> internalKey) {
        if (internalKey.hasKey()) {
            key.set(internalKey.key());
        }
        return this;
    }

    void bindLike(InternalValue<?> other) {
        key.compareAndSet(null, other.key.get());
    }

    @Nullable Object key() {
        return key.get();
    }

    boolean isReplacementSilent() {
        return replacementSilent;
    }

    void replacementSilent(boolean silent) {
        this.replacementSilent = silent;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
