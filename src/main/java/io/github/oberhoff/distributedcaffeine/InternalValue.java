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

import java.util.Objects;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalValue<V> {

    private final V value;
    // the operation of the write this value came from, kept for one purpose only: recognising the cache entry
    // written for it when it is delivered back, so that what is already held is not written a second time. How
    // that write is ordered against the others is deliberately not kept here but remembered by the cache instance
    // that published it (see InternalCacheManager), because a value is gone as soon as it is invalidated, evicted
    // or overwritten, while the ordering has to outlive exactly that
    private @Nullable String operation;
    // the activation this value became content of. Since activating renews the identifier,
    // everything held from before stops being of the current activation without a single value having to be
    // touched - which decides both whether a change to it may be distributed and whether synchronizing keeps it
    private @Nullable String activationId;

    private InternalValue(V value) {
        this.value = requireNonNull(value);
    }

    private V getValue() {
        return value;
    }

    @Nullable String getOperation() {
        return operation;
    }

    InternalValue<V> setOperation(@Nullable String operation) {
        this.operation = operation;
        return this;
    }

    @Nullable String getActivationId() {
        return activationId;
    }

    @SuppressWarnings("UnusedReturnValue")
    InternalValue<V> setActivationId(@Nullable String activationId) {
        this.activationId = activationId;
        return this;
    }

    @Override
    public boolean equals(@Nullable Object object) {
        return object instanceof InternalValue<?> that && Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value.toString();
    }

    static <V> InternalValue<V> iv(V value) {
        return new InternalValue<>(value);
    }

    static <V> @Nullable InternalValue<V> ivn(@Nullable V value) {
        return nonNull(value)
                ? new InternalValue<>(value)
                : null;
    }

    static <V> V v(InternalValue<V> value) {
        return value.getValue();
    }

    static <V> @Nullable V vn(@Nullable InternalValue<V> value) {
        return nonNull(value)
                ? value.getValue()
                : null;
    }
}
