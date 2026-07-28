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

import java.util.Objects;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

class InternalValue<V> {

    private final V value;
    private Integer operation;

    private InternalValue(V value) {
        this.value = requireNonNull(value);
    }

    private V getValue() {
        return value;
    }

    Integer getOperation() {
        return operation;
    }

    InternalValue<V> setOperation(Integer operation) {
        this.operation = operation;
        return this;
    }

    @Override
    public boolean equals(Object object) {
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
        return nonNull(value)
                ? new InternalValue<>(value)
                : null;
    }

    static <V> V v(InternalValue<V> value) {
        return nonNull(value)
                ? value.getValue()
                : null;
    }
}
