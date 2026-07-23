package io.github.oberhoff.distributedcaffeine;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

class InternalValue<V> {

    private final V value;

    private InternalValue(V value) {
        this.value = requireNonNull(value);
    }

    private V getValue() {
        return value;
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
        return Optional.ofNullable(value)
                .map(InternalValue::new)
                .orElse(null);
    }

    static <V> V v(InternalValue<V> value) {
        return Optional.ofNullable(value)
                .map(InternalValue::getValue)
                .orElse(null);
    }
}
