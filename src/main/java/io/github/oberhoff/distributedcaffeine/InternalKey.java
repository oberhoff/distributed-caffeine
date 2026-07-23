package io.github.oberhoff.distributedcaffeine;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

class InternalKey<K> {

    private final K key;

    private InternalKey(K key) {
        this.key = requireNonNull(key);
    }

    private K getKey() {
        return key;
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof InternalKey<?> that && Objects.equals(this.key, that.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return key.toString();
    }

    static <K> InternalKey<K> ik(K key) {
        return Optional.ofNullable(key)
                .map(InternalKey::new)
                .orElse(null);
    }

    static <K> K k(InternalKey<K> key) {
        return Optional.ofNullable(key)
                .map(InternalKey::getKey)
                .orElse(null);
    }
}
