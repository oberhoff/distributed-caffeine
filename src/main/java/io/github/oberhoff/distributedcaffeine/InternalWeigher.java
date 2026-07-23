package io.github.oberhoff.distributedcaffeine;

import com.github.benmanes.caffeine.cache.Weigher;

import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static java.util.Objects.requireNonNull;

class InternalWeigher<K, V> implements Weigher<InternalKey<K>, InternalValue<V>> {

    private final Weigher<K, V> weigher;

    InternalWeigher(Weigher<K, V> weigher) {
        this.weigher = requireNonNull(weigher);
    }

    @Override
    public int weigh(InternalKey<K> key, InternalValue<V> value) {
        return weigher.weigh(k(key), v(value));
    }
}
