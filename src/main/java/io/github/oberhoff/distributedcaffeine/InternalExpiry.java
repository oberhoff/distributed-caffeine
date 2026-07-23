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
