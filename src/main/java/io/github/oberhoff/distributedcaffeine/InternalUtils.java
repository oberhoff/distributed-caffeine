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

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static io.github.oberhoff.distributedcaffeine.InternalKey.ik;
import static io.github.oberhoff.distributedcaffeine.InternalKey.k;
import static io.github.oberhoff.distributedcaffeine.InternalValue.iv;
import static io.github.oberhoff.distributedcaffeine.InternalValue.v;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

class InternalUtils {

    static <K, V> Entry<K, V> entry(K key, V value) {
        return new SimpleEntry<>(key, value);
    }

    static <K> Set<InternalKey<K>> iks(Collection<K> keys) {
        return keys.stream().map(InternalKey::ik)
                .collect(toUnmodifiableSet());
    }

    static <K> Set<K> s(Collection<? extends InternalKey<K>> keys) {
        return keys.stream().map(InternalKey::k)
                .collect(toUnmodifiableSet());
    }

    static <V> Set<InternalValue<V>> ivs(Collection<V> keys) {
        return keys.stream().map(InternalValue::iv)
                .collect(toUnmodifiableSet());
    }

    static <K, V> Map<InternalKey<K>, InternalValue<V>> im(Map<? extends K, ? extends V> map) {
        return map.entrySet().stream()
                .collect(toUnmodifiableMap(entry -> ik(entry.getKey()), entry -> iv(entry.getValue())));
    }

    static <K, V> Map<K, V> m(Map<InternalKey<K>, InternalValue<V>> map) {
        return map.entrySet().stream()
                .collect(toUnmodifiableMap(entry -> k(entry.getKey()), entry -> v(entry.getValue())));
    }

    static void requireNonNullOnCondition(boolean condition, Object object, String message) {
        if (condition) {
            requireNonNull(object, message);
        }
    }

    static <T> Set<T> requireNonNullIterable(Iterable<? extends T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false)
                .collect(toUnmodifiableSet());
    }

    static <K, V> Map<K, V> requireNonNullMap(Map<K, V> map) {
        return Map.copyOf(map);
    }

    static void runFailable(FailableRunnable failableRunnable) {
        getFailable(() -> {
            failableRunnable.run();
            return null;
        });
    }

    static <T> T getFailable(FailableSupplier<T> failableSupplier) {
        return getFailable(failableSupplier, RuntimeException::new);
    }

    static <T> T getFailable(FailableSupplier<T> failableSupplier,
                             Function<Throwable, RuntimeException> runtimeExceptionFactory) {
        try {
            return failableSupplier.get();
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw runtimeExceptionFactory.apply(t);
        }
    }

    @FunctionalInterface
    interface FailableRunnable {

        @SuppressWarnings("java:S112")
        void run() throws Throwable;
    }

    @FunctionalInterface
    interface FailableSupplier<T> {

        @SuppressWarnings("java:S112")
        T get() throws Throwable;
    }
}
