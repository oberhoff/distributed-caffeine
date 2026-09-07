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

import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import org.jspecify.annotations.Nullable;

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
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

class InternalUtils {

    private InternalUtils() {
    }

    // the value type carries whatever nullness the caller passes in, instead of forcing a nullable one on callers
    // that hand over a value which cannot be null
    static <K, V extends @Nullable Object> Entry<K, V> entry(K key, V value) {
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

    static <K, V> Map<InternalKey<K>, InternalValue<V>> im(Map<? extends K, ? extends V> map) {
        return map.entrySet().stream()
                .collect(toUnmodifiableMap(entry -> ik(entry.getKey()), entry -> iv(entry.getValue())));
    }

    static <K, V> Map<K, V> m(Map<InternalKey<K>, InternalValue<V>> map) {
        return map.entrySet().stream()
                .collect(toUnmodifiableMap(entry -> k(entry.getKey()), entry -> v(entry.getValue())));
    }

    static <T> Set<T> requireNonNullIterable(Iterable<? extends T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false)
                .collect(toUnmodifiableSet());
    }

    @SuppressWarnings("UnusedReturnValue")
    static <K, V> Map<K, V> requireNonNullMap(Map<K, V> map) {
        return Map.copyOf(map);
    }

    static void runFailable(FailableRunnable failableRunnable) {
        FailableNullableSupplier<?> failableNullableSupplier = () -> {
            failableRunnable.run();
            return null;
        };
        getFailableOrNull(failableNullableSupplier);
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

    static <T> @Nullable T getFailableOrNull(FailableNullableSupplier<T> failableNullableSupplier) {
        return getFailableOrNull(failableNullableSupplier, RuntimeException::new);
    }

    static <T> @Nullable T getFailableOrNull(FailableNullableSupplier<T> failableNullableSupplier,
                                             Function<Throwable, RuntimeException> runtimeExceptionFactory) {
        try {
            return failableNullableSupplier.get();
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw runtimeExceptionFactory.apply(t);
        }
    }

    // for the paths a configured persistence tier already implies: an adapter that retains nothing is rejected at
    // build time as soon as any tier is configured, so reaching this means that check was bypassed rather than that
    // a cache instance is legitimately running without a repository
    static <K, V> Repository<K, V> requireRepository(@Nullable Repository<K, V> repository, String identifier) {
        if (isNull(repository)) {
            throw new IllegalStateException(format("The adapter for cache at '%s' retains nothing, so there is "
                    .concat("no repository to read from or to maintain"), identifier));
        }
        return repository;
    }

    static <T> @Nullable T nullable(@Nullable T nullable) {
        @SuppressWarnings("UnnecessaryLocalVariable")
        T workaround = nullable;
        return workaround;
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

    @FunctionalInterface
    interface FailableNullableSupplier<T> {

        @SuppressWarnings("java:S112")
        @Nullable T get() throws Throwable;
    }
}
