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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

class InternalUtils {

    static <K, V> Entry<K, V> entry(K key, V value) {
        return new SimpleEntry<>(key, value);
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

    static <T> List<Set<T>> splitIntoPartitions(Collection<T> collection, int partitionSize) {
        List<T> list = List.copyOf(collection);
        return IntStream.range(0, list.size())
                .filter(i -> i % partitionSize == 0)
                .mapToObj(i -> Set.copyOf(list.subList(i, min(i + partitionSize, list.size()))))
                .toList();
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
