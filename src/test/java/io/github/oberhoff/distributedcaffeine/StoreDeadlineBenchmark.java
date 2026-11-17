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

import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * What a deadline of the core's own for blocking store calls would cost (VISION.md, "Options for later", "A deadline
 * of the core's own"): every store call run on another thread, the caller waiting at most a deadline, as Hystrix's
 * thread isolation and Resilience4j's {@code TimeLimiter} do. The end-to-end benchmark ({@link EndToEndBenchmark}) as
 * it is, then with that hop on platform threads (a cached pool), then on virtual threads (one per call), each on a
 * fresh backend. Run by an experiment per store.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
public final class StoreDeadlineBenchmark {

    private static final Duration DEADLINE = Duration.ofSeconds(30);

    private StoreDeadlineBenchmark() {
    }

    /**
     * Opens a fresh backend.
     *
     * @author Andreas Oberhoff
     */
    public interface Backends {

        /**
         * Opens a fresh backend for one run.
         *
         * @return the backend
         * @throws Exception if it cannot be opened
         */
        CacheBackend open() throws Exception;
    }

    /**
     * Runs the end-to-end benchmark three times and prints the tables.
     *
     * @param label    what is measured, for the tables' titles
     * @param backends fresh backends of the store
     * @throws Exception if an operation fails
     */
    public static void run(String label, Backends backends) throws Exception {
        // -Ddeadline.reversed=true: the hops first, the direct run last, so that the JIT's warm-up favours neither
        if (Boolean.getBoolean("deadline.reversed")) {
            virtual(label, backends);
            platform(label, backends);
            direct(label, backends);
        } else {
            direct(label, backends);
            platform(label, backends);
            virtual(label, backends);
        }
    }

    private static void direct(String label, Backends backends) throws Exception {
        try (CacheBackend backend = backends.open()) {
            EndToEndBenchmark.run(label + ", direct", backend);
        }
    }

    private static void platform(String label, Backends backends) throws Exception {
        hopping(label + ", every store call on a platform thread, waited for with a deadline", backends,
                Executors.newCachedThreadPool());
    }

    // Java 21's, reached reflectively: the tests compile for Java 17 and run on a later JDK
    private static void virtual(String label, Backends backends) throws Exception {
        ExecutorService virtual = (ExecutorService) Executors.class.getMethod("newVirtualThreadPerTaskExecutor")
                .invoke(null);
        hopping(label + ", every store call on a virtual thread, waited for with a deadline", backends, virtual);
    }

    private static void hopping(String label, Backends backends, ExecutorService executor) throws Exception {
        try (CacheBackend backend = new Deadlined(backends.open(), executor)) {
            EndToEndBenchmark.run(label, backend);
        } finally {
            executor.shutdownNow();
        }
    }

    // a backend whose stores run every call on the executor, the caller waiting at most the deadline
    private record Deadlined(CacheBackend backend, ExecutorService executor) implements CacheBackend {

        @Override
        public Store open(StoreSettings settings) {
            return deadlined(backend.open(settings), executor);
        }

        @Override
        public boolean isIdle() {
            return backend.isIdle();
        }

        @Override
        public void induceGap() {
            backend.induceGap();
        }

        @Override
        public @Nullable InMemoryDatabase inMemory() {
            return backend.inMemory();
        }

        @Override
        public void close() {
            backend.close();
        }
    }

    // every method of every interface the store has, through the executor
    private static Store deadlined(Store store, ExecutorService executor) {
        Set<Class<?>> interfaces = new LinkedHashSet<>();
        for (Class<?> type = store.getClass(); type != null; type = type.getSuperclass()) {
            interfaces.addAll(List.of(type.getInterfaces()));
        }
        return (Store) Proxy.newProxyInstance(Store.class.getClassLoader(), interfaces.toArray(new Class<?>[0]),
                (proxy, method, arguments) -> {
                    Future<@Nullable Object> call = executor.submit(() -> {
                        try {
                            return method.invoke(store, arguments);
                        } catch (InvocationTargetException e) {
                            throw (Exception) e.getCause();
                        }
                    });
                    try {
                        return call.get(DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (ExecutionException e) {
                        throw e.getCause();
                    } catch (TimeoutException e) {
                        call.cancel(true);
                        throw new StoreException("A store call outlasted its deadline; its outcome is unknown", e,
                                true, true);
                    }
                });
    }
}
