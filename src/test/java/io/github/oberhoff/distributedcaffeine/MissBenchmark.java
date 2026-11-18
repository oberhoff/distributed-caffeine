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

import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What a miss costs, split into its parts, for write-behind of load results (VISION.md, "Write-behind for load
 * results"): a miss everywhere reads the store, calls the loader, and writes the loaded value conditionally before it
 * returns. Write-behind would return before that write, so the write's share of a miss is the most it can save. One
 * instance, one thread, fresh keys; the loader spinning for a set time (sleeping would add the timer's granularity),
 * small values and values of 1 KB. Run by an experiment per store.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
public final class MissBenchmark {

    private static final Duration LONG = Duration.ofHours(1);
    private static final int MISSES = 1_000;
    private static final int WARM_UP = 200;
    private static final long[] LOADER_MICROS = {0, 1_000, 5_000};
    private static final int[] VALUE_SIZES = {3, 1_024};
    // where the reads' results go, so that neither Error Prone nor the JIT takes them for unused
    @SuppressWarnings("unused")
    private static volatile @Nullable Object sink;

    private final CacheBackend backend;
    private final AtomicLong readNanos = new AtomicLong();
    private final AtomicLong writeNanos = new AtomicLong();
    private final AtomicLong writes = new AtomicLong();
    private long keys;

    private MissBenchmark(CacheBackend backend) {
        this.backend = backend;
    }

    /**
     * Runs the benchmark and prints its table.
     *
     * @param label   what is measured, for the table's title
     * @param backend the store
     */
    public static void run(String label, CacheBackend backend) {
        System.out.println(new MissBenchmark(backend).measure(label));
    }

    private String measure(String label) {
        StringBuilder table = new StringBuilder("\n").append(label).append(", a miss everywhere, split\n\n");
        table.append(String.format(Locale.ROOT, "| %6s | %9s | %8s | %8s | %8s | %9s | %9s | %10s | %10s | %11s |%n",
                "value", "loader µs", "p50 µs", "p99 µs", "mean µs", "read µs", "write µs", "writes/miss",
                "write share", "p50 without"));
        table.append(String.format(Locale.ROOT, "|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%n", "-".repeat(8), "-".repeat(11),
                "-".repeat(10), "-".repeat(10), "-".repeat(10), "-".repeat(11), "-".repeat(11), "-".repeat(12),
                "-".repeat(12), "-".repeat(13)));
        for (int size : VALUE_SIZES) {
            for (long loaderMicros : LOADER_MICROS) {
                table.append(row(size, loaderMicros));
            }
        }
        return table.toString();
    }

    private String row(int size, long loaderMicros) {
        String value = "v".repeat(size);
        long loaderNanos = loaderMicros * 1_000;
        try (DistributedLoadingCache<String, String> cache = builder().build(key -> {
            long start = System.nanoTime();
            while (System.nanoTime() - start < loaderNanos) {
                Thread.onSpinWait();
            }
            return value;
        })) {
            for (int i = 0; i < WARM_UP; i++) {
                sink = cache.get(fresh());
            }
            long[] nanos = new long[MISSES];
            long readNanosBefore = readNanos.get();
            long writeNanosBefore = writeNanos.get();
            long writesBefore = writes.get();
            for (int i = 0; i < MISSES; i++) {
                String key = fresh();
                long start = System.nanoTime();
                sink = cache.get(key);
                nanos[i] = System.nanoTime() - start;
            }
            double read = (readNanos.get() - readNanosBefore) / 1_000.0 / MISSES;
            double write = (writeNanos.get() - writeNanosBefore) / 1_000.0 / MISSES;
            double writesPerMiss = (double) (writes.get() - writesBefore) / MISSES;
            double mean = Arrays.stream(nanos).average().orElse(0) / 1_000.0;
            Arrays.sort(nanos);
            double p50 = nanos[MISSES / 2] / 1_000.0;
            return String.format(Locale.ROOT,
                    "| %6s | %9d | %8.1f | %8.1f | %8.1f | %9.1f | %9.1f | %10.2f | %9.1f %% | %11.1f |%n",
                    size < 1_024 ? size + " B" : size / 1_024 + " KB", loaderMicros, p50,
                    nanos[MISSES * 99 / 100] / 1_000.0, mean, read, write, writesPerMiss, 100 * write / mean,
                    p50 - write);
        }
    }

    private String fresh() {
        return "m" + ++keys;
    }

    private DistributedCaffeine<String, String> builder() {
        Adapter<String, String> adapter = settings -> timed(backend.open(settings));
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10_000_000));
    }

    // the store, measured: the time of its reads and writes added up (a proxy with every interface the store has)
    private Store timed(Store store) {
        Set<Class<?>> interfaces = new LinkedHashSet<>();
        for (Class<?> type = store.getClass(); type != null; type = type.getSuperclass()) {
            interfaces.addAll(List.of(type.getInterfaces()));
        }
        return (Store) Proxy.newProxyInstance(Store.class.getClassLoader(), interfaces.toArray(new Class<?>[0]),
                (proxy, method, arguments) -> {
                    long start = System.nanoTime();
                    try {
                        return method.invoke(store, arguments);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    } finally {
                        long elapsed = System.nanoTime() - start;
                        if (method.getName().equals("read")) {
                            readNanos.addAndGet(elapsed);
                        } else if (method.getName().equals("write")) {
                            writeNanos.addAndGet(elapsed);
                            writes.incrementAndGet();
                        }
                    }
                });
    }
}
