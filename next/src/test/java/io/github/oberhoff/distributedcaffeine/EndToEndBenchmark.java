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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

/**
 * The library end to end on a backend, for the performance part (VISION.md, "Performance: the baseline"): per
 * operation the latency (p50, p99) and the store calls it costs (reads and writes from the stats), the propagation
 * of a write to another instance, and the throughput of a mix on several threads. Run by an experiment per store;
 * local containers have almost no network latency, so the store calls per operation tell what a network adds.
 */
public final class EndToEndBenchmark {

    private static final Duration LONG = Duration.ofHours(1);
    private static final int OPERATIONS = 2_000;
    private static final int BATCHES = 200;
    private static final int BATCH = 100;
    // where the reads' results go, so that neither Error Prone nor the JIT takes them for unused
    @SuppressWarnings("unused")
    private static volatile Object sink;

    private final CacheBackend backend;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    // the time spent inside store calls, by every instance, for the store column
    private final AtomicLong storeNanos = new AtomicLong();
    private final AtomicInteger keys = new AtomicInteger();

    private EndToEndBenchmark(CacheBackend backend) {
        this.backend = backend;
    }

    /**
     * Runs the benchmark and prints its table.
     *
     * @param label   what is measured, for the table's title
     * @param backend the store
     * @return the table
     * @throws Exception if an operation fails
     */
    public static String run(String label, CacheBackend backend) throws Exception {
        EndToEndBenchmark benchmark = new EndToEndBenchmark(backend);
        try {
            String table = benchmark.measure(label);
            System.out.println(table);
            return table;
        } finally {
            for (AutoCloseable closeable : benchmark.closeables) {
                closeable.close();
            }
        }
    }

    private DistributedCaffeine<String, String> builder() {
        Adapter<String, String> adapter = settings -> timed(backend.open(settings));
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withCaffeine(CaffeineConfigurer::recordStats)
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10_000_000));
    }

    // the store, measured: every call's time added up (a proxy with every interface the store has, feed and scan)
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
                        storeNanos.addAndGet(System.nanoTime() - start);
                    }
                });
    }

    private DistributedLoadingCache<String, String> loading() {
        DistributedLoadingCache<String, String> cache = builder().build(key -> "loaded-" + key);
        closeables.add(cache);
        return cache;
    }

    private String fresh() {
        return "k" + keys.incrementAndGet();
    }

    private List<String> fresh(int count) {
        List<String> fresh = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            fresh.add(fresh());
        }
        return fresh;
    }

    // one row of the table: latency per call in microseconds, store calls per call on the instance measured, and the
    // time inside store calls per call (every instance's: a holder applying messages meanwhile counts too)
    private record Result(String operation, double p50, double p99, double storeCalls, double storeMicros) {
    }

    private static long storeCalls(DistributedCache<String, String> cache) {
        DistributedStats stats = cache.distributedPolicy().stats();
        return stats.storeReadCount() + stats.storeWriteCount();
    }

    private Result time(String operation, DistributedCache<String, String> on, int count, IntConsumer call) {
        for (int i = 0; i < Math.min(200, count); i++) {
            call.accept(-1 - i); // warm-up, on keys of its own
        }
        long[] nanos = new long[count];
        long before = storeCalls(on);
        long storeBefore = storeNanos.get();
        for (int i = 0; i < count; i++) {
            long start = System.nanoTime();
            call.accept(i);
            nanos[i] = System.nanoTime() - start;
        }
        double calls = (double) (storeCalls(on) - before) / count;
        double storeMicros = (storeNanos.get() - storeBefore) / 1_000.0 / count;
        Arrays.sort(nanos);
        return new Result(operation, nanos[count / 2] / 1_000.0, nanos[count * 99 / 100] / 1_000.0, calls,
                storeMicros);
    }

    private String measure(String label) throws Exception {
        DistributedLoadingCache<String, String> writer = loading();
        DistributedLoadingCache<String, String> reader = loading();
        List<Result> results = new ArrayList<>();

        // reads
        String held = fresh();
        writer.put(held, "v");
        results.add(time("get, held in L1", writer, OPERATIONS, i -> sink = writer.get(held)));
        List<String> stored = fresh(OPERATIONS + 200);
        stored.forEach(key -> writer.put(key, "v"));
        results.add(time("getIfPresent, from the store", reader, OPERATIONS,
                i -> sink = reader.getIfPresent(stored.get(i < 0 ? OPERATIONS - 1 - i : i))));
        results.add(time("get, loaded (a miss everywhere)", reader, OPERATIONS, i -> sink = reader.get(fresh())));
        List<List<String>> storedBatches = new ArrayList<>();
        for (int i = 0; i < BATCHES + 200; i++) {
            List<String> batch = fresh(BATCH);
            writer.putAll(batch.stream().collect(LinkedHashMap::new, (map, key) -> map.put(key, "v"), Map::putAll));
            storedBatches.add(batch);
        }
        results.add(time("getAll of " + BATCH + ", from the store", reader, BATCHES,
                i -> sink = reader.getAll(storedBatches.get(i < 0 ? BATCHES - 1 - i : i))));

        // writes
        results.add(time("put", writer, OPERATIONS, i -> writer.put(fresh(), "v")));
        results.add(time("putAll of " + BATCH, writer, BATCHES, i -> {
            Map<String, String> batch = new LinkedHashMap<>();
            fresh(BATCH).forEach(key -> batch.put(key, "v"));
            writer.putAll(batch);
        }));
        List<String> cold = fresh(OPERATIONS + 200);
        cold.forEach(key -> writer.put(key, "1"));
        results.add(time("compute, not held (cold)", reader, OPERATIONS,
                i -> reader.asMap().compute(cold.get(i < 0 ? OPERATIONS - 1 - i : i), (k, v) -> v + "+")));
        String hot = fresh();
        reader.asMap().compute(hot, (k, v) -> "1");
        results.add(time("compute, held (hot)", reader, OPERATIONS,
                i -> reader.asMap().compute(hot, (k, v) -> v == null ? "1" : String.valueOf(v.length()))));
        results.add(time("putIfAbsent, present (writes nothing)", reader, OPERATIONS,
                i -> reader.asMap().putIfAbsent(hot, "x")));
        List<String> doomed = fresh(OPERATIONS + 200);
        doomed.forEach(key -> writer.put(key, "v"));
        results.add(time("invalidate", writer, OPERATIONS, i -> writer.invalidate(doomed.get(i < 0 ? OPERATIONS - 1 - i
                : i))));
        results.add(time("invalidateAll", writer, 50, i -> writer.invalidateAll()));

        StringBuilder table = new StringBuilder("\n").append(label).append("\n\n");
        table.append(String.format(Locale.ROOT, "| %-38s | %9s | %9s | %11s | %13s |%n", "operation", "p50 µs",
                "p99 µs", "store calls", "store µs avg"));
        table.append("|").append("-".repeat(40)).append("|").append("-".repeat(11)).append("|")
                .append("-".repeat(11)).append("|").append("-".repeat(13)).append("|").append("-".repeat(15))
                .append("|\n");
        for (Result result : results) {
            table.append(String.format(Locale.ROOT, "| %-38s | %9.1f | %9.1f | %11.2f | %13.1f |%n",
                    result.operation(), result.p50(), result.p99(), result.storeCalls(), result.storeMicros()));
        }
        table.append(propagation(writer, reader));
        table.append(throughput(writer, reader));
        return table.toString();
    }

    // a write on one instance until the other dropped its copy (the message applied), paced so that it measures
    // the delay, not a queue
    private String propagation(DistributedCache<String, String> writer, DistributedCache<String, String> holder) {
        int count = 300;
        long[] nanos = new long[count];
        for (int i = 0; i < count; i++) {
            String key = fresh();
            writer.put(key, "old");
            sink = holder.getIfPresent(key);
            long start = System.nanoTime();
            writer.put(key, "new");
            while (holder.policy().getIfPresentQuietly(key) != null) {
                Thread.onSpinWait();
                if (System.nanoTime() - start > TimeUnit.SECONDS.toNanos(5)) {
                    throw new IllegalStateException("not propagated within 5 s");
                }
            }
            nanos[i] = System.nanoTime() - start;
        }
        Arrays.sort(nanos);
        return String.format(Locale.ROOT, "%npropagation of a put to a holder: p50 %.0f µs, p99 %.0f µs%n",
                nanos[count / 2] / 1_000.0, nanos[count * 99 / 100] / 1_000.0);
    }

    // 8 threads, 80 % gets of 1,000 held keys and 20 % puts on them, over both instances, for 5 s
    @SuppressWarnings("resource") // shut down in finally: an ExecutorService is AutoCloseable only from Java 19
    private String throughput(DistributedCache<String, String> first, DistributedCache<String, String> second)
            throws Exception {
        List<String> hotKeys = fresh(1_000);
        hotKeys.forEach(key -> first.put(key, "v"));
        hotKeys.forEach(key -> sink = second.getIfPresent(key));
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong operations = new AtomicLong();
        ExecutorService threads = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                DistributedCache<String, String> cache = t % 2 == 0 ? first : second;
                futures.add(threads.submit(() -> {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    while (running.get()) {
                        String key = hotKeys.get(random.nextInt(hotKeys.size()));
                        if (random.nextInt(10) < 8) {
                            sink = cache.getIfPresent(key);
                        } else {
                            cache.put(key, "v" + random.nextInt(100));
                        }
                        operations.incrementAndGet();
                    }
                }));
            }
            Supplier<Long> count = operations::get;
            Thread.sleep(1_000);
            long from = count.get();
            Thread.sleep(5_000);
            long done = count.get() - from;
            running.set(false);
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
            return String.format(Locale.ROOT, "throughput, 8 threads, 80 %% gets / 20 %% puts: %,d ops/s%n",
                    done / 5);
        } finally {
            threads.shutdownNow();
        }
    }
}
