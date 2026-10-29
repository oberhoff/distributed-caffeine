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

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
@NullMarked
public final class EndToEndBenchmark {

    private static final Duration LONG = Duration.ofHours(1);
    private static final int OPERATIONS = 2_000;
    private static final int BATCHES = 200;
    private static final int BATCH = 100;
    // where the reads' results go, so that neither Error Prone nor the JIT takes them for unused
    @SuppressWarnings("unused")
    private static volatile @Nullable Object sink;

    private final CacheBackend backend;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    // the time spent inside store calls, by every instance, for the store column
    private final AtomicLong storeNanos = new AtomicLong();
    // the writes sent to the store, and those of them starting a new incarnation of a key's version (counter 1, not on
    // "missing"): read off the writes by reflection, so that the benchmark compiles before the SPI had them too
    private final AtomicLong writesSent = new AtomicLong();
    private final AtomicLong incarnationsStarted = new AtomicLong();
    private volatile boolean versionsSeen;
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
                    if (method.getName().equals("write") && arguments != null && arguments.length == 1
                            && arguments[0] instanceof Collection<?> writes) {
                        countIncarnations(writes);
                    }
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

    private void countIncarnations(Collection<?> writes) throws ReflectiveOperationException {
        for (Object write : writes) {
            long version = (long) write.getClass().getMethod("version").invoke(write);
            long expected = (long) write.getClass().getMethod("expected").invoke(write);
            writesSent.incrementAndGet();
            versionsSeen = true;
            // the counter is the version's low 24 bits (InternalVersion); -1 is "missing" (a new key)
            if ((version & ((1L << 24) - 1)) == 1 && expected != -1) {
                incarnationsStarted.incrementAndGet();
            }
        }
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
        table.append(contention(writer, reader));
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

    // contention (VISION.md, build plan, step 6): two instances, four threads each, on few keys, for 5 s. What the
    // versions decide shows here: gets served from L1 (a held value dropped for an order not known is a miss and a
    // store read), conflicts of compute across instances, and, with versions chosen by the core, how often a write
    // starts a new incarnation
    private final List<String> breakdown = new ArrayList<>();

    private String contention(DistributedCache<String, String> first, DistributedCache<String, String> second)
            throws Exception {
        StringBuilder table = new StringBuilder(String.format(Locale.ROOT,
                "%ncontention, 2 instances x 4 threads, 5 s each%n%n"
                        + "| %-40s | %10s | %12s | %15s | %16s | %22s |%n", "scenario", "ops/s", "L1 hit rate",
                "store reads/get", "conflicts/compute", "new incarnations/1000 w"));
        table.append("|").append("-".repeat(42)).append("|").append("-".repeat(12)).append("|")
                .append("-".repeat(14)).append("|").append("-".repeat(17)).append("|").append("-".repeat(18))
                .append("|").append("-".repeat(24)).append("|\n");
        table.append(contend("1 hot key, 80 % gets / 20 % puts", first, second, fresh(1), 8, false));
        table.append(contend("10 hot keys, 80 % gets / 20 % puts", first, second, fresh(10), 8, false));
        table.append(contend("1 hot key, 50 % gets / 50 % puts", first, second, fresh(1), 5, false));
        table.append(contend("1 key, compute (increment) only", first, second, fresh(1), 0, true));
        table.append(String.format(Locale.ROOT, "%nwhere the store reads come from, and what messages did (counts in "
                + "the 5 s, both instances)%n%n| %-40s | %8s | %8s | %10s | %8s | %8s | %8s | %8s | %8s | %8s |%n",
                "scenario", "puts", "reads", "population", "loads", "refusals", "messages", "ignored", "replaced",
                "dropped"));
        breakdown.forEach(table::append);
        return table.toString();
    }

    @SuppressWarnings("resource") // shut down in finally: an ExecutorService is AutoCloseable only from Java 19
    private String contend(String scenario, DistributedCache<String, String> first,
                           DistributedCache<String, String> second, List<String> hotKeys, int getsOutOfTen,
                           boolean compute) throws Exception {
        hotKeys.forEach(key -> first.put(key, "0"));
        hotKeys.forEach(key -> sink = second.getIfPresent(key));
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean measuring = new AtomicBoolean();
        AtomicLong operations = new AtomicLong();
        AtomicLong gets = new AtomicLong();
        ExecutorService threads = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                DistributedCache<String, String> cache = t % 2 == 0 ? first : second;
                futures.add(threads.submit(() -> {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    while (running.get()) {
                        String key = hotKeys.get(random.nextInt(hotKeys.size()));
                        if (compute) {
                            cache.asMap().compute(key, (k, v) -> v == null ? "1"
                                    : String.valueOf(Long.parseLong(v) + 1));
                        } else if (random.nextInt(10) < getsOutOfTen) {
                            sink = cache.getIfPresent(key);
                            if (measuring.get()) {
                                gets.incrementAndGet();
                            }
                        } else {
                            cache.put(key, String.valueOf(random.nextInt(1_000)));
                        }
                        if (measuring.get()) {
                            operations.incrementAndGet();
                        }
                    }
                }));
            }
            Thread.sleep(1_000);
            CacheStats hitsBefore = first.stats().plus(second.stats());
            DistributedStats storeBefore = first.distributedPolicy().stats().plus(second.distributedPolicy().stats());
            long writesBefore = writesSent.get();
            long incarnationsBefore = incarnationsStarted.get();
            measuring.set(true);
            Thread.sleep(5_000);
            measuring.set(false);
            CacheStats hits = first.stats().plus(second.stats()).minus(hitsBefore);
            DistributedStats store = first.distributedPolicy().stats().plus(second.distributedPolicy().stats())
                    .minus(storeBefore);
            long writes = writesSent.get() - writesBefore;
            long incarnations = incarnationsStarted.get() - incarnationsBefore;
            running.set(false);
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
            long getCount = gets.get();
            // store reads: population (a newer write elsewhere, UPDATE mode), loads (a miss), and the rest (a refused
            // advance reading the row, compute reading the store); messages: ignored (not newer, often the own
            // echo), replaced (read again by population), dropped (invalidated or expired in L1)
            breakdown.add(String.format(Locale.ROOT,
                    "| %-40s | %,8d | %,8d | %,10d | %,8d | %,8d | %,11d | %,8d | %,8d | %,8d | %,8d |%n", scenario,
                    store.putCount(), store.storeReadCount(), store.populationReadCount(),
                    store.storeHitCount() + store.storeMissCount(), store.versionRefusalCount(),
                    store.revalidatedCount(),
                    store.writtenMessageCount() + store.invalidatedMessageCount() + store.expiredMessageCount(),
                    store.ignoredMessageCount(), store.replacedCount(),
                    store.invalidatedCount() + store.expiredCount()));
            double computes = compute ? operations.get() : 0;
            return String.format(Locale.ROOT, "| %-40s | %,10d | %12s | %15s | %16s | %22s |%n", scenario,
                    operations.get() / 5,
                    compute ? "-" : String.format(Locale.ROOT, "%.1f %%", 100.0 * hits.hitRate()),
                    compute || getCount == 0 ? "-"
                            : String.format(Locale.ROOT, "%.2f", (double) store.storeReadCount() / getCount),
                    compute ? String.format(Locale.ROOT, "%.2f", store.conflictCount() / computes) : "-",
                    !versionsSeen || writes == 0 ? "n/a"
                            : String.format(Locale.ROOT, "%.1f", 1_000.0 * incarnations / writes));
        } finally {
            threads.shutdownNow();
        }
    }
}
