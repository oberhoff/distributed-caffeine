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
import io.github.oberhoff.distributedcaffeine.testing.Awaiting;
import org.jspecify.annotations.NullMarked;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The cost of a refill while serving (VISION.md, "Refill while serving", "Open work"): instances holding every
 * key, a refill rewriting most of them, then {@code invalidateAllBeforeMark}. Per distribution mode and size: how long
 * the call takes, how long until every instance's revalidation pass is done, the store reads and rows the passes cost,
 * and how much of L1 is still held afterwards (hot). Run by an experiment per store; it decides whether the
 * optimisations recorded for the pass (trusting the caller's own writes, a pass reading versions only) are needed.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@SuppressWarnings("JavaPrintToLogpoint") // a benchmark prints its table
public final class RefillBenchmark {

    private static final Duration LONG = Duration.ofHours(1);
    private static final int INSTANCES = 3;
    private static final int BATCH = 1_000;
    // the share of keys the refill rewrites; the others are dropped by the call
    private static final double REWRITTEN = 0.9;

    private final CacheBackend backend;
    // the length of every value, in characters: the pass transfers each held value once
    private final int valueLength;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private int scenario;

    private RefillBenchmark(CacheBackend backend, int valueLength) {
        this.backend = backend;
        this.valueLength = valueLength;
    }

    /**
     * Runs the benchmark and prints its table.
     *
     * @param label       what is measured, for the table's title
     * @param backend     the store
     * @param valueLength the length of every value, in characters (at least 3)
     * @param sizes       the numbers of keys every instance holds
     * @throws Exception if an operation fails
     */
    public static void run(String label, CacheBackend backend, int valueLength, int... sizes) throws Exception {
        RefillBenchmark benchmark = new RefillBenchmark(backend, valueLength);
        StringBuilder table = new StringBuilder("\nRefill while serving, " + label + ", " + INSTANCES
                + " instances, " + (int) (REWRITTEN * 100) + " % of the keys rewritten, values of " + valueLength
                + " characters\n");
        table.append(String.format("%-10s %9s %9s %9s %12s %12s %10s %10s%n", "mode", "keys", "call ms",
                "pass ms", "pass reads", "pass rows", "held after", "fill ms"));
        try {
            for (DistributionMode mode : List.of(DistributionMode.UPDATE, DistributionMode.INVALIDATE)) {
                for (int size : sizes) {
                    table.append(benchmark.measure(mode, size));
                }
            }
        } finally {
            for (AutoCloseable closeable : benchmark.closeables) {
                closeable.close();
            }
        }
        System.out.println(table);
    }

    // one instance and the store calls it made
    private record Instance(DistributedCache<String, String> cache, AtomicLong reads, AtomicLong rows) {
    }

    private Instance instance(DistributionMode mode) {
        AtomicLong reads = new AtomicLong();
        AtomicLong rows = new AtomicLong();
        Adapter<String, String> adapter = settings -> counted(backend.open(settings), reads, rows);
        Instance instance = new Instance(DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10_000_000))
                .withDistributionMode(mode)
                .build(), reads, rows);
        // closed by the scenario; also here, should a scenario fail before
        closeables.add(instance.cache());
        return instance;
    }

    // the store, its reads counted: calls and the rows they ask, through a proxy with every interface the store has
    private static Store counted(Store store, AtomicLong reads, AtomicLong rows) {
        Set<Class<?>> interfaces = new LinkedHashSet<>();
        for (Class<?> type = store.getClass(); type != null; type = type.getSuperclass()) {
            interfaces.addAll(List.of(type.getInterfaces()));
        }
        return (Store) Proxy.newProxyInstance(Store.class.getClassLoader(), interfaces.toArray(new Class<?>[0]),
                (proxy, method, arguments) -> {
                    if ((method.getName().equals("read") || method.getName().equals("readVersions"))
                            && arguments != null && arguments[0] instanceof Collection<?> hashes) {
                        reads.incrementAndGet();
                        rows.addAndGet(hashes.size());
                    }
                    try {
                        return method.invoke(store, arguments);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    // the instances' caches are closed at the end of the scenario, and by run should it fail before
    @SuppressWarnings("resource")
    private String measure(DistributionMode mode, int size) {
        String prefix = "s" + scenario++ + "-";
        List<String> keys = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            keys.add(prefix + index);
        }
        List<Instance> instances = new ArrayList<>();
        for (int index = 0; index < INSTANCES; index++) {
            instances.add(instance(mode));
        }
        // the first instance refills
        Instance refiller = instances.get(0);
        putAll(refiller.cache(), keys, value("old"));
        for (Instance instance : instances) {
            for (int from = 0; from < size; from += BATCH) {
                instance.cache().getAll(keys.subList(from, Math.min(size, from + BATCH)), missing -> Map.of());
            }
        }
        settle(instances);

        DistributedPolicy.Mark mark = refiller.cache().distributedPolicy().store().mark();
        List<String> rewritten = keys.subList(0, (int) (size * REWRITTEN));
        long fillStart = System.nanoTime();
        putAll(refiller.cache(), rewritten, value("new"));
        settle(instances);
        long fillMillis = (System.nanoTime() - fillStart) / 1_000_000;
        long readsBefore = instances.stream().mapToLong(instance -> instance.reads().get()).sum();
        long rowsBefore = instances.stream().mapToLong(instance -> instance.rows().get()).sum();

        long callStart = System.nanoTime();
        refiller.cache().distributedPolicy().store().invalidateAllBeforeMark(mark);
        long callMillis = (System.nanoTime() - callStart) / 1_000_000;
        settle(instances);
        long passMillis = (System.nanoTime() - callStart) / 1_000_000;
        long reads = instances.stream().mapToLong(instance -> instance.reads().get()).sum() - readsBefore;
        long rows = instances.stream().mapToLong(instance -> instance.rows().get()).sum() - rowsBefore;

        long held = 0;
        for (Instance instance : instances) {
            for (int index = 0; index < size; index++) {
                String value = instance.cache().policy().getIfPresentQuietly(keys.get(index));
                if (index >= rewritten.size() && value != null) {
                    throw new AssertionError("a dropped key still held: " + keys.get(index));
                }
                if (value != null) {
                    held++;
                }
            }
        }
        String heldShare = held * 100 / ((long) rewritten.size() * INSTANCES) + " %";
        for (Instance instance : instances) {
            instance.cache().close();
        }
        return String.format("%-10s %9d %9d %9d %12d %12d %10s %10d%n", mode, size, callMillis, passMillis, reads,
                rows, heldShare, fillMillis);
    }

    // the word padded to the value length
    private String value(String word) {
        return word + "x".repeat(Math.max(0, valueLength - word.length()));
    }

    private static void putAll(DistributedCache<String, String> cache, List<String> keys, String value) {
        for (int from = 0; from < keys.size(); from += BATCH) {
            Map<String, String> batch = new LinkedHashMap<>();
            keys.subList(from, Math.min(keys.size(), from + BATCH)).forEach(key -> batch.put(key, value));
            cache.putAll(batch);
        }
    }

    // every message delivered and applied, every revalidation pass done
    private void settle(List<Instance> instances) {
        if (!Awaiting.awaitUpTo(() -> backend.isIdle()
                && instances.stream().allMatch(instance -> core(instance.cache()).isQuiescent()),
                Duration.ofMinutes(10), Duration.ofMillis(1))) {
            throw new AssertionError("not settled within 10 minutes");
        }
    }

    private static InternalCore<String, String> core(DistributedCache<String, String> cache) {
        return ((InternalDistributedCache<String, String>) cache).core;
    }
}
