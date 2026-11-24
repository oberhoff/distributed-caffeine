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
import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.testing.Awaiting;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * The cost of catching up after a gap (VISION.md, "Open work", Performance: installing what the background pass read,
 * a scan next to a feed): an instance holding every key misses the changes to some of them, then catches up. Three
 * scenarios, per distribution mode, size and share of keys changed:
 * <ul>
 *     <li>{@code gap}: the instance's feed misses the changes, then reports a gap; the background pass checks every
 *     held copy;</li>
 *     <li>{@code start}: the instance starts without the store and loads every key from the source, so its copies are
 *     unconfirmed; the store is reached, and the pass checks them all (every key counts as changed);</li>
 *     <li>{@code down}: the instance's feed stops (lost, and not resumed) while the changes are made, so every read
 *     checks its copy against the store.</li>
 * </ul>
 * Measured: the pass (time, store reads, rows read, copies dropped, changed keys still held afterwards), then one
 * {@code get} of each hot key (store reads and time). The store reads are counted at the store, so they do not depend
 * on the network's timings; the times do. Run by an experiment per store.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@SuppressWarnings("JavaPrintToLogpoint") // a benchmark prints its table
public final class GapBenchmark {

    private static final Duration LONG = Duration.ofHours(1);
    private static final Duration SETTLE = Duration.ofMinutes(10);
    private static final int BATCH = 1_000;
    // the share of the keys read once after the catch-up, drawn independently of the changed ones
    private static final double HOT = 0.1;
    // the seeds of the draws: the same keys in every run, the hot ones independent of the changed ones
    private static final long CHANGED_SEED = 1;
    private static final long HOT_SEED = 2;
    private static final double[] CHANGED = {0.01, 0.1, 0.5};

    private final CacheBackend backend;
    // the length of every value, in characters: the pass transfers each held value once
    private final int valueLength;
    private int scenario;

    private GapBenchmark(CacheBackend backend, int valueLength) {
        this.backend = backend;
        this.valueLength = valueLength;
    }

    /**
     * Runs the benchmark and prints its table.
     *
     * @param label       what is measured, for the table's title
     * @param backend     the store
     * @param valueLength the length of every value, in characters (at least 3)
     * @param sizes       the numbers of keys the instance holds
     */
    public static void run(String label, CacheBackend backend, int valueLength, int... sizes) {
        GapBenchmark benchmark = new GapBenchmark(backend, valueLength);
        StringBuilder table = new StringBuilder("\nCatching up after a gap, " + label + ", values of " + valueLength
                + " characters, " + (int) (HOT * 100) + " % of the keys read once afterwards\n");
        table.append(String.format("%-9s %-10s %8s %8s %9s %10s %10s %9s %9s %10s %10s %9s%n", "scenario", "mode",
                "keys", "changed", "pass ms", "pass reads", "pass rows", "dropped", "held new", "read calls",
                "read rows", "read ms"));
        for (DistributionMode mode : List.of(DistributionMode.UPDATE, DistributionMode.INVALIDATE)) {
            for (int size : sizes) {
                for (double changed : CHANGED) {
                    table.append(benchmark.gap(mode, size, changed));
                }
                table.append(benchmark.start(mode, size));
                for (double changed : CHANGED) {
                    table.append(benchmark.down(mode, size, changed));
                }
            }
        }
        System.out.println(table);
    }

    // the feed of the instance that misses the changes: muted, it drops what it delivers; down, it cannot be resumed
    private static final class Feed {
        volatile boolean muted;
        volatile boolean down;
        // the core's listener, to report a gap or a loss to
        volatile @Nullable ChangeListener listener;

        ChangeListener listener() {
            ChangeListener current = listener;
            if (current == null) {
                throw new AssertionError("not subscribed");
            }
            return current;
        }
    }

    // the instance's store calls: reads, and the rows they ask
    private record Counted(AtomicLong reads, AtomicLong rows) {

        Counted() {
            this(new AtomicLong(), new AtomicLong());
        }

        long[] now() {
            return new long[]{reads.get(), rows.get()};
        }
    }

    // the instance that misses the changes: a loading cache, its stats recorded, its feed under the benchmark's
    // control; it keeps its cache to itself, closed with it
    @SuppressWarnings("ClassCanBeRecord") // a record would hand its cache out, to be used outside a try-with-resources
    private static final class Holder implements AutoCloseable {

        private final DistributedLoadingCache<String, String> cache;
        final Counted counted;
        final Feed feed;

        Holder(DistributedLoadingCache<String, String> cache, Counted counted, Feed feed) {
            this.cache = cache;
            this.counted = counted;
            this.feed = feed;
        }

        DistributedStats stats() {
            return cache.distributedStats();
        }

        @Nullable String get(String key) {
            return cache.get(key);
        }

        void getAll(List<String> keys) {
            cache.getAll(keys);
        }

        boolean holds(String key) {
            return cache.policy().getIfPresentQuietly(key) != null;
        }

        // every message delivered here applied, no revalidation running
        boolean isQuiescent() {
            return ((InternalDistributedCache<?, ?>) cache).core.isQuiescent();
        }

        @Override
        public void close() {
            cache.close();
        }
    }

    // the instance that makes the changes
    private DistributedCache<String, String> writer(DistributionMode mode) {
        return configured(DistributedCaffeine.newBuilder(backend::open), mode).build();
    }

    // reachable: whether the adapter reaches the store; until it does, the cache starts without it
    private Holder holder(DistributionMode mode, BooleanSupplier reachable) {
        Counted counted = new Counted();
        Feed feed = new Feed();
        Adapter<String, String> adapter = settings -> {
            if (!reachable.getAsBoolean()) {
                throw new StoreException("not reached (benchmark)", null, false, true);
            }
            return controlled(backend.open(settings), counted, feed);
        };
        return new Holder(configured(DistributedCaffeine.newBuilder(adapter), mode)
                .withCaffeine(CaffeineConfigurer::recordStats)
                .build(key -> value("new")), counted, feed);
    }

    // a heartbeat far longer than a scenario: a muted feed must not be noticed as silent and resumed meanwhile
    private static DistributedCaffeine<String, String> configured(DistributedCaffeine<String, String> builder,
                                                               DistributionMode mode) {
        return builder
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10_000_000)
                        .withHeartbeatInterval(LONG))
                .withDistributionMode(mode);
    }

    // the store with its reads counted and its feed controlled, through a proxy with every interface the store has
    private static Store controlled(Store store, Counted counted, Feed feed) {
        return (Store) proxy(store, (method, arguments) -> {
            if ((method.getName().equals("read") || method.getName().equals("readVersions"))
                    && arguments != null && arguments[0] instanceof Collection<?> hashes) {
                counted.reads().incrementAndGet();
                counted.rows().addAndGet(hashes.size());
            }
            if (method.getName().equals("subscribe") && arguments != null
                    && arguments[0] instanceof ChangeListener listener) {
                feed.listener = listener;
                ChangeSubscription subscription = (ChangeSubscription) method.invoke(store, muted(listener, feed));
                return proxy(subscription, (subscriptionMethod, subscriptionArguments) -> {
                    if (subscriptionMethod.getName().equals("resume") && feed.down) {
                        throw new StoreException("delivery down (benchmark)", null, false, true);
                    }
                    return subscriptionMethod.invoke(subscription, subscriptionArguments);
                });
            }
            return method.invoke(store, arguments);
        });
    }

    // the core's listener behind the feed's mute: changes delivered while muted are lost, as in a gap
    private static ChangeListener muted(ChangeListener listener, Feed feed) {
        return new ChangeListener() {
            @Override
            public void onChanges(List<Change> changes) {
                if (!feed.muted) {
                    listener.onChanges(changes);
                }
            }

            @Override
            public void onGap() {
                listener.onGap();
            }

            @Override
            public void onLost(Throwable cause) {
                listener.onLost(cause);
            }
        };
    }

    private interface Call {
        @Nullable Object invoke(Method method, Object @Nullable [] arguments) throws Exception;
    }

    private static Object proxy(Object target, Call call) {
        Set<Class<?>> interfaces = new LinkedHashSet<>();
        for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
            interfaces.addAll(List.of(type.getInterfaces()));
        }
        return Proxy.newProxyInstance(GapBenchmark.class.getClassLoader(), interfaces.toArray(new Class<?>[0]),
                (proxy, method, arguments) -> {
                    try {
                        return call.invoke(method, arguments);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    // the feed misses the changes, then reports a gap: the background pass checks every held copy
    private String gap(DistributionMode mode, int size, double share) {
        List<String> keys = keys(size);
        try (DistributedCache<String, String> writer = writer(mode); Holder holder = holder(mode, () -> true)) {
            putAll(writer, keys, value("old"));
            hold(holder, keys);
            settle(holder);

            List<String> changed = drawn(keys, share, CHANGED_SEED);
            holder.feed.muted = true;
            putAll(writer, changed, value("new"));
            settle(holder);
            holder.feed.muted = false;

            long gaps = holder.stats().gapCount();
            long dropped = holder.stats().revalidationDroppedCount();
            long[] before = holder.counted.now();
            long start = System.nanoTime();
            holder.feed.listener().onGap();
            await(() -> holder.stats().gapCount() > gaps);
            settle(holder);
            return row("gap", mode, size, share, holder, keys, changed, start, before, dropped);
        }
    }

    // the instance starts without the store and loads every key from the source; then the store is reached
    private String start(DistributionMode mode, int size) {
        List<String> keys = keys(size);
        boolean[] reachable = {false};
        try (DistributedCache<String, String> writer = writer(mode)) {
            putAll(writer, keys, value("new"));
            try (Holder holder = holder(mode, () -> reachable[0])) {
                hold(holder, keys);

                long gaps = holder.stats().gapCount();
                long dropped = holder.stats().revalidationDroppedCount();
                long[] before = holder.counted.now();
                long start = System.nanoTime();
                reachable[0] = true;
                await(() -> holder.stats().gapCount() > gaps);
                settle(holder);
                return row("start", mode, size, 1.0, holder, keys, keys, start, before, dropped);
            }
        }
    }

    // the feed stops while the changes are made: every read checks its copy against the store
    private String down(DistributionMode mode, int size, double share) {
        List<String> keys = keys(size);
        try (DistributedCache<String, String> writer = writer(mode); Holder holder = holder(mode, () -> true)) {
            putAll(writer, keys, value("old"));
            hold(holder, keys);
            settle(holder);

            long interruptions = holder.stats().interruptionCount();
            holder.feed.muted = true;
            holder.feed.down = true;
            holder.feed.listener().onLost(new IOException("delivery stopped (benchmark)"));
            await(() -> holder.stats().interruptionCount() > interruptions);
            List<String> changed = drawn(keys, share, CHANGED_SEED);
            putAll(writer, changed, value("new"));
            await(backend::isIdle);
            return String.format("%-9s %-10s %8d %7d%% %9s %10s %10s %9s %9s %s", "down", mode, size,
                    Math.round(share * 100), "-", "-", "-", "-", "-", access(holder, keys, changed));
        }
    }

    // the pass's columns, then the access's
    private String row(String name, DistributionMode mode, int size, double share, Holder holder, List<String> keys,
                       List<String> changed, long start, long[] before, long droppedBefore) {
        long passMillis = (System.nanoTime() - start) / 1_000_000;
        long[] after = holder.counted.now();
        long dropped = holder.stats().revalidationDroppedCount() - droppedBefore;
        long heldNew = changed.stream().filter(holder::holds).count();
        return String.format("%-9s %-10s %8d %7d%% %9d %10d %10d %9d %8d%% %s", name, mode, size,
                Math.round(share * 100), passMillis, after[0] - before[0], after[1] - before[1], dropped,
                heldNew * 100 / changed.size(), access(holder, keys, changed));
    }

    // one get of each hot key, each checked for the latest value
    private String access(Holder holder, List<String> keys, List<String> changed) {
        Set<String> changedKeys = new LinkedHashSet<>(changed);
        List<String> hot = drawn(keys, HOT, HOT_SEED);
        long[] before = holder.counted.now();
        long start = System.nanoTime();
        for (String key : hot) {
            String expected = value(changedKeys.contains(key) ? "new" : "old");
            String read = holder.get(key);
            if (!expected.equals(read)) {
                throw new AssertionError("not the latest value of " + key + ": " + read);
            }
        }
        long millis = (System.nanoTime() - start) / 1_000_000;
        long[] after = holder.counted.now();
        return String.format("%10d %10d %9d%n", after[0] - before[0], after[1] - before[1], millis);
    }

    // the keys of a new scenario, apart from every earlier one's
    private List<String> keys(int size) {
        String prefix = "s" + scenario++ + "-";
        List<String> keys = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            keys.add(prefix + index);
        }
        return keys;
    }

    // each key with the probability given, by a seeded draw: the hot keys then hold about the share of changed ones
    private static List<String> drawn(List<String> keys, double share, long seed) {
        Random random = new Random(seed);
        List<String> chosen = new ArrayList<>();
        for (String key : keys) {
            if (random.nextDouble() < share) {
                chosen.add(key);
            }
        }
        return chosen;
    }

    // every key held by the instance
    private static void hold(Holder holder, List<String> keys) {
        for (int from = 0; from < keys.size(); from += BATCH) {
            holder.getAll(keys.subList(from, Math.min(keys.size(), from + BATCH)));
        }
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

    // every message delivered and applied, the holder's pass done
    private void settle(Holder holder) {
        await(() -> backend.isIdle() && holder.isQuiescent());
    }

    private static void await(BooleanSupplier condition) {
        if (!Awaiting.awaitUpTo(condition, SETTLE, Duration.ofMillis(1))) {
            throw new AssertionError("not settled within " + SETTLE);
        }
    }
}
