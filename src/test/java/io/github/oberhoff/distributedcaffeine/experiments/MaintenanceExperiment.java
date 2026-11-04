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
package io.github.oberhoff.distributedcaffeine.experiments;

import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.ToIntBiFunction;

/**
 * What one maintenance call costs on a large cache (VISION.md, build plan, step 7, "maintenance in paced batches"):
 * the cache filled with rows directly, then each call timed while another instance keeps writing a probe key, whose
 * latency shows what the call does to the cache's own operations, and the messages it announces counted until they
 * have arrived. Run per store by its {@code *MaintenanceExperimentTest}, with {@code -Dexperiments=true}.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
@SuppressWarnings({"java:S2925", "java:S106"})
public final class MaintenanceExperiment {

    private static final Duration LONG = Duration.ofHours(1);
    private static final Duration DELIVERY_WAIT = Duration.ofMinutes(2);

    /**
     * The store under measurement.
     */
    public interface Backend {

        /**
         * Opens a store of the cache (another instance each time).
         *
         * @return the store
         */
        Store open();

        /**
         * Replaces the cache's rows by the given number of value rows, written two hours ago (older than {@link #LONG}),
         * hashes {@code h0} to {@code h(n-1)}.
         *
         * @param rows the number of rows
         */
        void fill(int rows);
    }

    private MaintenanceExperiment() {
    }

    /**
     * Measures expiry, pruning hidden rows after an {@code invalidateAll}, and pruning to size, each once per size.
     *
     * @param name    what is measured, for the output
     * @param backend the store
     * @param limits  the limits per call to compare ({@link Integer#MAX_VALUE}: none)
     * @param sizes   the numbers of rows
     */
    public static void run(String name, Backend backend, int[] limits, int... sizes) throws Exception {
        StringBuilder out = new StringBuilder(String.format(Locale.ROOT, "%n=== maintenance, %s ===%n%n"
                        + "| %-28s | %7s | %10s | %10s | %6s | %10s | %10s | %15s | %13s | %10s |%n", name, "operation",
                "limit", "rows", "changed", "calls", "longest ms", "total ms", "messages in ms", "probe max ms",
                "probe p99"));
        out.append("|------------------------------|---------|------------|------------|--------|------------|"
                + "------------|-----------------|---------------|------------|\n");
        for (int rows : sizes) {
            for (int limit : limits) {
                out.append(measure("expire", backend, rows, limit,
                        (store, max) -> store.expire(LONG, Long.MAX_VALUE / 2, max), null));
                out.append(measure("pruneTombstones (hidden)", backend, rows, limit,
                        (store, max) -> store.pruneTombstones(LONG, max), store -> {
                            byte[] bytes = new byte[0];
                            store.write(List.of(Write.value(Store.CONTROL_HASH, bytes, bytes, Long.MAX_VALUE / 4)));
                            pause(50); // written after the rows by every clock
                        }));
                out.append(measure("pruneToSize (to 1,000)", backend, rows, limit,
                        (store, max) -> store.pruneToSize(1_000, Duration.ZERO, max), null));
            }
        }
        System.out.println(out);
    }

    // the call repeated while it reaches the limit, as the core does (without its pauses, which double the time a
    // backlog takes and leave the store to others meanwhile)
    private static String measure(String operation, Backend backend, int rows, int limit,
                                  ToIntBiFunction<Store, Integer> call,
                                  @Nullable Consumer<Store> before)
            throws Exception {
        backend.fill(rows);
        AtomicLong messages = new AtomicLong();
        AtomicLong gaps = new AtomicLong();
        try (Store maintaining = backend.open(); Store probing = backend.open(); Store subscribed = backend.open()) {
            ChangeSubscription subscription = ((ChangeFeed) subscribed).subscribe(new ChangeListener() {
                @Override
                public void onChanges(List<Change> batch) {
                    for (Change change : batch) {
                        if (!change.hash().equals("probe")) {
                            messages.incrementAndGet();
                        }
                    }
                }

                @Override
                public void onGap() {
                    gaps.incrementAndGet();
                }

                @Override
                public void onLost(Throwable cause) {
                    gaps.incrementAndGet();
                }
            });
            try {
                List<Long> probes = Collections.synchronizedList(new ArrayList<>());
                AtomicBoolean running = new AtomicBoolean(true);
                byte[] value = "p".getBytes(StandardCharsets.UTF_8);
                Thread prober = new Thread(() -> {
                    long version = Long.MAX_VALUE / 2;
                    while (running.get()) {
                        long start = System.nanoTime();
                        probing.write(List.of(Write.value("probe", value, value, ++version)));
                        probes.add(System.nanoTime() - start);
                        pause(20);
                    }
                });
                prober.start();
                if (before != null) {
                    before.accept(maintaining);
                }
                pause(500);
                long start = System.nanoTime();
                int changed = 0;
                int calls = 0;
                long longest = 0;
                for (int batch = limit; batch >= limit; ) {
                    long callStart = System.nanoTime();
                    batch = call.applyAsInt(maintaining, limit);
                    longest = Math.max(longest, System.nanoTime() - callStart);
                    changed += batch;
                    calls++;
                }
                long callNanos = System.nanoTime() - start;
                pause(200);
                running.set(false);
                prober.join();
                long deadline = System.nanoTime() + DELIVERY_WAIT.toNanos();
                // pruning announces nothing: no wait
                boolean announces = operation.startsWith("expire");
                while (announces && messages.get() < changed && gaps.get() == 0 && System.nanoTime() < deadline) {
                    pause(10);
                }
                long deliveredNanos = System.nanoTime() - start;
                List<Long> sorted;
                synchronized (probes) {
                    sorted = new ArrayList<>(probes);
                }
                Collections.sort(sorted);
                String delivered = changed == 0 || messages.get() == 0 ? "-"
                        : gaps.get() > 0 ? "gap after " + messages.get()
                        : messages.get() < changed ? messages.get() + " of " + changed
                        : String.format(Locale.ROOT, "%,d", deliveredNanos / 1_000_000);
                return String.format(Locale.ROOT,
                        "| %-28s | %7s | %,10d | %,10d | %,6d | %,10d | %,10d | %15s | %,13.1f | %,10.1f |%n",
                        operation, limit == Integer.MAX_VALUE ? "none" : String.format(Locale.ROOT, "%,d", limit),
                        rows, changed, calls, longest / 1_000_000, callNanos / 1_000_000, delivered,
                        sorted.isEmpty() ? 0 : sorted.get(sorted.size() - 1) / 1e6,
                        sorted.isEmpty() ? 0 : sorted.get((int) (sorted.size() * 0.99)) / 1e6);
            } finally {
                subscription.close();
            }
        }
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
