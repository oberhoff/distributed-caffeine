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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.oberhoff.distributedcaffeine.DistributedAsyncLoadingCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.util.Objects.requireNonNull;

/**
 * Async variants (VISION.md): what does running the synchronous load path on the store executor cost, when the
 * user's loader is truly asynchronous? A burst of N concurrent misses on distinct keys; the loader completes after
 * 100 ms without holding a thread (an HTTP client, say); every store call blocks for 1 ms (JDBC, say). Compared:
 * <ul>
 *     <li>{@code current}: the async cache as built, with a cached platform pool as store executor (as the default): a
 *     store-executor
 *     thread waits in {@code future.get()} while the loader runs;</li>
 *     <li>{@code current+virtual}: the same with a virtual-thread store executor;</li>
 *     <li>{@code non-blocking}: the proposed variant, modelled with a plain Caffeine {@code AsyncLoadingCache}: the
 *     store read on a cached platform pool, then the loader's future, then the conditional write on the pool; no
 *     thread is held while the loader runs.</li>
 * </ul>
 * Reported: time until all N loads complete, and the peak of live platform threads above the baseline.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=AsyncLoadThreadsExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
@SuppressWarnings({"java:S2699", "java:S2925"})
public class AsyncLoadThreadsExperimentTest {

    private static final long STORE_LATENCY_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long LOADER_MILLIS = 100;
    private static final ThreadMXBean THREADS = ManagementFactory.getThreadMXBean();

    @Test
    void run() throws Exception {
        for (String variant : List.of("current", "current+virtual", "non-blocking")) {
            measure(variant, 200); // warm-up
        }
        System.out.printf("%-16s %7s %12s %22s%n", "variant", "N", "all done ms", "peak platform threads");
        for (int n : new int[]{100, 1_000, 5_000, 10_000}) {
            for (int repetition = 0; repetition < 2; repetition++) {
                for (String variant : List.of("current", "current+virtual", "non-blocking")) {
                    long[] result = measure(variant, n);
                    System.out.printf("%-16s %7d %12d %22d%n", variant, n, result[0], result[1]);
                }
            }
        }
    }

    @SuppressWarnings("resource") // the cache and the pool of whichever variant ran are closed in finally
    private static long[] measure(String variant, int n) throws Exception {
        System.gc();
        Thread.sleep(200);
        int baseline = THREADS.getThreadCount();
        THREADS.resetPeakThreadCount();
        AsyncCacheLoader<String, String> loader = AsyncLoadThreadsExperimentTest::sourceLoad;
        InMemoryDatabase database = new InMemoryDatabase();
        ExecutorService pool = null;
        AutoCloseable cache = null;
        try {
            List<CompletableFuture<String>> loads = new ArrayList<>(n);
            long start;
            switch (variant) {
                case "current", "current+virtual" -> {
                    Adapter<String, String> adapter = settings -> new SlowStore(database.openStore("t"));
                    DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter)
                            .withSerializers(serializers -> serializers
                                    .withKeySerializer(StringSerializer.ofString())
                                    .withValueSerializer(StringSerializer.ofString()))
                            .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000_000));
                    // a fresh pool per run, like the default (cached daemon threads), whose idle threads from an earlier
                    // run would otherwise hide new ones from the peak
                    pool = variant.equals("current+virtual") ? virtualThreads() : Executors.newCachedThreadPool(daemon());
                    builder.withStoreExecutor(pool);
                    DistributedAsyncLoadingCache<String, String> distributed = builder.buildAsync(loader);
                    cache = distributed;
                    start = System.nanoTime();
                    for (int i = 0; i < n; i++) {
                        loads.add(distributed.get("k" + i));
                    }
                }
                default -> {
                    ExecutorService storePool = Executors.newCachedThreadPool(daemon());
                    pool = storePool;
                    Store store = new SlowStore(database.openStore("t"));
                    AsyncLoadingCache<String, String> plain = Caffeine.newBuilder().buildAsync((key, executor) ->
                            CompletableFuture.supplyAsync(() -> store.read(List.of(key), Duration.ofHours(1)), storePool)
                                    .thenCompose(read -> read.containsKey(key)
                                            ? CompletableFuture.completedFuture(new String(requireNonNull(read.get(key).value()),
                                                    StandardCharsets.UTF_8))
                                            : sourceLoad(key, executor).thenApplyAsync(value -> {
                                        // any positive version: the store only compares it
                                        store.write(List.of(Write.value(key, key.getBytes(StandardCharsets.UTF_8),
                                                value.getBytes(StandardCharsets.UTF_8),
                                                ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE))
                                                .ifMissing()));
                                        return value;
                                    }, storePool)));
                    start = System.nanoTime();
                    for (int i = 0; i < n; i++) {
                        loads.add(plain.get("k" + i));
                    }
                }
            }
            CompletableFuture.allOf(loads.toArray(new CompletableFuture[0])).get(120, TimeUnit.SECONDS);
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            return new long[]{millis, THREADS.getPeakThreadCount() - baseline};
        } finally {
            if (cache != null) {
                cache.close();
            }
            if (pool != null) {
                pool.shutdownNow();
            }
        }
    }

    // the user's loader: completes after 100 ms without holding a thread
    private static CompletableFuture<String> sourceLoad(String key, java.util.concurrent.Executor executor) {
        return CompletableFuture.supplyAsync(() -> "v-" + key,
                CompletableFuture.delayedExecutor(LOADER_MILLIS, TimeUnit.MILLISECONDS, executor));
    }

    private static ExecutorService virtualThreads() throws Exception {
        // the baseline is Java 17, the experiment runs on 21 or later
        return (ExecutorService) Executors.class.getMethod("newVirtualThreadPerTaskExecutor").invoke(null);
    }

    private static java.util.concurrent.ThreadFactory daemon() {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        };
    }

    // 1 ms of blocking per call, as a JDBC round trip
    private record SlowStore(Store delegate) implements Store, ChangeFeed {

        private static <T> T slow(T result) {
            LockSupport.parkNanos(STORE_LATENCY_NANOS);
            return result;
        }

        @Override
        public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
            return slow(delegate.read(hashes, maxAge));
        }

        @Override
        public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
            return slow(delegate.readVersions(hashes, maxAge));
        }

        @Override
        public Set<String> write(Collection<Write> writes) {
            return slow(delegate.write(writes));
        }

        @Override
        public int expire(Duration maxAge, long version) {
            return delegate.expire(maxAge, version);
        }

        @Override
        public int pruneToSize(long maxSize) {
            return delegate.pruneToSize(maxSize);
        }

        @Override
        public int pruneTombstones(Duration retention) {
            return delegate.pruneTombstones(retention);
        }

        @Override
        public ChangeSubscription subscribe(ChangeListener listener) {
            return ((ChangeFeed) delegate).subscribe(listener);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
