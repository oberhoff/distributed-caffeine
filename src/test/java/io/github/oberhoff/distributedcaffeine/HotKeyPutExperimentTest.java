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
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.StoreProfile;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Puts of one hot key from many threads on two instances, on a store applying proposed versions conditionally (the
 * jdbc profile, as MariaDB and MySQL): a refused put reads the row and proposes above it, and a competitor's write
 * landing between that read and the write refuses it again, up to the bound of attempts. Found 2026-11-17 when every
 * store call took a thread hop (StoreDeadlineBenchmark). Per store latency: how many puts failed, how many were
 * refused at least once. Run with {@code -Dexperiments=true}.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@EnabledIfSystemProperty(named = "experiments", matches = "true")
@Timeout(600)
@SuppressWarnings("java:S2699") // an experiment reports what it measures
class HotKeyPutExperimentTest {

    private static final int INSTANCES = 2;
    private static final int THREADS = 4;
    private static final Duration RUN = Duration.ofSeconds(5);

    @Test
    void puts_of_one_hot_key_on_a_conditional_store() throws Exception {
        System.out.printf("%n%-16s %10s %10s %14s%n", "store latency", "puts", "failed", "refusals");
        for (long micros : new long[]{0, 50, 200, 1_000}) {
            measure(micros);
        }
    }

    private static void measure(long latencyMicros) throws Exception {
        InMemoryDatabase database = new InMemoryDatabase(StoreProfile.JDBC);
        List<DistributedCache<String, String>> caches = new ArrayList<>();
        for (int index = 0; index < INSTANCES; index++) {
            Adapter<String, String> adapter = settings -> new Slow(database.openStore("cache"), latencyMicros);
            caches.add(DistributedCaffeine.newBuilder(InternalCoreTest.offered(adapter))
                    .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                            .withValueSerializer(StringSerializer.ofString()))
                    .withCaffeine(CaffeineConfigurer::recordStats)
                    .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                            .withMaximumAge(Duration.ofHours(1)))
                    .build());
        }
        AtomicLong puts = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        // per instance: whether the failures fall on one of them (the version's node bits break ties in a millisecond)
        List<AtomicLong> failedBy = new ArrayList<>();
        ExecutorService threads = Executors.newFixedThreadPool(INSTANCES * THREADS);
        long end = System.nanoTime() + RUN.toNanos();
        List<Future<?>> running = new ArrayList<>();
        for (DistributedCache<String, String> cache : caches) {
            AtomicLong failedHere = new AtomicLong();
            failedBy.add(failedHere);
            for (int thread = 0; thread < THREADS; thread++) {
                running.add(threads.submit(() -> {
                    while (System.nanoTime() < end) {
                        try {
                            cache.put("hot", "v");
                            puts.incrementAndGet();
                        } catch (StoreException e) {
                            failed.incrementAndGet();
                            failedHere.incrementAndGet();
                        }
                    }
                }));
            }
        }
        for (Future<?> future : running) {
            future.get();
        }
        threads.shutdown();
        if (!threads.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("the writing threads did not end within 10 s");
        }
        long refusals = caches.stream().mapToLong(cache -> cache.distributedStats().versionRefusalCount()).sum();
        for (DistributedCache<String, String> cache : caches) {
            cache.close();
        }
        System.out.printf("%-16s %10d %10d %14d   per instance: %s%n", latencyMicros + " µs", puts.get(), failed.get(),
                refusals, failedBy);
    }

    // every read and write delayed by the latency, as a network adds it
    private static final class Slow extends InternalCoreTest.DelegatingStore {

        private final long latencyNanos;

        Slow(io.github.oberhoff.distributedcaffeine.spi.Store delegate, long latencyMicros) {
            super(delegate);
            this.latencyNanos = TimeUnit.MICROSECONDS.toNanos(latencyMicros);
        }

        private void delay() {
            if (latencyNanos > 0) {
                LockSupport.parkNanos(latencyNanos);
            }
        }

        @Override
        public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
            delay();
            return super.read(hashes, maxAge);
        }

        @Override
        public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
            delay();
            return super.readVersions(hashes, maxAge);
        }

        @Override
        public WriteResult write(Collection<Write> writes) {
            delay();
            return super.write(writes);
        }
    }
}
