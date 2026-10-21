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

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Shared loads in VISION.md: what does an {@code AsyncCache} as L1 for every variant cost on the hit path, against a
 * plain {@code Cache}? Keys are precomputed {@code InternalKey}-like wrappers of two longs, so only the cache
 * differs: {@code getIfPresent} and {@code get(key, function)} on a filled cache of 10,000 keys, on a plain
 * {@code Cache}, on the {@code synchronous()} view of an {@code AsyncCache}, and directly on the {@code AsyncCache}
 * (future, then {@code join()}).
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=HitPathAsyncBenchmarkTest -Dexperiments=true}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
@SuppressWarnings({"java:S2699", "java:S2925"})
public class HitPathAsyncBenchmarkTest {

    private static final int KEYS = 10_000;

    private Key[] keys;
    private Cache<Key, String> sync;
    private AsyncCache<Key, String> async;
    private Cache<Key, String> asyncView;

    record Key(long high, long low) {
        @Override
        public int hashCode() {
            return Long.hashCode(low);
        }
    }

    @State(Scope.Thread)
    public static class Cursor {
        int index = ThreadLocalRandom.current().nextInt(KEYS);

        int next() {
            index = index + 1 == KEYS ? 0 : index + 1;
            return index;
        }
    }

    @Setup(Level.Trial)
    public void fillKeys() {
        keys = new Key[KEYS];
        for (int i = 0; i < KEYS; i++) {
            keys[i] = new Key(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong());
        }
        sync = Caffeine.newBuilder().maximumSize(2 * KEYS).build();
        async = Caffeine.newBuilder().maximumSize(2 * KEYS).buildAsync();
        asyncView = async.synchronous();
        for (Key key : keys) {
            sync.put(key, "value");
            async.put(key, CompletableFuture.completedFuture("value"));
        }
    }

    @Benchmark
    public String syncGetIfPresent(Cursor cursor) {
        return sync.getIfPresent(keys[cursor.next()]);
    }

    @Benchmark
    public String asyncViewGetIfPresent(Cursor cursor) {
        return asyncView.getIfPresent(keys[cursor.next()]);
    }

    @Benchmark
    public String asyncGetIfPresentJoin(Cursor cursor) {
        CompletableFuture<String> future = async.getIfPresent(keys[cursor.next()]);
        return future == null ? null : future.join();
    }

    @Benchmark
    public String syncGet(Cursor cursor) {
        return sync.get(keys[cursor.next()], key -> "loaded");
    }

    @Benchmark
    public String asyncViewGet(Cursor cursor) {
        return asyncView.get(keys[cursor.next()], key -> "loaded");
    }

    @Benchmark
    public String asyncGetJoin(Cursor cursor) {
        return async.get(keys[cursor.next()], key -> "loaded").join();
    }

    @Test
    void run() throws Exception {
        for (int threads : new int[]{1, 4}) {
            Options options = new OptionsBuilder()
                    .include(HitPathAsyncBenchmarkTest.class.getName() + "\\.(sync|async).*$")
                    .forks(1)
                    .threads(threads)
                    .warmupIterations(3)
                    .warmupTime(TimeValue.seconds(1))
                    .measurementIterations(5)
                    .measurementTime(TimeValue.seconds(1))
                    .addProfiler(GCProfiler.class)
                    .build();
            Objects.requireNonNull(new Runner(options).run());
        }
    }
}
