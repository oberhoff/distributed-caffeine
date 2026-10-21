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

import com.dynatrace.hash4j.hashing.HashStream128;
import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hasher128;
import com.dynatrace.hash4j.hashing.Hashing;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * The first benchmark of VISION.md ("How a message finds the held key"): what does comparing {@code InternalKey} by
 * the hash string cost on the hit path, against comparing by key? {@code getIfPresent} on a filled cache of 10,000
 * keys; variants: plain Caffeine, a wrapper compared by key (today's {@code InternalKey}, nothing hashed on a hit),
 * a wrapper compared by the hash string computed the old way (new hash stream, {@code toByteArray}, {@code HexFormat},
 * as today's {@code Hasher.getHash()}), the same with a reused stream and hex formatted from the two longs, and a
 * wrapper compared by the two longs (the compact form).
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=HitPathBenchmarkTest -Dexperiments=true}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
@SuppressWarnings({"java:S2699", "java:S2925"})
public class HitPathBenchmarkTest {

    private static final int KEYS = 10_000;
    private static final Hasher128 XXH3 = Hashing.xxh3_128();
    private static final HexFormat HEX = HexFormat.of();
    private static final ThreadLocal<HashStream128> STREAM = ThreadLocal.withInitial(XXH3::hashStream);

    @Param({"String", "Long"})
    public String keyType;

    private Object[] keys;
    private Cache<Object, String> plain;
    private Cache<ByKey, String> byKey;
    private Cache<ByString, String> byHashStringOld;
    private Cache<ByString, String> byHashStringLean;
    private Cache<ByLongs, String> byHashLongs;

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
        keys = new Object[KEYS];
        for (int i = 0; i < KEYS; i++) {
            keys[i] = keyType.equals("String")
                    ? String.format("user:%06d:profile", i)
                    : (Object) (ThreadLocalRandom.current().nextLong() | 1L);
        }
        plain = Caffeine.newBuilder().maximumSize(2 * KEYS).build();
        byKey = Caffeine.newBuilder().maximumSize(2 * KEYS).build();
        byHashStringOld = Caffeine.newBuilder().maximumSize(2 * KEYS).build();
        byHashStringLean = Caffeine.newBuilder().maximumSize(2 * KEYS).build();
        byHashLongs = Caffeine.newBuilder().maximumSize(2 * KEYS).build();
        for (Object key : keys) {
            plain.put(key, "value");
            byKey.put(new ByKey(key), "value");
            byHashStringOld.put(new ByString(hashOld(key)), "value");
            byHashStringLean.put(new ByString(hashLean(key)), "value");
            byHashLongs.put(hashLongs(key), "value");
        }
    }

    @Benchmark
    public String plain(Cursor cursor) {
        return plain.getIfPresent(keys[cursor.next()]);
    }

    @Benchmark
    public String byKey(Cursor cursor) {
        return byKey.getIfPresent(new ByKey(keys[cursor.next()]));
    }

    @Benchmark
    public String byHashStringOld(Cursor cursor) {
        return byHashStringOld.getIfPresent(new ByString(hashOld(keys[cursor.next()])));
    }

    @Benchmark
    public String byHashStringLean(Cursor cursor) {
        return byHashStringLean.getIfPresent(new ByString(hashLean(keys[cursor.next()])));
    }

    @Benchmark
    public String byHashLongs(Cursor cursor) {
        return byHashLongs.getIfPresent(hashLongs(keys[cursor.next()]));
    }

    // as today's Hasher: a new stream per hash, the value as bytes, formatted as hex
    private static String hashOld(Object key) {
        HashStream128 stream = XXH3.hashStream().reset();
        put(stream, key);
        return HEX.formatHex(stream.get().toByteArray());
    }

    // a reused stream per thread, hex formatted straight from the two longs
    private static String hashLean(Object key) {
        HashStream128 stream = STREAM.get().reset();
        put(stream, key);
        HashValue128 value = stream.get();
        return HEX.toHexDigits(value.getMostSignificantBits()) + HEX.toHexDigits(value.getLeastSignificantBits());
    }

    private static ByLongs hashLongs(Object key) {
        HashStream128 stream = STREAM.get().reset();
        put(stream, key);
        HashValue128 value = stream.get();
        return new ByLongs(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static void put(HashStream128 stream, Object key) {
        if (key instanceof String string) {
            stream.putString(string);
        } else {
            stream.putLong((Long) key);
        }
    }

    record ByKey(Object key) {
    }

    record ByString(String hash) {
    }

    record ByLongs(long high, long low) {
        @Override
        public int hashCode() {
            return Long.hashCode(low);
        }
    }

    @Test
    void run() throws Exception {
        for (int threads : new int[]{1, 4}) {
            Options options = new OptionsBuilder()
                    .include(HitPathBenchmarkTest.class.getName() + "\\.(plain|byKey|byHash.*)$")
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
