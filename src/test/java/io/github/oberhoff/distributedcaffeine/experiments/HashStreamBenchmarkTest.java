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
import io.github.oberhoff.distributedcaffeine.hasher.Hash;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * How to hash a built-in key on the hit path without a stream kept per virtual thread (VISION.md, "How a message finds
 * the held key")? Hashes a key into its 128 bits; variants: a stream per thread, a fresh stream, a pool of streams
 * striped by thread id (unpadded and padded), one-shot hashing over a char view of the stream's encoding, and what is
 * built, {@link Hasher#builtInHash} (one-shot, seeded per kind, no stream). For virtual
 * threads, add {@code -jvmArgsAppend -Djmh.executor=VIRTUAL}.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=HashStreamBenchmarkTest -Dexperiments=true}.
 *
 * @author Andreas Oberhoff
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it
@SuppressWarnings("java:S2699")
public class HashStreamBenchmarkTest {

    private static final Hasher128 XXH3 = Hashing.xxh3_128();
    @SuppressWarnings("java:S5164") // the variant under test
    private static final ThreadLocal<HashStream128> STREAM = ThreadLocal.withInitial(XXH3::hashStream);

    // a pool striped by thread: a slot per thread id, twice the cores, fresh when its slot is taken
    private static final int SLOTS = Integer.highestOneBit(Runtime.getRuntime().availableProcessors() * 2 - 1) << 1;
    private static final AtomicReferenceArray<HashStream128> POOL = new AtomicReferenceArray<>(SLOTS);
    // the same, a slot per 128 bytes, so that no two slots share a cache line
    private static final int PAD = 32;
    private static final AtomicReferenceArray<HashStream128> PADDED = new AtomicReferenceArray<>(SLOTS * PAD);

    @Param({"String", "Long", "String100", "String1000"})
    public String keyType;

    private String string;
    private long number;

    @Setup
    public void keys() {
        string = switch (keyType) {
            case "String100" -> "user:000123:profile:".repeat(5);
            case "String1000" -> "user:000123:profile:".repeat(50);
            default -> "user:000123:profile";
        };
        encodedString = stringBytes(string);
        number = 123_456_789L;
    }

    private long bits(HashStream128 stream) {
        if (keyType.startsWith("String")) {
            stream.putString(string);
        } else {
            stream.putLong(number);
        }
        HashValue128 value = stream.get();
        return value.getLeastSignificantBits() ^ value.getMostSignificantBits();
    }

    @Benchmark
    public long perThread() {
        return bits(STREAM.get().reset());
    }

    // what the cache does: Hasher.builtInHash, one-shot, seeded per kind
    @Benchmark
    public long hashBuiltIn() {
        Hash value = Hasher.builtInHash(keyType.startsWith("String") ? string : (Object) number);
        return value == null ? 0 : value.leastSignificantBits() ^ value.mostSignificantBits();
    }

    @Benchmark
    public long fresh() {
        return bits(XXH3.hashStream());
    }

    @Benchmark
    public long pooled() {
        int slot = (int) Thread.currentThread().getId() & (SLOTS - 1);
        HashStream128 stream = POOL.getAndSet(slot, null);
        if (stream == null) {
            stream = XXH3.hashStream();
        }
        try {
            return bits(stream.reset());
        } finally {
            POOL.lazySet(slot, stream);
        }
    }

    @Benchmark
    public long pooledPadded() {
        int slot = ((int) Thread.currentThread().getId() & (SLOTS - 1)) * PAD;
        HashStream128 stream = PADDED.getAndSet(slot, null);
        if (stream == null) {
            stream = XXH3.hashStream();
        }
        try {
            return bits(stream.reset());
        } finally {
            PADDED.lazySet(slot, stream);
        }
    }

    // the key's stream encoding as chars (little-endian 16-bit units), hashed in one shot: no stream
    @Benchmark
    public long oneShot() {
        CharSequence chars = keyType.startsWith("String") ? new StringChars(string) : new LongChars(number);
        HashValue128 value = XXH3.hashCharsTo128Bits(chars);
        return value.getLeastSignificantBits() ^ value.getMostSignificantBits();
    }

    // the key's stream encoding written into bytes, hashed in one shot (hash4j reads bytes in bulk): no stream
    @Benchmark
    public long oneShotBytes() {
        byte[] bytes = keyType.startsWith("String") ? stringBytes(string) : longBytes(number);
        HashValue128 value = XXH3.hashBytesTo128Bits(bytes);
        return value.getLeastSignificantBits() ^ value.getMostSignificantBits();
    }

    // a custom hash as a Hashable computes it: putString(id), then putLong(version)
    // a custom hash as a Hashable computes it, putString(id) then putLong(version): the Hasher before (a fresh hash4j
    // stream per hasher), and the Hasher built (the values encoded into a small array, hashed at once)
    @Benchmark
    public String customStream() {
        return HexFormat.of().formatHex(XXH3.hashStream().putString(string).putLong(number).get().toByteArray());
    }

    // the parts of it: XXH3 at once over the encoded bytes alone, and the stream over the same string
    private byte[] encodedString = new byte[0];

    @Benchmark
    public long partOneShot() {
        HashValue128 value = XXH3.hashBytesTo128Bits(encodedString);
        return value.getLeastSignificantBits() ^ value.getMostSignificantBits();
    }

    @Benchmark
    public long partStream() {
        HashValue128 value = XXH3.hashStream().putString(string).get();
        return value.getLeastSignificantBits() ^ value.getMostSignificantBits();
    }

    @Benchmark
    public Hash customHasher() {
        return new Hasher().putString(string).putLong(number).getHash();
    }

    @Benchmark
    public long customReused() {
        HashValue128 value = STREAM.get().reset().putString(string).putLong(number).get();
        return value.getLeastSignificantBits() ^ value.getMostSignificantBits();
    }

    // a lighter hasher: the bytes written into a small array, hashed one-shot
    @Benchmark
    public long customLight() {
        HashValue128 value = new LightHasher().putString(string).putLong(number).hash();
        return value.getLeastSignificantBits() ^ value.getMostSignificantBits();
    }

    static final class LightHasher {
        private byte[] bytes = new byte[64];
        private int size;

        private void ensure(int more) {
            if (size + more > bytes.length) {
                bytes = java.util.Arrays.copyOf(bytes, Math.max(bytes.length * 2, size + more));
            }
        }

        LightHasher putLong(long v) {
            ensure(8);
            for (int i = 0; i < 8; i++) {
                bytes[size++] = (byte) (v >>> (8 * i));
            }
            return this;
        }

        LightHasher putInt(int v) {
            ensure(4);
            for (int i = 0; i < 4; i++) {
                bytes[size++] = (byte) (v >>> (8 * i));
            }
            return this;
        }

        LightHasher putString(String s) {
            int n = s.length();
            ensure(2 * n);
            for (int i = 0; i < n; i++) {
                char c = s.charAt(i);
                bytes[size++] = (byte) c;
                bytes[size++] = (byte) (c >>> 8);
            }
            return putInt(n);
        }

        HashValue128 hash() {
            return XXH3.hashBytesTo128Bits(bytes, 0, size);
        }
    }

    @Test
    void the_light_hasher_hashes_as_the_stream() {
        for (String s : new String[]{"", "a", "user:000123:profile", "ü€𝄞", "x".repeat(70_000)}) {
            for (long v : new long[]{0, -1, 42, Long.MIN_VALUE}) {
                org.assertj.core.api.Assertions.assertThat(new LightHasher().putString(s).putLong(v).hash())
                        .isEqualTo(XXH3.hashStream().putString(s).putLong(v).get());
            }
        }
    }

    // design B: each value one-shot over its natural bytes, XXH3 seeded per type (no length suffix, no stream)
    private static final Hasher128 SEEDED_STRING = Hashing.xxh3_128(1);
    private static final Hasher128 SEEDED_LONG = Hashing.xxh3_128(2);

    @Benchmark
    public long seeded() {
        HashValue128 value = keyType.startsWith("String") ? SEEDED_STRING.hashCharsTo128Bits(string)
                : SEEDED_LONG.hashBytesTo128Bits(longBytes(number));
        return value.getLeastSignificantBits() ^ value.getMostSignificantBits();
    }

    // other algorithms on the same encoded key, one-shot: what a format change could switch to
    private static final Hasher128 MURMUR3 = Hashing.murmur3_128();

    @Benchmark
    public long algoXxh3() {
        HashValue128 value = XXH3.hashBytesTo128Bits(encoded());
        return value.getLeastSignificantBits() ^ value.getMostSignificantBits();
    }

    @Benchmark
    public long algoMurmur3() {
        HashValue128 value = MURMUR3.hashBytesTo128Bits(encoded());
        return value.getLeastSignificantBits() ^ value.getMostSignificantBits();
    }

    @Benchmark
    public long algoSha256() throws java.security.NoSuchAlgorithmException {
        byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(encoded());
        return digest[0] ^ digest[15];
    }

    private byte[] encoded() {
        return keyType.startsWith("String") ? stringBytes(string) : longBytes(number);
    }

    // putString: each char little-endian, then the length as a little-endian int
    static byte[] stringBytes(String s) {
        int n = s.length();
        byte[] bytes = new byte[2 * n + 4];
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            bytes[2 * i] = (byte) c;
            bytes[2 * i + 1] = (byte) (c >>> 8);
        }
        bytes[2 * n] = (byte) n;
        bytes[2 * n + 1] = (byte) (n >>> 8);
        bytes[2 * n + 2] = (byte) (n >>> 16);
        bytes[2 * n + 3] = (byte) (n >>> 24);
        return bytes;
    }

    // putLong: eight bytes, little-endian
    static byte[] longBytes(long v) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (v >>> (8 * i));
        }
        return bytes;
    }

    // putString: the chars, then the length as an int
    record StringChars(String string) implements CharSequence {
        @Override
        public int length() {
            return string.length() + 2;
        }

        @Override
        public char charAt(int index) {
            int n = string.length();
            return index < n ? string.charAt(index) : (char) (index == n ? n : n >>> 16);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            throw new UnsupportedOperationException();
        }
    }

    // putLong: four 16-bit units, least significant first
    record LongChars(long value) implements CharSequence {
        @Override
        public int length() {
            return 4;
        }

        @Override
        public char charAt(int index) {
            return (char) (value >>> (16 * index));
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void the_one_shot_hash_is_the_streams() {
        for (String s : new String[]{"", "a", "user:000123:profile", "ü€𝄞", "x".repeat(70_000)}) {
            org.assertj.core.api.Assertions.assertThat(XXH3.hashCharsTo128Bits(new StringChars(s)))
                    .isEqualTo(XXH3.hashStream().putString(s).get());
        }
        for (String s : new String[]{"", "a", "user:000123:profile", "ü€𝄞", "x".repeat(70_000)}) {
            org.assertj.core.api.Assertions.assertThat(XXH3.hashBytesTo128Bits(stringBytes(s)))
                    .isEqualTo(XXH3.hashStream().putString(s).get());
        }
        for (long v : new long[]{0, -1, 42, Long.MIN_VALUE, Long.MAX_VALUE, 123_456_789L}) {
            org.assertj.core.api.Assertions.assertThat(XXH3.hashBytesTo128Bits(longBytes(v)))
                    .isEqualTo(XXH3.hashStream().putLong(v).get());
            org.assertj.core.api.Assertions.assertThat(XXH3.hashCharsTo128Bits(new LongChars(v)))
                    .isEqualTo(XXH3.hashStream().putLong(v).get());
        }
    }

    @Test
    void run() throws Exception {
        for (int threads : new int[]{1, 8}) {
        Options options = new OptionsBuilder()
                .include(HashStreamBenchmarkTest.class.getName() + "\\.(perThread|fresh|oneShot|oneShotBytes|pooled"
                        + "|pooledPadded|hashBuiltIn|customStream|customHasher)$")
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
