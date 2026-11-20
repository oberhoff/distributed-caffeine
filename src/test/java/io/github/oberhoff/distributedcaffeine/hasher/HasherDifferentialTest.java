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
package io.github.oberhoff.distributedcaffeine.hasher;

import com.dynatrace.hash4j.hashing.HashFunnel;
import com.dynatrace.hash4j.hashing.HashSink;
import com.dynatrace.hash4j.hashing.HashStream128;
import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hashing;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.nio.CharBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Hasher} against hash4j's XXH3-128 stream, on random sequences of every put method: the hashes of {@link
 * Hashable}s and hash providers are hash4j's stream encoding, and must stay bit for bit what it gives, whatever
 * implements them; an unordered iterable put through a funnel, Hasher's own encoding, is written out on the stream.
 * Sizes cross the stream's buffer and the hasher's own; strings hold every kind of char, unpaired surrogates
 * included; unordered iterables cover lists, other collections and plain iterables of every size the stream sorts
 * differently.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class HasherDifferentialTest {

    // per build; the nightly runs 100,000 (-Dhasher.sequences=100000)
    private static final int SEQUENCES = Integer.getInteger("hasher.sequences", 3_000);
    private static final Hasher.Funnel<String> FUNNEL = (value, hasher) -> hasher.putString(value);
    private static final HashFunnel<String> STRING_FUNNEL = (value, sink) -> sink.putString(value);

    // one put, applied to each side: Hasher, and hash4j's stream
    private interface Put {
        void to(Hasher hasher);

        void to(HashSink sink);
    }

    @Test
    void every_put_hashes_as_hash4j_s_stream() {
        Random random = new Random(20261119);
        for (int sequence = 0; sequence < SEQUENCES; sequence++) {
            List<String> names = new ArrayList<>();
            List<Put> puts = new ArrayList<>();
            int count = 1 + random.nextInt(12);
            for (int i = 0; i < count; i++) {
                add(random, names, puts);
            }
            Hasher hasher = new Hasher();
            HashStream128 stream = Hashing.xxh3_128().hashStream();
            for (Put put : puts) {
                put.to(hasher);
                put.to(stream);
            }

            assertThat(hasher.getHash().toHex()).as("sequence %s: %s", sequence, names)
                    .isEqualTo(HexFormat.of().formatHex(stream.get().toByteArray()));
        }
    }

    private static void add(Random random, List<String> names, List<Put> puts) {
        int kind = random.nextInt(46);
        Consumer<Put> put = puts::add;
        switch (kind) {
            case 0 -> {
                byte v = (byte) random.nextInt();
                named(names, "putByte", put, hasher -> hasher.putByte(v), sink -> sink.putByte(v));
            }
            case 1 -> {
                byte[] x = bytes(random);
                named(names, "putBytes", put, hasher -> hasher.putBytes(x), sink -> sink.putBytes(x));
            }
            case 2 -> {
                byte[] x = bytes(random);
                int off = random.nextInt(x.length + 1);
                int len = random.nextInt(x.length - off + 1);
                named(names, "putBytes(off, len)", put, hasher -> hasher.putBytes(x, off, len),
                        sink -> sink.putBytes(x, off, len));
            }
            case 3 -> {
                // a long run of bytes, across the hasher's own buffer
                byte[] x = new byte[5_000 + random.nextInt(5_000)];
                random.nextBytes(x);
                named(names, "putBytes, long", put, hasher -> hasher.putBytes(x), sink -> sink.putBytes(x));
            }
            case 4 -> {
                byte[] x = bytes(random);
                named(names, "putByteArray", put, hasher -> hasher.putByteArray(x), sink -> sink.putByteArray(x));
            }
            case 5 -> {
                boolean v = random.nextBoolean();
                named(names, "putBoolean", put, hasher -> hasher.putBoolean(v), sink -> sink.putBoolean(v));
            }
            case 6 -> {
                boolean[] x = booleans(random);
                named(names, "putBooleans", put, hasher -> hasher.putBooleans(x), sink -> sink.putBooleans(x));
            }
            case 7 -> {
                boolean[] x = booleans(random);
                int off = random.nextInt(x.length + 1);
                int len = random.nextInt(x.length - off + 1);
                named(names, "putBooleans(off, len)", put, hasher -> hasher.putBooleans(x, off, len),
                        sink -> sink.putBooleans(x, off, len));
            }
            case 8 -> {
                boolean[] x = booleans(random);
                named(names, "putBooleanArray", put, hasher -> hasher.putBooleanArray(x),
                        sink -> sink.putBooleanArray(x));
            }
            case 9 -> {
                short v = (short) random.nextInt();
                named(names, "putShort", put, hasher -> hasher.putShort(v), sink -> sink.putShort(v));
            }
            case 10 -> {
                short[] x = shorts(random);
                named(names, "putShorts", put, hasher -> hasher.putShorts(x), sink -> sink.putShorts(x));
            }
            case 11 -> {
                short[] x = shorts(random);
                int off = random.nextInt(x.length + 1);
                int len = random.nextInt(x.length - off + 1);
                named(names, "putShorts(off, len)", put, hasher -> hasher.putShorts(x, off, len),
                        sink -> sink.putShorts(x, off, len));
            }
            case 12 -> {
                short[] x = shorts(random);
                named(names, "putShortArray", put, hasher -> hasher.putShortArray(x), sink -> sink.putShortArray(x));
            }
            case 13 -> {
                char v = (char) random.nextInt();
                named(names, "putChar", put, hasher -> hasher.putChar(v), sink -> sink.putChar(v));
            }
            case 14 -> {
                char[] x = string(random).toCharArray();
                named(names, "putChars(char[])", put, hasher -> hasher.putChars(x), sink -> sink.putChars(x));
            }
            case 15 -> {
                char[] x = string(random).toCharArray();
                int off = random.nextInt(x.length + 1);
                int len = random.nextInt(x.length - off + 1);
                named(names, "putChars(char[], off, len)", put, hasher -> hasher.putChars(x, off, len),
                        sink -> sink.putChars(x, off, len));
            }
            case 16 -> {
                String s = string(random);
                named(names, "putChars(String)", put, hasher -> hasher.putChars(s), sink -> sink.putChars(s));
            }
            case 17 -> {
                CharSequence s = CharBuffer.wrap(string(random));
                named(names, "putChars(CharBuffer)", put, hasher -> hasher.putChars(s), sink -> sink.putChars(s));
            }
            case 18 -> {
                String s = string(random);
                named(names, "putCharsUTF8", put, hasher -> hasher.putCharsUTF8(s), sink -> sink.putCharsUTF8(s));
            }
            case 19 -> {
                String s = string(random);
                named(names, "putStringUTF8", put, hasher -> hasher.putStringUTF8(s), sink -> sink.putStringUTF8(s));
            }
            case 20 -> {
                char[] x = string(random).toCharArray();
                named(names, "putCharArray", put, hasher -> hasher.putCharArray(x), sink -> sink.putCharArray(x));
            }
            case 21 -> {
                String s = string(random);
                named(names, "putString", put, hasher -> hasher.putString(s), sink -> sink.putString(s));
            }
            case 22 -> {
                int v = random.nextInt();
                named(names, "putInt", put, hasher -> hasher.putInt(v), sink -> sink.putInt(v));
            }
            case 23 -> {
                int[] x = ints(random);
                named(names, "putInts", put, hasher -> hasher.putInts(x), sink -> sink.putInts(x));
            }
            case 24 -> {
                int[] x = ints(random);
                int off = random.nextInt(x.length + 1);
                int len = random.nextInt(x.length - off + 1);
                named(names, "putInts(off, len)", put, hasher -> hasher.putInts(x, off, len),
                        sink -> sink.putInts(x, off, len));
            }
            case 25 -> {
                int[] x = ints(random);
                named(names, "putIntArray", put, hasher -> hasher.putIntArray(x), sink -> sink.putIntArray(x));
            }
            case 26 -> {
                long v = random.nextLong();
                named(names, "putLong", put, hasher -> hasher.putLong(v), sink -> sink.putLong(v));
            }
            case 27 -> {
                long[] x = longs(random);
                named(names, "putLongs", put, hasher -> hasher.putLongs(x), sink -> sink.putLongs(x));
            }
            case 28 -> {
                long[] x = longs(random);
                int off = random.nextInt(x.length + 1);
                int len = random.nextInt(x.length - off + 1);
                named(names, "putLongs(off, len)", put, hasher -> hasher.putLongs(x, off, len),
                        sink -> sink.putLongs(x, off, len));
            }
            case 29 -> {
                long[] x = longs(random);
                named(names, "putLongArray", put, hasher -> hasher.putLongArray(x), sink -> sink.putLongArray(x));
            }
            case 30 -> {
                float v = Float.intBitsToFloat(random.nextInt());
                named(names, "putFloat", put, hasher -> hasher.putFloat(v), sink -> sink.putFloat(v));
            }
            case 31 -> {
                float[] x = floats(random);
                int off = random.nextInt(x.length + 1);
                int len = random.nextInt(x.length - off + 1);
                named(names, "putFloats(off, len)", put, hasher -> hasher.putFloats(x, off, len),
                        sink -> sink.putFloats(x, off, len));
            }
            case 32 -> {
                float[] x = floats(random);
                named(names, "putFloatArray", put, hasher -> hasher.putFloats(x).putFloatArray(x), sink -> {
                    sink.putFloats(x);
                    sink.putFloatArray(x);
                });
            }
            case 33 -> {
                double v = Double.longBitsToDouble(random.nextLong());
                named(names, "putDouble", put, hasher -> hasher.putDouble(v), sink -> sink.putDouble(v));
            }
            case 34 -> {
                double[] x = doubles(random);
                int off = random.nextInt(x.length + 1);
                int len = random.nextInt(x.length - off + 1);
                named(names, "putDoubles(off, len)", put, hasher -> hasher.putDoubles(x, off, len),
                        sink -> sink.putDoubles(x, off, len));
            }
            case 35 -> {
                double[] x = doubles(random);
                named(names, "putDoubleArray", put, hasher -> hasher.putDoubles(x).putDoubleArray(x), sink -> {
                    sink.putDoubles(x);
                    sink.putDoubleArray(x);
                });
            }
            case 36 -> {
                UUID v = new UUID(random.nextLong(), random.nextLong());
                named(names, "putUUID", put, hasher -> hasher.putUUID(v), sink -> sink.putUUID(v));
            }
            case 37 -> {
                String s = string(random);
                named(names, "put(funnel)", put, hasher -> hasher.put(s, FUNNEL), sink -> sink.put(s, STRING_FUNNEL));
            }
            case 38 -> {
                @Nullable String s = random.nextBoolean() ? null : string(random);
                named(names, "putNullable", put, hasher -> hasher.putNullable(s, FUNNEL),
                        sink -> sink.putNullable(s, STRING_FUNNEL));
            }
            case 39 -> {
                List<String> data = strings(random);
                named(names, "putOrderedIterable", put, hasher -> hasher.putOrderedIterable(data, FUNNEL),
                        sink -> sink.putOrderedIterable(data, STRING_FUNNEL));
            }
            case 40 -> {
                Iterable<String> data = iterable(random, strings(random));
                ToLongFunction<String> elementHash = value -> Hashing.komihash5_0().hashCharsToLong(value);
                named(names, "putUnorderedIterable(" + data.getClass().getSimpleName() + ", function)", put,
                        hasher -> hasher.putUnorderedIterable(data, elementHash),
                        sink -> sink.putUnorderedIterable(data, elementHash));
            }
            case 41, 42 -> {
                // ours: each element's XXH3-128 over its values as two longs, the least significant first, sorted by
                // that one, then by the other, then their number; written out here on hash4j's stream
                Iterable<String> data = iterable(random, strings(random));
                named(names, "putUnorderedIterable(" + data.getClass().getSimpleName() + ", funnel)", put,
                        hasher -> hasher.putUnorderedIterable(data, FUNNEL),
                        sink -> {
                            List<HashValue128> hashes = new ArrayList<>();
                            data.forEach(value -> hashes.add(Hashing.xxh3_128().hashStream().putString(value).get()));
                            hashes.sort(Comparator.comparingLong(HashValue128::getLeastSignificantBits)
                                    .thenComparingLong(HashValue128::getMostSignificantBits));
                            hashes.forEach(hash -> sink.putLong(hash.getLeastSignificantBits())
                                    .putLong(hash.getMostSignificantBits()));
                            sink.putInt(hashes.size());
                        });
            }
            case 43 -> {
                Optional<String> v = random.nextBoolean() ? Optional.empty() : Optional.of(string(random));
                named(names, "putOptional", put, hasher -> hasher.putOptional(v, FUNNEL),
                        sink -> sink.putOptional(v, STRING_FUNNEL));
            }
            case 44 -> {
                OptionalInt i = random.nextBoolean() ? OptionalInt.empty() : OptionalInt.of(random.nextInt());
                OptionalLong l = random.nextBoolean() ? OptionalLong.empty() : OptionalLong.of(random.nextLong());
                named(names, "putOptionalInt, putOptionalLong", put,
                        hasher -> hasher.putOptionalInt(i).putOptionalLong(l), sink -> {
                            sink.putOptionalInt(i);
                            sink.putOptionalLong(l);
                        });
            }
            default -> {
                OptionalDouble v = random.nextBoolean() ? OptionalDouble.empty()
                        : OptionalDouble.of(random.nextDouble());
                named(names, "putOptionalDouble", put, hasher -> hasher.putOptionalDouble(v),
                        sink -> sink.putOptionalDouble(v));
            }
        }
    }

    private static void named(List<String> names, String name, Consumer<Put> puts, Consumer<Hasher> toHasher,
                              Consumer<HashSink> toSink) {
        names.add(name);
        puts.accept(new Put() {
            @Override
            public void to(Hasher hasher) {
                toHasher.accept(hasher);
            }

            @Override
            public void to(HashSink sink) {
                toSink.accept(sink);
            }
        });
    }

    // mostly short, now and then long enough to cross the stream's buffer and the hasher's own
    private static int length(Random random) {
        int roll = random.nextInt(20);
        return roll == 0 ? 2_000 + random.nextInt(9_000) : roll < 3 ? 100 + random.nextInt(300) : random.nextInt(40);
    }

    private static byte[] bytes(Random random) {
        byte[] x = new byte[length(random)];
        random.nextBytes(x);
        return x;
    }

    private static boolean[] booleans(Random random) {
        boolean[] x = new boolean[length(random)];
        for (int i = 0; i < x.length; i++) {
            x[i] = random.nextBoolean();
        }
        return x;
    }

    private static short[] shorts(Random random) {
        short[] x = new short[length(random)];
        for (int i = 0; i < x.length; i++) {
            x[i] = (short) random.nextInt();
        }
        return x;
    }

    private static int[] ints(Random random) {
        return random.ints(length(random)).toArray();
    }

    private static long[] longs(Random random) {
        return random.longs(length(random)).toArray();
    }

    private static float[] floats(Random random) {
        float[] x = new float[length(random)];
        for (int i = 0; i < x.length; i++) {
            x[i] = Float.intBitsToFloat(random.nextInt());
        }
        return x;
    }

    private static double[] doubles(Random random) {
        return random.longs(length(random)).mapToDouble(Double::longBitsToDouble).toArray();
    }

    // ASCII, Latin-1, the rest of the BMP, valid surrogate pairs and unpaired surrogates
    private static String string(Random random) {
        int length = length(random);
        StringBuilder builder = new StringBuilder(length);
        while (builder.length() < length) {
            switch (random.nextInt(6)) {
                case 0, 1 -> builder.append((char) (32 + random.nextInt(95)));
                case 2 -> builder.append((char) (128 + random.nextInt(128)));
                case 3 -> builder.append((char) (0x800 + random.nextInt(0xD000)));
                case 4 -> builder.appendCodePoint(0x10000 + random.nextInt(0x100000));
                default -> builder.append((char) (0xD800 + random.nextInt(0x800)));
            }
        }
        return builder.toString();
    }

    private static List<String> strings(Random random) {
        int roll = random.nextInt(10);
        int size = roll == 0 ? 50 + random.nextInt(200) : random.nextInt(20);
        List<String> strings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String string = string(random);
            strings.add(random.nextInt(4) == 0 ? "" : string.substring(0, Math.min(8, string.length())));
        }
        return strings;
    }

    // a random-access list, another collection, a set, or a plain iterable: the stream takes another way for each
    private static Iterable<String> iterable(Random random, List<String> strings) {
        return switch (random.nextInt(4)) {
            case 0 -> strings;
            case 1 -> new ArrayDeque<>(strings);
            case 2 -> new LinkedHashSet<>(strings);
            default -> new PlainIterable(strings);
        };
    }

    private record PlainIterable(List<String> strings) implements Iterable<String> {
        @Override
        public Iterator<String> iterator() {
            return strings.iterator();
        }
    }
}
