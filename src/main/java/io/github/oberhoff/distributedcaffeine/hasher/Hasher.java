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

import com.dynatrace.hash4j.hashing.HashStream128;
import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hasher128;
import com.dynatrace.hash4j.hashing.Hashing;
import org.jspecify.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.ToLongFunction;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * Computes a key's hash from its parts: put each value that {@code equals} compares, then call {@link #getHash()}. The
 * hash is 128 bits (XXH3); for a string, {@link #putString(String)} is the usual choice, as it also marks where the
 * string ends, so that {@code ("ab", "c")} and {@code ("a", "bc")} hash differently. A hasher is meant for one hash at
 * a time and is not thread-safe.
 * <p>
 * Keys of the built-in kinds ({@link String}, {@link Long}, {@link Integer}, {@link UUID} and {@link Enum enums}) need
 * no hasher: the cache hashes them itself ({@link #builtInHash(Object)}). Both formats, the built-in one and that of
 * the put methods, are fixed: a hash once stored is computed alike by every later version.
 *
 * @author Andreas Oberhoff
 */
@SuppressWarnings("UnusedReturnValue") // a fluent API: each put returns the hasher, chained or not
public final class Hasher {

    // XXH3-128 unseeded, for the hashes of Hashables and hash providers; built-in kinds are seeded (below)
    private static final Hasher128 XXH3 = Hashing.xxh3_128();

    // The stored hash format of the built-in kinds (Store.HASH_FORMAT), fixed for good, by one rule: a value's hash is
    // XXH3-128, seeded by the value's kind, over the value's natural bytes. The whole input is hashed at once, so its
    // length is part of it, and the seed separates the kinds, so that bytes alike in two kinds never hash alike. Each
    // seed is the 64-bit XXH3 hash of "distributed-caffeine:" and the kind's name, over its chars (HasherBuiltInTest
    // recomputes them). The bytes per kind:
    // - String: its chars, UTF-16LE.
    // - Long, Integer: its 8 or 4 bytes, little-endian.
    // - UUID: its least significant long, then its most significant, each little-endian.
    // - enum constant: the name of its declaring class, the char U+0000, the constant's name, as chars.
    // Little-endian is the order the usual CPUs and XXH3's own reading use, so nothing is swapped; U+0000 occurs in no
    // class name. A stored hash, built-in or custom, is the 32 lowercase hex characters of its 16 bytes (Hash.toHex)
    private static final Hasher128 STRING_KIND = Hashing.xxh3_128(0xE68419576BCD7641L);
    private static final Hasher128 LONG_KIND = Hashing.xxh3_128(0x20C220B12413CD8CL);
    private static final Hasher128 INTEGER_KIND = Hashing.xxh3_128(0x14C32D217F455375L);
    private static final Hasher128 UUID_KIND = Hashing.xxh3_128(0x147950E84C3FFAFFL);
    private static final Hasher128 ENUM_KIND = Hashing.xxh3_128(0x9BA907CB5ADD779EL);

    // the order of the elements of an unordered iterable, part of the format: by their hashes' least significant
    // longs, then their most significant ones, as signed longs
    private static final Comparator<Hash> ELEMENT_ORDER = Comparator.comparingLong(Hash::leastSignificantBits)
            .thenComparingLong(Hash::mostSignificantBits);

    // The values are encoded here, as hash4j's streams encode them (every value little-endian, a string's chars, then
    // its length, and so on; HasherDifferentialTest compares the two on every method), into a small array hashed at
    // once by getHash(): no stream with its 264-byte buffer per hasher, which the core creates for every access to a
    // key hashed by a Hashable or hash provider. Beyond FLUSH bytes, the array goes into a stream as raw bytes, so a
    // large value takes no more memory; XXH3 hashes the same bytes alike streamed or at once
    private static final int INITIAL = 64;
    private static final int FLUSH = 4096;
    // the chars of a string copied at a time, so that a long string takes no scratch array as long
    private static final int SCRATCH = 256;
    private static final VarHandle SHORT = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private byte[] bytes = new byte[INITIAL];
    // the scratch array of putString, made on first use
    private char @Nullable [] stringChars;
    private int size;
    private @Nullable HashStream128 stream;
    // whether any value was put: getHash() refuses to hash nothing, which is no key's values
    private boolean hasData;

    /**
     * Constructs a hasher.
     */
    public Hasher() {
        // nothing put yet
    }

    /**
     * Puts a byte.
     *
     * @param value the value
     * @return this hasher
     */
    public Hasher putByte(byte value) {
        writeByte(value);
        return this;
    }

    /**
     * Puts the bytes, without their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putBytes(byte[] values) {
        return putBytes(values, 0, values.length);
    }

    /**
     * Puts {@code length} bytes from index {@code offset}, without their number.
     *
     * @param values the values
     * @param offset the index of the first value
     * @param length the number of values
     * @return this hasher
     */
    public Hasher putBytes(byte[] values, int offset, int length) {
        hasData = true;
        int from = offset;
        int left = length;
        while (left > 0) {
            if (size == bytes.length) {
                makeRoom();
            }
            int chunk = Math.min(left, bytes.length - size);
            System.arraycopy(values, from, bytes, size, chunk);
            size += chunk;
            from += chunk;
            left -= chunk;
        }
        return this;
    }

    /**
     * Puts the bytes, then their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putByteArray(byte[] values) {
        putBytes(values);
        return putInt(values.length);
    }

    /**
     * Puts a boolean as one byte, 1 or 0.
     *
     * @param value the value
     * @return this hasher
     */
    public Hasher putBoolean(boolean value) {
        writeByte(value ? 1 : 0);
        return this;
    }

    /**
     * Puts the booleans, one byte each, without their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putBooleans(boolean[] values) {
        return putBooleans(values, 0, values.length);
    }

    /**
     * Puts {@code length} booleans from index {@code offset}, one byte each, without their number.
     *
     * @param values the values
     * @param offset the index of the first value
     * @param length the number of values
     * @return this hasher
     */
    public Hasher putBooleans(boolean[] values, int offset, int length) {
        hasData = true;
        for (int i = offset; i < offset + length; i++) {
            writeByte(values[i] ? 1 : 0);
        }
        return this;
    }

    /**
     * Puts the booleans, one byte each, then their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putBooleanArray(boolean[] values) {
        putBooleans(values);
        return putInt(values.length);
    }

    /**
     * Puts a short.
     *
     * @param value the value
     * @return this hasher
     */
    public Hasher putShort(short value) {
        writeShort(value);
        return this;
    }

    /**
     * Puts the shorts, without their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putShorts(short[] values) {
        return putShorts(values, 0, values.length);
    }

    /**
     * Puts {@code length} shorts from index {@code offset}, without their number.
     *
     * @param values the values
     * @param offset the index of the first value
     * @param length the number of values
     * @return this hasher
     */
    public Hasher putShorts(short[] values, int offset, int length) {
        hasData = true;
        for (int i = offset; i < offset + length; i++) {
            writeShort(values[i]);
        }
        return this;
    }

    /**
     * Puts the shorts, then their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putShortArray(short[] values) {
        putShorts(values);
        return putInt(values.length);
    }

    /**
     * Puts a char, as its two bytes.
     *
     * @param value the value
     * @return this hasher
     */
    public Hasher putChar(char value) {
        writeShort(value);
        return this;
    }

    /**
     * Puts the chars, without their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putChars(char[] values) {
        return putChars(values, 0, values.length);
    }

    /**
     * Puts {@code length} chars from index {@code offset}, without their number.
     *
     * @param values the values
     * @param offset the index of the first value
     * @param length the number of values
     * @return this hasher
     */
    public Hasher putChars(char[] values, int offset, int length) {
        hasData = true;
        int i = offset;
        int end = offset + length;
        while (i < end) {
            int fit = room(end - i);
            writeChars(values, i, fit);
            i += fit;
        }
        return this;
    }

    /**
     * Puts the chars, then their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putCharArray(char[] values) {
        putChars(values);
        return putInt(values.length);
    }

    /**
     * Puts an int.
     *
     * @param value the value
     * @return this hasher
     */
    public Hasher putInt(int value) {
        writeInt(value);
        return this;
    }

    /**
     * Puts the ints, without their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putInts(int[] values) {
        return putInts(values, 0, values.length);
    }

    /**
     * Puts {@code length} ints from index {@code offset}, without their number.
     *
     * @param values the values
     * @param offset the index of the first value
     * @param length the number of values
     * @return this hasher
     */
    public Hasher putInts(int[] values, int offset, int length) {
        hasData = true;
        for (int i = offset; i < offset + length; i++) {
            writeInt(values[i]);
        }
        return this;
    }

    /**
     * Puts the ints, then their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putIntArray(int[] values) {
        putInts(values);
        return putInt(values.length);
    }

    /**
     * Puts a long.
     *
     * @param value the value
     * @return this hasher
     */
    public Hasher putLong(long value) {
        writeLong(value);
        return this;
    }

    /**
     * Puts the longs, without their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putLongs(long[] values) {
        return putLongs(values, 0, values.length);
    }

    /**
     * Puts {@code length} longs from index {@code offset}, without their number.
     *
     * @param values the values
     * @param offset the index of the first value
     * @param length the number of values
     * @return this hasher
     */
    public Hasher putLongs(long[] values, int offset, int length) {
        hasData = true;
        for (int i = offset; i < offset + length; i++) {
            writeLong(values[i]);
        }
        return this;
    }

    /**
     * Puts the longs, then their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putLongArray(long[] values) {
        putLongs(values);
        return putInt(values.length);
    }

    /**
     * Puts a float, as its raw bits.
     *
     * @param value the value
     * @return this hasher
     */
    public Hasher putFloat(float value) {
        writeInt(Float.floatToRawIntBits(value));
        return this;
    }

    /**
     * Puts the floats, as their raw bits, without their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putFloats(float[] values) {
        return putFloats(values, 0, values.length);
    }

    /**
     * Puts {@code length} floats from index {@code offset}, as their raw bits, without their number.
     *
     * @param values the values
     * @param offset the index of the first value
     * @param length the number of values
     * @return this hasher
     */
    public Hasher putFloats(float[] values, int offset, int length) {
        hasData = true;
        for (int i = offset; i < offset + length; i++) {
            writeInt(Float.floatToRawIntBits(values[i]));
        }
        return this;
    }

    /**
     * Puts the floats, as their raw bits, then their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putFloatArray(float[] values) {
        putFloats(values);
        return putInt(values.length);
    }

    /**
     * Puts a double, as its raw bits.
     *
     * @param value the value
     * @return this hasher
     */
    public Hasher putDouble(double value) {
        writeLong(Double.doubleToRawLongBits(value));
        return this;
    }

    /**
     * Puts the doubles, as their raw bits, without their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putDoubles(double[] values) {
        return putDoubles(values, 0, values.length);
    }

    /**
     * Puts {@code length} doubles from index {@code offset}, as their raw bits, without their number.
     *
     * @param values the values
     * @param offset the index of the first value
     * @param length the number of values
     * @return this hasher
     */
    public Hasher putDoubles(double[] values, int offset, int length) {
        hasData = true;
        for (int i = offset; i < offset + length; i++) {
            writeLong(Double.doubleToRawLongBits(values[i]));
        }
        return this;
    }

    /**
     * Puts the doubles, as their raw bits, then their number.
     *
     * @param values the values
     * @return this hasher
     */
    public Hasher putDoubleArray(double[] values) {
        putDoubles(values);
        return putInt(values.length);
    }

    /**
     * Puts the chars, without their number.
     *
     * @param chars the chars
     * @return this hasher
     */
    public Hasher putChars(CharSequence chars) {
        hasData = true;
        int length = chars.length();
        if (chars instanceof String string) {
            putChars(string, length);
            return this;
        }
        int i = 0;
        while (i < length) {
            int fit = room(length - i);
            byte[] b = bytes;
            int at = size;
            for (int stop = i + fit; i < stop; i++) {
                char ch = chars.charAt(i);
                b[at++] = (byte) ch;
                b[at++] = (byte) (ch >>> 8);
            }
            size = at;
        }
        return this;
    }

    /**
     * Puts the string's chars, then their number, which marks where the string ends.
     *
     * @param string the string
     * @return this hasher
     */
    public Hasher putString(String string) {
        putChars(string);
        return putInt(string.length());
    }

    /**
     * Puts the chars as UTF-8, an unpaired surrogate as {@code ?}, without their number.
     *
     * @param chars the chars
     * @return this hasher
     */
    public Hasher putCharsUTF8(CharSequence chars) {
        putUtf8(chars);
        return this;
    }

    /**
     * Puts the string as UTF-8, an unpaired surrogate as {@code ?}, then the number of those bytes.
     *
     * @param string the string
     * @return this hasher
     */
    public Hasher putStringUTF8(String string) {
        return putInt(putUtf8(string));
    }

    /**
     * Puts a UUID: its least significant long, then its most significant.
     *
     * @param uuid the UUID
     * @return this hasher
     */
    public Hasher putUUID(UUID uuid) {
        writeLong(uuid.getLeastSignificantBits());
        writeLong(uuid.getMostSignificantBits());
        return this;
    }

    /**
     * Puts a hash as its 16 bytes ({@link Hash#toBytes}). For a key's part with a hash of its own: a part of a built-in
     * kind as the cache hashes it, {@code putHash(Hasher.builtInHash(part))}, or a part's {@link Hashable} hash.
     *
     * @param hash the hash
     * @return this hasher
     */
    public Hasher putHash(Hash hash) {
        writeLong(hash.leastSignificantBits());
        writeLong(hash.mostSignificantBits());
        return this;
    }

    /**
     * Puts an object's values, as the funnel puts them.
     *
     * @param <T>    the type of the object
     * @param object the object
     * @param funnel puts an object's values
     * @return this hasher
     */
    public <T> Hasher put(T object, Funnel<? super T> funnel) {
        hasData = true;
        funnel.put(object, this);
        return this;
    }

    /**
     * Puts an object's values, as the funnel puts them, then {@code true}; for {@code null}, {@code false}.
     *
     * @param <T>    the type of the object
     * @param object the object
     * @param funnel puts an object's values
     * @return this hasher
     */
    public <T> Hasher putNullable(@Nullable T object, Funnel<? super T> funnel) {
        if (object != null) {
            put(object, funnel);
        }
        return putBoolean(object != null);
    }

    /**
     * Puts the elements' values in their order, as the funnel puts them, then their number.
     *
     * @param <T> the type of the elements
     * @param data   the elements
     * @param funnel puts an object's values
     * @return this hasher
     */
    public <T> Hasher putOrderedIterable(Iterable<T> data, Funnel<? super T> funnel) {
        hasData = true;
        int count = 0;
        for (T element : data) {
            funnel.put(element, this);
            count++;
        }
        return putInt(count);
    }

    /**
     * Puts the elements whatever their order, as a set's equal elements in any order: each element's 64-bit hash from
     * the function, sorted, then their number. With 64-bit element hashes, a key that is a set of one element is only
     * as unique as 64 bits; {@link #putUnorderedIterable(Iterable, Funnel)} keeps 128.
     *
     * @param <T> the type of the elements
     * @param data                the elements
     * @param elementHashFunction returns an element's 64-bit hash
     * @return this hasher
     */
    public <T> Hasher putUnorderedIterable(Iterable<T> data, ToLongFunction<? super T> elementHashFunction) {
        requireNonNull(data);
        requireNonNull(elementHashFunction);
        hasData = true;
        long[] hashes = new long[data instanceof Collection<?> collection ? collection.size() : 8];
        int count = 0;
        for (T element : data) {
            if (count == hashes.length) {
                hashes = Arrays.copyOf(hashes, Math.max(8, hashes.length * 2));
            }
            hashes[count++] = elementHashFunction.applyAsLong(element);
        }
        Arrays.sort(hashes, 0, count);
        putLongs(hashes, 0, count);
        return putInt(count);
    }

    /**
     * Puts the elements whatever their order, as a set's equal elements in any order: each element's 128-bit hash over
     * the values the funnel puts, sorted, then their number.
     *
     * @param <T> the type of the elements
     * @param data   the elements
     * @param funnel puts an object's values
     * @return this hasher
     */
    public <T> Hasher putUnorderedIterable(Iterable<T> data, Funnel<? super T> funnel) {
        requireNonNull(data);
        requireNonNull(funnel);
        // 128-bit element hashes, so that a key that is a set keeps the 128 bits of every other key; each put as its
        // two longs, the least significant first, in ELEMENT_ORDER (signed, as hash4j sorts 64-bit ones)
        hasData = true;
        Hasher elementHasher = new Hasher();
        List<Hash> hashes = StreamSupport.stream(data.spliterator(), false)
                .map(element -> elementHasher.elementHash(element, funnel))
                .sorted(ELEMENT_ORDER)
                .toList();
        for (Hash hash : hashes) {
            putLong(hash.leastSignificantBits());
            putLong(hash.mostSignificantBits());
        }
        return putInt(hashes.size());
    }

    /**
     * Puts a present value, as the funnel puts it, then {@code true}; for an empty one, {@code false}.
     *
     * @param <T>    the type of the object
     * @param object the object
     * @param funnel puts an object's values
     * @return this hasher
     */
    @SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "java:S3553"}) // hashes an Optional a key holds
    public <T> Hasher putOptional(Optional<T> object, Funnel<? super T> funnel) {
        object.ifPresent(value -> funnel.put(value, this));
        return putBoolean(object.isPresent());
    }

    /**
     * Puts a present value, then {@code true}; for an empty one, {@code false}.
     *
     * @param value the value
     * @return this hasher
     */
    @SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "java:S3553"}) // hashes an Optional a key holds
    public Hasher putOptionalInt(OptionalInt value) {
        if (value.isPresent()) {
            writeInt(value.getAsInt());
        }
        return putBoolean(value.isPresent());
    }

    /**
     * Puts a present value, then {@code true}; for an empty one, {@code false}.
     *
     * @param value the value
     * @return this hasher
     */
    @SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "java:S3553"}) // hashes an Optional a key holds
    public Hasher putOptionalLong(OptionalLong value) {
        if (value.isPresent()) {
            writeLong(value.getAsLong());
        }
        return putBoolean(value.isPresent());
    }

    /**
     * Puts a present value, as its raw bits, then {@code true}; for an empty one, {@code false}.
     *
     * @param value the value
     * @return this hasher
     */
    @SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "java:S3553"}) // hashes an Optional a key holds
    public Hasher putOptionalDouble(OptionalDouble value) {
        if (value.isPresent()) {
            writeLong(Double.doubleToRawLongBits(value.getAsDouble()));
        }
        return putBoolean(value.isPresent());
    }

    /**
     * Returns the hash of the values put so far. Further values may be put after it, and a later call hashes all of
     * them.
     *
     * @return the hash
     * @throws IllegalStateException if no value was put
     */
    public Hash getHash() {
        if (!hasData) {
            throw new IllegalStateException("Nothing to hash");
        }
        if (stream == null) {
            return of(XXH3.hashBytesTo128Bits(bytes, 0, size));
        }
        // a copy, so that the hasher's state stays as it was for values put after
        return of(stream.copy().putBytes(bytes, 0, size).get());
    }

    /**
     * Returns the hash of a value of a built-in kind: a {@link String}, {@link Long}, {@link Integer}, {@link UUID} or
     * {@link Enum enum} constant.
     *
     * @param value the value
     * @return the hash, or {@code null} if the value is of no built-in kind
     */
    public static @Nullable Hash builtInHash(Object value) {
        requireNonNull(value, "value cannot be null");
        if (value instanceof String string) {
            return of(STRING_KIND.hashCharsTo128Bits(string));
        }
        if (value instanceof Long longValue) {
            byte[] bytes = new byte[8];
            LONG.set(bytes, 0, (long) longValue);
            return of(LONG_KIND.hashBytesTo128Bits(bytes));
        }
        if (value instanceof Integer integerValue) {
            byte[] bytes = new byte[4];
            INT.set(bytes, 0, (int) integerValue);
            return of(INTEGER_KIND.hashBytesTo128Bits(bytes));
        }
        if (value instanceof UUID uuid) {
            byte[] bytes = new byte[16];
            LONG.set(bytes, 0, uuid.getLeastSignificantBits());
            LONG.set(bytes, 8, uuid.getMostSignificantBits());
            return of(UUID_KIND.hashBytesTo128Bits(bytes));
        }
        if (value instanceof Enum<?> constant) {
            // the declaring class: a constant with a body of its own is an anonymous subclass, named by its position
            String name = constant.getDeclaringClass().getName() + '\u0000' + constant.name();
            return of(ENUM_KIND.hashCharsTo128Bits(name));
        }
        return null;
    }

    // the position to write length bytes at, at most 8
    private int reserve(int length) {
        hasData = true;
        if (size + length > bytes.length) {
            makeRoom();
        }
        int at = size;
        size += length;
        return at;
    }

    // the array doubles up to FLUSH bytes, then goes into the stream; either leaves at least 8 bytes free
    private void makeRoom() {
        if (bytes.length < FLUSH) {
            bytes = Arrays.copyOf(bytes, bytes.length * 2);
        } else {
            flush();
        }
    }

    private void flush() {
        if (stream == null) {
            stream = XXH3.hashStream();
        }
        stream.putBytes(bytes, 0, size);
        size = 0;
    }

    // each reserves first: reserving may replace the array, which an argument read before it would miss
    private void writeByte(int value) {
        int at = reserve(1);
        bytes[at] = (byte) value;
    }

    private void writeShort(int value) {
        int at = reserve(2);
        SHORT.set(bytes, at, (short) value);
    }

    private void writeInt(int value) {
        int at = reserve(4);
        INT.set(bytes, at, value);
    }

    private void writeLong(long value) {
        int at = reserve(8);
        LONG.set(bytes, at, value);
    }

    // count chars little-endian, where room() made space for them: through the VarHandle, which the JIT turns into
    // plain stores, rather than a little-endian CharBuffer view, which takes two objects per call
    private void writeChars(char[] chars, int from, int count) {
        int at = size;
        for (int c = 0; c < count; c++) {
            SHORT.set(bytes, at + 2 * c, (short) chars[from + c]);
        }
        size = at + 2 * count;
    }

    // a string's chars in bulk, through a scratch array of up to SCRATCH chars: String.getChars copies them at once (a
    // Latin-1 string widened vectorized), charAt would check every index and the string's coding
    private void putChars(String string, int length) {
        int i = 0;
        while (i < length) {
            int fit = Math.min(room(length - i), SCRATCH);
            char[] chars = stringChars;
            if (chars == null || chars.length < fit) {
                chars = new char[Math.max(fit, Math.min(chars == null ? 0 : 2 * chars.length, SCRATCH))];
                stringChars = chars;
            }
            string.getChars(i, i + fit, chars, 0);
            writeChars(chars, 0, fit);
            i += fit;
        }
    }

    // room for up to chars chars, at least one, made at once: the array sized to all of them up to FLUSH bytes, or
    // emptied into the stream; returns how many fit
    private int room(int chars) {
        int needed = size + 2 * chars;
        if (needed > bytes.length) {
            if (needed <= FLUSH) {
                bytes = Arrays.copyOf(bytes, Math.max(bytes.length * 2, Integer.highestOneBit(needed - 1) << 1));
            } else if (bytes.length < FLUSH) {
                bytes = Arrays.copyOf(bytes, FLUSH);
            }
            if (bytes.length - size < 2) {
                flush();
            }
        }
        return Math.min(chars, (bytes.length - size) >>> 1);
    }

    // UTF-8, an unpaired surrogate as '?', as hash4j encodes it; returns the number of bytes
    private int putUtf8(CharSequence chars) {
        hasData = true;
        int count = 0;
        int length = chars.length();
        int i = 0;
        while (i < length) {
            char ch = chars.charAt(i++);
            if (ch < 0x80) {
                writeByte(ch);
                count += 1;
            } else if (ch < 0x800) {
                writeByte(0xC0 | (ch >>> 6));
                writeByte(0x80 | (ch & 0x3F));
                count += 2;
            } else if (Character.isSurrogate(ch)) {
                char next = i < length ? chars.charAt(i) : 0;
                if (Character.isHighSurrogate(ch) && Character.isLowSurrogate(next)) {
                    int codePoint = Character.toCodePoint(ch, next);
                    writeByte(0xF0 | (codePoint >>> 18));
                    writeByte(0x80 | ((codePoint >>> 12) & 0x3F));
                    writeByte(0x80 | ((codePoint >>> 6) & 0x3F));
                    writeByte(0x80 | (codePoint & 0x3F));
                    count += 4;
                    i++; // the low surrogate too
                } else {
                    writeByte('?');
                    count += 1;
                }
            } else {
                writeByte(0xE0 | (ch >>> 12));
                writeByte(0x80 | ((ch >>> 6) & 0x3F));
                writeByte(0x80 | (ch & 0x3F));
                count += 3;
            }
        }
        return count;
    }

    // an element's hash: XXH3-128 over its encoding, the one algorithm of all hashes here (not hash4j's XXH3-64), so
    // that a port of XXH3 would be of XXH3-128 alone; an element putting nothing hashes the empty input
    private <T> Hash elementHash(T element, Funnel<? super T> funnel) {
        size = 0;
        stream = null;
        funnel.put(element, this);
        HashStream128 flushed = stream;
        return of(flushed == null ? XXH3.hashBytesTo128Bits(bytes, 0, size) : flushed.putBytes(bytes, 0, size).get());
    }

    // hash4j's value as ours, so that no hash4j type is part of the API
    private static Hash of(HashValue128 value) {
        return new Hash(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    /**
     * Puts the values of an object into a hasher, as a {@link Hashable} puts its own: for the objects of
     * {@link #put(Object, Funnel)} and {@link #putOptional}, and the elements of iterables. For example, a key with a
     * set of strings and a nested object:
     * <pre>{@code
     * hasher.get()
     *         .putUnorderedIterable(setOfStrings, (string, h) -> h.putString(string))
     *         .put(object, (o, h) -> h.putString(o.getThis()).putLong(o.getThat()))
     *         .getHash();
     * }</pre>
     *
     * @param <T> the type of the objects
     */
    @FunctionalInterface
    public interface Funnel<T> {

        /**
         * Puts the object's values into the hasher.
         *
         * @param object the object
         * @param hasher the hasher
         */
        void put(T object, Hasher hasher);
    }
}
