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

import com.dynatrace.hash4j.hashing.ByteAccess;
import com.dynatrace.hash4j.hashing.HashFunnel;
import com.dynatrace.hash4j.hashing.HashSink;
import com.dynatrace.hash4j.hashing.HashStream128;
import com.dynatrace.hash4j.hashing.HashStream64;
import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hasher128;
import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import org.jspecify.annotations.Nullable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HexFormat;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.ToLongFunction;

import static java.util.Objects.requireNonNull;

/**
 * Computes a key's hash from its parts: put each value that {@code equals} compares, then call {@link #getHash()}. The
 * hash is 128 bits (xxh3, from <a href="https://github.com/dynatrace-oss/hash4j">hash4j</a>, whose {@code HashSink}
 * methods it offers); for a string, {@link #putString(String)} is the usual choice, as it also marks where the string
 * ends, so that {@code ("ab", "c")} and {@code ("a", "bc")} hash differently.
 *
 * @author Andreas Oberhoff
 */
public final class Hasher implements HashSink {

    // the algorithm of every stored hash
    private static final Hasher128 hasher128 = Hashing.xxh3_128();

    // the cache's own hashing of built-in keys (hashBuiltIn) reuses one stream per platform thread, reset before each
    // key: a fresh stream costs 416 bytes and up to 3.5 times the time (HashStreamBenchmarkTest). A virtual thread is
    // never reused, so there a stream per thread would be kept for nothing: it takes a fresh one, as JEP 444 advises.
    // Safe to reuse since hashing a built-in key calls no code of the application's
    @SuppressWarnings("java:S5164") // platform threads only, which are pooled and live as long as the application
    private static final ThreadLocal<HashStream128> STREAM = ThreadLocal.withInitial(hasher128::hashStream);
    // Thread.isVirtual(), from Java 21 on; absent before, where every thread is a platform thread
    private static final @Nullable MethodHandle IS_VIRTUAL = isVirtual();

    private final HashStream128 hashStream128;
    // tracks whether any value was put; used to reject "nothing to hash" without relying on the
    // algorithm-specific hash of empty input as a sentinel
    private boolean hasData;

    /**
     * Constructs a hasher.
     */
    public Hasher() {
        this.hashStream128 = hasher128.hashStream().reset();
        this.hasData = false;
    }

    @Override
    public Hasher putByte(byte v) {
        hashStream128.putByte(v);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putBytes(byte[] x) {
        hashStream128.putBytes(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putBytes(byte[] x, int off, int len) {
        hashStream128.putBytes(x, off, len);
        hasData = true;
        return this;
    }

    @Override
    public <T> Hasher putBytes(T input, long off, long len, ByteAccess<T> access) {
        hashStream128.putBytes(input, off, len, access);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putByteArray(byte[] x) {
        hashStream128.putByteArray(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putBoolean(boolean v) {
        hashStream128.putBoolean(v);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putBooleans(boolean[] x) {
        hashStream128.putBooleans(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putBooleans(boolean[] x, int off, int len) {
        hashStream128.putBooleans(x, off, len);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putBooleanArray(boolean[] x) {
        hashStream128.putBooleanArray(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putShort(short v) {
        hashStream128.putShort(v);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putShorts(short[] x) {
        hashStream128.putShorts(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putShorts(short[] x, int off, int len) {
        hashStream128.putShorts(x, off, len);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putShortArray(short[] x) {
        hashStream128.putShortArray(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putChar(char v) {
        hashStream128.putChar(v);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putChars(char[] x) {
        hashStream128.putChars(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putChars(char[] x, int off, int len) {
        hashStream128.putChars(x, off, len);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putChars(CharSequence c) {
        hashStream128.putChars(c);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putCharsUTF8(CharSequence c) {
        hashStream128.putCharsUTF8(c);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putStringUTF8(String s) {
        hashStream128.putStringUTF8(s);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putCharArray(char[] x) {
        hashStream128.putCharArray(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putString(String s) {
        hashStream128.putString(s);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putInt(int v) {
        hashStream128.putInt(v);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putInts(int[] x) {
        hashStream128.putInts(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putInts(int[] x, int off, int len) {
        hashStream128.putInts(x, off, len);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putIntArray(int[] x) {
        hashStream128.putIntArray(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putLong(long v) {
        hashStream128.putLong(v);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putLongs(long[] x) {
        hashStream128.putLongs(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putLongs(long[] x, int off, int len) {
        hashStream128.putLongs(x, off, len);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putLongArray(long[] x) {
        hashStream128.putLongArray(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putFloat(float v) {
        hashStream128.putFloat(v);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putFloats(float[] x) {
        hashStream128.putFloats(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putFloats(float[] x, int off, int len) {
        hashStream128.putFloats(x, off, len);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putFloatArray(float[] x) {
        hashStream128.putFloatArray(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putDouble(double v) {
        hashStream128.putDouble(v);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putDoubles(double[] x) {
        hashStream128.putDoubles(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putDoubles(double[] x, int off, int len) {
        hashStream128.putDoubles(x, off, len);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putDoubleArray(double[] x) {
        hashStream128.putDoubleArray(x);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putUUID(UUID uuid) {
        hashStream128.putUUID(uuid);
        hasData = true;
        return this;
    }

    @Override
    public <T> Hasher put(T obj, HashFunnel<T> funnel) {
        hashStream128.put(obj, funnel);
        hasData = true;
        return this;
    }

    @Override
    public <T> Hasher putNullable(@Nullable T obj, HashFunnel<T> funnel) {
        hashStream128.putNullable(obj, funnel);
        hasData = true;
        return this;
    }

    @Override
    public <T> Hasher putOrderedIterable(Iterable<T> data, HashFunnel<? super T> funnel) {
        hashStream128.putOrderedIterable(data, funnel);
        hasData = true;
        return this;
    }

    @Override
    public <T> Hasher putUnorderedIterable(Iterable<T> data, ToLongFunction<? super T> elementHashFunction) {
        hashStream128.putUnorderedIterable(data, elementHashFunction);
        hasData = true;
        return this;
    }

    @Override
    public <T> Hasher putUnorderedIterable(Iterable<T> data, HashFunnel<? super T> funnel, HashStream64 hashStream) {
        hashStream128.putUnorderedIterable(data, funnel, hashStream);
        hasData = true;
        return this;
    }

    @Override
    public <T> Hasher putUnorderedIterable(Iterable<T> data, HashFunnel<? super T> funnel, Hasher64 hasher) {
        hashStream128.putUnorderedIterable(data, funnel, hasher);
        hasData = true;
        return this;
    }

    @Override
    public <T> Hasher putOptional(Optional<T> obj, HashFunnel<? super T> funnel) {
        hashStream128.putOptional(obj, funnel);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putOptionalInt(OptionalInt v) {
        hashStream128.putOptionalInt(v);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putOptionalLong(OptionalLong v) {
        hashStream128.putOptionalLong(v);
        hasData = true;
        return this;
    }

    @Override
    public Hasher putOptionalDouble(OptionalDouble v) {
        hashStream128.putOptionalDouble(v);
        hasData = true;
        return this;
    }

    /**
     * Puts a key of a built-in type into the hasher, as the cache hashes such a key: a {@code String}
     * ({@link #putString(String)}), a {@code Long} ({@link #putLong(long)}), an {@code Integer}
     * ({@link #putInt(int)}), a {@code UUID} ({@link #putUUID(UUID)}) or an enum constant (the name of its enum
     * class, then its own name, each by {@link #putString(String)}). A hash provider can use it for the keys, or the
     * parts of keys, of these types: {@code Hasher.putBuiltIn(part, hasher)}.
     * <p>
     * An enum constant's hash changes when its enum class or the constant is renamed or moved, as does the hash of a
     * renamed key class's fields; reordering the constants changes nothing.
     *
     * @param key    the key
     * @param hasher the hasher to put it into
     * @return whether the key is of a built-in type, and so was put
     */
    public static boolean putBuiltIn(Object key, Hasher hasher) {
        return putBuiltInTo(key, requireNonNull(hasher, "hasher cannot be null"));
    }

    /**
     * Returns the hash of a key of a built-in type (see {@link #putBuiltIn(Object, Hasher)}) as the cache computes
     * it: the 128 bits that {@link #getHash()} formats, without creating a string. Mainly what the cache itself uses.
     *
     * @param key the key
     * @return the hash, or {@code null} if the key is of no built-in type
     */
    public static @Nullable HashValue128 hashBuiltIn(Object key) {
        HashStream128 stream = isVirtualThread() ? hasher128.hashStream() : STREAM.get().reset();
        return putBuiltInTo(key, stream) ? stream.get() : null;
    }

    // the one list of built-in types
    private static boolean putBuiltInTo(Object key, HashSink sink) {
        if (key instanceof String string) {
            sink.putString(string);
        } else if (key instanceof Long longValue) {
            sink.putLong(longValue);
        } else if (key instanceof Integer integerValue) {
            sink.putInt(integerValue);
        } else if (key instanceof UUID uuid) {
            sink.putUUID(uuid);
        } else if (key instanceof Enum<?> constant) {
            // the declaring class: a constant with a body of its own is an anonymous subclass, named by its position
            sink.putString(constant.getDeclaringClass().getName()).putString(constant.name());
        } else {
            return false;
        }
        return true;
    }

    private static @Nullable MethodHandle isVirtual() {
        try {
            return MethodHandles.publicLookup()
                    .findVirtual(Thread.class, "isVirtual", MethodType.methodType(boolean.class));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            return null;
        }
    }

    // the handle is a constant, so the JIT reduces the check to the thread's flag
    @SuppressWarnings("java:S1181") // invokeExact declares Throwable; isVirtual throws nothing
    private static boolean isVirtualThread() {
        if (IS_VIRTUAL == null) {
            return false;
        }
        try {
            return (boolean) IS_VIRTUAL.invokeExact(Thread.currentThread());
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns the hash of the values put so far.
     *
     * @return the hash
     * @throws IllegalStateException if no value was put
     */
    public String getHash() {
        if (!hasData) {
            throw new IllegalStateException("Nothing to hash");
        }
        return HexFormat.of().formatHex(hashStream128.get().toByteArray());
    }
}
