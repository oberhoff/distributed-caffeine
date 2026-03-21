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
import com.dynatrace.hash4j.hashing.Hasher128;
import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import org.jspecify.annotations.NullMarked;

import java.util.HexFormat;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.ToLongFunction;

import static java.util.Objects.isNull;

/**
 * Hasher for computing a hash for an object using appropriate values.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
public final class Hasher implements HashSink {

    private static final Hasher128 hasher128 = Hashing.xxh3_128();

    private final HashStream128 hashStream128;

    /**
     * Constructs a hasher.
     */
    public Hasher() {
        this.hashStream128 = hasher128.hashStream().reset();
    }

    @Override
    public Hasher putByte(byte v) {
        hashStream128.putByte(v);
        return this;
    }

    @Override
    public Hasher putBytes(byte[] x) {
        hashStream128.putBytes(x);
        return this;
    }

    @Override
    public Hasher putBytes(byte[] x, int off, int len) {
        hashStream128.putBytes(x, off, len);
        return this;
    }

    @Override
    public <T> Hasher putBytes(T input, long off, long len, ByteAccess<T> access) {
        hashStream128.putBytes(input, off, len, access);
        return this;
    }

    @Override
    public Hasher putByteArray(byte[] x) {
        hashStream128.putByteArray(x);
        return this;
    }

    @Override
    public Hasher putBoolean(boolean v) {
        hashStream128.putBoolean(v);
        return this;
    }

    @Override
    public Hasher putBooleans(boolean[] x) {
        hashStream128.putBooleans(x);
        return this;
    }

    @Override
    public Hasher putBooleans(boolean[] x, int off, int len) {
        hashStream128.putBooleans(x, off, len);
        return this;
    }

    @Override
    public Hasher putBooleanArray(boolean[] x) {
        hashStream128.putBooleanArray(x);
        return this;
    }

    @Override
    public Hasher putShort(short v) {
        hashStream128.putShort(v);
        return this;
    }

    @Override
    public Hasher putShorts(short[] x) {
        hashStream128.putShorts(x);
        return this;
    }

    @Override
    public Hasher putShorts(short[] x, int off, int len) {
        hashStream128.putShorts(x, off, len);
        return this;
    }

    @Override
    public Hasher putShortArray(short[] x) {
        hashStream128.putShortArray(x);
        return this;
    }

    @Override
    public Hasher putChar(char v) {
        hashStream128.putChar(v);
        return this;
    }

    @Override
    public Hasher putChars(char[] x) {
        hashStream128.putChars(x);
        return this;
    }

    @Override
    public Hasher putChars(char[] x, int off, int len) {
        hashStream128.putChars(x, off, len);
        return this;
    }

    @Override
    public Hasher putChars(CharSequence c) {
        hashStream128.putChars(c);
        return this;
    }

    @Override
    public Hasher putCharArray(char[] x) {
        hashStream128.putCharArray(x);
        return this;
    }

    @Override
    public Hasher putString(String s) {
        hashStream128.putString(s);
        return this;
    }

    @Override
    public Hasher putInt(int v) {
        hashStream128.putInt(v);
        return this;
    }

    @Override
    public Hasher putInts(int[] x) {
        hashStream128.putInts(x);
        return this;
    }

    @Override
    public Hasher putInts(int[] x, int off, int len) {
        hashStream128.putInts(x, off, len);
        return this;
    }

    @Override
    public Hasher putIntArray(int[] x) {
        hashStream128.putIntArray(x);
        return this;
    }

    @Override
    public Hasher putLong(long v) {
        hashStream128.putLong(v);
        return this;
    }

    @Override
    public Hasher putLongs(long[] x) {
        hashStream128.putLongs(x);
        return this;
    }

    @Override
    public Hasher putLongs(long[] x, int off, int len) {
        hashStream128.putLongs(x, off, len);
        return this;
    }

    @Override
    public Hasher putLongArray(long[] x) {
        hashStream128.putLongArray(x);
        return this;
    }

    @Override
    public Hasher putFloat(float v) {
        hashStream128.putFloat(v);
        return this;
    }

    @Override
    public Hasher putFloats(float[] x) {
        hashStream128.putFloats(x);
        return this;
    }

    @Override
    public Hasher putFloats(float[] x, int off, int len) {
        hashStream128.putFloats(x, off, len);
        return this;
    }

    @Override
    public Hasher putFloatArray(float[] x) {
        hashStream128.putFloatArray(x);
        return this;
    }

    @Override
    public Hasher putDouble(double v) {
        hashStream128.putDouble(v);
        return this;
    }

    @Override
    public Hasher putDoubles(double[] x) {
        hashStream128.putDoubles(x);
        return this;
    }

    @Override
    public Hasher putDoubles(double[] x, int off, int len) {
        hashStream128.putDoubles(x, off, len);
        return this;
    }

    @Override
    public Hasher putDoubleArray(double[] x) {
        hashStream128.putDoubleArray(x);
        return this;
    }

    @Override
    public Hasher putUUID(UUID uuid) {
        hashStream128.putUUID(uuid);
        return this;
    }

    @Override
    public <T> Hasher put(T obj, HashFunnel<T> funnel) {
        hashStream128.put(obj, funnel);
        return this;
    }

    @Override
    public <T> Hasher putNullable(T obj, HashFunnel<T> funnel) {
        hashStream128.putNullable(obj, funnel);
        return this;
    }

    @Override
    public <T> Hasher putOrderedIterable(Iterable<T> data, HashFunnel<? super T> funnel) {
        hashStream128.putOrderedIterable(data, funnel);
        return this;
    }

    @Override
    public <T> Hasher putUnorderedIterable(Iterable<T> data, ToLongFunction<? super T> elementHashFunction) {
        hashStream128.putUnorderedIterable(data, elementHashFunction);
        return this;
    }

    @Override
    public <T> Hasher putUnorderedIterable(Iterable<T> data, HashFunnel<? super T> funnel, HashStream64 hashStream) {
        hashStream128.putUnorderedIterable(data, funnel, hashStream);
        return this;
    }

    @Override
    public <T> Hasher putUnorderedIterable(Iterable<T> data, HashFunnel<? super T> funnel, Hasher64 hasher) {
        hashStream128.putUnorderedIterable(data, funnel, hasher);
        return this;
    }

    @Override
    public <T> Hasher putOptional(Optional<T> obj, HashFunnel<? super T> funnel) {
        hashStream128.putOptional(obj, funnel);
        return this;
    }

    @Override
    public Hasher putOptionalInt(OptionalInt v) {
        hashStream128.putOptionalInt(v);
        return this;
    }

    @Override
    public Hasher putOptionalLong(OptionalLong v) {
        hashStream128.putOptionalLong(v);
        return this;
    }

    @Override
    public Hasher putOptionalDouble(OptionalDouble v) {
        hashStream128.putOptionalDouble(v);
        return this;
    }

    /**
     * Returns the computed hash.
     *
     * @return the hash
     */
    public String getHash() {
        String hash = HexFormat.of().formatHex(hashStream128.get().toByteArray());
        if (isNull(hash) || hash.isBlank() || hash.equalsIgnoreCase("7f498d4624c30160d8984701d306aa99")) {
            throw new IllegalStateException("Hasher is not being used correctly");
        }
        return hash;
    }
}
