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

import java.util.HexFormat;

/**
 * A 128-bit hash, as stored: a built-in value's ({@link Hasher#builtInHash}) or a custom one
 * ({@link Hasher#getHash()}).
 *
 * @param mostSignificantBits  the most significant 64 bits
 * @param leastSignificantBits the least significant 64 bits
 * @author Andreas Oberhoff
 */
public record Hash(long mostSignificantBits, long leastSignificantBits) {

    /**
     * Returns the hash's 16 bytes: its least significant 8 bytes, then its most significant, each little-endian.
     *
     * @return the bytes
     */
    public byte[] toBytes() {
        byte[] bytes = new byte[16];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (leastSignificantBits >>> (8 * i));
            bytes[8 + i] = (byte) (mostSignificantBits >>> (8 * i));
        }
        return bytes;
    }

    /**
     * Returns the stored form of the hash: the 32 lowercase hex characters of its 16 bytes ({@link #toBytes()}).
     *
     * @return the stored hash
     */
    public String toHex() {
        // each half's bytes little-endian are its reversed bytes written big-endian, as toHexDigits writes them
        HexFormat hex = HexFormat.of();
        return hex.toHexDigits(Long.reverseBytes(leastSignificantBits))
                + hex.toHexDigits(Long.reverseBytes(mostSignificantBits));
    }

    /**
     * Returns the stored form of the hash ({@link #toHex()}).
     *
     * @return the stored hash
     */
    @Override
    public String toString() {
        return toHex();
    }
}
