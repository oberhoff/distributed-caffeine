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

import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hashing;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HexFormat;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * hash4j's XXH3-128, on which every stored hash rests, against the reference implementation's own test vectors: an
 * oracle independent of hash4j (VISION.md, "The stored hash format"). The lengths are every
 * boundary of XXH3's length classes (0, 1–3, 4–8, 9–16, 17–128, 129–240, longer, its stripes and blocks), each with the
 * three seeds of the reference.
 * <p>
 * The vectors are taken from xxHash v0.8.2, {@code tests/sanity_test_vectors.h} ({@code XSUM_XXH128_testdata}), over
 * the input of {@code cli/xsum_sanity_check.c} ({@code XSUM_fillTestBuffer}). xxHash is Copyright (c) 2012-2021 Yann
 * Collet, all rights reserved, under the
 * <a href="https://www.opensource.org/licenses/bsd-license.php">BSD 2-Clause License</a>.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class Xxh3VectorsTest {

    private static final long PRIME32 = 2654435761L;
    private static final long PRIME64 = 0x9E3779B185EBCA8DL;

    // length, seed, then the hash's low and high 64 bits, as XXH128_hash_t gives them
    private static final String VECTORS = """
            0 0000000000000000 6001c324468d497f 99aa06d3014798d8
            0 000000009e3779b1 5444f7869c671ab0 92220ae55e14ab50
            0 9e3779b185ebca8d a986dfc5d7605bfe 00feaa732a3ce25e
            1 0000000000000000 c44bdff4074eecdb a6cd5e9392000f6a
            1 000000009e3779b1 b53d5557e7f76f8d 89b99554ba22467c
            1 9e3779b185ebca8d 032be332dd766ef8 20e49abcc53b3842
            2 0000000000000000 7a9978044cb8a8bb 76750c3c7bf95668
            2 000000009e3779b1 8295910c7638b180 8b75a791ec034873
            2 9e3779b185ebca8d 764b35c90519ad88 7b96e6a600dae67d
            3 0000000000000000 54247382a8d6b94d 20efc49ff02422ea
            3 000000009e3779b1 f173d14dad53a5dc 48f82c2fe0abd468
            3 9e3779b185ebca8d 634b8990b4976373 1c7ecf6a308cf00e
            4 0000000000000000 2e7d8d6876a39fe9 970d585ac632bf8e
            4 000000009e3779b1 ef78d5c489cfe10b 7170492a2aa08992
            4 9e3779b185ebca8d bfaf51f1e67e0b0f 3d53e5dfd837d927
            8 0000000000000000 64c69cab4bb21dc5 47a7f080d82bb456
            8 000000009e3779b1 5f462f3de2e8b940 f959013232655ff1
            8 9e3779b185ebca8d 7b29471dc729b5ff f50cec145bcd5c5a
            9 0000000000000000 ed7ccbc501eb7501 564ef6078950d457
            9 000000009e3779b1 07de00b45eee033a 75fb6d1bd353b45c
            9 9e3779b185ebca8d aef5dfc0ac9f9044 6b380b43ffa61042
            16 0000000000000000 562980258a998629 c68c368ecf8a9c05
            16 000000009e3779b1 b07eeeab4c56392b 3767c90d0cdbb93d
            16 9e3779b185ebca8d 0346d13a7a5498c7 6ffcb80cd33085c8
            17 0000000000000000 abbc12d11973d7db 955fa78643ed3669
            17 000000009e3779b1 3cc9ff6cae79accb 99e7c628e75d6431
            17 9e3779b185ebca8d 980a14119985a7df d77681219e464828
            128 0000000000000000 ebb15e34a7fb5ab1 39992220e045260a
            128 000000009e3779b1 1453819941d93c1d 98801187df8d614d
            128 9e3779b185ebca8d 8394f5c51f1d8246 a0f7ccb68ee02add
            129 0000000000000000 86c9e3bc8f0a3b5c 03815fc91f1b30b6
            129 000000009e3779b1 b37b716f66b40f02 b7f7349a47b39e56
            129 9e3779b185ebca8d d4aae26fcec7dc03 ad559266067c0bf3
            240 0000000000000000 5c9aae94c8ebe5a0 aa4202daa2769dc8
            240 000000009e3779b1 ca19087f1d335dae da888104beae5ae0
            240 9e3779b185ebca8d 604e98db085c1864 29d2133d6ea58c5b
            241 0000000000000000 c5a639ecd2030e5e 99a80ecf0ecfc647
            241 000000009e3779b1 5927e3637bac8149 4bf2229c3a8fc3c3
            241 9e3779b185ebca8d dda9b0a161d4829a ec64afae6a137582
            255 0000000000000000 e98f979f4ed8a197 961375c87e09efbc
            255 000000009e3779b1 437ea109cb7ce24d ee657e12607adffe
            255 9e3779b185ebca8d 2aca7901d9538c75 e72ec0137d62df44
            256 0000000000000000 55de574ad89d0ac5 8b1c66091423d288
            256 000000009e3779b1 443d04d43f60c57f d540cc8620d8dd65
            256 9e3779b185ebca8d 4d30234b7a3aa61c aaa57235b92d5e7c
            257 0000000000000000 b17fd5a8ae75bb0b f15fee7f9f457599
            257 000000009e3779b1 02f16a1476c65d95 52c36ca232fc662b
            257 9e3779b185ebca8d 802a6fbf3cacd97c 15c1f9c667c815ba
            1024 0000000000000000 dd85c9b5c1109c5c 0d30d24071c64c57
            1024 000000009e3779b1 b8b95c07cd4a75fa 885b0b4debe3d2ff
            1024 9e3779b185ebca8d ef368a8a2ebabaef 17600efe2b493a18
            1025 0000000000000000 d870c0fa13211c6a fd3ee4fe7f2954c6
            1025 000000009e3779b1 2f15255340ae4f6c 3364fad6f5ff1741
            1025 9e3779b185ebca8d 96792bcf9af88519 2c383949f57bf7e1
            2048 0000000000000000 dd59e2c3a5f038e0 f736557fd47073a5
            2048 000000009e3779b1 230d43f30206260b 7fb03f7e7186c3ea
            2048 9e3779b185ebca8d 66f81670669ababc 23cc3a2e75ebaaea
            2049 0000000000000000 d3afa4329779b921 4cd2bd192f2d70bd
            2049 000000009e3779b1 60e0f49946d79daf 782bc6262b86bfe8
            2049 9e3779b185ebca8d e48083836cd58024 e4000f7a288a82ce
            4160 0000000000000000 4f323b15321e94e1 67140711c1e3e335
            4160 000000009e3779b1 71c9218c2dc48ff4 82466b99feb516a4
            4160 9e3779b185ebca8d 1bf6f5faf9eecabd ee430626ba3b2f3c
            """;

    // XSUM_fillTestBuffer: the top byte of a 64-bit generator, multiplied by PRIME64 after each byte
    private static byte[] input(int length) {
        byte[] bytes = new byte[length];
        long generator = PRIME32;
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (generator >>> 56);
            generator *= PRIME64;
        }
        return bytes;
    }

    static Stream<Arguments> vectors() {
        return VECTORS.lines().map(String::strip).filter(line -> !line.isEmpty()).map(line -> line.split(" "))
                .map(fields -> Arguments.of(Integer.parseInt(fields[0]), Long.parseUnsignedLong(fields[1], 16),
                        Long.parseUnsignedLong(fields[2], 16), Long.parseUnsignedLong(fields[3], 16)));
    }

    @ParameterizedTest(name = "length {0}, seed {1}")
    @MethodSource("vectors")
    void xxh3_128_gives_the_reference_s_hash(int length, long seed, long low, long high) {
        HashValue128 value = Hashing.xxh3_128(seed).hashBytesTo128Bits(input(length));

        HexFormat hex = HexFormat.of();

        assertThat(hex.toHexDigits(value.getLeastSignificantBits())).isEqualTo(hex.toHexDigits(low));
        assertThat(hex.toHexDigits(value.getMostSignificantBits())).isEqualTo(hex.toHexDigits(high));
    }
}
