/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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

import org.bson.types.ObjectId;

import java.security.SecureRandom;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;

class InternalObjectIdGenerator {

    private final ReentrantLock reentrantLock;
    private final byte[] random;
    private final int offset;
    private final String origin;

    private int time;
    private int counter;

    InternalObjectIdGenerator() {
        this.reentrantLock = new ReentrantLock();
        this.random = new byte[5];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(this.random);
        this.offset = secureRandom.nextInt(16 * 1_000_000);
        this.origin = IntStream.range(0, random.length)
                .mapToObj(i -> format("%02x", random[i]))
                .collect(Collectors.joining());
    }

    ObjectId generate() {
        reentrantLock.lock();
        try {
            int currentTime = (int) new Date().getTime() / 1_000;
            if (time != currentTime) {
                time = currentTime;
                counter = offset;
            } else {
                counter++;
            }
            byte[] bytes = new byte[]{
                    b(time, 24), b(time, 16), b(time, 8), b(time, 0),
                    random[0], random[1], random[2], random[3], random[4],
                    b(counter, 16), b(counter, 8), b(counter, 0)
            };
            return new ObjectId(bytes);
        } finally {
            reentrantLock.unlock();
        }
    }

    String getOrigin() {
        return origin;
    }

    private byte b(int i, int b) {
        return (byte) (i >> b);
    }
}
