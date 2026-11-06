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
package io.github.oberhoff.distributedcaffeine.testing;

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Captures what is logged while it is open, for tests of the library's logging: the tests' logging backend
 * (slf4j-simple, behind {@code System.Logger}) writes each message to the {@code System.err} of that moment, which this
 * replaces by a stream writing to the original one and to a buffer. Not for tests running concurrently in one JVM.
 *
 * @author Andreas Oberhoff
 */
public final class LogCapture implements AutoCloseable {

    private final PrintStream original = System.err;
    private final StringBuilder captured = new StringBuilder();

    /**
     * Starts capturing.
     */
    public LogCapture() {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {
                original.write(b);
                synchronized (captured) {
                    captured.append((char) b);
                }
            }

            @Override
            public void write(byte[] bytes, int offset, int length) {
                original.write(bytes, offset, length);
                synchronized (captured) {
                    captured.append(new String(bytes, offset, length, StandardCharsets.UTF_8));
                }
            }
        }, true, StandardCharsets.UTF_8));
    }

    /**
     * Returns how often the fragment was logged so far.
     *
     * @param fragment a part of a message
     * @return the number of occurrences
     */
    public int count(String fragment) {
        String text = text();
        int count = 0;
        for (int at = text.indexOf(fragment); at >= 0; at = text.indexOf(fragment, at + fragment.length())) {
            count++;
        }
        return count;
    }

    /**
     * Returns everything captured so far.
     *
     * @return the captured text
     */
    public String text() {
        synchronized (captured) {
            return captured.toString();
        }
    }

    /**
     * Stops capturing: {@code System.err} is the original one again.
     */
    @Override
    public void close() {
        System.setErr(original);
    }
}
