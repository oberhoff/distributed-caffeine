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

import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * Waiting for a condition in tests.
 */
public final class Awaiting {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final Duration POLL = Duration.ofMillis(5);

    private Awaiting() {
    }

    /**
     * Polls the condition until it holds, failing after 20 seconds.
     *
     * @param condition the condition
     */
    public static void await(BooleanSupplier condition) {
        if (!awaitUpTo(condition, TIMEOUT, POLL)) {
            throw new AssertionError("condition not met within " + TIMEOUT.toSeconds() + " s");
        }
    }

    /**
     * Lets time pass: an age or an expiry to reach, or a quiet period in which nothing may happen, which no condition
     * can be waited for. An interrupt ends the pause early, with the interrupt flag set again.
     *
     * @param millis how long
     */
    @SuppressWarnings("java:S2925") // time passing is the point, there is nothing to poll
    public static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Polls the condition until it holds or the timeout passes.
     *
     * @param condition the condition
     * @param timeout   how long to wait at most
     * @param poll      the pause between two checks
     * @return whether the condition holds
     */
    public static boolean awaitUpTo(BooleanSupplier condition, Duration timeout, Duration poll) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            pause(poll.toMillis());
            if (Thread.currentThread().isInterrupted()) {
                throw new AssertionError("interrupted while waiting");
            }
        }
        return true;
    }
}
