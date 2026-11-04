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
package io.github.oberhoff.distributedcaffeine;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * What the core takes from its surroundings and a test may want to control (VISION.md, "Deterministic simulation", A):
 * the time, sleeping, scheduling and randomness. REAL in use; a test of this package injects a simulated one
 * (DistributedCaffeine#withEnvironment), whose time moves only when the test moves it, so that the paths waiting for
 * time (the polls' grace, the heartbeat, the store guard's suspension) run in milliseconds and a failure replays from
 * its seed
 *
 * @author Andreas Oberhoff
 */
interface InternalEnvironment {

    InternalEnvironment REAL = new Real();

    // a monotonic time, as System.nanoTime
    long nanoTime();

    // the wall clock, as System.currentTimeMillis
    long currentTimeMillis();

    void sleep(Duration duration) throws InterruptedException;

    // runs the task once after the delay (at least a nanosecond); the task's future, to cancel it
    @SuppressWarnings("java:S1452") // what a ScheduledExecutorService returns: a future of no particular result
    ScheduledFuture<?> schedule(Runnable task, Duration delay);

    // a random number from 0 (inclusive) to the bound (exclusive, positive)
    long random(long bound);

    // the JVM's own: one daemon thread schedules for every cache, each task handed on to a cache's executor
    @SuppressWarnings("java:S9398") // the one real implementation, reached only through REAL
    final class Real implements InternalEnvironment {

        private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread thread = new Thread(runnable, "distributed-caffeine-maintenance");
                    thread.setDaemon(true);
                    return thread;
                });

        private Real() {
        }

        @Override
        public long nanoTime() {
            return System.nanoTime();
        }

        @Override
        public long currentTimeMillis() {
            return System.currentTimeMillis();
        }

        @Override
        public void sleep(Duration duration) throws InterruptedException {
            TimeUnit.NANOSECONDS.sleep(duration.toNanos());
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable task, Duration delay) {
            return SCHEDULER.schedule(task, Math.max(1, delay.toNanos()), TimeUnit.NANOSECONDS);
        }

        @Override
        public long random(long bound) {
            return ThreadLocalRandom.current().nextLong(bound);
        }
    }
}
