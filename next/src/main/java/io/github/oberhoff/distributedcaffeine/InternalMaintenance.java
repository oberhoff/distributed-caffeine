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

// One maintenance scheduler per JVM for all caches; it only triggers, the store calls run on the store executor.
final class InternalMaintenance {

    static final Duration TOMBSTONE_RETENTION = Duration.ofMinutes(5);

    private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "distributed-caffeine-maintenance");
        thread.setDaemon(true);
        return thread;
    });

    private InternalMaintenance() {
    }

    // the interval with ±20 % jitter, so that instances do not prune in step; the task's future, as the scheduler
    // returns it
    @SuppressWarnings("java:S1452")
    static ScheduledFuture<?> schedule(Runnable task, Duration interval) {
        long nanos = interval.toNanos();
        long jittered = nanos * 4 / 5 + ThreadLocalRandom.current().nextLong(Math.max(1, nanos * 2 / 5));
        return SCHEDULER.schedule(task, Math.max(1, jittered), TimeUnit.NANOSECONDS);
    }
}
