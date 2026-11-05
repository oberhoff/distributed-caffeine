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
import java.util.concurrent.ScheduledFuture;

/**
 * One maintenance scheduler per JVM for all caches; it only triggers, the store calls run on the store executor.
 *
 * @author Andreas Oberhoff
 */
final class InternalMaintenance {

    static final Duration TOMBSTONE_RETENTION = Duration.ofMinutes(5);
    // the longest heartbeat interval with the maximum age for the store only (DistributedCaffeine#validate): three
    // intervals within the retention of tombstones
    static final Duration STORE_ONLY_HEARTBEAT_LIMIT = Duration.ofMinutes(1);
    // the least time size pruning spares a row (DistributedCaffeine#pruneGrace): a load slower than this that met
    // the cache turning over its whole maximum size meanwhile may leave an older value, up to the maximum age
    static final Duration PRUNE_GRACE = Duration.ofSeconds(4);
    // the rows one maintenance call changes at most (Store#expire): a call over a million rows took 7 to 41 s, and
    // one statement over a few million would outlast the store's default timeout of 30 s, every run again (VISION.md,
    // "Maintenance in paced batches"). Expiry announces every row, and an instance falling more than 100,000 messages
    // behind drops them and revalidates (InternalDelivery): 10,000 per batch, tens to hundreds of milliseconds each.
    // Pruning is silent: 100,000, which brought a million rows close to one call's total (PostgreSQL 2.7 against
    // 1.7 s, MongoDB 43 against 38 s; with 10,000 up to twice that), the longest batch below 5 s
    static final int EXPIRY_BATCH = 10_000;
    static final int PRUNE_BATCH = 100_000;

    private InternalMaintenance() {
    }

    // once, after the delay and never before it (no jitter), never cancelled
    @SuppressWarnings("FutureReturnValueIgnored") // nothing to cancel or wait for
    static void scheduleOnce(InternalEnvironment environment, Runnable task, Duration delay) {
        environment.schedule(task, delay);
    }

    // once, after the delay (no jitter); the task's future, as the scheduler returns it
    @SuppressWarnings("java:S1452")
    static ScheduledFuture<?> scheduleAfter(InternalEnvironment environment, Runnable task, Duration delay) {
        return environment.schedule(task, delay);
    }

    // the interval with ±20 % jitter, so that instances do not prune in step; the task's future, as the scheduler
    // returns it
    @SuppressWarnings("java:S1452")
    static ScheduledFuture<?> schedule(InternalEnvironment environment, Runnable task, Duration interval) {
        long nanos = interval.toNanos();
        long jittered = nanos * 4 / 5 + environment.random(Math.max(1, nanos * 2 / 5));
        return environment.schedule(task, Duration.ofNanos(jittered));
    }
}
