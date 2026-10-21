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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

// The store guard, as in the current generation (measured and tested): it opens after 3 consecutive failures; while
// open, store calls are refused at once instead of waiting for timeouts; after 1 s the next call goes through as the
// probe; a successful call, or the maintenance worker's success, closes it. Reads are guarded as well as writes.
//
// And the retries (VISION.md, "Write retries"): a store does not retry, it classifies its failures; a call is tried up
// to 3 times with jitter (20 ms base) while the failure is retryable and repeating the call is safe: always for reads
// and every write but the unconditional put, for which an unknown outcome is final (a repetition could overwrite a
// newer write of another instance). A write whose outcome was unknown in any attempt stays unknown. Anything else a
// store throws is a failure that is not retryable, a write's outcome unknown. One logical call counts once, for the
// guard and in the stats.
final class InternalStoreGuard {

    static final int THRESHOLD = 3;
    static final long SUSPENSION_NANOS = TimeUnit.SECONDS.toNanos(1);
    static final int ATTEMPTS = 3;
    private static final long BACKOFF_MILLIS = 20;

    // what a call does, which decides whether repeating it is safe
    enum Operation {
        READ, WRITE, PUT
    }

    private final InternalStatsCounter stats;
    @GuardedBy("this")
    private int failures;
    @GuardedBy("this")
    private boolean open;
    @GuardedBy("this")
    private boolean probing;
    @GuardedBy("this")
    private long openedAt;

    InternalStoreGuard(InternalStatsCounter stats) {
        this.stats = stats;
    }

    // a read call (values or versions), timed and counted
    <T> T read(Supplier<T> call) {
        return call(call, Operation.READ);
    }

    // a write call that may be repeated (conditional writes, invalidations), timed and counted
    <T> T write(Supplier<T> call) {
        return call(call, Operation.WRITE);
    }

    // the unconditional put, timed and counted: not repeated once its outcome is unknown
    <T> T put(Supplier<T> call) {
        return call(call, Operation.PUT);
    }

    private <T> T call(Supplier<T> call, Operation operation) {
        admit();
        boolean write = operation != Operation.READ;
        long start = System.nanoTime();
        T result;
        try {
            result = retrying(call, operation);
        } catch (StoreException e) {
            failure();
            stats.increment(InternalStatsCounter.Counter.STORE_FAILURE);
            if (write && e.isOutcomeUnknown()) {
                stats.increment(InternalStatsCounter.Counter.UNKNOWN_OUTCOME);
            }
            throw e;
        } finally {
            stats.increment(write ? InternalStatsCounter.Counter.STORE_WRITE : InternalStatsCounter.Counter.STORE_READ);
            stats.add(write ? InternalStatsCounter.Counter.STORE_WRITE_TIME : InternalStatsCounter.Counter.STORE_READ_TIME,
                    System.nanoTime() - start);
        }
        success();
        return result;
    }

    // the call with the retries, unguarded (maintenance calls the store this way: it is how the guard learns that the
    // store answers again)
    static <T> T retrying(Supplier<T> call, Operation operation) {
        boolean unknown = false;
        for (int attempt = 1; ; attempt++) {
            StoreException failure;
            try {
                return call.get();
            } catch (StoreException e) {
                failure = e;
            } catch (RuntimeException e) {
                failure = new StoreException("The store failed unexpectedly: " + e, e,
                        operation != Operation.READ, false);
            }
            unknown |= operation != Operation.READ && failure.isOutcomeUnknown();
            boolean again = failure.isRetryable() && attempt < ATTEMPTS
                    && !(operation == Operation.PUT && failure.isOutcomeUnknown());
            if (!again) {
                if (attempt == 1 && unknown == failure.isOutcomeUnknown()) {
                    throw failure;
                }
                throw new StoreException(failure.getMessage() + " (after " + attempt + " attempts)", failure, unknown,
                        failure.isRetryable());
            }
            backoff(attempt, unknown);
        }
    }

    private static void backoff(int attempt, boolean unknown) {
        try {
            Thread.sleep(BACKOFF_MILLIS * attempt + ThreadLocalRandom.current().nextLong(BACKOFF_MILLIS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StoreException("Interrupted while waiting to retry", e, unknown, false);
        }
    }

    synchronized boolean isOpen() {
        return open;
    }

    synchronized void success() {
        failures = 0;
        open = false;
        probing = false;
    }

    synchronized void failure() {
        failures++;
        probing = false;
        if (open) {
            openedAt = System.nanoTime();
        } else if (failures >= THRESHOLD) {
            open = true;
            openedAt = System.nanoTime();
            stats.increment(InternalStatsCounter.Counter.GUARD_OPEN);
        }
    }

    private void admit() {
        synchronized (this) {
            if (!open) {
                return;
            }
            if (!probing && System.nanoTime() - openedAt >= SUSPENSION_NANOS) {
                probing = true;
                return;
            }
        }
        stats.increment(InternalStatsCounter.Counter.REFUSED);
        throw new StoreException("The store is not answering; calls are refused until it answers again", false);
    }
}
