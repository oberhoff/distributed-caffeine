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

/**
 * The lock between the atomic map operations and this instance's own {@code invalidateAll} (VISION.md, "Target
 * design: {@code invalidateAll} by the store's time, hidden on read"): an operation holds the shared side from its
 * decision to its commit, {@code invalidateAll} the exclusive side while it writes the control row and clears L1, so
 * that an operation is atomic per key against it, as in Caffeine.
 * <p>
 * Built on the monitor rather than on {@code ReentrantReadWriteLock}: Lincheck's model checker reports correct uses of
 * the latter as hung once more locking happens inside it (Caffeine's own lock in {@code clear}), and models monitors
 * correctly (checked 2026-10-28 with a probe). Shared holds are reentrant per thread (a remapping function may run an
 * atomic operation on another key); an exclusive holder waiting blocks new shared holders, but not a thread already
 * holding the shared side, so neither starves nor deadlocks the other.
 */
final class InternalClearLock {

    // the shared holds of the current thread; a thread holding any is let in again at once
    private final ThreadLocal<int[]> holds = ThreadLocal.withInitial(() -> new int[1]);
    @GuardedBy("this")
    private int readers;
    @GuardedBy("this")
    private int writersWaiting;
    @GuardedBy("this")
    private boolean writing;

    void lockShared() {
        int[] held = holds.get();
        if (held[0] == 0) {
            synchronized (this) {
                boolean interrupted = false;
                while (writing || writersWaiting > 0) {
                    interrupted |= await();
                }
                readers++;
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        held[0]++;
    }

    void unlockShared() {
        int[] held = holds.get();
        if (--held[0] == 0) {
            synchronized (this) {
                if (--readers == 0) {
                    notifyAll();
                }
            }
        }
    }

    // refused inside an atomic operation of the same thread, which would wait for itself (Caffeine likewise forbids
    // changing the cache from inside a remapping function)
    void lockExclusive() {
        if (holds.get()[0] > 0) {
            throw new IllegalStateException("invalidateAll() cannot be called from inside an atomic map operation "
                    + "(compute, merge, replace and their kin) of the same cache");
        }
        synchronized (this) {
            boolean interrupted = false;
            writersWaiting++;
            try {
                while (writing || readers > 0) {
                    interrupted |= await();
                }
            } finally {
                writersWaiting--;
            }
            writing = true;
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    synchronized void unlockExclusive() {
        writing = false;
        notifyAll();
    }

    // waits for a change; true if interrupted meanwhile (the wait goes on, the interrupt is restored by the caller)
    @GuardedBy("this")
    @SuppressWarnings("WaitNotInLoop") // every caller loops on its condition
    private boolean await() {
        try {
            wait();
            return false;
        } catch (InterruptedException e) {
            return true;
        }
    }
}
