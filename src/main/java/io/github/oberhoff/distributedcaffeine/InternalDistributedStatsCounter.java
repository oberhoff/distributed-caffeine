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

import java.util.concurrent.atomic.LongAdder;

/**
 * Records what {@link DistributedStats} reports, for one cache instance.
 * <p>
 * Created disabled with the registry - the store guard needs it from the start - and enabled while building where
 * Caffeine records statistics, so that recording follows the one switch a user already knows. Enabled before the
 * cache instance is handed out, which is what makes a plain field enough to see it.
 * <p>
 * Adders rather than atomics, because most of what is counted here happens under contention by design: every
 * writer of a cache instance publishes, and every change of another cache instance arrives on a thread of its own.
 */
class InternalDistributedStatsCounter {

    private boolean enabled;

    private final LongAdder storeHitCount = new LongAdder();
    private final LongAdder storeMissCount = new LongAdder();
    private final LongAdder storeFailureCount = new LongAdder();
    private final LongAdder storeWriteSuccessCount = new LongAdder();
    private final LongAdder storeWriteFailureCount = new LongAdder();
    private final LongAdder asynchronousStoreWriteFailureCount = new LongAdder();
    private final LongAdder synchronousStoreWriteCount = new LongAdder();
    private final LongAdder totalSynchronousStoreWriteTime = new LongAdder();
    private final LongAdder storeRejectionCount = new LongAdder();
    private final LongAdder remoteUpdateCount = new LongAdder();
    private final LongAdder remoteRemovalCount = new LongAdder();
    private final LongAdder discardedLoadCount = new LongAdder();
    private final LongAdder reconciliationCount = new LongAdder();

    void enable() {
        enabled = true;
    }

    void recordStoreLookup(int hitCount, int missCount) {
        if (enabled) {
            storeHitCount.add(hitCount);
            storeMissCount.add(missCount);
        }
    }

    void recordStoreLookupFailure(int keyCount) {
        if (enabled) {
            storeFailureCount.add(keyCount);
        }
    }

    void recordStoreWriteSuccess() {
        if (enabled) {
            storeWriteSuccessCount.increment();
        }
    }

    void recordStoreWriteFailure(boolean synchronous) {
        if (enabled) {
            storeWriteFailureCount.increment();
            if (!synchronous) {
                asynchronousStoreWriteFailureCount.increment();
            }
        }
    }

    // count and time together, because the one is what the other is divided by
    void recordSynchronousStoreWrite(long writeTime) {
        if (enabled) {
            synchronousStoreWriteCount.increment();
            totalSynchronousStoreWriteTime.add(writeTime);
        }
    }

    void recordStoreRejection() {
        if (enabled) {
            storeRejectionCount.increment();
        }
    }

    void recordRemoteUpdate() {
        if (enabled) {
            remoteUpdateCount.increment();
        }
    }

    void recordRemoteRemovals(int count) {
        if (enabled) {
            remoteRemovalCount.add(count);
        }
    }

    void recordDiscardedLoad() {
        if (enabled) {
            discardedLoadCount.increment();
        }
    }

    void recordReconciliation() {
        if (enabled) {
            reconciliationCount.increment();
        }
    }

    // Each count is read on its own, so a snapshot taken while counting goes on is not one instant across all of
    // them - which is what Caffeine's own snapshot of its adders amounts to as well
    DistributedStats snapshot() {
        if (!enabled) {
            return DistributedStats.empty();
        }
        return new DistributedStats(
                storeHitCount.sum(),
                storeMissCount.sum(),
                storeFailureCount.sum(),
                storeWriteSuccessCount.sum(),
                storeWriteFailureCount.sum(),
                asynchronousStoreWriteFailureCount.sum(),
                synchronousStoreWriteCount.sum(),
                totalSynchronousStoreWriteTime.sum(),
                storeRejectionCount.sum(),
                remoteUpdateCount.sum(),
                remoteRemovalCount.sum(),
                discardedLoadCount.sum(),
                reconciliationCount.sum());
    }
}
