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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

// The heartbeat of the change feed (VISION.md, "Heartbeat"): a feed can stop delivering without any error (a silent
// feed), and the cache would believe it is in step while it misses every change. Each instance invalidates a reserved
// key every interval and expects the announcement before its next beat. Per key the feed delivers in commit order, so
// the reserved key announced at this version or a later one (another instance's beat) proves that its own beat
// arrived. A beat missed makes the feed silent: what the cache holds is no longer trusted. The next announcement of
// the reserved key ends the silence with a gap, since what was announced meanwhile may be lost.
final class InternalHeartbeat {

    // the hash of the reserved key; the built-in hashes are hex, and a custom hash equal to it is refused
    static final String HASH = "#heartbeat";

    private final LongSupplier beat;
    private final Runnable silenced;
    private final Runnable resumed;
    private final AtomicLong received = new AtomicLong();
    private final AtomicBoolean silent = new AtomicBoolean();
    // the version of the last beat sent, 0 if none was (the store did not answer)
    private volatile long sent;

    // beat: invalidates the reserved key and returns the version, or 0 if the store cannot be asked now; silenced and
    // resumed: run once when a beat was missed, and once when the feed delivers again
    InternalHeartbeat(LongSupplier beat, Runnable silenced, Runnable resumed) {
        this.beat = beat;
        this.silenced = silenced;
        this.resumed = resumed;
    }

    // one interval passed: checks the last beat, sends the next. Not run concurrently with itself
    void tick() {
        if (sent > received.get() && silent.compareAndSet(false, true)) {
            silenced.run();
        }
        long version;
        try {
            version = beat.getAsLong();
        } catch (RuntimeException e) {
            version = 0; // a store that does not answer is the store guard's business, not the feed's
        }
        sent = version;
    }

    // the reserved key was announced
    void received(long version) {
        received.accumulateAndGet(version, Math::max);
        if (silent.compareAndSet(true, false)) {
            resumed.run();
        }
    }

    boolean isSilent() {
        return silent.get();
    }
}
