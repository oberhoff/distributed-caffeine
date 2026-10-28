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
import java.util.function.LongPredicate;

// The heartbeat of the change feed (VISION.md, "Heartbeat"): a feed can stop delivering without any error (a silent
// feed), and the cache would believe it is in step while it misses every change. Each instance invalidates a reserved
// key every interval, at versions of an incarnation of its own (each beat the next), and expects the announcement of
// its beat before its next one. Per key the feed delivers in commit order, so its own beat announced at this version
// or a later one proves that it arrived; other instances' beats, of other incarnations, prove nothing about its own. A
// beat missed makes the feed silent: what the cache holds is no longer trusted. The next announcement of the reserved
// key, anyone's, ends the silence with a gap, since what was announced meanwhile may be lost.
final class InternalHeartbeat {

    // the hash of the reserved key; the built-in hashes are hex, and a custom hash equal to it is refused
    static final String HASH = "#heartbeat";

    private final LongPredicate beat;
    private final Runnable silenced;
    private final Runnable resumed;
    // the highest of its own beats announced, 0 if none was
    private final AtomicLong received = new AtomicLong();
    private final AtomicBoolean silent = new AtomicBoolean();
    // the version of the last beat sent, 0 if none was (the store did not answer)
    private volatile long sent;
    // the version of the last beat attempted, whose successor the next one gets; written by the ticking thread only
    private volatile long last;

    // beat: invalidates the reserved key at the given version, false if the store cannot be asked now; silenced and
    // resumed: run once when a beat was missed, and once when the feed delivers again
    InternalHeartbeat(LongPredicate beat, Runnable silenced, Runnable resumed) {
        this.beat = beat;
        this.silenced = silenced;
        this.resumed = resumed;
    }

    // one interval passed: checks the last beat, sends the next. Not run concurrently with itself
    void tick() {
        long beaten = sent;
        if (beaten != 0 && !InternalVersion.atLeast(received.get(), beaten) && silent.compareAndSet(false, true)) {
            silenced.run();
        }
        long version = last == 0 ? InternalVersion.fresh() : InternalVersion.next(last);
        last = version;
        boolean sentNow;
        try {
            sentNow = beat.test(version);
        } catch (RuntimeException e) {
            sentNow = false; // a store that does not answer is the store guard's business, not the feed's
        }
        sent = sentNow ? version : 0;
    }

    // the reserved key was announced, by this instance or another
    void received(long version) {
        if (InternalVersion.incarnation(version) == InternalVersion.incarnation(last)) {
            received.accumulateAndGet(version, (seen, now) -> InternalVersion.atLeast(seen, now) ? seen : now);
        }
        if (silent.compareAndSet(true, false)) {
            resumed.run();
        }
    }

    boolean isSilent() {
        return silent.get();
    }
}
