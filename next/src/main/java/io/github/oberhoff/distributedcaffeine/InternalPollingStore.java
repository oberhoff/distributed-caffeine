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
import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeScan;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.Expected;
import io.github.oberhoff.distributedcaffeine.spi.ExpectedEntry;
import io.github.oberhoff.distributedcaffeine.spi.ReadResult;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

// A store without a change feed of its own but with a ChangeScan, made into one by the core (VISION.md, "The polling
// feed"). Every poll interval it reads the changes written since its last poll plus an overlap (a write takes its time
// before it becomes visible), and the cut-off (a change of it is INVALIDATED_ALL). A poll failing for longer than the
// grace period of tombstones minus the overlap may have missed deletes, so delivery is reported lost; shorter failures
// lose nothing, the next poll reads from the same point. A write of this instance that may have committed too late
// for every poll (its call slower than the overlap, or its outcome unknown) is touched again: its keys rewritten as
// they are, conditionally on their version, so that a poll finds them. Every other call passes through.
final class InternalPollingStore implements Store, ChangeFeed {

    static final Duration OVERLAP = Duration.ofSeconds(2);

    private final Store store;
    private final ChangeScan scan;
    private final Duration interval;
    private final Duration overlap;
    // how long polls may fail before delivery counts as lost: the grace period of tombstones minus the overlap, after
    // which a delete may have gone unseen
    private final Duration grace;
    private final Duration timeout;
    private final Executor executor;

    InternalPollingStore(Store store, ChangeScan scan, Duration interval, Duration overlap, Duration grace,
                         Duration timeout, Executor executor) {
        this.store = store;
        this.scan = scan;
        this.interval = interval;
        this.overlap = overlap;
        this.grace = grace;
        this.timeout = timeout;
        this.executor = executor;
    }

    // reads and maintenance without announcements: as they are

    @Override
    public ReadResult read(Collection<String> hashes, Duration maxAge) {
        return store.read(hashes, maxAge);
    }

    @Override
    public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        return store.readVersions(hashes, maxAge);
    }

    @Override
    public int pruneToSize(long maxSize) {
        return store.pruneToSize(maxSize);
    }

    @Override
    public int pruneTombstones(Duration retention) {
        return store.pruneTombstones(retention);
    }

    @Override
    public void close() {
        store.close();
    }

    // writes: timed, and touched again if a poll may have missed them

    @Override
    public Map<String, Long> put(Collection<Entry> entries) {
        return touchingIfLate(hashesOf(entries), () -> store.put(entries));
    }

    @Override
    public boolean checksBasis() {
        return store.checksBasis();
    }

    @Override
    public Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge) {
        Set<String> hashes = new LinkedHashSet<>();
        entries.forEach(expected -> hashes.add(expected.entry().hash()));
        return touchingIfLate(hashes, () -> store.putIfUnchanged(entries, maxAge));
    }

    @Override
    public Map<String, Long> invalidate(Collection<String> hashes) {
        return touchingIfLate(new LinkedHashSet<>(hashes), () -> store.invalidate(hashes));
    }

    @Override
    public Map<String, Long> invalidateIfUnchanged(Collection<Expected> expectations) {
        Map<String, Long> expected = Expected.byHash(expectations);
        return touchingIfLate(new LinkedHashSet<>(expected.keySet()), () -> store.invalidateIfUnchanged(expectations));
    }

    // the cut-off: read by every poll, so a late commit is found whenever it becomes visible
    @Override
    public long invalidateAll() {
        return store.invalidateAll();
    }

    // which rows it expired is not returned: if late, the expired rows of the window are touched
    @Override
    public int expire(Duration maxAge) {
        long start = System.nanoTime();
        try {
            int expired = store.expire(maxAge);
            Duration took = Duration.ofNanos(System.nanoTime() - start);
            if (took.compareTo(overlap) > 0) {
                touchLater(Duration.ZERO, () -> expiredWithin(took.plus(overlap)));
            }
            return expired;
        } catch (StoreException e) {
            if (e.isOutcomeUnknown()) {
                Duration window = timeout.multipliedBy(2).plus(overlap);
                touchLater(timeout.multipliedBy(2), () -> expiredWithin(window));
            }
            throw e;
        }
    }

    private Set<String> expiredWithin(Duration window) {
        Set<String> hashes = new LinkedHashSet<>();
        for (Change change : scan.changedWithin(window)) {
            if (change.kind() == Change.Kind.EXPIRED) {
                hashes.add(requireNonNull(change.hash()));
            }
        }
        return hashes;
    }

    private <T> T touchingIfLate(Set<String> hashes, Supplier<T> write) {
        long start = System.nanoTime();
        try {
            T result = write.get();
            if (System.nanoTime() - start > overlap.toNanos()) {
                touchLater(Duration.ZERO, () -> hashes); // returned: committed, but maybe after a poll's window
            }
            return result;
        } catch (StoreException e) {
            if (e.isOutcomeUnknown()) {
                touchLater(timeout.multipliedBy(2), () -> hashes); // by then it has landed or never will
            }
            throw e;
        }
    }

    // rewrites the keys as they are now, conditionally on their version: a newer write announces itself, a deleted
    // row has nothing to announce, a row hidden by the cut-off stays hidden (rewritten, it would come back); best
    // effort, a failure only leaves what the overlap left
    @SuppressWarnings("FutureReturnValueIgnored") // the delayed touch is scheduled for its effect, never cancelled
    private void touchLater(Duration delay, Supplier<Set<String>> hashes) {
        Runnable touch = () -> {
            try {
                touch(hashes.get());
            } catch (RuntimeException e) {
                // the store does not answer: the late write stays unannounced, as it would without the touch
            }
        };
        if (delay.isZero()) {
            execute(touch);
        } else {
            InternalMaintenance.schedule(() -> execute(touch), delay);
        }
    }

    private void touch(Set<String> hashes) {
        if (hashes.isEmpty()) {
            return;
        }
        ReadResult read = store.read(hashes, Duration.ofNanos(Long.MAX_VALUE));
        List<ExpectedEntry> values = new ArrayList<>();
        List<Expected> tombstones = new ArrayList<>();
        read.rows().forEach((hash, row) -> {
            if (!row.visible(read.cutoff())) {
                return;
            }
            byte[] value = row.value();
            if (row.state() == State.VALUE && value != null) {
                values.add(ExpectedEntry.ifVersion(new Entry(hash, row.key(), value), row.version(), row.basis()));
            } else {
                tombstones.add(Expected.ifVersion(hash, row.version()));
            }
        });
        if (!values.isEmpty()) {
            store.putIfUnchanged(values, Duration.ofNanos(Long.MAX_VALUE));
        }
        if (!tombstones.isEmpty()) {
            store.invalidateIfUnchanged(tombstones);
        }
    }

    private static Set<String> hashesOf(Collection<Entry> entries) {
        Set<String> hashes = new LinkedHashSet<>();
        entries.forEach(entry -> hashes.add(entry.hash()));
        return hashes;
    }

    // a closed executor ends the polling with the cache
    private boolean execute(Runnable task) {
        try {
            executor.execute(task);
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    // the feed

    @Override
    public ChangeSubscription subscribe(ChangeListener listener) {
        Poller poller = new Poller(listener, readCutoff());
        poller.schedule();
        return poller;
    }

    private long readCutoff() {
        return store.read(List.of(), Duration.ofNanos(Long.MAX_VALUE)).cutoff();
    }

    // one subscription: polls on the store executor, one poll at a time
    private final class Poller implements ChangeSubscription {

        private final ChangeListener listener;
        private final AtomicReference<@Nullable ScheduledFuture<?>> task = new AtomicReference<>();
        // guarded by this: when the last successful poll began (System.nanoTime), the cut-off it saw, and whether
        // delivery was reported lost (no further polls until resumed)
        @GuardedBy("this")
        private long polled;
        @GuardedBy("this")
        private long cutoff;
        @GuardedBy("this")
        private boolean lost;
        private volatile boolean closed;

        Poller(ChangeListener listener, long cutoff) {
            this.listener = listener;
            this.polled = System.nanoTime();
            this.cutoff = cutoff;
        }

        void schedule() {
            if (!closed) {
                task.set(InternalMaintenance.schedule(() -> {
                    if (!execute(this::poll)) {
                        closed = true;
                    }
                }, interval));
            }
        }

        private void poll() {
            List<Change> messages = new ArrayList<>();
            Throwable failure = null;
            synchronized (this) {
                if (closed || lost) {
                    return;
                }
                long start = System.nanoTime();
                try {
                    messages.addAll(scan.changedWithin(Duration.ofNanos(start - polled).plus(overlap)));
                    long seen = readCutoff();
                    if (seen != cutoff) {
                        cutoff = seen;
                        messages.add(Change.invalidatedAll(seen));
                    }
                    polled = start;
                } catch (RuntimeException e) {
                    if (Duration.ofNanos(start - polled).compareTo(grace) > 0) {
                        lost = true;
                        failure = e;
                    }
                }
            }
            if (failure != null) {
                listener.onLost(failure);
                return;
            }
            if (!messages.isEmpty()) {
                listener.onChanges(messages);
            }
            schedule();
        }

        // from now on: a resume is a gap anyway; trying once, so that a store still down fails it
        @Override
        public void resume() {
            if (closed) {
                return;
            }
            long seen = readCutoff();
            boolean restart;
            synchronized (this) {
                restart = lost;
                lost = false;
                polled = System.nanoTime();
                cutoff = seen;
            }
            if (restart) {
                schedule();
            }
        }

        @Override
        public void close() {
            closed = true;
            ScheduledFuture<?> scheduled = task.get();
            if (scheduled != null) {
                scheduled.cancel(false);
            }
        }
    }
}
