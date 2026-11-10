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
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * A store without a change feed of its own but with a ChangeScan, made into one by the core (VISION.md, "The polling
 * feed"). Every poll interval it reads the changes written since its last poll plus an overlap (a write takes its time
 * before it becomes visible), the control row of invalidateAll among them. A poll failing for longer than the grace
 * period of tombstones minus the overlap may have missed deletes, so delivery is reported lost. A shorter failure is
 * reported as a gap once a poll succeeds again: the next poll reads from the same point, but a store that failed over
 * meanwhile may have lost writes the caches learned of, which no scan of the new primary can report (a key it has no
 * row of at all), so what is held is checked against the store (found with MySQL's asynchronous replication, VISION.md
 * "MariaDB and MySQL"); a store noticing another server answering fails its scan once for that reason. A write of this
 * instance that may have committed too late for every poll (its call slower than the overlap, or its outcome unknown)
 * is touched again: its keys rewritten as they are, conditionally on their version, so that a poll finds them. Every
 * other call passes through.
 *
 * @author Andreas Oberhoff
 */
final class InternalPollingStore implements Store, ChangeFeed {

    private static final System.Logger LOGGER = System.getLogger(DistributedCaffeine.class.getPackageName());

    static final Duration POLL_OVERLAP = Duration.ofSeconds(2);

    // the cache's, for its log messages (Adapter#toString)
    private final String name;
    private final Store store;
    private final ChangeScan scan;
    private final Duration interval;
    private final Duration overlap;
    // how long polls may fail before delivery counts as lost: the grace period of tombstones minus the overlap, after
    // which a delete may have gone unseen
    private final Duration grace;
    private final Duration timeout;
    private final Executor executor;
    private final InternalEnvironment environment;
    // the rewrite of a changed row proposes its version like a cache instance (above the row's, read just before)
    private final InternalVersion clock;

    // environment: the timing of polls and the scheduling of the next one
    // each a setting of its own, all given by the builder: an object would only group them
    @SuppressWarnings("java:S107")
    InternalPollingStore(String name, Store store, ChangeScan scan, Duration interval, Duration overlap,
                         Duration grace, Duration timeout, Executor executor, InternalEnvironment environment) {
        this.name = name;
        this.store = store;
        this.scan = scan;
        this.interval = interval;
        this.overlap = overlap;
        this.grace = grace;
        this.timeout = timeout;
        this.executor = executor;
        this.environment = environment;
        this.clock = new InternalVersion(environment, ignored -> {
        });
    }

    // reads and maintenance without announcements: as they are

    @Override
    public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
        return store.read(hashes, maxAge);
    }

    @Override
    public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        return store.readVersions(hashes, maxAge);
    }

    @Override
    public int pruneToSize(long maxSize, Duration minAge, int limit) {
        return store.pruneToSize(maxSize, minAge, limit);
    }

    @Override
    public int pruneTombstones(Duration retention, int limit) {
        return store.pruneTombstones(retention, limit);
    }

    @Override
    public Duration clockAllowance() {
        return store.clockAllowance();
    }

    @Override
    public void close() {
        store.close();
    }

    // writes: timed, and touched again if a poll may have missed them

    @Override
    public WriteResult write(Collection<Write> writes) {
        Set<String> hashes = new LinkedHashSet<>();
        writes.forEach(write -> hashes.add(write.hash()));
        return touchingIfLate(hashes, () -> store.write(writes));
    }

    // which rows it expired is not returned: if late, the expired rows of the window are touched
    @Override
    public int expire(Duration maxAge, long version, int limit) {
        long start = environment.nanoTime();
        try {
            int expired = store.expire(maxAge, version, limit);
            Duration took = Duration.ofNanos(environment.nanoTime() - start);
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
        return scan.changedWithin(window).stream().filter(change -> change.kind() == Change.Kind.EXPIRED)
                .map(change -> requireNonNull(change.hash())).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private <T> T touchingIfLate(Set<String> hashes, Supplier<T> write) {
        long start = environment.nanoTime();
        try {
            T result = write.get();
            if (environment.nanoTime() - start > overlap.toNanos()) {
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

    // rewrites the keys as they are now, conditionally on their version and at a version above it: a newer write
    // announces itself, a deleted
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
            InternalMaintenance.schedule(environment, () -> execute(touch), delay);
        }
    }

    private void touch(Set<String> hashes) {
        if (hashes.isEmpty()) {
            return;
        }
        List<String> toRead = new ArrayList<>(hashes);
        toRead.add(Store.CONTROL_HASH);
        Map<String, Row> read = store.read(toRead, Duration.ofNanos(Long.MAX_VALUE));
        Row control = read.get(Store.CONTROL_HASH);
        List<Write> writes = new ArrayList<>();
        read.forEach((hash, row) -> {
            // a row the control row hides stays hidden (rewritten, it would come back); the control row itself is
            // rewritten like any row, its change being an invalidateAll
            if (!hash.equals(Store.CONTROL_HASH) && control != null && row.age().compareTo(control.age()) >= 0) {
                return;
            }
            long version = clock.next(row.version());
            byte[] value = row.value();
            writes.add(row.state() == State.VALUE && value != null
                    ? Write.value(hash, row.key(), value, version).ifVersion(row.version())
                    : Write.tombstone(hash, version).ifVersion(row.version()));
        });
        if (!writes.isEmpty()) {
            store.write(writes);
        }
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
        Poller poller = new Poller(listener);
        poller.schedule();
        return poller;
    }

    // one subscription: polls on the store executor, one poll at a time
    final class Poller implements ChangeSubscription {

        private final ChangeListener listener;
        private final AtomicReference<@Nullable ScheduledFuture<?>> task = new AtomicReference<>();
        // guarded by this: when the last successful poll began (the environment's nanoTime), and whether delivery was
        // reported lost (no further polls until resumed)
        @GuardedBy("this")
        private long polled;
        @GuardedBy("this")
        private boolean lost;
        // whether a poll failed since the last successful one (a gap to report): written under this, read without it,
        // as every read of the core asks (isFailing), and must not wait for a poll in progress
        private volatile boolean failed;
        // guarded by this: the changes delivered while a poll can still report them again (within its overlap), by
        // hash: the version and when the poll began. Each change is delivered once: one reported again carries
        // nothing new, and as a message it would drop an operation in flight on its key (which cannot compare
        // versions yet), and the control row's would make the core look at it again
        @GuardedBy("this")
        private final Map<String, Delivered> delivered = new HashMap<>();
        private volatile boolean closed;

        Poller(ChangeListener listener) {
            this.listener = listener;
            this.polled = environment.nanoTime();
        }

        // whether the latest poll failed: changes may be missed until one succeeds (and reports the gap); the core
        // trusts no copy meanwhile and shows it as DEGRADED, also on an instance that neither reads nor writes through
        // the store
        boolean isFailing() {
            return failed;
        }

        void schedule() {
            if (!closed) {
                task.set(InternalMaintenance.schedule(environment, () -> {
                    if (!execute(this::poll)) {
                        closed = true;
                    }
                }, interval));
            }
        }

        private void poll() {
            List<Change> messages = new ArrayList<>();
            Throwable failure = null;
            // the failure that began a run of failed polls, logged once, as the poll succeeding again is
            RuntimeException began = null;
            boolean gap = false;
            synchronized (this) {
                if (closed || lost) {
                    return;
                }
                long start = environment.nanoTime();
                try {
                    // the control row's change too, which the core takes as an invalidateAll
                    List<Change> changes = scan.changedWithin(Duration.ofNanos(start - polled).plus(overlap));
                    // a change written before the last poll's start minus the overlap is outside every later window
                    // (with twice the overlap for the clocks: the window is measured by the store's)
                    long reportable = polled - overlap.toNanos() * 2;
                    delivered.values().removeIf(entry -> entry.polled() - reportable < 0);
                    for (Change change : changes) {
                        Delivered before = delivered.put(change.hash(), new Delivered(change.version(), start));
                        if (before == null || before.version() != change.version()) {
                            messages.add(change);
                        }
                    }
                    polled = start;
                    gap = failed;
                    failed = false;
                } catch (RuntimeException e) {
                    if (!failed) {
                        began = e;
                    }
                    failed = true;
                    if (Duration.ofNanos(start - polled).compareTo(grace) > 0) {
                        lost = true;
                        failure = e;
                    }
                }
            }
            if (began != null) {
                LOGGER.log(System.Logger.Level.WARNING, name + ": Polling the store for changes failed; changes may "
                        + "be missed until a poll succeeds: " + began.getMessage());
            }
            if (failure != null) {
                listener.onLost(failure);
                return;
            }
            if (gap) {
                LOGGER.log(System.Logger.Level.INFO, name + ": Polling the store for changes succeeded again");
                listener.onGap();
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
            probe();
            boolean restart;
            synchronized (this) {
                restart = lost;
                lost = false;
                polled = environment.nanoTime();
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

        // a change delivered: its version, and when the poll that delivered it began (the environment's nanoTime)
        private record Delivered(long version, long polled) {
        }

        // one read of the control row: whether the store answers (it fails otherwise)
        private void probe() {
            store.read(List.of(Store.CONTROL_HASH), Duration.ofNanos(Long.MAX_VALUE));
        }
    }
}
