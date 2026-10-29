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
package io.github.oberhoff.distributedcaffeine.testing;

import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * The reference store: an in-memory database that fulfils the SPI contract exactly, shared by several
 * {@link InMemoryStore}s the way several instances share one MongoDB or PostgreSQL database. It exists to test the
 * core quickly and deterministically, without Docker, and to inject the faults the real stores produce only by
 * luck: write failures before or after commit, the store being down, gaps, delivery delays, duplicates and
 * reordering across keys.
 * <p>
 * One table per cache (the table name plays the role of table plus discriminator). Writes are serialized per table,
 * messages are queued on commit in commit order, and every subscription delivers on a thread of its own. How it
 * behaves otherwise (versions, change source, clock, reads, batches) its {@link StoreProfile} says; the events a
 * profile allows (a failover, an eviction, lost messages) are faults triggered here.
 */
public final class InMemoryDatabase {

    private final LongSupplier clockMicros;
    private final Random random;
    private final StoreProfile profile;
    // the store's own sequence (StoreProfile.Versioning.ASSIGNING_OWN), drawn by every write: it runs ahead of every
    // count of a key, so that a new row starts above every version an earlier row of the key had
    private long sequence;
    private final Map<String, InMemoryTable> tables = new ConcurrentHashMap<>();
    final Faults faults = new Faults();
    // the write time a delayed write took before it waited (see setCommitDelay), for the thread running it
    private final ThreadLocal<@Nullable Long> commitTime = new ThreadLocal<>();
    // the last write time handed out by a monotonic clock (StoreProfile.Clock.MONOTONIC): write times increase
    // strictly, also within a microsecond, so that the ages of one read order the writes as they happened
    private long lastWriteTime;
    // how far a wall clock was stepped back (stepClockBack), in microseconds
    private long stepBackMicros;
    // the stores polled by the core (StoreProfile.Changes.SCAN), with the number of scans each had begun at the last
    // commit: idle once each began two more, the second after everything committed before it
    private final List<ScanOnlyStore> scanning = new CopyOnWriteArrayList<>();
    private final Map<ScanOnlyStore, Long> scansAtCommit = new ConcurrentHashMap<>();

    /**
     * Creates a database on the system clock with a random seed, under the profile of the system property
     * ({@link StoreProfile#ofProperty}).
     */
    public InMemoryDatabase() {
        this(InMemoryDatabase::systemMicros, new Random().nextLong());
    }

    /**
     * Creates a database on the system clock with a random seed, under the given profile.
     *
     * @param profile how the store behaves
     */
    public InMemoryDatabase(StoreProfile profile) {
        this(InMemoryDatabase::systemMicros, new Random().nextLong(), profile);
    }

    /**
     * Creates a database on a clock of the test's, under the profile of the system property with a monotonic clock: a
     * test's clock may stand still or count coarsely, where only a store clock that orders every write keeps the
     * test's outcome independent of the profile.
     *
     * @param clockMicros the store's clock, in microseconds since the epoch
     * @param seed        the seed for the random faults (duplicates, reordering, delays), for reproducible runs
     */
    public InMemoryDatabase(LongSupplier clockMicros, long seed) {
        this(clockMicros, seed, StoreProfile.ofProperty().with(StoreProfile.Clock.MONOTONIC, Duration.ZERO));
    }

    /**
     * Creates a database under the given profile.
     *
     * @param clockMicros the store's clock, in microseconds since the epoch
     * @param seed        the seed for the random faults (duplicates, reordering, delays), for reproducible runs
     * @param profile     how the store behaves
     */
    public InMemoryDatabase(LongSupplier clockMicros, long seed, StoreProfile profile) {
        this.clockMicros = requireNonNull(clockMicros);
        this.random = new Random(seed);
        this.profile = requireNonNull(profile);
    }

    /**
     * Returns how the store behaves.
     *
     * @return the profile
     */
    public StoreProfile profile() {
        return profile;
    }

    /**
     * Opens a store of a cache instance on the given table.
     *
     * @param table the table of the cache
     * @return a new store, as one instance would hold it
     */
    public Store openStore(String table) {
        InMemoryStore store = openFeed(table);
        return switch (profile.changes()) {
            case FEED -> store;
            case SCAN -> {
                ScanOnlyStore polled = new ScanOnlyStore(store) {
                    @Override
                    public void close() {
                        scanning.remove(this);
                        scansAtCommit.remove(this);
                        super.close();
                    }
                };
                scanning.add(polled);
                yield polled;
            }
            case NONE -> new StorageOnlyStore(store);
        };
    }

    /**
     * Opens a store of a cache instance on the given table with its feed and its scan, whatever the profile says: for
     * the contract tests of the feed and the scan, and for tests of the feed's faults.
     *
     * @param table the table of the cache
     * @return a new store, as one instance would hold it
     */
    public InMemoryStore openFeed(String table) {
        return new InMemoryStore(tables.computeIfAbsent(table, name -> new InMemoryTable(this)));
    }

    // faults

    /**
     * Makes the next write operations fail. With {@code outcomeUnknown} the write commits (and announces itself)
     * before the failure is reported, as after a timeout once the write was sent; without, nothing commits.
     *
     * @param count          how many write operations fail
     * @param outcomeUnknown whether the failing writes commit
     */
    public void failNextWrites(int count, boolean outcomeUnknown) {
        faults.failNextWrites(count, outcomeUnknown);
    }

    /**
     * Makes the next writes of several rows fail after applying part of them, as an unordered bulk write does
     * (reported with {@link StoreException#afterPartlyApplied()}): the first half of each batch applies and announces
     * itself, the rest does not. Only where the profile writes row by row ({@link StoreProfile.Batch#PER_ROW}); an
     * atomic store fails the batch as a whole, before it commits. A write of one row fails before it commits.
     *
     * @param count how many write operations fail
     */
    public void failNextWritesPartly(int count) {
        faults.failNextWritesPartly(count);
    }

    /**
     * Steps the store's wall clock back, as NTP may: write times and ages follow it, and a later write can be stamped
     * before an earlier one. No effect on a monotonic clock ({@link StoreProfile.Clock#MONOTONIC}).
     *
     * @param amount how far
     */
    public synchronized void stepClockBack(Duration amount) {
        stepBackMicros += amount.toNanos() / 1_000;
    }

    /**
     * Fails over to a replica that lacks the writes of the last window, as a store replicating asynchronously does
     * (MongoDB with {@code w:1}, Redis): every row goes back to its state before the window (to older versions, or
     * missing), the messages not delivered yet are lost, and every subscription is told about a gap. The store's own
     * sequence ({@link StoreProfile.Versioning#ASSIGNING_OWN}) is not set back.
     *
     * @param window how long before now the lost writes reach
     */
    public void failover(Duration window) {
        long since = System.nanoTime() - window.toNanos();
        tables.values().forEach(table -> table.undoSince(since));
        tables.values().forEach(InMemoryTable::gap);
    }

    /**
     * Deletes a row silently, as a store evicting under memory pressure does (Redis with {@code maxmemory}): no
     * tombstone, no message, in every table.
     *
     * @param hash the row's hash
     */
    public void evict(String hash) {
        tables.values().forEach(table -> table.evict(hash));
    }

    /**
     * Sets the probability that a message is lost without a gap reported, as a feed losing messages silently does
     * (Redis pub/sub across a disconnect it did not notice).
     *
     * @param probability between 0 and 1
     */
    public void setDropProbability(double probability) {
        faults.dropProbability = probability;
    }

    /**
     * Runs a step between two rows of a read of several rows where the profile does not read them at one instant
     * ({@link StoreProfile#snapshotReads()}), outside the table's monitor: a write committed there is seen by the rest
     * of the read, as with MongoDB's {@code $in}.
     *
     * @param step the step, {@code null} for none
     */
    public void setBetweenRows(@Nullable Runnable step) {
        faults.betweenRows = step;
    }

    /**
     * Makes the next read operations ({@code read}, {@code readVersions}) fail, without anything else failing.
     *
     * @param count how many reads fail
     */
    public void failNextReads(int count) {
        faults.failNextReads(count);
    }

    /**
     * Takes the store down or brings it back. While down, every operation fails and nothing is delivered; coming
     * back reports a gap to every subscription.
     *
     * @param down whether the store is down
     */
    public void setDown(boolean down) {
        boolean wasDown = faults.down;
        faults.down = down;
        if (wasDown && !down) {
            tables.values().forEach(table -> table.resume(false));
            tables.values().forEach(InMemoryTable::gap);
        } else if (!wasDown && down) {
            tables.values().forEach(InMemoryTable::dropUndelivered);
            tables.values().forEach(InMemoryTable::interrupt);
        }
    }

    /**
     * Interrupts every subscription while the store keeps answering: listeners get {@code onLost()} at once, messages
     * queue, and {@code resume()} fails until {@link #resumeSubscriptions(boolean)}.
     */
    public void interruptSubscriptions() {
        tables.values().forEach(InMemoryTable::interrupt);
    }

    /**
     * Lets interrupted subscriptions be resumed: the messages queued meanwhile follow, or are lost. Both are allowed
     * by the contract, since the core counts every resume as a gap.
     *
     * @param deliverMissed whether the messages queued meanwhile are delivered
     */
    public void resumeSubscriptions(boolean deliverMissed) {
        tables.values().forEach(table -> table.resume(deliverMissed));
    }

    /**
     * Loses every message not delivered yet and reports a gap to every subscription.
     */
    public void gap() {
        tables.values().forEach(InMemoryTable::gap);
    }

    /**
     * Holds delivery back until {@link #resumeDelivery()}; messages keep queueing.
     */
    public void pauseDelivery() {
        faults.paused = true;
    }

    /**
     * Resumes delivery held back by {@link #pauseDelivery()}.
     */
    public void resumeDelivery() {
        faults.paused = false;
        tables.values().forEach(InMemoryTable::wakeDeliveries);
    }

    /**
     * Returns whether every subscription has handed over everything committed so far (nothing queued, no batch on
     * its way); for tests waiting until the store has nothing left to deliver.
     *
     * @return whether delivery is idle
     */
    public boolean isIdle() {
        return tables.values().stream().allMatch(InMemoryTable::isIdle) && scanning.stream().allMatch(store ->
                store.scans() >= scansAtCommit.getOrDefault(store, 0L) + 2);
    }

    /**
     * Sets the probability that a message is delivered twice.
     *
     * @param probability between 0 and 1
     */
    public void setDuplicateProbability(double probability) {
        faults.duplicateProbability = probability;
    }

    /**
     * Reorders the messages of a delivered batch across keys (per key the commit order stays, and nothing moves
     * across a write of the control row, {@code Store#CONTROL_HASH}).
     *
     * @param reorder whether to reorder
     */
    public void setReorderAcrossKeys(boolean reorder) {
        faults.reorderAcrossKeys = reorder;
    }

    /**
     * Delays every delivered batch by a random duration up to the given one.
     *
     * @param maxDelay the maximum delay, zero for none
     */
    public void setDeliveryDelay(Duration maxDelay) {
        faults.maxDelayNanos = maxDelay.toNanos();
    }

    /**
     * Delays writes between taking their time and becoming visible, as a real store takes a write's time when the
     * statement runs and makes it visible at commit: a delayed write gets the write time of its start, waits a random
     * duration up to the given one outside the table's monitor, and is applied then.
     *
     * @param maxDelay    the maximum delay, zero for none
     * @param probability the share of writes delayed, between 0 and 1
     */
    public void setCommitDelay(Duration maxDelay, double probability) {
        faults.commitDelayNanos = maxDelay.toNanos();
        faults.commitDelayProbability = probability;
    }

    /**
     * Runs a step before every batch with a conditional write ({@code Store#write}) is applied, outside the table's monitor:
     * between an operation's read and its write, e.g. to run an {@code invalidateAll} there.
     *
     * @param step the step, {@code null} for none
     */
    public void setBeforeConditionalWrite(@Nullable Runnable step) {
        faults.beforeConditionalWrite = step;
    }

    /**
     * Runs a step after a write of the control row ({@code Store#CONTROL_HASH}, the core's {@code invalidateAll}) took
     * its write time and before it becomes visible, outside the table's monitor: the window between a write's time
     * and its commit, opened on purpose for the control row.
     *
     * @param step the step, {@code null} for none
     */
    public void setBeforeInvalidateAllApplies(@Nullable Runnable step) {
        faults.beforeInvalidateAllApplies = step;
    }

    // a write of the control row: its write time taken first, then the step, then the write at that time
    <T> T invalidating(Supplier<T> write) {
        Runnable step = faults.beforeInvalidateAllApplies;
        if (step == null) {
            return write(write);
        }
        commitTime.set(nextWriteTime());
        try {
            step.run();
            return write.get();
        } finally {
            commitTime.remove();
        }
    }

    // a write, delayed between its time and its effect if the fault says so
    <T> T write(Supplier<T> write) {
        long maxDelay = faults.commitDelayNanos;
        if (maxDelay <= 0 || nextDouble() >= faults.commitDelayProbability) {
            return write.get();
        }
        commitTime.set(nextWriteTime());
        try {
            LockSupport.parkNanos(nextLong(maxDelay));
            return write.get();
        } finally {
            commitTime.remove();
        }
    }

    void beforeConditionalWrite() {
        Runnable step = faults.beforeConditionalWrite;
        if (step != null) {
            step.run();
        }
    }

    // a write committed: the polled stores are idle again only after two more scans
    void committed() {
        scanning.forEach(store -> scansAtCommit.put(store, store.scans()));
    }

    // the store's time now: a monotonic clock never behind a write time handed out, so that no age is negative; a
    // wall clock as it reads, in its unit, where a write after the reading has a negative age
    synchronized long nowMicros() {
        if (profile.clock() == StoreProfile.Clock.MONOTONIC) {
            return Math.max(clockMicros.getAsLong(), lastWriteTime);
        }
        return wallClock();
    }

    // the time of a write (a delayed write keeps the one it took before it waited): a monotonic clock above every
    // earlier write time; a wall clock as it reads, repeats and steps back included
    synchronized long nextWriteTime() {
        Long pinned = commitTime.get();
        if (pinned != null) {
            return pinned;
        }
        if (profile.clock() == StoreProfile.Clock.MONOTONIC) {
            lastWriteTime = Math.max(lastWriteTime + 1, clockMicros.getAsLong());
            return lastWriteTime;
        }
        return wallClock();
    }

    private long wallClock() {
        long micros = clockMicros.getAsLong() - stepBackMicros;
        return profile.clock() == StoreProfile.Clock.MILLIS ? Math.floorDiv(micros, 1_000) * 1_000 : micros;
    }

    // the next value of the store's own sequence
    synchronized long nextSequence() {
        return ++sequence;
    }

    synchronized double nextDouble() {
        return random.nextDouble();
    }

    synchronized int nextInt(int bound) {
        return random.nextInt(bound);
    }

    synchronized long nextLong(long bound) {
        return bound <= 0 ? 0 : random.nextLong(bound);
    }

    private static long systemMicros() {
        return ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
    }

    static final class Faults {

        private int failingWrites;
        private int partlyFailingWrites;
        private int failingReads;
        private boolean failingOutcomeUnknown;
        volatile boolean down;
        volatile boolean paused;
        volatile double duplicateProbability;
        volatile boolean reorderAcrossKeys;
        volatile long maxDelayNanos;
        volatile long commitDelayNanos;
        volatile double commitDelayProbability;
        volatile @Nullable Runnable beforeConditionalWrite;
        volatile @Nullable Runnable beforeInvalidateAllApplies;
        volatile @Nullable Runnable betweenRows;
        volatile double dropProbability;

        synchronized void failNextWritesPartly(int count) {
            this.partlyFailingWrites = count;
        }

        synchronized boolean takePartialFailure() {
            if (partlyFailingWrites == 0) {
                return false;
            }
            partlyFailingWrites--;
            return true;
        }

        synchronized void failNextWrites(int count, boolean outcomeUnknown) {
            this.failingWrites = count;
            this.failingOutcomeUnknown = outcomeUnknown;
        }

        synchronized void failNextReads(int count) {
            this.failingReads = count;
        }

        synchronized boolean takeReadFailure() {
            if (failingReads == 0) {
                return false;
            }
            failingReads--;
            return true;
        }

        // returns null if the write succeeds, otherwise whether it commits before failing
        synchronized @Nullable Boolean takeWriteFailure() {
            if (failingWrites == 0) {
                return null;
            }
            failingWrites--;
            return failingOutcomeUnknown;
        }
    }
}
