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

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
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
 * messages are queued on commit in commit order, and every subscription delivers on a thread of its own.
 */
public final class InMemoryDatabase {

    private final LongSupplier clockMicros;
    private final Random random;
    private final Map<String, InMemoryTable> tables = new ConcurrentHashMap<>();
    final Faults faults = new Faults();
    // the write time a delayed write took before it waited (see setCommitDelay), for the thread running it
    private final ThreadLocal<Long> commitTime = new ThreadLocal<>();
    // the last write time handed out: write times increase strictly, also within a microsecond, so that the ages of
    // one read order the writes as they happened (a clock that never goes backwards, Store point 2)
    private long lastWriteTime;

    /**
     * Creates a database on the system clock with a random seed.
     */
    public InMemoryDatabase() {
        this(InMemoryDatabase::systemMicros, new Random().nextLong());
    }

    /**
     * Creates a database.
     *
     * @param clockMicros the store's clock, in microseconds since the epoch
     * @param seed        the seed for the random faults (duplicates, reordering, delays), for reproducible runs
     */
    public InMemoryDatabase(LongSupplier clockMicros, long seed) {
        this.clockMicros = requireNonNull(clockMicros);
        this.random = new Random(seed);
    }

    /**
     * Opens a store of a cache instance on the given table.
     *
     * @param table the table of the cache
     * @return a new store, as one instance would hold it
     */
    public InMemoryStore openStore(String table) {
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
        return tables.values().stream().allMatch(InMemoryTable::isIdle);
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

    // the store's time now: never behind a write time handed out, so that no age is negative
    synchronized long nowMicros() {
        return Math.max(clockMicros.getAsLong(), lastWriteTime);
    }

    // the time of a write: the store's time, but above every earlier write time (a delayed write keeps the one it
    // took before it waited)
    synchronized long nextWriteTime() {
        Long pinned = commitTime.get();
        if (pinned != null) {
            return pinned;
        }
        lastWriteTime = Math.max(lastWriteTime + 1, clockMicros.getAsLong());
        return lastWriteTime;
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
        synchronized Boolean takeWriteFailure() {
            if (failingWrites == 0) {
                return null;
            }
            failingWrites--;
            return failingOutcomeUnknown;
        }
    }
}
