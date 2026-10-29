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

import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
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
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

// One cache's rows and subscriptions. Every operation holds the table's monitor, so writes are serialized per table
// and their messages are queued in commit order. The control row (Store#CONTROL_HASH) is an ordinary row here, as in
// any store; only maintenance knows it: reserved rows ('#') are never expired or pruned, and the rows it hides
// (written at or before it) are neither expired nor counted for the size, but deleted with the old tombstones.
final class InMemoryTable {

    private static final byte[] NO_KEY = new byte[0];
    // the core's version format (InternalVersion): milliseconds since 2020 above 20 bits of counter and node
    private static final long CORE_EPOCH_MILLIS = 1_577_836_800_000L;
    private static final int CORE_TIME_SHIFT = 20;

    private final InMemoryDatabase database;
    private final Map<String, StoredRow> rows = new HashMap<>();
    // every change of a row in the last seconds, oldest first, for a failover to undo (InMemoryDatabase#failover)
    private final Deque<Undo> journal = new ArrayDeque<>();
    private static final long JOURNAL_NANOS = 30_000_000_000L;
    private final List<InMemoryDelivery> deliveries = new CopyOnWriteArrayList<>();

    InMemoryTable(InMemoryDatabase database) {
        this.database = database;
    }

    InMemoryDatabase database() {
        return database;
    }

    @SuppressWarnings("ArrayRecordComponent") // never compared or hashed, only held by hash
    private record StoredRow(byte[] key, byte @Nullable [] value, State state, long version, long writeTimeMicros) {
    }

    // a row as it was before a change (null: missing), and when the change happened
    private record Undo(long nanoTime, String hash, @Nullable StoredRow previous) {
    }

    // reads: every age of one read measured at the same instant; the rows at that instant too (one snapshot), or
    // each as it is when it is read (StoreProfile#snapshotReads, MongoDB's $in), the step between rows run outside
    // the monitor, where a write committed then is seen by the rest of the read

    Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
        Map<String, Row> result = new LinkedHashMap<>();
        reading(hashes, (hash, row, now) -> {
            Row read = asRead(hash, row, maxAge, now);
            if (read != null) {
                result.put(hash, read);
            }
        });
        return result;
    }

    VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        Map<String, Long> versions = new LinkedHashMap<>();
        Map<String, Duration> ages = new LinkedHashMap<>();
        reading(hashes, (hash, row, now) -> {
            if (row != null && row.state == State.VALUE && younger(row, maxAge, now)) {
                versions.put(hash, row.version);
                ages.put(hash, age(row, now));
            }
        });
        return new VersionsResult(versions, ages);
    }

    private interface RowReader {
        void accept(String hash, @Nullable StoredRow row, long nowMicros);
    }

    private void reading(Collection<String> hashes, RowReader reader) {
        long now;
        synchronized (this) {
            checkUp();
            checkRead();
            now = database.nowMicros();
            if (database.profile().snapshotReads()) {
                hashes.forEach(hash -> reader.accept(hash, rows.get(hash), now));
                return;
            }
        }
        boolean first = true;
        for (String hash : hashes) {
            Runnable step = database.faults.betweenRows;
            if (!first && step != null) {
                step.run();
            }
            first = false;
            StoredRow row;
            synchronized (this) {
                row = rows.get(hash);
            }
            reader.accept(hash, row, now);
        }
    }

    // writes

    // values and tombstones, each under its condition, at the version proposed or, where the profile assigns, at one
    // the store chooses (StoreProfile.Versioning)
    synchronized WriteResult write(Collection<Write> writes) {
        boolean assigns = database.profile().assigns(writes.size());
        // a batch failing part way (InMemoryDatabase#failNextWritesPartly): where the store writes row by row, the
        // first half applies, the rest does not; otherwise nothing does
        boolean partly = writes.size() > 1 && database.faults.takePartialFailure();
        if (partly && database.profile().batch() == StoreProfile.Batch.ATOMIC) {
            checkUp();
            throw new StoreException("injected failure of an atomic batch, not committed", false);
        }
        int applying = partly ? writes.size() / 2 : writes.size();
        return committing(() -> {
            Set<String> conflicts = new LinkedHashSet<>();
            Map<String, Long> assigned = new LinkedHashMap<>();
            List<Change> messages = new ArrayList<>();
            int index = 0;
            for (Write write : writes) {
                if (index++ == applying) {
                    return new Outcome<>(null, messages, new StoreException(
                            "injected failure of a batch written row by row", false).afterPartlyApplied());
                }
                StoredRow current = rows.get(write.hash());
                boolean matches;
                if (write.isAdvance()) {
                    // assigned: always above the stored version; proposed: the version only grows, applied over a
                    // smaller one or where no row is
                    matches = assigns || current == null || current.version < write.version();
                } else if (write.expectsMissing()) {
                    matches = current == null;
                } else {
                    matches = current != null && current.version == write.expected();
                }
                if (!matches) {
                    conflicts.add(write.hash());
                    if (current == null && !write.isAdvance() && !write.expectsMissing()
                            && database.profile().tombstoneOnPurgedRow()) {
                        // the row was purged since it was read: the store's upsert inserts, and writes a tombstone
                        // so that no value from a failed condition is visible (MongoDB's bulk write, step 5)
                        store(write.hash(), NO_KEY, null, State.TOMBSTONE, write.version());
                        messages.add(new Change(Change.Kind.INVALIDATED, write.hash(), write.version()));
                    }
                    continue;
                }
                long version = assigns ? assign(current, write.version()) : write.version();
                if (version != write.version()) {
                    assigned.put(write.hash(), version);
                }
                if (write.isTombstone()) {
                    store(write.hash(), current == null ? NO_KEY : current.key, null, State.TOMBSTONE, version);
                    messages.add(new Change(Change.Kind.INVALIDATED, write.hash(), version));
                } else {
                    store(write.hash(), requireNonNull(write.key()), write.value(), State.VALUE, version);
                    messages.add(new Change(Change.Kind.WRITTEN, write.hash(), version));
                }
            }
            return new Outcome<>(new WriteResult(conflicts, assigned), messages, null);
        });
    }

    // the version an assigning store chooses: its own domain (a new row at the sequence, which every write draws, an
    // existing one one above), or the core's (the largest of one above the stored version, the proposal, and the
    // store's time in the core's format)
    private long assign(@Nullable StoredRow current, long proposed) {
        long sequence = database.nextSequence();
        if (database.profile().versioning() == StoreProfile.Versioning.ASSIGNING_OWN) {
            return current == null ? sequence : current.version + 1;
        }
        long floor = Math.max(0, database.nowMicros() / 1_000 - CORE_EPOCH_MILLIS) << CORE_TIME_SHIFT;
        return Math.max(current == null ? 0 : current.version + 1, Math.max(proposed, floor));
    }

    // maintenance: never a reserved row; rows the control row hides are neither expired nor counted

    synchronized int expire(Duration maxAge, long version) {
        return committing(() -> {
            long now = database.nowMicros();
            List<Change> messages = new ArrayList<>();
            for (String hash : List.copyOf(rows.keySet())) {
                StoredRow current = rows.get(hash);
                boolean own = database.profile().versioning() == StoreProfile.Versioning.ASSIGNING_OWN;
                // in a domain of the store's own the given version means nothing: every expiry assigns one above
                if (maintained(hash) && current.state == State.VALUE && !hidden(current)
                        && !younger(current, maxAge, now) && (own || current.version < version)) {
                    long expired = own ? assign(current, version) : version;
                    // the write time of the expiry: its grace period before pruning counts from now
                    store(hash, current.key, null, State.EXPIRED, expired);
                    messages.add(new Change(Change.Kind.EXPIRED, hash, expired));
                }
            }
            return new Outcome<>(messages.size(), messages, null);
        });
    }

    synchronized int pruneToSize(long maxSize, Duration minAge) {
        checkUp();
        long now = database.nowMicros();
        List<Map.Entry<String, StoredRow>> values = rows.entrySet().stream()
                .filter(entry -> maintained(entry.getKey()) && entry.getValue().state == State.VALUE
                        && !hidden(entry.getValue()))
                // oldest first; among equally old ones the higher hash goes first, so the lowest is kept
                .sorted(Comparator.<Map.Entry<String, StoredRow>>comparingLong(entry -> entry.getValue().writeTimeMicros)
                        .thenComparing(Map.Entry::getKey, Comparator.reverseOrder()))
                .toList();
        // beyond the rank, but none younger than the minimum age
        int excess = (int) Math.max(0, values.size() - maxSize);
        int pruned = 0;
        for (int i = 0; i < excess; i++) {
            if (!younger(values.get(i).getValue(), minAge, now)) {
                remove(values.get(i).getKey());
                pruned++;
            }
        }
        return pruned;
    }

    synchronized int pruneTombstones(Duration retention) {
        checkUp();
        long now = database.nowMicros();
        List<String> deleted = rows.entrySet().stream()
                .filter(entry -> maintained(entry.getKey()) && (hidden(entry.getValue())
                        || (entry.getValue().state != State.VALUE && !younger(entry.getValue(), retention, now))))
                .map(Map.Entry::getKey)
                .toList();
        deleted.forEach(this::remove);
        return deleted.size();
    }

    // the changes written within the window, by the store's clock: the latest state of each row, without values
    synchronized List<Change> changedWithin(Duration within) {
        checkUp();
        checkRead();
        long now = database.nowMicros();
        long micros = within.toNanos() / 1_000;
        List<Change> changes = new ArrayList<>();
        rows.forEach((hash, row) -> {
            if (now - row.writeTimeMicros <= micros) {
                changes.add(new Change(switch (row.state) {
                    case VALUE -> Change.Kind.WRITTEN;
                    case TOMBSTONE -> Change.Kind.INVALIDATED;
                    case EXPIRED -> Change.Kind.EXPIRED;
                }, hash, row.version));
            }
        });
        return changes;
    }

    // faults: a failover undoing the changes since a time, an eviction

    synchronized void undoSince(long nanoTime) {
        while (!journal.isEmpty() && journal.peekLast().nanoTime >= nanoTime) {
            Undo undo = journal.pollLast();
            if (undo.previous == null) {
                rows.remove(undo.hash);
            } else {
                rows.put(undo.hash, undo.previous);
            }
        }
        deliveries.forEach(InMemoryDelivery::dropUndelivered);
    }

    synchronized void evict(String hash) {
        remove(hash);
    }

    // subscriptions

    synchronized InMemoryDelivery subscribe(ChangeListener listener) {
        checkUp();
        InMemoryDelivery delivery = new InMemoryDelivery(database, listener, deliveries::remove);
        deliveries.add(delivery);
        return delivery;
    }

    synchronized void gap() {
        deliveries.forEach(InMemoryDelivery::gap);
    }

    synchronized void dropUndelivered() {
        deliveries.forEach(InMemoryDelivery::dropUndelivered);
    }

    void interrupt() {
        deliveries.forEach(InMemoryDelivery::interrupt);
    }

    void resume(boolean deliverMissed) {
        deliveries.forEach(delivery -> delivery.allowResume(deliverMissed));
    }

    void wakeDeliveries() {
        deliveries.forEach(InMemoryDelivery::wake);
    }

    boolean isIdle() {
        return deliveries.stream().allMatch(InMemoryDelivery::isIdle);
    }

    // internals

    // what a write did: its result, or the failure it ended with after its messages' part applied
    private record Outcome<T>(@Nullable T result, List<Change> messages, @Nullable StoreException failure) {
    }

    // runs a write under the table's monitor (held by the caller), with the injected failures: before commit
    // nothing happens; after commit the write and its messages stand, and the failure is reported anyway
    private <T> T committing(Supplier<Outcome<T>> write) {
        checkUp();
        Boolean failure = database.faults.takeWriteFailure();
        if (Boolean.FALSE.equals(failure)) {
            throw new StoreException("injected failure, not committed", false);
        }
        Outcome<T> outcome = write.get();
        if (!outcome.messages.isEmpty()) {
            deliveries.forEach(delivery -> delivery.enqueue(outcome.messages));
            database.committed();
        }
        if (Boolean.TRUE.equals(failure)) {
            throw new StoreException("injected failure, outcome unknown", true);
        }
        if (outcome.failure != null) {
            throw outcome.failure;
        }
        return requireNonNull(outcome.result);
    }

    // the version as the core chose it: stored, never computed here
    private void store(String hash, byte[] key, byte @Nullable [] value, State state, long version) {
        journal(hash, rows.put(hash, new StoredRow(key, value, state, version, database.nextWriteTime())));
    }

    private void remove(String hash) {
        StoredRow removed = rows.remove(hash);
        if (removed != null) {
            journal(hash, removed);
        }
    }

    private void journal(String hash, @Nullable StoredRow previous) {
        long now = System.nanoTime();
        journal.addLast(new Undo(now, hash, previous));
        while (!journal.isEmpty() && now - journal.peekFirst().nanoTime > JOURNAL_NANOS) {
            journal.pollFirst();
        }
    }

    // an ordinary row: maintenance leaves reserved rows ('#') alone
    private static boolean maintained(String hash) {
        return !hash.startsWith("#");
    }

    // written at or before the control row: counted as missing by the core
    private boolean hidden(StoredRow row) {
        StoredRow control = rows.get(Store.CONTROL_HASH);
        return control != null && row.writeTimeMicros <= control.writeTimeMicros;
    }

    // negative for a row written after the read's instant (a read row by row, a wall clock stepping back): the ages of
    // one read compare as the write times do (Store#read)
    private static Duration age(StoredRow row, long nowMicros) {
        return Duration.ofNanos((nowMicros - row.writeTimeMicros) * 1_000);
    }

    private static boolean younger(StoredRow row, Duration maxAge, long nowMicros) {
        return nowMicros - row.writeTimeMicros < maxAge.toNanos() / 1_000;
    }

    // a value row at least as old as the maximum age reads as expired, without its value but with its version
    private static @Nullable Row asRead(String hash, @Nullable StoredRow row, Duration maxAge, long nowMicros) {
        if (row == null) {
            return null;
        }
        Duration age = age(row, nowMicros);
        if (row.state == State.VALUE && !younger(row, maxAge, nowMicros)) {
            return new Row(hash, row.key, null, State.EXPIRED, row.version, age);
        }
        return new Row(hash, row.key, row.value, row.state, row.version, age);
    }

    private void checkRead() {
        if (database.faults.takeReadFailure()) {
            throw new StoreException("injected read failure", false);
        }
    }

    private void checkUp() {
        if (database.faults.down) {
            throw new StoreException("injected: the store is down", false);
        }
    }
}
