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
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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

    private final InMemoryDatabase database;
    private final Map<String, StoredRow> rows = new HashMap<>();
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

    // reads: every age of one read measured at the same instant

    synchronized Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
        checkUp();
        checkRead();
        long now = database.nowMicros();
        Map<String, Row> result = new LinkedHashMap<>();
        for (String hash : hashes) {
            Row row = asRead(hash, rows.get(hash), maxAge, now);
            if (row != null) {
                result.put(hash, row);
            }
        }
        return result;
    }

    synchronized VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        checkUp();
        checkRead();
        long now = database.nowMicros();
        Map<String, Long> versions = new LinkedHashMap<>();
        Map<String, Duration> ages = new LinkedHashMap<>();
        for (String hash : hashes) {
            StoredRow row = rows.get(hash);
            if (row != null && row.state == State.VALUE && younger(row, maxAge, now)) {
                versions.put(hash, row.version);
                ages.put(hash, age(row, now));
            }
        }
        return new VersionsResult(versions, ages);
    }

    // writes

    // values and tombstones at the versions given, each under its condition; returns the hashes that conflicted
    synchronized Set<String> write(Collection<Write> writes) {
        return committing(() -> {
            Set<String> conflicts = new LinkedHashSet<>();
            List<Change> messages = new ArrayList<>();
            for (Write write : writes) {
                StoredRow current = rows.get(write.hash());
                boolean matches;
                if (write.isUnconditional()) {
                    matches = true;
                } else if (write.expectsMissing()) {
                    matches = current == null;
                } else {
                    matches = current != null && current.version == write.expected();
                }
                if (!matches) {
                    conflicts.add(write.hash());
                } else if (write.isTombstone()) {
                    store(write.hash(), current == null ? NO_KEY : current.key, null, State.TOMBSTONE,
                            write.version());
                    messages.add(new Change(Change.Kind.INVALIDATED, write.hash(), write.version()));
                } else {
                    store(write.hash(), requireNonNull(write.key()), write.value(), State.VALUE, write.version());
                    messages.add(new Change(Change.Kind.WRITTEN, write.hash(), write.version()));
                }
            }
            return new Outcome<>(conflicts, messages);
        });
    }

    // maintenance: never a reserved row; rows the control row hides are neither expired nor counted

    synchronized int expire(Duration maxAge, long version) {
        return committing(() -> {
            long now = database.nowMicros();
            List<Change> messages = new ArrayList<>();
            for (String hash : List.copyOf(rows.keySet())) {
                StoredRow current = rows.get(hash);
                if (!reserved(hash) && current.state == State.VALUE && !hidden(current)
                        && !younger(current, maxAge, now)) {
                    // the write time of the expiry: its grace period before pruning counts from now
                    store(hash, current.key, null, State.EXPIRED, version);
                    messages.add(new Change(Change.Kind.EXPIRED, hash, version));
                }
            }
            return new Outcome<>(messages.size(), messages);
        });
    }

    synchronized int pruneToSize(long maxSize) {
        checkUp();
        List<Map.Entry<String, StoredRow>> values = rows.entrySet().stream()
                .filter(entry -> !reserved(entry.getKey()) && entry.getValue().state == State.VALUE
                        && !hidden(entry.getValue()))
                // oldest first; among equally old ones the higher hash goes first, so the lowest is kept
                .sorted(Comparator.<Map.Entry<String, StoredRow>>comparingLong(entry -> entry.getValue().writeTimeMicros)
                        .thenComparing(Map.Entry::getKey, Comparator.reverseOrder()))
                .toList();
        int excess = (int) Math.max(0, values.size() - maxSize);
        for (int i = 0; i < excess; i++) {
            rows.remove(values.get(i).getKey());
        }
        return excess;
    }

    synchronized int pruneTombstones(Duration retention) {
        checkUp();
        long now = database.nowMicros();
        List<String> deleted = rows.entrySet().stream()
                .filter(entry -> !reserved(entry.getKey()) && (hidden(entry.getValue())
                        || (entry.getValue().state != State.VALUE && !younger(entry.getValue(), retention, now))))
                .map(Map.Entry::getKey)
                .toList();
        deleted.forEach(rows::remove);
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

    private record Outcome<T>(T result, List<Change> messages) {
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
        }
        if (Boolean.TRUE.equals(failure)) {
            throw new StoreException("injected failure, outcome unknown", true);
        }
        return outcome.result;
    }

    // the version as the core chose it: stored, never computed here
    private void store(String hash, byte[] key, byte @Nullable [] value, State state, long version) {
        rows.put(hash, new StoredRow(key, value, state, version, database.nextWriteTime()));
    }

    private static boolean reserved(String hash) {
        return hash.startsWith("#");
    }

    // written at or before the control row: counted as missing by the core
    private boolean hidden(StoredRow row) {
        StoredRow control = rows.get(Store.CONTROL_HASH);
        return control != null && row.writeTimeMicros <= control.writeTimeMicros;
    }

    private static Duration age(StoredRow row, long nowMicros) {
        return Duration.ofNanos(Math.max(0, nowMicros - row.writeTimeMicros) * 1_000);
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
