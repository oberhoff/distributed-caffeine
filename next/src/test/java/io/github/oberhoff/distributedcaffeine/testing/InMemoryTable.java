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
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.Expected;
import io.github.oberhoff.distributedcaffeine.spi.ExpectedEntry;
import io.github.oberhoff.distributedcaffeine.spi.ReadResult;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

// One cache's rows and subscriptions. Every operation holds the table's monitor, so writes are serialized per table
// and their messages are queued in commit order.
final class InMemoryTable {

    private static final byte[] NO_KEY = new byte[0];

    private final InMemoryDatabase database;
    private final Map<String, StoredRow> rows = new HashMap<>();
    private final List<InMemoryDelivery> deliveries = new CopyOnWriteArrayList<>();
    // the cut-off: the stamp of the last invalidateAll (the control row), 0 if none
    private long invalidatedAll;

    InMemoryTable(InMemoryDatabase database) {
        this.database = database;
    }

    InMemoryDatabase database() {
        return database;
    }

    @SuppressWarnings("ArrayRecordComponent") // never compared or hashed, only held by hash
    private record StoredRow(byte[] key, byte @Nullable [] value, State state, long version, long writeTimeMicros,
                             long stamp, long basis) {
    }

    // reads

    synchronized ReadResult read(Collection<String> hashes, Duration maxAge) {
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
        return new ReadResult(result, invalidatedAll);
    }

    synchronized VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        checkUp();
        checkRead();
        long now = database.nowMicros();
        Map<String, Long> versions = new LinkedHashMap<>();
        Map<String, Long> stamps = new LinkedHashMap<>();
        Map<String, Long> bases = new LinkedHashMap<>();
        for (String hash : hashes) {
            StoredRow row = rows.get(hash);
            if (row != null && row.state == State.VALUE && younger(row, maxAge, now)) {
                versions.put(hash, row.version);
                stamps.put(hash, row.stamp);
                bases.put(hash, row.basis);
            }
        }
        return new VersionsResult(versions, stamps, bases, invalidatedAll);
    }

    // writes

    synchronized Map<String, Long> put(Collection<Entry> entries) {
        return write(() -> {
            Map<String, Long> versions = new LinkedHashMap<>();
            List<Change> messages = new ArrayList<>();
            for (Entry entry : entries) {
                long version = store(entry.hash(), entry.key(), entry.value(), State.VALUE, Row.NO_BASIS);
                versions.put(entry.hash(), version);
                messages.add(new Change(Change.Kind.WRITTEN, entry.hash(), version));
            }
            return new Outcome<>(versions, messages);
        });
    }

    synchronized Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge) {
        return write(() -> {
            long now = database.nowMicros();
            Map<String, WriteResult> results = new LinkedHashMap<>();
            List<Change> messages = new ArrayList<>();
            for (ExpectedEntry expected : entries) {
                Entry entry = expected.entry();
                StoredRow current = rows.get(entry.hash());
                boolean matches;
                if (expected.expectsMissing()) {
                    matches = current == null;
                } else {
                    matches = current != null && current.version == expected.expectedVersion();
                }
                // decided under an older cut-off: refused in the same step (Store#checksBasis)
                matches &= !database.checksBasis || invalidatedAll == 0 || expected.basis() >= invalidatedAll;
                if (matches) {
                    long version = store(entry.hash(), entry.key(), entry.value(), State.VALUE, expected.basis());
                    results.put(entry.hash(), WriteResult.written(version));
                    messages.add(new Change(Change.Kind.WRITTEN, entry.hash(), version));
                } else {
                    results.put(entry.hash(), WriteResult.conflict(asRead(entry.hash(), current, maxAge, now),
                            current == null ? 0L : current.version));
                }
            }
            return new Outcome<>(results, messages);
        });
    }

    synchronized Map<String, Long> invalidate(Collection<String> hashes) {
        return write(() -> {
            Map<String, Long> versions = new LinkedHashMap<>();
            List<Change> messages = new ArrayList<>();
            for (String hash : hashes) {
                StoredRow current = rows.get(hash);
                long version = store(hash, current == null ? NO_KEY : current.key, null, State.TOMBSTONE,
                        Row.NO_BASIS);
                versions.put(hash, version);
                messages.add(new Change(Change.Kind.INVALIDATED, hash, version));
            }
            return new Outcome<>(versions, messages);
        });
    }

    synchronized Map<String, Long> invalidateIfUnchanged(Collection<Expected> expectations) {
        Map<String, Long> expected = Expected.byHash(expectations);
        return write(() -> {
            Map<String, Long> versions = new LinkedHashMap<>();
            List<Change> messages = new ArrayList<>();
            expected.forEach((hash, expectedVersion) -> {
                StoredRow current = rows.get(hash);
                boolean matches = expectedVersion == ExpectedEntry.MISSING ? current == null
                        : current != null && current.version == expectedVersion;
                if (matches) {
                    long version = store(hash, current == null ? NO_KEY : current.key, null, State.TOMBSTONE,
                        Row.NO_BASIS);
                    versions.put(hash, version);
                    messages.add(new Change(Change.Kind.INVALIDATED, hash, version));
                }
            });
            return new Outcome<>(versions, messages);
        });
    }

    // the control row only: its stamp is the cut-off, before which every row counts as missing
    synchronized long invalidateAll() {
        return write(() -> {
            invalidatedAll = database.nextStamp();
            return new Outcome<>(invalidatedAll, List.of(Change.invalidatedAll(invalidatedAll)));
        });
    }

    // maintenance

    synchronized int expire(Duration maxAge) {
        return write(() -> {
            long now = database.nowMicros();
            List<Change> messages = new ArrayList<>();
            for (String hash : List.copyOf(rows.keySet())) {
                StoredRow current = rows.get(hash);
                if (current.state == State.VALUE && visible(current) && !younger(current, maxAge, now)) {
                    // the write time of the expiry: its grace period before pruning counts from now
                    long version = store(hash, current.key, null, State.EXPIRED, Row.NO_BASIS);
                    messages.add(new Change(Change.Kind.EXPIRED, hash, version));
                }
            }
            return new Outcome<>(messages.size(), messages);
        });
    }

    synchronized int pruneToSize(long maxSize) {
        checkUp();
        List<Map.Entry<String, StoredRow>> values = rows.entrySet().stream()
                .filter(entry -> entry.getValue().state == State.VALUE && visible(entry.getValue()))
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
        List<String> expired = rows.entrySet().stream()
                .filter(entry -> !visible(entry.getValue())
                        || (entry.getValue().state != State.VALUE && !younger(entry.getValue(), retention, now)))
                .map(Map.Entry::getKey)
                .toList();
        expired.forEach(rows::remove);
        return expired.size();
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
    private <T> T write(Supplier<Outcome<T>> write) {
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

    private long store(String hash, byte[] key, byte @Nullable [] value, State state, long basis) {
        long now = database.nowMicros();
        long clock = database.versionMicros();
        StoredRow current = rows.get(hash);
        // a per-key counter: a new row starts at the store's time, above every version an earlier row of the key had
        long version = current == null ? clock : current.version + 1;
        rows.put(hash, new StoredRow(key, value, state, version, now, database.nextStamp(), basis));
        return version;
    }

    // written after the last invalidateAll and decided under it
    private boolean visible(StoredRow row) {
        return Row.visible(row.stamp, row.basis, invalidatedAll);
    }

    private static boolean younger(StoredRow row, Duration maxAge, long nowMicros) {
        return nowMicros - row.writeTimeMicros < maxAge.toNanos() / 1_000;
    }

    private static @Nullable Row asRead(String hash, @Nullable StoredRow row, Duration maxAge, long nowMicros) {
        if (row == null || (row.state == State.VALUE && !younger(row, maxAge, nowMicros))) {
            return null;
        }
        Duration age = Duration.ofNanos(Math.max(0, nowMicros - row.writeTimeMicros) * 1_000);
        return new Row(hash, row.key, row.value, row.state, row.version, age, row.stamp, row.basis);
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
