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
package io.github.oberhoff.distributedcaffeine.spi;

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * A {@link Store}, and a {@link ChangeScan} for the core to poll, built on a few primitives that nearly every store
 * offers (VISION.md, "Design study: portable store primitives", "Two levels"): a plain SQL database through JDBC, a
 * key-value or document store. An adapter implements the primitives (the abstract methods below) and has a
 * distributed cache; it may override any operation of {@link Store} with a native one later (a batch in one round
 * trip, say), one at a time. As built here: one primitive per key and operation, maintenance by scans of the cache's
 * rows without values. The {@link Store#CONTROL_HASH control row} is an ordinary row to the primitives; only
 * maintenance knows it, comparing the ages of one scan: a row at least as old as it is hidden (VISION.md, "Target
 * design: {@code invalidateAll} by the store's time, hidden on read").
 * Every primitive concerns the rows of the store's own cache only, and runs atomically per row.
 * <p>
 * The contract, checked through {@code StoreContractTest} and {@code ChangeScanContractTest} on the
 * implementation:
 * <ol>
 *     <li><b>Versions by the core:</b> every write stores the version it is given, and a row's version only grows:
 *     an advancing write applies only over a smaller one; conditions compare the stored version with the expected one
 *     for equality (no {@code +1}, no start value).</li>
 *     <li><b>Write time by the store:</b> every write sets the row's write time on the store's clock; a read or a
 *     scan returns every row's age measured at one instant, so that the ages of one read or scan compare like the
 *     times of the writes.</li>
 *     <li><b>Atomic per row:</b> a conditional write or delete decides and writes in one step: of concurrent ones on
 *     one version, one wins; of concurrent inserts of one id, one wins. A write that is not applied changes
 *     nothing.</li>
 *     <li><b>Visibility and durability:</b> as {@link Store} point 3.</li>
 *     <li><b>Bytes:</b> keys and values come back byte for byte.</li>
 *     <li><b>Failures and settings:</b> as {@link Store} points 8 and 9.</li>
 * </ol>
 */
public abstract class PrimitiveStore implements Store, ChangeScan {

    // the primitives

    /**
     * Reads rows by hash, with their ages by the store's clock, all measured at one instant of the read.
     *
     * @param hashes the hashes
     * @return the rows found, by hash
     */
    protected abstract Map<String, Row> readRows(Collection<String> hashes);

    /**
     * Writes a row only if none exists or its version is smaller than the given one (the version only grows).
     *
     * @param hash    the hash
     * @param key     the key, as written; {@code null} keeps the row's key (a tombstone), or stores none for a new row
     * @param value   the value, {@code null} unless the state is {@link State#VALUE}
     * @param state   the state
     * @param version the version the row gets
     * @return whether it was written; {@code false} if the row has a version at least as large
     */
    protected abstract boolean advanceRow(String hash, byte @Nullable [] key, byte @Nullable [] value, State state,
                                          long version);

    /**
     * Writes a row only if it still has the expected version.
     *
     * @param hash     the hash
     * @param expected the version it must still have
     * @param version  the version the row gets
     * @param key      the key, as written; {@code null} keeps the row's key
     * @param value    the value, {@code null} unless the state is {@link State#VALUE}
     * @param state    the state
     * @return whether it was written: {@code false} if the row has another version or none
     */
    protected abstract boolean replaceRow(String hash, long expected, long version, byte @Nullable [] key,
                                          byte @Nullable [] value, State state);

    /**
     * Writes a row only if none exists for the hash.
     *
     * @param hash    the hash
     * @param version the version the row gets
     * @param key     the key, as written; {@code null} stores none (a tombstone)
     * @param value   the value, {@code null} unless the state is {@link State#VALUE}
     * @param state   the state
     * @return whether it was written: {@code false} if a row exists
     */
    protected abstract boolean insertRow(String hash, long version, byte @Nullable [] key, byte @Nullable [] value,
                                         State state);

    /**
     * Deletes a row only if it still has the given version.
     *
     * @param hash    the hash
     * @param version the version it must still have
     * @return whether it was deleted
     */
    protected abstract boolean deleteRow(String hash, long version);

    /**
     * Returns every row of the cache, without key and value, with their ages measured at one instant of the scan
     * (maintenance).
     *
     * @return the rows
     */
    protected abstract List<Head> scanRows();

    /**
     * Returns the rows of the cache written within the given duration before the scan, by the store's clock, without
     * key and value ({@link ChangeScan}); rows written earlier may be returned too.
     *
     * @param within how far back
     * @return the rows
     */
    protected abstract List<Head> scanRowsWrittenWithin(Duration within);

    // reads: every row as it is; which ones the control row hides is the core's to decide

    @Override
    public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
        Map<String, Row> found = readRows(hashes);
        Map<String, Row> result = new LinkedHashMap<>();
        for (String hash : hashes) {
            Row row = found.get(hash);
            if (row == null) {
                continue;
            }
            // a value too old to count: reported as expired, at its version, so that a write can be conditioned on it
            result.put(hash, row.state() != State.VALUE || younger(row.age(), maxAge) ? row
                    : new Row(hash, row.key(), null, State.EXPIRED, row.version(), row.age()));
        }
        return result;
    }

    @Override
    public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        Map<String, Row> found = readRows(hashes);
        Map<String, Long> versions = new LinkedHashMap<>();
        Map<String, Duration> ages = new LinkedHashMap<>();
        for (String hash : hashes) {
            Row row = found.get(hash);
            if (row != null && row.state() == State.VALUE && younger(row.age(), maxAge)) {
                versions.put(hash, row.version());
                ages.put(hash, row.age());
            }
        }
        return new VersionsResult(versions, ages);
    }

    // writes: one primitive per key, in the order of their hashes; a batch failing after a part was written has its
    // outcome unknown

    @Override
    public WriteResult write(Collection<Write> writes) {
        List<Write> ordered = new ArrayList<>(writes);
        ordered.sort(Comparator.comparing(Write::hash));
        Set<String> conflicts = new LinkedHashSet<>();
        for (int i = 0; i < ordered.size(); i++) {
            Write write = ordered.get(i);
            State state = write.isTombstone() ? State.TOMBSTONE : State.VALUE;
            try {
                if (write.isAdvance()) {
                    if (!advanceRow(write.hash(), write.key(), write.value(), state, write.version())) {
                        conflicts.add(write.hash());
                    }
                } else if (!(write.expectsMissing()
                        ? insertRow(write.hash(), write.version(), write.key(), write.value(), state)
                        : replaceRow(write.hash(), write.expected(), write.version(), write.key(), write.value(),
                        state))) {
                    conflicts.add(write.hash());
                }
            } catch (StoreException e) {
                throw i == 0 ? e : e.afterPartlyApplied();
            }
        }
        return WriteResult.refused(conflicts);
    }

    // maintenance: one scan each, so that every age compares with the control row's; every change conditional on the
    // version scanned, so instances running it at once change each row once. Reserved rows are never touched

    @Override
    public int expire(Duration maxAge, long version) {
        List<Head> heads = scanRows();
        Duration cutoff = cutoff(heads);
        int expired = 0;
        for (Head head : heads) {
            if (head.state() == State.VALUE && maintained(head) && !hidden(head, cutoff)
                    && !younger(head.age(), maxAge) && head.version() < version
                    && replaceRow(head.hash(), head.version(), version, null, null, State.EXPIRED)) {
                expired++;
            }
        }
        return expired;
    }

    // keeps the newest maxSize values not hidden (ties by id); the rows beyond that rank are the same for every
    // instance pruning at once; of those, the ones younger than the minimum age stay
    @Override
    public int pruneToSize(long maxSize, Duration minAge) {
        List<Head> heads = scanRows();
        Duration cutoff = cutoff(heads);
        List<Head> values = new ArrayList<>();
        for (Head head : heads) {
            if (head.state() == State.VALUE && maintained(head) && !hidden(head, cutoff)) {
                values.add(head);
            }
        }
        values.sort(Comparator.comparing(Head::age).thenComparing(Head::hash));
        int pruned = 0;
        for (int i = (int) Math.min(Integer.MAX_VALUE, maxSize); i < values.size(); i++) {
            if (younger(values.get(i).age(), minAge)) {
                continue;
            }
            if (deleteRow(values.get(i).hash(), values.get(i).version())) {
                pruned++;
            }
        }
        return pruned;
    }

    // rows hidden by the control row go at once, without a grace period: they count as missing already
    @Override
    public int pruneTombstones(Duration retention) {
        List<Head> heads = scanRows();
        Duration cutoff = cutoff(heads);
        int pruned = 0;
        for (Head head : heads) {
            boolean doomed = hidden(head, cutoff) || (head.state() != State.VALUE && !younger(head.age(), retention));
            if (maintained(head) && doomed && deleteRow(head.hash(), head.version())) {
                pruned++;
            }
        }
        return pruned;
    }

    // the scan the core polls: every row, reserved ones included (the control row's change is invalidateAll's)

    @Override
    public List<Change> changedWithin(Duration within) {
        List<Change> changes = new ArrayList<>();
        for (Head head : scanRowsWrittenWithin(within)) {
            changes.add(new Change(switch (head.state()) {
                case VALUE -> Change.Kind.WRITTEN;
                case TOMBSTONE -> Change.Kind.INVALIDATED;
                case EXPIRED -> Change.Kind.EXPIRED;
            }, head.hash(), head.version()));
        }
        return changes;
    }

    // helpers

    // the control row's age in the scan, null if there is none (no invalidateAll yet)
    private static @Nullable Duration cutoff(List<Head> heads) {
        for (Head head : heads) {
            if (CONTROL_HASH.equals(head.hash())) {
                return head.age();
            }
        }
        return null;
    }

    // written at or before the control row: at least as old as it, by the same scan's clock
    private static boolean hidden(Head head, @Nullable Duration cutoff) {
        return cutoff != null && maintained(head) && head.age().compareTo(cutoff) >= 0;
    }

    // an ordinary row: maintenance leaves reserved rows ('#') alone
    private static boolean maintained(Head head) {
        return !head.hash().startsWith("#");
    }

    private static boolean younger(Duration age, Duration maxAge) {
        return age.compareTo(maxAge) < 0;
    }

    /**
     * A row as a scan of {@link PrimitiveStore} returns it: without key and value.
     *
     * @param hash    the hash of the row's key, or a reserved one of the library's own rows
     * @param state   the state of the row
     * @param version the version of the row's last write
     * @param age     how long ago the row was written, by the store's clock at the scan
     */
    public record Head(String hash, State state, long version, Duration age) {

        public Head {
            requireNonNull(hash, "hash cannot be null");
            requireNonNull(state, "state cannot be null");
            requireNonNull(age, "age cannot be null");
        }
    }
}
