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
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;

/**
 * A {@link Store}, and a {@link ChangeScan} for the core to poll, built on a few primitives that nearly every store
 * offers (VISION.md, "Design study: portable store primitives", "Two levels"): a plain SQL database through JDBC, a
 * key-value or document store. An adapter implements the primitives (the abstract methods below) and has a
 * distributed cache; it may override any operation of {@link Store} with a native one later (a batch in one round
 * trip, say), one at a time. As built here: one primitive per key and operation, maintenance by scans of the cache's
 * rows without values, the cut-off of {@link Store#invalidateAll} as a reserved row whose stamp every read returns,
 * visibility as {@link Row#visible(long, long, long)}.
 * Every primitive concerns the rows of the store's own cache only, and runs atomically per row.
 * <p>
 * The contract, checked through {@code StoreContractTest} and {@code ChangeScanContractTest} on the
 * implementation:
 * <ol>
 *     <li><b>Versions by the store:</b> a new row gets a version above every version an earlier row of its id had
 *     (the store's time in microseconds, or a counter that never goes backwards); every later write of the row
 *     {@code old + 1}. The write returns it.</li>
 *     <li><b>Write time and stamp by the store:</b> every write sets the row's write time (ages, on the store's clock)
 *     and its stamp (a write confirmed before another one began has the lower stamp, also across rows); the basis
 *     is stored as given.</li>
 *     <li><b>Atomic per row:</b> a conditional write or delete decides and writes in one step: of concurrent ones on
 *     one version, one wins; of concurrent inserts of one id, one wins.</li>
 *     <li><b>Visibility and durability:</b> as {@link Store} point 3.</li>
 *     <li><b>Bytes:</b> keys and values come back byte for byte.</li>
 *     <li><b>Failures and settings:</b> as {@link Store} points 8 and 9.</li>
 * </ol>
 * An implementation whose {@link #replaceRow} and {@link #insertRow} also refuse a basis below the cut-off (the
 * stamp of the row {@link #CONTROL_ID}), in the same atomic step, overrides {@link #checksBasis()}.
 */
public abstract class PrimitiveStore implements Store, ChangeScan {

    /**
     * The id of the control row of {@link Store#invalidateAll}, whose stamp is the cut-off (the core refuses hashes
     * starting with '#').
     */
    protected static final String CONTROL_ID = "#invalidated";
    private static final byte[] NO_KEY = new byte[0];

    // the primitives

    /**
     * Reads rows by hash, with their ages by the store's clock at the read.
     *
     * @param hashes the hashes
     * @return the rows found, by hash
     */
    protected abstract Map<String, Row> readRows(Collection<String> hashes);

    /**
     * Writes a row whatever it holds: a new one, or one more version of the existing one; its basis is
     * {@link Row#NO_BASIS} (an upsert is never decided on a read).
     *
     * @param hash  the hash
     * @param key   the key, as written
     * @param value the value, {@code null} unless the state is {@link State#VALUE}
     * @param state the state
     * @return the version written
     */
    protected abstract long upsertRow(String hash, byte[] key, byte @Nullable [] value, State state);

    /**
     * Writes a row only if it still has the given version.
     *
     * @param hash    the hash
     * @param version the version it must still have
     * @param key     the key, as written
     * @param value   the value, {@code null} unless the state is {@link State#VALUE}
     * @param state   the state
     * @param basis   the basis ({@link Row#basis()})
     * @return the version written, or {@code null} if the row has another version or none
     */
    protected abstract @Nullable Long replaceRow(String hash, long version, byte[] key, byte @Nullable [] value,
                                                 State state, long basis);

    /**
     * Writes a row only if none exists for the hash.
     *
     * @param hash  the hash
     * @param key   the key, as written
     * @param value the value, {@code null} unless the state is {@link State#VALUE}
     * @param state the state
     * @param basis the basis ({@link Row#basis()})
     * @return the version written, or {@code null} if a row exists
     */
    protected abstract @Nullable Long insertRow(String hash, byte[] key, byte @Nullable [] value, State state,
                                                long basis);

    /**
     * Deletes a row only if it still has the given version.
     *
     * @param hash    the hash
     * @param version the version it must still have
     * @return whether it was deleted
     */
    protected abstract boolean deleteRow(String hash, long version);

    /**
     * Returns every row of the cache in the given state, or in any if {@code null}, without key and value
     * (maintenance).
     *
     * @param state the state, or {@code null} for every row
     * @return the rows
     */
    protected abstract List<Head> scanRows(@Nullable State state);

    /**
     * Returns the rows of the cache written within the given duration before the scan, by the store's clock, without
     * key and value ({@link ChangeScan}); rows written earlier may be returned too.
     *
     * @param within how far back
     * @return the rows
     */
    protected abstract List<Head> scanRowsWrittenWithin(Duration within);

    // reads

    @Override
    public ReadResult read(Collection<String> hashes, Duration maxAge) {
        Map<String, Row> found = readRows(withControl(hashes));
        Map<String, Row> result = new LinkedHashMap<>();
        for (String hash : hashes) {
            Row row = found.get(hash);
            if (row != null && (row.state() != State.VALUE || younger(row.age(), maxAge))) {
                result.put(hash, row);
            }
        }
        return new ReadResult(result, cutoff(found));
    }

    @Override
    public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        Map<String, Row> found = readRows(withControl(hashes));
        Map<String, Long> versions = new LinkedHashMap<>();
        Map<String, Long> stamps = new LinkedHashMap<>();
        Map<String, Long> bases = new LinkedHashMap<>();
        for (String hash : hashes) {
            Row row = found.get(hash);
            if (row != null && row.state() == State.VALUE && younger(row.age(), maxAge)) {
                versions.put(hash, row.version());
                stamps.put(hash, row.stamp());
                bases.put(hash, row.basis());
            }
        }
        return new VersionsResult(versions, stamps, bases, cutoff(found));
    }

    // writes: one primitive per key; a batch failing after a part was written has its outcome unknown

    @Override
    public Map<String, Long> put(Collection<Entry> entries) {
        Map<String, Long> versions = new LinkedHashMap<>();
        each(versions, entries, entry -> versions.put(entry.hash(),
                upsertRow(entry.hash(), entry.key(), entry.value(), State.VALUE)));
        return versions;
    }

    @Override
    public Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge) {
        Map<String, WriteResult> results = new LinkedHashMap<>();
        each(results, entries, expected -> {
            Entry entry = expected.entry();
            Long version = expected.expectsMissing()
                    ? insertRow(entry.hash(), entry.key(), entry.value(), State.VALUE, expected.basis())
                    : replaceRow(entry.hash(), expected.expectedVersion(), entry.key(), entry.value(), State.VALUE,
                    expected.basis());
            results.put(entry.hash(), version != null ? WriteResult.written(version)
                    : conflict(entry.hash(), maxAge));
        });
        return results;
    }

    // what prevented a conditional write: the row as a read sees it (a value too old to count is no row, but its
    // version is reported), or nothing if it is gone meanwhile
    private WriteResult conflict(String hash, Duration maxAge) {
        Row row = readRows(List.of(hash)).get(hash);
        if (row == null) {
            return WriteResult.conflict(null, 0);
        }
        boolean counts = row.state() != State.VALUE || younger(row.age(), maxAge);
        return WriteResult.conflict(counts ? row : null, row.version());
    }

    @Override
    public Map<String, Long> invalidate(Collection<String> hashes) {
        Map<String, Long> versions = new LinkedHashMap<>();
        each(versions, new TreeSet<>(hashes), hash -> versions.put(hash,
                upsertRow(hash, NO_KEY, null, State.TOMBSTONE)));
        return versions;
    }

    @Override
    public Map<String, Long> invalidateIfUnchanged(Collection<Expected> expected) {
        Map<String, Long> versions = new LinkedHashMap<>();
        each(versions, expected, expectation -> {
            String hash = expectation.hash();
            Long written = expectation.expectsMissing() ? insertRow(hash, NO_KEY, null, State.TOMBSTONE, Row.NO_BASIS)
                    : replaceRow(hash, expectation.expectedVersion(), NO_KEY, null, State.TOMBSTONE, Row.NO_BASIS);
            if (written != null) {
                versions.put(hash, written);
            }
        });
        return versions;
    }

    @Override
    public long invalidateAll() {
        upsertRow(CONTROL_ID, NO_KEY, null, State.TOMBSTONE);
        return cutoff(readRows(List.of(CONTROL_ID)));
    }

    // maintenance: every change conditional on the version scanned, so instances running it at once change each row
    // once

    @Override
    public int expire(Duration maxAge) {
        long cutoff = cutoff(readRows(List.of(CONTROL_ID)));
        int expired = 0;
        for (Head head : scanRows(State.VALUE)) {
            if (!younger(head.age(), maxAge) && visible(head, cutoff)
                    && replaceRow(head.hash(), head.version(), NO_KEY, null, State.EXPIRED, Row.NO_BASIS) != null) {
                expired++;
            }
        }
        return expired;
    }

    // keeps the newest maxSize visible values (ties by id); the rows beyond that rank are the same for every instance
    // pruning at once
    @Override
    public int pruneToSize(long maxSize) {
        long cutoff = cutoff(readRows(List.of(CONTROL_ID)));
        List<Head> values = new ArrayList<>();
        for (Head head : scanRows(State.VALUE)) {
            if (visible(head, cutoff)) {
                values.add(head);
            }
        }
        values.sort(Comparator.comparing(Head::age).thenComparing(Head::hash));
        int pruned = 0;
        for (int i = (int) Math.min(Integer.MAX_VALUE, maxSize); i < values.size(); i++) {
            if (deleteRow(values.get(i).hash(), values.get(i).version())) {
                pruned++;
            }
        }
        return pruned;
    }

    @Override
    public int pruneTombstones(Duration retention) {
        long cutoff = cutoff(readRows(List.of(CONTROL_ID)));
        int pruned = 0;
        for (Head head : scanRows(null)) {
            boolean doomed = !visible(head, cutoff) || (head.state() != State.VALUE && !younger(head.age(), retention));
            if (!CONTROL_ID.equals(head.hash()) && doomed && deleteRow(head.hash(), head.version())) {
                pruned++;
            }
        }
        return pruned;
    }

    // the scan the core polls

    @Override
    public List<Change> changedWithin(Duration within) {
        List<Change> changes = new ArrayList<>();
        for (Head head : scanRowsWrittenWithin(within)) {
            if (!CONTROL_ID.equals(head.hash())) {
                changes.add(new Change(switch (head.state()) {
                    case VALUE -> Change.Kind.WRITTEN;
                    case TOMBSTONE -> Change.Kind.INVALIDATED;
                    case EXPIRED -> Change.Kind.EXPIRED;
                }, head.hash(), head.version()));
            }
        }
        return changes;
    }

    // helpers

    private static Set<String> withControl(Collection<String> hashes) {
        Set<String> ids = new LinkedHashSet<>(hashes);
        ids.add(CONTROL_ID);
        return ids;
    }

    private static long cutoff(Map<String, Row> found) {
        Row control = found.get(CONTROL_ID);
        return control == null ? 0 : control.stamp();
    }

    private static boolean visible(Head head, long cutoff) {
        return Row.visible(head.stamp(), head.basis(), cutoff);
    }

    private static boolean younger(Duration age, Duration maxAge) {
        return age.compareTo(maxAge) < 0;
    }

    @FunctionalInterface
    private interface Step<T> {
        void apply(T item);
    }

    // each item in turn; a failure after a part of the batch was written has its outcome unknown
    private static <T> void each(Map<String, ?> done, Iterable<? extends T> items, Step<T> step) {
        for (T item : items) {
            try {
                step.apply(item);
            } catch (StoreException e) {
                throw done.isEmpty() ? e : e.afterPartlyApplied();
            }
        }
    }

    /**
     * A row as a scan of {@link PrimitiveStore} returns it: without key and value.
     *
     * @param hash    the hash of the row's key, or a reserved one of the library's own rows
     * @param state   the state of the row
     * @param version the version the store assigned to the row's last write
     * @param age     how long ago the row was written, by the store's clock at the scan
     * @param stamp   the store's stamp of the row's last write (see {@link Row#stamp()})
     * @param basis   the basis of the row's last write (see {@link Row#basis()})
     */
    public record Head(String hash, State state, long version, Duration age, long stamp, long basis) {

        public Head {
            requireNonNull(hash, "hash cannot be null");
            requireNonNull(state, "state cannot be null");
            requireNonNull(age, "age cannot be null");
        }
    }
}
