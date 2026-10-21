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

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * The store of one cache instance, created by its adapter, non-generic: keys and values travel as bytes in the
 * formats fixed when the store was created. A store that can announce its writes, which distribution needs, also
 * implements {@link ChangeFeed}; one that cannot supports only {@code DistributionMode.NONE}.
 * <p>
 * The contract every implementation must fulfil; each point is checked by the tests named after it in
 * {@code StoreContractTest} ({@code contractN_...}), points 8 and 9 by each adapter's own tests:
 * <ol>
 *     <li><b>Versions:</b> the version per hash is a counter kept by the store in the row: a new row starts at the
 *     store's time in microseconds (or at a counter of the store that never goes backwards), every later write of the
 *     row is {@code old + 1}. It increases strictly, also across instances and after a row was deleted (the start
 *     lies above every earlier version of the key), needs no clock for a row that exists, and is returned to the
 *     writer.</li>
 *     <li><b>Clock:</b> the age of a row comes from the store's clock; the stamp of a write ({@link Row#stamp()})
 *     too, or from a counter of the store: a write confirmed before another one began has the lower stamp, also
 *     across rows (the cut-off of {@link #invalidateAll} compares them).</li>
 *     <li><b>Visibility and durability:</b> a read, on any instance of the cache, reflects every write that was
 *     confirmed (returned to its writer) before the read began: its version, or a later one. A confirmed write is not
 *     lost later, unless in an event (a crash, a failover) that every subscription of the change feed sees as a loss
 *     or a gap.</li>
 *     <li><b>Atomic conditional writes:</b> {@link #putIfUnchanged} and {@link #invalidateIfUnchanged} decide and
 *     write as one step per key: of concurrent conditional writes on one version, one wins.</li>
 *     <li><b>Idempotent maintenance:</b> expiry and pruning run on several instances at once without expiring a row
 *     twice or pruning more than once.</li>
 *     <li><b>Bytes and cache:</b> keys and values come back byte for byte as written, in the formats of
 *     {@link StoreSettings}; a store reads and writes only its own cache's rows, also where several caches share a
 *     table or collection.</li>
 *     <li><b>Semantics:</b> every method does what its documentation says, for every state a row can be in.</li>
 *     <li><b>Failures:</b> a store reports a failure only as {@link StoreException}, classified (applied or not,
 *     retryable or not); a write applied in part reports its outcome unknown
 *     ({@link StoreException#afterPartlyApplied()}); a store does not retry, the core decides. Anything else it
 *     throws counts as a failure that is not retryable, with the outcome of a write unknown.</li>
 *     <li><b>Settings:</b> a store applies the {@link StoreSettings} it was created with: no operation waits longer
 *     than the timeout for the store.</li>
 * </ol>
 * All methods take batches; a single key is a collection of one, and keys of a batch are independent (a result per
 * key, no atomicity across keys).
 */
public interface Store extends AutoCloseable {

    // reads

    /**
     * Reads rows. Returns value rows younger than the maximum age, and tombstones and expired rows (without value);
     * value rows older than the maximum age and missing rows are omitted. Rows written at or before the last
     * {@link #invalidateAll} are returned as they are (the core counts them as missing). Also returns the stamp of the
     * last {@code invalidateAll}, even if no row is found; with no hashes, only that (the core's check after a
     * conditional write).
     *
     * @param hashes the hashes to read, possibly none
     * @param maxAge the maximum age of a value row
     * @return the rows found, by hash, and the stamp of the last {@code invalidateAll}
     */
    ReadResult read(Collection<String> hashes, Duration maxAge);

    /**
     * Reads only the versions and stamps of value rows younger than the maximum age (revalidation,
     * {@code containsKey}), and the stamp of the last {@link #invalidateAll}.
     *
     * @param hashes the hashes to read
     * @param maxAge the maximum age of a value row
     * @return the versions and stamps of the value rows found, by hash, and the stamp of the last
     * {@code invalidateAll}
     */
    VersionsResult readVersions(Collection<String> hashes, Duration maxAge);

    // writes: "announces" names what the store's ChangeFeed, if it has one, delivers on commit

    /**
     * Writes values unconditionally. Announces {@link Change.Kind#WRITTEN} per entry.
     *
     * @param entries the entries to write
     * @return the version assigned to each write, by hash
     */
    Map<String, Long> put(Collection<Entry> entries);

    /**
     * Writes values conditionally (ordering rule 6), atomically per key. Announces {@link Change.Kind#WRITTEN} per
     * written entry.
     *
     * @param entries the conditional writes
     * @param maxAge  the maximum age, so that a value row older than it counts as holding no value
     * @return per hash, whether it was written and, if not, the row that prevented it
     */
    Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge);

    /**
     * Whether {@link #putIfUnchanged} also refuses a write whose {@link ExpectedEntry#basis()} is below the current
     * cut-off, as a conflict (VISION.md, "The decision's cut-off on every row"): checked in the same atomic step as
     * the version, against the cut-off as of that step. Optional: a store whose writes cannot see the cut-off (an
     * update conditioned on one document only, say) does not; its writes are hidden by their basis instead. With it,
     * the core decides on a held value (one store operation for a write), since a write decided before an
     * {@code invalidateAll} it has not heard of then conflicts rather than vanishes.
     *
     * @return {@code true} if conditional writes refuse a basis below the cut-off; {@code false} by default
     */
    default boolean checksBasis() {
        return false;
    }

    /**
     * Turns rows into tombstones, creating a tombstone where no row exists, so that the version reaches loads in
     * flight. Announces {@link Change.Kind#INVALIDATED} per hash.
     *
     * @param hashes the hashes to invalidate
     * @return the version of each tombstone, by hash
     */
    Map<String, Long> invalidate(Collection<String> hashes);

    /**
     * Writes tombstones only where the row still has the expected version, whatever its state, or, for
     * {@link ExpectedEntry#MISSING}, only where no row exists ({@code remove(k, v)}, and {@code remove(k)} on a key
     * without a value, which must still move the version so that a load in flight conflicts). Announces
     * {@link Change.Kind#INVALIDATED} per tombstone written.
     *
     * @param expected the expected version (or {@link ExpectedEntry#MISSING}), by hash
     * @return the version of each tombstone written, by hash; hashes not matching are omitted
     */
    Map<String, Long> invalidateIfUnchanged(Collection<Expected> expected);

    /**
     * Writes the cache's control row and nothing else: its stamp becomes the cache's cut-off, before which every row
     * counts as missing (VISION.md, "invalidateAll: a cut-off time"), whatever the size of the cache. Announces one
     * {@link Change.Kind#INVALIDATED_ALL} carrying the stamp.
     *
     * @return the stamp of the control row's write, as announced
     */
    long invalidateAll();

    // maintenance: scheduled by the core, idempotent here

    /**
     * Marks value rows at least as old as the maximum age as expired, with a new version and the store's time as the
     * new write time, so that their grace period before {@link #pruneTombstones} counts from the expiry. Announces
     * {@link Change.Kind#EXPIRED} per row. Safe to run on several instances concurrently: every row is expired and
     * announced once.
     *
     * @param maxAge the maximum age
     * @return the number of rows expired
     */
    int expire(Duration maxAge);

    /**
     * Deletes the oldest value rows (by write time) beyond the maximum size, silently.
     *
     * @param maxSize the maximum number of value rows
     * @return the number of rows deleted
     */
    int pruneToSize(long maxSize);

    /**
     * Deletes tombstones and expired rows written at least the retention ago, and every row written at or before the
     * last {@link #invalidateAll} (counted as missing already, so without a grace period), silently; never the control
     * row.
     *
     * @param retention how long they are kept
     * @return the number of rows deleted
     */
    int pruneTombstones(Duration retention);

    /**
     * Closes the store. Shared connections stay open for other users; subscriptions to its {@link ChangeFeed} are
     * closed by whoever subscribed (the core closes its own first).
     */
    @Override
    void close();
}
