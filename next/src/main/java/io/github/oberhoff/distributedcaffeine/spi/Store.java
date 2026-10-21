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
 *     <li><b>Versions:</b> the version per hash comes from the store ({@code max(old + 1, server time in
 *     microseconds)}), increases strictly, also across instances and after a row was deleted, and is returned to
 *     the writer.</li>
 *     <li><b>Clock:</b> the age of a row and the time of a read come from the store's clock, the clock of the
 *     versions.</li>
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
     * value rows older than the maximum age and missing rows are omitted. Also returns the store's time of the read,
     * on the clock of the versions, even if no row is found.
     *
     * @param hashes the hashes to read
     * @param maxAge the maximum age of a value row
     * @return the rows found, by hash, and the time of the read
     */
    ReadResult read(Collection<String> hashes, Duration maxAge);

    /**
     * Reads only the versions of value rows younger than the maximum age (revalidation, {@code containsKey}).
     *
     * @param hashes the hashes to read
     * @param maxAge the maximum age of a value row
     * @return the versions of the value rows found, by hash
     */
    Map<String, Long> readVersions(Collection<String> hashes, Duration maxAge);

    // writes: "announces" names what the store's ChangeFeed, if it has one, delivers on commit

    /**
     * Writes values unconditionally. Announces {@link Kind#WRITTEN} per entry.
     *
     * @param entries the entries to write
     * @return the version assigned to each write, by hash
     */
    Map<String, Long> put(Collection<Entry> entries);

    /**
     * Writes values conditionally (ordering rule 6), atomically per key. Announces {@link Kind#WRITTEN} per written
     * entry.
     *
     * @param entries the conditional writes
     * @param maxAge  the maximum age, so that a value row older than it counts as holding no value
     * @return per hash, whether it was written and, if not, the row that prevented it
     */
    Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge);

    /**
     * Turns rows into tombstones, creating a tombstone where no row exists, so that the version reaches loads in
     * flight. Announces {@link Kind#INVALIDATED} per hash.
     *
     * @param hashes the hashes to invalidate
     * @return the version of each tombstone, by hash
     */
    Map<String, Long> invalidate(Collection<String> hashes);

    /**
     * Writes tombstones only where the row still has the expected version, whatever its state, or, for
     * {@link ExpectedEntry#MISSING}, only where no row exists ({@code remove(k, v)}, and {@code remove(k)} on a key
     * without a value, which must still move the version so that a load in flight conflicts). Announces
     * {@link Kind#INVALIDATED} per tombstone written.
     *
     * @param expected the expected version (or {@link ExpectedEntry#MISSING}), by hash
     * @return the version of each tombstone written, by hash; hashes not matching are omitted
     */
    Map<String, Long> invalidateIfUnchanged(Map<String, Long> expected);

    /**
     * Turns every value row of the cache into a tombstone and gives every other row (tombstones, expired rows) a new
     * version too, so that every load in flight conflicts. Announces one {@link Kind#INVALIDATED_ALL} carrying the
     * store's time of the sweep, which is at most every version the sweep assigns; adapters may additionally announce
     * {@link Kind#INVALIDATED} per row (MongoDB's change streams do).
     *
     * @return the store's time of the sweep, as announced
     */
    long invalidateAll();

    // maintenance: scheduled by the core, idempotent here

    /**
     * Marks value rows at least as old as the maximum age as expired, with a new version. Announces
     * {@link Kind#EXPIRED} per row. Safe to run on several instances concurrently: every row is expired and announced
     * once.
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
     * Deletes tombstones and expired rows written at least the retention ago, silently.
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
