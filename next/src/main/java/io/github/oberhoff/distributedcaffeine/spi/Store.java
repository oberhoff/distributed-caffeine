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
import java.util.Set;

/**
 * The store of one cache instance, created by its adapter, non-generic: keys and values travel as bytes in the
 * formats fixed when the store was created. A store that can announce its writes, which distribution needs, also
 * implements {@link ChangeFeed}; one that cannot supports only {@code DistributionMode.NONE}.
 * <p>
 * The contract every implementation must fulfil; each point is checked by the tests named after it in
 * {@code StoreContractTest} ({@code contractN_...}), points 8 and 9 by each adapter's own tests:
 * <ol>
 *     <li><b>Versions:</b> the version of a row is the one its last write carried ({@link Write#version()}),
 *     chosen by the core; the store stores it and compares it for equality, nothing else: no {@code +1}, no start
 *     value, no order.</li>
 *     <li><b>Clock:</b> the age of a row comes from the store's clock, set by every write ({@link Row#age()}); within
 *     one read every age is measured at the same instant, so the ages of a read compare like the times of the writes
 *     (the core compares them with the age of the {@link #CONTROL_HASH control row}). A clock that never goes
 *     backwards is best; a wall clock is fine within the core's allowance for skew.</li>
 *     <li><b>Visibility and durability:</b> a read, on any instance of the cache, reflects every write that was
 *     confirmed (returned to its writer) before the read began: its version, or a later one. A confirmed write is not
 *     lost later, unless in an event (a crash, a failover) that every subscription of the change feed sees as a loss
 *     or a gap.</li>
 *     <li><b>Atomic conditional writes:</b> {@link #write} decides and writes as one step per key: of concurrent
 *     writes conditioned on one version, or on the key missing, one wins.</li>
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
 * <p>
 * <b>Reserved rows.</b> Hashes starting with {@code #} are the core's own rows (keys never have one): the
 * {@link #CONTROL_HASH control row} of {@code invalidateAll}, and the heartbeat's row. A store reads and writes them
 * like any other row, but maintenance ({@link #expire}, {@link #pruneToSize}, {@link #pruneTombstones}) never touches
 * them.
 */
public interface Store extends AutoCloseable {

    /**
     * The hash of the control row (VISION.md, "Target design: {@code invalidateAll} by the store's time, hidden on
     * read"): the core writes it on {@code invalidateAll}, and every row written at or before it (at least as old as
     * it) counts as missing. Maintenance never expires or prunes it, ignores the rows it hides for expiry and size
     * pruning, and deletes them in {@link #pruneTombstones}.
     */
    String CONTROL_HASH = "#invalidated";

    // reads

    /**
     * Reads rows. Returns value rows younger than the maximum age, tombstones and expired rows, and value rows at
     * least as old as the maximum age as {@link State#EXPIRED} (without value, with their version, so that a write
     * can be conditioned on it); missing rows are omitted. Every row as it is, with its age, also rows the control
     * row hides (the core decides, comparing ages within the read).
     *
     * @param hashes the hashes to read
     * @param maxAge the maximum age of a value row
     * @return the rows found, by hash
     */
    Map<String, Row> read(Collection<String> hashes, Duration maxAge);

    /**
     * Reads only the versions and ages of value rows younger than the maximum age (revalidation,
     * {@code containsKey}), also of rows the control row hides (the core decides).
     *
     * @param hashes the hashes to read
     * @param maxAge the maximum age of a value row
     * @return the versions and ages of the value rows found, by hash
     */
    VersionsResult readVersions(Collection<String> hashes, Duration maxAge);

    /**
     * How far the store's clock may step back, or how coarse it counts (VISION.md, "Target design: {@code
     * invalidateAll} by the store's time, hidden on read"): the core waits this long before {@code invalidateAll}
     * returns, so that a write after it is never stamped at or before the control row. A store whose write times never
     * go backwards and never repeat (a monotonic counter, a strictly increasing clock) returns zero.
     *
     * @return the allowance; 10 ms by default, for a wall clock counting in milliseconds
     */
    default Duration clockAllowance() {
        return Duration.ofMillis(10);
    }

    // writes: "announces" names what the store's ChangeFeed, if it has one, delivers on commit

    /**
     * Writes values and tombstones, each at its version and under its condition ({@link Write#expected()}),
     * atomically per key: unconditionally, only if the row still has the expected version (whatever its state), or
     * only if no row exists. A tombstone keeps the row's key, or has none where no row existed. Announces
     * {@link Change.Kind#WRITTEN} per value written and {@link Change.Kind#INVALIDATED} per tombstone written, at the
     * write's version.
     *
     * @param writes the writes, at most one per hash
     * @return the hashes whose writes were not applied (their condition did not hold); empty if all were
     */
    Set<String> write(Collection<Write> writes);

    // maintenance: scheduled by the core, idempotent here

    /**
     * Marks value rows at least as old as the maximum age as expired, at the given version and with the store's time
     * as the new write time, so that their grace period before {@link #pruneTombstones} counts from the expiry.
     * Announces {@link Change.Kind#EXPIRED} per row. Neither reserved rows nor rows the control row hides are
     * expired. Safe to run on several instances concurrently: every row is expired and announced once.
     *
     * @param maxAge  the maximum age
     * @param version the version every row expired now gets, chosen by the core (one no key had before)
     * @return the number of rows expired
     */
    int expire(Duration maxAge, long version);

    /**
     * Deletes the oldest value rows (by write time) beyond the maximum size, silently. Neither reserved rows nor rows
     * the control row hides count or are deleted here.
     *
     * @param maxSize the maximum number of value rows
     * @return the number of rows deleted
     */
    int pruneToSize(long maxSize);

    /**
     * Deletes tombstones and expired rows written at least the retention ago, and every row written at or before the
     * control row (counted as missing already, so without a grace period), silently; never a reserved row.
     *
     * @param retention how long tombstones and expired rows are kept
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
