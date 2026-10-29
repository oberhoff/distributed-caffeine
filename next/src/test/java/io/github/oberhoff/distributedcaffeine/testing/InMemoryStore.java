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
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeScan;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;

/**
 * The store of one cache instance on an {@link InMemoryDatabase}.
 */
public final class InMemoryStore implements Store, ChangeFeed, ChangeScan {

    private final InMemoryTable table;
    private final List<ChangeSubscription> subscriptions = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    InMemoryStore(InMemoryTable table) {
        this.table = table;
    }

    @Override
    public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
        checkOpen();
        return table.read(requireNonNull(hashes), requireNonNull(maxAge));
    }

    @Override
    public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        checkOpen();
        return table.readVersions(requireNonNull(hashes), requireNonNull(maxAge));
    }

    @Override
    public WriteResult write(Collection<Write> writes) {
        checkOpen();
        if (requireNonNull(writes).stream().anyMatch(write -> !write.isAdvance())) {
            table.database().beforeConditionalWrite();
        }
        // a write of the control row is the core's invalidateAll: its own fault window (setBeforeInvalidateAllApplies)
        if (writes.stream().anyMatch(write -> Store.CONTROL_HASH.equals(write.hash()))) {
            return table.database().invalidating(() -> table.write(writes));
        }
        return table.database().write(() -> table.write(writes));
    }

    @Override
    public int expire(Duration maxAge, long version) {
        checkOpen();
        return table.database().write(() -> table.expire(requireNonNull(maxAge), version));
    }

    @Override
    public int pruneToSize(long maxSize, Duration minAge) {
        checkOpen();
        return table.pruneToSize(maxSize, requireNonNull(minAge));
    }

    @Override
    public int pruneTombstones(Duration retention) {
        checkOpen();
        return table.pruneTombstones(requireNonNull(retention));
    }

    @Override
    public List<Change> changedWithin(Duration within) {
        checkOpen();
        return table.changedWithin(requireNonNull(within));
    }

    @Override
    public ChangeSubscription subscribe(ChangeListener listener) {
        checkOpen();
        ChangeSubscription subscription = table.subscribe(requireNonNull(listener));
        subscriptions.add(subscription);
        return subscription;
    }

    // the profile's clock: its unit and how far it may step back (nothing for a monotonic one)
    @Override
    public Duration clockAllowance() {
        return table.database().profile().clockAllowance();
    }

    @Override
    public void close() {
        closed = true;
        subscriptions.forEach(ChangeSubscription::close);
        subscriptions.clear();
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("store is closed");
        }
    }
}
