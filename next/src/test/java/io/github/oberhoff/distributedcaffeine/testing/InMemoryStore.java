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
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.Expected;
import io.github.oberhoff.distributedcaffeine.spi.ExpectedEntry;
import io.github.oberhoff.distributedcaffeine.spi.ReadResult;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
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
    public ReadResult read(Collection<String> hashes, Duration maxAge) {
        checkOpen();
        return table.read(requireNonNull(hashes), requireNonNull(maxAge));
    }

    @Override
    public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        checkOpen();
        return table.readVersions(requireNonNull(hashes), requireNonNull(maxAge));
    }

    @Override
    public Map<String, Long> put(Collection<Entry> entries) {
        checkOpen();
        return table.database().write(() -> table.put(requireNonNull(entries)));
    }

    @Override
    public boolean checksBasis() {
        return table.database().checksBasis;
    }

    @Override
    public Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge) {
        checkOpen();
        table.database().beforeConditionalWrite();
        return table.database().write(() -> table.putIfUnchanged(requireNonNull(entries), requireNonNull(maxAge)));
    }

    @Override
    public Map<String, Long> invalidate(Collection<String> hashes) {
        checkOpen();
        return table.database().write(() -> table.invalidate(requireNonNull(hashes)));
    }

    @Override
    public Map<String, Long> invalidateIfUnchanged(Collection<Expected> expectations) {
        checkOpen();
        return table.database().write(() -> table.invalidateIfUnchanged(requireNonNull(expectations)));
    }

    @Override
    public long invalidateAll() {
        checkOpen();
        return table.database().invalidating(table::invalidateAll);
    }

    @Override
    public int expire(Duration maxAge) {
        checkOpen();
        return table.database().write(() -> table.expire(requireNonNull(maxAge)));
    }

    @Override
    public int pruneToSize(long maxSize) {
        checkOpen();
        return table.pruneToSize(maxSize);
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
