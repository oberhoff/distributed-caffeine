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

import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.ExpectedEntry;
import io.github.oberhoff.distributedcaffeine.spi.Listener;
import io.github.oberhoff.distributedcaffeine.spi.ReadResult;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.Subscription;
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
public final class InMemoryStore implements Store, ChangeFeed {

    private final InMemoryTable table;
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
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
    public Map<String, Long> readVersions(Collection<String> hashes, Duration maxAge) {
        checkOpen();
        return table.readVersions(requireNonNull(hashes), requireNonNull(maxAge));
    }

    @Override
    public Map<String, Long> put(Collection<Entry> entries) {
        checkOpen();
        return table.put(requireNonNull(entries));
    }

    @Override
    public Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge) {
        checkOpen();
        return table.putIfUnchanged(requireNonNull(entries), requireNonNull(maxAge));
    }

    @Override
    public Map<String, Long> invalidate(Collection<String> hashes) {
        checkOpen();
        return table.invalidate(requireNonNull(hashes));
    }

    @Override
    public Map<String, Long> invalidateIfUnchanged(Map<String, Long> expected) {
        checkOpen();
        return table.invalidateIfUnchanged(requireNonNull(expected));
    }

    @Override
    public long invalidateAll() {
        checkOpen();
        return table.invalidateAll();
    }

    @Override
    public int expire(Duration maxAge) {
        checkOpen();
        return table.expire(requireNonNull(maxAge));
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
    public Subscription subscribe(Listener listener) {
        checkOpen();
        Subscription subscription = table.subscribe(requireNonNull(listener));
        subscriptions.add(subscription);
        return subscription;
    }

    @Override
    public void close() {
        closed = true;
        subscriptions.forEach(Subscription::close);
        subscriptions.clear();
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("store is closed");
        }
    }
}
