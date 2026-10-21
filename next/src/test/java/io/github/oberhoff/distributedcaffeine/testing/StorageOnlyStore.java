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

import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.ExpectedEntry;
import io.github.oberhoff.distributedcaffeine.spi.ReadResult;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * A store forwarding its storage to another and offering no change feed, as an adapter for a plain key-value store
 * without notifications would: caches on it support only {@code DistributionMode.NONE}. Subclasses wrap single
 * methods.
 */
public class StorageOnlyStore implements Store {

    private final Store delegate;

    public StorageOnlyStore(Store delegate) {
        this.delegate = delegate;
    }

    @Override
    public ReadResult read(Collection<String> hashes, Duration maxAge) {
        return delegate.read(hashes, maxAge);
    }

    @Override
    public Map<String, Long> readVersions(Collection<String> hashes, Duration maxAge) {
        return delegate.readVersions(hashes, maxAge);
    }

    @Override
    public Map<String, Long> put(Collection<Entry> entries) {
        return delegate.put(entries);
    }

    @Override
    public Map<String, WriteResult> putIfUnchanged(
            Collection<ExpectedEntry> entries, Duration maxAge) {
        return delegate.putIfUnchanged(entries, maxAge);
    }

    @Override
    public Map<String, Long> invalidate(Collection<String> hashes) {
        return delegate.invalidate(hashes);
    }

    @Override
    public Map<String, Long> invalidateIfUnchanged(Map<String, Long> expected) {
        return delegate.invalidateIfUnchanged(expected);
    }

    @Override
    public long invalidateAll() {
        return delegate.invalidateAll();
    }

    @Override
    public int expire(Duration maxAge) {
        return delegate.expire(maxAge);
    }

    @Override
    public int pruneToSize(long maxSize) {
        return delegate.pruneToSize(maxSize);
    }

    @Override
    public int pruneTombstones(Duration retention) {
        return delegate.pruneTombstones(retention);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
