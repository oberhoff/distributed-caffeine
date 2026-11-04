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
package io.github.oberhoff.distributedcaffeine.adapter.jdbc;

import io.github.oberhoff.distributedcaffeine.CacheBackend;
import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.PrimitiveStore;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import io.github.oberhoff.distributedcaffeine.testing.ScanOnlyStore;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Assumptions;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The cache suites on MariaDB or MySQL (see {@link JdbcTestDatabase}): a fresh table per backend, each store polled
 * by the core. Idle once every open store began two scans after the last write returned, the second one after
 * everything written before it, as the in-memory store's polled profile counts them. A gap is a poll failing once,
 * which the core reports as one when the next poll succeeds, as after a store's outage.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class JdbcCacheBackend implements CacheBackend {

    private final String table = JdbcTestDatabase.freshTable();
    private final List<Polled> stores = new CopyOnWriteArrayList<>();
    private final Map<Polled, Long> scansAtCommit = new ConcurrentHashMap<>();

    @Override
    public Store open(StoreSettings settings) {
        PrimitiveStore store = (PrimitiveStore) JdbcTestDatabase.adapter(JdbcTestDatabase.dataSource(), table,
                JdbcTestDatabase.DEFAULT_DISCRIMINATOR).createStore(settings);
        Polled polled = new Polled(store);
        stores.add(polled);
        return polled;
    }

    private void committed() {
        stores.forEach(store -> scansAtCommit.put(store, store.scans()));
    }

    @Override
    public boolean isIdle() {
        return stores.stream().allMatch(store -> store.scans() >= scansAtCommit.getOrDefault(store, 0L) + 2);
    }

    @Override
    public void induceGap() {
        stores.forEach(store -> store.failNextScan.set(true));
    }

    // five times the store's chunk of a read: each row is written in a transaction of its own, a few round trips
    // (25,000 took minutes, under the load of the matrix up to twenty)
    @Override
    public int largeBatch() {
        return 5_000;
    }

    // the messages are the core's polls, not a feed's
    @Override
    public void requireFeed() {
        Assumptions.abort("needs the store's change feed: MariaDB and MySQL are polled by the core");
    }

    private final class Polled extends ScanOnlyStore {

        private final AtomicBoolean failNextScan = new AtomicBoolean();

        Polled(PrimitiveStore store) {
            super(store);
        }

        @Override
        public WriteResult write(Collection<Write> writes) {
            try {
                return super.write(writes);
            } finally {
                committed();
            }
        }

        @Override
        public int expire(Duration maxAge, long version, int limit) {
            try {
                return super.expire(maxAge, version, limit);
            } finally {
                committed();
            }
        }

        @Override
        public int pruneToSize(long maxSize, Duration minAge, int limit) {
            try {
                return super.pruneToSize(maxSize, minAge, limit);
            } finally {
                committed();
            }
        }

        @Override
        public int pruneTombstones(Duration retention, int limit) {
            try {
                return super.pruneTombstones(retention, limit);
            } finally {
                committed();
            }
        }

        @Override
        public List<Change> changedWithin(Duration within) {
            if (failNextScan.compareAndSet(true, false)) {
                throw new StoreException("A poll failed, induced by the test", null, false, true);
            }
            return super.changedWithin(within);
        }

        // a closed store polls no more, and waits for nothing
        @Override
        public void close() {
            stores.remove(this);
            scansAtCommit.remove(this);
            super.close();
        }
    }
}
