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
package io.github.oberhoff.distributedcaffeine.postgresql;

import io.github.oberhoff.distributedcaffeine.CacheBackend;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;

/**
 * The cache suites on PostgreSQL (see {@link PostgresTestDatabase}): a fresh table per backend, the shared listening
 * session. A gap is a terminated listening session, as every reconnect is one.
 */
public final class PostgresCacheBackend implements CacheBackend {

    private final PostgresTestDatabase database = PostgresTestDatabase.get();
    private final String table = PostgresTestDatabase.freshTable();

    @Override
    public Store open(StoreSettings settings) {
        return PostgresAdapter.newBuilder(database.dataSource, "suite", table)
                .withListenerDataSource(database.listener).build().createStore(settings);
    }

    @Override
    public boolean isIdle() {
        return PostgresListener.barrier(database.listener, database.dataSource);
    }

    @Override
    public void induceGap() {
        await(this::isIdle); // the listening session is up, so that there is one to terminate
        database.terminateListeners();
    }
}
