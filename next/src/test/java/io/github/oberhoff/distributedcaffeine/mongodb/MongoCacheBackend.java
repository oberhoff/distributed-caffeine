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
package io.github.oberhoff.distributedcaffeine.mongodb;

import io.github.oberhoff.distributedcaffeine.CacheBackend;
import io.github.oberhoff.distributedcaffeine.spi.Format;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;

/**
 * The cache suites on MongoDB (see {@link MongoTestDatabase}): a fresh collection per backend, one shared change
 * stream. A gap is simulated (the watcher reopens without its resume token, as after errors 280 and 286).
 */
public final class MongoCacheBackend implements CacheBackend {

    private static final String DATABASE = "suite";

    private final MongoTestDatabase database = MongoTestDatabase.get();
    private final MongoLocation location = new MongoLocation(DATABASE, MongoTestDatabase.freshCollection(),
            MongoAdapter.DEFAULT_DISCRIMINATOR, Format.TEXT, Format.TEXT);

    @Override
    public Store open(StoreSettings settings) {
        return MongoAdapter.newBuilder(database.client, DATABASE, location.collectionName).build()
                .createStore(settings);
    }

    @Override
    public boolean isIdle() {
        return MongoWatcher.barrier(database.client, location);
    }

    @Override
    public void induceGap() {
        await(this::isIdle); // the watcher is up, so that there is one to lose its history
        MongoWatcher.simulateHistoryLost(database.client, location);
    }
}
