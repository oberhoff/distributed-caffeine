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

import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreContractTest;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import org.jspecify.annotations.NullMarked;

/**
 * The storage contract on MongoDB, on the image and topology of the enclosing {@link MongoIntegrationTest} class:
 * every test on a fresh collection, several stores as several instances, one shared change stream. The gap is
 * simulated (the watcher reopens without its resume token, as after errors 280 and 286, which a test cannot provoke
 * on demand).
 */
@NullMarked
abstract class MongoStoreContractTest extends StoreContractTest {

    @Override
    protected Fixture createFixture() {
        return fixture();
    }

    // shared with the change feed contract
    static Fixture fixture() {
        MongoTestDatabase database = MongoTestDatabase.get();
        String collection = MongoTestDatabase.freshCollection();
        MongoAdapter<String, String> adapter = MongoAdapter.newBuilder(database.client, "contract", collection).build();
        // another cache in the same collection
        MongoAdapter<String, String> other = MongoAdapter.newBuilder(database.client, "contract", collection)
                .withDiscriminator("other").build();
        return new Fixture() {
            @Override
            public Store openStore() {
                return adapter.createStore(StoreSettings.of(Format.BINARY, Format.BINARY));
            }

            @Override
            public boolean induceGap() {
                MongoWatcher.simulateHistoryLost(database.client, new MongoLocation("contract", collection,
                        MongoAdapter.DEFAULT_DISCRIMINATOR, Format.BINARY, Format.BINARY));
                return true;
            }

            @Override
            public Store openStoreOfAnotherCache() {
                return other.createStore(StoreSettings.of(Format.BINARY, Format.BINARY));
            }

            @Override
            public boolean induceLoss() {
                MongoWatcher.simulateInterruption(database.client, new MongoLocation("contract", collection,
                        MongoAdapter.DEFAULT_DISCRIMINATOR, Format.BINARY, Format.BINARY));
                return true;
            }
        };
    }
}
