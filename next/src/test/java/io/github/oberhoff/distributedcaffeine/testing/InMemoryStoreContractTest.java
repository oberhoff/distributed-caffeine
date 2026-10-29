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

import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreContractTest;

/**
 * The storage contract on the reference store, which also tests the contract tests themselves.
 */
class InMemoryStoreContractTest extends StoreContractTest {

    @Override
    protected Fixture createFixture() {
        return fixture();
    }

    // shared with the change feed contract: the store with its feed and its scan, whatever the profile's change source
    static Fixture fixture() {
        InMemoryDatabase database = new InMemoryDatabase();
        return new Fixture() {
            @Override
            public boolean assigns(int batchSize) {
                return database.profile().assigns(batchSize);
            }

            @Override
            public boolean ownVersions() {
                return database.profile().versioning() == StoreProfile.Versioning.ASSIGNING_OWN;
            }

            @Override
            public Store openStore() {
                return database.openFeed("cache");
            }

            @Override
            public boolean induceGap() {
                database.gap();
                return true;
            }

            @Override
            public Store openStoreOfAnotherCache() {
                return database.openFeed("other");
            }

            @Override
            public boolean induceLoss() {
                database.interruptSubscriptions();
                database.resumeSubscriptions(true);
                return true;
            }
        };
    }
}
