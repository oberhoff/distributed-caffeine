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
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreContractTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The storage contract on a store without a change feed: the storage contract asks nothing of announcements.
 */
class StorageOnlyStoreContractTest extends StoreContractTest {

    @Override
    protected Fixture createFixture() {
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
                return new StorageOnlyStore(database.openFeed("cache"));
            }

            @Override
            public Store openStoreOfAnotherCache() {
                return new StorageOnlyStore(database.openFeed("other"));
            }
        };
    }

    @Test
    void the_store_offers_no_change_feed() {
        assertThat(open()).isNotInstanceOf(ChangeFeed.class);
    }
}
