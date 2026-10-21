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

import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.RecordingListener;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreContractTest;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;

/**
 * The storage contract on PostgreSQL, on the image and topology of the enclosing {@link PostgresIntegrationTest}
 * class: every test on a fresh table, several stores as several instances. Without a listener, as a store offering
 * only storage; the {@link PostgresChangeFeedContractTest} runs with one.
 */
abstract class PostgresStoreContractTest extends StoreContractTest {

    @Override
    protected Fixture createFixture() {
        return fixture(false);
    }

    // shared with the change feed contract, which listens
    static Fixture fixture(boolean listening) {
        PostgresTestDatabase database = PostgresTestDatabase.get();
        String table = PostgresTestDatabase.freshTable();
        PostgresAdapter.Builder builder = PostgresAdapter.newBuilder(database.dataSource, "contract", table);
        // another cache in the same table
        PostgresAdapter.Builder otherBuilder = PostgresAdapter.newBuilder(database.dataSource, "contract", table)
                .withDiscriminator("other");
        if (listening) {
            builder.withListener(database.listener);
            otherBuilder.withListener(database.listener);
        }
        PostgresAdapter<String, String> adapter = builder.build();
        PostgresAdapter<String, String> other = otherBuilder.build();
        return new Fixture() {
            @Override
            public Store openStore() {
                return adapter.createStore(StoreSettings.of(Format.BINARY, Format.BINARY));
            }

            // another instance re-establishes the shared session, which seemed alive to it: the others delivering
            // on it may have missed messages (PostgreSQL keeps no history)
            @Override
            public boolean induceGap() {
                if (!listening) {
                    return false;
                }
                try (Store other = openStore()) {
                    ChangeSubscription subscription = ((ChangeFeed) other).subscribe(new RecordingListener());
                    subscription.resume();
                    subscription.close();
                }
                return true;
            }

            @Override
            public Store openStoreOfAnotherCache() {
                return other.createStore(StoreSettings.of(Format.BINARY, Format.BINARY));
            }

            @Override
            public boolean induceLoss() {
                database.terminateListeners();
                return listening;
            }
        };
    }
}
