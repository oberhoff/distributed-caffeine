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
package io.github.oberhoff.distributedcaffeine.redis;

import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreContractTest;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.jspecify.annotations.NullMarked;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * The storage contract on Redis or Valkey, on the server of the enclosing image: every test on a cache of its own,
 * several stores as several instances, through Lettuce.
 */
@NullMarked
abstract class RedisStoreContractTest extends StoreContractTest {

    // the tests' own commands on the server or cluster of the enclosing image
    protected abstract RedisClusterCommands<String, String> admin();

    // the binding under test: a driver of the server's client of its library
    protected abstract RedisDriver driver();

    @Override
    protected Fixture createFixture() {
        return fixture(admin(), driver());
    }

    // shared with the change feed contract. A gap is induced as entries trimmed or lost with a failover look: a
    // number skipped (the counter moved past one, then a write); a loss by closing the reader's connection under it
    // (killing it on the server is no loss with Lettuce, which reconnects and replays the read, missing nothing)
    static Fixture fixture(RedisClusterCommands<String, String> admin, RedisDriver driver) {
        String name = RedisTestServer.freshName();
        RedisAdapter<String, String> adapter = RedisAdapter.newBuilder(driver, name).build();
        // another cache in the same keyspace
        RedisAdapter<String, String> other = RedisAdapter.newBuilder(driver, name).withDiscriminator("other").build();
        return new Fixture() {
            @Override
            public Store openStore() {
                return adapter.createStore(StoreSettings.of(Format.BINARY, Format.BINARY));
            }

            // every write assigned in the core's domain, batches included (StoreProfile.REDIS)
            @Override
            public boolean assigns(int batchSize) {
                return true;
            }

            @Override
            public Store openStoreOfAnotherCache() {
                return other.createStore(StoreSettings.of(Format.BINARY, Format.BINARY));
            }

            @Override
            public boolean induceGap() {
                admin.incr(new RedisKeys(name, RedisAdapter.DEFAULT_DISCRIMINATOR).sequence());
                try (Store store = openStore()) {
                    byte[] bytes = "gap".getBytes(StandardCharsets.UTF_8);
                    store.write(List.of(Write.value("gap", bytes, bytes, 1)));
                }
                return true;
            }

            @Override
            public boolean induceLoss() {
                RedisFeed.simulateLoss(driver);
                return true;
            }
        };
    }
}
