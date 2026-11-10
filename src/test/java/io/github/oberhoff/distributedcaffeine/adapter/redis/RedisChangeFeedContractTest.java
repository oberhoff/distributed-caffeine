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
package io.github.oberhoff.distributedcaffeine.adapter.redis;

import io.github.oberhoff.distributedcaffeine.spi.ChangeFeedContractTest;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.RecordingListener;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The change feed contract on Redis or Valkey, on the server of the enclosing image: the cache's stream read by the
 * shared reader; a gap induced as a skipped number, a loss by closing the reader's connection under it.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
abstract class RedisChangeFeedContractTest extends ChangeFeedContractTest {

    // the tests' own commands on the server or cluster of the enclosing image
    protected abstract RedisClusterCommands<String, String> admin();

    // the binding under test: a driver of the server's client of its library
    protected abstract RedisDriver driver();

    @Override
    protected Fixture createFixture() {
        return RedisStoreContractTest.fixture(admin(), driver());
    }

    // the reader shared per driver ends with its last subscription: no thread or connection left behind
    @Test
    void the_shared_reader_ends_with_its_last_subscription() {
        int before = RedisFeed.openReaders();
        Store store = open();
        ChangeSubscription first = subscribe(store, new RecordingListener());
        ChangeSubscription second = subscribe(store, new RecordingListener());
        assertThat(RedisFeed.openReaders()).as("one reader for both").isEqualTo(before + 1);

        first.close();
        assertThat(RedisFeed.openReaders()).as("still reading for the other").isEqualTo(before + 1);
        second.close();
        assertThat(RedisFeed.openReaders()).isEqualTo(before);
    }
}
