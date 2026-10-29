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

import io.github.oberhoff.distributedcaffeine.CacheBackend;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * The cache suites on Redis or Valkey (see {@link RedisTestServer}): a cache of its own per backend, its instances
 * sharing one driver, so one reader. A gap is simulated (every subscription of the cache told of one, as a skipped
 * number tells it).
 */
@NullMarked
final class RedisCacheBackend implements CacheBackend {

    private final RedisDriver driver;
    private final RedisKeys keys = new RedisKeys(RedisTestServer.freshName(), RedisAdapter.DEFAULT_DISCRIMINATOR);
    private final RedisAdapter<String, String> adapter;
    // a store of the backend's own, for the stream's position in the barrier
    private @Nullable RedisStore probe;

    RedisCacheBackend(RedisDriver driver) {
        this.driver = driver;
        this.adapter = RedisAdapter.newBuilder(driver, keys.keyspace()).build();
    }

    @Override
    public Store open(StoreSettings settings) {
        return adapter.createStore(settings);
    }

    @Override
    public boolean isIdle() {
        return RedisFeed.barrier(driver, probe(), keys.stream());
    }

    @Override
    public void induceGap() {
        RedisFeed.simulateGap(driver, keys.stream());
    }

    private synchronized RedisStore probe() {
        RedisStore current = probe;
        if (current == null) {
            current = (RedisStore) open(StoreSettings.of(Format.TEXT, Format.TEXT));
            probe = current;
        }
        return current;
    }

    @Override
    public synchronized void close() {
        if (probe != null) {
            probe.close();
            probe = null;
        }
    }
}
