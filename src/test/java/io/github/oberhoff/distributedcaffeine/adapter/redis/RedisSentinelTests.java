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

import io.github.oberhoff.distributedcaffeine.AdapterOutageTest;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.testing.TestStoreSettings;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The suites on a primary, a replica and Sentinel of one Redis or Valkey image (the subclass names it): the topology
 * is started with the class and stopped after its suites.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class RedisSentinelTests {

    private final RedisSentinel sentinel;

    RedisSentinelTests(String image) {
        this.sentinel = new RedisSentinel(image);
    }

    @AfterAll
    void stopTopology() {
        sentinel.close();
    }

    // a plain client of the replica's address, as a misconfigured application would have: refused at once, where
    // every write would fail (READONLY) or, with replica-read-only off, apply on the replica alone
    @ParameterizedTest
    @ValueSource(strings = {"Lettuce", "Jedis", "Redisson"})
    void a_client_reaching_a_replica_is_refused_at_store_creation(String library) {
        int port = sentinel.replicaPort();
        io.lettuce.core.RedisClient lettuce = io.lettuce.core.RedisClient.create("redis://127.0.0.1:" + port);
        redis.clients.jedis.RedisClient jedis = redis.clients.jedis.RedisClient.create("127.0.0.1", port);
        org.redisson.api.RedissonClient redisson = RedisTestServer.redisson("127.0.0.1", port);
        try {
            RedisDriver driver = switch (library) {
                case "Lettuce" -> LettuceRedis.of(lettuce);
                case "Jedis" -> JedisRedis.of(jedis);
                default -> RedissonRedis.of(redisson);
            };
            RedisAdapter<String, String> adapter = RedisAdapter.newBuilder(driver, RedisTestServer.freshName())
                    .build();

            assertThatThrownBy(() -> {
                try (Store ignored = adapter.createStore(TestStoreSettings.of(Format.TEXT, Format.TEXT))) {
                    throw new AssertionError("the store was created");
                }
            }).isInstanceOfSatisfying(StoreException.class, e -> {
                assertThat(e).hasMessageContaining("is a replica");
                assertThat(e.isRetryable()).as("behind Sentinel, a failover passes").isTrue();
            });
        } finally {
            redisson.shutdown();
            jedis.close();
            lettuce.shutdown();
        }
    }

    @Nested
    class LettuceOutages extends AdapterOutageTest {

        @Override
        protected Outage outage() {
            return new RedisOutage(() -> sentinel.ownClient(RedisOutage.Library.LETTUCE), sentinel::containers,
                    key -> sentinel.withoutPrimary(), false);
        }
    }

    @Nested
    class JedisOutages extends AdapterOutageTest {

        @Override
        protected Outage outage() {
            return new RedisOutage(() -> sentinel.ownClient(RedisOutage.Library.JEDIS), sentinel::containers,
                    key -> sentinel.withoutPrimary(), true);
        }
    }

    @Nested
    class RedissonOutages extends AdapterOutageTest {

        @Override
        protected Outage outage() {
            return new RedisOutage(() -> sentinel.ownClient(RedisOutage.Library.REDISSON), sentinel::containers,
                    key -> sentinel.withoutPrimary(), false);
        }
    }

    @Nested
    class LettuceFailover extends RedisFailoverTests {

        @Override
        protected RedisTopology topology() {
            return sentinel;
        }

        @Override
        protected RedisDriver driver() {
            return LettuceRedis.of(sentinel.client);
        }
    }

    @Nested
    class JedisFailover extends RedisFailoverTests {

        @Override
        protected RedisTopology topology() {
            return sentinel;
        }

        @Override
        protected RedisDriver driver() {
            return JedisRedis.of(sentinel.jedis);
        }
    }

    @Nested
    class RedissonFailover extends RedisFailoverTests {

        @Override
        protected RedisTopology topology() {
            return sentinel;
        }

        @Override
        protected RedisDriver driver() {
            return RedissonRedis.of(sentinel.redisson);
        }
    }
}
