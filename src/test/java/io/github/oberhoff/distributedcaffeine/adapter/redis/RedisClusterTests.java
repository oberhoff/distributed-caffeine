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

import io.github.oberhoff.distributedcaffeine.CacheBackend;
import io.github.oberhoff.distributedcaffeine.InternalCoreTest;
import io.github.oberhoff.distributedcaffeine.InternalDistributionTest;
import io.github.oberhoff.distributedcaffeine.InternalRecoveryTest;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

/**
 * The suites on a cluster of one Redis, Valkey or Dragonfly image (the subclass names it): both contracts and the
 * failover of a cache's primary through each client library (not on Dragonfly, whose control plane fails over), the
 * cache suites (core, distribution, recovery) through Lettuce; each cache in a keyspace of its own, so on a slot of its
 * own. The cluster is started with the class and stopped after its suites.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class RedisClusterTests {

    private final RedisCluster cluster;

    RedisClusterTests(String image) {
        this.cluster = new RedisCluster(image);
    }

    RedisClusterTests(String image, RedisCluster.Kind kind) {
        this.cluster = new RedisCluster(image, kind);
    }

    @AfterAll
    void stopCluster() {
        cluster.close();
    }

    @Nested
    class StoreContract extends RedisStoreContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return cluster.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return LettuceRedis.of(cluster.client);
        }
    }

    @Nested
    class ChangeFeedContract extends RedisChangeFeedContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return cluster.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return LettuceRedis.of(cluster.client);
        }
    }

    @Nested
    class Core extends InternalCoreTest {

        @Override
        protected CacheBackend backend() {
            return new RedisCacheBackend(LettuceRedis.of(cluster.client));
        }
    }

    @Nested
    class Distribution extends InternalDistributionTest {

        @Override
        protected CacheBackend backend() {
            return new RedisCacheBackend(LettuceRedis.of(cluster.client));
        }
    }

    @Nested
    class Recovery extends InternalRecoveryTest {

        @Override
        protected CacheBackend backend() {
            return new RedisCacheBackend(LettuceRedis.of(cluster.client));
        }
    }

    @Nested
    class JedisStoreContract extends RedisStoreContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return cluster.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return JedisRedis.of(cluster.jedis);
        }
    }

    @Nested
    class JedisChangeFeedContract extends RedisChangeFeedContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return cluster.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return JedisRedis.of(cluster.jedis);
        }
    }

    @Nested
    class RedissonStoreContract extends RedisStoreContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return cluster.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return RedissonRedis.of(cluster.redisson);
        }
    }

    @Nested
    class RedissonChangeFeedContract extends RedisChangeFeedContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return cluster.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return RedissonRedis.of(cluster.redisson);
        }
    }

    @Nested
    class Feed extends RedisFeedTests {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return cluster.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return LettuceRedis.of(cluster.client);
        }
    }

    @Nested
    class LettuceFailover extends RedisFailoverTests {

        @Override
        protected RedisTopology topology() {
            return cluster;
        }

        @Override
        protected RedisDriver driver() {
            return LettuceRedis.of(cluster.client);
        }
    }

    @Nested
    class JedisFailover extends RedisFailoverTests {

        @Override
        protected RedisTopology topology() {
            return cluster;
        }

        @Override
        protected RedisDriver driver() {
            return JedisRedis.of(cluster.jedis);
        }
    }

    @Nested
    class RedissonFailover extends RedisFailoverTests {

        @Override
        protected RedisTopology topology() {
            return cluster;
        }

        @Override
        protected RedisDriver driver() {
            return RedissonRedis.of(cluster.redisson);
        }
    }
}
