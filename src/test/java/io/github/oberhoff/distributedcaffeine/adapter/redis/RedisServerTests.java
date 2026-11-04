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
import io.github.oberhoff.distributedcaffeine.CaffeineParityTest;
import io.github.oberhoff.distributedcaffeine.InternalCoreTest;
import io.github.oberhoff.distributedcaffeine.InternalDistributionTest;
import io.github.oberhoff.distributedcaffeine.InternalRecoveryTest;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Nested;

/**
 * Every suite on one Redis or Valkey image (the subclass names it): those of {@link RedisCompatibilityTests}, and the
 * cache suites (core, distribution, recovery, Caffeine parity, property).
 *
 * @author Andreas Oberhoff
 */
@NullMarked
abstract class RedisServerTests extends RedisCompatibilityTests {

    RedisServerTests(String image, String... arguments) {
        super(image, arguments);
    }

    @Nested
    class Core extends InternalCoreTest {

        @Override
        protected CacheBackend backend() {
            return new RedisCacheBackend(LettuceRedis.of(server.client));
        }
    }

    @Nested
    class Distribution extends InternalDistributionTest {

        @Override
        protected CacheBackend backend() {
            return new RedisCacheBackend(LettuceRedis.of(server.client));
        }
    }

    @Nested
    class Recovery extends InternalRecoveryTest {

        @Override
        protected CacheBackend backend() {
            return new RedisCacheBackend(LettuceRedis.of(server.client));
        }
    }

    @Nested
    class CaffeineParity extends CaffeineParityTest {

        @Override
        protected CacheBackend backend() {
            return new RedisCacheBackend(LettuceRedis.of(server.client));
        }
    }

    @Nested
    class Feed extends RedisFeedTests {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return server.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return LettuceRedis.of(server.client);
        }
    }

    @Nested
    class Acl extends RedisAclTests {

        @Override
        protected RedisTestServer server() {
            return server;
        }
    }

    @Nested
    class Restart extends RedisRestartTests {

        @Override
        protected String image() {
            return server.image;
        }
    }

    @Nested
    class Property extends RedisPropertyTest {

        @Override
        protected RedisTestServer server() {
            return server;
        }
    }
}
