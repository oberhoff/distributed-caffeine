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

import io.github.oberhoff.distributedcaffeine.AdapterConformanceTest;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

/**
 * The suites where versions differ, on one Redis or Valkey image (the subclass names it): each client library's
 * binding, the storage and change feed contracts over each, and the store's own tests (Lua, streams, error replies and
 * eviction policies vary by version). The server is started with the class and stopped after its suites.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class RedisCompatibilityTests {

    protected final RedisTestServer server;

    RedisCompatibilityTests(String image, String... arguments) {
        this.server = new RedisTestServer(image, arguments);
    }

    @AfterAll
    void stopServer() {
        server.close();
    }

    @Nested
    class LettuceBinding extends RedisBindingTests {

        @Override
        protected RedisTestServer server() {
            return server;
        }

        @Override
        protected RedisDriver driver() {
            return LettuceRedis.of(server.client);
        }

        @Override
        protected Proxied driverThrough(int port) {
            io.lettuce.core.RedisClient client = io.lettuce.core.RedisClient.create("redis://127.0.0.1:" + port);
            return new Proxied(LettuceRedis.of(client), client::shutdown);
        }

        @Override
        protected Proxied driverOfUnknownUser() {
            io.lettuce.core.RedisClient client = io.lettuce.core.RedisClient.create(io.lettuce.core.RedisURI.builder()
                    .withHost(server.host()).withPort(server.port()).withAuthentication(UNKNOWN_USER, WRONG_PASSWORD)
                    .build());
            return new Proxied(LettuceRedis.of(client), client::shutdown);
        }
    }

    @Nested
    class JedisBinding extends RedisBindingTests {

        @Override
        protected RedisTestServer server() {
            return server;
        }

        @Override
        protected RedisDriver driver() {
            return JedisRedis.of(server.jedis);
        }

        @Override
        protected Proxied driverThrough(int port) {
            redis.clients.jedis.RedisClient client = redis.clients.jedis.RedisClient.create("127.0.0.1", port);
            return new Proxied(JedisRedis.of(client), client);
        }

        @Override
        protected Proxied driverOfUnknownUser() {
            redis.clients.jedis.RedisClient client = redis.clients.jedis.RedisClient.create(server.host(),
                    server.port(), UNKNOWN_USER, WRONG_PASSWORD);
            return new Proxied(JedisRedis.of(client), client);
        }
    }

    @Nested
    class RedissonBinding extends RedisBindingTests {

        @Override
        protected RedisTestServer server() {
            return server;
        }

        @Override
        protected RedisDriver driver() {
            return RedissonRedis.of(server.redisson);
        }

        // pinging every second, so that it replaces a connection the network stopped carrying within a second (30 s
        // by default)
        @Override
        protected Proxied driverThrough(int port) {
            org.redisson.config.Config config = new org.redisson.config.Config();
            config.useSingleServer().setAddress("redis://127.0.0.1:" + port).setPingConnectionInterval(1_000);
            org.redisson.api.RedissonClient client = org.redisson.Redisson.create(config);
            return new Proxied(RedissonRedis.of(client), client::shutdown);
        }

        // connecting on first use: otherwise Redisson.create itself fails on credentials the server refuses, before
        // the cache sees them
        @Override
        protected Proxied driverOfUnknownUser() {
            org.redisson.config.Config config = new org.redisson.config.Config().setLazyInitialization(true)
                    .setUsername(UNKNOWN_USER).setPassword(WRONG_PASSWORD);
            config.useSingleServer().setAddress("redis://" + server.host() + ":" + server.port());
            org.redisson.api.RedissonClient client = org.redisson.Redisson.create(config);
            return new Proxied(RedissonRedis.of(client), client::shutdown);
        }

        // Redisson goes on retrying a server it cannot reach past the cache's timeout
        @Override
        protected RedisCommandException.Kind unreachable() {
            return RedisCommandException.Kind.TIMEOUT;
        }
    }

    @Nested
    class LettuceConformance extends AdapterConformanceTest {

        @Override
        protected AdapterConformance conformance() {
            return new RedisConformance(server, RedisOutage.Library.LETTUCE);
        }
    }

    @Nested
    class JedisConformance extends AdapterConformanceTest {

        @Override
        protected AdapterConformance conformance() {
            return new RedisConformance(server, RedisOutage.Library.JEDIS);
        }
    }

    @Nested
    class RedissonConformance extends AdapterConformanceTest {

        @Override
        protected AdapterConformance conformance() {
            return new RedisConformance(server, RedisOutage.Library.REDISSON);
        }
    }

    @Nested
    class Store extends RedisStoreTests {

        @Override
        protected RedisTestServer server() {
            return server;
        }
    }

    @Nested
    class StoreContract extends RedisStoreContractTest {

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
    class ChangeFeedContract extends RedisChangeFeedContractTest {

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
    class JedisStoreContract extends RedisStoreContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return server.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return JedisRedis.of(server.jedis);
        }
    }

    @Nested
    class JedisChangeFeedContract extends RedisChangeFeedContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return server.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return JedisRedis.of(server.jedis);
        }
    }

    @Nested
    class RedissonStoreContract extends RedisStoreContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return server.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return RedissonRedis.of(server.redisson);
        }
    }

    @Nested
    class RedissonChangeFeedContract extends RedisChangeFeedContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return server.admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return RedissonRedis.of(server.redisson);
        }
    }
}
