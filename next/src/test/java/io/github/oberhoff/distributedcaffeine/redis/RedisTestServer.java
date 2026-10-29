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

import com.github.dockerjava.api.model.Ulimit;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.jspecify.annotations.NullMarked;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * A Redis or Valkey server in a container, with a Lettuce, a Jedis and a Redisson client of the tests and an admin
 * connection beside the library's; one per image, started by its test class before its suites and stopped after them.
 */
@NullMarked
final class RedisTestServer implements AutoCloseable {

    private static final AtomicInteger NAMES = new AtomicInteger();
    // this run's own, so that a server kept between runs (a managed service, RedisManagedServiceTest) holds no cache of
    // the same name from an earlier one
    static final String RUN = Long.toString(System.currentTimeMillis(), 36);

    final String image;
    final RedisClient client;
    final StatefulRedisConnection<String, String> admin;
    // a Jedis and a Redisson client of the tests, beside Lettuce's
    final redis.clients.jedis.RedisClient jedis;
    final RedissonClient redisson;
    private final GenericContainer<?> container;

    RedisTestServer(String image) {
        this(image, new String[0]);
    }

    // arguments: Dragonfly's flags (--lock_on_hashtags), on its defaults where none
    @SuppressWarnings("resource") // stopped with the server
    RedisTestServer(String image, String... arguments) {
        this.image = image;
        this.container = image.contains("kvrocks")
                // Kvrocks, on the port the tests expect (its own default is 6666)
                ? new GenericContainer<>(image)
                        .withExposedPorts(6379)
                        .withCommand("--port", "6379")
                        .waitingFor(Wait.forLogMessage("(?is).*ready to accept connections.*", 1))
                : image.contains("dragonfly")
                // Dragonfly, on its defaults (which refuse a script touching a key it does not declare), with the
                // locked memory its docs ask for
                ? new GenericContainer<>(image)
                        .withExposedPorts(6379)
                        .withCreateContainerCmdModifier(cmd -> requireNonNull(cmd.getHostConfig())
                                .withUlimits(List.of(new Ulimit("memlock", -1L, -1L))))
                        .waitingFor(Wait.forListeningPort())
                : new GenericContainer<>(image)
                        .withExposedPorts(6379)
                        .waitingFor(Wait.forLogMessage("(?is).*ready to accept connections.*", 1));
        if (arguments.length > 0 && dragonfly()) {
            container.withCommand(arguments);
        }
        container.start();
        this.client = RedisClient.create("redis://" + host() + ":" + port());
        this.admin = client.connect();
        this.jedis = redis.clients.jedis.RedisClient.create(host(), port());
        this.redisson = redisson(host(), port());
    }

    // a Redisson client of a single server, on Redisson's defaults
    static RedissonClient redisson(String host, int port) {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://" + host + ":" + port);
        return Redisson.create(config);
    }

    // whether the server is Dragonfly, which has no maxmemory-policy to set (its eviction is a flag at start,
    // --cache_mode)
    // whether maxmemory-policy can be set (CONFIG SET): not on Dragonfly (its eviction is --cache_mode) nor on Kvrocks
    // (on disk, nothing to evict)
    boolean evictionConfigurable() {
        return !dragonfly() && !image.contains("kvrocks");
    }

    boolean dragonfly() {
        return image.contains("dragonfly");
    }

    String host() {
        return container.getHost();
    }

    int port() {
        return container.getMappedPort(6379);
    }

    // a cache name no other test uses
    static String freshName() {
        return "cache-" + RUN + "-" + NAMES.incrementAndGet();
    }

    @Override
    public void close() {
        redisson.shutdown();
        jedis.close();
        admin.close();
        client.shutdown();
        container.stop();
    }
}
