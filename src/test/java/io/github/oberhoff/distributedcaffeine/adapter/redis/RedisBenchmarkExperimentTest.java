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

import io.github.oberhoff.distributedcaffeine.EndToEndBenchmark;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.function.Function;

/**
 * The end-to-end benchmark (see {@link EndToEndBenchmark}) on Redis 8, through each client library, and on Valkey 9
 * and Dragonfly 2 (on its defaults and with {@code --lock_on_hashtags}) through Lettuce, each a single server in a
 * local container. Run with {@code -Dexperiments=true}.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@EnabledIfSystemProperty(named = "experiments", matches = "true")
@Timeout(1_800)
@SuppressWarnings("java:S2699") // an experiment reports what it measures, it asserts nothing
class RedisBenchmarkExperimentTest {

    private static void measure(String image, String label, Function<RedisTestServer, RedisDriver> driver,
                                String... arguments) throws Exception {
        try (RedisTestServer server = new RedisTestServer(image, arguments);
             RedisCacheBackend backend = new RedisCacheBackend(driver.apply(server))) {
            EndToEndBenchmark.run(label, backend);
            // what the server itself spent per command, against what the client waited: the difference is the
            // transport (Docker Desktop stalls requests above a few KB, VISION.md, "Not the library")
            System.out.println("server time per command (INFO commandstats):");
            server.admin.sync().info("commandstats").lines()
                    .filter(line -> line.startsWith("cmdstat_eval") || line.startsWith("cmdstat_xread"))
                    .forEach(line -> System.out.println("  " + line));
        }
    }

    @Test
    void measures_redis_through_lettuce() throws Exception {
        measure("redis:8", "Redis 8 through Lettuce (single server, local container)",
                server -> LettuceRedis.of(server.client));
    }

    @Test
    void measures_redis_through_jedis() throws Exception {
        measure("redis:8", "Redis 8 through Jedis (single server, local container)",
                server -> JedisRedis.of(server.jedis));
    }

    @Test
    void measures_redis_through_redisson() throws Exception {
        measure("redis:8", "Redis 8 through Redisson (single server, local container)",
                server -> RedissonRedis.of(server.redisson));
    }

    @Test
    void measures_valkey_through_lettuce() throws Exception {
        measure("valkey/valkey:9", "Valkey 9 through Lettuce (single server, local container)",
                server -> LettuceRedis.of(server.client));
    }

    @Test
    void measures_dragonfly_through_lettuce() throws Exception {
        measure("docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2",
                "Dragonfly 2.0.2 through Lettuce (single server, local container)",
                server -> LettuceRedis.of(server.client));
    }

    // as RedisAdapter advises: the cache's keys locked by their hashtag, not one by one across Dragonfly's threads
    @Test
    void measures_dragonfly_locking_on_hashtags_through_lettuce() throws Exception {
        measure("docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2",
                "Dragonfly 2.0.2 with --lock_on_hashtags through Lettuce (single server, local container)",
                server -> LettuceRedis.of(server.client), "--lock_on_hashtags");
    }
}
