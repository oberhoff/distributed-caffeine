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

import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import static io.github.oberhoff.distributedcaffeine.testing.Topology.Tests.RUNS_ON_GITHUB;

/**
 * Every Redis-compatible image the suites run on, one at a time: from Redis 6.2, the oldest supported, and Valkey
 * 7.2, its first, to the latest, each major version's first release and its latest. A first release runs the suites
 * where versions differ ({@link RedisCompatibilityTests}: the bindings and both contracts over each, the store's own
 * tests), the latest every suite ({@link RedisServerTests}), behind Sentinel the failover ({@link RedisSentinelTests}),
 * which also runs on Redis 6.2.0, and in a cluster the contracts, the cache suites, the failover and a slot moving
 * ({@link RedisClusterTests}). Valkey 7.2.4, its first release, was never published as an image: 7.2.5 stands for it.
 * Dragonfly (1.4.0, its first release with every stream command the store needs, and the latest 1.x; 2.0.0 and the
 * latest 2.x; pinned to their releases, as its registry lists no tags without a token) on a single server, on its
 * defaults (which refuse a script touching a key it does not declare), the latest also with {@code --lock_on_hashtags}
 * (which {@code RedisAdapter} advises), and as a cluster, of three nodes or emulated by one (the latest of each
 * major); not behind Sentinel. Kvrocks (on disk; 2.16.0, its first release taking an
 * approximate MINID, and the latest 2.x, published as images from 2.7.0) on a single server, and the latest behind
 * Sentinel (Redis's) and as a cluster of three primaries with a replica each (its layout pushed by the tests, as its
 * controller does). Refused at store creation ({@link RedisRefusalTests}): Dragonfly 1.3.0 (no {@code XREAD}) and
 * Kvrocks 2.15.0 (no approximate MINID, though it reports Redis 7.0.0; earlier releases report 4.0.0). With
 * {@code -DrunsOnGitHub=true}, only the latest Redis runs on every topology (single server, Sentinel, cluster), and the
 * latest Valkey, Dragonfly and Kvrocks on a single server: Sentinel and the cluster are the same code paths on every
 * server, and their failover suites take most of the time.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class RedisIntegrationTest {


    @Nested
    @DisplayName("Redis 6.2.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Redis_6_2_0 extends RedisCompatibilityTests {

        Redis_6_2_0() {
            super("redis:6.2.0");
        }
    }

    @Nested
    @DisplayName("Redis 6.2.0 (Primary, Replica and Sentinel)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Redis_6_2_0__Sentinel extends RedisSentinelTests {

        Redis_6_2_0__Sentinel() {
            super("redis:6.2.0");
        }
    }

    @Nested
    @DisplayName("Redis 6.2 latest")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Redis_6_2_latest extends RedisServerTests {

        Redis_6_2_latest() {
            super("redis:6.2");
        }
    }

    @Nested
    @DisplayName("Redis 6.2 latest (Primary, Replica and Sentinel)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Redis_6_2_latest__Sentinel extends RedisSentinelTests {

        Redis_6_2_latest__Sentinel() {
            super("redis:6.2");
        }
    }

    @Nested
    @DisplayName("Redis 6.2 latest (Cluster)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Redis_6_2_latest__Cluster extends RedisClusterTests {

        Redis_6_2_latest__Cluster() {
            super("redis:6.2");
        }
    }

    @Nested
    @DisplayName("Redis 7.0.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Redis_7_0_0 extends RedisCompatibilityTests {

        Redis_7_0_0() {
            super("redis:7.0.0");
        }
    }

    @Nested
    @DisplayName("Redis 7 latest")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Redis_7_latest extends RedisServerTests {

        Redis_7_latest() {
            super("redis:7");
        }
    }

    @Nested
    @DisplayName("Redis 7 latest (Primary, Replica and Sentinel)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Redis_7_latest__Sentinel extends RedisSentinelTests {

        Redis_7_latest__Sentinel() {
            super("redis:7");
        }
    }

    @Nested
    @DisplayName("Redis 7 latest (Cluster)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Redis_7_latest__Cluster extends RedisClusterTests {

        Redis_7_latest__Cluster() {
            super("redis:7");
        }
    }

    @Nested
    @DisplayName("Redis 8.0.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Redis_8_0_0 extends RedisCompatibilityTests {

        Redis_8_0_0() {
            super("redis:8.0.0");
        }
    }

    @Nested
    @DisplayName("Redis 8 latest")
    class Redis_8_latest extends RedisServerTests {

        Redis_8_latest() {
            super("redis:8");
        }
    }

    @Nested
    @DisplayName("Redis 8 latest (Primary, Replica and Sentinel)")
    class Redis_8_latest__Sentinel extends RedisSentinelTests {

        Redis_8_latest__Sentinel() {
            super("redis:8");
        }
    }

    @Nested
    @DisplayName("Redis 8 latest (Cluster)")
    class Redis_8_latest__Cluster extends RedisClusterTests {

        Redis_8_latest__Cluster() {
            super("redis:8");
        }
    }

    @Nested
    @DisplayName("Valkey 7.2.5")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Valkey_7_2_5 extends RedisCompatibilityTests {

        Valkey_7_2_5() {
            super("valkey/valkey:7.2.5");
        }
    }

    @Nested
    @DisplayName("Valkey 7.2 latest")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Valkey_7_2_latest extends RedisServerTests {

        Valkey_7_2_latest() {
            super("valkey/valkey:7.2");
        }
    }

    @Nested
    @DisplayName("Valkey 7.2 latest (Primary, Replica and Sentinel)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Valkey_7_2_latest__Sentinel extends RedisSentinelTests {

        Valkey_7_2_latest__Sentinel() {
            super("valkey/valkey:7.2");
        }
    }

    @Nested
    @DisplayName("Valkey 7.2 latest (Cluster)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Valkey_7_2_latest__Cluster extends RedisClusterTests {

        Valkey_7_2_latest__Cluster() {
            super("valkey/valkey:7.2");
        }
    }

    @Nested
    @DisplayName("Valkey 8.0.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Valkey_8_0_0 extends RedisCompatibilityTests {

        Valkey_8_0_0() {
            super("valkey/valkey:8.0.0");
        }
    }

    @Nested
    @DisplayName("Valkey 8 latest")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Valkey_8_latest extends RedisServerTests {

        Valkey_8_latest() {
            super("valkey/valkey:8");
        }
    }

    @Nested
    @DisplayName("Valkey 8 latest (Primary, Replica and Sentinel)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Valkey_8_latest__Sentinel extends RedisSentinelTests {

        Valkey_8_latest__Sentinel() {
            super("valkey/valkey:8");
        }
    }

    @Nested
    @DisplayName("Valkey 8 latest (Cluster)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Valkey_8_latest__Cluster extends RedisClusterTests {

        Valkey_8_latest__Cluster() {
            super("valkey/valkey:8");
        }
    }

    @Nested
    @DisplayName("Valkey 9.0.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Valkey_9_0_0 extends RedisCompatibilityTests {

        Valkey_9_0_0() {
            super("valkey/valkey:9.0.0");
        }
    }

    @Nested
    @DisplayName("Valkey 9 latest")
    class Valkey_9_latest extends RedisServerTests {

        Valkey_9_latest() {
            super("valkey/valkey:9");
        }
    }

    @Nested
    @DisplayName("Valkey 9 latest (Primary, Replica and Sentinel)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Valkey_9_latest__Sentinel extends RedisSentinelTests {

        Valkey_9_latest__Sentinel() {
            super("valkey/valkey:9");
        }
    }

    @Nested
    @DisplayName("Valkey 9 latest (Cluster)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Valkey_9_latest__Cluster extends RedisClusterTests {

        Valkey_9_latest__Cluster() {
            super("valkey/valkey:9");
        }
    }

    @Nested
    @DisplayName("Kvrocks 2.15.0 (refused: no approximate MINID)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Kvrocks_2_15_0 extends RedisRefusalTests {

        Kvrocks_2_15_0() {
            super("apache/kvrocks:2.15.0");
        }
    }

    @Nested
    @DisplayName("Kvrocks 2.16.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Kvrocks_2_16_0 extends RedisCompatibilityTests {

        Kvrocks_2_16_0() {
            super("apache/kvrocks:2.16.0");
        }
    }

    @Nested
    @DisplayName("Kvrocks 2 latest (2.17.0)")
    class Kvrocks_2_latest extends RedisServerTests {

        Kvrocks_2_latest() {
            super("apache/kvrocks:2.17.0");
        }
    }

    @Nested
    @DisplayName("Kvrocks 2 latest (2.17.0, Primary, Replica and Sentinel)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Kvrocks_2_latest__Sentinel extends RedisSentinelTests {

        Kvrocks_2_latest__Sentinel() {
            super("apache/kvrocks:2.17.0");
        }
    }

    @Nested
    @DisplayName("Kvrocks 2 latest (2.17.0, Cluster)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Kvrocks_2_latest__Cluster extends RedisClusterTests {

        Kvrocks_2_latest__Cluster() {
            super("apache/kvrocks:2.17.0", RedisCluster.Kind.KVROCKS);
        }
    }

    @Nested
    @DisplayName("Dragonfly 1.3.0 (refused: no XREAD)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Dragonfly_1_3_0 extends RedisRefusalTests {

        Dragonfly_1_3_0() {
            super("docker.dragonflydb.io/dragonflydb/dragonfly:v1.3.0");
        }
    }

    @Nested
    @DisplayName("Dragonfly 1.4.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Dragonfly_1_4_0 extends RedisCompatibilityTests {

        Dragonfly_1_4_0() {
            super("docker.dragonflydb.io/dragonflydb/dragonfly:v1.4.0");
        }
    }

    @Nested
    @DisplayName("Dragonfly 1 latest (1.40.2)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Dragonfly_1_latest extends RedisServerTests {

        Dragonfly_1_latest() {
            super("docker.dragonflydb.io/dragonflydb/dragonfly:v1.40.2");
        }
    }

    @Nested
    @DisplayName("Dragonfly 1 latest (1.40.2, Cluster)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Dragonfly_1_latest__Cluster extends RedisClusterTests {

        Dragonfly_1_latest__Cluster() {
            super("docker.dragonflydb.io/dragonflydb/dragonfly:v1.40.2", RedisCluster.Kind.DRAGONFLY);
        }
    }

    @Nested
    @DisplayName("Dragonfly 1 latest (1.40.2, emulated Cluster)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Dragonfly_1_latest__EmulatedCluster extends RedisClusterTests {

        Dragonfly_1_latest__EmulatedCluster() {
            super("docker.dragonflydb.io/dragonflydb/dragonfly:v1.40.2", RedisCluster.Kind.DRAGONFLY_EMULATED);
        }
    }

    @Nested
    @DisplayName("Dragonfly 2.0.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Dragonfly_2_0_0 extends RedisCompatibilityTests {

        Dragonfly_2_0_0() {
            super("docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.0");
        }
    }

    @Nested
    @DisplayName("Dragonfly 2 latest (2.0.2)")
    class Dragonfly_2_latest extends RedisServerTests {

        Dragonfly_2_latest() {
            super("docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2");
        }
    }

    @Nested
    @DisplayName("Dragonfly 2 latest (2.0.2, --lock_on_hashtags)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Dragonfly_2_latest__LockOnHashtags extends RedisServerTests {

        Dragonfly_2_latest__LockOnHashtags() {
            super("docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2", "--lock_on_hashtags");
        }
    }

    @Nested
    @DisplayName("Dragonfly 2 latest (2.0.2, Cluster)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Dragonfly_2_latest__Cluster extends RedisClusterTests {

        Dragonfly_2_latest__Cluster() {
            super("docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2", RedisCluster.Kind.DRAGONFLY);
        }
    }

    @Nested
    @DisplayName("Dragonfly 2 latest (2.0.2, emulated Cluster)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Dragonfly_2_latest__EmulatedCluster extends RedisClusterTests {

        Dragonfly_2_latest__EmulatedCluster() {
            super("docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2", RedisCluster.Kind.DRAGONFLY_EMULATED);
        }
    }
}
