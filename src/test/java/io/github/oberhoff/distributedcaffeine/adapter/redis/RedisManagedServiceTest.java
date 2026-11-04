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
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static java.util.Objects.requireNonNull;

/**
 * The adapter against a server of the caller's, such as a managed service (ElastiCache, Azure Managed Redis,
 * Memorystore, Upstash): both contracts, the shared reader's tests and the core suite, through Lettuce. Skipped unless
 * a URI is given; every cache of the run is in keyspaces of this run's own ({@code cache-<run>-<n>}), deleted after
 * the suites. Run:
 * <pre>{@code
 * mvn surefire:test -Dtest='RedisManagedServiceTest*' -Dredis.external='rediss://user:password@host:6380'
 *     [-Dredis.external.cluster=true]
 * }</pre>
 * with {@code rediss://} for TLS, the user and password the service gives (an ACL user needs the permissions listed
 * in {@link RedisAdapter}, and {@code XTRIM}, {@code SCAN} and {@code DEL} for the tests' own commands), and
 * {@code redis.external.cluster} where the service is a cluster (Lettuce's cluster client then). A service that cannot
 * serve a cache fails at the first store created, with the reason.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfSystemProperty(named = "redis.external", matches = ".+")
class RedisManagedServiceTest {

    private final boolean clustered = Boolean.getBoolean("redis.external.cluster");
    private final RedisURI uri = RedisURI.create(System.getProperty("redis.external"));
    private final @Nullable RedisClient client = clustered ? null : RedisClient.create(uri);
    private final @Nullable RedisClusterClient clusterClient = clustered ? RedisClusterClient.create(uri) : null;
    private final StatefulConnection<String, String> connection = clusterClient != null ? clusterClient.connect()
            : requireNonNull(client).connect();

    private RedisClusterCommands<String, String> admin() {
        return connection instanceof StatefulRedisClusterConnection<String, String> cluster ? cluster.sync()
                : ((StatefulRedisConnection<String, String>) connection).sync();
    }

    // a new driver of the service, one per call
    private RedisDriver driver() {
        return clusterClient != null ? LettuceRedis.of(clusterClient) : LettuceRedis.of(requireNonNull(client));
    }

    // every key this run created, on every node (Lettuce's cluster scan visits them all)
    @AfterAll
    void deleteThisRunsKeys() {
        try {
            ScanArgs matching = ScanArgs.Builder.matches("{dc:cache-" + RedisTestServer.RUN + "-*").limit(1_000);
            ScanCursor cursor = ScanCursor.INITIAL;
            do {
                KeyScanCursor<String> scanned = admin().scan(cursor, matching);
                if (!scanned.getKeys().isEmpty()) {
                    scanned.getKeys().forEach(key -> admin().del(key));
                }
                cursor = scanned;
            } while (!cursor.isFinished());
        } finally {
            connection.close();
            if (client != null) {
                client.shutdown();
            }
            if (clusterClient != null) {
                clusterClient.shutdown();
            }
        }
    }

    @Nested
    class StoreContract extends RedisStoreContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return RedisManagedServiceTest.this.admin();
        }

        @Override
        protected RedisDriver driver() {
            return RedisManagedServiceTest.this.driver();
        }
    }

    @Nested
    class ChangeFeedContract extends RedisChangeFeedContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return RedisManagedServiceTest.this.admin();
        }

        @Override
        protected RedisDriver driver() {
            return RedisManagedServiceTest.this.driver();
        }
    }

    @Nested
    class Feed extends RedisFeedTests {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return RedisManagedServiceTest.this.admin();
        }

        @Override
        protected RedisDriver driver() {
            return RedisManagedServiceTest.this.driver();
        }
    }

    @Nested
    class Core extends InternalCoreTest {

        @Override
        protected CacheBackend backend() {
            return new RedisCacheBackend(driver());
        }
    }
}
