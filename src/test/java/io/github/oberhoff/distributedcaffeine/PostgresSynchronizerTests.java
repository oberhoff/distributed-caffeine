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
package io.github.oberhoff.distributedcaffeine;

import io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresAdapter;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Test PostgresSynchronizer")
final class PostgresSynchronizerTests {

    private static final String SCHEMA_NAME = "public";
    private static final Duration WAITING_DURATION = Duration.ofSeconds(5);

    private PostgreSQLContainer container;
    private DataSource dataSource;
    private final AtomicInteger testCounter = new AtomicInteger();
    private final Collection<DistributedCache<Key, Value>> caches = new ArrayList<>();
    private String tableName;

    @BeforeAll
    void beforeAll() {
        container = new PostgreSQLContainer("postgres:17");
        container.start();
        PGSimpleDataSource pgDataSource = new PGSimpleDataSource();
        pgDataSource.setUrl(container.getJdbcUrl());
        pgDataSource.setUser(container.getUsername());
        pgDataSource.setPassword(container.getPassword());
        dataSource = pgDataSource;
    }

    @AfterAll
    void afterAll() {
        container.stop();
    }

    @BeforeEach
    void beforeEach(TestInfo testInfo) {
        tableName = format("%s_%05d", testInfo.getTestMethod().orElseThrow().getName(), testCounter.incrementAndGet());
    }

    @AfterEach
    void afterEach() {
        // releases the connection each of them holds for listening
        caches.forEach(cache -> cache.distributedPolicy().stopSynchronization());
        caches.clear();
    }

    @DisplayName("that a population reaches another cache instance on the same table")
    @Test
    void test_PostgresSynchronizer_distributes_a_population() {
        DistributedCache<Key, Value> cacheA = cache(null);
        DistributedCache<Key, Value> cacheB = cache(null);

        Key key = Key.of(1);
        Value value = Value.of(1);

        cacheA.put(key, value);

        await("synchronization between cache instances")
                .atMost(WAITING_DURATION)
                .untilAsserted(() -> assertThat(cacheB.getIfPresent(key)).isEqualTo(value));
    }

    @DisplayName("that an invalidation reaches another cache instance")
    @Test
    void test_PostgresSynchronizer_distributes_an_invalidation() {
        DistributedCache<Key, Value> cacheA = cache(null);
        DistributedCache<Key, Value> cacheB = cache(null);

        Key key = Key.of(1);
        Value value = Value.of(1);

        cacheA.put(key, value);

        await("synchronization of the population")
                .atMost(WAITING_DURATION)
                .untilAsserted(() -> assertThat(cacheB.getIfPresent(key)).isEqualTo(value));

        cacheA.invalidate(key);

        await("synchronization of the invalidation")
                .atMost(WAITING_DURATION)
                .untilAsserted(() -> assertThat(cacheB.getIfPresent(key)).isNull());
    }

    @DisplayName("that a batch of cache entries arrives as a batch")
    @Test
    void test_PostgresSynchronizer_distributes_a_batch() {
        DistributedCache<Key, Value> cacheA = cache(null);
        DistributedCache<Key, Value> cacheB = cache(null);

        List<Key> keys = java.util.stream.IntStream.rangeClosed(1, 200)
                .mapToObj(Key::of)
                .toList();
        keys.forEach(key -> cacheA.put(key, Value.of(key.getId())));

        await("synchronization of every cache entry")
                .atMost(WAITING_DURATION)
                .untilAsserted(() -> assertThat(cacheB.estimatedSize()).isEqualTo(keys.size()));
        assertThat(cacheB.getIfPresent(Key.of(200))).isEqualTo(Value.of(200));
    }

    @DisplayName("that a status transition made in the store reaches another cache instance")
    @Test
    void test_PostgresSynchronizer_distributes_a_status_transition() throws Exception {
        DistributedCache<Key, Value> cacheA = cache(null);
        DistributedCache<Key, Value> cacheB = cache(null);

        Key key = Key.of(1);
        Value value = Value.of(1);

        cacheA.put(key, value);

        await("synchronization of the population")
                .atMost(WAITING_DURATION)
                .untilAsserted(() -> assertThat(cacheB.getIfPresent(key)).isEqualTo(value));

        // what maintenance does when it prunes: the status is transitioned in place rather than the record
        // deleted, and unlike a delete that has to reach every cache instance - which on a store that
        // distributes only what a writer announces means the transition has to announce itself
        cacheA.distributedPolicy().getAdapter().getRepository().orElseThrow()
                .updateStatusOfCacheEntries(null, Set.of(CACHED), null, INVALIDATED);

        await("synchronization of the status transition")
                .atMost(WAITING_DURATION)
                .untilAsserted(() -> assertThat(cacheB.getIfPresent(key)).isNull());
    }

    @DisplayName("that cache instances of different discriminators do not hear each other")
    @Test
    void test_PostgresSynchronizer_separates_discriminators() {
        DistributedCache<Key, Value> cacheA = cache("a");
        DistributedCache<Key, Value> cacheB = cache("b");
        DistributedCache<Key, Value> cacheA2 = cache("a");

        Key key = Key.of(1);

        cacheA.put(key, Value.of(1));

        await("synchronization within the shared discriminator")
                .atMost(WAITING_DURATION)
                .untilAsserted(() -> assertThat(cacheA2.getIfPresent(key)).isEqualTo(Value.of(1)));

        // the other scope had its chance on the same table while that one arrived
        assertThat(cacheB.getIfPresent(key)).isNull();
    }

    private DistributedCache<Key, Value> cache(String discriminator) {
        PostgresAdapter.Builder builder = PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, tableName);
        if (discriminator != null) {
            builder.withDiscriminator(discriminator);
        }
        DistributedCache<Key, Value> cache = DistributedCaffeine.<Key, Value>newBuilder(builder.build()).build();
        caches.add(cache);
        return cache;
    }
}
