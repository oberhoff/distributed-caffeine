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

import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntryMetadata;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresAdapter;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JavaObjectSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MICROS;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Test PostgresRepository")
final class PostgresRepositoryTests {

    private static final String SCHEMA_NAME = "public";

    private PostgreSQLContainer container;
    private DataSource dataSource;
    private final AtomicInteger testCounter = new AtomicInteger();
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
        // a table per test, as the MongoDB tests use a collection per test
        tableName = format("%s_%05d", testInfo.getTestMethod().orElseThrow().getName(), testCounter.incrementAndGet());
    }

    @DisplayName("that cache entries are published, read back, filtered and ordered")
    @Test
    void test_PostgresRepository_publishes_and_reads_cache_entries() throws Exception {
        Repository<Key, Value> repository = repositoryOf(null);

        // timestamps in the past and spaced apart, so that a status update - which refreshes the timestamp to the
        // real 'now' - reliably produces a newer one. Truncated to what the column keeps, so the round-trip compares
        // equal rather than nearly so
        Instant now = Instant.now().truncatedTo(MICROS);
        Instant timestamp1 = now.minusSeconds(30);
        Instant timestamp2 = now.minusSeconds(20);
        Instant timestamp3 = now.minusSeconds(10);

        CacheEntry<Key, Value> cachedEntry1 = CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, timestamp1);
        CacheEntry<Key, Value> cachedEntry2 = CacheEntry.of("h2", "op2", Key.of(2), Value.of(2), CACHED, timestamp2);
        CacheEntry<Key, Value> invalidatedEntry3 =
                CacheEntry.of("h3", "op3", Key.of(3), null, INVALIDATED, timestamp3);

        repository.publishCacheEntries(List.of(cachedEntry1, cachedEntry2, invalidatedEntry3));

        assertThat(repository.countCacheEntries(null)).isEqualTo(3);
        assertThat(repository.countCacheEntries(Set.of(CACHED))).isEqualTo(2);

        // unfiltered, then by hashes, by statuses, by both, and ordered
        try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(null, null, false)) {
            assertThat(stream.toList())
                    .containsExactlyInAnyOrder(cachedEntry1, cachedEntry2, invalidatedEntry3);
        }
        try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(Set.of("h1", "h2"), null, false)) {
            assertThat(stream.toList()).containsExactlyInAnyOrder(cachedEntry1, cachedEntry2);
        }
        try (Stream<CacheEntry<Key, Value>> stream =
                     repository.streamCacheEntries(Set.of("h1", "h2", "h3"), Set.of(INVALIDATED), false)) {
            assertThat(stream.toList()).containsExactly(invalidatedEntry3);
        }
        try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(null, null, true)) {
            assertThat(stream.toList()).containsExactly(cachedEntry1, cachedEntry2, invalidatedEntry3);
        }

        // an invalidated cache entry carries no value, which has to survive the round-trip as null rather than as
        // something that fails to deserialize
        try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(Set.of("h3"), null, false)) {
            assertThat(stream.toList())
                    .singleElement()
                    .satisfies(cacheEntry -> {
                        assertThat(cacheEntry.getKey()).isEqualTo(Key.of(3));
                        assertThat(cacheEntry.getValue()).isNull();
                    });
        }

        // publishing the same hash again replaces what was there, which is the uniqueness the primary key enforces
        repository.publishCacheEntries(List.of(
                CacheEntry.of("h1", "op9", Key.of(1), Value.of(9), CACHED, timestamp1)));
        assertThat(repository.countCacheEntries(null)).isEqualTo(3);
        try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(Set.of("h1"), null, false)) {
            assertThat(stream.toList())
                    .singleElement()
                    .satisfies(cacheEntry -> assertThat(cacheEntry.getValue()).isEqualTo(Value.of(9)));
        }
    }

    @DisplayName("that metadata is read without touching key and value")
    @Test
    void test_PostgresRepository_reads_metadata_only() throws Exception {
        Repository<Key, Value> repository = repositoryOf(null);

        Instant timestamp = Instant.now().truncatedTo(MICROS).minusSeconds(30);
        CacheEntry<Key, Value> cachedEntry = CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, timestamp);
        repository.publishCacheEntries(List.of(cachedEntry));

        try (Stream<CacheEntryMetadata> stream = repository.streamCacheEntryMetadata(Set.of("h1"), null, false)) {
            assertThat(stream.toList())
                    .singleElement()
                    // the metadata of a cache entry is unrelated to the cache entry it belongs to, so the two are
                    // never equal
                    .isNotEqualTo(cachedEntry)
                    .isEqualTo(CacheEntryMetadata.of("h1", "op1", CACHED, timestamp));
        }
    }

    @DisplayName("that a status update sets the status, clears the operation and refreshes the timestamp")
    @Test
    void test_PostgresRepository_updates_status() throws Exception {
        Repository<Key, Value> repository = repositoryOf(null);

        Instant now = Instant.now().truncatedTo(MICROS);
        Instant timestamp1 = now.minusSeconds(30);
        Instant timestamp3 = now.minusSeconds(10);
        repository.publishCacheEntries(List.of(
                CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, timestamp1),
                CacheEntry.of("h3", "op3", Key.of(3), Value.of(3), CACHED, timestamp3)));

        repository.updateStatusOfCacheEntries(Set.of("h1"), Set.of(CACHED), null, INVALIDATED);

        try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(Set.of("h1"), null, false)) {
            assertThat(stream.toList())
                    .singleElement()
                    .satisfies(cacheEntry -> {
                        assertThat(cacheEntry.getStatus()).isEqualTo(INVALIDATED);
                        assertThat(cacheEntry.getOperation()).isNull();
                        assertThat(cacheEntry.getKey()).isEqualTo(Key.of(1));
                        assertThat(cacheEntry.getValue()).isEqualTo(Value.of(1));
                        assertThat(cacheEntry.getTimestamp()).isAfter(timestamp3);
                    });
        }

        // filtered by olderThan, so the entry just refreshed is not caught by it while the older one is
        repository.updateStatusOfCacheEntries(null, null, now, EVICTED_SIZE);
        assertThat(repository.countCacheEntries(Set.of(EVICTED_SIZE))).isEqualTo(1);
        assertThat(repository.countCacheEntries(Set.of(INVALIDATED))).isEqualTo(1);
    }

    @DisplayName("that deleting is filtered by hashes, statuses and age")
    @Test
    void test_PostgresRepository_deletes_cache_entries() throws Exception {
        Repository<Key, Value> repository = repositoryOf(null);

        Instant now = Instant.now().truncatedTo(MICROS);
        repository.publishCacheEntries(List.of(
                CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, now.minusSeconds(30)),
                CacheEntry.of("h2", "op2", Key.of(2), Value.of(2), INVALIDATED, now.minusSeconds(20)),
                CacheEntry.of("h3", "op3", Key.of(3), Value.of(3), CACHED, now)));

        repository.deleteCacheEntries(Set.of("h1"), null, null);
        assertThat(repository.countCacheEntries(null)).isEqualTo(2);

        repository.deleteCacheEntries(null, Set.of(INVALIDATED), null);
        assertThat(repository.countCacheEntries(null)).isEqualTo(1);

        repository.deleteCacheEntries(null, null, now.minusSeconds(5));
        assertThat(repository.countCacheEntries(null)).isEqualTo(1);

        repository.deleteCacheEntries(null, null, null);
        assertThat(repository.countCacheEntries(null)).isEqualTo(0);
    }

    @DisplayName("that every repository addresses its own discriminator only")
    @Test
    void test_PostgresRepository_scopes_by_discriminator() throws Exception {
        Repository<Key, Value> repositoryA = repositoryOf("a");
        Repository<Key, Value> repositoryB = repositoryOf("b");

        Instant timestamp = Instant.now().truncatedTo(MICROS);
        // the same hash in both scopes, which the primary key allows precisely because it carries the discriminator
        repositoryA.publishCacheEntries(List.of(
                CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, timestamp)));
        repositoryB.publishCacheEntries(List.of(
                CacheEntry.of("h1", "op1", Key.of(1), Value.of(2), CACHED, timestamp)));

        assertThat(repositoryA.countCacheEntries(null)).isEqualTo(1);
        assertThat(repositoryB.countCacheEntries(null)).isEqualTo(1);
        try (Stream<CacheEntry<Key, Value>> stream = repositoryA.streamCacheEntries(null, null, false)) {
            assertThat(stream.toList())
                    .singleElement()
                    .satisfies(cacheEntry -> assertThat(cacheEntry.getValue()).isEqualTo(Value.of(1)));
        }

        // and deleting within one scope leaves the other untouched
        repositoryB.deleteCacheEntries(null, null, null);
        assertThat(repositoryB.countCacheEntries(null)).isEqualTo(0);
        assertThat(repositoryA.countCacheEntries(null)).isEqualTo(1);
    }

    // through the adapter rather than the repository directly, because wiring the serializers and the discriminator
    // is what an adapter does - and the repository is package-private anyway
    private Repository<Key, Value> repositoryOf(String discriminator) {
        PostgresAdapter.Builder builder = PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, tableName);
        if (discriminator != null) {
            builder.withDiscriminator(discriminator);
        }
        PostgresAdapter<Key, Value> adapter = builder.build();
        // a byte array serializer for the key and a string one for the value, so that both encodings the column
        // has to carry are exercised at once
        adapter.setKeySerializer(new JavaObjectSerializer<>());
        adapter.setValueSerializer(new JacksonSerializer<>(Value.class, false));
        return adapter.getRepository().orElseThrow();
    }
}
