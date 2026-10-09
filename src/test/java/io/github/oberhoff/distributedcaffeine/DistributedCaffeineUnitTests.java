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

import com.dynatrace.hash4j.hashing.ByteAccess;
import com.dynatrace.hash4j.hashing.HashFunnel;
import com.dynatrace.hash4j.hashing.HashStream128;
import com.dynatrace.hash4j.hashing.Hashing;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.CachedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.Configurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.PersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntryMetadata;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.adapter.Repository.Order;
import io.github.oberhoff.distributedcaffeine.adapter.SerializerAware;
import io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresAdapter;
import io.github.oberhoff.distributedcaffeine.common.DistributedCaffeineCommonTestInstance;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;
import io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JavaObjectSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer.LoadingStrategy.CACHE_LOADER;
import static io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer.LoadingStrategy.MAPPING_FUNCTION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME;
import static io.github.oberhoff.distributedcaffeine.adapter.DiscriminatorAware.DEFAULT_DISCRIMINATOR;
import static io.github.oberhoff.distributedcaffeine.adapter.Repository.Order.UNORDERED;
import static java.time.temporal.ChronoUnit.FOREVER;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@DisplayName("Distributed Caffeine Unit Test Suite")
final class DistributedCaffeineUnitTests {

    @Nested
    @DisplayName("Test Caffeine")
    @SuppressWarnings("java:S5838")
    final class CaffeineUnit extends DistributedCaffeineUnitTestInstance {

        @DisplayName("that every key is either still held or was reported as removed, under the full operation mix")
        @Test
        // the reads below are driven for what they do to the cache, not for what they return, so discarding their
        // result is the point rather than an oversight
        @SuppressWarnings({"CheckReturnValue", "ResultOfMethodCallIgnored"})
        void test_Caffeine_accounts_for_every_key_under_the_full_operation_mix() throws Exception {
            // The control above only writes, and it accounts for every key. This one adds the paths the stress
            // test also drives - loading through a cache loader, refreshing after every write, computing through
            // getAll, and explicit invalidation - because a key was seen leaving a cache instance there with no
            // removal reported for it at all, and only a path that skips the notification can do that. This class
            // already records one such path in the test below, where a refresh returning the old value is not
            // reported, so refreshing is the one to cover
            int maximumSize = 500;
            int keys = 2_000;
            int rounds = 30;
            int operationsPerWorker = 2_000;
            int workers = 4;

            for (int round = 0; round < rounds; round++) {
                ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(8);
                ExecutorService workerExecutor = Executors.newFixedThreadPool(workers);
                Set<Integer> touched = ConcurrentHashMap.newKeySet();
                Set<Integer> removed = ConcurrentHashMap.newKeySet();
                try {
                    CacheLoader<Integer, String> cacheLoader = key -> {
                        touched.add(key);
                        return "loaded-" + key;
                    };
                    LoadingCache<Integer, String> cache = Caffeine.newBuilder()
                            .executor(executor)
                            .maximumSize(maximumSize)
                            .expireAfter(Expiry.creating((Integer key, String value) -> FOREVER.getDuration()))
                            .refreshAfterWrite(Duration.ofNanos(1))
                            .removalListener((Integer key, String value, RemovalCause cause) -> {
                                if (cause != RemovalCause.REPLACED) {
                                    removed.add(key);
                                }
                            })
                            .build(cacheLoader);

                    int currentRound = round;
                    List<? extends Future<?>> running = IntStream.range(0, workers)
                            .mapToObj(worker -> workerExecutor.submit(() -> {
                                Random random = new Random(currentRound * 31L + worker);
                                for (int operation = 0; operation < operationsPerWorker; operation++) {
                                    int id = random.nextInt(keys);
                                    switch (random.nextInt(5)) {
                                        case 0 -> {
                                            touched.add(id);
                                            cache.put(id, "put-" + id);
                                        }
                                        case 1 -> cache.get(id);
                                        // a set, so the two ids colliding is not a duplicate element
                                        case 2 -> cache.getAll(new HashSet<>(List.of(id, random.nextInt(keys))));
                                        case 3 -> cache.invalidate(id);
                                        default -> cache.asMap().remove(id);
                                    }
                                }
                            }))
                            .toList();
                    for (Future<?> future : running) {
                        future.get();
                    }

                    // everything the cache still has to do, so that no notification is merely late
                    cache.cleanUp();
                    await("pending cache work")
                            .atMost(Duration.ofSeconds(10))
                            .until(() -> executor.getActiveCount() == 0 && executor.getQueue().isEmpty());
                    cache.cleanUp();
                    sleep(Duration.ofMillis(200));

                    Set<Integer> held = cache.asMap().keySet();
                    Set<Integer> unaccountedFor = touched.stream()
                            .filter(id -> !held.contains(id) && !removed.contains(id))
                            .collect(toCollection(LinkedHashSet::new));
                    assertThat(unaccountedFor)
                            .describedAs("round %d: keys neither held nor reported as removed, out of %d touched",
                                    round, touched.size())
                            .isEmpty();
                } finally {
                    workerExecutor.shutdownNow();
                    executor.shutdownNow();
                }
            }
        }

        @DisplayName("that every key put is either still held or was reported as removed")
        @Test
        void test_Caffeine_accounts_for_every_key_under_size_pressure() throws Exception {
            // Distributed Caffeine relies on being told about every removal: an eviction is what it distributes,
            // so a key that leaves a cache unannounced is one the other cache instances go on serving. A stress
            // run turned up exactly that shape - a key gone from the cache with no removal reported for it - so
            // this checks the assumption directly, with no distribution involved at all.
            // The configuration is the one that run used: a maximum size with more keys than it holds, variable
            // expiry that never expires, and writes from several threads against a real executor
            int maximumSize = 1_000;
            int keys = 4_000;
            int writers = 4;
            ExecutorService executor = Executors.newFixedThreadPool(8);
            Set<Object> removed = ConcurrentHashMap.newKeySet();
            try {
                Cache<Integer, String> cache = Caffeine.newBuilder()
                        .executor(executor)
                        .maximumSize(maximumSize)
                        .expireAfter(Expiry.creating((Integer key, String value) -> FOREVER.getDuration()))
                        .removalListener((Integer key, String value, RemovalCause cause) -> {
                            // a replacement leaves the key in place, so it is not a removal of the key
                            if (cause != RemovalCause.REPLACED) {
                                removed.add(key);
                            }
                        })
                        .build();

                List<? extends Future<?>> written = IntStream.range(0, writers)
                        .mapToObj(writer -> executor.submit(() -> IntStream.range(0, keys)
                                .filter(id -> id % writers == writer)
                                .forEach(id -> cache.put(id, "value-" + id))))
                        .toList();
                for (Future<?> future : written) {
                    future.get();
                }
                cache.cleanUp();
                await("pending removal notifications")
                        .atMost(Duration.ofSeconds(10))
                        .until(() -> cache.estimatedSize() <= maximumSize);
                cache.cleanUp();
                // removal notifications are handed to the executor, so they arrive after the fact
                sleep(Duration.ofMillis(500));

                Set<Integer> held = cache.asMap().keySet();
                Set<Integer> unaccountedFor = IntStream.range(0, keys)
                        .boxed()
                        .filter(id -> !held.contains(id) && !removed.contains(id))
                        .collect(toCollection(LinkedHashSet::new));
                assertThat(unaccountedFor)
                        .describedAs("keys neither held nor reported as removed, out of %d put", keys)
                        .isEmpty();
            } finally {
                executor.shutdownNow();
            }
        }

        @DisplayName("that removal listener is not invoked if refresh returns old value")
        @Test
        void test_Caffeine_removal_listener_is_not_invoked_if_refresh_returns_old_value() {
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> removalListener = mock(RemovalListener.class);

            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                public Value load(Key key) {
                    return Value.of(key.getId());
                }

                @Override
                public @NonNull CompletableFuture<? extends Value> asyncLoad(@NonNull Key key, @NonNull Executor executor) {
                    return CompletableFuture.completedFuture(load(key));
                }

                @Override
                public @NonNull CompletableFuture<? extends Value> asyncReload(@NonNull Key key, @NonNull Value oldValue, @NonNull Executor executor) {
                    return CompletableFuture.completedFuture(oldValue);
                }
            });

            LoadingCache<Key, Value> loadingCache = Caffeine.newBuilder()
                    .removalListener(removalListener)
                    .build(cacheLoader);

            Key key1 = Key.of(1);
            Set<Key> keys2to3 = Set.of(Key.of(2), Key.of(3));

            loadingCache.refresh(key1);
            loadingCache.refreshAll(keys2to3);

            await("refresh (initial load)")
                    .failFast(loadingCache::cleanUp)
                    .untilAsserted(() -> {
                        assertThat(loadingCache.estimatedSize()).isEqualTo(3);
                        verifyNoInteractions(removalListener);
                        verify(cacheLoader, times(3))
                                .load(any(Key.class));
                        verify(cacheLoader, times(3))
                                .asyncLoad(any(Key.class), any(Executor.class));
                        verify(cacheLoader, never())
                                .asyncReload(any(Key.class), any(Value.class), any(Executor.class));
                    });

            loadingCache.refresh(key1);
            loadingCache.refreshAll(keys2to3);

            await("refresh (reload)")
                    .failFast(loadingCache::cleanUp)
                    .untilAsserted(() -> {
                        assertThat(loadingCache.estimatedSize()).isEqualTo(3);
                        verifyNoInteractions(removalListener);
                        verify(cacheLoader, times(3))
                                .load(any(Key.class));
                        verify(cacheLoader, times(3))
                                .asyncLoad(any(Key.class), any(Executor.class));
                        verify(cacheLoader, times(3))
                                .asyncReload(any(Key.class), any(Value.class), any(Executor.class));
                    });

            loadingCache.invalidateAll();

            await("invalidation")
                    .failFast(loadingCache::cleanUp)
                    .untilAsserted(() -> {
                        assertThat(loadingCache.estimatedSize()).isEqualTo(0);
                        verify(removalListener, times(3))
                                .onRemoval(any(Key.class), any(Value.class), any(RemovalCause.class));
                        verify(cacheLoader, times(3))
                                .load(any(Key.class));
                        verify(cacheLoader, times(3))
                                .asyncLoad(any(Key.class), any(Executor.class));
                        verify(cacheLoader, times(3))
                                .asyncReload(any(Key.class), any(Value.class), any(Executor.class));
                    });
        }

        @DisplayName("that cache can be build for arbitrary types using same builder")
        @Test
        @SuppressWarnings("unchecked")
        void test_Caffeine_cache_can_be_build_for_arbitrary_types_using_same_builder() throws Exception {
            AtomicInteger removalCount = new AtomicInteger(0);

            RemovalListener<String, String> stringRemovalListener = (key, value, removalCause) ->
                    removalCount.incrementAndGet();

            Caffeine<?, ?> caffeine = Caffeine.newBuilder()
                    .removalListener(stringRemovalListener);

            Cache<String, String> stringCache = (Cache<String, String>) caffeine.build();

            stringCache.put("key", "value");
            assertThat(stringCache.getIfPresent("key")).isEqualTo("value");
            stringCache.invalidateAll();
            stringCache.cleanUp();
            assertThat(stringCache.estimatedSize()).isEqualTo(0);

            RemovalListener<Integer, Integer> integerRemovalListener = (key, value, removalCause) ->
                    removalCount.incrementAndGet();

            Field field = Caffeine.class.getDeclaredField("removalListener");
            field.setAccessible(true);
            field.set(caffeine, integerRemovalListener);

            Cache<Integer, Integer> integerCache = (Cache<Integer, Integer>) caffeine.build();

            integerCache.put(0, 1);
            assertThat(integerCache.getIfPresent(0)).isEqualTo(1);
            integerCache.invalidateAll();
            integerCache.cleanUp();
            assertThat(integerCache.estimatedSize()).isEqualTo(0);

            await("removal")
                    .untilAsserted(() ->
                            assertThat(removalCount).hasValue(2));
        }
    }

    @Nested
    @DisplayName("Test Policy")
    final class PolicyUnit extends DistributedCaffeineUnitTestInstance {

        private static final Key RESIDENT = Key.of(1);
        private static final Key ABSENT = Key.of(99);

        // Every method below is a delegation that unwraps what it hands back, and the two ways it can be wrong
        // both compile: a key passed on without being wrapped finds nothing, and a map handed back without being
        // unwrapped carries the library's own key and value types into application code
        @DisplayName("that eviction, both fixed expirations and fixed refresh delegate and unwrap")
        @Test
        void test_Policy_delegations_pass_keys_through_and_hand_back_application_types() throws Exception {
            DistributedCache<Key, Value> cache = createCache(mockAdapter(),
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                            .maximumSize(10)
                            .expireAfterWrite(Duration.ofMinutes(5))
                            .expireAfterAccess(Duration.ofMinutes(7))
                            .refreshAfterWrite(Duration.ofMinutes(1))),
                    dc -> dc.build(key -> Value.of(key.getId())));
            cache.put(RESIDENT, Value.of(1));
            cache.put(Key.of(2), Value.of(2));

            Policy<Key, Value> policy = cache.policy();

            Policy.Eviction<Key, Value> eviction = policy.eviction().orElseThrow();
            assertThat(eviction.isWeighted()).isFalse();
            assertThat(eviction.weightOf(RESIDENT)).isEmpty();
            assertThat(eviction.weightedSize()).isEmpty();
            assertThat(eviction.getMaximum()).isEqualTo(10);
            eviction.setMaximum(20);
            assertThat(eviction.getMaximum()).as("the setter reaches the cache rather than being a no-op")
                    .isEqualTo(20);
            assertThat(eviction.coldest(10)).containsEntry(RESIDENT, Value.of(1));
            assertThat(eviction.hottest(10)).containsEntry(RESIDENT, Value.of(1));

            Policy.FixedExpiration<Key, Value> afterWrite = policy.expireAfterWrite().orElseThrow();
            assertThat(afterWrite.getExpiresAfter(TimeUnit.MINUTES)).isEqualTo(5);
            afterWrite.setExpiresAfter(9, TimeUnit.MINUTES);
            assertThat(afterWrite.getExpiresAfter(TimeUnit.MINUTES)).isEqualTo(9);
            // present for a key the cache holds and empty for one it does not, which is what a key handed on
            // unwrapped would get wrong in the same direction for both
            assertThat(afterWrite.ageOf(RESIDENT, TimeUnit.NANOSECONDS)).isPresent();
            assertThat(afterWrite.ageOf(ABSENT, TimeUnit.NANOSECONDS)).isEmpty();
            assertThat(afterWrite.oldest(10)).containsEntry(RESIDENT, Value.of(1));
            assertThat(afterWrite.youngest(10)).containsEntry(RESIDENT, Value.of(1));

            // the same factory serves both, so this proves the accessor reaches it at all
            assertThat(policy.expireAfterAccess().orElseThrow().getExpiresAfter(TimeUnit.MINUTES)).isEqualTo(7);

            Policy.FixedRefresh<Key, Value> refresh = policy.refreshAfterWrite().orElseThrow();
            assertThat(refresh.getRefreshesAfter(TimeUnit.MINUTES)).isEqualTo(1);
            refresh.setRefreshesAfter(3, TimeUnit.MINUTES);
            assertThat(refresh.getRefreshesAfter(TimeUnit.MINUTES)).isEqualTo(3);
            assertThat(refresh.ageOf(RESIDENT, TimeUnit.NANOSECONDS)).isPresent();
            assertThat(refresh.ageOf(ABSENT, TimeUnit.NANOSECONDS)).isEmpty();
        }

        @DisplayName("that a quietly fetched entry delegates what it is asked and refuses to be written through")
        @Test
        @SuppressWarnings("java:S5778")
        void test_Policy_cache_entry_delegates_and_refuses_to_be_set() throws Exception {
            DistributedCache<Key, Value> cache = createCache(mockAdapter(),
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                            .expireAfterWrite(Duration.ofMinutes(5))
                            .refreshAfterWrite(Duration.ofMinutes(1))),
                    dc -> dc.build(key -> Value.of(key.getId())));
            cache.put(RESIDENT, Value.of(1));

            Policy.CacheEntry<Key, Value> cacheEntry =
                    requireNonNull(cache.policy().getEntryIfPresentQuietly(RESIDENT));

            assertThat(cacheEntry.getKey()).isEqualTo(RESIDENT);
            assertThat(cacheEntry.getValue()).isEqualTo(Value.of(1));
            // the three moments the entry carries, all of them the underlying entry's rather than invented here
            assertThat(cacheEntry.expiresAt()).isGreaterThan(cacheEntry.snapshotAt());
            assertThat(cacheEntry.refreshableAt()).isGreaterThan(cacheEntry.snapshotAt());
            assertThat(cacheEntry.expiresAt()).isGreaterThan(cacheEntry.refreshableAt());

            // a view of what the cache holds, not a way into it - writing through it would bypass everything
            // that makes a write distributable
            assertThatThrownBy(() -> cacheEntry.setValue(Value.of(2)))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @DisplayName("that what the cache was not configured for is absent rather than empty-handed")
        @Test
        void test_Policy_absent_configurations_are_reported_as_absent() throws Exception {
            DistributedCache<Key, Value> cache = createCache(mockAdapter(),
                    CacheBuilder.identity(), DistributedCaffeine::build);

            Policy<Key, Value> policy = cache.policy();

            assertThat(policy.eviction()).isEmpty();
            assertThat(policy.expireAfterWrite()).isEmpty();
            assertThat(policy.expireAfterAccess()).isEmpty();
            assertThat(policy.refreshAfterWrite()).isEmpty();
            assertThat(policy.expireVariably()).isEmpty();
        }

        @SuppressWarnings("unchecked")
        private Adapter<Key, Value> mockAdapter() throws Exception {
            Adapter<Key, Value> adapter = mock(Adapter.class);
            Repository<Key, Value> repository = mock(Repository.class);
            when(adapter.getIdentifier()).thenReturn("policy.test");
            when(adapter.getPublisher()).thenReturn(repository);
            when(adapter.getRepository()).thenReturn(Optional.of(repository));
            when(repository.streamCacheEntries(any(), any(), any(Order.class)))
                    .thenAnswer(invocation -> Stream.empty());
            return adapter;
        }
    }

    @Nested
    @DisplayName("Test SerializerAware")
    final class SerializerAwareUnit extends DistributedCaffeineUnitTestInstance {

        @DisplayName("that nothing is serialized or deserialized for a null, either way")
        @Test
        void test_SerializerAware_passes_null_through() throws Exception {
            assertThat(SerializerAware.serialize(null, new JavaObjectSerializer<Key>())).isNull();
            // held as an Object rather than asserted on where it is produced, because what comes back is declared
            // nullable while the assertion it is passed to is not, which is the whole point of the call
            Object deserialized = SerializerAware.deserialize(null, new JavaObjectSerializer<Key>());
            assertThat(deserialized).isNull();
        }

        // The dispatch asks what KIND of serializer it was given, so a serializer implementing the base interface
        // alone matches none of the three and is reported rather than silently producing nothing
        @DisplayName("that a serializer of no known kind is reported instead of being guessed at")
        @Test
        @SuppressWarnings("java:S5778")
        void test_SerializerAware_rejects_a_serializer_of_no_known_kind() {
            Serializer<Key, String> ofNoKnownKind = new Serializer<>() {

                @Override
                public @NonNull String serialize(@NonNull Key object) {
                    return "serialized";
                }

                @Override
                public @NonNull Key deserialize(@NonNull String value) {
                    return Key.of(1);
                }
            };

            assertThatThrownBy(() -> SerializerAware.serialize(Key.of(1), ofNoKnownKind))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("No Serializer found for serializing object of type Key");
            assertThatThrownBy(() -> SerializerAware.deserialize("serialized", ofNoKnownKind))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("No Serializer found for deserializing value of type String");
        }

        // Deserializing asks about the VALUE as well, because what the store hands back has to match the kind of
        // serializer configured for it - a column or field holding the other representation is reported, not cast
        @DisplayName("that a value of the wrong representation for its serializer is reported")
        @Test
        @SuppressWarnings("java:S5778")
        void test_SerializerAware_rejects_a_value_the_serializer_cannot_take() {
            assertThatThrownBy(() -> SerializerAware.deserialize("a string, not bytes",
                    new JavaObjectSerializer<Key>()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("No Serializer found for deserializing value of type String");
            assertThatThrownBy(() -> SerializerAware.deserialize("not bytes".getBytes(StandardCharsets.UTF_8),
                    new JacksonSerializer<>(Value.class, false)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("No Serializer found for deserializing value of type byte[]");
        }
    }

    @Nested
    @DisplayName("Test Weigher")
    final class WeigherUnit extends DistributedCaffeineUnitTestInstance {

        // A weigher is not handed to Caffeine through its builder: the library reaches into the private field by
        // reflection and replaces the application's weigher with a wrapper, because Caffeine weighs InternalKey
        // and InternalValue rather than the application's own types. Take the replacement away and the
        // application's weigher is handed those wrappers and fails with a ClassCastException on the first write -
        // loud for whoever configured a weigher, and silent here, because nothing exercised the path at all
        @DisplayName("that a configured weigher is the one in effect and is asked with the application's own types")
        @Test
        void test_Weigher_is_injected_and_reports_through_the_policy() throws Exception {
            List<String> weighed = new CopyOnWriteArrayList<>();
            Weigher<Key, Value> weigher = (key, value) -> {
                weighed.add(key.getId() + "=" + value.getId());
                return key.getId();
            };

            DistributedCache<Key, Value> cache = createCache(mockAdapter(),
                    dc -> dc.withCaffeine(Caffeine.newBuilder().maximumWeight(100).weigher(weigher)),
                    DistributedCaffeine::build);
            cache.put(Key.of(10), Value.of(10));
            cache.put(Key.of(20), Value.of(20));

            assertThat(weighed).as("the configured weigher, not Caffeine's default of one per entry")
                    .contains("10=10", "20=20");

            // the weigher runs while the entry is written, but the weighted size below is only accumulated once
            // Caffeine drains its write buffer, which it schedules on the executor - so reading it straight after
            // the writes reports 0 until that has happened, which a loaded machine is slow enough to lose. The per
            // entry weights further down are read from the entry itself and never needed this
            cache.cleanUp();

            Policy.Eviction<Key, Value> eviction = cache.policy().eviction().orElseThrow();
            assertThat(eviction.isWeighted()).isTrue();
            // what the weigher returned for that key, which is what proves it is this weigher in effect
            assertThat(eviction.weightOf(Key.of(10))).hasValue(10);
            assertThat(eviction.weightedSize()).hasValue(30);
            // in weight units rather than entries, unlike the same method on a cache bounded by size
            assertThat(eviction.getMaximum()).isEqualTo(100);

            assertThat(requireNonNull(cache.policy().getEntryIfPresentQuietly(Key.of(10))).weight()).isEqualTo(10);
        }

        // Weight and count are different quantities, but they leave the cache by the same door: Caffeine reports
        // RemovalCause.SIZE for both, so the engine cannot tell them apart and does not need to. This pins that
        // an eviction the weigher caused is recorded exactly as one a maximum size would have caused - which is
        // also why a store-side bound stays a count of records: weight measures the live object on this heap and
        // says nothing about the serialized record
        @DisplayName("that an eviction caused by weight is recorded as a size eviction, like a count one")
        @Test
        void test_Weigher_eviction_is_reported_as_a_size_eviction() throws Exception {
            Adapter<Key, Value> adapter = mockAdapter();
            DistributedCache<Key, Value> cache = createCache(adapter,
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                            .maximumWeight(100)
                            .weigher((Weigher<Key, Value>) (key, value) -> key.getId())),
                    DistributedCaffeine::build);

            // 60 and 70 cannot both be held under a maximum weight of 100, while two entries never exceed a
            // maximum size of 100 - so only the weigher can be what evicts here
            cache.put(Key.of(60), Value.of(60));
            cache.put(Key.of(70), Value.of(70));
            cache.cleanUp();

            // reported asynchronously, so it is awaited rather than read straight afterwards
            ArgumentCaptor<Collection<CacheEntry<Key, Value>>> captor = ArgumentCaptor.captor();
            verify(adapter.getPublisher(), timeout(5_000).atLeastOnce()).publishCacheEntries(captor.capture());

            assertThat(captor.getAllValues().stream().flatMap(Collection::stream))
                    .anySatisfy(cacheEntry -> assertThat(cacheEntry.getStatus()).isEqualTo(EVICTED_SIZE));
        }

        @SuppressWarnings("unchecked")
        private Adapter<Key, Value> mockAdapter() throws Exception {
            Adapter<Key, Value> adapter = mock(Adapter.class);
            Repository<Key, Value> repository = mock(Repository.class);
            when(adapter.getIdentifier()).thenReturn("weigher.test");
            when(adapter.getPublisher()).thenReturn(repository);
            when(adapter.getRepository()).thenReturn(Optional.of(repository));
            when(repository.streamCacheEntries(any(), any(), any(Order.class)))
                    .thenAnswer(invocation -> Stream.empty());
            return adapter;
        }
    }

    @Nested
    @DisplayName("Test Scheduler")
    final class SchedulerUnit extends DistributedCaffeineUnitTestInstance {

        private static final Duration EXPIRY = Duration.ofMillis(200);
        // comfortably under the maintenance interval of one minute, so the library's own cleanUp cannot be what
        // drives the expiry below - only a scheduler can
        private static final Duration WAITING_DURATION = Duration.ofSeconds(10);

        @DisplayName("that a scheduler of the application's own is wrapped rather than replaced")
        @Test
        @SuppressWarnings("unchecked")
        void test_Scheduler_configured_by_the_application_is_the_one_used() throws Exception {
            AtomicInteger scheduled = new AtomicInteger();
            Scheduler application = (executor, command, delay, unit) -> {
                scheduled.incrementAndGet();
                return Scheduler.systemScheduler().schedule(executor, command, delay, unit);
            };

            DistributedCache<Key, Value> cache = createCache(mockAdapter(mock(Repository.class)),
                    dc -> dc.withCaffeine(Caffeine.newBuilder().expireAfterWrite(EXPIRY).scheduler(application)),
                    DistributedCaffeine::build);
            cache.put(Key.of(1), Value.of(1));

            // the library puts a wrapper of its own into the field, and this is what says the wrapper delegates
            // to the scheduler that was configured instead of standing in for it
            await("the application's scheduler being asked to schedule")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(scheduled).hasPositiveValue());
        }

        // Deliberate and undocumented: a scheduler is what makes the eviction listener reliable, so the library
        // overrides an application that switched scheduling off rather than honouring it. Nothing else pins that
        // decision, and were it to go the entry below would sit in the cache unnoticed until something touched it
        @DisplayName("that a scheduler the application disabled is overridden, so expiry is still reported")
        @Test
        void test_Scheduler_disabled_by_the_application_is_overridden() throws Exception {
            List<CacheEntry<Key, Value>> published = new CopyOnWriteArrayList<>();
            @SuppressWarnings("unchecked")
            Repository<Key, Value> repository = mock(Repository.class);
            Adapter<Key, Value> adapter = mockAdapter(repository);
            doAnswer(invocation -> {
                published.addAll(invocation.getArgument(0));
                return null;
            }).when(repository).publishCacheEntries(any());

            DistributedCache<Key, Value> cache = createCache(adapter,
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                            .expireAfterWrite(EXPIRY)
                            .scheduler(Scheduler.disabledScheduler())),
                    DistributedCaffeine::build);
            cache.put(Key.of(1), Value.of(1));

            // nothing reads the cache from here on, so no access can be what notices the expiry either
            await("the expiry being reported without the cache being touched")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(published)
                            .anySatisfy(cacheEntry -> assertThat(cacheEntry.getStatus()).isEqualTo(EVICTED_TIME)));
        }

        // the repository is handed in rather than made here, so that a test can stub it without Mockito seeing
        // a stubbed call (adapter.getPublisher()) inside a stubbing of its own
        private Adapter<Key, Value> mockAdapter(Repository<Key, Value> repository) throws Exception {
            @SuppressWarnings("unchecked")
            Adapter<Key, Value> adapter = mock(Adapter.class);
            when(adapter.getIdentifier()).thenReturn("scheduler.test");
            when(adapter.getPublisher()).thenReturn(repository);
            when(adapter.getRepository()).thenReturn(Optional.of(repository));
            when(repository.streamCacheEntries(any(), any(), any(Order.class)))
                    .thenAnswer(invocation -> Stream.empty());
            return adapter;
        }
    }

    @Nested
    @DisplayName("Test Expiry")
    final class ExpiryUnit extends DistributedCaffeineUnitTestInstance {

        // Three durations far enough apart to tell which callback produced the one in force
        private static final long AFTER_CREATE_SECONDS = 600;
        private static final long AFTER_UPDATE_SECONDS = 1_200;
        private static final long AFTER_READ_SECONDS = 1_800;

        // Every other test configures expiry with Expiry.creating(...), whose update and read callbacks hand back
        // the duration already in force. Against that, the wrapper's own expireAfterUpdate and expireAfterRead are
        // executed but unobservable: returning currentDuration instead of delegating, or swapping the two, gives
        // the same answer either way. An Expiry whose three callbacks differ is what makes them observable
        @DisplayName("that all three expiry callbacks are delegated with the application's own types")
        @Test
        void test_Expiry_delegates_create_update_and_read_separately() throws Exception {
            List<String> calls = new CopyOnWriteArrayList<>();
            Expiry<Key, Value> expiry = new Expiry<>() {

                @Override
                public long expireAfterCreate(Key key, Value value, long currentTime) {
                    calls.add("create:" + key.getId() + "=" + value.getId());
                    return SECONDS.toNanos(AFTER_CREATE_SECONDS);
                }

                @Override
                public long expireAfterUpdate(Key key, Value value, long currentTime, long currentDuration) {
                    calls.add("update:" + key.getId() + "=" + value.getId());
                    return SECONDS.toNanos(AFTER_UPDATE_SECONDS);
                }

                @Override
                public long expireAfterRead(Key key, Value value, long currentTime, long currentDuration) {
                    calls.add("read:" + key.getId() + "=" + value.getId());
                    return SECONDS.toNanos(AFTER_READ_SECONDS);
                }
            };

            DistributedCache<Key, Value> cache = createCache(mockAdapter(),
                    dc -> dc.withCaffeine(Caffeine.newBuilder().expireAfter(expiry)),
                    DistributedCaffeine::build);
            Key key = Key.of(1);

            cache.put(key, Value.of(1));
            assertThat(expiresAfter(cache, key)).isBetween(AFTER_CREATE_SECONDS - 5, AFTER_CREATE_SECONDS);

            cache.put(key, Value.of(2));
            assertThat(expiresAfter(cache, key)).isBetween(AFTER_UPDATE_SECONDS - 5, AFTER_UPDATE_SECONDS);

            assertThat(cache.getIfPresent(key)).isEqualTo(Value.of(2));
            assertThat(expiresAfter(cache, key)).isBetween(AFTER_READ_SECONDS - 5, AFTER_READ_SECONDS);

            // each callback saw the application's own key and value, and the value it saw on update is the new
            // one rather than the one it replaced
            assertThat(calls).containsExactly("create:1=1", "update:1=2", "read:1=2");
        }

        private long expiresAfter(DistributedCache<Key, Value> cache, Key key) {
            return cache.policy().expireVariably().orElseThrow().getExpiresAfter(key, SECONDS).orElseThrow();
        }

        @SuppressWarnings("unchecked")
        private Adapter<Key, Value> mockAdapter() throws Exception {
            Adapter<Key, Value> adapter = mock(Adapter.class);
            Repository<Key, Value> repository = mock(Repository.class);
            when(adapter.getIdentifier()).thenReturn("expiry.test");
            when(adapter.getPublisher()).thenReturn(repository);
            when(adapter.getRepository()).thenReturn(Optional.of(repository));
            when(repository.streamCacheEntries(any(), any(), any(Order.class)))
                    .thenAnswer(invocation -> Stream.empty());
            return adapter;
        }
    }

    @Nested
    @DisplayName("Test builder and configurers")
    final class BuilderUnit extends DistributedCaffeineUnitTestInstance {

        @DisplayName("that arguments and states are checked")
        @Test
        @SuppressWarnings({"unchecked", "java:S5778", "java:S5961"})
        void test_Builder_checks_on_arguments_and_states() {
            Adapter<Key, Value> adapter = mock(Adapter.class);
            Repository<Key, Value> repository = mock(Repository.class);

            when(adapter.getRepository()).thenReturn(Optional.of(repository));

            assertThatThrownBy(() ->
                    DistributedCaffeine.newBuilder(_null()))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("adapter cannot be null");

            Adapter<Key, Value> publishingAdapter = mock(Adapter.class);
            when(publishingAdapter.getIdentifier()).thenReturn("broker.topic");
            when(publishingAdapter.getRepository()).thenReturn(Optional.empty());
            Stream.<Configurer<PersistenceConfigurer>>of(
                            configurer -> configurer.withCachedEntries(
                                    CachedEntryPersistenceConfigurer::withCacheResidency),
                            configurer -> configurer.withCachedEntries(tier ->
                                    tier.withMaximumSize(1)),
                            configurer -> configurer.withEvictedEntries(tier ->
                                    tier.withMaximumSize(1)),
                            // a loading strategy reads the store on a miss, so it must not get past this rule
                            // either - it cannot, because a strategy without a retention is rejected in turn
                            configurer -> configurer.withEvictedEntries(tier ->
                                    tier.withMaximumSize(1).withLoadingStrategies(MAPPING_FUNCTION)))
                    .forEach(persistence -> assertThatThrownBy(() ->
                            createCache(publishingAdapter,
                                    dc -> dc.withPersistence(persistence),
                                    DistributedCaffeine::build))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("Persistence is not supported by this adapter"));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withCaffeine(_null()),
                            DistributedCaffeine::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("caffeine cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withHashProvider(_null()),
                            DistributedCaffeine::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("hashProvider cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withDistributionMode(_null()),
                            DistributedCaffeine::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("distributionMode cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withSerializers(_null()),
                            DistributedCaffeine::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("configurer cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withSerializers(configurer -> _null()),
                            DistributedCaffeine::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("configurer cannot return null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withSerializers(configurer -> configurer
                                    .withKeySerializer(_null())),
                            DistributedCaffeine::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("keySerializer cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withSerializers(configurer -> configurer
                                    .withValueSerializer(_null())),
                            DistributedCaffeine::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("valueSerializer cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withSerializers(configurer ->
                                    configurer.withKeySerializer(new Serializer<>() {
                                        @Override
                                        public @NonNull Object serialize(@NonNull Key object) {
                                            return _null();
                                        }

                                        @Override
                                        public @NonNull Key deserialize(@NonNull Object value) {
                                            return _null();
                                        }
                                    })),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Serializers must implement one of the following interfaces: "
                            .concat("ByteArraySerializer, StringSerializer, JsonSerializer"));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withSerializers(configurer ->
                                    configurer.withValueSerializer(new Serializer<>() {
                                        @Override
                                        public @NonNull Object serialize(@NonNull Value object) {
                                            return _null();
                                        }

                                        @Override
                                        public @NonNull Value deserialize(@NonNull Object value) {
                                            return _null();
                                        }
                                    })),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Serializers must implement one of the following interfaces: "
                            .concat("ByteArraySerializer, StringSerializer, JsonSerializer"));

            Stream.<Configurer<PersistenceConfigurer>>of(
                            _null(),
                            configurer -> configurer.withCachedEntries(_null()),
                            configurer -> configurer.withEvictedEntries(_null()))
                    .forEach(persistence -> assertThatThrownBy(() ->
                            createCache(adapter,
                                    dc -> dc.withPersistence(persistence),
                                    DistributedCaffeine::build))
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("configurer cannot be null"));

            // a configurer is expected to hand back the configurer it was given, so a null return breaks its
            // contract rather than the caller's - and it is caught here instead of deep inside the build
            Stream.<Configurer<PersistenceConfigurer>>of(
                            configurer -> _null(),
                            configurer -> configurer.withCachedEntries(tier -> _null()),
                            configurer -> configurer.withEvictedEntries(tier -> _null()))
                    .forEach(persistence -> assertThatThrownBy(() ->
                            createCache(adapter,
                                    dc -> dc.withPersistence(persistence),
                                    DistributedCaffeine::build))
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("configurer cannot return null"));

            // both persistence tiers check their arguments the same way, so neither is exercised on its own
            Stream.<Configurer<PersistenceConfigurer>>of(
                            configurer -> configurer.withCachedEntries(tier -> tier.withMaximumSize(0)),
                            configurer -> configurer.withEvictedEntries(tier -> tier.withMaximumSize(0)))
                    .forEach(persistence -> assertThatThrownBy(() ->
                            createCache(adapter,
                                    dc -> dc.withPersistence(persistence),
                                    DistributedCaffeine::build))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("maximumSize must be positive"));

            Stream.<Configurer<PersistenceConfigurer>>of(
                            configurer -> configurer.withCachedEntries(tier -> tier.withMaximumTime(_null())),
                            configurer -> configurer.withEvictedEntries(tier -> tier.withMaximumTime(_null())))
                    .forEach(persistence -> assertThatThrownBy(() ->
                            createCache(adapter,
                                    dc -> dc.withPersistence(persistence),
                                    DistributedCaffeine::build))
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("maximumTime cannot be null"));

            Stream.<Configurer<PersistenceConfigurer>>of(
                            configurer -> configurer.withCachedEntries(tier ->
                                    tier.withMaximumTime(Duration.ZERO)),
                            configurer -> configurer.withEvictedEntries(tier ->
                                    tier.withMaximumTime(Duration.ZERO)),
                            configurer -> configurer.withCachedEntries(tier ->
                                    tier.withMaximumTime(Duration.ofMillis(-1))),
                            configurer -> configurer.withEvictedEntries(tier ->
                                    tier.withMaximumTime(Duration.ofMillis(-1))))
                    .forEach(persistence -> assertThatThrownBy(() ->
                            createCache(adapter,
                                    dc -> dc.withPersistence(persistence),
                                    DistributedCaffeine::build))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("maximumTime must be positive"));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withLoadingStrategies(_null()))),
                            DistributedCaffeine::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("loadingStrategies cannot be null");

            // a null next to a valid strategy, because the elements are checked and not just the array
            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withLoadingStrategies(CACHE_LOADER, _null()))),
                            DistributedCaffeine::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("loadingStrategies cannot contain null");

            Stream.<CacheConstructor<Key, Value>>of(DistributedCaffeine::build, dc -> dc.build(key -> null))
                    .forEach(cacheConstructor -> assertThatThrownBy(() ->
                            createCache(adapter,
                                    dc -> dc.withPersistence(configurer -> configurer
                                            .withEvictedEntries(evictedEntries -> evictedEntries
                                                    .withMaximumSize(1))),
                                    cacheConstructor))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("If persistence of evicted entries is configured, "
                                    .concat("at least one eviction strategy must be set")));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withCaffeine(Caffeine.newBuilder()
                                            .maximumSize(1))
                                    .withPersistence(configurer -> configurer
                                            .withEvictedEntries(evictedEntries -> evictedEntries
                                                    .withMaximumSize(1)
                                                    .withLoadingStrategies(CACHE_LOADER))),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("If persistence of evicted entries is configured and loading strategy "
                            .concat("for cache loader is enabled, cache must be built as loading cache"));

            // only an explicit strategy is objected to - the default is not chosen against any distribution mode
            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withDistributionMode(INVALIDATION)
                                    .withPersistence(configurer -> configurer
                                            .withCachedEntries(CachedEntryPersistenceConfigurer
                                                    ::withCacheResidency)),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("If persistence of cached entries is configured, "
                            .concat("the distribution mode must include population"));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withPersistence(configurer -> configurer
                                    .withCachedEntries(cachedEntries -> cachedEntries
                                            .withCacheResidency()
                                            .withMaximumSize(1))),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("If persistence of cached entries is configured, cache residency must not be "
                            .concat("combined with a maximum size or a maximum amount of time"));

            // residency is only objected to where the cache can actually evict, which is what raises the question
            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withCaffeine(Caffeine.newBuilder()
                                            .maximumSize(1))
                                    .withDistributionMode(POPULATION_AND_INVALIDATION)
                                    .withPersistence(configurer -> configurer
                                            .withCachedEntries(CachedEntryPersistenceConfigurer
                                                    ::withCacheResidency)),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("If persistence of cached entries is configured with cache residency and an "
                            .concat("eviction policy is set, the distribution mode must include evictions"));

            // cache residency is neither bounded nor reclaimable without something reading it back, so the two
            // are asserted against each other rather than against a setting the user might merely have forgotten
            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withPersistence(configurer -> configurer
                                    .withCachedEntries(cachedEntries -> cachedEntries
                                            .withCacheResidency()
                                            .withColdStart())),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("If persistence of cached entries is configured with cache residency, "
                            .concat("a cold start must not be specified"));

            // a loading strategy on its own retains nothing, so it can never take effect
            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withLoadingStrategies(CACHE_LOADER))),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("If a loading strategy is enabled, persistence of evicted entries must be "
                            .concat("configured with a maximum size or a maximum amount of time"));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            CacheBuilder.identity(),
                            dc -> dc.build(_null())))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("cacheLoader cannot be null");

            // rejected for keys and for values on their own, not only for both of them together
            Stream.<UnaryOperator<Caffeine<Object, Object>>>of(Caffeine::weakKeys, Caffeine::weakValues,
                            Caffeine::softValues, c -> c.weakKeys().weakValues())
                    .forEach(referenceStrength -> assertThatThrownBy(() ->
                            createCache(adapter,
                                    dc -> dc.withCaffeine(referenceStrength.apply(Caffeine.newBuilder())),
                                    DistributedCaffeine::build))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("The use of weak or soft references is not supported"));
        }

        // an adapter that is complete enough to be built upon: synchronizing on activation streams from the
        // repository, so an adapter without one cannot get a cache instance off the ground
        @SuppressWarnings("unchecked")
        private Adapter<Key, Value> mockAdapter(String identifier) throws Exception {
            Adapter<Key, Value> adapter = mock(Adapter.class);
            Repository<Key, Value> repository = mock(Repository.class);
            when(adapter.getIdentifier()).thenReturn(identifier);
            when(adapter.getPublisher()).thenReturn(repository);
            when(adapter.getRepository()).thenReturn(Optional.of(repository));
            // answered rather than returned, so that every synchronization gets a stream of its own instead of
            // re-consuming one that an earlier one already closed
            when(repository.streamCacheEntries(any(), any(), any(Order.class)))
                    .thenAnswer(invocation -> Stream.empty());
            return adapter;
        }

        @DisplayName("that an adapter already in use is rejected")
        @Test
        @SuppressWarnings("java:S5778")
        void test_Builder_rejects_adapter_already_in_use() throws Exception {
            Adapter<Key, Value> adapter = mockAdapter("database.collection");

            createCache(adapter, CacheBuilder.identity(), DistributedCaffeine::build);

            // handing the same adapter to another cache instance used to rewire its change stream to that instance
            // and then hang, because activating an adapter that is already watching joins a watcher that only
            // completes once it stops. It has to fail fast instead
            Stream.<CacheConstructor<Key, Value>>of(DistributedCaffeine::build, dc -> dc.build(key -> null))
                    .forEach(cacheConstructor -> assertThatThrownBy(() ->
                            createCache(adapter, CacheBuilder.identity(), cacheConstructor))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("The adapter for cache at 'database.collection' is already in use by "
                                    .concat("another cache instance, every cache instance requires its own adapter")));

            // the rejected attempts must not have rewired the adapter of the cache instance holding it
            verify(adapter, times(1))
                    .setReceiver(any());

            // an own adapter for each cache instance is what the rejection asks for, so that has to work
            assertThat(createCache(mockAdapter("database.other"), CacheBuilder.identity(),
                    DistributedCaffeine::build))
                    .isNotNull();
        }

        @DisplayName("that an adapter is released again if constructing fails")
        @Test
        @SuppressWarnings("java:S5778")
        void test_Builder_releases_adapter_if_constructing_fails() throws Exception {
            Adapter<Key, Value> adapter = mockAdapter("database.collection");

            // failing while configuring, before the adapter is wired to anything
            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .weakKeys()
                                    .weakValues()),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("The use of weak or soft references is not supported");

            // failing while activating, as a store that is not reachable yet would
            doThrow(new MongoException("not reachable"))
                    .when(adapter).activate();
            assertThatThrownBy(() ->
                    createCache(adapter, CacheBuilder.identity(), DistributedCaffeine::build))
                    .isInstanceOf(MongoException.class)
                    .hasMessage("not reachable");

            // neither attempt produced a cache instance, so the very same adapter has to be accepted by a later one
            doNothing()
                    .when(adapter).activate();
            assertThat(createCache(adapter, CacheBuilder.identity(), DistributedCaffeine::build))
                    .isNotNull();
        }
    }

    @Nested
    @DisplayName("Test Hasher")
    final class HasherUnit extends DistributedCaffeineUnitTestInstance {

        // Each case names a put method and performs the SAME call twice: once on the Hasher, once on the stream
        // it is supposed to delegate to. The duplication is the test - a put wired to the wrong underlying method
        // still compiles, still returns a hash, and produces one no other cache instance computes for that key
        record Delegation(String name, UnaryOperator<Hasher> onHasher, Consumer<HashStream128> onStream) {

            @Override
            public @NonNull String toString() {
                return name;
            }
        }

        private static Delegation put(String name, UnaryOperator<Hasher> onHasher,
                                      Consumer<HashStream128> onStream) {
            return new Delegation(name, onHasher, onStream);
        }

        // ranged variants take an offset and a length that both matter, so that one quietly delegating to the
        // whole-array method is caught rather than agreeing by accident
        static Stream<Delegation> provideDelegations() {
            byte[] bytes = {1, 2, 3, 4, 5};
            boolean[] booleans = {true, false, true, true, false};
            short[] shorts = {11, 22, 33, 44, 55};
            char[] chars = {'a', 'b', 'c', 'd', 'e'};
            int[] ints = {101, 202, 303, 404, 505};
            long[] longs = {1_001L, 2_002L, 3_003L, 4_004L, 5_005L};
            float[] floats = {1.5f, 2.5f, 3.5f, 4.5f, 5.5f};
            double[] doubles = {1.25, 2.25, 3.25, 4.25, 5.25};
            UUID uuid = UUID.fromString("1b4e28ba-2fa1-11d2-883f-0016d3cca427");
            HashFunnel<String> funnel = (value, sink) -> sink.putString(value);
            List<String> elements = List.of("alpha", "beta");
            ToLongFunction<String> elementHash = value -> (long) value.hashCode();

            return Stream.of(
                    put("putByte", h -> h.putByte((byte) 7), s -> s.putByte((byte) 7)),
                    put("putBytes", h -> h.putBytes(bytes), s -> s.putBytes(bytes)),
                    put("putBytes(off,len)", h -> h.putBytes(bytes, 1, 3), s -> s.putBytes(bytes, 1, 3)),
                    put("putBytes(access)", h -> h.putBytes(bytes, 1L, 3L, ByteAccess.forByteArray()),
                            s -> s.putBytes(bytes, 1L, 3L, ByteAccess.forByteArray())),
                    put("putByteArray", h -> h.putByteArray(bytes), s -> s.putByteArray(bytes)),
                    put("putBoolean", h -> h.putBoolean(true), s -> s.putBoolean(true)),
                    put("putBooleans", h -> h.putBooleans(booleans), s -> s.putBooleans(booleans)),
                    put("putBooleans(off,len)", h -> h.putBooleans(booleans, 1, 3),
                            s -> s.putBooleans(booleans, 1, 3)),
                    put("putBooleanArray", h -> h.putBooleanArray(booleans), s -> s.putBooleanArray(booleans)),
                    put("putShort", h -> h.putShort((short) 1234), s -> s.putShort((short) 1234)),
                    put("putShorts", h -> h.putShorts(shorts), s -> s.putShorts(shorts)),
                    put("putShorts(off,len)", h -> h.putShorts(shorts, 1, 3), s -> s.putShorts(shorts, 1, 3)),
                    put("putShortArray", h -> h.putShortArray(shorts), s -> s.putShortArray(shorts)),
                    put("putChar", h -> h.putChar('x'), s -> s.putChar('x')),
                    put("putChars", h -> h.putChars(chars), s -> s.putChars(chars)),
                    put("putChars(off,len)", h -> h.putChars(chars, 1, 3), s -> s.putChars(chars, 1, 3)),
                    put("putChars(CharSequence)", h -> h.putChars("sequence"),
                            s -> s.putChars("sequence")),
                    put("putCharArray", h -> h.putCharArray(chars), s -> s.putCharArray(chars)),
                    put("putString", h -> h.putString("string"), s -> s.putString("string")),
                    put("putInt", h -> h.putInt(42), s -> s.putInt(42)),
                    put("putInts", h -> h.putInts(ints), s -> s.putInts(ints)),
                    put("putInts(off,len)", h -> h.putInts(ints, 1, 3), s -> s.putInts(ints, 1, 3)),
                    put("putIntArray", h -> h.putIntArray(ints), s -> s.putIntArray(ints)),
                    put("putLong", h -> h.putLong(42L), s -> s.putLong(42L)),
                    put("putLongs", h -> h.putLongs(longs), s -> s.putLongs(longs)),
                    put("putLongs(off,len)", h -> h.putLongs(longs, 1, 3), s -> s.putLongs(longs, 1, 3)),
                    put("putLongArray", h -> h.putLongArray(longs), s -> s.putLongArray(longs)),
                    put("putFloat", h -> h.putFloat(1.5f), s -> s.putFloat(1.5f)),
                    put("putFloats", h -> h.putFloats(floats), s -> s.putFloats(floats)),
                    put("putFloats(off,len)", h -> h.putFloats(floats, 1, 3), s -> s.putFloats(floats, 1, 3)),
                    put("putFloatArray", h -> h.putFloatArray(floats), s -> s.putFloatArray(floats)),
                    put("putDouble", h -> h.putDouble(1.25), s -> s.putDouble(1.25)),
                    put("putDoubles", h -> h.putDoubles(doubles), s -> s.putDoubles(doubles)),
                    put("putDoubles(off,len)", h -> h.putDoubles(doubles, 1, 3), s -> s.putDoubles(doubles, 1, 3)),
                    put("putDoubleArray", h -> h.putDoubleArray(doubles), s -> s.putDoubleArray(doubles)),
                    put("putUUID", h -> h.putUUID(uuid), s -> s.putUUID(uuid)),
                    put("put(funnel)", h -> h.put("alpha", funnel), s -> s.put("alpha", funnel)),
                    put("putNullable", h -> h.putNullable("alpha", funnel), s -> s.putNullable("alpha", funnel)),
                    put("putNullable(null)", h -> h.putNullable(null, funnel), s -> s.putNullable(null, funnel)),
                    put("putOrderedIterable", h -> h.putOrderedIterable(elements, funnel),
                            s -> s.putOrderedIterable(elements, funnel)),
                    put("putUnorderedIterable(toLong)", h -> h.putUnorderedIterable(elements, elementHash),
                            s -> s.putUnorderedIterable(elements, elementHash)),
                    put("putUnorderedIterable(stream)",
                            h -> h.putUnorderedIterable(elements, funnel, Hashing.xxh3_64().hashStream()),
                            s -> s.putUnorderedIterable(elements, funnel, Hashing.xxh3_64().hashStream())),
                    put("putUnorderedIterable(hasher)",
                            h -> h.putUnorderedIterable(elements, funnel, Hashing.xxh3_64()),
                            s -> s.putUnorderedIterable(elements, funnel, Hashing.xxh3_64())),
                    put("putOptional", h -> h.putOptional(Optional.of("alpha"), funnel),
                            s -> s.putOptional(Optional.of("alpha"), funnel)),
                    put("putOptionalInt", h -> h.putOptionalInt(OptionalInt.of(5)),
                            s -> s.putOptionalInt(OptionalInt.of(5))),
                    put("putOptionalLong", h -> h.putOptionalLong(OptionalLong.of(5L)),
                            s -> s.putOptionalLong(OptionalLong.of(5L))),
                    put("putOptionalDouble", h -> h.putOptionalDouble(OptionalDouble.of(5.0)),
                            s -> s.putOptionalDouble(OptionalDouble.of(5.0))));
        }

        @DisplayName("that every put method delegates to the matching one, records that it carried data and chains")
        @ParameterizedTest(name = "{0}")
        @MethodSource("provideDelegations")
        void test_Hasher_put_methods_delegate(Delegation delegation) {
            Hasher hasher = new Hasher();

            // chaining: a put answers with the hasher it was called on, not a new one
            assertThat(delegation.onHasher().apply(hasher)).isSameAs(hasher);

            HashStream128 expected = Hashing.xxh3_128().hashStream().reset();
            delegation.onStream().accept(expected);

            // getHash() throws unless the put recorded that something was put, so this covers that flag as well
            assertThat(hasher.getHash())
                    .isEqualTo(HexFormat.of().formatHex(expected.get().toByteArray()));
        }

        @DisplayName("that empty hash stream throws exception")
        @Test
        void test_Hasher_empty_hash_stream_throws_exception() {
            assertThatException().isThrownBy(() -> new Hasher().getHash())
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .withMessage("Nothing to hash");
        }

        @DisplayName("that populated hash stream returns a hash")
        @Test
        void test_Hasher_populated_hash_stream_returns_hash() {
            String hash = new Hasher().putString("something").getHash();
            assertThat(hash).isNotBlank().hasSize(32);
        }

        @DisplayName("that keys of supported types are hashed out of the box")
        @Test
        void test_Hasher_keys_of_supported_types_are_hashed_out_of_the_box() {
            InternalHasher<Object> hasher = new InternalHasher<>(null);
            UUID uuid = UUID.randomUUID();

            // hashed exactly as putting the key into a hasher by hand would, so that an application migrating to
            // a hash provider of its own can keep the entries already written to the store
            assertThat(hasher.getHash("key")).isEqualTo(new Hasher().putString("key").getHash());
            assertThat(hasher.getHash(1L)).isEqualTo(new Hasher().putLong(1L).getHash());
            assertThat(hasher.getHash(1)).isEqualTo(new Hasher().putInt(1).getHash());
            assertThat(hasher.getHash(uuid)).isEqualTo(new Hasher().putUUID(uuid).getHash());

            // each type is put with the accessor of its own instead of a shared one, so keys that are equal in
            // value but not in type stay apart
            assertThat(hasher.getHash(1L)).isNotEqualTo(hasher.getHash(1));
        }

        @DisplayName("that keys implementing Hashable are hashed by themselves")
        @Test
        void test_Hasher_keys_implementing_hashable_are_hashed_by_themselves() {
            Key key = Key.of(1, "name");

            assertThat(new InternalHasher<Key>(null).getHash(key))
                    .isEqualTo(key.getHash(Hasher::new));
        }

        @DisplayName("that a configured hash provider takes precedence")
        @Test
        void test_Hasher_configured_hash_provider_takes_precedence() {
            String hash = UUID.randomUUID().toString();
            InternalHasher<Object> hasher = new InternalHasher<>((key, hasherSupplier) -> hash);

            // over the types hashed out of the box as well as over keys hashing themselves, so that configuring
            // one is enough to take over hashing entirely
            assertThat(hasher.getHash("key")).isEqualTo(hash);
            assertThat(hasher.getHash(Key.of(1))).isEqualTo(hash);
        }

        @DisplayName("that keys of unsupported types throw exception")
        @Test
        void test_Hasher_keys_of_unsupported_types_throw_exception() {
            InternalHasher<Double> hasher = new InternalHasher<>(null);

            assertThatException().isThrownBy(() -> hasher.getHash(1.0))
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .withMessage("Keys of type Double are not hashable out of the box (only String, Long, Integer and UUID are), "
                            .concat("keys have to implement the Hashable interface or a HashProvider has to be specified."));
        }
    }

    @Nested
    @DisplayName("Test CacheEntry and CacheEntryMetadata")
    final class CacheEntryUnit extends DistributedCaffeineUnitTestInstance {

        // the same instant twice, once with a nanosecond an underlying store cannot be expected to keep
        private static final Instant TIMESTAMP = Instant.ofEpochMilli(1_700_000_000_000L);
        private static final Instant TIMESTAMP_WITH_NANOS = TIMESTAMP.plusNanos(1);

        @DisplayName("that field values are checked against the conditions of a cache entry")
        @Test
        @SuppressWarnings("java:S5778")
        void test_CacheEntry_checks_on_field_values() {
            // the fields no cache entry can do without, whatever its status
            assertThatThrownBy(() ->
                    CacheEntry.of(_null(), "op", Key.of(1), Value.of(1), Status.CACHED, TIMESTAMP))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("hash cannot be null");
            assertThatThrownBy(() ->
                    CacheEntry.of("h", "op", Key.of(1), Value.of(1), _null(), TIMESTAMP))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("status cannot be null");
            assertThatThrownBy(() ->
                    CacheEntry.of("h", "op", Key.of(1), Value.of(1), Status.CACHED, _null()))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("timestamp cannot be null");

            // a key belongs to every cache entry except one carrying a command, and a value to every one except an
            // invalidated one or one carrying a command - checked for every status, so that a status added later is
            // covered by whichever of the two rules it falls under
            Stream.of(Status.values()).forEach(status -> {
                if (!status.isCommand()) {
                    assertThatThrownBy(() ->
                            CacheEntry.of("h", "op", null, Value.of(1), status, TIMESTAMP))
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("key cannot be null");
                }
                if (!status.isInvalidated() && !status.isCommand()) {
                    assertThatThrownBy(() ->
                            CacheEntry.of("h", "op", Key.of(1), null, status, TIMESTAMP))
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("value cannot be null");
                }
            });

            // ... while an absent key or value is what those statuses call for, and an operation is optional throughout
            Stream.of(Status.values())
                    .filter(Status::isInvalidated)
                    .forEach(status -> assertThat(CacheEntry.of("h", "op", Key.of(1), null, status, TIMESTAMP))
                            .satisfies(cacheEntry -> assertThat(cacheEntry.getValue()).isNull()));
            assertThat(CacheEntry.of("invalidate_all", "op", null, null, Status.COMMAND, TIMESTAMP))
                    .satisfies(cacheEntry -> {
                        assertThat(cacheEntry.getKey()).isNull();
                        assertThat(cacheEntry.getValue()).isNull();
                        assertThat(cacheEntry.isCommand()).isTrue();
                    });
            assertThat(CacheEntry.of("h", null, Key.of(1), Value.of(1), Status.CACHED, TIMESTAMP).getOperation())
                    .isNull();
        }

        @DisplayName("that cache entries are compared at the timestamp resolution of an underlying store")
        @Test
        void test_CacheEntry_equals_at_store_resolution() {
            assertThat(CacheEntry.of("h", "op", Key.of(1), Value.of(1), Status.CACHED, TIMESTAMP))
                    .isEqualTo(CacheEntry.of("h", "op", Key.of(1), Value.of(1), Status.CACHED, TIMESTAMP_WITH_NANOS))
                    .hasSameHashCodeAs(CacheEntry.of("h", "op", Key.of(1), Value.of(1), Status.CACHED,
                            TIMESTAMP_WITH_NANOS));
        }

        @DisplayName("that field values are checked against the conditions of cache entry metadata")
        @Test
        @SuppressWarnings("java:S5778")
        void test_CacheEntryMetadata_checks_on_field_values() {
            // metadata has no key and value, so all of its fields but the operation are required
            assertThatThrownBy(() -> CacheEntryMetadata.of(_null(), "op", Status.CACHED, TIMESTAMP))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("hash cannot be null");
            assertThatThrownBy(() -> CacheEntryMetadata.of("h", "op", _null(), TIMESTAMP))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("status cannot be null");
            assertThatThrownBy(() -> CacheEntryMetadata.of("h", "op", Status.CACHED, _null()))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("timestamp cannot be null");
            assertThat(CacheEntryMetadata.of("h", null, Status.CACHED, TIMESTAMP).getOperation())
                    .isNull();
        }

        @DisplayName("that cache entry metadata implements equals(), hashCode() and toString()")
        @Test
        @SuppressWarnings("EqualsIncompatibleType")
            // comparing unrelated types is the point of the equals() contract test
        void test_CacheEntryMetadata_equals_hashCode_toString() {
            CacheEntryMetadata metadata = CacheEntryMetadata.of("h1", "op1", Status.CACHED, TIMESTAMP);
            CacheEntryMetadata equalMetadata = CacheEntryMetadata.of("h1", "op1", Status.CACHED, TIMESTAMP);
            CacheEntryMetadata otherMetadata = CacheEntryMetadata.of("h2", "op2", Status.CACHED, TIMESTAMP);

            // noinspection ConstantValue
            assertThat(metadata.equals(null)).isFalse();
            // noinspection EqualsBetweenInconvertibleTypes
            assertThat(metadata.equals("other class")).isFalse();
            // noinspection EqualsWithItself
            assertThat(metadata.equals(metadata)).isTrue();
            assertThat(metadata).isEqualTo(equalMetadata)
                    .hasSameHashCodeAs(equalMetadata)
                    .isNotEqualTo(otherMetadata);
            assertThat(metadata.hashCode()).isNotEqualTo(otherMetadata.hashCode());
            assertThat(metadata.toString()).isNotEqualTo(otherMetadata.toString());
            // compared at the timestamp resolution of an underlying store, just like a cache entry
            assertThat(metadata)
                    .isEqualTo(CacheEntryMetadata.of("h1", "op1", Status.CACHED, TIMESTAMP_WITH_NANOS));
            // the field names of an underlying store are what it names its values by
            assertThat(metadata.toString())
                    .startsWith("CacheEntryMetadata{hash=h1, operation=op1, status=cached, timestamp=");
        }
    }

    @Nested
    @DisplayName("Test MongoAdapter")
    final class MongoAdapterUnit extends DistributedCaffeineUnitTestInstance {

        @DisplayName("that a discriminator is checked where it is configured")
        @Test
        @SuppressWarnings({"resource", "java:S5778"})
            // a mock holds nothing that could be closed
        void test_MongoAdapter_checks_the_discriminator() {
            // nothing here reaches a server: what a builder rejects, it rejects before it is built
            MongoClient mongoClient = mock(MongoClient.class, RETURNS_DEEP_STUBS);

            // a discriminator gone missing is reported instead of silently placing the cache in a scope of its
            // own, where it would neither synchronize with the caches it was meant to nor say so
            assertThatThrownBy(() -> MongoAdapter.newBuilder(mongoClient, "database", "collection")
                    .withDiscriminator(_null()))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("discriminator cannot be null");
            Stream.of("", " ", "\t\n").forEach(blank ->
                    assertThatThrownBy(() -> MongoAdapter.newBuilder(mongoClient, "database", "collection")
                            .withDiscriminator(blank))
                            .isExactlyInstanceOf(IllegalArgumentException.class)
                            .hasMessage("discriminator cannot be blank"));
        }
    }

    @Nested
    @DisplayName("Test MongoRepository")
    final class MongoRepositoryUnit extends DistributedCaffeineUnitTestInstance {

        private static final String DATABASE_NAME = "database";
        private static final String COLLECTION_NAME = "collection";

        @DisplayName("that binary JSON values round-trip through BSON conversion (including scalars)")
        @Test
        void test_MongoRepository_binary_json_round_trip() throws Exception {
            // a scalar value stored as binary JSON must round-trip
            assertBinaryJsonRoundTrip(new JacksonSerializer<>(String.class, true), "hello");
            // an object value stored as binary JSON must round-trip
            assertBinaryJsonRoundTrip(new JacksonSerializer<>(Value.class, true), Value.of(1));
        }

        private <T> void assertBinaryJsonRoundTrip(Serializer<T, ?> serializer, T original) throws Exception {
            // MongoRepository (and its BSON conversion helpers) is package-private in another package, so the
            // round-trip is exercised reflectively (via the inherited invokeMethod helper) without requiring a
            // running MongoDB instance
            Class<?> mongoRepositoryClass = Class.forName(
                    "io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoRepository");

            Object stored = invokeMethod(null, mongoRepositoryClass, "serializeToMongo",
                    List.of(Object.class, Serializer.class), List.of(original, serializer));
            @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
            Document document = new Document("value", stored);
            Object roundTripped = invokeMethod(null, mongoRepositoryClass, "deserializeFromMongo",
                    List.of(Document.class, String.class, Serializer.class), List.of(document, "value", serializer));

            assertThat(roundTripped)
                    .isEqualTo(original);
        }

        @DisplayName("that bulk upserts are unordered and duplicate key errors are retried")
        @Test
        void test_MongoRepository_bulk_upsert_retries_duplicate_key_errors() throws Exception {
            MongoClient mongoClient = mock(MongoClient.class, RETURNS_DEEP_STUBS);
            MongoCollection<Document> mongoCollection = mongoCollectionOf(mongoClient);
            Repository<Key, Value> repository = repositoryOf(mongoClient);

            // only the second entry loses the race against a concurrent insert of the same key
            when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
                    .thenThrow(bulkWriteExceptionOf(
                            new BulkWriteError(11000, "E11000 duplicate key error", new BsonDocument(), 1)))
                    .thenReturn(BulkWriteResult.unacknowledged());

            repository.publishCacheEntries(List.of(cacheEntry("hash1", 1), cacheEntry("hash2", 2)));

            ArgumentCaptor<List<UpdateOneModel<Document>>> updatesCaptor = ArgumentCaptor.captor();
            ArgumentCaptor<BulkWriteOptions> optionsCaptor = ArgumentCaptor.captor();
            verify(mongoCollection, times(2)).bulkWrite(updatesCaptor.capture(), optionsCaptor.capture());

            List<UpdateOneModel<Document>> attempted = updatesCaptor.getAllValues().get(0);
            // the rejected operation is applied again on its own; the document exists by now, so it becomes an update
            assertThat(attempted).hasSize(2);
            assertThat(updatesCaptor.getAllValues().get(1)).containsExactly(attempted.get(1));
            // unordered, so a rejected operation never discards the rest of the batch
            assertThat(optionsCaptor.getAllValues()).allSatisfy(options ->
                    assertThat(options.isOrdered()).isFalse());
        }

        @DisplayName("that bulk upserts do not swallow errors other than duplicate key errors")
        @Test
        @SuppressWarnings("java:S5778")
        void test_MongoRepository_bulk_upsert_propagates_other_errors() throws Exception {
            MongoClient mongoClient = mock(MongoClient.class, RETURNS_DEEP_STUBS);
            MongoCollection<Document> mongoCollection = mongoCollectionOf(mongoClient);
            Repository<Key, Value> repository = repositoryOf(mongoClient);

            MongoBulkWriteException validationException = bulkWriteExceptionOf(
                    new BulkWriteError(121, "Document failed validation", new BsonDocument(), 0));
            when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
                    .thenThrow(validationException);

            assertThatThrownBy(() -> repository.publishCacheEntries(List.of(cacheEntry("hash1", 1))))
                    .isSameAs(validationException);

            // failed once and was not retried
            verify(mongoCollection, times(1)).bulkWrite(anyList(), any(BulkWriteOptions.class));
        }

        @DisplayName("that reading metadata leaves the key and the value out of the query")
        @Test
        @SuppressWarnings("unchecked")
        void test_MongoRepository_projects_metadata_fields_only() throws Exception {
            MongoClient mongoClient = mock(MongoClient.class, RETURNS_DEEP_STUBS);
            MongoCollection<Document> mongoCollection = mongoCollectionOf(mongoClient);
            Repository<Key, Value> repository = repositoryOf(mongoClient);

            FindIterable<Document> findIterable = mock(FindIterable.class);
            MongoCursor<Document> mongoCursor = mock(MongoCursor.class);
            when(mongoCollection.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.projection(any())).thenReturn(findIterable);
            when(findIterable.sort(any())).thenReturn(findIterable);
            when(findIterable.cursor()).thenReturn(mongoCursor);
            when(mongoCursor.hasNext()).thenReturn(false);

            ArgumentCaptor<Bson> projectionCaptor = ArgumentCaptor.captor();

            // what the query asks the store for is what decides whether the key and the value are read at all, which
            // no assertion on the returned metadata could tell (metadata never looks at those fields either way)
            try (Stream<CacheEntryMetadata> stream = repository.streamCacheEntryMetadata(null, null, UNORDERED)) {
                assertThat(stream).isEmpty();
            }
            verify(findIterable, times(1)).projection(projectionCaptor.capture());
            assertThat(projectionCaptor.getValue().toBsonDocument().keySet())
                    .containsExactlyInAnyOrder(
                            CacheEntry.Field.HASH.toString(), CacheEntry.Field.OPERATION.toString(),
                            CacheEntry.Field.STATUS.toString(), CacheEntry.Field.TIMESTAMP.toString())
                    .doesNotContain(CacheEntry.Field.KEY.toString(), CacheEntry.Field.VALUE.toString());

            // a cache entry, in contrast, is read with all of its fields
            try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(null, null, UNORDERED)) {
                assertThat(stream).isEmpty();
            }
            verify(findIterable, times(2)).projection(projectionCaptor.capture());
            assertThat(projectionCaptor.getValue().toBsonDocument().keySet())
                    .containsExactlyInAnyOrder(Stream.of(CacheEntry.Field.values())
                            .map(CacheEntry.Field::toString)
                            .toArray(String[]::new));
        }

        private MongoCollection<Document> mongoCollectionOf(MongoClient mongoClient) {
            // deep stubs return the same collection mock the repository resolves for these names - the write
            // concern it pins included, which resolves a collection of its own that the calls below land on
            return mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME)
                    .withWriteConcern(WriteConcern.MAJORITY);
        }

        // MongoRepository is package-private in another package, so it is constructed reflectively - but Repository
        // is public, so the upsert itself is driven through the normal API. Its constructor only resolves the
        // collection and ensures indexes, both of which the deep stubs absorb
        @SuppressWarnings("unchecked")
        private Repository<Key, Value> repositoryOf(MongoClient mongoClient) throws Exception {
            Constructor<?> constructor = Class
                    .forName("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoRepository")
                    .getDeclaredConstructor(MongoClient.class, String.class, String.class);
            constructor.setAccessible(true);
            Repository<Key, Value> repository = (Repository<Key, Value>)
                    constructor.newInstance(mongoClient, DATABASE_NAME, COLLECTION_NAME);
            // wiring an adapter would normally do, which constructing the repository directly skips
            repository.setDiscriminator(DEFAULT_DISCRIMINATOR);
            repository.setKeySerializer(new JacksonSerializer<>(Key.class, false));
            repository.setValueSerializer(new JacksonSerializer<>(Value.class, false));
            return repository;
        }

        private CacheEntry<Key, Value> cacheEntry(String hash, int id) {
            return CacheEntry.of(hash, "op" + id, Key.of(id), Value.of(id), Status.CACHED, Instant.now());
        }

        private MongoBulkWriteException bulkWriteExceptionOf(BulkWriteError bulkWriteError) {
            return new MongoBulkWriteException(BulkWriteResult.unacknowledged(), List.of(bulkWriteError),
                    null, new ServerAddress(), Set.of());
        }
    }

    @Nested
    @DisplayName("Test MongoWatcher")
    final class MongoWatcherUnit extends DistributedCaffeineUnitTestInstance {

        private static final String DATABASE_NAME = "database";
        private static final String COLLECTION_NAME = "collection";
        private static final String WATCHER_CLASS_NAME =
                "io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoWatcher";

        @DisplayName("that watching resumes after the position reported while no events occurred")
        @Test
        void test_MongoWatcher_resumes_after_position_reported_while_idle() throws Exception {
            // the position the server reports for a polled batch, here an empty one because nothing has happened
            BsonDocument tokenWhileIdle = new BsonDocument("_data", new BsonString("tokenWhileIdle"));

            MongoClient mongoClient = mock(MongoClient.class, RETURNS_DEEP_STUBS);
            ChangeStreamIterable<Document> changeStreamIterable = mock();
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = mock();

            // the watcher watches through the database limited to its operation timeout
            when(mongoClient.getDatabase(DATABASE_NAME).withTimeout(anyLong(), any())
                    .getCollection(COLLECTION_NAME).watch(anyList()))
                    .thenReturn(changeStreamIterable);
            when(changeStreamIterable.fullDocument(any())).thenReturn(changeStreamIterable);
            when(changeStreamIterable.resumeAfter(any())).thenReturn(changeStreamIterable);
            when(changeStreamIterable.cursor()).thenReturn(cursor);
            when(cursor.getResumeToken()).thenReturn(tokenWhileIdle);
            // the cursor is still held by the server, so an empty poll is an idle one rather than the end of the stream
            when(cursor.getServerCursor()).thenReturn(new ServerCursor(1, new ServerAddress()));
            // an idle poll delivering no event, then watching fails - so no event was ever there to take a position
            // from, which is the situation an operation time cannot cover (it only ever comes from an event)
            MongoException connectionLost = new MongoException("connection lost");
            when(cursor.tryNext()).thenReturn(null).thenThrow(connectionLost);

            Object watcher = watcherOf(mongoClient);
            Object subscriber = subscriberOn(watcher);

            // the first attempt has nothing to resume from, so it watches from wherever the stream currently is
            assertThatThrownBy(() -> watch(watcher)).isSameAs(connectionLost);
            verify(changeStreamIterable, never()).resumeAfter(any());
            // and it is a first start rather than a reopen, so nothing can have been missed yet
            assertThat(invocationsOf(subscriber, "receiveRestart")).isZero();

            // the retry must not start over at "now", which would skip everything written while watching was down.
            // It resumes strictly after the position the failed attempt saw while idle
            assertThatThrownBy(() -> watch(watcher)).isSameAs(connectionLost);
            verify(changeStreamIterable, times(1)).resumeAfter(tokenWhileIdle);
            // resuming is a reopen, and an event whose document was swept while watching was down would not be
            // delivered by it, so the possibility of having missed one is reported - once, to whoever relied on it
            assertThat(invocationsOf(subscriber, "receiveRestart")).isOne();
        }

        // MongoWatcher and its watch loop are package-private in another package, so both are reached reflectively.
        // A watcher of one cache instance alone, which is the one selecting by discriminator
        private Object watcherOf(MongoClient mongoClient) throws Exception {
            Constructor<?> constructor = Class.forName(WATCHER_CLASS_NAME)
                    .getDeclaredConstructor(MongoClient.class, String.class, String.class, boolean.class,
                            Duration.class);
            constructor.setAccessible(true);
            return constructor.newInstance(mongoClient, DATABASE_NAME, COLLECTION_NAME, false, Duration.ofSeconds(10));
        }

        // a subscriber of the collection watched, subscribed the way a synchronizer subscribes - its subscription is
        // carried out by the watching thread, which is the watch loop driven by the test
        // (the interface cannot be named here, so the mock answers by method name)
        private Object subscriberOn(Object watcher) {
            Class<?> subscriberClass = classOf(WATCHER_CLASS_NAME + "$Subscriber");
            Object subscriber = mock(subscriberClass, invocation -> switch (invocation.getMethod().getName()) {
                case "getCollectionName" -> COLLECTION_NAME;
                case "getDiscriminator" -> DEFAULT_DISCRIMINATOR;
                case "getIdentifier" -> "mongodb:database:collection:default";
                default -> null;
            });
            invokeMethod(watcher, watcher.getClass(), "subscribe", List.of(subscriberClass), List.of(subscriber));
            return subscriber;
        }

        private void watch(Object watcher) {
            invokeMethod(watcher, watcher.getClass(), "watch", List.of(), List.of());
        }
    }

    @Nested
    @DisplayName("Test PostgresAdapter")
    final class PostgresAdapterUnit extends DistributedCaffeineUnitTestInstance {

        @DisplayName("that a schema and a table name are taken as they are, and only an empty one is refused")
        @Test
        void test_PostgresAdapter_checks_names() {
            DataSource dataSource = mock(DataSource.class);

            // A name reaches a statement as text, because it cannot be a parameter of one - and it is quoted
            // there, which is what lets it be a name PostgreSQL would otherwise need explaining: a table some
            // migration tool called this, a schema with a space in it, one that is not ASCII at all. Refusing
            // them here would have bought nothing and shut out every such schema
            Stream.of("public", "_leading", "with_underscore", "With$Dollar", "a1", "cache-entries", "has space",
                            "1leading", "a.b", "Großschreibung", "キャッシュ")
                    .forEach(name -> assertThatCode(() -> PostgresAdapter.newBuilder(dataSource, name, name))
                            .doesNotThrowAnyException());

            // including one carrying a quote of its own, which the quoting doubles rather than lets out
            assertThatCode(() -> PostgresAdapter.newBuilder(dataSource, "a\"quote", "t\"; DROP TABLE x --"))
                    .doesNotThrowAnyException();

            // and the one name there is nothing to address by
            assertThatThrownBy(() -> PostgresAdapter.newBuilder(dataSource, "", "table"))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("schemaName cannot be empty");
            assertThatThrownBy(() -> PostgresAdapter.newBuilder(dataSource, "schema", ""))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("tableName cannot be empty");
        }

        @DisplayName("that missing arguments and a blank discriminator are reported where they are configured")
        @Test
        @SuppressWarnings("java:S5778")
        void test_PostgresAdapter_checks_arguments() {
            DataSource dataSource = mock(DataSource.class);

            assertThatThrownBy(() -> PostgresAdapter.newBuilder(_null(), "schema", "table"))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("dataSource cannot be null");
            assertThatThrownBy(() -> PostgresAdapter.newBuilder(dataSource, _null(), "table"))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("schemaName cannot be null");
            assertThatThrownBy(() -> PostgresAdapter.newBuilder(dataSource, "schema", _null()))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("tableName cannot be null");

            assertThatThrownBy(() -> PostgresAdapter.newBuilder(dataSource, "schema", "table")
                    .withDiscriminator(_null()))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("discriminator cannot be null");
            Stream.of("", " ", "\t\n").forEach(blank ->
                    assertThatThrownBy(() -> PostgresAdapter.newBuilder(dataSource, "schema", "table")
                            .withDiscriminator(blank))
                            .isExactlyInstanceOf(IllegalArgumentException.class)
                            .hasMessage("discriminator cannot be blank"));
        }
    }

    @Nested
    @DisplayName("Test PostgresChannel")
    final class PostgresChannelUnit extends DistributedCaffeineUnitTestInstance {

        // package-private in another package, so it is reached reflectively (via the inherited invokeMethod
        // helper) - which is what keeps a test of what are pure functions out of the integration suite and away
        // from a running PostgreSQL instance
        private static final Class<?> CHANNEL =
                classOf("io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresChannel");

        // NAMEDATALEN - 1, what the server allows an identifier to be
        private static final int MAXIMUM_IDENTIFIER_LENGTH = 63;
        // what the adapter keeps a notification payload under, itself below the 8000 bytes the server refuses at
        private static final int MAXIMUM_PAYLOAD_LENGTH = 7900;

        @DisplayName("that a channel is derived from the scope, fits an identifier whatever the scope is, and "
                + "separates one scope from another")
        @Test
        void test_PostgresChannel_derives_a_bounded_channel() {
            String identifier = "postgresql:public:cache:discriminator";
            String longIdentifier = "postgresql:public:" + "t".repeat(500) + ":discriminator";

            assertThat(channelOf(identifier))
                    // the same scope always reaches the same channel, which is what makes a listener and a writer
                    // of that scope meet at all
                    .isEqualTo(channelOf(identifier))
                    .startsWith("distributed_caffeine_")
                    .hasSizeLessThanOrEqualTo(MAXIMUM_IDENTIFIER_LENGTH);

            // a scope of any length still fits, which is the whole reason the channel is a digest and not the name
            assertThat(channelOf(longIdentifier)).hasSizeLessThanOrEqualTo(MAXIMUM_IDENTIFIER_LENGTH);
            // and no cache instance is ever woken by writes of a scope other than its own
            assertThat(channelOf(identifier)).isNotEqualTo(channelOf(longIdentifier));
        }

        @DisplayName("that a publish too large for one notification becomes several that put back together")
        @Test
        void test_PostgresChannel_splits_and_rejoins_payloads() {
            assertThat(payloadsOf(List.of())).isEmpty();

            List<String> few = List.of("h1", "h2", "h3");
            assertThat(payloadsOf(few)).singleElement().isEqualTo("h1,h2,h3");
            assertThat(hashesOf("h1,h2,h3")).isEqualTo(few);

            // hashes the length the hasher really produces, and enough of them to need more than one notification
            List<String> many = IntStream.range(0, 1000)
                    .mapToObj(index -> String.format("%032x", index))
                    .toList();
            List<String> payloads = payloadsOf(many);

            assertThat(payloads)
                    .hasSizeGreaterThan(1)
                    // every part has to hold before anything is sent, because the server refuses an over-long
                    // payload at the call rather than truncating it
                    .allSatisfy(payload -> assertThat(payload).hasSizeLessThanOrEqualTo(MAXIMUM_PAYLOAD_LENGTH));
            // nothing is lost and nothing is repeated across the parts, which is what the reader reads back
            assertThat(payloads.stream()
                    .flatMap(payload -> hashesOf(payload).stream())
                    .toList())
                    .containsExactlyElementsOf(many);
        }

        @DisplayName("that a hash carrying the separator is refused rather than silently split in two")
        @Test
        @SuppressWarnings("java:S5778")
        void test_PostgresChannel_refuses_a_hash_containing_the_separator() {
            assertThatThrownBy(() -> payloadsOf(List.of("h1,h2")))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        private String channelOf(String identifier) {
            return invokeMethod(null, CHANNEL, "channelOf", List.of(String.class), List.of(identifier));
        }

        private List<String> payloadsOf(List<String> hashes) {
            return invokeMethod(null, CHANNEL, "payloadsOf", List.of(List.class), List.of(hashes));
        }

        private List<String> hashesOf(String payload) {
            return invokeMethod(null, CHANNEL, "hashesOf", List.of(String.class), List.of(payload));
        }
    }

    @Nested
    @DisplayName("Test PostgresIdentifier")
    final class PostgresIdentifierUnit extends DistributedCaffeineUnitTestInstance {

        private static final Class<?> IDENTIFIER =
                classOf("io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresIdentifier");

        // NAMEDATALEN - 1, what the server allows an identifier to be
        private static final int MAXIMUM_IDENTIFIER_LENGTH = 63;

        @DisplayName("that a name too long to be an identifier is shortened without two of them becoming one")
        @Test
        void test_PostgresIdentifier_keeps_long_names_apart() {
            // what fits is left alone, so a name stays legible wherever it can be
            assertThat(limited("cache_status_timestamp_idx")).isEqualTo("cache_status_timestamp_idx");
            String exactly = "t".repeat(MAXIMUM_IDENTIFIER_LENGTH);
            assertThat(limited(exactly)).isEqualTo(exactly);

            // The pair that the index names of two tables would otherwise truncate into one another, which is what
            // this exists to prevent: the suffix opens with "_status", so a table named after another one plus
            // exactly that runs into it
            String suffix = "_status_timestamp_idx";
            String shorter = "t".repeat(55) + suffix;
            String longer = "t".repeat(55) + "_status" + suffix;
            assertThat(shorter.substring(0, MAXIMUM_IDENTIFIER_LENGTH))
                    .as("what the server would truncate these two to, were they left as they are")
                    .isEqualTo(longer.substring(0, MAXIMUM_IDENTIFIER_LENGTH));

            assertThat(limited(shorter)).hasSize(MAXIMUM_IDENTIFIER_LENGTH);
            assertThat(limited(longer)).hasSize(MAXIMUM_IDENTIFIER_LENGTH);
            assertThat(limited(shorter)).isNotEqualTo(limited(longer));
        }

        private String limited(String identifier) {
            return invokeMethod(null, IDENTIFIER, "limited", List.of(String.class), List.of(identifier));
        }
    }

    @Nested
    @DisplayName("Test PostgresRepository")
    final class PostgresRepositoryUnit extends DistributedCaffeineUnitTestInstance {

        // package-private in another package, and so is the enum naming the columns, so both are reached by name
        private static final Class<?> REPOSITORY =
                classOf("io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresRepository");
        private static final Class<?> STORAGE =
                classOf("io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresRepository$Storage");

        @DisplayName("that a serializer decides which column its field is written to")
        @Test
        void test_PostgresRepository_stores_a_field_by_what_serializes_it() {
            // bytes are opaque wherever they are kept, so they go to the column that keeps bytes
            assertThat(columnFor(CacheEntry.Field.KEY, new JavaObjectSerializer<>())).isEqualTo("key_binary");
            // a string serializer stores a string, which is what keeps a record legible to whoever looks at it
            assertThat(columnFor(CacheEntry.Field.VALUE, stringSerializer())).isEqualTo("value_text");
            // and a JSON serializer stores JSON - as text, or as the store's own JSON when it asks for binary,
            // which is the same distinction the MongoDB adapter makes between a string and a BSON document
            assertThat(columnFor(CacheEntry.Field.VALUE, new JacksonSerializer<>(Value.class, false))).isEqualTo("value_text");
            assertThat(columnFor(CacheEntry.Field.VALUE, new JacksonSerializer<>(Value.class, true))).isEqualTo("value_jsonb");
            assertThat(columnFor(CacheEntry.Field.KEY, new JacksonSerializer<>(Key.class, true))).isEqualTo("key_jsonb");
        }

        @DisplayName("that a timestamp the column could not represent is clamped rather than refused")
        @Test
        void test_PostgresRepository_clamps_a_timestamp_below_what_a_column_holds() {
            // "older than everything" reaches the repository as a point in time, and this is the one maintenance
            // expresses it with - a year no timestamp column can hold
            OffsetDateTime clamped = toOffsetDateTime(Instant.ofEpochMilli(Long.MIN_VALUE));
            assertThat(clamped.getOffset()).isEqualTo(ZoneOffset.UTC);
            assertThat(clamped.getYear()).isEqualTo(1);
            // the floor is below every timestamp a cache entry can carry, so what such a filter selects is unchanged
            assertThat(clamped.toInstant()).isBefore(Instant.EPOCH.minusSeconds(62_000_000_000L));

            // and an ordinary instant passes through untouched, at UTC whatever the machine running this is set to
            Instant instant = Instant.parse("2026-10-02T11:22:33.123456Z");
            OffsetDateTime passedThrough = toOffsetDateTime(instant);
            assertThat(passedThrough.getOffset()).isEqualTo(ZoneOffset.UTC);
            assertThat(passedThrough.toInstant()).isEqualTo(instant);
        }

        @DisplayName("that only a write the server rolled back is worth making again, however deeply it is reported")
        @Test
        void test_PostgresRepository_recognizes_a_rolled_back_write() {
            // class 40 is transaction rollback: a serialization failure and a deadlock both say the same thing to
            // a caller that can simply ask again
            assertThat(isTransactionRollback(new SQLException("serialization failure", "40001"))).isTrue();
            assertThat(isTransactionRollback(new SQLException("deadlock detected", "40P01"))).isTrue();

            // and nothing else is: a table that is not there, a syntax error, or no state at all
            assertThat(isTransactionRollback(new SQLException("no such table", "42P01"))).isFalse();
            assertThat(isTransactionRollback(new SQLException("nothing to go on"))).isFalse();

            // A batch reports what went wrong through the chain rather than through the exception it is reached
            // by, so the one that matters can sit behind an exception saying nothing in particular - which is why
            // the whole chain is walked and not just its head
            SQLException batchFailed = new SQLException("batch entry failed", "00000");
            batchFailed.setNextException(new SQLException("deadlock detected", "40P01"));
            assertThat(isTransactionRollback(batchFailed)).isTrue();

            // and a chain carrying nothing of class 40 stays a plain failure
            SQLException plainChain = new SQLException("batch entry failed", "00000");
            plainChain.setNextException(new SQLException("no such table", "42P01"));
            assertThat(isTransactionRollback(plainChain)).isFalse();
        }

        // the interface rather than an implementation of it, because what decides the column is which interface a
        // serializer answers to and not what it does with the object
        private StringSerializer<Value> stringSerializer() {
            return new StringSerializer<>() {

                @Override
                public @NonNull String serialize(@NonNull Value object) {
                    return object.toString();
                }

                @Override
                public @NonNull Value deserialize(@NonNull String string) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        private String columnFor(CacheEntry.Field field, Serializer<?, ?> serializer) {
            Object storage = invokeMethod(null, REPOSITORY, "storageOf",
                    List.of(Serializer.class), List.of(serializer));
            return invokeMethod(null, REPOSITORY, "columnName",
                    List.of(CacheEntry.Field.class, STORAGE), List.of(field, storage));
        }

        private OffsetDateTime toOffsetDateTime(Instant instant) {
            return invokeMethod(null, REPOSITORY, "toOffsetDateTime", List.of(Instant.class), List.of(instant));
        }

        private boolean isTransactionRollback(SQLException exception) {
            return invokeMethod(null, REPOSITORY, "isTransactionRollback",
                    List.of(SQLException.class), List.of(exception));
        }
    }

    @Nested
    @DisplayName("Test PostgresListener")
    @SuppressWarnings("SqlNoDataSourceInspection")
    final class PostgresListenerUnit extends DistributedCaffeineUnitTestInstance {

        private static final String CHANNEL = "distributed_caffeine_test";
        private static final String SESSION_CHANNEL_PREFIX = "distributed_caffeine_session_";
        private static final String LISTENER_CLASS_NAME =
                "io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresListener";

        @DisplayName("that a connection taken over is unsubscribed and drained before it is listened on")
        @Test
        void test_PostgresListener_takes_over_a_connection_without_inheriting_it() throws Exception {
            Connection connection = connectionMock();
            Statement statement = connection.createStatement();
            PGConnection pgConnection = connection.unwrap(PGConnection.class);
            when(connection.isValid(anyInt())).thenReturn(true);

            // what a previous tenant of this pooled connection was handed before it was given up, followed by the
            // poll that ends the session - so whatever is drained cannot be mistaken for something that arrived here
            PGNotification stale = mock();
            when(stale.getName()).thenReturn(CHANNEL);
            when(stale.getParameter()).thenReturn("h1");
            SQLException sessionEnded = new SQLException("provoked");
            when(pgConnection.getNotifications(anyInt()))
                    .thenReturn(new PGNotification[]{stale})
                    .thenReturn(null)
                    .thenThrow(sessionEnded);

            Object listener = listenerOf(connection);
            Object subscriber = subscriberOn(listener);

            assertThatThrownBy(() -> listen(listener)).isSameAs(sessionEnded);

            // dropped first, because a connection that was not given the chance to unsubscribe hands its
            // subscriptions on with it - and they name channels nobody here subscribes to
            InOrder inOrder = inOrder(statement);
            inOrder.verify(statement).execute("UNLISTEN *");
            inOrder.verify(statement).addBatch("LISTEN " + CHANNEL);

            // and what the session had already been handed is thrown away rather than handed over: unsubscribing
            // stops what comes next, while whatever reached it before is queued and arrives on the first poll
            assertThat(invocationsOf(subscriber, "receiveHashes")).isZero();
        }

        @DisplayName("that a first start reports nothing while listening again reports what may have been missed")
        @Test
        void test_PostgresListener_reports_only_a_restart() throws Exception {
            Connection connection = connectionMock();
            Statement statement = connection.createStatement();
            PGConnection pgConnection = connection.unwrap(PGConnection.class);
            when(connection.isValid(anyInt())).thenReturn(true);
            // The probe arrives on the channel the session listens on for it, which is only known once listened
            // on. Draining does not wait at all, the probe and the polls do: nothing is ever drained, the probe is
            // answered once per session, and the poll after it is what ends the session
            AtomicReference<String> unansweredProbe = new AtomicReference<>();
            doAnswer(invocation -> {
                String sql = invocation.getArgument(0);
                if (sql.startsWith("LISTEN " + SESSION_CHANNEL_PREFIX)) {
                    unansweredProbe.set(sql.substring("LISTEN ".length()));
                }
                return false;
            }).when(statement).execute(anyString());
            SQLException sessionEnded = new SQLException("provoked");
            when(pgConnection.getNotifications(anyInt())).thenAnswer(invocation -> {
                if (invocation.<Integer>getArgument(0) <= 1) {
                    return null;
                }
                String probeChannel = unansweredProbe.getAndSet(null);
                if (probeChannel != null) {
                    PGNotification probe = mock();
                    when(probe.getName()).thenReturn(probeChannel);
                    return new PGNotification[]{probe};
                }
                throw sessionEnded;
            });

            Object listener = listenerOf(connection);
            Object subscriber = subscriberOn(listener);

            // a first start has missed nothing, because activating reconciles anyway
            assertThatThrownBy(() -> listen(listener)).isSameAs(sessionEnded);
            assertThat(invocationsOf(subscriber, "receiveRestart")).isZero();

            // and listening again means nothing was listening in between, where a notification reaches the sessions
            // listening at the time and is kept for nobody - so the possibility of having missed one is reported to
            // whoever relied on the session that was lost
            assertThatThrownBy(() -> listen(listener)).isSameAs(sessionEnded);
            assertThat(invocationsOf(subscriber, "receiveRestart")).isOne();
        }

        @DisplayName("that a connection which can no longer carry a statement is not unsubscribed from")
        @Test
        void test_PostgresListener_does_not_unsubscribe_a_broken_connection() throws Exception {
            Connection connection = connectionMock();
            Statement statement = connection.createStatement();
            PGConnection pgConnection = connection.unwrap(PGConnection.class);
            SQLException sessionEnded = new SQLException("connection lost");
            when(pgConnection.getNotifications(anyInt())).thenReturn(null).thenThrow(sessionEnded);
            // a broken connection does not report itself as closed, so it is asked rather than assumed - and the
            // pool discards it instead of handing it on, so there is nothing left to unsubscribe from
            when(connection.isValid(anyInt())).thenReturn(false);

            Object listener = listenerOf(connection);
            subscriberOn(listener);

            assertThatThrownBy(() -> listen(listener)).isSameAs(sessionEnded);

            // unsubscribed from everything once, on taking the connection over, and not again on giving it up
            verify(statement, times(1)).execute("UNLISTEN *");
            verify(connection, atLeastOnce()).close();
        }

        // one connection serving both listening and the probe, which is sent through a connection of its own taken
        // from the same mocked data source
        private Connection connectionMock() throws SQLException {
            Connection connection = mock();
            Statement statement = mock();
            PGConnection pgConnection = mock();
            PreparedStatement preparedStatement = mock();
            when(connection.createStatement()).thenReturn(statement);
            when(connection.unwrap(PGConnection.class)).thenReturn(pgConnection);
            when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
            return connection;
        }

        // PostgresListener and its session loop are package-private in another package, so both are reached
        // reflectively - with the production settings, none of which a test here waits out
        private Object listenerOf(Connection connection) throws Exception {
            DataSource dataSource = mock();
            when(dataSource.getConnection()).thenReturn(connection);
            Class<?> settingsClass = classOf(LISTENER_CLASS_NAME + "$Settings");
            Constructor<?> settingsConstructor = settingsClass.getDeclaredConstructor(Duration.class, Duration.class,
                    Duration.class, Duration.class);
            settingsConstructor.setAccessible(true);
            Object settings = settingsConstructor.newInstance(Duration.ofSeconds(10), Duration.ofSeconds(5),
                    Duration.ofSeconds(5), Duration.ofSeconds(30));
            Constructor<?> constructor = Class.forName(LISTENER_CLASS_NAME)
                    .getDeclaredConstructor(DataSource.class, DataSource.class, settingsClass);
            constructor.setAccessible(true);
            return constructor.newInstance(dataSource, dataSource, settings);
        }

        // a subscriber of one channel, subscribed the way a synchronizer subscribes - its subscription is carried
        // out by the listening thread, which is the session loop driven by the test (the interface cannot be named
        // here, so the mock answers by method name)
        private Object subscriberOn(Object listener) {
            Class<?> subscriberClass = classOf(LISTENER_CLASS_NAME + "$Subscriber");
            Object subscriber = mock(subscriberClass, invocation -> switch (invocation.getMethod().getName()) {
                case "getChannel" -> CHANNEL;
                case "getIdentifier" -> "postgresql:public:table:default";
                default -> null;
            });
            invokeMethod(listener, listener.getClass(), "subscribe", List.of(subscriberClass), List.of(subscriber));
            return subscriber;
        }

        private void listen(Object listener) {
            invokeMethod(listener, listener.getClass(), "listen", List.of(), List.of());
        }
    }

    @Nested
    @DisplayName("Test Aware getters")
    final class AwareUnit extends DistributedCaffeineUnitTestInstance {

        @DisplayName("that what an adapter was given is what it, and the parts it is made of, answer with")
        @Test
        void test_Aware_answers_with_what_it_was_given() {
            MongoClient mongoClient = mock(MongoClient.class, RETURNS_DEEP_STUBS);
            MongoAdapter<Key, Value> adapter = MongoAdapter
                    .newBuilder(mongoClient, "database", "collection")
                    .withDiscriminator("d1")
                    .build();
            Serializer<Key, ?> keySerializer = new JavaObjectSerializer<>();
            Serializer<Value, ?> valueSerializer = new JacksonSerializer<>(Value.class, false);
            adapter.setKeySerializer(keySerializer);
            adapter.setValueSerializer(valueSerializer);

            assertThat(adapter.getIdentifier()).isEqualTo("mongodb:database:collection:d1");
            assertThat(adapter.getDiscriminator()).isEqualTo("d1");
            assertThat(adapter.getKeySerializer()).isSameAs(keySerializer);
            assertThat(adapter.getValueSerializer()).isSameAs(valueSerializer);

            // and the parts it handed them down to say the same, which is what being told them is for: before
            // these getters existed nothing could be asked what it had been wired with
            assertThat(adapter.getPublisher().getIdentifier()).isEqualTo(adapter.getIdentifier());
            assertThat(adapter.getPublisher().getDiscriminator()).isEqualTo("d1");
            assertThat(adapter.getPublisher().getKeySerializer()).isSameAs(keySerializer);
            assertThat(adapter.getPublisher().getValueSerializer()).isSameAs(valueSerializer);

            // including the one an adapter answers from rather than keeping: an adapter holds no serializer of
            // its own, so a serializer set on it afterwards has to be what it reports
            Serializer<Value, ?> replacement = new JacksonSerializer<>(Value.class, true);
            adapter.setValueSerializer(replacement);
            assertThat(adapter.getValueSerializer()).isSameAs(replacement);
        }
    }

    @Nested
    @DisplayName("Test StoreGuard")
    @SuppressWarnings("java:S5778")
    final class StoreGuardUnit extends DistributedCaffeineUnitTestInstance {

        private static final String IDENTIFIER = "store:scope:default";
        // what the guard is built with, restated here so that a change to either is a failing test rather than a
        // test that quietly stops covering the threshold it was written for
        private static final int FAILURE_THRESHOLD = 3;

        @DisplayName("that the store is left alone after a run of failures, and asked again once it answers")
        @Test
        void test_StoreGuard_stops_asking_a_store_that_keeps_failing() {
            InternalStoreGuard storeGuard = new InternalStoreGuard();

            // short of the threshold the store is still asked, and what comes back is what it raised
            for (int attempt = 1; attempt < FAILURE_THRESHOLD; attempt++) {
                assertThatThrownBy(() -> storeGuard.runGuarded(IDENTIFIER, failing()))
                        .as("attempt %d, which is still asking", attempt)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("provoked");
            }
            assertThat(storeGuard.isStoreUnreachable()).isFalse();

            assertThatThrownBy(() -> storeGuard.runGuarded(IDENTIFIER, failing()))
                    .hasMessage("provoked");

            // and from here it is not asked at all, which is said in terms of the store rather than of Failsafe
            assertThat(storeGuard.isStoreUnreachable()).isTrue();
            AtomicBoolean asked = new AtomicBoolean();
            assertThatThrownBy(() -> storeGuard.runGuarded(IDENTIFIER, () -> asked.set(true)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(IDENTIFIER)
                    .hasMessageContaining("because the last 3 attempts to contact it failed");
            assertThat(asked).isFalse();

            // being told it answers is enough, which is what the maintenance worker reports once a minute
            storeGuard.reportReachable();

            assertThat(storeGuard.isStoreUnreachable()).isFalse();
            assertThatCode(() -> storeGuard.runGuarded(IDENTIFIER, () -> asked.set(true)))
                    .doesNotThrowAnyException();
            assertThat(asked).isTrue();
        }

        @DisplayName("that a caller asking the store itself is never refused, while its answer still counts")
        @Test
        void test_StoreGuard_observes_without_refusing() throws Throwable {
            InternalStoreGuard storeGuard = new InternalStoreGuard();

            // an observed failure counts like any other: what a caller reached the store for is theirs to be
            // told, and the outcome is evidence all the same
            for (int attempt = 0; attempt < FAILURE_THRESHOLD; attempt++) {
                assertThatThrownBy(() -> storeGuard.observed(failingSupplier()))
                        .hasMessage("provoked");
            }

            // which the guarded side then acts on, and that is the whole reason for there being one of these
            assertThat(storeGuard.isStoreUnreachable()).isTrue();
            assertThatThrownBy(() -> storeGuard.runGuarded(IDENTIFIER, () -> {
            })).hasMessageContaining("because the last 3 attempts to contact it failed");

            // while the observing side is still carried out, however little the guard thinks of the store
            AtomicBoolean asked = new AtomicBoolean();
            assertThat(storeGuard.observed(() -> {
                asked.set(true);
                return "answered";
            })).isEqualTo("answered");
            assertThat(asked).isTrue();

            // and an answer is better evidence than anything the guard could gather on its own, so it ends there
            assertThat(storeGuard.isStoreUnreachable()).isFalse();
        }

        @DisplayName("that a caller is told what the store raised, whether or not it had to be carried")
        @Test
        void test_StoreGuard_hands_back_what_the_store_raised() {
            InternalStoreGuard storeGuard = new InternalStoreGuard();

            // Failsafe carries a checked exception out wrapped and an unchecked one as it is, and a caller is
            // owed the difference: what wraps a store failure is decided where it is caught, not here
            Exception checked = new Exception("checked");
            assertThatThrownBy(() -> storeGuard.runGuarded(IDENTIFIER, () -> {
                throw checked;
            })).isSameAs(checked);

            RuntimeException unchecked = new IllegalArgumentException("unchecked");
            assertThatThrownBy(() -> storeGuard.getGuarded(IDENTIFIER, () -> {
                throw unchecked;
            })).isSameAs(unchecked);
        }

        private InternalUtils.FailableRunnable failing() {
            return () -> {
                throw new IllegalStateException("provoked");
            };
        }

        private InternalUtils.FailableSupplier<String> failingSupplier() {
            return () -> {
                throw new IllegalStateException("provoked");
            };
        }
    }

    // the adapter classes under test are package-private in their own package, so the tests above reach them
    // by name rather than by reference
    // how often a mock of a type that cannot be named here was called by the given method
    private static long invocationsOf(Object mock, String methodName) {
        return mockingDetails(mock).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals(methodName))
                .count();
    }

    private static Class<?> classOf(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    abstract static class DistributedCaffeineUnitTestInstance extends DistributedCaffeineCommonTestInstance {
    }
}
