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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.Policy.VarExpiration;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.adapter.Retriever;
import io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoAdapter;
import io.github.oberhoff.distributedcaffeine.common.DistributedCaffeineCommonTestInstance;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import io.github.oberhoff.distributedcaffeine.common.logging.CaptureLogger;
import io.github.oberhoff.distributedcaffeine.common.logging.CaptureLoggerFactory;
import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.ForySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JavaObjectSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.assertj.core.api.AbstractLongAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.platform.commons.util.ReflectionUtils.HierarchyTraversalMode;
import org.slf4j.event.Level;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.core.type.TypeReference;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributedCaffeineIntegrationTests.DistributedCaffeineIntegrationTestInstance.DockerImage;
import static io.github.oberhoff.distributedcaffeine.DistributedCaffeineIntegrationTests.DistributedCaffeineIntegrationTestInstance.RUNS_ON_GITHUB;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.entry;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.runFailable;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_LOADED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_EXTENDED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.SHORT_LIVING_GROUP;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.time.temporal.ChronoUnit.FOREVER;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.ParameterizedInvocationConstants.ARGUMENTS_WITH_NAMES_PLACEHOLDER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@DisplayName("Distributed Caffeine Integration Test Suite")
final class DistributedCaffeineIntegrationTests {

    @Nested
    @DisplayName("MongoDB 4.2.0")
    @DockerImage("mongo:4.2.0") // oldest version supported by the latest driver
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_4_2_0 extends DistributedCaffeineIntegration {
    }

    @Nested
    @DisplayName("MongoDB 4.latest")
    @DockerImage("mongo:4")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_4_latest extends DistributedCaffeineIntegration {
    }

    @Nested
    @DisplayName("MongoDB 5.0.0")
    @DockerImage("mongo:5.0.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_5_0_0 extends DistributedCaffeineIntegration {
    }

    @Nested
    @DisplayName("MongoDB 5.latest")
    @DockerImage("mongo:5")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_5_latest extends DistributedCaffeineIntegration {
    }

    @Nested
    @DisplayName("MongoDB 6.0.1")
    @DockerImage("mongo:6.0.1") // mongo:6.0.0 is not available
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_6_0_1 extends DistributedCaffeineIntegration {
    }

    @Nested
    @DisplayName("MongoDB 6.latest")
    @DockerImage("mongo:6")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_6_latest extends DistributedCaffeineIntegration {
    }

    @Nested
    @DisplayName("MongoDB 7.0.0")
    @DockerImage("mongo:7.0.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_7_0_0 extends DistributedCaffeineIntegration {
    }

    @Nested
    @DisplayName("MongoDB 7.latest")
    @DockerImage("mongo:7")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_7_latest extends DistributedCaffeineIntegration {
    }

    @Nested
    @DisplayName("MongoDB 8.0.0")
    @DockerImage("mongo:8.0.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_8_0_0 extends DistributedCaffeineIntegration {
    }

    @Nested
    @DisplayName("MongoDB 8.latest")
    @DockerImage("mongo:8")
    final class Mongo_8_latest extends DistributedCaffeineIntegration {
    }

    @SuppressWarnings({"java:S5838", "java:S5778", "java:S5961", "ResultOfMethodCallIgnored"})
    abstract static class DistributedCaffeineIntegration extends DistributedCaffeineIntegrationTestInstance {

        @DisplayName("Test put() and getIfPresent()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedCache_put_getIfPresent(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
            Set<Cache<Key, Value>> featureParityCaches = Set.of(distributedCache, caffeineCache);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            featureParityCaches.forEach(cache -> {
                assertThatNullPointerException().isThrownBy(() -> cache.put(_null(), Value.of(0)));
                assertThatNullPointerException().isThrownBy(() -> cache.put(Key.of(0), _null()));
                assertThatNullPointerException().isThrownBy(() -> cache.getIfPresent(_null()));

                cache.put(key1, value1);
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(cache -> {
                            assertThat(cache.estimatedSize()).isEqualTo(1);
                            assertThat(cache.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(cache.getIfPresent(Key.of(0))).isNull();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
        }

        @DisplayName("Test putAll() and getAllPresent()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedCache_putAll_getAllPresent(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
            Set<Cache<Key, Value>> featureParityCaches = Set.of(distributedCache, caffeineCache);

            Map<Key, Value> map1to2 = Map.of(
                    Key.of(1), Value.of(1),
                    Key.of(2), Value.of(2));

            featureParityCaches.forEach(cache -> {
                assertThatNullPointerException().isThrownBy(() -> cache.putAll(_null()));
                assertThatNullPointerException().isThrownBy(() -> cache.putAll(_map(null, Value.of(0))));
                assertThatNullPointerException().isThrownBy(() -> cache.putAll(_map(Key.of(0), _null())));
                assertThatNullPointerException().isThrownBy(() -> cache.getAllPresent(_null()));
                assertThatNullPointerException().isThrownBy(() -> cache.getAllPresent(_set(null)));

                cache.putAll(map1to2);
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(cache -> {
                            assertThat(cache.estimatedSize()).isEqualTo(2);
                            assertThat(cache.getAllPresent(map1to2.keySet()))
                                    .containsAllEntriesOf(map1to2)
                                    .hasSize(2)
                                    .isUnmodifiable();
                            assertThat(cache.getAllPresent(Set.of(Key.of(0)))).isEmpty();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
        }

        @DisplayName("Test get() and getAll()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedCache_get_getAll(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
            Set<Cache<Key, Value>> featureParityCaches = Set.of(distributedCache, caffeineCache);

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);
            Set<Key> keys3to4 = Set.of(Key.of(3), Key.of(4));
            Set<Key> keys5to6 = Set.of(Key.of(5), Key.of(6));
            Set<Key> keys7to8 = Set.of(Key.of(7), Key.of(8));

            EqualResult<Key, Value> computedValue1 = new EqualResult<>();
            EqualResult<Key, Value> computedValue2 = new EqualResult<>();
            EqualResult<Key, Value> computedMap3to4 = new EqualResult<>();
            EqualResult<Key, Value> computedMap5to6 = new EqualResult<>();

            featureParityCaches.forEach(cache -> {
                assertThatNullPointerException().isThrownBy(() -> cache.get(_null(), key -> Value.of(0)));
                assertThatNullPointerException().isThrownBy(() -> cache.get(Key.of(0), _null()));
                // cache.get(Key.of(0), key -> null)) is allowed and tested below
                assertThatNullPointerException().isThrownBy(() -> cache.getAll(_null(), keys -> _map(Key.of(0), Value.of(0))));
                assertThatNullPointerException().isThrownBy(() -> cache.getAll(_set(Key.of(0)), _null()));
                assertThatNullPointerException().isThrownBy(() -> cache.getAll(_set(null), keys -> _map(Key.of(0), Value.of(0))));
                assertThatNullPointerException().isThrownBy(() -> cache.getAll(_set(Key.of(0)), keys -> _map(null, Value.of(0))));
                assertThatNullPointerException().isThrownBy(() -> cache.getAll(_set(Key.of(0)), keys -> _map(Key.of(0), _null())));
                assertThatThrownBy(() -> cache.get(Key.of(0, "unchecked"), key -> {
                    throw new IllegalStateException("unchecked");
                })).isExactlyInstanceOf(IllegalStateException.class)
                        .hasMessage("unchecked");
                assertThatThrownBy(() -> cache.getAll(Set.of(Key.of(0, "unchecked")), keys -> {
                    throw new IllegalStateException("unchecked");
                })).isExactlyInstanceOf(IllegalStateException.class)
                        .hasMessage("unchecked");

                computedValue1.setValue(cache.get(key1, key -> Value.of(key.getId(), "computed")));
                computedValue2.setValue(cache.get(key2, key -> null));
                computedMap3to4.setMap(cache.getAll(keys3to4, keys -> keys.stream()
                        .collect(toMap(Function.identity(), key -> Value.of(key.getId(), "computed")))));
                // return more entries than requested keys (which are cached additionally but not returned by getAll())
                computedMap5to6.setMap(cache.getAll(keys5to6, keys -> Stream.concat(keys.stream(), keys7to8.stream())
                        .collect(toMap(Function.identity(), key -> Value.of(key.getId(), "computed")))));
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(cache -> {
                            assertThat(cache.estimatedSize()).isEqualTo(7);
                            assertThat(cache.getIfPresent(key1)).isEqualTo(computedValue1.getValue())
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("computed"));
                            assertThat(cache.getIfPresent(key2)).isEqualTo(computedValue2.getValue())
                                    .isNull();
                            assertThat(cache.getAllPresent(keys3to4))
                                    .containsAllEntriesOf(computedMap3to4.getMap())
                                    .hasSize(2)
                                    .isUnmodifiable()
                                    .values()
                                    .allSatisfy(value -> assertThat(value.getName()).isEqualTo("computed"));
                            assertThat(cache.getAllPresent(keys5to6))
                                    // only entries for requested keys were returned by getAll() and stored in the map
                                    .containsAllEntriesOf(computedMap5to6.getMap())
                                    .hasSize(2)
                                    .isUnmodifiable()
                                    .values()
                                    .allSatisfy(value -> assertThat(value.getName()).isEqualTo("computed"));
                            // check additionally cached entries
                            assertThat(cache.getAllPresent(keys7to8))
                                    .containsOnlyKeys(keys7to8)
                                    .hasSize(2)
                                    .isUnmodifiable()
                                    .values()
                                    .allSatisfy(value -> assertThat(value.getName()).isEqualTo("computed"));
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(7)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(7)));
        }

        @DisplayName("Test invalidate() and invalidateAll()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedCache_invalidate_invalidateAll(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
            Set<Cache<Key, Value>> featureParityCaches = Set.of(distributedCache, caffeineCache);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Map<Key, Value> map2to3 = Map.of(
                    Key.of(2), Value.of(2),
                    Key.of(3), Value.of(3));
            Map<Key, Value> map4to5 = Map.of(
                    Key.of(4), Value.of(4),
                    Key.of(5), Value.of(5));

            featureParityCaches.forEach(cache -> {
                assertThatNullPointerException().isThrownBy(() -> cache.invalidate(_null()));
                assertThatNullPointerException().isThrownBy(() -> cache.invalidateAll(_null()));
                assertThatNullPointerException().isThrownBy(() -> cache.invalidateAll(_set(null)));

                cache.put(key1, value1);
                cache.putAll(map2to3);
                cache.putAll(map4to5);
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(cache -> {
                            assertThat(cache.estimatedSize()).isEqualTo(5);
                            assertThat(cache.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(cache.getAllPresent(map2to3.keySet()))
                                    .containsAllEntriesOf(map2to3);
                            assertThat(cache.getAllPresent(map4to5.keySet()))
                                    .containsAllEntriesOf(map4to5);
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(5)));
                    });

            featureParityCaches.forEach(cache ->
                    cache.invalidate(key1));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(cache -> {
                            assertThat(cache.estimatedSize()).isEqualTo(4);
                            assertThat(cache.getIfPresent(key1)).isNull();
                            assertThat(cache.getAllPresent(map2to3.keySet()))
                                    .containsAllEntriesOf(map2to3);
                            assertThat(cache.getAllPresent(map4to5.keySet()))
                                    .containsAllEntriesOf(map4to5);
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(4)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(1)));
                    });

            featureParityCaches.forEach(cache ->
                    cache.invalidateAll(map2to3.keySet()));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(cache -> {
                            assertThat(cache.estimatedSize()).isEqualTo(2);
                            assertThat(cache.getIfPresent(key1)).isNull();
                            assertThat(cache.getAllPresent(map2to3.keySet())).isEmpty();
                            assertThat(cache.getAllPresent(map4to5.keySet()))
                                    .containsAllEntriesOf(map4to5);
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(2)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(3)));
                    });

            featureParityCaches.forEach(Cache::invalidateAll);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(cache -> {
                            assertThat(cache.estimatedSize()).isEqualTo(0);
                            assertThat(cache.getIfPresent(key1)).isNull();
                            assertThat(cache.getAllPresent(map2to3.keySet())).isEmpty();
                            assertThat(cache.getAllPresent(map4to5.keySet())).isEmpty();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(5)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.empty());
        }

        @DisplayName("Test get() with cache loader")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedLoadingCache_get_with_cache_loader(CacheFactory<Key, Value> cacheFactory) throws Exception {
            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(Key key) throws Exception {
                    throw new UnsupportedOperationException();
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    dc -> dc.build(cacheLoader));
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    dc -> dc.build(cacheLoader));
            LoadingCache<Key, Value> caffeineLoadingCache = Caffeine.newBuilder()
                    .build(cacheLoader);

            Set<LoadingCache<Key, Value>> allCaches = Set.of(distributedLoadingCache, syncedDistributedLoadingCache, caffeineLoadingCache);
            Set<LoadingCache<Key, Value>> featureParityCaches = Set.of(distributedLoadingCache, caffeineLoadingCache);

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);
            Set<Key> keys3to4 = Set.of(Key.of(3), Key.of(4));

            EqualResult<Key, Value> loadedValue1 = new EqualResult<>();
            EqualResult<Key, Value> loadedValue2 = new EqualResult<>();
            EqualResult<Key, Value> loadedMap2to3 = new EqualResult<>();

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded"))
                    .when(cacheLoader).load(any(Key.class));
            doAnswer(invocation -> null)
                    .when(cacheLoader).load(key2);
            doThrow(new Exception("checked"))
                    .when(cacheLoader).load(Key.of(0, "checked"));
            doThrow(new IllegalStateException("unchecked"))
                    .when(cacheLoader).load(Key.of(0, "unchecked"));

            InternalCacheLoader<Key, Value> internalCacheLoader = getInstanceRegistry(distributedLoadingCache).getCacheLoader();
            assertThatExceptionOfType(IllegalAccessException.class).isThrownBy(() -> internalCacheLoader.asyncLoad(_null(), _null()));
            assertThatExceptionOfType(IllegalAccessException.class).isThrownBy(() -> internalCacheLoader.asyncLoadAll(_null(), _null()));
            assertThatExceptionOfType(IllegalAccessException.class).isThrownBy(() -> internalCacheLoader.reload(_null(), _null()));

            featureParityCaches.forEach(loadingCache -> {
                assertThatNullPointerException().isThrownBy(() -> loadingCache.get(_null()));
                assertThatThrownBy(() -> loadingCache.get(Key.of(0, "checked")))
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasRootCauseExactlyInstanceOf(Exception.class)
                        .hasMessage("java.lang.Exception: checked");
                assertThatThrownBy(() -> loadingCache.get(Key.of(0, "unchecked")))
                        .isExactlyInstanceOf(IllegalStateException.class)
                        .hasMessage("unchecked");
                assertThatThrownBy(() -> loadingCache.getAll(Set.of(Key.of(0, "checked"))))
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasRootCauseExactlyInstanceOf(Exception.class)
                        .hasMessage("java.lang.Exception: checked");
                assertThatThrownBy(() -> loadingCache.getAll(Set.of(Key.of(0, "unchecked"))))
                        .isExactlyInstanceOf(IllegalStateException.class)
                        .hasMessage("unchecked");

                loadedValue1.setValue(loadingCache.get(key1));
                loadedValue2.setValue(loadingCache.get(key2));
                loadedMap2to3.setMap(loadingCache.getAll(keys3to4));
            });

            verify(cacheLoader, times(16)).load(any(Key.class));
            verifyNoMoreInteractions(cacheLoader);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            assertThat(loadingCache.estimatedSize()).isEqualTo(3);
                            assertThat(loadingCache.getIfPresent(key1)).isEqualTo(loadedValue1.getValue())
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("loaded"));
                            assertThat(loadingCache.getIfPresent(key2)).isEqualTo(loadedValue2.getValue())
                                    .isNull();
                            assertThat(loadingCache.getAllPresent(keys3to4))
                                    .containsAllEntriesOf(loadedMap2to3.getMap())
                                    .hasSize(2)
                                    .isUnmodifiable()
                                    .values()
                                    .allSatisfy(value -> assertThat(value.getName()).isEqualTo("loaded"));
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(3)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(3)));
        }

        @DisplayName("Test getAll() with cache loader")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedLoadingCache_getAll_with_cache_loader(CacheFactory<Key, Value> cacheFactory) throws Exception {
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(Key key) throws Exception {
                    throw new UnsupportedOperationException(); // ensure load() is never invoked
                }

                @Override
                @SuppressWarnings("RedundantThrows")
                public Map<? extends Key, ? extends Value> loadAll(Set<? extends Key> keys) throws Exception {
                    throw new UnsupportedOperationException(); // override loadAll() explicitly
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    dc -> dc.build(cacheLoader));
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    dc -> dc.build(cacheLoader));
            LoadingCache<Key, Value> caffeineLoadingCache = Caffeine.newBuilder()
                    .build(cacheLoader);

            Set<LoadingCache<Key, Value>> allCaches = Set.of(distributedLoadingCache, syncedDistributedLoadingCache, caffeineLoadingCache);
            Set<LoadingCache<Key, Value>> featureParityCaches = Set.of(distributedLoadingCache, caffeineLoadingCache);

            Set<Key> keys1to2 = Set.of(Key.of(1), Key.of(2));
            Set<Key> keys3to4 = Set.of(Key.of(3), Key.of(4));
            Set<Key> keys5to6 = Set.of(Key.of(5), Key.of(6));

            EqualResult<Key, Value> loadedMap1to2 = new EqualResult<>();
            EqualResult<Key, Value> loadedMap3to4 = new EqualResult<>();

            doAnswer(invocation -> invocation.<Set<Key>>getArgument(0).stream()
                    .collect(toMap(Function.identity(), key -> Value.of(key.getId(), "loaded"))))
                    .when(cacheLoader).loadAll(keys1to2);
            // return more entries than requested keys (which are cached additionally but not returned by getAll())
            doAnswer(invocation -> Stream.concat(invocation.<Set<Key>>getArgument(0).stream(), keys5to6.stream())
                    .collect(toMap(Function.identity(), key -> Value.of(key.getId(), "loaded"))))
                    .when(cacheLoader).loadAll(keys3to4);
            doAnswer(invocation -> _map(null, Value.of(0)))
                    .when(cacheLoader).loadAll(Set.of(Key.of(0, "null key")));
            doAnswer(invocation -> _map(Key.of(0), null))
                    .when(cacheLoader).loadAll(Set.of(Key.of(0, "null value")));
            doThrow(new Exception("checked"))
                    .when(cacheLoader).loadAll(Set.of(Key.of(0, "checked")));
            doThrow(new IllegalStateException("unchecked"))
                    .when(cacheLoader).loadAll(Set.of(Key.of(0, "unchecked")));

            InternalCacheLoader<Key, Value> internalCacheLoader = getInstanceRegistry(distributedLoadingCache).getCacheLoader();
            assertThatExceptionOfType(IllegalAccessException.class).isThrownBy(() -> internalCacheLoader.asyncLoad(_null(), _null()));
            assertThatExceptionOfType(IllegalAccessException.class).isThrownBy(() -> internalCacheLoader.asyncLoadAll(_null(), _null()));
            assertThatExceptionOfType(IllegalAccessException.class).isThrownBy(() -> internalCacheLoader.reload(_null(), _null()));

            featureParityCaches.forEach(loadingCache -> {
                assertThatNullPointerException().isThrownBy(() -> loadingCache.get(_null()));
                assertThatNullPointerException().isThrownBy(() -> loadingCache.getAll(_null()));
                assertThatNullPointerException().isThrownBy(() -> loadingCache.getAll(_set(null)));
                assertThatNullPointerException().isThrownBy(() -> loadingCache.getAll(Set.of(Key.of(0, "null key"))));
                assertThatNullPointerException().isThrownBy(() -> loadingCache.getAll(Set.of(Key.of(0, "null value"))));
                assertThatThrownBy(() -> loadingCache.getAll(Set.of(Key.of(0, "checked"))))
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasRootCauseExactlyInstanceOf(Exception.class)
                        .hasMessage("java.lang.Exception: checked");
                assertThatThrownBy(() -> loadingCache.getAll(Set.of(Key.of(0, "unchecked"))))
                        .isExactlyInstanceOf(IllegalStateException.class)
                        .hasMessage("unchecked");

                loadedMap1to2.setMap(loadingCache.getAll(keys1to2));
                loadedMap3to4.setMap(loadingCache.getAll(keys3to4));
            });

            verify(cacheLoader, times(12)).loadAll(anySet());
            verifyNoMoreInteractions(cacheLoader);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            assertThat(loadingCache.estimatedSize()).isEqualTo(6);
                            assertThat(loadingCache.getAllPresent(keys1to2))
                                    .containsAllEntriesOf(loadedMap1to2.getMap())
                                    .hasSize(2)
                                    .isUnmodifiable()
                                    .values()
                                    .allSatisfy(value -> assertThat(value.getName()).isEqualTo("loaded"));
                            assertThat(loadingCache.getAllPresent(keys3to4))
                                    // only entries for requested keys were returned by getAll() and stored in the map
                                    .containsAllEntriesOf(loadedMap3to4.getMap())
                                    .hasSize(2)
                                    .isUnmodifiable()
                                    .values()
                                    .allSatisfy(value -> assertThat(value.getName()).isEqualTo("loaded"));
                            // check additionally cached entries
                            assertThat(loadingCache.getAllPresent(keys5to6))
                                    .containsOnlyKeys(keys5to6)
                                    .hasSize(2)
                                    .isUnmodifiable()
                                    .values()
                                    .allSatisfy(value -> assertThat(value.getName()).isEqualTo("loaded"));
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(6)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(6)));
        }

        @DisplayName("Test refresh() with cache loader")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_DistributedLoadingCache_refresh_with_cache_loader(CacheFactory<Key, Value> cacheFactory) throws Exception {
            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(Key key) throws Exception {
                    throw new UnsupportedOperationException();
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    dc -> dc.build(cacheLoader));
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    dc -> dc.build(cacheLoader));
            LoadingCache<Key, Value> caffeineLoadingCache = Caffeine.newBuilder()
                    .build(cacheLoader);

            Set<LoadingCache<Key, Value>> allCaches = Set.of(distributedLoadingCache, syncedDistributedLoadingCache, caffeineLoadingCache);
            Set<LoadingCache<Key, Value>> featureParityCaches = Set.of(distributedLoadingCache, caffeineLoadingCache);

            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);
            CaptureLogger loggerLocalLoadingCache = CaptureLoggerFactory
                    .getCaptureLogger("com.github.benmanes.caffeine.cache.LocalLoadingCache");

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);

            EqualResult<Key, Value> refreshedValue1 = new EqualResult<>();
            EqualResult<Key, Value> refreshedValue2 = new EqualResult<>();

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded"))
                    .when(cacheLoader).load(key1);
            doAnswer(invocation -> null)
                    .when(cacheLoader).load(key2);
            doThrow(new Exception("checked"))
                    .when(cacheLoader).load(Key.of(0, "checked"));
            doThrow(new IllegalStateException("unchecked"))
                    .when(cacheLoader).load(Key.of(0, "unchecked"));

            loggerDistributedCaffeine.startCapturing();
            loggerLocalLoadingCache.startCapturing();

            featureParityCaches.forEach(loadingCache -> {
                assertThatNullPointerException().isThrownBy(() -> loadingCache.refresh(_null()));
                assertThatThrownBy(() -> loadingCache.refresh(Key.of(0, "checked")).join())
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasRootCauseExactlyInstanceOf(Exception.class)
                        .hasMessage("java.lang.Exception: checked");
                assertThatThrownBy(() -> loadingCache.refresh(Key.of(0, "unchecked")).join())
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasCauseInstanceOf(IllegalStateException.class)
                        .hasMessage("java.lang.IllegalStateException: unchecked");

                refreshedValue1.setValue(loadingCache.refresh(key1).join());
                refreshedValue2.setValue(loadingCache.refresh(key2).join());
            });

            verify(cacheLoader, times(8)).load(any(Key.class));
            verify(cacheLoader, times(8)).asyncLoad(any(Key.class), any(Executor.class));
            verifyNoMoreInteractions(cacheLoader);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            assertThat(loadingCache.estimatedSize()).isEqualTo(1);
                            assertThat(loadingCache.getIfPresent(key1)).isEqualTo(refreshedValue1.getValue())
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("loaded"));
                            assertThat(loadingCache.getIfPresent(key2)).isEqualTo(refreshedValue2.getValue())
                                    .isNull();
                        });
                        Stream.of(loggerDistributedCaffeine, loggerLocalLoadingCache).forEach(logger -> {
                            String message = "Exception thrown during refresh";
                            assertThat(logger.getLoggingEvents()).hasSize(2)
                                    .satisfiesOnlyOnce(loggingEvent -> {
                                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                        assertThat(loggingEvent.getMessage()).startsWith(message);
                                        assertThat(loggingEvent.getThrowable())
                                                .isExactlyInstanceOf(CompletionException.class)
                                                .hasRootCauseExactlyInstanceOf(Exception.class)
                                                .hasMessage("java.lang.Exception: checked");
                                    })
                                    .satisfiesOnlyOnce(loggingEvent -> {
                                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                        assertThat(loggingEvent.getMessage()).startsWith(message);
                                        assertThat(loggingEvent.getThrowable())
                                                .isExactlyInstanceOf(CompletionException.class)
                                                .hasCauseInstanceOf(IllegalStateException.class)
                                                .hasMessage("java.lang.IllegalStateException: unchecked");
                                    });
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED_REFRESHED, assertion -> assertion.isEqualTo(1)));
                    });

            loggerDistributedCaffeine.stopCapturing();
            loggerLocalLoadingCache.stopCapturing();

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "reloaded"))
                    .when(cacheLoader).load(key1);

            loggerDistributedCaffeine.startCapturing();
            loggerLocalLoadingCache.startCapturing();

            refreshedValue1.reset();
            featureParityCaches.forEach(loadingCache -> {
                loadingCache.put(Key.of(0, "checked"), Value.of(0, "checked"));
                loadingCache.put(Key.of(0, "unchecked"), Value.of(0, "unchecked"));
                assertThatThrownBy(() -> loadingCache.refresh(Key.of(0, "checked")).join())
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasRootCauseExactlyInstanceOf(Exception.class)
                        .hasMessage("java.lang.Exception: checked");
                assertThatThrownBy(() -> loadingCache.refresh(Key.of(0, "unchecked")).join())
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasCauseInstanceOf(IllegalStateException.class)
                        .hasMessage("java.lang.IllegalStateException: unchecked");
                loadingCache.invalidateAll(Set.of(Key.of(0, "checked"), Key.of(0, "unchecked")));

                refreshedValue1.setValue(loadingCache.refresh(key1).join());
            });

            verify(cacheLoader, times(14)).load(any(Key.class));
            verify(cacheLoader, times(8)).asyncLoad(any(Key.class), any(Executor.class));
            verify(cacheLoader, times(6)).reload(any(Key.class), any(Value.class));
            verify(cacheLoader, times(6)).asyncReload(any(Key.class), any(Value.class), any(Executor.class));
            verifyNoMoreInteractions(cacheLoader);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            assertThat(loadingCache.estimatedSize()).isEqualTo(1);
                            assertThat(loadingCache.getIfPresent(key1)).isEqualTo(refreshedValue1.getValue())
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("reloaded"));
                        });
                        Stream.of(loggerDistributedCaffeine, loggerLocalLoadingCache).forEach(logger -> {
                            String message = "Exception thrown during refresh";
                            assertThat(logger.getLoggingEvents()).hasSize(2)
                                    .satisfiesOnlyOnce(loggingEvent -> {
                                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                        assertThat(loggingEvent.getMessage()).startsWith(message);
                                        assertThat(loggingEvent.getThrowable())
                                                .isExactlyInstanceOf(CompletionException.class)
                                                .hasRootCauseExactlyInstanceOf(Exception.class)
                                                .hasMessage("java.lang.Exception: checked");
                                    })
                                    .satisfiesOnlyOnce(loggingEvent -> {
                                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                        assertThat(loggingEvent.getMessage()).startsWith(message);
                                        assertThat(loggingEvent.getThrowable())
                                                .isExactlyInstanceOf(CompletionException.class)
                                                .hasCauseInstanceOf(IllegalStateException.class)
                                                .hasMessage("java.lang.IllegalStateException: unchecked");
                                    });
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED_REFRESHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(2)));
                    });

            loggerDistributedCaffeine.stopCapturing();
            loggerLocalLoadingCache.stopCapturing();

            doAnswer(invocation -> null)
                    .when(cacheLoader).load(key1);

            refreshedValue1.reset();
            featureParityCaches.forEach(loadingCache ->
                    refreshedValue1.setValue(loadingCache.refresh(key1).join()));

            verify(cacheLoader, times(16)).load(any(Key.class));
            verify(cacheLoader, times(8)).asyncLoad(any(Key.class), any(Executor.class));
            verify(cacheLoader, times(8)).reload(any(Key.class), any(Value.class));
            verify(cacheLoader, times(8)).asyncReload(any(Key.class), any(Value.class), any(Executor.class));
            verifyNoMoreInteractions(cacheLoader);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            assertThat(loadingCache.estimatedSize()).isEqualTo(0);
                            assertThat(loadingCache.getIfPresent(key1)).isEqualTo(refreshedValue1.getValue())
                                    .isNull();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(INVALIDATED_REFRESHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(2)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.empty());

            // test sharing of refresh operations
            int levelOfParallelism = 10;
            AtomicInteger counter = new AtomicInteger(0);

            doAnswer(invocation -> {
                sleep(Duration.ofMillis(100));
                return Value.of(counter.incrementAndGet(), "counted");
            }).when(cacheLoader).load(key1);

            featureParityCaches.forEach(loadingCache -> {
                loadingCache.invalidate(key1);
                IntStream.rangeClosed(1, levelOfParallelism)
                        .mapToObj(i -> loadingCache.refresh(key1))
                        .toList() // intermediate step to ensure concurrency
                        .forEach(CompletableFuture::join);
                counter.set(0);
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            allCaches.forEach(loadingCache ->
                                    assertThat(loadingCache.getIfPresent(key1))
                                            .isNotNull()
                                            .satisfies(value -> assertThat(requireNonNull(value).getId()).isLessThan(levelOfParallelism))
                                            .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("counted"))));
        }

        @DisplayName("Test refreshAll() with cache loader")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_DistributedLoadingCache_refreshAll_with_cache_loader(CacheFactory<Key, Value> cacheFactory) throws Exception {
            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(Key key) throws Exception {
                    throw new UnsupportedOperationException();
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    dc -> dc.build(cacheLoader));
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    dc -> dc.build(cacheLoader));
            LoadingCache<Key, Value> caffeineLoadingCache = Caffeine.newBuilder()
                    .build(cacheLoader);

            Set<LoadingCache<Key, Value>> allCaches = Set.of(distributedLoadingCache, syncedDistributedLoadingCache, caffeineLoadingCache);
            Set<LoadingCache<Key, Value>> featureParityCaches = Set.of(distributedLoadingCache, caffeineLoadingCache);

            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);
            CaptureLogger loggerLocalLoadingCache = CaptureLoggerFactory
                    .getCaptureLogger("com.github.benmanes.caffeine.cache.LocalLoadingCache");

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);
            Set<Key> keys1 = Set.of(key1);
            Set<Key> keys2 = Set.of(key2);

            EqualResult<Key, Value> refreshedMap1 = new EqualResult<>();
            EqualResult<Key, Value> refreshedMap2 = new EqualResult<>();

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded"))
                    .when(cacheLoader).load(key1);
            doAnswer(invocation -> null)
                    .when(cacheLoader).load(key2);
            doThrow(new Exception("checked"))
                    .when(cacheLoader).load(Key.of(0, "checked"));
            doThrow(new IllegalStateException("unchecked"))
                    .when(cacheLoader).load(Key.of(0, "unchecked"));

            loggerDistributedCaffeine.startCapturing();
            loggerLocalLoadingCache.startCapturing();

            featureParityCaches.forEach(loadingCache -> {
                assertThatNullPointerException().isThrownBy(() -> loadingCache.refreshAll(_null()));
                assertThatNullPointerException().isThrownBy(() -> loadingCache.refreshAll(_set(null)));
                assertThatThrownBy(() -> loadingCache.refreshAll(Set.of(Key.of(0, "checked"))).join())
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasRootCauseExactlyInstanceOf(Exception.class)
                        .hasMessage("java.lang.Exception: checked");
                assertThatThrownBy(() -> loadingCache.refreshAll(Set.of(Key.of(0, "unchecked"))).join())
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasCauseInstanceOf(IllegalStateException.class)
                        .hasMessage("java.lang.IllegalStateException: unchecked");

                refreshedMap1.setMap(loadingCache.refreshAll(keys1).join());
                refreshedMap2.setMap(loadingCache.refreshAll(keys2).join());
            });

            verify(cacheLoader, times(8)).load(any(Key.class));
            verify(cacheLoader, times(8)).asyncLoad(any(Key.class), any(Executor.class));
            verifyNoMoreInteractions(cacheLoader);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            assertThat(loadingCache.estimatedSize()).isEqualTo(1);
                            assertThat(loadingCache.getAllPresent(keys1))
                                    .containsAllEntriesOf(refreshedMap1.getMap())
                                    .hasSize(1)
                                    .isUnmodifiable()
                                    .values()
                                    .allSatisfy(value -> assertThat(value.getName()).isEqualTo("loaded"));
                            assertThat(loadingCache.getAllPresent(keys2))
                                    .containsAllEntriesOf(refreshedMap2.getMap())
                                    .isUnmodifiable()
                                    .isEmpty();
                        });
                        Stream.of(loggerDistributedCaffeine, loggerLocalLoadingCache).forEach(logger -> {
                            String message = "Exception thrown during refresh";
                            assertThat(logger.getLoggingEvents()).hasSize(2)
                                    .satisfiesOnlyOnce(loggingEvent -> {
                                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                        assertThat(loggingEvent.getMessage()).startsWith(message);
                                        assertThat(loggingEvent.getThrowable())
                                                .isExactlyInstanceOf(CompletionException.class)
                                                .hasRootCauseExactlyInstanceOf(Exception.class)
                                                .hasMessage("java.lang.Exception: checked");
                                    })
                                    .satisfiesOnlyOnce(loggingEvent -> {
                                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                        assertThat(loggingEvent.getMessage()).startsWith(message);
                                        assertThat(loggingEvent.getThrowable())
                                                .isExactlyInstanceOf(CompletionException.class)
                                                .hasCauseInstanceOf(IllegalStateException.class)
                                                .hasMessage("java.lang.IllegalStateException: unchecked");
                                    });
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED_REFRESHED, assertion -> assertion.isEqualTo(1)));
                    });

            loggerDistributedCaffeine.stopCapturing();
            loggerLocalLoadingCache.stopCapturing();

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "reloaded"))
                    .when(cacheLoader).load(key1);

            loggerDistributedCaffeine.startCapturing();
            loggerLocalLoadingCache.startCapturing();

            refreshedMap1.reset();
            featureParityCaches.forEach(loadingCache -> {
                loadingCache.put(Key.of(0, "checked"), Value.of(0, "checked"));
                loadingCache.put(Key.of(0, "unchecked"), Value.of(0, "unchecked"));
                assertThatThrownBy(() -> loadingCache.refreshAll(Set.of(Key.of(0, "checked"))).join())
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasRootCauseExactlyInstanceOf(Exception.class)
                        .hasMessage("java.lang.Exception: checked");
                assertThatThrownBy(() -> loadingCache.refreshAll(Set.of(Key.of(0, "unchecked"))).join())
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasCauseInstanceOf(IllegalStateException.class)
                        .hasMessage("java.lang.IllegalStateException: unchecked");
                loadingCache.invalidateAll(Set.of(Key.of(0, "checked"), Key.of(0, "unchecked")));

                refreshedMap1.setMap(loadingCache.refreshAll(keys1).join());
            });

            verify(cacheLoader, times(14)).load(any(Key.class));
            verify(cacheLoader, times(8)).asyncLoad(any(Key.class), any(Executor.class));
            verify(cacheLoader, times(6)).reload(any(Key.class), any(Value.class));
            verify(cacheLoader, times(6)).asyncReload(any(Key.class), any(Value.class), any(Executor.class));
            verifyNoMoreInteractions(cacheLoader);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            assertThat(loadingCache.estimatedSize()).isEqualTo(1);
                            assertThat(loadingCache.getAllPresent(keys1))
                                    .containsAllEntriesOf(refreshedMap1.getMap())
                                    .hasSize(1)
                                    .isUnmodifiable()
                                    .values()
                                    .allSatisfy(value -> assertThat(value.getName()).isEqualTo("reloaded"));
                            assertThat(loadingCache.getAllPresent(keys2))
                                    .containsAllEntriesOf(refreshedMap2.getMap())
                                    .isUnmodifiable()
                                    .isEmpty();
                        });
                        Stream.of(loggerDistributedCaffeine, loggerLocalLoadingCache).forEach(logger -> {
                            String message = "Exception thrown during refresh";
                            assertThat(logger.getLoggingEvents()).hasSize(2)
                                    .satisfiesOnlyOnce(loggingEvent -> {
                                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                        assertThat(loggingEvent.getMessage()).startsWith(message);
                                        assertThat(loggingEvent.getThrowable())
                                                .isExactlyInstanceOf(CompletionException.class)
                                                .hasRootCauseExactlyInstanceOf(Exception.class)
                                                .hasMessage("java.lang.Exception: checked");
                                    })
                                    .satisfiesOnlyOnce(loggingEvent -> {
                                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                        assertThat(loggingEvent.getMessage()).startsWith(message);
                                        assertThat(loggingEvent.getThrowable())
                                                .isExactlyInstanceOf(CompletionException.class)
                                                .hasCauseInstanceOf(IllegalStateException.class)
                                                .hasMessage("java.lang.IllegalStateException: unchecked");
                                    });
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED_REFRESHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(2)));
                    });

            loggerDistributedCaffeine.stopCapturing();
            loggerLocalLoadingCache.stopCapturing();

            doAnswer(invocation -> null)
                    .when(cacheLoader).load(key1);

            refreshedMap1.reset();
            featureParityCaches.forEach(loadingCache ->
                    refreshedMap1.setMap(loadingCache.refreshAll(keys1).join()));

            verify(cacheLoader, times(16)).load(any(Key.class));
            verify(cacheLoader, times(8)).asyncLoad(any(Key.class), any(Executor.class));
            verify(cacheLoader, times(8)).reload(any(Key.class), any(Value.class));
            verify(cacheLoader, times(8)).asyncReload(any(Key.class), any(Value.class), any(Executor.class));
            verifyNoMoreInteractions(cacheLoader);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            assertThat(loadingCache.estimatedSize()).isEqualTo(0);
                            assertThat(loadingCache.getAllPresent(keys1))
                                    .containsAllEntriesOf(refreshedMap1.getMap())
                                    .isUnmodifiable()
                                    .isEmpty();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(INVALIDATED_REFRESHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(2)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.empty());

            // test sharing of refresh operations
            int levelOfParallelism = 10;
            AtomicInteger counter = new AtomicInteger(0);

            doAnswer(invocation -> {
                sleep(Duration.ofMillis(100));
                return Value.of(counter.incrementAndGet(), "counted");
            }).when(cacheLoader).load(key1);

            featureParityCaches.forEach(loadingCache -> {
                loadingCache.invalidateAll(keys1);
                IntStream.rangeClosed(1, levelOfParallelism)
                        .mapToObj(i -> loadingCache.refreshAll(keys1))
                        .toList() // intermediate step to ensure concurrency
                        .forEach(CompletableFuture::join);
                counter.set(0);
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            allCaches.forEach(loadingCache ->
                                    assertThat(loadingCache.getIfPresent(key1)).isNotNull()
                                            .satisfies(value -> assertThat(requireNonNull(value).getId()).isLessThan(levelOfParallelism))
                                            .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("counted"))));
        }

        @DisplayName("Test refreshAfterWrite() with cache loader")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_DistributedLoadingCache_refreshAfterWrite_with_cache_loader(CacheFactory<Key, Value> cacheFactory) throws Exception {
            AtomicLong ticker = new AtomicLong(0);

            Caffeine<Object, Object> caffeine = Caffeine.newBuilder()
                    .ticker(ticker::get)
                    .refreshAfterWrite(Duration.ofNanos(1));

            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withCaffeine(caffeine);

            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(Key key) throws Exception {
                    throw new UnsupportedOperationException();
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    dc -> dc.build(cacheLoader));
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    dc -> dc.build(cacheLoader));
            LoadingCache<Key, Value> caffeineLoadingCache = caffeine
                    .build(cacheLoader);

            Set<LoadingCache<Key, Value>> allCaches = Set.of(distributedLoadingCache, syncedDistributedLoadingCache, caffeineLoadingCache);
            Set<LoadingCache<Key, Value>> featureParityCaches = Set.of(distributedLoadingCache, caffeineLoadingCache);

            CaptureLogger loggerBoundedLocalCache = CaptureLoggerFactory
                    .getCaptureLogger("com.github.benmanes.caffeine.cache.BoundedLocalCache");

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            Key keyChecked = Key.of(0, "checked");
            Value valueChecked = Value.of(0, "checked");
            Key keyUnchecked = Key.of(0, "unchecked");
            Value valueUnchecked = Value.of(0, "unchecked");

            EqualResult<Key, Value> getValue1 = new EqualResult<>();
            EqualResult<Key, Value> getValue2 = new EqualResult<>();
            EqualResult<Key, Value> getValueChecked = new EqualResult<>();
            EqualResult<Key, Value> getValueUnchecked = new EqualResult<>();

            doAnswer(invocation -> {
                sleep(Duration.ofMillis(100));
                return Value.of(invocation.<Key>getArgument(0).getId(), "reloaded");
            }).when(cacheLoader).load(key1);
            doAnswer(invocation -> {
                sleep(Duration.ofMillis(100));
                return null;
            }).when(cacheLoader).load(key2);
            doThrow(new Exception("checked"))
                    .when(cacheLoader).load(keyChecked);
            doThrow(new IllegalStateException("unchecked"))
                    .when(cacheLoader).load(keyUnchecked);

            loggerBoundedLocalCache.startCapturing();

            featureParityCaches.forEach(loadingCache -> {
                loadingCache.put(key1, value1);
                loadingCache.put(key2, value2);
                loadingCache.put(keyChecked, valueChecked);
                loadingCache.put(keyUnchecked, valueUnchecked);

                // set ticker to start triggering expiration/refreshing
                ticker.addAndGet(Duration.ofHours(1).toNanos());

                // trigger refresh after write
                getValue1.setValue(loadingCache.getIfPresent(key1));
                getValue2.setValue(loadingCache.getIfPresent(key2));
                getValueChecked.setValue(loadingCache.getIfPresent(keyChecked));
                await("logging") // workaround due to logging interference
                        .atMost(WAITING_DURATION)
                        .untilAsserted(() -> assertThat(loggerBoundedLocalCache.getLoggingEvents().size()).isOdd());
                sleep(Duration.ofMillis(100));
                getValueUnchecked.setValue(loadingCache.getIfPresent(keyUnchecked));
                await("logging") // workaround due to logging interference
                        .atMost(WAITING_DURATION)
                        .untilAsserted(() -> assertThat(loggerBoundedLocalCache.getLoggingEvents().size()).isEven());
            });

            // reset ticker to stop triggering expiration/refreshing
            ticker.set(0);

            await("interactions")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        verify(cacheLoader, times(8)).load(any(Key.class));
                        verify(cacheLoader, times(8)).reload(any(Key.class), any(Value.class));
                        verify(cacheLoader, times(8)).asyncReload(any(Key.class), any(Value.class), any(Executor.class));
                        verifyNoMoreInteractions(cacheLoader);
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            assertThat(loadingCache.estimatedSize()).isEqualTo(3);
                            assertThat(loadingCache.getIfPresent(key1))
                                    .isNotNull()
                                    .isNotEqualTo(value1)
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("reloaded"));
                            assertThat(loadingCache.getIfPresent(key2)).isNull();
                            assertThat(getValue1.getValue()).isEqualTo(value1);
                            assertThat(getValue2.getValue()).isEqualTo(value2);
                            assertThat(loadingCache.getIfPresent(keyChecked))
                                    .isEqualTo(valueChecked)
                                    .isEqualTo(getValueChecked.getValue());
                            assertThat(loadingCache.getIfPresent(keyUnchecked))
                                    .isEqualTo(valueUnchecked)
                                    .isEqualTo(getValueUnchecked.getValue());
                        });
                        String message = "Exception thrown during refresh";
                        assertThat(loggerBoundedLocalCache.getLoggingEvents()).hasSize(4)
                                .anySatisfy(loggingEvent -> {
                                    assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                    assertThat(loggingEvent.getMessage()).startsWith(message);
                                    assertThat(loggingEvent.getThrowable())
                                            .isExactlyInstanceOf(CompletionException.class)
                                            .hasRootCauseExactlyInstanceOf(Exception.class)
                                            .hasMessage("java.lang.Exception: checked");
                                })
                                .anySatisfy(loggingEvent -> {
                                    assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                    assertThat(loggingEvent.getMessage()).startsWith(message);
                                    assertThat(loggingEvent.getThrowable())
                                            .isExactlyInstanceOf(CompletionException.class)
                                            .hasCauseInstanceOf(IllegalStateException.class)
                                            .hasMessage("java.lang.IllegalStateException: unchecked");
                                });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(2)),
                                Count.of(CACHED_REFRESHED_AFTER_WRITE, assertion -> assertion.isEqualTo(1)),
                                Count.of(INVALIDATED_REFRESHED_AFTER_WRITE, assertion -> assertion.isEqualTo(1)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)),
                    Count.of(CACHED_REFRESHED_AFTER_WRITE, assertion -> assertion.isEqualTo(1)));

            loggerBoundedLocalCache.stopCapturing();
        }

        @DisplayName("Test stats()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_DistributedLoadingCache_stats(CacheFactory<Key, Value> cacheFactory) throws Exception {
            AtomicLong ticker = new AtomicLong(0);

            Caffeine<Object, Object> caffeine = Caffeine.newBuilder()
                    .ticker(ticker::get)
                    .recordStats()
                    .refreshAfterWrite(Duration.ofNanos(1));

            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withCaffeine(caffeine);

            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(Key key) throws Exception {
                    throw new UnsupportedOperationException();
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    dc -> dc.build(cacheLoader));
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    dc -> dc.build(cacheLoader));
            LoadingCache<Key, Value> caffeineLoadingCache = caffeine
                    .build(cacheLoader);

            Set<LoadingCache<Key, Value>> allCaches = Set.of(distributedLoadingCache, syncedDistributedLoadingCache, caffeineLoadingCache);
            Set<LoadingCache<Key, Value>> featureParityCaches = Set.of(distributedLoadingCache, caffeineLoadingCache);

            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);
            CaptureLogger loggerLocalLoadingCache = CaptureLoggerFactory
                    .getCaptureLogger("com.github.benmanes.caffeine.cache.LocalLoadingCache");
            CaptureLogger loggerBoundedLocalCache = CaptureLoggerFactory
                    .getCaptureLogger("com.github.benmanes.caffeine.cache.BoundedLocalCache");

            UnaryOperator<CacheStats> sanitizeStats = stats -> CacheStats.of(
                    stats.hitCount(), stats.missCount(),
                    stats.loadSuccessCount(), stats.loadFailureCount(),
                    0, // ensure that totalLoadTime is constant
                    stats.evictionCount(), stats.evictionWeight());

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            Key key3 = Key.of(3);
            Value value3 = Value.of(3);
            Key key4 = Key.of(4);
            Value value4 = Value.of(4);
            Key key5 = Key.of(5);
            Value value5 = Value.of(5);
            Key key6 = Key.of(6);
            Value value6 = Value.of(6);
            Key key7 = Key.of(7);
            Value value7 = Value.of(7);

            EqualResult<Key, Value> statsResult = new EqualResult<>();

            doAnswer(invocation -> value4)
                    .when(cacheLoader).load(key4);
            doAnswer(invocation -> value5)
                    .when(cacheLoader).load(key5);
            doAnswer(invocation -> value6)
                    .when(cacheLoader).load(key6);
            doAnswer(invocation -> value7)
                    .when(cacheLoader).load(key7);

            loggerDistributedCaffeine.startCapturing();
            loggerLocalLoadingCache.startCapturing();
            loggerBoundedLocalCache.startCapturing();

            featureParityCaches.forEach(loadingCache -> {
                loadingCache.put(key1, value1);
                loadingCache.getIfPresent(key1);
                loadingCache.getIfPresent(Key.of(0));
                loadingCache.getAllPresent(Set.of(key1, Key.of(0)));
                loadingCache.get(key1, key -> Value.of(1, "never returned"));
                loadingCache.get(key2, key -> value2);
                assertThatException().isThrownBy(() -> loadingCache.get(Key.of(0), key -> {
                    throw new IllegalStateException();
                }));
                loadingCache.getAll(Set.of(key1, Key.of(0)), keys -> Map.of(key3, value3));
                assertThatException().isThrownBy(() -> loadingCache.getAll(Set.of(Key.of(0)), keys -> _map(null, null)));
                loadingCache.get(key3);
                loadingCache.get(key4);
                assertThatException().isThrownBy(() -> loadingCache.get(Key.of(0)));
                loadingCache.getAll(Set.of(key4, key5));
                assertThatException().isThrownBy(() -> loadingCache.getAll(Set.of(Key.of(0))));
                loadingCache.refresh(key5).join();
                loadingCache.refresh(key6).join();
                assertThatException().isThrownBy(() -> loadingCache.refresh(Key.of(0)).join());
                loadingCache.refreshAll(Set.of(key6, key7)).join();
                assertThatException().isThrownBy(() -> loadingCache.refreshAll(Set.of(Key.of(0))).join());

                // set ticker to start triggering expiration/refreshing
                ticker.addAndGet(Duration.ofHours(1).toNanos());

                loadingCache.getIfPresent(key1); // loadFailureCount + 1
                loadingCache.getIfPresent(key7); // loadSuccessCount + 1

                // reset ticker to stop triggering expiration/refreshing
                ticker.set(0);

                await("asynchronous count of stats")
                        .atMost(WAITING_DURATION)
                        .untilAsserted(() -> statsResult.setObject(sanitizeStats.apply(loadingCache.stats())));
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            CacheStats stats = statsResult.getObject();
                            assertThat(stats.hitCount()).isEqualTo(8);
                            assertThat(stats.missCount()).isEqualTo(10);
                            assertThat(stats.loadSuccessCount()).isEqualTo(9);
                            assertThat(stats.loadFailureCount()).isEqualTo(7);
                            assertThat(stats.evictionCount()).isEqualTo(0);
                            assertThat(stats.evictionWeight()).isEqualTo(0);
                            assertThat(loadingCache.asMap()).containsExactlyInAnyOrderEntriesOf(Map.of(
                                    key1, value1, key2, value2, key3, value3, key4, value4,
                                    key5, value5, key6, value6, key7, value7));
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(3)),
                                Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                Count.of(CACHED_REFRESHED, assertion -> assertion.isEqualTo(2)),
                                Count.of(CACHED_REFRESHED_AFTER_WRITE, assertion -> assertion.isEqualTo(1)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(3)),
                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                    Count.of(CACHED_REFRESHED, assertion -> assertion.isEqualTo(2)),
                    Count.of(CACHED_REFRESHED_AFTER_WRITE, assertion -> assertion.isEqualTo(1)));

            StatsCounter statsCounter = getInstanceRegistry(distributedLoadingCache).getStatsCounter();

            // ensure that extracted stats counter is unique across different instances
            assertThat(statsCounter)
                    .isNotSameAs(getInstanceRegistry(syncedDistributedLoadingCache).getStatsCounter());

            statsCounter.recordEviction(1, RemovalCause.EXPLICIT);

            // ensure that extracted stats counter does not count across instances
            assertThat(distributedLoadingCache.stats().evictionCount()).isEqualTo(1);
            assertThat(distributedLoadingCache.stats().evictionWeight()).isEqualTo(1);
            assertThat(syncedDistributedLoadingCache.stats().evictionWeight()).isEqualTo(0);
            assertThat(syncedDistributedLoadingCache.stats().evictionWeight()).isEqualTo(0);
            assertThat(caffeineLoadingCache.stats().evictionWeight()).isEqualTo(0);
            assertThat(caffeineLoadingCache.stats().evictionWeight()).isEqualTo(0);

            loggerDistributedCaffeine.stopCapturing();
            loggerLocalLoadingCache.stopCapturing();
            loggerBoundedLocalCache.stopCapturing();
        }

        @DisplayName("Test put(), putIfAbsent(), putAll() and get() via asMap()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_put_putIfAbsent_putAll_get(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            List<ConcurrentMap<Key, Value>> allMaps = List.of(distributedCache.asMap(), syncedDistributedCache.asMap(), caffeineCache.asMap());
            List<ConcurrentMap<Key, Value>> featureParityMaps = List.of(distributedCache.asMap(), caffeineCache.asMap());

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            Map<Key, Value> keyValueMap = Map.of(
                    Key.of(3), Value.of(3),
                    Key.of(4), Value.of(4));

            EqualResult<Key, Value> oldValue1x1 = new EqualResult<>();
            EqualResult<Key, Value> oldValue1x2 = new EqualResult<>();
            EqualResult<Key, Value> oldValue2x1 = new EqualResult<>();
            EqualResult<Key, Value> oldValue2x2 = new EqualResult<>();

            featureParityMaps.forEach(map -> {
                assertThatNullPointerException().isThrownBy(() -> map.put(null, Value.of(0)));
                assertThatNullPointerException().isThrownBy(() -> map.put(Key.of(0), null));
                assertThatNullPointerException().isThrownBy(() -> map.putIfAbsent(_null(), Value.of(0)));
                assertThatNullPointerException().isThrownBy(() -> map.putIfAbsent(Key.of(0), null));
                assertThatNullPointerException().isThrownBy(() -> map.putAll(_null()));
                assertThatNullPointerException().isThrownBy(() -> map.putAll(_map(null, Value.of(0))));
                assertThatNullPointerException().isThrownBy(() -> map.putAll(_map(Key.of(0), null)));
                assertThatNullPointerException().isThrownBy(() -> map.get(null));

                oldValue1x1.setValue(map.put(key1, value1));
                oldValue1x2.setValue(map.put(key1, value1));
                oldValue2x1.setValue(map.putIfAbsent(key2, value2));
                oldValue2x2.setValue(map.putIfAbsent(key2, Value.of(0, "not absent")));
                map.putAll(keyValueMap);
            });

            // drive-by testing of equals(), hashCode() and toString()
            assertThat(featureParityMaps).containsExactlyInAnyOrderElementsOf(featureParityMaps);
            featureParityMaps.forEach(map -> assertThat(map).isNotEqualTo(null)); // cover 'instanceof' branch
            assertThat(featureParityMaps.stream()
                    .map(Object::hashCode)
                    .toList())
                    .containsExactlyInAnyOrderElementsOf(featureParityMaps.stream()
                            .map(Object::hashCode)
                            .toList());
            assertThat(featureParityMaps.stream()
                    .map(Object::toString)
                    .toList())
                    .containsExactlyInAnyOrderElementsOf(featureParityMaps.stream()
                            .map(Object::toString)
                            .toList());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map -> {
                            assertThat(map).hasSize(4);
                            assertThat(map.get(key1)).isEqualTo(value1)
                                    .isEqualTo(oldValue1x2.getValue())
                                    .isNotEqualTo(oldValue1x1.getValue());
                            assertThat(map.get(key2)).isEqualTo(value2)
                                    .isEqualTo(oldValue2x2.getValue())
                                    .isNotEqualTo(oldValue2x1.getValue());
                            assertThat(map).containsAllEntriesOf(keyValueMap);
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(4)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(4)));
        }

        @DisplayName("Test replace()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_replace(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            List<ConcurrentMap<Key, Value>> allMaps = List.of(distributedCache.asMap(), syncedDistributedCache.asMap(), caffeineCache.asMap());
            List<ConcurrentMap<Key, Value>> featureParityMaps = List.of(distributedCache.asMap(), caffeineCache.asMap());

            Value toBeReplacedValue = Value.of(0, "to be replaced");
            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            EqualResult<Key, Value> replacedValue1x1 = new EqualResult<>();
            EqualResult<Key, Value> replacedValue1x2 = new EqualResult<>();
            EqualResult<Key, Value> replacedBool2x1 = new EqualResult<>();
            EqualResult<Key, Value> replacedBool2x2 = new EqualResult<>();
            EqualResult<Key, Value> replacedBool2x3 = new EqualResult<>();

            featureParityMaps.forEach(map -> {
                assertThatNullPointerException().isThrownBy(() -> map.replace(_null(), Value.of(0)));
                assertThatNullPointerException().isThrownBy(() -> map.replace(Key.of(0), _null()));
                assertThatNullPointerException().isThrownBy(() -> map.replace(_null(), Value.of(0), Value.of(0)));
                assertThatNullPointerException().isThrownBy(() -> map.replace(Key.of(0), _null(), Value.of(0)));
                assertThatNullPointerException().isThrownBy(() -> map.replace(Key.of(0), Value.of(0), _null()));

                replacedValue1x1.setValue(map.replace(key1, value1));
                replacedBool2x1.setObject(map.replace(key2, toBeReplacedValue, value2));
                map.put(key1, toBeReplacedValue);
                map.put(key2, toBeReplacedValue);
                replacedValue1x2.setValue(map.replace(key1, value1));
                replacedBool2x2.setObject(map.replace(key2, toBeReplacedValue, value2));
                replacedBool2x3.setObject(map.replace(key2, toBeReplacedValue, value2));
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map -> {
                            assertThat(map).hasSize(2);
                            assertThat(map.get(key1)).isEqualTo(value1)
                                    .isNotEqualTo(replacedValue1x1.getValue())
                                    .isNotEqualTo(replacedValue1x2.getValue());
                            assertThat(replacedValue1x1.getValue()).isNull();
                            assertThat(replacedBool2x1.<Boolean>getObject()).isFalse();
                            assertThat(map.get(key2)).isEqualTo(value2);
                            assertThat(replacedValue1x2.getValue()).isEqualTo(toBeReplacedValue);
                            assertThat(replacedBool2x2.<Boolean>getObject()).isTrue();
                            assertThat(replacedBool2x3.<Boolean>getObject()).isFalse();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
        }

        @DisplayName("Test remove(), contains*() and clear()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_remove_contains_clear(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            List<ConcurrentMap<Key, Value>> allMaps = List.of(distributedCache.asMap(), syncedDistributedCache.asMap(), caffeineCache.asMap());
            List<ConcurrentMap<Key, Value>> featureParityMaps = List.of(distributedCache.asMap(), caffeineCache.asMap());

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            Key key3 = Key.of(3);
            Value value3 = Value.of(3);

            EqualResult<Key, Value> removedValue1x1 = new EqualResult<>();
            EqualResult<Key, Value> removedValue1x2 = new EqualResult<>();
            EqualResult<Key, Value> removedBool2x1 = new EqualResult<>();
            EqualResult<Key, Value> removedBool2x2 = new EqualResult<>();
            EqualResult<Key, Value> removedBool2x3 = new EqualResult<>();
            EqualResult<Key, Value> containsKeyBool3x1 = new EqualResult<>();
            EqualResult<Key, Value> containsKeyBool3x2 = new EqualResult<>();
            EqualResult<Key, Value> containsValueBool3x1 = new EqualResult<>();
            EqualResult<Key, Value> containsValueBool3x2 = new EqualResult<>();

            featureParityMaps.forEach(map -> {
                assertThatNullPointerException().isThrownBy(() -> map.remove(null));
                // noinspection SuspiciousMethodCalls
                assertThatNullPointerException().isThrownBy(() -> map.remove(_null(), Value.of(0)));
                assertThatNoException().isThrownBy(() -> map.remove(Key.of(0), null));
                // noinspection ResultOfMethodCallIgnored
                assertThatNullPointerException().isThrownBy(() -> map.containsKey(null));
                // noinspection ResultOfMethodCallIgnored
                assertThatNullPointerException().isThrownBy(() -> map.containsValue(null));

                removedValue1x1.setValue(map.remove(key1));
                removedBool2x1.setObject(map.remove(key2, value2));
                containsKeyBool3x1.setObject(map.containsKey(key3));
                containsValueBool3x1.setObject(map.containsValue(value3));
                map.put(key1, value1);
                map.put(key2, value2);
                map.put(key3, value3);
                removedValue1x2.setValue(map.remove(key1));
                removedBool2x2.setObject(map.remove(key2, Value.of(0)));
                removedBool2x3.setObject(map.remove(key2, value2));
                containsKeyBool3x2.setObject(map.containsKey(key3));
                containsValueBool3x2.setObject(map.containsValue(value3));
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map -> {
                            assertThat(map).hasSize(1);
                            assertThat(map).containsOnly(entry(key3, value3));
                            assertThat(removedValue1x1.getValue()).isNull();
                            assertThat(removedBool2x1.<Boolean>getObject()).isFalse();
                            assertThat(containsKeyBool3x1.<Boolean>getObject()).isFalse();
                            assertThat(containsValueBool3x1.<Boolean>getObject()).isFalse();
                            assertThat(removedValue1x2.getValue()).isEqualTo(value1);
                            assertThat(removedBool2x2.<Boolean>getObject()).isFalse();
                            assertThat(removedBool2x3.<Boolean>getObject()).isTrue();
                            assertThat(containsKeyBool3x2.<Boolean>getObject()).isTrue();
                            assertThat(containsValueBool3x2.<Boolean>getObject()).isTrue();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(2)));
                    });

            featureParityMaps.forEach(Map::clear);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map ->
                                assertThat(map).isEmpty());
                        assertThatDataStoreHasCounts(
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(3)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.empty());
        }

        @DisplayName("Test keySet()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_keySet(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            List<ConcurrentMap<Key, Value>> allMaps = List.of(distributedCache.asMap(), syncedDistributedCache.asMap(), caffeineCache.asMap());
            List<ConcurrentMap<Key, Value>> featureParityMaps = List.of(distributedCache.asMap(), caffeineCache.asMap());

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            Key key3 = Key.of(3);
            Value value3 = Value.of(3);
            Key key4 = Key.of(4);
            Value value4 = Value.of(4);
            Key key5 = Key.of(5);
            Value value5 = Value.of(5);
            Key key6 = Key.of(6);
            Value value6 = Value.of(6);

            EqualResult<Key, Value> removedBool1x1 = new EqualResult<>();
            EqualResult<Key, Value> removedBool1x2 = new EqualResult<>();
            EqualResult<Key, Value> removedAllBool2to3x1 = new EqualResult<>();
            EqualResult<Key, Value> removedAllBool2to3x2 = new EqualResult<>();
            EqualResult<Key, Value> retainedAllBool5to6x1 = new EqualResult<>();
            EqualResult<Key, Value> retainedAllBool5to6x2 = new EqualResult<>();
            EqualResult<Key, Value> hasNextBool5x1 = new EqualResult<>();
            EqualResult<Key, Value> hasNextBool5x2 = new EqualResult<>();
            EqualResult<Key, Value> nextKey5 = new EqualResult<>();

            featureParityMaps.forEach(map -> {
                assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> map.keySet().add(Key.of(0)));
                assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> map.keySet().add(null));
                assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> map.keySet().addAll(_set(null)));
                // noinspection SuspiciousMethodCalls
                assertThatNullPointerException().isThrownBy(() -> map.keySet().removeAll(_null()));
                // noinspection SuspiciousMethodCalls
                assertThatNoException().isThrownBy(() -> map.keySet().removeAll(_set(null)));
                // noinspection SuspiciousMethodCalls
                assertThatNullPointerException().isThrownBy(() -> map.keySet().retainAll(_null()));
                // noinspection SuspiciousMethodCalls
                assertThatNoException().isThrownBy(() -> map.keySet().retainAll(_set(null)));

                // noinspection All
                removedBool1x1.setObject(map.keySet().remove(key1));
                removedAllBool2to3x1.setObject(map.keySet().removeAll(Set.of(key2, key3)));
                retainedAllBool5to6x1.setObject(map.keySet().retainAll(Set.of(key5, key6)));
                hasNextBool5x1.setObject(map.keySet().iterator().hasNext());
                assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> map.keySet().iterator().next());
                assertThatIllegalStateException().isThrownBy(() -> map.keySet().iterator().remove());
                map.put(key1, value1);
                map.put(key2, value2);
                map.put(key3, value3);
                map.put(key4, value4);
                map.put(key5, value5);
                map.put(key6, value6);
                // noinspection All
                removedBool1x2.setObject(map.keySet().remove(key1));
                removedAllBool2to3x2.setObject(map.keySet().removeAll(Set.of(key2, key3)));
                retainedAllBool5to6x2.setObject(map.keySet().retainAll(Set.of(key5, key6)));
                Iterator<Key> iterator = map.keySet().iterator();
                hasNextBool5x2.setObject(iterator.hasNext());
                nextKey5.setKey(iterator.next());
                iterator.remove();
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map -> {
                            assertThat(map.keySet()).hasSize(1);
                            assertThat(map.keySet()).containsOnly(key6);
                            assertThat(removedBool1x1.<Boolean>getObject()).isFalse();
                            assertThat(removedAllBool2to3x1.<Boolean>getObject()).isFalse();
                            assertThat(retainedAllBool5to6x1.<Boolean>getObject()).isFalse();
                            assertThat(hasNextBool5x1.<Boolean>getObject()).isFalse();
                            assertThat(removedBool1x2.<Boolean>getObject()).isTrue();
                            assertThat(removedAllBool2to3x2.<Boolean>getObject()).isTrue();
                            assertThat(retainedAllBool5to6x2.<Boolean>getObject()).isTrue();
                            assertThat(hasNextBool5x2.<Boolean>getObject()).isTrue();
                            assertThat(nextKey5.getKey()).isEqualTo(key5);
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(5)));
                    });

            // noinspection All
            featureParityMaps.forEach(map ->
                    map.keySet().clear());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map ->
                                assertThat(map.keySet()).isEmpty());
                        assertThatDataStoreHasCounts(
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(6)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.empty());
        }

        @DisplayName("Test values()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_values(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            List<ConcurrentMap<Key, Value>> allMaps = List.of(distributedCache.asMap(), syncedDistributedCache.asMap(), caffeineCache.asMap());
            List<ConcurrentMap<Key, Value>> featureParityMaps = List.of(distributedCache.asMap(), caffeineCache.asMap());

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            Key key3 = Key.of(3);
            Value value3 = Value.of(3);
            Key key4 = Key.of(4);
            Value value4 = Value.of(4);
            Key key5 = Key.of(5);
            Value value5 = Value.of(5);
            Key key6 = Key.of(6);
            Value value6 = Value.of(6);

            EqualResult<Key, Value> removedBool1x1 = new EqualResult<>();
            EqualResult<Key, Value> removedBool1x2 = new EqualResult<>();
            EqualResult<Key, Value> removedAllBool2to3x1 = new EqualResult<>();
            EqualResult<Key, Value> removedAllBool2to3x2 = new EqualResult<>();
            EqualResult<Key, Value> retainedAllBool5to6x1 = new EqualResult<>();
            EqualResult<Key, Value> retainedAllBool5to6x2 = new EqualResult<>();
            EqualResult<Key, Value> hasNextBool5x1 = new EqualResult<>();
            EqualResult<Key, Value> hasNextBool5x2 = new EqualResult<>();
            EqualResult<Key, Value> nextValue5 = new EqualResult<>();

            featureParityMaps.forEach(map -> {
                assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> map.values().add(Value.of(0)));
                assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> map.values().add(null));
                assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> map.values().addAll(_set(null)));
                // noinspection SuspiciousMethodCalls
                assertThatNullPointerException().isThrownBy(() -> map.values().removeAll(_null()));
                // noinspection SuspiciousMethodCalls
                assertThatNoException().isThrownBy(() -> map.values().removeAll(_set(null)));
                // noinspection SuspiciousMethodCalls
                assertThatNullPointerException().isThrownBy(() -> map.values().retainAll(_null()));
                // noinspection SuspiciousMethodCalls
                assertThatNoException().isThrownBy(() -> map.values().retainAll(_set(null)));

                removedBool1x1.setObject(map.values().remove(value1));
                removedAllBool2to3x1.setObject(map.values().removeAll(Set.of(value2, value3)));
                retainedAllBool5to6x1.setObject(map.values().retainAll(Set.of(value5, value6)));
                hasNextBool5x1.setObject(map.values().iterator().hasNext());
                assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> map.values().iterator().next());
                assertThatIllegalStateException().isThrownBy(() -> map.values().iterator().remove());
                map.put(key1, value1);
                map.put(key2, value2);
                map.put(key3, value3);
                map.put(key4, value4);
                map.put(key5, value5);
                map.put(key6, value6);
                removedBool1x2.setObject(map.values().remove(value1));
                removedAllBool2to3x2.setObject(map.values().removeAll(Set.of(value2, value3)));
                retainedAllBool5to6x2.setObject(map.values().retainAll(Set.of(value5, value6)));
                Iterator<Value> iterator = map.values().iterator();
                hasNextBool5x2.setObject(iterator.hasNext());
                nextValue5.setValue(iterator.next());
                iterator.remove();
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map -> {
                            assertThat(map.values()).hasSize(1);
                            assertThat(map.values()).containsOnly(value6);
                            assertThat(removedBool1x1.<Boolean>getObject()).isFalse();
                            assertThat(removedAllBool2to3x1.<Boolean>getObject()).isFalse();
                            assertThat(retainedAllBool5to6x1.<Boolean>getObject()).isFalse();
                            assertThat(hasNextBool5x1.<Boolean>getObject()).isFalse();
                            assertThat(removedBool1x2.<Boolean>getObject()).isTrue();
                            assertThat(removedAllBool2to3x2.<Boolean>getObject()).isTrue();
                            assertThat(retainedAllBool5to6x2.<Boolean>getObject()).isTrue();
                            assertThat(hasNextBool5x2.<Boolean>getObject()).isTrue();
                            assertThat(nextValue5.getValue()).isEqualTo(value5);
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(5)));
                    });

            // noinspection All
            featureParityMaps.forEach(map ->
                    map.values().clear());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map ->
                                assertThat(map.values()).isEmpty());
                        assertThatDataStoreHasCounts(
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(6)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.empty());
        }

        @DisplayName("Test entrySet()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_entrySet(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            List<ConcurrentMap<Key, Value>> allMaps = List.of(distributedCache.asMap(), syncedDistributedCache.asMap(), caffeineCache.asMap());
            List<ConcurrentMap<Key, Value>> featureParityMaps = List.of(distributedCache.asMap(), caffeineCache.asMap());

            Entry<Key, Value> entry1 = entry(Key.of(1), Value.of(1));
            Entry<Key, Value> entry2 = entry(Key.of(2), Value.of(2));
            Entry<Key, Value> entry3 = entry(Key.of(3), Value.of(3));
            Entry<Key, Value> entry4 = entry(Key.of(4), Value.of(4));
            Entry<Key, Value> entry5 = entry(Key.of(5), Value.of(5));
            Entry<Key, Value> entry6 = entry(Key.of(6), Value.of(6));

            EqualResult<Key, Value> removedBool1x1 = new EqualResult<>();
            EqualResult<Key, Value> removedBool1x2 = new EqualResult<>();
            EqualResult<Key, Value> removedAllBool2to3x1 = new EqualResult<>();
            EqualResult<Key, Value> removedAllBool2to3x2 = new EqualResult<>();
            EqualResult<Key, Value> retainedAllBool5to6x1 = new EqualResult<>();
            EqualResult<Key, Value> retainedAllBool5to6x2 = new EqualResult<>();
            EqualResult<Key, Value> hasNextBool5x1 = new EqualResult<>();
            EqualResult<Key, Value> hasNextBool5x2 = new EqualResult<>();
            EqualResult<Key, Value> nextEntry5 = new EqualResult<>();

            featureParityMaps.forEach(map -> {
                assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> map.entrySet().add(null));
                assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> map.entrySet().addAll(_set(null)));
                // noinspection SuspiciousMethodCalls
                assertThatNullPointerException().isThrownBy(() -> map.entrySet().removeAll(_null()));
                // noinspection SuspiciousMethodCalls
                assertThatNoException().isThrownBy(() -> map.entrySet().removeAll(_set(null)));
                // noinspection SuspiciousMethodCalls
                assertThatNullPointerException().isThrownBy(() -> map.entrySet().retainAll(_null()));
                // noinspection SuspiciousMethodCalls
                assertThatNoException().isThrownBy(() -> map.entrySet().retainAll(_set(null)));

                removedBool1x1.setObject(map.entrySet().remove(entry1));
                removedAllBool2to3x1.setObject(map.entrySet().removeAll(Set.of(entry2, entry3)));
                retainedAllBool5to6x1.setObject(map.entrySet().retainAll(Set.of(entry5, entry6)));
                hasNextBool5x1.setObject(map.entrySet().iterator().hasNext());
                assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> map.entrySet().iterator().next());
                assertThatIllegalStateException().isThrownBy(() -> map.entrySet().iterator().remove());
                map.put(entry1.getKey(), entry1.getValue());
                map.put(entry2.getKey(), entry2.getValue());
                map.put(entry3.getKey(), entry3.getValue());
                map.put(entry4.getKey(), entry4.getValue());
                map.put(entry5.getKey(), entry5.getValue());
                map.put(entry6.getKey(), entry6.getValue());
                removedBool1x2.setObject(map.entrySet().remove(entry1));
                removedAllBool2to3x2.setObject(map.entrySet().removeAll(Set.of(entry2, entry3)));
                retainedAllBool5to6x2.setObject(map.entrySet().retainAll(Set.of(entry5, entry6)));
                Iterator<Entry<Key, Value>> iterator = map.entrySet().iterator();
                hasNextBool5x2.setObject(iterator.hasNext());
                nextEntry5.setEntry(iterator.next());
                iterator.remove();
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map -> {
                            assertThat(map.entrySet()).hasSize(1);
                            assertThat(map.entrySet()).containsOnly(entry6);
                            assertThat(removedBool1x1.<Boolean>getObject()).isFalse();
                            assertThat(removedAllBool2to3x1.<Boolean>getObject()).isFalse();
                            assertThat(retainedAllBool5to6x1.<Boolean>getObject()).isFalse();
                            assertThat(hasNextBool5x1.<Boolean>getObject()).isFalse();
                            assertThat(removedBool1x2.<Boolean>getObject()).isTrue();
                            assertThat(removedAllBool2to3x2.<Boolean>getObject()).isTrue();
                            assertThat(retainedAllBool5to6x2.<Boolean>getObject()).isTrue();
                            assertThat(hasNextBool5x2.<Boolean>getObject()).isTrue();
                            assertThat(nextEntry5.getEntry()).isEqualTo(entry5);
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(5)));
                    });

            featureParityMaps.forEach(map ->
                    map.entrySet().iterator().next().setValue(Value.of(6, "write through")));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map ->
                                assertThat(map.get(entry6.getKey())).isEqualTo(Value.of(6, "write through")));
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(5)));
                    });

            // noinspection All
            featureParityMaps.forEach(map ->
                    map.entrySet().clear());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map ->
                                assertThat(map.entrySet()).isEmpty());
                        assertThatDataStoreHasCounts(
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(6)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.empty());
        }

        @DisplayName("Test policy()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_Policy(CacheFactory<Key, Value> cacheFactory) {
            Caffeine<Object, Object> caffeine = Caffeine.newBuilder()
                    .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration()));

            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withCaffeine(caffeine);

            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    cacheBuilder,
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    cacheBuilder,
                    DistributedCaffeine::build);
            Cache<Key, Value> caffeineCache = caffeine
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
            List<Policy<Key, Value>> allPolicies = List.of(distributedCache.policy(), syncedDistributedCache.policy(), caffeineCache.policy());
            List<Cache<Key, Value>> featureParityCaches = List.of(distributedCache, caffeineCache);
            List<Policy<Key, Value>> featureParityPolicies = List.of(distributedCache.policy(), caffeineCache.policy());

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            Key key3 = Key.of(3);

            EqualResult<Key, Value> oldValue1x1 = new EqualResult<>();
            EqualResult<Key, Value> oldValue1x2 = new EqualResult<>();
            EqualResult<Key, Value> oldValue2x1 = new EqualResult<>();
            EqualResult<Key, Value> oldValue2x2 = new EqualResult<>();
            EqualResult<Key, Value> computedValue3 = new EqualResult<>();

            featureParityPolicies.forEach(policy -> {
                assertThat(policy.isRecordingStats()).isFalse();
                assertThat(policy.refreshes()).isEmpty();
                assertThat(policy.eviction()).isEmpty();
                assertThat(policy.expireAfterAccess()).isEmpty();
                assertThat(policy.expireAfterWrite()).isEmpty();
                assertThat(policy.refreshAfterWrite()).isEmpty();

                VarExpiration<Key, Value> varExpiration = policy.expireVariably().orElseThrow();

                assertThatNullPointerException().isThrownBy(() -> varExpiration.put(_null(), Value.of(0), 1, TimeUnit.HOURS));
                assertThatNullPointerException().isThrownBy(() -> varExpiration.put(Key.of(0), _null(), 1, TimeUnit.HOURS));
                assertThatNullPointerException().isThrownBy(() -> varExpiration.put(Key.of(0), Value.of(0), 1, _null()));
                assertThatNullPointerException().isThrownBy(() -> varExpiration.putIfAbsent(_null(), Value.of(0), 1, TimeUnit.HOURS));
                assertThatNullPointerException().isThrownBy(() -> varExpiration.putIfAbsent(Key.of(0), _null(), 1, TimeUnit.HOURS));
                assertThatNullPointerException().isThrownBy(() -> varExpiration.putIfAbsent(Key.of(0), Value.of(0), 1, _null()));
                assertThatNullPointerException().isThrownBy(() -> varExpiration.compute(_null(), (k, v) -> v, Duration.ofHours(1)));
                assertThatNullPointerException().isThrownBy(() -> varExpiration.compute(Key.of(0), _null(), Duration.ofHours(1)));
                assertThatNullPointerException().isThrownBy(() -> varExpiration.compute(Key.of(0), (k, v) -> v, _null()));

                assertThat(varExpiration.oldest(1)).isEmpty();
                assertThat(varExpiration.oldest(Function.identity())).isInstanceOf(Stream.class);
                assertThat(varExpiration.youngest(1)).isEmpty();
                assertThat(varExpiration.youngest(Function.identity())).isInstanceOf(Stream.class);

                oldValue1x1.setValue(varExpiration.put(key1, value1, 1, TimeUnit.HOURS));
                oldValue1x2.setValue(varExpiration.put(key1, value1, 1, TimeUnit.HOURS));
                oldValue2x1.setValue(varExpiration.putIfAbsent(key2, value2, 1, TimeUnit.HOURS));
                oldValue2x2.setValue(varExpiration.putIfAbsent(key2, Value.of(0, "not absent"), 1, TimeUnit.HOURS));
                computedValue3.setValue(varExpiration.compute(key3, (k, v) -> Value.of(k.getId(), "computed"), Duration.ofHours(1)));

                varExpiration.setExpiresAfter(key3, 1, TimeUnit.DAYS);
                assertThat(varExpiration.getExpiresAfter(key3).orElseThrow()).isCloseTo(Duration.ofDays(1), WAITING_DURATION);

                Policy.CacheEntry<Key, Value> cacheEntry = policy.getEntryIfPresentQuietly(key1);
                assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() ->
                        requireNonNull(cacheEntry).setValue(Value.of(0)));
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(cache ->
                                assertThat(cache.estimatedSize()).isEqualTo(3));
                        allPolicies.forEach(policy -> {
                            assertThat(policy.getIfPresentQuietly(key1)).isEqualTo(value1)
                                    .isEqualTo(oldValue1x2.getValue())
                                    .isNotEqualTo(oldValue1x1.getValue());
                            assertThat(requireNonNull(policy.getEntryIfPresentQuietly(key1)).expiresAt())
                                    .isPositive();
                            assertThat(policy.getIfPresentQuietly(key2)).isEqualTo(value2)
                                    .isEqualTo(oldValue2x2.getValue())
                                    .isNotEqualTo(oldValue2x1.getValue());
                            assertThat(requireNonNull(policy.getEntryIfPresentQuietly(key2)).expiresAt())
                                    .isPositive();
                            assertThat(policy.getIfPresentQuietly(key3)).isEqualTo(computedValue3.getValue())
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("computed"));
                            assertThat(requireNonNull(policy.getEntryIfPresentQuietly(key3)).expiresAt())
                                    .isPositive();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(3)));
                    });

            computedValue3.reset();
            featureParityPolicies.forEach(policy -> {
                VarExpiration<Key, Value> varExpiration = policy.expireVariably().orElseThrow();

                varExpiration.setExpiresAfter(key1, Duration.ZERO);
                varExpiration.setExpiresAfter(key2, Duration.ZERO);
                computedValue3.setValue(varExpiration.compute(key3, (k, v) -> null, Duration.ofHours(1)));
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(cache ->
                                assertThat(cache.estimatedSize()).isEqualTo(0));
                        allPolicies.forEach(policy -> {
                            assertThat(policy.getIfPresentQuietly(key1)).isNull();
                            assertThat(policy.getIfPresentQuietly(key2)).isNull();
                            assertThat(policy.getIfPresentQuietly(key3)).isNull();
                            assertThat(computedValue3.getValue()).isNull();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(1)),
                                Count.of(EVICTED_TIME, assertion -> assertion.isEqualTo(2)));
                    });

            featureParityPolicies.forEach(policy -> {
                VarExpiration<Key, Value> varExpiration = policy.expireVariably().orElseThrow();

                // should have no impact on not yet existing entries
                varExpiration.setExpiresAfter(key1, Duration.ZERO);
            });

            featureParityCaches.forEach(cache ->
                    cache.put(key1, value1));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allCaches.forEach(cache ->
                                assertThat(cache.estimatedSize()).isEqualTo(1));
                        allPolicies.forEach(policy ->
                                assertThat(policy.getIfPresentQuietly(key1)).isEqualTo(value1));
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(1)),
                                Count.of(EVICTED_TIME, assertion -> assertion.isEqualTo(1)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
        }

        @DisplayName("Test distributedPolicy()")
        @Test
        void test_DistributedPolicy() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration())))
                            .withExtendedPersistence(configurer -> configurer
                                    .withMaximumTime(FOREVER.getDuration())),
                    DistributedCaffeine::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            assertThat(distributedPolicy.getKeySerializer())
                    .isInstanceOfAny(ByteArraySerializer.class, StringSerializer.class, JsonSerializer.class);
            assertThat(distributedPolicy.getValueSerializer())
                    .isInstanceOfAny(ByteArraySerializer.class, StringSerializer.class, JsonSerializer.class);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            assertThatNullPointerException().isThrownBy(() -> distributedPolicy.getFromStore(_null(), true));
            assertThatNullPointerException().isThrownBy(() -> distributedPolicy.getAllFromStore(_null(), true));
            assertThatNullPointerException().isThrownBy(() -> distributedPolicy.getAllFromStore(_set(null), true));

            VarExpiration<Key, Value> varExpiration = distributedCache.policy().expireVariably().orElseThrow();

            varExpiration.put(key1, value1, Duration.ofHours(1));
            varExpiration.put(key2, value2, Duration.ZERO); // fast eviction

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() ->
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(1))));

            // test equals() and hashCode() and toString()
            CacheEntry<Key, Value> cacheEntry1 = distributedPolicy.getFromStore(key1, true);
            CacheEntry<Key, Value> cacheEntry2 = distributedPolicy.getFromStore(key2, true);
            assertThat(cacheEntry1).isNotNull();
            assertThat(cacheEntry2).isNotNull();
            // noinspection ConstantValue
            assertThat(cacheEntry1.equals(null)).isFalse();
            // noinspection EqualsBetweenInconvertibleTypes
            assertThat(cacheEntry1.equals("other class")).isFalse();
            // noinspection EqualsWithItself
            assertThat(cacheEntry1.equals(cacheEntry1)).isTrue();
            assertThat(cacheEntry1.equals(cacheEntry2)).isFalse();
            assertThat(cacheEntry1.hashCode()).isNotEqualTo(cacheEntry2.hashCode());
            assertThat(cacheEntry1.toString()).isNotEqualTo(cacheEntry2.toString());

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        assertThat(distributedPolicy.getFromStore(key1, false)).isNotNull();
                        assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull();
                        assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                        assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull();
                        assertThat(distributedPolicy.getAllFromStore(Set.of(key1, key2), false))
                                .containsOnly(cacheEntry1)
                                .allSatisfy(entry -> assertThat(entry.getDiscriminator()).isNull())
                                .allSatisfy(entry -> assertThat(entry.getHash()).isNotBlank())
                                .allSatisfy(entry -> assertThat(entry.getOperation()).isNotNull())
                                .allSatisfy(entry -> assertThat(entry.getKey()).isEqualTo(key1))
                                .allSatisfy(entry -> assertThat(entry.getValue()).isEqualTo(value1))
                                .allSatisfy(entry -> assertThat(entry.getStatus()).isEqualTo(CACHED))
                                .allSatisfy(entry -> assertThat(entry.isCached()).isTrue())
                                .allSatisfy(entry -> assertThat(entry.isInvalidated()).isFalse())
                                .allSatisfy(entry -> assertThat(entry.isEvicted()).isFalse())
                                .allSatisfy(entry -> assertThat(entry.isEvictedExtended()).isFalse());
                        assertThat(distributedPolicy.getAllFromStore(Set.of(key1, key2), true))
                                .containsOnly(cacheEntry1, cacheEntry2);
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(1)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(1)));
        }

        @DisplayName("Test population")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_population(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCacheA = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> distributedCacheB = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);

            DistributionMode distributionMode = getInstanceRegistry(distributedCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);
            distributedCacheB.put(key2, value2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            distributedCacheA.put(key2, value2);
            distributedCacheB.put(key1, value1);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            processMaintenance();

            if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
            } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.empty());
            }
        }

        @DisplayName("Test invalidation")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_invalidation(CacheFactory<Key, Value> cacheFactory) {
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> removalListener = mock(RemovalListener.class);

            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                            .removalListener(removalListener));

            DistributedCache<Key, Value> distributedCacheA = cacheFactory.create(
                    cacheBuilder,
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> distributedCacheB = cacheFactory.create(
                    cacheBuilder,
                    DistributedCaffeine::build);

            DistributionMode distributionMode = getInstanceRegistry(distributedCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);
            distributedCacheB.put(key2, value2);

            verifyNoInteractions(removalListener);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            distributedCacheA.invalidate(key2);
            distributedCacheB.invalidate(key1);

            await("invalidation")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(removalListener, times(4))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPLICIT));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verifyNoInteractions(removalListener);
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(INVALIDATED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            distributedCacheA.invalidate(key1);
            distributedCacheB.invalidate(key2);

            await("invalidation")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(removalListener, times(4))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPLICIT));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(removalListener, times(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPLICIT));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(INVALIDATED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(INVALIDATED, assertion -> assertion.isEqualTo(2)));
                        }
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.empty());
        }

        @DisplayName("Test eviction by size")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_eviction_by_size(CacheFactory<Key, Value> cacheFactory) {
            int maximumSize = 1;

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                            .evictionListener(evictionListener)
                            .maximumSize(maximumSize));

            DistributedCache<Key, Value> distributedCacheA = cacheFactory.create(
                    cacheBuilder,
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> distributedCacheB = cacheFactory.create(
                    cacheBuilder,
                    DistributedCaffeine::build);

            DistributionMode distributionMode = getInstanceRegistry(distributedCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);

            verifyNoInteractions(evictionListener);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            distributedCacheB.put(key2, value2); // implicit eviction

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, atLeast(1))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                            verify(evictionListener, atMost(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verifyNoInteractions(evictionListener);
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            distributedCacheA.put(key2, value2);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, atLeast(1))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                            verify(evictionListener, atMost(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(1))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            distributedCacheB.put(key1, value1); // implicit eviction

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, atLeast(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                            verify(evictionListener, atMost(4))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)) {
                            verify(evictionListener, times(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        } else if (distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            processMaintenance();

            if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                assertThatDataStoreHasCounts(
                        Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
            } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
            } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.empty());
            }
        }

        @DisplayName("Test eviction by time")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_eviction_by_time(CacheFactory<Key, Value> cacheFactory) {
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                            .evictionListener(evictionListener)
                            // variable expiration policy provides more control over evictions
                            .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration())));

            DistributedCache<Key, Value> distributedCacheA = cacheFactory.create(
                    cacheBuilder,
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> distributedCacheB = cacheFactory.create(
                    cacheBuilder,
                    DistributedCaffeine::build);

            DistributionMode distributionMode = getInstanceRegistry(distributedCacheA).getDistributionMode();
            VarExpiration<Key, Value> varExpirationA = distributedCacheA.policy().expireVariably().orElseThrow();
            VarExpiration<Key, Value> varExpirationB = distributedCacheB.policy().expireVariably().orElseThrow();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);

            verifyNoInteractions(evictionListener);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            distributedCacheB.put(key2, value2);
            // explicit eviction
            varExpirationA.setExpiresAfter(key1, Duration.ZERO);
            varExpirationB.setExpiresAfter(key1, Duration.ZERO);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, times(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(1))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            distributedCacheA.put(key2, value2);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, times(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(1))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            distributedCacheB.put(key1, value1);
            // explicit eviction
            varExpirationA.setExpiresAfter(key2, Duration.ZERO);
            varExpirationB.setExpiresAfter(key2, Duration.ZERO);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, times(4))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(3))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            processMaintenance();

            if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                assertThatDataStoreHasCounts(
                        Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
            } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
            } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.empty());
            }
        }

        @DisplayName("Test refresh")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_refresh(CacheFactory<Key, Value> cacheFactory) throws Exception {
            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(Key key) throws Exception {
                    throw new UnsupportedOperationException();
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCacheA = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    dc -> dc.build(cacheLoader));
            DistributedLoadingCache<Key, Value> distributedLoadingCacheB = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    dc -> dc.build(cacheLoader));

            DistributionMode distributionMode = getInstanceRegistry(distributedLoadingCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded"))
                    .when(cacheLoader).load(any(Key.class));

            Value loadedValue1 = distributedLoadingCacheA.refresh(key1).join();
            Value loadedValue2 = distributedLoadingCacheB.refresh(key2).join();

            verify(cacheLoader, times(2)).load(any(Key.class));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap())
                                    .values()
                                    .allSatisfy(value -> assertThat(value.getName()).isEqualTo("loaded"));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_REFRESHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1)
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("loaded"));
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2)
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("loaded"));
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "reloaded"))
                    .when(cacheLoader).load(any(Key.class));

            Value reloadedValue2 = distributedLoadingCacheA.refreshAll(Set.of(key2)).join().values().stream()
                    .findFirst().orElseThrow();
            Value reloadedValue1 = distributedLoadingCacheB.refreshAll(Set.of(key1)).join().values().stream()
                    .findFirst().orElseThrow();

            verify(cacheLoader, times(4)).load(any(Key.class));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(reloadedValue1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isEqualTo(reloadedValue2);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap())
                                    .values()
                                    .allSatisfy(value -> assertThat(value.getName()).isEqualTo("reloaded"));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_REFRESHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1)
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("loaded"));
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isEqualTo(reloadedValue2)
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("reloaded"));
                            assertThat(distributedLoadingCacheB.getIfPresent(key1)).isEqualTo(reloadedValue1)
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("reloaded"));
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2)
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("loaded"));
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            doAnswer(invocation -> null)
                    .when(cacheLoader).load(any(Key.class));

            distributedLoadingCacheA.refresh(key1).join();
            distributedLoadingCacheB.refreshAll(Set.of(key2)).join();

            verify(cacheLoader, times(6)).load(any(Key.class));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(INVALIDATED_REFRESHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(INVALIDATED_REFRESHED, assertion -> assertion.isEqualTo(2)));
                        }
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.empty());
        }

        @DisplayName("Test refresh after write")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_refresh_after_write(CacheFactory<Key, Value> cacheFactory) throws Exception {
            AtomicLong ticker = new AtomicLong(0);

            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                            .ticker(ticker::get)
                            .refreshAfterWrite(Duration.ofNanos(1)));

            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(Key key) throws Exception {
                    throw new UnsupportedOperationException();
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCacheA = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    dc -> dc.build(cacheLoader));
            DistributedLoadingCache<Key, Value> distributedLoadingCacheB = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    dc -> dc.build(cacheLoader));

            DistributionMode distributionMode = getInstanceRegistry(distributedLoadingCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedLoadingCacheA.put(key1, value1);
            distributedLoadingCacheB.put(key2, value2);

            verifyNoInteractions(cacheLoader);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedLoadingCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "refreshed"))
                    .when(cacheLoader).load(any(Key.class));

            // set ticker to start triggering expiration/refreshing
            ticker.addAndGet(Duration.ofHours(1).toNanos());

            distributedLoadingCacheA.getIfPresent(key1);
            distributedLoadingCacheB.getIfPresent(key2);

            // reset ticker to stop triggering expiration/refreshing
            ticker.set(0);

            await("asynchronous refreshes")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            verify(cacheLoader, times(2)).load(any(Key.class)));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(requireNonNull(distributedLoadingCacheA.getIfPresent(key1)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key1.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(requireNonNull(distributedLoadingCacheA.getIfPresent(key2)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key2.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(requireNonNull(distributedLoadingCacheB.getIfPresent(key1)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key1.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(requireNonNull(distributedLoadingCacheB.getIfPresent(key2)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key2.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_REFRESHED_AFTER_WRITE, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(requireNonNull(distributedLoadingCacheA.getIfPresent(key1)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key1.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isNull();
                            assertThat(distributedLoadingCacheB.getIfPresent(key1)).isNull();
                            assertThat(requireNonNull(distributedLoadingCacheB.getIfPresent(key2)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key2.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            doAnswer(invocation -> null)
                    .when(cacheLoader).load(any(Key.class));

            // set ticker to start triggering expiration/refreshing
            ticker.addAndGet(Duration.ofHours(2).toNanos());

            distributedLoadingCacheA.getIfPresent(key1);
            distributedLoadingCacheB.getIfPresent(key2);

            // reset ticker to stop triggering expiration/refreshing
            ticker.set(0);

            await("asynchronous refreshes")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            verify(cacheLoader, times(4)).load(any(Key.class)));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(INVALIDATED_REFRESHED_AFTER_WRITE, assertion -> assertion.isEqualTo(2)));
                        }
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.empty());
        }

        @DisplayName("Test synchronization")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_synchronization(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCacheA = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);

            DistributionMode distributionMode = getInstanceRegistry(distributedCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);
            distributedCacheA.put(key2, value2);

            DistributedCache<Key, Value> distributedCacheB = cacheFactory.create(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            processMaintenance();

            if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
            } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.empty());
            }
        }

        @DisplayName("Test extended persistence by size")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_extended_persistence_by_size(CacheFactory<Key, Value> cacheFactory) throws Exception {
            int maximumSize = 1;
            int extendedMaximumSize = 2;

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .evictionListener(evictionListener)
                                    .maximumSize(maximumSize))
                            .withExtendedPersistence(configurer -> configurer
                                    .withMaximumSize(extendedMaximumSize)
                                    // just to test the distinction in logic
                                    .withMaximumTime(FOREVER.getDuration())
                                    .withLoadingStrategy(true));

            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(Key key) throws Exception {
                    throw new UnsupportedOperationException();
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCacheA = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    dc -> dc.build(cacheLoader));
            DistributedLoadingCache<Key, Value> distributedLoadingCacheB = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    dc -> dc.build(cacheLoader));

            DistributionMode distributionMode = getInstanceRegistry(distributedLoadingCacheA).getDistributionMode();
            DistributedPolicy<Key, Value> distributedPolicy = distributedLoadingCacheA.distributedPolicy();

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);
            Key key3 = Key.of(3);
            Key key4 = Key.of(4);

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded"))
                    .when(cacheLoader).load(any(Key.class));

            Value loadedValue1 = distributedLoadingCacheA.get(key1);

            verify(cacheLoader, times(1)).load(any(Key.class));
            verifyNoInteractions(evictionListener);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            Value loadedValue2 = distributedLoadingCacheB.get(key2); // implicit eviction

            verify(cacheLoader, times(2)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, atLeast(1))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                            verify(evictionListener, atMost(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verifyNoInteractions(evictionListener);
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            // use getAll()
            distributedLoadingCacheA.getAll(Set.of(key1)); // implicit eviction

            verifyNoMoreInteractions(cacheLoader);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, atLeast(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                            verify(evictionListener, atMost(4))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verifyNoInteractions(evictionListener);
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            Value loadedValue3 = distributedLoadingCacheB.get(key3); // implicit eviction

            verify(cacheLoader, times(3)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, atLeast(3))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                            verify(evictionListener, atMost(6))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(1))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(1)));
                        }
                    });

            distributedLoadingCacheA.get(key1); // implicit eviction

            verifyNoMoreInteractions(cacheLoader);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, atLeast(4))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                            verify(evictionListener, atMost(8))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(1))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(1)));
                        }
                    });

            // create more cache entries (extended by size) than the maximum size allows
            Value loadedValue4 = distributedLoadingCacheA.get(key4); // implicit eviction

            verify(cacheLoader, times(4)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, atLeast(5))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                            verify(evictionListener, atMost(10))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key4)).isEqualTo(loadedValue4);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromStore(key4, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue4));
                            assertThat(distributedPolicy.getFromStore(key4, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue4));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(3)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key4)).isEqualTo(loadedValue4);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNull();
                            assertThat(distributedPolicy.getFromStore(key4, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key4, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(2)));
                        }
                    });

            processMaintenance();

            if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                        Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(extendedMaximumSize)));
            } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                    distributionMode.equals(INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(extendedMaximumSize)));
            }

            // test cache without loading strategy
            DistributedLoadingCache<Key, Value> distributedLoadingCacheWithoutLoadingStrategy = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    dc -> dc.withExtendedPersistence(configurer -> configurer
                            .withLoadingStrategy(false)),
                    dc -> dc.build(cacheLoader));

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded but not from store"))
                    .when(cacheLoader).load(any(Key.class));

            Value loadedFromStoreValue = requireNonNull(distributedPolicy.getFromStore(key1, true)).getValue();
            Value notFoundValue = distributedLoadingCacheWithoutLoadingStrategy.getIfPresent(key1);
            Value loadedButNotFromStoreValue = distributedLoadingCacheWithoutLoadingStrategy.get(key1);

            verify(cacheLoader, times(5)).load(any(Key.class));

            assertThat(loadedFromStoreValue).isEqualTo(loadedValue1);
            assertThat(notFoundValue).isNull();
            assertThat(loadedButNotFromStoreValue).isNotNull()
                    .satisfies(value -> assertThat(value.getName()).isEqualTo("loaded but not from store"));
        }

        @DisplayName("Test extended persistence by time")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_extended_persistence_by_time(CacheFactory<Key, Value> cacheFactory) throws Exception {
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .evictionListener(evictionListener)
                                    // variable expiration policy provides more control over evictions
                                    .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration())))
                            .withExtendedPersistence(configurer -> configurer
                                    .withMaximumTime(FOREVER.getDuration())
                                    // just to test the distinction in logic
                                    .withMaximumSize(Integer.MAX_VALUE)
                                    .withLoadingStrategy(true));

            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(Key key) throws Exception {
                    throw new UnsupportedOperationException();
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCacheA = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    dc -> dc.build(cacheLoader));
            DistributedLoadingCache<Key, Value> distributedLoadingCacheB = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    dc -> dc.build(cacheLoader));

            DistributionMode distributionMode = getInstanceRegistry(distributedLoadingCacheA).getDistributionMode();
            DistributedPolicy<Key, Value> distributedPolicy = distributedLoadingCacheA.distributedPolicy();
            VarExpiration<Key, Value> varExpirationA = distributedLoadingCacheA.policy().expireVariably().orElseThrow();
            VarExpiration<Key, Value> varExpirationB = distributedLoadingCacheB.policy().expireVariably().orElseThrow();

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);
            Key key3 = Key.of(3);
            Key key4 = Key.of(4);

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded"))
                    .when(cacheLoader).load(any(Key.class));

            Value loadedValue1 = distributedLoadingCacheA.get(key1);

            verify(cacheLoader, times(1)).load(any(Key.class));
            verifyNoInteractions(evictionListener);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.empty());
                        }
                    });

            Value loadedValue2 = distributedLoadingCacheB.get(key2);
            // explicit eviction
            varExpirationA.setExpiresAfter(key1, Duration.ZERO);
            varExpirationB.setExpiresAfter(key1, Duration.ZERO);

            verify(cacheLoader, times(2)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, times(2))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(1))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(1)));
                        }
                    });

            // use getAll()
            distributedLoadingCacheA.getAll(Set.of(key1));
            // explicit eviction
            varExpirationA.setExpiresAfter(key2, Duration.ZERO);
            varExpirationB.setExpiresAfter(key2, Duration.ZERO);

            verifyNoMoreInteractions(cacheLoader);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, times(4))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(1))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(2)));
                        }
                    });

            Value loadedValue3 = distributedLoadingCacheB.get(key3);
            // explicit eviction
            varExpirationA.setExpiresAfter(key1, Duration.ZERO);
            varExpirationB.setExpiresAfter(key1, Duration.ZERO);

            verify(cacheLoader, times(3)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, times(6))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(3))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(2)));
                        }
                    });

            distributedLoadingCacheA.get(key1);
            // explicit eviction
            varExpirationA.setExpiresAfter(key3, Duration.ZERO);
            varExpirationB.setExpiresAfter(key3, Duration.ZERO);

            verifyNoMoreInteractions(cacheLoader);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, times(8))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(4))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(3)));
                        }
                    });

            Value loadedValue4 = distributedLoadingCacheA.get(key4);
            // explicit eviction
            varExpirationA.setExpiresAfter(key3, Duration.ZERO);
            varExpirationB.setExpiresAfter(key3, Duration.ZERO);

            verify(cacheLoader, times(4)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            verify(evictionListener, times(8))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            verify(evictionListener, times(4))
                                    .onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key4)).isEqualTo(loadedValue4);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromStore(key4, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue4));
                            assertThat(distributedPolicy.getFromStore(key4, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue4));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(2)),
                                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key4)).isEqualTo(loadedValue4);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromStore(key4, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key4, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(3)));
                        }
                    });

            processMaintenance();

            if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(2)),
                        Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(2)));
            } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(EVICTED_TIME_EXTENDED, assertion -> assertion.isEqualTo(3)));
            }

            // test cache without loading strategy
            DistributedLoadingCache<Key, Value> distributedLoadingCacheWithoutLoadingStrategy = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    dc -> dc.withExtendedPersistence(configurer -> configurer
                            .withLoadingStrategy(false)),
                    dc -> dc.build(cacheLoader));

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded but not from store"))
                    .when(cacheLoader).load(any(Key.class));

            Value loadedFromStoreValue = requireNonNull(distributedPolicy.getFromStore(key2, true)).getValue();
            Value notFoundValue = distributedLoadingCacheWithoutLoadingStrategy.getIfPresent(key2);
            Value loadedButNotFromStoreValue = distributedLoadingCacheWithoutLoadingStrategy.get(key2);

            verify(cacheLoader, times(5)).load(any(Key.class));

            assertThat(loadedFromStoreValue).isEqualTo(loadedValue2);
            assertThat(notFoundValue).isNull();
            assertThat(loadedButNotFromStoreValue).isNotNull()
                    .satisfies(value -> assertThat(value.getName()).isEqualTo("loaded but not from store"));
        }

        @DisplayName("Test synchronization")
        @Test
        void test_DistributedCaffeine_synchronization() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            Key key3 = Key.of(3);
            Value value3 = Value.of(3);

            distributedCache.put(key1, value1);

            DistributedCache<Key, Value> syncedDistributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(distributedCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedCache.asMap());
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
                    });

            distributedCache.put(key2, value2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedCache.asMap());
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                    });

            syncedDistributedCache.distributedPolicy().stopSynchronization();

            syncedDistributedCache.put(key1, Value.of(1, "overwritten"));
            syncedDistributedCache.invalidate(key2);
            syncedDistributedCache.put(key3, value3);

            await("no synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCache.asMap().entrySet())
                                .doesNotContainAnyElementsOf(syncedDistributedCache.asMap().entrySet());
                        assertThat(syncedDistributedCache.asMap().entrySet())
                                .doesNotContainAnyElementsOf(distributedCache.asMap().entrySet());
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                    });

            syncedDistributedCache.distributedPolicy().startSynchronization();

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedCache.asMap());
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
        }

        @DisplayName("Test same value instance handling and invalidation of already absent value")
        @Test
        void test_DistributedCaffeine_same_value_and_already_absent() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            distributedCache.put(key1, value1);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(distributedCache.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(syncedDistributedCache.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(distributedCache.getIfPresent(Key.of(0, "not present"))).isNull();
                        assertThat(syncedDistributedCache.getIfPresent(Key.of(0, "not present"))).isNull();
                        // identity checks
                        assertThat(distributedCache.getIfPresent(key1)).isSameAs(value1);
                        assertThat(syncedDistributedCache.getIfPresent(key1)).isNotSameAs(value1);
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
                    });

            Instant timestamp = requireNonNull(distributedCache.distributedPolicy()
                    .getFromStore(key1, false)).getTimestamp();

            distributedCache.put(key1, value1); // same value instance should produce new timestamp
            distributedCache.invalidate(Key.of(0, "not present"));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(distributedCache.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(syncedDistributedCache.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(distributedCache.getIfPresent(Key.of(0, "not present"))).isNull();
                        assertThat(syncedDistributedCache.getIfPresent(Key.of(0, "not present"))).isNull();
                        // identity checks
                        assertThat(distributedCache.getIfPresent(key1)).isSameAs(value1);
                        assertThat(syncedDistributedCache.getIfPresent(key1)).isNotSameAs(value1);
                        assertThat(requireNonNull(distributedCache.distributedPolicy()
                                .getFromStore(key1, false)).getTimestamp())
                                .isAfter(timestamp);
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
        }

        @DisplayName("Test Adapter")
        @Test
        void test_Adapter() throws Exception {
            Set<io.github.oberhoff.distributedcaffeine.adapter.CacheEntry<Key, Value>> retrievedCacheEntries = new HashSet<>();
            @SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
            Retriever<Key, Value> retriever = spy(new Retriever<Key, Value>() {
                @Override
                public void retrieveCacheEntries(Collection<io.github.oberhoff.distributedcaffeine.adapter.CacheEntry<Key, Value>> cacheEntries) {
                    retrievedCacheEntries.addAll(cacheEntries);
                }
            });
            MongoAdapter<Key, Value> mongoAdapter = new MongoAdapter<>(mongoClient, DATABASE_NAME, getCollectionName());
            mongoAdapter.setKeySerializer(new ForySerializer<>());
            mongoAdapter.setValueSerializer(new ForySerializer<>());
            mongoAdapter.setRetriever(retriever);
            Repository<Key, Value> repository = mongoAdapter.getRepository();

            mongoAdapter.activate();
            assertThat(mongoAdapter.isActivated()).isTrue();

            CacheEntry<Key, Value> insertCacheEntry1 = CacheEntry.of(
                    "discriminator",
                    "hash1",
                    1,
                    Key.of(1),
                    Value.of(1),
                    CACHED,
                    Instant.now());
            CacheEntry<Key, Value> insertCacheEntry2 = CacheEntry.of(
                    "discriminator",
                    "hash2",
                    2,
                    Key.of(2),
                    Value.of(2),
                    CACHED,
                    Instant.now());

            repository.upsertCacheEntries(Set.of(insertCacheEntry1, insertCacheEntry2));

            CacheEntry<Key, Value> updateCacheEntry1 = CacheEntry.of(
                    insertCacheEntry1.getDiscriminator(),
                    insertCacheEntry1.getHash(),
                    insertCacheEntry1.getOperation(),
                    insertCacheEntry1.getKey(),
                    insertCacheEntry1.getValue(),
                    insertCacheEntry1.getStatus(),
                    Instant.now());
            CacheEntry<Key, Value> updateCacheEntry2 = CacheEntry.of(
                    insertCacheEntry2.getDiscriminator(),
                    insertCacheEntry2.getHash(),
                    insertCacheEntry2.getOperation(),
                    insertCacheEntry2.getKey(),
                    insertCacheEntry2.getValue(),
                    insertCacheEntry2.getStatus(),
                    Instant.now());

            repository.upsertCacheEntries(Set.of(updateCacheEntry1, updateCacheEntry2));

            List<io.github.oberhoff.distributedcaffeine.adapter.CacheEntry<Key, Value>> foundCacheEntries = new ArrayList<>();
            try (Stream<io.github.oberhoff.distributedcaffeine.adapter.CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries("discriminator", null, null, null, false)) {
                stream.forEach(foundCacheEntries::add);
            }

            await("retrieving")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        verify(retriever, times(4))
                                .retrieveCacheEntries(anySet());
                        assertThat(retrievedCacheEntries)
                                .containsExactlyInAnyOrder(
                                        insertCacheEntry1, insertCacheEntry2,
                                        updateCacheEntry1, updateCacheEntry2);
                    });


            assertThat(foundCacheEntries).hasSize(2)
                    .containsExactlyInAnyOrder(updateCacheEntry1, updateCacheEntry2);
            assertThat(repository.countCacheEntries("discriminator", null))
                    .isEqualTo(2);

            repository.deleteCacheEntries("discriminator", null, null, null);

            assertThat(repository.countCacheEntries("discriminator", null))
                    .isEqualTo(0);

            mongoAdapter.deactivate();
            assertThat(mongoAdapter.isActivated()).isFalse();
        }

        // TODO
        /* @DisplayName("Test ChangeStreamWatcher")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_ChangeStreamWatcher_fails_and_retries() {
            // test early failure
            assertThatThrownBy(() -> DistributedCaffeine.newBuilder(mongoDatabase
                            .getCollection(UUID.randomUUID().toString())
                            .withReadConcern(ReadConcern.LOCAL))
                    .build())
                    .isExactlyInstanceOf(MongoClientException.class)
                    .hasMessageStartingWith("Watching change streams failed")
                    .hasMessageNotContaining("Retrying")
                    .cause()
                    .isExactlyInstanceOf(MongoCommandException.class)
                    .hasMessageContainingAll(ReadConcernLevel.LOCAL.getValue(), ReadConcernLevel.MAJORITY.getValue());

            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);

            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    Builder::build);

            // inject spies
            InternalChangeStreamWatcher<Key, Value> changeStreamWatcher = getDistributedCaffeine(distributedCache)
                    .getChangeStreamWatcher();
            AtomicReference<BsonTimestamp> operationTime = injectSpy(changeStreamWatcher, InternalChangeStreamWatcher.class,
                    "operationTime", AtomicReference.class);
            InternalCacheManager<Key, Value> cacheManager = injectSpy(changeStreamWatcher, InternalChangeStreamWatcher.class,
                    "cacheManager", InternalCacheManager.class);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            distributedCache.put(key1, value1);

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedCache));

            loggerDistributedCaffeine.startCapturing();

            // provoke failure
            doThrow(new IllegalStateException()).when(operationTime).set(any(BsonTimestamp.class));

            distributedCache.put(key1, value1);

            await("failure")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        List<LoggingEvent> loggingEvents = loggerDistributedCaffeine.getLoggingEvents();
                        assertThat(loggingEvents).isNotEmpty();
                        assertThat(loggingEvents).allMatch(loggingEvent ->
                                loggingEvent.getLevel().equals(Level.WARN)
                                        && loggingEvent.getMessage().startsWith("Watching change streams failed")
                                        && loggingEvent.getMessage().endsWith("Retrying..."));
                    });

            await("cache manager maintenance")
                    .pollInterval(WAITING_DURATION) // await (no) cache manager maintenance
                    .untilAsserted(() -> assertThatThrownBy(() -> assertThatMaintenanceIsDone(distributedCache))
                            .isExactlyInstanceOf(AssertionError.class));

            // fix failure
            doCallRealMethod().when(operationTime).set(any(BsonTimestamp.class));

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION.plusSeconds(10)) // retry delay is increased on failure
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedCache));

            // provoke failure
            doThrow(new IllegalStateException()).when(cacheManager).manageInboundInsert(any(), anyBoolean());

            loggerDistributedCaffeine.stopCapturing();
            loggerDistributedCaffeine.startCapturing();

            distributedCache.put(key1, value1);

            await("failure")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        List<LoggingEvent> loggingEvents = loggerDistributedCaffeine.getLoggingEvents();
                        assertThat(loggingEvents).isNotEmpty();
                        assertThat(loggingEvents).allMatch(loggingEvent ->
                                loggingEvent.getLevel().equals(Level.WARN)
                                        && loggingEvent.getMessage().startsWith("Deserializing of cache entry failed")
                                        && loggingEvent.getMessage().endsWith("Skipping..."));
                    });

            loggerDistributedCaffeine.stopCapturing();

            // fix failure
            doCallRealMethod().when(cacheManager).manageInboundInsert(any(), anyBoolean());

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION.plusSeconds(10)) // retry delay is increased on failure
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedCache));
        } */

        // TODO
        /* @DisplayName("Test MaintenanceWorker")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_MaintenanceWorker_fails_and_retries() {
            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);

            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .maximumSize(1))
                            .withExtendedPersistence(configurer -> configurer
                                    .withMaximumSize(1)),
                    Builder::build);

            // inject spy
            InternalMaintenanceWorker<Key, Value> maintenanceWorker = getDistributedCaffeine(distributedCache)
                    .getMaintenanceWorker();
            Collection<ObjectId> toBeMarkedAsStale = injectSpy(maintenanceWorker, InternalMaintenanceWorker.class,
                    "toBeMarkedAsStale", Collection.class);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCache.put(key1, value1);

            // create stale cache entry
            distributedCache.put(key1, value1);

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(1), s -> s.isEqualTo(1))));

            // check that 'expires' is set on datastore level for stale cache entries
            Set<InternalCacheDocument<Key, Value>> cacheDocuments = new HashSet<>();
            getDistributedCaffeine(distributedCache).getMongoRepository()
                    .consumeCacheDocumentsGroupedByKeyNewestFirstForKeys(
                            Set.of(key1), null,
                            null, null,
                            stream -> stream.forEach(cacheDocuments::addAll));
            assertThat(cacheDocuments)
                    .satisfiesOnlyOnce(cacheDocument -> assertThat(cacheDocument.getExpires()).isNull())
                    .satisfiesOnlyOnce(cacheDocument -> assertThat(cacheDocument.getExpires()).isNotNull());

            // create inconsistencies in relation to not stale cache entries but prevent instant correction by cache manager
            getDistributedCaffeine(distributedCache).getCacheManager().manageCleanUp(Duration.ZERO);
            distributedCache.put(key2, value2);

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(1), s -> s.isEqualTo(2)),
                            Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(1), s -> s.isEqualTo(0))));

            // create more cache entries (extended by size) than the maximum size allows
            distributedCache.put(key1, value1);

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(1), s -> s.isEqualTo(3)),
                            Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(1), s -> s.isEqualTo(1))));

            loggerDistributedCaffeine.startCapturing();

            // provoke failure
            doThrow(new IllegalStateException()).when(toBeMarkedAsStale).stream();

            await("failure")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        List<LoggingEvent> loggingEvents = loggerDistributedCaffeine.getLoggingEvents();
                        assertThat(loggingEvents).isNotEmpty();
                        assertThat(loggingEvents).allMatch(loggingEvent ->
                                loggingEvent.getLevel().equals(Level.WARN)
                                        && loggingEvent.getMessage().startsWith("Maintenance failed")
                                        && loggingEvent.getMessage().endsWith("Retrying..."));
                    });

            loggerDistributedCaffeine.stopCapturing();

            // create stale cache entry
            distributedCache.put(key1, value1);

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(2), s -> s.isEqualTo(3)),
                            Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(1), s -> s.isEqualTo(1))));

            // create inconsistencies in relation to not stale cache entries but prevent instant replace by cache manager
            getDistributedCaffeine(distributedCache).getCacheManager().manageCleanUp(Duration.ZERO);
            distributedCache.put(key2, value2);

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(3), s -> s.isEqualTo(3)),
                            Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(2), s -> s.isEqualTo(1))));

            // create more cache entries (extended by size) than the maximum size allows
            distributedCache.put(key1, value1);

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(4), s -> s.isEqualTo(3)),
                            Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(3), s -> s.isEqualTo(1))));

            // fix failure
            doCallRealMethod().when(toBeMarkedAsStale).stream();

            await("maintenance")
                    .atMost(WAITING_DURATION.plusSeconds(10)) // retry delay is increased on failure
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(1), s -> s.isEqualTo(6)),
                            Count.of(EVICTED_SIZE_EXTENDED, assertion -> assertion.isEqualTo(1), s -> s.isEqualTo(3))));
        } */

        // TODO
        /* @DisplayName("Test MongoRepository")
        @Test
        void test_MongoRepository_indexes() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    Builder::build);

            MongoCollection<Document> mongoCollection = distributedCache.distributedPolicy().getMongoCollection();

            String indexName = UUID.randomUUID().toString();
            IndexModel index = new IndexModel(Indexes.compoundIndex(
                    Stream.of(InternalCacheDocument.Field.values())
                            .map(field -> Indexes.ascending(field.toString()))
                            .toArray(Bson[]::new)),
                    new IndexOptions()
                            .name(indexName)
                            .unique(false)
                            .background(true));

            mongoCollection.createIndexes(List.of(index));

            HashSet<String> names = new HashSet<>();
            mongoCollection.listIndexes().forEach(document ->
                    names.add(document.getString("name")));
            int indexCount = names.size();

            assertThat(names).containsOnlyOnce(indexName);

            createCache(
                    CacheBuilder.identity(),
                    Builder::build);

            names.clear();
            mongoCollection.listIndexes().forEach(document ->
                    names.add(document.getString("name")));

            assertThat(names).doesNotContain(indexName)
                    .hasSize(indexCount - 1);
        } */

        @DisplayName("Stress test synchronization from data store")
        @Test
        void stress_test_DistributedCaffeine_synchronization_from_data_store() throws Exception {
            int maximumSize = runsOnGitHub(10_000, 100_000);
            int extendedMaximumSize = maximumSize / 100;
            int numberOfOperations = 10_000;

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> removalListener = mock(RemovalListener.class);
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                public Value load(Key key) {
                    return nextInt(10) == 0
                            ? null
                            : Value.of(key.getId(), nameWithMillisAndPrefixes("load"));
                }

                @Override
                public Map<? extends Key, ? extends Value> loadAll(Set<? extends Key> keys) {
                    return keys.stream()
                            .collect(toMap(Function.identity(),
                                    key -> Value.of(key.getId(), nameWithMillisAndPrefixes("load"))));
                }
            });

            Supplier<DistributedLoadingCache<Key, Value>> cacheSupplier = () -> {
                DistributedCache<Key, Value> cache = createCache(
                        dc -> dc.withCaffeine(Caffeine.newBuilder()
                                        .executor(executorService)
                                        .removalListener(removalListener)
                                        .evictionListener(evictionListener)
                                        .maximumSize(maximumSize)
                                        .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration())))
                                .withExtendedPersistence(configurer -> configurer
                                        .withMaximumSize(extendedMaximumSize)
                                        .withLoadingStrategy(true)),
                        dc -> dc.build(cacheLoader));
                return (DistributedLoadingCache<Key, Value>) cache;
            };

            DistributedLoadingCache<Key, Value> distributedLoadingCache = cacheSupplier.get();

            Map<Key, Value> keyValueMap = IntStream.rangeClosed(1, maximumSize)
                    .boxed()
                    .collect(toMap(Key::of, i -> Value.of(i, nameWithMillisAndPrefixes("init"))));
            distributedLoadingCache.putAll(keyValueMap);

            assertThatDataStoreHasCounts(
                    CountGrouped.of(CACHED_GROUP, assertion -> assertion.isEqualTo(keyValueMap.size())));

            AtomicBoolean loopCondition = new AtomicBoolean(true);
            AtomicInteger loopCounter = new AtomicInteger(numberOfOperations);

            CompletableFuture.runAsync(() -> {
                while (loopCondition.get()) {
                    executeRandomOperation(distributedLoadingCache, maximumSize);
                    loopCounter.decrementAndGet();
                }
            }, executorService);

            await("loop counter")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .untilAtomic(loopCounter, count -> assertThat(count).isLessThanOrEqualTo(0));

            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = cacheSupplier.get();

            loopCondition.set(false);

            await("synchronization between cache instances")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedLoadingCache.asMap());
                        assertThatDataStoreHasCounts(
                                CountGrouped.of(CACHED_GROUP, assertion -> assertion.isEqualTo(distributedLoadingCache.estimatedSize())));
                    });

            syncedDistributedLoadingCache.distributedPolicy().stopSynchronization();

            IntStream.rangeClosed(1, numberOfOperations).forEach(i ->
                    executeRandomOperation(syncedDistributedLoadingCache, maximumSize));

            loopCondition.set(true);
            loopCounter.set(numberOfOperations);

            CompletableFuture.runAsync(() -> {
                while (loopCondition.get()) {
                    executeRandomOperation(distributedLoadingCache, maximumSize);
                    loopCounter.decrementAndGet();
                }
            }, executorService);

            await("loop counter")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .untilAtomic(loopCounter, count -> assertThat(count).isLessThanOrEqualTo(0));

            syncedDistributedLoadingCache.distributedPolicy().startSynchronization();

            loopCondition.set(false);

            await("synchronization between cache instances")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedLoadingCache.asMap());
                        assertThatDataStoreHasCounts(
                                CountGrouped.of(CACHED_GROUP, assertion -> assertion.isEqualTo(distributedLoadingCache.estimatedSize())),
                                CountGrouped.of(EVICTED_EXTENDED_GROUP, assertion -> assertion.isGreaterThanOrEqualTo(extendedMaximumSize)));
                    });

            await("maintenance")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process maintenance", this::processMaintenance)
                    .untilAsserted(() ->
                            assertThatDataStoreHasCounts(
                                    CountGrouped.of(SHORT_LIVING_GROUP, assertion -> assertion.isEqualTo(0)),
                                    CountGrouped.of(EVICTED_EXTENDED_GROUP, assertion -> assertion.isEqualTo(extendedMaximumSize))));

            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.EXPLICIT));
            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.REPLACED));
            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.SIZE));
            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.EXPIRED));
            verifyNoMoreInteractions(removalListener);

            verify(evictionListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.SIZE));
            verify(evictionListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.EXPIRED));
            verifyNoMoreInteractions(evictionListener);

            verify(cacheLoader, atLeastOnce()).load(any(Key.class));
            verify(cacheLoader, atLeastOnce()).loadAll(anySet());
            verify(cacheLoader, atLeastOnce()).asyncLoad(any(Key.class), any(Executor.class));
            verify(cacheLoader, never()).asyncLoadAll(anySet(), any(Executor.class));
            // invocation cannot be not guaranteed for reload() and asyncReload()
            verify(cacheLoader, atLeast(0)).reload(any(Key.class), any(Value.class));
            verify(cacheLoader, atLeast(0)).asyncReload(any(Key.class), any(Value.class), any(Executor.class));
            verifyNoMoreInteractions(cacheLoader);
        }

        @DisplayName("Stress test thread safety")
        @ParameterizedTest(name = "with {0}-executor")
        @ValueSource(strings = {"same thread", "single thread", "common pool", "cached thread pool", "work stealing thread pool"})
        void stress_test_DistributedCaffeine_thread_safety(String valueSource) throws Exception {
            int maximumSize = 100;
            int extendedMaximumSize = maximumSize / 10;
            int numberOfOperations = 1_000;
            int levelOfParallelism = runsOnGitHub(3, 10);

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> removalListener = mock(RemovalListener.class);
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                public Value load(Key key) {
                    return nextInt(10) == 0
                            ? null
                            : Value.of(key.getId(), nameWithMillisAndPrefixes("load"));
                }

                @Override
                public Map<? extends Key, ? extends Value> loadAll(Set<? extends Key> keys) {
                    return keys.stream()
                            .collect(toMap(Function.identity(),
                                    key -> Value.of(key.getId(), nameWithMillisAndPrefixes("load"))));
                }
            });

            List<Executor> executors = new ArrayList<>();
            Supplier<Executor> executorSupplier = () -> {
                Executor executor = switch (valueSource) {
                    case "same thread" -> Runnable::run;
                    case "single thread" -> Executors.newSingleThreadExecutor();
                    case "common pool" -> ForkJoinPool.commonPool();
                    case "cached thread pool" -> Executors.newCachedThreadPool();
                    case "work stealing thread pool" -> Executors.newWorkStealingPool(levelOfParallelism);
                    default -> throw new NoSuchElementException();
                };
                executors.add(executor);
                return executor;
            };

            Function<AtomicLong, DistributedLoadingCache<Key, Value>> cacheSupplier = ticker -> {
                DistributedCache<Key, Value> cache = createCache(
                        dc -> dc.withCaffeine(Caffeine.newBuilder()
                                        .ticker(ticker::get)
                                        .removalListener(removalListener)
                                        .evictionListener(evictionListener)
                                        .executor(executorSupplier.get())
                                        .maximumSize(maximumSize)
                                        .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration()))
                                        .refreshAfterWrite(Duration.ofNanos(1)))
                                .withExtendedPersistence(configurer -> configurer
                                        .withMaximumSize(extendedMaximumSize)
                                        .withLoadingStrategy(true)),
                        dc -> dc.build(cacheLoader));
                return (DistributedLoadingCache<Key, Value>) cache;
            };

            AtomicLong ticker = new AtomicLong();
            DistributedLoadingCache<Key, Value> distributedLoadingCache =
                    cacheSupplier.apply(ticker);
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache =
                    cacheSupplier.apply(new AtomicLong(0));

            List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

            IntStream.rangeClosed(1, levelOfParallelism).forEach(threadIndex ->
                    completableFutures.add(CompletableFuture.runAsync(() -> {
                        IntStream.rangeClosed(1, numberOfOperations).forEach(operationIndex -> {
                            executeRandomOperation(distributedLoadingCache, maximumSize);
                            if (threadIndex == levelOfParallelism / 2 && operationIndex == numberOfOperations / 2) {
                                distributedLoadingCache.distributedPolicy().stopSynchronization();
                                sleep(Duration.ofMillis(1_000));
                                distributedLoadingCache.distributedPolicy().startSynchronization();
                                // set ticker to start triggering expiration/refreshing
                                ticker.addAndGet(Duration.ofHours(1).toNanos());
                            }
                        });
                        // increment ticker to trigger last pending evictions
                        ticker.addAndGet(Duration.ofHours(1).toNanos());
                    }, executorService)));

            CompletableFuture.allOf(completableFutures.toArray(CompletableFuture[]::new)).join();

            // no reset of ticker, wait for still inbounding 'refresh after write' entries
            AtomicLong estimatedSize = new AtomicLong(distributedLoadingCache.estimatedSize());
            await("inbounding refreshes")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process clean up", this::cleanUp)
                    .during(WAITING_DURATION)
                    .until(() -> {
                        if (distributedLoadingCache.estimatedSize() == estimatedSize.get()) {
                            return true;
                        } else {
                            estimatedSize.set(distributedLoadingCache.estimatedSize());
                            return false;
                        }
                    });

            await("synchronization between cache instances")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedLoadingCache.asMap());
                        assertThatDataStoreHasCounts(
                                CountGrouped.of(CACHED_GROUP, assertion -> assertion.isEqualTo(distributedLoadingCache.estimatedSize())),
                                CountGrouped.of(EVICTED_EXTENDED_GROUP, assertion -> assertion.isGreaterThanOrEqualTo(extendedMaximumSize)));
                    });

            await("maintenance")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process maintenance", this::processMaintenance)
                    .untilAsserted(() ->
                            assertThatDataStoreHasCounts(
                                    CountGrouped.of(SHORT_LIVING_GROUP, assertion -> assertion.isEqualTo(0)),
                                    CountGrouped.of(EVICTED_EXTENDED_GROUP, assertion -> assertion.isEqualTo(extendedMaximumSize))));

            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.EXPLICIT));
            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.REPLACED));
            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.SIZE));
            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.EXPIRED));
            // pending invocations are accepted, so no verifyNoMoreInteractions()

            verify(evictionListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.SIZE));
            verify(evictionListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.EXPIRED));
            // pending invocations are accepted, so no verifyNoMoreInteractions()

            verify(cacheLoader, atLeastOnce()).load(any(Key.class));
            verify(cacheLoader, atLeastOnce()).loadAll(anySet());
            verify(cacheLoader, atLeastOnce()).asyncLoad(any(Key.class), any(Executor.class));
            verify(cacheLoader, never()).asyncLoadAll(anySet(), any(Executor.class));
            verify(cacheLoader, atLeastOnce()).reload(any(Key.class), any(Value.class));
            verify(cacheLoader, atLeastOnce()).asyncReload(any(Key.class), any(Value.class), any(Executor.class));
            // pending invocations are accepted, so no verifyNoMoreInteractions()

            // delayed shutdown of executors to prevent pending error logs
            CompletableFuture.runAsync(() -> {
                sleep(WAITING_DURATION);
                executors.stream()
                        .filter(ExecutorService.class::isInstance)
                        .map(ExecutorService.class::cast)
                        .forEach(ExecutorService::shutdown);
            }, executorService);
        }

        @DisplayName("Stress test with multiple threads")
        @Test
        void stress_test_DistributedCaffeine_multiple_threads() throws Exception {
            int maximumSize = 1_000;
            int extendedMaximumSize = maximumSize / 2;
            int numberOfOperations = 10_000;
            int levelOfParallelism = runsOnGitHub(3, 10);

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> removalListener = mock(RemovalListener.class);
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                public Value load(Key key) {
                    return nextInt(10) == 0
                            ? null
                            : Value.of(key.getId(), nameWithMillisAndPrefixes("load"));
                }

                @Override
                public Map<? extends Key, ? extends Value> loadAll(Set<? extends Key> keys) {
                    return keys.stream()
                            .collect(toMap(Function.identity(),
                                    key -> Value.of(key.getId(), nameWithMillisAndPrefixes("load"))));
                }
            });

            Function<AtomicLong, DistributedLoadingCache<Key, Value>> cacheSupplier = ticker -> {
                DistributedCache<Key, Value> cache = createCache(
                        dc -> dc.withCaffeine(Caffeine.newBuilder()
                                        .ticker(ticker::get)
                                        .removalListener(removalListener)
                                        .evictionListener(evictionListener)
                                        .executor(executorService)
                                        .maximumSize(maximumSize)
                                        .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration()))
                                        .refreshAfterWrite(Duration.ofNanos(1)))
                                .withExtendedPersistence(configurer -> configurer
                                        .withMaximumSize(extendedMaximumSize)
                                        .withLoadingStrategy(true)),
                        dc -> dc.build(cacheLoader));
                return (DistributedLoadingCache<Key, Value>) cache;
            };

            List<DistributedLoadingCache<Key, Value>> distributedLoadingCaches = new ArrayList<>();
            List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

            IntStream.rangeClosed(1, levelOfParallelism).forEach(cacheIndex ->
                    completableFutures.add(CompletableFuture.runAsync(() -> {
                        sleep(Duration.ofMillis(1_000).multipliedBy(min(10, cacheIndex - 1)));
                        AtomicLong ticker = new AtomicLong(0);
                        DistributedLoadingCache<Key, Value> distributedLoadingCache = cacheSupplier.apply(ticker);
                        distributedLoadingCaches.add(distributedLoadingCache);
                        IntStream.rangeClosed(1, numberOfOperations).forEach(operationIndex -> {
                            executeRandomOperation(distributedLoadingCache, maximumSize);
                            if (operationIndex == numberOfOperations / 2) {
                                distributedLoadingCache.distributedPolicy().stopSynchronization();
                                sleep(Duration.ofMillis(1_000).multipliedBy(min(10, cacheIndex)));
                                distributedLoadingCache.distributedPolicy().startSynchronization();
                                // set ticker to start triggering expiration/refreshing
                                ticker.addAndGet(Duration.ofHours(1).toNanos());
                            }
                        });
                        // increment ticker to trigger last pending evictions
                        ticker.addAndGet(Duration.ofHours(1).toNanos());
                    }, executorService)));

            CompletableFuture.allOf(completableFutures.toArray(CompletableFuture[]::new)).join();

            DistributedLoadingCache<Key, Value> lastDistributedLoadingCache = distributedLoadingCaches
                    .get(distributedLoadingCaches.size() - 1);

            await("synchronization between cache instances")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        IntStream.range(0, distributedLoadingCaches.size() - 1).forEach(i ->
                                assertThat(distributedLoadingCaches.get(i).asMap())
                                        .describedAs(() -> format("%s (%s) vs. %s (%s)",
                                                i, distributedLoadingCaches.get(i),
                                                i + 1, distributedLoadingCaches.get(i + 1)))
                                        .containsExactlyInAnyOrderEntriesOf(distributedLoadingCaches.get(i + 1).asMap()));
                        assertThatDataStoreHasCounts(
                                CountGrouped.of(CACHED_GROUP, assertion -> assertion.isEqualTo(lastDistributedLoadingCache.estimatedSize())));
                    });

            // no reset of ticker, wait for still inbounding 'refresh after write' entries
            AtomicLong estimatedSize = new AtomicLong(lastDistributedLoadingCache.estimatedSize());
            await("inbounding refreshes")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process clean up", this::cleanUp)
                    .during(WAITING_DURATION)
                    .until(() -> {
                        if (lastDistributedLoadingCache.estimatedSize() == estimatedSize.get()) {
                            return true;
                        } else {
                            estimatedSize.set(lastDistributedLoadingCache.estimatedSize());
                            return false;
                        }
                    });

            distributedLoadingCaches.forEach(distributedCache ->
                    completableFutures.add(CompletableFuture
                            .runAsync(distributedCache::invalidateAll, executorService)));

            CompletableFuture.allOf(completableFutures.toArray(CompletableFuture[]::new)).join();

            await("synchronization between cache instances")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCaches)
                                .allSatisfy(distributedLoadingCache -> assertThat(distributedLoadingCache.estimatedSize()).isEqualTo(0));
                        assertThatDataStoreHasCounts(
                                CountGrouped.of(CACHED_GROUP, assertion -> assertion.isEqualTo(0)),
                                CountGrouped.of(EVICTED_EXTENDED_GROUP, assertion -> assertion.isGreaterThanOrEqualTo(extendedMaximumSize)));
                    });

            await("maintenance")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process maintenance", this::processMaintenance)
                    .untilAsserted(() ->
                            assertThatDataStoreHasCounts(
                                    CountGrouped.of(SHORT_LIVING_GROUP, assertion -> assertion.isEqualTo(0)),
                                    CountGrouped.of(EVICTED_EXTENDED_GROUP, assertion -> assertion.isEqualTo(extendedMaximumSize))));

            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.EXPLICIT));
            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.REPLACED));
            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.SIZE));
            verify(removalListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.EXPIRED));
            verifyNoMoreInteractions(removalListener);

            verify(evictionListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.SIZE));
            verify(evictionListener, atLeastOnce()).onRemoval(nullable(Key.class), nullable(Value.class), eq(RemovalCause.EXPIRED));
            verifyNoMoreInteractions(evictionListener);

            verify(cacheLoader, atLeastOnce()).load(any(Key.class));
            verify(cacheLoader, atLeastOnce()).loadAll(anySet());
            verify(cacheLoader, atLeastOnce()).asyncLoad(any(Key.class), any(Executor.class));
            verify(cacheLoader, never()).asyncLoadAll(anySet(), any(Executor.class));
            verify(cacheLoader, atLeastOnce()).reload(any(Key.class), any(Value.class));
            verify(cacheLoader, atLeastOnce()).asyncReload(any(Key.class), any(Value.class), any(Executor.class));
            verifyNoMoreInteractions(cacheLoader);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    abstract static class DistributedCaffeineIntegrationTestInstance extends DistributedCaffeineCommonTestInstance {

        static final String RUNS_ON_GITHUB = "runsOnGitHub";
        static final String DATABASE_NAME = "distributedCaffeineDatabase";
        static final String LOGGER_RESOURCE_LOCK = "logger";
        static final Duration WAITING_DURATION = Duration.ofSeconds(3);
        static final Duration EXTENDED_WAITING_DURATION = Duration.ofMinutes(3);
        static final Duration EXTENDED_POLL_INTERVAL = Duration.ofSeconds(1);

        SecureRandom secureRandom;
        MongoDBContainer mongoContainer;
        MongoClient mongoClient;

        @BeforeAll
        void beforeAll() {
            this.secureRandom = new SecureRandom();

            DockerImageName dockerImageName = Optional.ofNullable(getClass().getAnnotation(DockerImage.class))
                    .map(DockerImage::value)
                    .map(DockerImageName::parse)
                    .orElseThrow();
            String displayName = Optional.ofNullable(getClass().getAnnotation(DisplayName.class))
                    .map(DisplayName::value)
                    .map(value -> value.replace(" ", "-"))
                    .map(String::toLowerCase)
                    .orElseThrow();

            this.mongoContainer = new MongoDBContainer(dockerImageName)
                    .withReplicaSet()
                    .withCreateContainerCmdModifier(cmd -> cmd.withName(displayName))
                    .withImagePullPolicy(PullPolicy.alwaysPull());
            this.mongoContainer.start();
            this.mongoClient = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(mongoContainer.getReplicaSetUrl()))
                    .applyToSocketSettings(socketSettings -> socketSettings
                            .connectTimeout(30, TimeUnit.SECONDS)
                            .readTimeout(30, TimeUnit.SECONDS))
                    .build());
        }

        @AfterAll
        void afterAll() {
            this.mongoClient.close();
            this.mongoContainer.stop();
        }

        Stream<Arguments> provideCacheFactoriesWithDifferentSerializers() {
            Stream<DistributedCaffeineConfiguration<Key, Value>> distributedCaffeineConfigurations = createDistributedCaffeineConfigurationWithDifferentSerializers();
            return createNamedCacheFactoriesForParametrizedTests(distributedCaffeineConfigurations)
                    .map(Arguments::of);
        }

        Stream<Arguments> provideCacheFactoriesWithDifferentDistributionModes() {
            Stream<DistributedCaffeineConfiguration<Key, Value>> distributedCaffeineConfigurations = createDistributedCaffeineConfigurationsWithDifferentDistributionModes();
            return createNamedCacheFactoriesForParametrizedTests(distributedCaffeineConfigurations)
                    .map(Arguments::of);
        }

        Stream<DistributedCaffeineConfiguration<Key, Value>> createDistributedCaffeineConfigurationWithDifferentSerializers() {
            return Stream.of(
                    new DistributedCaffeineConfiguration<>(
                            "with Fory Serializer",
                            CacheBuilder.identity()),
                    new DistributedCaffeineConfiguration<>(
                            "with Fory Serializer (Class)",
                            dc -> dc.withSerializers(configurer -> configurer
                                    .withKeySerializer(new ForySerializer<>(Key.class))
                                    .withValueSerializer(new ForySerializer<>(Value.class)))),
                    new DistributedCaffeineConfiguration<>(
                            "with Java Object Serializer",
                            dc -> dc.withSerializers(configurer -> configurer
                                    .withKeySerializer(new JavaObjectSerializer<>())
                                    .withValueSerializer(new JavaObjectSerializer<>()))),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (Class, BSON)",
                            dc -> dc.withSerializers(configurer -> configurer
                                    .withKeySerializer(new JacksonSerializer<>(Key.class, true))
                                    .withValueSerializer(new JacksonSerializer<>(Value.class, true)))),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (Class, JSON)",
                            dc -> dc.withSerializers(configurer -> configurer
                                    .withKeySerializer(new JacksonSerializer<>(Key.class, false))
                                    .withValueSerializer(new JacksonSerializer<>(Value.class, false)))),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (TypeReference, BSON)",
                            dc -> dc.withSerializers(configurer -> configurer
                                    .withKeySerializer(new JacksonSerializer<>(new TypeReference<>() {
                                    }, true))
                                    .withValueSerializer(new JacksonSerializer<>(new TypeReference<>() {
                                    }, true)))),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (TypeReference, JSON)",
                            dc -> dc.withSerializers(configurer -> configurer
                                    .withKeySerializer(new JacksonSerializer<>(new TypeReference<>() {
                                    }, false))
                                    .withValueSerializer(new JacksonSerializer<>(new TypeReference<>() {
                                    }, false))))
            );
        }

        Stream<DistributedCaffeineConfiguration<Key, Value>> createDistributedCaffeineConfigurationsWithDifferentDistributionModes() {
            return Stream.of(DistributionMode.values())
                    .map(distributionMode -> new DistributedCaffeineConfiguration<>(
                            format("with %s.%s", distributionMode.getClass().getSimpleName(), distributionMode.name()),
                            dc -> dc.withDistributionMode(distributionMode)));
        }

        <K, V> Stream<Named<CacheFactory<K, V>>> createNamedCacheFactoriesForParametrizedTests(Stream<DistributedCaffeineConfiguration<K, V>> distributedCaffeineConfigurations) {
            return distributedCaffeineConfigurations
                    .map(distributedCaffeineConfiguration -> {
                        CacheFactory<K, V> cacheFactory = (cacheBuilder, cacheConstructor) -> {
                            CacheBuilder<K, V> aggregatedCacheBuilder = b -> distributedCaffeineConfiguration.cacheBuilder()
                                    .apply(cacheBuilder.apply(b));
                            return createCache(aggregatedCacheBuilder, cacheConstructor);
                        };
                        return Named.of(distributedCaffeineConfiguration.displayName(), cacheFactory);
                    });
        }

        <K, V> DistributedCache<K, V> createCache(CacheBuilder<K, V> cacheBuilder, CacheConstructor<K, V> cacheConstructor) {
            MongoAdapter<K, V> mongoAdapter = new MongoAdapter<>(mongoClient, DATABASE_NAME, getCollectionName());
            return createCache(mongoAdapter, cacheBuilder, cacheConstructor);
        }

        String getCollectionName() {
            return format("%s_%05d", testInfo.getTestMethod().orElseThrow().getName(), testCounter.get());
        }

        void processMaintenance() {
            // speed up using parallel stream
            distributedCacheInstances.stream().parallel().forEach(distributedCache -> {
                distributedCache.cleanUp();
                InternalInstanceRegistry<?, ?> instanceRegistry = getInstanceRegistry(distributedCache);
                InternalMaintenanceWorker<?, ?> maintenanceWorker = instanceRegistry.getMaintenanceWorker();
                invokeMethod(maintenanceWorker, InternalMaintenanceWorker.class,
                        "processMaintenance", List.of(Duration.class), List.of(Duration.ZERO));
            });
        }

        void cleanUp() {
            distributedCacheInstances.forEach(DistributedCache::cleanUp);
        }

        void assertThatDataStoreHasCounts(Count... counts) {
            Repository<?, ?> repository = getInstanceRegistry(distributedCacheInstances.stream()
                    .findFirst()
                    .orElseThrow())
                    .getAdapter()
                    .getRepository();
            Map<Status, Count> statusToCount = Stream.of(counts)
                    .collect(toMap(Count::status, Function.identity()));
            Stream.of(Status.values())
                    .map(status -> statusToCount.getOrDefault(status,
                            Count.of(status, assertion -> assertion.isEqualTo(0))))
                    .forEach(count -> count.assertion()
                            .apply(assertThat(getFailable(() ->
                                    // TODO discriminator
                                    repository.countCacheEntries(null, Set.of(count.status()))))
                                    .describedAs("%nCount for %s", count.status().name())));
        }

        void assertThatDataStoreHasCounts(CountGrouped... countsGrouped) {
            Repository<?, ?> repository = getInstanceRegistry(distributedCacheInstances.stream()
                    .findFirst()
                    .orElseThrow())
                    .getAdapter()
                    .getRepository();
            Stream.of(countsGrouped)
                    .forEach(count -> count.assertion()
                            .apply(assertThat(getFailable(() ->
                                    // TODO discriminator
                                    repository.countCacheEntries(null, count.statuses())))
                                    .describedAs("%nCount for %s", count.statuses())));
        }

        <K, V> InternalInstanceRegistry<K, V> getInstanceRegistry(DistributedCache<K, V> distributedCache) {
            return ((InternalDistributedCache<K, V>) distributedCache).instanceRegistry;
        }

        void executeRandomOperation(DistributedCache<Key, Value> distributedCache, int cacheSize) {
            List<Runnable> operations = listOperations(distributedCache, cacheSize);
            operations.get(nextInt(operations.size())).run();
        }

        List<Runnable> listOperations(DistributedCache<Key, Value> distributedCache, int cacheSize) {
            List<Runnable> operations = new ArrayList<>();
            operations.add(() -> {
                int addId = nextInt(cacheSize, 2 * cacheSize);
                distributedCache.put(Key.of(addId), Value.of(addId, nameWithMillisAndPrefixes("add")));
            });
            operations.add(() -> {
                int addId1 = nextInt(cacheSize, cacheSize + cacheSize / 2);
                int addId2 = nextInt(cacheSize + cacheSize / 2, 2 * cacheSize);
                distributedCache.putAll(Map.of(
                        Key.of(addId1), Value.of(addId1, nameWithMillisAndPrefixes("add")),
                        Key.of(addId2), Value.of(addId2, nameWithMillisAndPrefixes("add"))));
            });
            operations.add(() -> {
                int updateId = nextInt(cacheSize);
                distributedCache.put(Key.of(updateId), Value.of(updateId, nameWithMillisAndPrefixes("update")));
            });
            operations.add(() -> {
                int updateId1 = nextInt(0, cacheSize / 2);
                int updateId2 = nextInt(cacheSize / 2, cacheSize);
                distributedCache.putAll(Map.of(
                        Key.of(updateId1), Value.of(updateId1, nameWithMillisAndPrefixes("update")),
                        Key.of(updateId2), Value.of(updateId2, nameWithMillisAndPrefixes("update"))));
            });
            operations.add(() -> {
                int mappingId = nextInt(cacheSize * 2);
                distributedCache.get(Key.of(mappingId), key -> Value.of(mappingId, nameWithMillisAndPrefixes("update")));
            });
            operations.add(() -> {
                int mappingId1 = nextInt(cacheSize);
                int mappingId2 = nextInt(cacheSize, cacheSize * 2);
                distributedCache.getAll(Set.of(Key.of(mappingId1), Key.of(mappingId2)), keys -> Map.of(
                        Key.of(mappingId1), Value.of(mappingId1, nameWithMillisAndPrefixes("update")),
                        Key.of(mappingId2), Value.of(mappingId2, nameWithMillisAndPrefixes("update"))));
            });
            operations.add(() -> {
                int removeId = nextInt(cacheSize);
                distributedCache.invalidate(Key.of(removeId));
            });
            operations.add(() -> {
                int removeId1 = nextInt(0, cacheSize / 2);
                int removeId2 = nextInt(cacheSize / 2, cacheSize);
                distributedCache.invalidateAll(Set.of(Key.of(removeId1), Key.of(removeId2)));
            });
            if (distributedCache.policy().expireVariably().isPresent()) {
                operations.add(() -> {
                    int expireId = nextInt(cacheSize);
                    distributedCache.policy().expireVariably().orElseThrow().setExpiresAfter(Key.of(expireId), Duration.ZERO);
                });
            }
            if (distributedCache instanceof DistributedLoadingCache<Key, Value> distributedLoadingCache) {
                operations.add(() -> {
                    int loadId = nextInt(cacheSize, 2 * cacheSize);
                    distributedLoadingCache.get(Key.of(loadId));
                });
                operations.add(() -> {
                    int loadId1 = nextInt(cacheSize, cacheSize + cacheSize / 2);
                    int loadId2 = nextInt(cacheSize + cacheSize / 2, 2 * cacheSize);
                    distributedLoadingCache.getAll(Set.of(Key.of(loadId1), Key.of(loadId2)));
                });
                operations.add(() -> {
                    int refreshId = nextInt(0, cacheSize);
                    distributedLoadingCache.refresh(Key.of(refreshId));
                });
                operations.add(() -> {
                    int refreshId1 = nextInt(0, cacheSize / 2);
                    int refreshId2 = nextInt(cacheSize / 2, cacheSize);
                    distributedLoadingCache.refreshAll(Set.of(Key.of(refreshId1), Key.of(refreshId2)));
                });
            }
            return operations;
        }

        int nextInt(int bound) {
            return secureRandom.nextInt(bound);
        }

        int nextInt(int origin, int bound) {
            return secureRandom.nextInt(bound - origin) + origin;
        }

        String nameWithMillisAndPrefixes(String... prefixes) {
            String delimiter = "_";
            String prefix = String.join(delimiter, prefixes);
            String time = Instant.now().toString();
            return prefix.isBlank()
                    ? time
                    : String.join(delimiter, prefix, time);
        }

        <T, R> R injectSpy(Object instanceObject, Class<T> instanceClass, String fieldName, Class<? super R> fieldClass) {
            R spy = spy(readFieldValue(instanceObject, instanceClass, fieldName, fieldClass));
            writeFieldValue(instanceObject, instanceClass, fieldName, spy);
            return spy;
        }

        @SuppressWarnings("unchecked")
        <T, R> R readFieldValue(Object instanceObject, Class<T> instanceClass, String fieldName, Class<? super R> fieldClass) {
            return (R) ReflectionUtils.tryToReadFieldValue(instanceClass, fieldName, instanceClass.cast(instanceObject))
                    .toOptional()
                    .filter(fieldClass::isInstance)
                    .orElseThrow(NoSuchFieldError::new);
        }

        <T> void writeFieldValue(Object instanceObject, Class<T> instanceClass, String fieldName, Object fieldValue) {
            Predicate<Field> fieldPredicate = field -> field.getName().equals(fieldName);
            Field field = ReflectionUtils.streamFields(instanceClass, fieldPredicate, HierarchyTraversalMode.TOP_DOWN)
                    .findFirst()
                    .orElseThrow(NoSuchFieldError::new);
            ReflectionUtils.makeAccessible(field);
            runFailable(() -> field.set(instanceObject, fieldValue));
        }

        @SuppressWarnings({"unchecked", "UnusedReturnValue", "SameParameterValue"})
        <T, R> R invokeMethod(Object instanceObject, Class<T> instanceClass, String methodName, List<Class<?>> parameterClasses, List<Object> parameterObjects) {
            return (R) ReflectionUtils.invokeMethod(
                    ReflectionUtils.findMethod(instanceClass, methodName, parameterClasses.toArray(Class[]::new))
                            .orElseThrow(NoSuchMethodError::new),
                    instanceObject, parameterObjects.toArray(Object[]::new));
        }

        @SuppressWarnings("unused")
        <K, V> void printMongoCollection(Status... statuses) {
            AtomicInteger counter = new AtomicInteger(0);
            Repository<?, ?> repository = distributedCacheInstances.stream()
                    .findFirst()
                    .map(this::getInstanceRegistry)
                    .map(InternalInstanceRegistry::getAdapter)
                    .map(Adapter::getRepository)
                    .orElseThrow();
            Set<Status> statusesOrNull = statuses.length == 0
                    ? null
                    : Set.of(statuses);
            try (Stream<? extends CacheEntry<?, ?>> cacheEntryStream = getFailable(() ->
                    repository.streamCacheEntries(null, null, statusesOrNull, null, false))) {
                cacheEntryStream.forEach(cacheEntry ->
                        System.out.printf("%05d %s%n", counter.incrementAndGet(), cacheEntry));
            }
        }

        <T> T runsOnGitHub(T yes, T no) {
            return Boolean.parseBoolean(getProperty(RUNS_ON_GITHUB, Boolean.toString(false)))
                    ? yes
                    : no;
        }

        record DistributedCaffeineConfiguration<K, V>(String displayName, CacheBuilder<K, V> cacheBuilder) {
        }

        static class EqualResult<K, V> {

            private boolean initialized = false;
            private Object object;
            private K key;
            private V value;
            private Entry<K, V> entry;
            private Map<K, V> map;

            void setObject(Object object) {
                if (initialized) {
                    assertThat(this.object).isEqualTo(object);
                }
                this.object = object;
                this.initialized = true;
            }

            void setKey(K key) {
                if (initialized) {
                    assertThat(this.key).isEqualTo(key);
                }
                this.key = key;
                this.initialized = true;
            }

            void setValue(V value) {
                if (initialized) {
                    assertThat(this.value).isEqualTo(value);
                }
                this.value = value;
                this.initialized = true;
            }

            void setEntry(Entry<K, V> entry) {
                if (initialized) {
                    assertThat(this.entry).isEqualTo(entry);
                }
                this.entry = entry;
                this.initialized = true;
            }

            void setMap(Map<K, V> map) {
                if (initialized) {
                    assertThat(this.map).containsExactlyInAnyOrderEntriesOf(map);
                }
                this.map = map;
                this.initialized = true;
            }

            @SuppressWarnings("unchecked")
            <T> T getObject() {
                return (T) object;
            }

            K getKey() {
                return key;
            }

            V getValue() {
                return value;
            }

            public Entry<K, V> getEntry() {
                return entry;
            }

            Map<K, V> getMap() {
                return map;
            }

            void reset() {
                initialized = false;
                object = null;
                key = null;
                value = null;
                entry = null;
                map = null;
            }
        }

        record Count(Status status, Function<AbstractLongAssert<?>, AbstractLongAssert<?>> assertion) {

            static Count of(Status status, Function<AbstractLongAssert<?>, AbstractLongAssert<?>> assertion) {
                return new Count(status, assertion);
            }

            static Count[] empty() {
                return new Count[]{};
            }
        }

        record CountGrouped(Set<Status> statuses, Function<AbstractLongAssert<?>, AbstractLongAssert<?>> assertion) {

            static CountGrouped of(Set<Status> statuses,
                                   Function<AbstractLongAssert<?>, AbstractLongAssert<?>> assertion) {
                return new CountGrouped(statuses, assertion);
            }
        }

        @Target(ElementType.TYPE)
        @Retention(RetentionPolicy.RUNTIME)
        @Inherited
        public @interface DockerImage {

            String value();
        }
    }
}
