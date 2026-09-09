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
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Sorts;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.CachedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractSynchronizer;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntryMetadata;
import io.github.oberhoff.distributedcaffeine.adapter.DiscriminatorAware;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.adapter.Receiver;
import io.github.oberhoff.distributedcaffeine.adapter.Synchronizer;
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
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.assertj.core.api.AbstractLongAssert;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.core.type.TypeReference;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer.LoadingStrategy.CACHE_LOADER;
import static io.github.oberhoff.distributedcaffeine.DistributedCaffeineIntegrationTests.DistributedCaffeineIntegrationTestInstance.DockerImage;
import static io.github.oberhoff.distributedcaffeine.DistributedCaffeineIntegrationTests.DistributedCaffeineIntegrationTestInstance.RUNS_ON_GITHUB;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.entry;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_LOADED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.COMMAND;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_RETAINED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE_RETAINED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME_RETAINED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.DISTRIBUTION_ONLY_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.STALE;
import static io.github.oberhoff.distributedcaffeine.adapter.DiscriminatorAware.DEFAULT_DISCRIMINATOR;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.time.temporal.ChronoUnit.FOREVER;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
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

    @SuppressWarnings({"java:S5838", "java:S5778", "java:S5961", "ResultOfMethodCallIgnored", "DataFlowIssue"})
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

            assertThatDataStoreIsEmpty();
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
            //noinspection ExtractMethodRecommender
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

            assertThatDataStoreIsEmpty();
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
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("computed"));
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

            assertThatDataStoreIsEmpty();
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
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(5)),
                                Count.of(COMMAND, assertion -> assertion.isEqualTo(1)));
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
                public Value load(@NonNull Key key) throws Exception {
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
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("loaded"));
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

            assertThatDataStoreIsEmpty();
        }

        @DisplayName("Test getAll() with cache loader")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedLoadingCache_getAll_with_cache_loader(CacheFactory<Key, Value> cacheFactory) throws Exception {
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(@NonNull Key key) throws Exception {
                    throw new UnsupportedOperationException(); // ensure load() is never invoked
                }

                @Override
                @SuppressWarnings("RedundantThrows")
                public @NonNull Map<? extends Key, ? extends Value> loadAll(@NonNull Set<? extends Key> keys) throws Exception {
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

            assertThatDataStoreIsEmpty();
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
                public Value load(@NonNull Key key) throws Exception {
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
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("loaded"));
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
                                Count.of(CACHED_REFRESHED, assertion -> assertion.isEqualTo(1)),
                                // key2 is refreshed to nothing (its cache loader returns null), which is an
                                // invalidation and therefore reaches the underlying store even though no cache
                                // instance ever held that key
                                Count.of(INVALIDATED_REFRESHED, assertion -> assertion.isEqualTo(1)));
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
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("reloaded"));
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
                                // still the one written for key2 further above, nothing has cleaned it up yet
                                Count.of(INVALIDATED_REFRESHED, assertion -> assertion.isEqualTo(1)),
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
                                // key1 now refreshes to nothing as well, joining the one written for key2
                                Count.of(INVALIDATED_REFRESHED, assertion -> assertion.isEqualTo(2)),
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
                                            .satisfies(value -> assertThat(value.getId()).isLessThan(levelOfParallelism))
                                            .satisfies(value -> assertThat(value.getName()).isEqualTo("counted"))));
        }

        @DisplayName("Test refresh() coalesces concurrent operations per key")
        @Test
        void test_DistributedLoadingCache_refresh_coalesces_concurrent_operations_per_key() throws Exception {
            CountDownLatch reloadStarted = new CountDownLatch(1);
            CountDownLatch releaseReload = new CountDownLatch(1);
            AtomicInteger reloadInvocations = new AtomicInteger(0);

            // reload blocks until released, so multiple refresh() calls issued in the meantime coalesce onto the
            // single in-flight operation for the key
            CacheLoader<Key, Value> cacheLoader = new CacheLoader<>() {
                @Override
                public Value load(Key key) {
                    return Value.of(key.getId());
                }

                @Override
                public @NonNull CompletableFuture<? extends Value> asyncReload(@NonNull Key key, @NonNull Value oldValue, @NonNull Executor executor) {
                    reloadInvocations.incrementAndGet();
                    return CompletableFuture.supplyAsync(() -> {
                        reloadStarted.countDown();
                        try {
                            releaseReload.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new CompletionException(e);
                        }
                        return Value.of(key.getId(), "reloaded");
                    }, executor);
                }
            };

            DistributedLoadingCache<Key, Value> distributedLoadingCache = (DistributedLoadingCache<Key, Value>) this.<Key, Value>createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                            .recordStats()),
                    dc -> dc.build(cacheLoader));

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            distributedLoadingCache.put(key1, value1);

            // first refresh starts the (blocked) reload
            CompletableFuture<Value> refresh1 = distributedLoadingCache.refresh(key1);
            reloadStarted.await();
            // further refreshes while the reload is in flight must coalesce onto the same operation
            CompletableFuture<Value> refresh2 = distributedLoadingCache.refresh(key1);
            CompletableFuture<Value> refresh3 = distributedLoadingCache.refresh(key1);

            releaseReload.countDown();
            CompletableFuture.allOf(refresh1, refresh2, refresh3).join();

            await("coalesced refresh")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        // three refresh() calls for the same key triggered only one reload
                        assertThat(reloadInvocations).hasValue(1);
                        // and a load success is recorded exactly once, not once per coalesced refresh
                        assertThat(distributedLoadingCache.stats().loadSuccessCount()).isEqualTo(1);
                    });
        }

        @DisplayName("Test rollback of a failed activation")
        @Test
        void test_DistributedCaffeine_failed_activation_rolls_back() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            InternalInstanceRegistry<Key, Value> instanceRegistry = getInstanceRegistry(distributedCache);
            InternalMaintenanceWorker<Key, Value> maintenanceWorker = instanceRegistry.getMaintenanceWorker();
            InternalCacheManager<Key, Value> cacheManager = instanceRegistry.getCacheManager();

            distributedCache.distributedPolicy().stopSynchronization();

            // let the adapter fail, which happens only after the cache manager and the maintenance worker have
            // already been activated
            Synchronizer<Key, Value> synchronizerSpy = injectSpy(instanceRegistry.getAdapter(),
                    AbstractAdapter.class, "synchronizer", Synchronizer.class);
            doThrow(new IllegalStateException("activation failed")).when(synchronizerSpy).activate();

            assertThatThrownBy(() -> distributedCache.distributedPolicy().startSynchronization())
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .hasMessage("activation failed");

            // whatever came up before the failure has to be taken down again, because isActivated() requires all
            // three components: a half activated instance reports false, which makes deactivate() skip its body and
            // leaves those components running with no way to stop them from the outside
            assertThat(maintenanceWorker.isActivated()).isFalse();
            assertThat(cacheManager.isActivated()).isFalse();
            assertThat(instanceRegistry.isActivated()).isFalse();

            // and the instance stays usable: activating again must not join the maintenance worker future of the
            // failed attempt, which never completes while that worker still considers itself activated
            doCallRealMethod().when(synchronizerSpy).activate();
            distributedCache.distributedPolicy().startSynchronization();

            assertThat(instanceRegistry.isActivated()).isTrue();
            distributedCache.put(Key.of(1), Value.of(1));
            assertThat(distributedCache.getIfPresent(Key.of(1))).isEqualTo(Value.of(1));

            // the same has to hold in the other direction: stopping the adapter on its own (which its public API
            // allows) must not stop stopSynchronization() from taking the remaining components down as well
            instanceRegistry.getAdapter().deactivate();
            distributedCache.distributedPolicy().stopSynchronization();

            assertThat(maintenanceWorker.isActivated()).isFalse();
            assertThat(cacheManager.isActivated()).isFalse();
        }

        @DisplayName("Test refresh() with a same-thread executor")
        @Test
        void test_DistributedLoadingCache_refresh_with_same_thread_executor() {
            CacheLoader<Key, Value> cacheLoader = key -> Value.of(key.getId(), "reloaded");

            DistributedLoadingCache<Key, Value> distributedLoadingCache = (DistributedLoadingCache<Key, Value>) this.<Key, Value>createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                            .executor(Runnable::run)
                            .recordStats()),
                    dc -> dc.build(cacheLoader));

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            distributedLoadingCache.put(key1, value1);

            // a same-thread executor runs the reload synchronously, so the refresh operation is already complete by
            // the time its completion callback is attached and that callback runs inline on this thread
            Value refreshedValue = distributedLoadingCache.refresh(key1).join();

            assertThat(refreshedValue).isEqualTo(Value.of(1, "reloaded"));
            assertThat(distributedLoadingCache.getIfPresent(key1)).isEqualTo(Value.of(1, "reloaded"));
            assertThat(distributedLoadingCache.stats().loadSuccessCount()).isEqualTo(1);

            // the callback still has to clean up after itself, which it cannot do from inside the mapping function
            // of the very map it removes from
            ConcurrentMap<?, ?> refreshOperations = readFieldValue(distributedLoadingCache,
                    InternalDistributedLoadingCache.class, "refreshOperations", ConcurrentMap.class);
            assertThat(refreshOperations).isEmpty();
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
                public Value load(@NonNull Key key) throws Exception {
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
                                Count.of(CACHED_REFRESHED, assertion -> assertion.isEqualTo(1)),
                                // key2 is refreshed to nothing (its cache loader returns null), which is an
                                // invalidation and therefore reaches the underlying store even though no cache
                                // instance ever held that key
                                Count.of(INVALIDATED_REFRESHED, assertion -> assertion.isEqualTo(1)));
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
                                // still the one written for key2 further above, nothing has cleaned it up yet
                                Count.of(INVALIDATED_REFRESHED, assertion -> assertion.isEqualTo(1)),
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
                                // key1 now refreshes to nothing as well, joining the one written for key2
                                Count.of(INVALIDATED_REFRESHED, assertion -> assertion.isEqualTo(2)),
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
                                            .satisfies(value -> assertThat(value.getId()).isLessThan(levelOfParallelism))
                                            .satisfies(value -> assertThat(value.getName()).isEqualTo("counted"))));
        }

        @DisplayName("Test refreshAll() applies successful keys despite a failing key")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_DistributedLoadingCache_refreshAll_applies_successful_keys_on_partial_failure() {
            Key key1 = Key.of(1);
            Key key2 = Key.of(2);
            Value value1 = Value.of(1);
            Value value2 = Value.of(2);

            // reload succeeds for key1 and fails for key2, so a single refreshAll() call mixes both outcomes
            CacheLoader<Key, Value> cacheLoader = new CacheLoader<>() {
                @Override
                public Value load(Key key) {
                    return Value.of(key.getId());
                }

                @Override
                public @NonNull CompletableFuture<? extends Value> asyncReload(Key key, @NonNull Value oldValue, @NonNull Executor executor) {
                    return key.equals(key2)
                            ? CompletableFuture.failedFuture(new IllegalStateException("unchecked"))
                            : CompletableFuture.completedFuture(Value.of(key.getId(), "reloaded"));
                }
            };

            DistributedLoadingCache<Key, Value> distributedLoadingCache =
                    (DistributedLoadingCache<Key, Value>) this.<Key, Value>createCache(
                            CacheBuilder.identity(),
                            dc -> dc.build(cacheLoader));
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache =
                    (DistributedLoadingCache<Key, Value>) this.<Key, Value>createCache(
                            CacheBuilder.identity(),
                            dc -> dc.build(cacheLoader));
            LoadingCache<Key, Value> caffeineLoadingCache = Caffeine.newBuilder()
                    .build(cacheLoader);

            Set<LoadingCache<Key, Value>> allCaches = Set.of(distributedLoadingCache, syncedDistributedLoadingCache,
                    caffeineLoadingCache);
            Set<LoadingCache<Key, Value>> featureParityCaches = Set.of(distributedLoadingCache, caffeineLoadingCache);

            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);
            CaptureLogger loggerLocalLoadingCache = CaptureLoggerFactory
                    .getCaptureLogger("com.github.benmanes.caffeine.cache.LocalLoadingCache");

            loggerDistributedCaffeine.startCapturing();
            loggerLocalLoadingCache.startCapturing();

            featureParityCaches.forEach(loadingCache -> {
                loadingCache.put(key1, value1);
                loadingCache.put(key2, value2);
                // the aggregated future still fails, exactly as before, ...
                assertThatThrownBy(() -> loadingCache.refreshAll(Set.of(key1, key2)).join())
                        .isExactlyInstanceOf(CompletionException.class)
                        .hasCauseInstanceOf(IllegalStateException.class);
            });

            loggerDistributedCaffeine.stopCapturing();
            loggerLocalLoadingCache.stopCapturing();

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> allCaches.forEach(loadingCache -> {
                        // ... but the key that reloaded successfully must still be applied locally and distributed,
                        // instead of being discarded because a sibling key failed
                        assertThat(loadingCache.getIfPresent(key1)).isEqualTo(Value.of(1, "reloaded"));
                        // while the failing key keeps its current value, just like plain Caffeine
                        assertThat(loadingCache.getIfPresent(key2)).isEqualTo(value2);
                    }));
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
                public Value load(@NonNull Key key) throws Exception {
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
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("reloaded"));
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

            assertThatDataStoreIsEmpty();

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
                public Value load(@NonNull Key key) throws Exception {
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

            assertThatDataStoreIsEmpty();

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

            assertThatDataStoreIsEmpty();
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

            assertThatDataStoreIsEmpty();
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
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(3)),
                                Count.of(COMMAND, assertion -> assertion.isEqualTo(1)));
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.empty());
        }

        @DisplayName("Test computeIfAbsent(), computeIfPresent(), compute() and merge()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_compute_and_merge(CacheFactory<Key, Value> cacheFactory) {
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
            Key key3 = Key.of(3);
            Key key4 = Key.of(4);
            Value value4 = Value.of(4);
            Key key5 = Key.of(5);
            Key key6 = Key.of(6);
            Value value6 = Value.of(6);
            Key key7 = Key.of(7);
            Value value7 = Value.of(7);
            Value value7updated = Value.of(7, "updated");
            Key key8 = Key.of(8);
            Value value8 = Value.of(8);
            Key key9 = Key.of(9);
            Value value9 = Value.of(9);
            Value value9updated = Value.of(9, "updated");
            Key key10 = Key.of(10);
            Value value10 = Value.of(10);
            Key key11 = Key.of(11);
            Value value11 = Value.of(11);
            Key key12 = Key.of(12);
            Value value12 = Value.of(12);
            Value value12merged = Value.of(12, "merged");

            // computeIfAbsent: absent -> computed / present -> not computed / mapping returns null -> no mapping
            EqualResult<Key, Value> computeIfAbsent1x1 = new EqualResult<>();
            EqualResult<Key, Value> computeIfAbsent1x2 = new EqualResult<>();
            EqualResult<Key, Value> computeIfAbsent2x1 = new EqualResult<>();
            // computeIfPresent: absent -> not computed / present -> updated / remapping returns null -> removed
            EqualResult<Key, Value> computeIfPresent3x1 = new EqualResult<>();
            EqualResult<Key, Value> computeIfPresent7x1 = new EqualResult<>();
            EqualResult<Key, Value> computeIfPresent8x1 = new EqualResult<>();
            // compute: absent -> computed / absent, returns null -> no mapping / present -> updated / present, returns null -> removed
            EqualResult<Key, Value> compute4x1 = new EqualResult<>();
            EqualResult<Key, Value> compute5x1 = new EqualResult<>();
            EqualResult<Key, Value> compute9x1 = new EqualResult<>();
            EqualResult<Key, Value> compute10x1 = new EqualResult<>();
            // merge: absent -> value (remap not called) / present -> remapped / remapping returns null -> removed
            EqualResult<Key, Value> merge6x1 = new EqualResult<>();
            EqualResult<Key, Value> merge12x1 = new EqualResult<>();
            EqualResult<Key, Value> merge11x1 = new EqualResult<>();

            featureParityMaps.forEach(map -> {
                assertThatNullPointerException().isThrownBy(() -> map.computeIfAbsent(_null(), key -> Value.of(0)));
                assertThatNullPointerException().isThrownBy(() -> map.computeIfAbsent(Key.of(0), _null()));
                assertThatNullPointerException().isThrownBy(() -> map.computeIfPresent(_null(), (key, value) -> value));
                assertThatNullPointerException().isThrownBy(() -> map.computeIfPresent(Key.of(0), _null()));
                assertThatNullPointerException().isThrownBy(() -> map.compute(_null(), (key, value) -> value));
                assertThatNullPointerException().isThrownBy(() -> map.compute(Key.of(0), _null()));
                assertThatNullPointerException().isThrownBy(() -> map.merge(_null(), Value.of(0), (oldValue, value) -> value));
                assertThatNullPointerException().isThrownBy(() -> map.merge(Key.of(0), _null(), (oldValue, value) -> value));
                assertThatNullPointerException().isThrownBy(() -> map.merge(Key.of(0), Value.of(0), _null()));

                // computeIfAbsent
                computeIfAbsent1x1.setValue(map.computeIfAbsent(key1, key -> value1));                    // absent -> computed
                computeIfAbsent1x2.setValue(map.computeIfAbsent(key1, key -> Value.of(0)));                // present -> not computed
                computeIfAbsent2x1.setValue(map.computeIfAbsent(key2, key -> null));                       // absent, returns null -> no mapping

                // computeIfPresent
                computeIfPresent3x1.setValue(map.computeIfPresent(key3, (key, value) -> Value.of(0)));     // absent -> not computed
                map.put(key7, value7);
                computeIfPresent7x1.setValue(map.computeIfPresent(key7, (key, value) -> value7updated));   // present -> updated
                map.put(key8, value8);
                computeIfPresent8x1.setValue(map.computeIfPresent(key8, (key, value) -> null));            // present, returns null -> removed

                // compute
                compute4x1.setValue(map.compute(key4, (key, value) -> value4));                            // absent -> computed
                compute5x1.setValue(map.compute(key5, (key, value) -> null));                              // absent, returns null -> no mapping
                map.put(key9, value9);
                compute9x1.setValue(map.compute(key9, (key, value) -> value9updated));                     // present -> updated
                map.put(key10, value10);
                compute10x1.setValue(map.compute(key10, (key, value) -> null));                            // present, returns null -> removed

                // merge
                merge6x1.setValue(map.merge(key6, value6, (oldValue, value) -> Value.of(0)));              // absent -> value (remap not called)
                map.put(key12, value12);
                merge12x1.setValue(map.merge(key12, Value.of(0), (oldValue, value) -> value12merged));     // present -> remapped
                map.put(key11, value11);
                merge11x1.setValue(map.merge(key11, Value.of(0), (oldValue, value) -> null));              // present, remap returns null -> removed
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map -> {
                            assertThat(map).hasSize(6);
                            assertThat(map.get(key1)).isEqualTo(value1);
                            assertThat(map.get(key2)).isNull();
                            assertThat(map.get(key3)).isNull();
                            assertThat(map.get(key4)).isEqualTo(value4);
                            assertThat(map.get(key5)).isNull();
                            assertThat(map.get(key6)).isEqualTo(value6);
                            assertThat(map.get(key7)).isEqualTo(value7updated);
                            assertThat(map.get(key8)).isNull();
                            assertThat(map.get(key9)).isEqualTo(value9updated);
                            assertThat(map.get(key10)).isNull();
                            assertThat(map.get(key11)).isNull();
                            assertThat(map.get(key12)).isEqualTo(value12merged);
                            assertThat(computeIfAbsent1x1.getValue()).isEqualTo(value1);
                            assertThat(computeIfAbsent1x2.getValue()).isEqualTo(value1);
                            assertThat(computeIfAbsent2x1.getValue()).isNull();
                            assertThat(computeIfPresent3x1.getValue()).isNull();
                            assertThat(computeIfPresent7x1.getValue()).isEqualTo(value7updated);
                            assertThat(computeIfPresent8x1.getValue()).isNull();
                            assertThat(compute4x1.getValue()).isEqualTo(value4);
                            assertThat(compute5x1.getValue()).isNull();
                            assertThat(compute9x1.getValue()).isEqualTo(value9updated);
                            assertThat(compute10x1.getValue()).isNull();
                            assertThat(merge6x1.getValue()).isEqualTo(value6);
                            assertThat(merge12x1.getValue()).isEqualTo(value12merged);
                            assertThat(merge11x1.getValue()).isNull();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(6)),
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(3)));
                    });

            processMaintenance();

            assertThatDataStoreIsEmpty();
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
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(6)),
                                Count.of(COMMAND, assertion -> assertion.isEqualTo(1)));
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
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(6)),
                                Count.of(COMMAND, assertion -> assertion.isEqualTo(1)));
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
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(6)),
                                Count.of(COMMAND, assertion -> assertion.isEqualTo(1)));
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
                        cacheEntry.setValue(Value.of(0)));
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
                            assertThat(policy.getEntryIfPresentQuietly(key1).expiresAt())
                                    .isPositive();
                            assertThat(policy.getIfPresentQuietly(key2)).isEqualTo(value2)
                                    .isEqualTo(oldValue2x2.getValue())
                                    .isNotEqualTo(oldValue2x1.getValue());
                            assertThat(policy.getEntryIfPresentQuietly(key2).expiresAt())
                                    .isPositive();
                            assertThat(policy.getIfPresentQuietly(key3)).isEqualTo(computedValue3.getValue())
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("computed"));
                            assertThat(policy.getEntryIfPresentQuietly(key3).expiresAt())
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

            assertThatDataStoreIsEmpty();
        }

        @DisplayName("Test distributedPolicy()")
        @Test
        @SuppressWarnings("EqualsIncompatibleType") // comparing unrelated types is the point of the equals() contract test
        void test_DistributedPolicy() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration())))
                            .withPersistence(configurer -> configurer
                                    // both tiers, because both are what getFromStore reports on
                                    .withCachedEntries(cachedEntries -> cachedEntries
                                            .withMaximumTime(FOREVER.getDuration()))
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumTime(FOREVER.getDuration()))),
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
                                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(1))));

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
                                .allSatisfy(entry -> assertThat(entry.getHash()).isNotBlank())
                                .allSatisfy(entry -> assertThat(entry.getOperation()).isNotNull())
                                .allSatisfy(entry -> assertThat(entry.getKey()).isEqualTo(key1))
                                .allSatisfy(entry -> assertThat(entry.getValue()).isEqualTo(value1))
                                .allSatisfy(entry -> assertThat(entry.getStatus()).isEqualTo(CACHED))
                                .allSatisfy(entry -> assertThat(entry.isCached()).isTrue())
                                .allSatisfy(entry -> assertThat(entry.isInvalidated()).isFalse())
                                .allSatisfy(entry -> assertThat(entry.isEvicted()).isFalse())
                                .allSatisfy(entry -> assertThat(entry.isEvictedRetained()).isFalse());
                        assertThat(distributedPolicy.getAllFromStore(Set.of(key1, key2), true))
                                .containsOnly(cacheEntry1, cacheEntry2);
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(1)));
                    });

            processMaintenance();

            // both tiers are configured to hold, so maintenance prunes neither of them
            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(1)));
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
                            // without population being distributed each cache instance holds only the key it
                            // populated itself, so here each of them invalidates a key only the other one holds -
                            // which is exactly what an invalidation has to reach to be of any use in these modes
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

        @DisplayName("Test that a repopulation is not reverted by the invalidation preceding it")
        @Test
        void test_DistributionMode_invalidation_does_not_revert_repopulation() {
            // The invalidation and the repopulation happen one after the other on the same cache instance, so the
            // repopulation is unambiguously the later one.
            // This mode is not interchangeable with the others here, so do not parameterize this test over them:
            // without population being distributed, nothing is written for the repopulation and no cache entry
            // follows the invalidation that could restore what its event removes, which is what makes losing it
            // permanent. Where population is distributed the very same sequence recovers on its own, because the
            // cache entry written for the repopulation follows the invalidated one and is applied after it - so the
            // assertion below would hold there whether or not the ordering it tests exists at all
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withDistributionMode(INVALIDATION),
                    DistributedCaffeine::build);

            Key key = Key.of(1);
            Value value = Value.of(1);

            distributedCache.put(key, Value.of(0));
            distributedCache.invalidate(key);
            distributedCache.put(key, value);

            // no awaiting: the point is not that something arrives eventually but that the repopulation survives the
            // invalidation being delivered back to this very cache instance, so it has to be given time to arrive
            sleep(Duration.ofSeconds(2));

            assertThat(distributedCache.getIfPresent(key)).isEqualTo(value);
        }

        @DisplayName("Test that a repopulation is not reverted by the invalidation of an absent cache entry")
        @Test
        void test_DistributionMode_invalidation_of_absent_does_not_revert_repopulation() {
            // same reasoning about the distribution mode as above: only without population being distributed is
            // losing the repopulation permanent, and therefore observable once everything has settled
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withDistributionMode(INVALIDATION),
                    DistributedCaffeine::build);

            Key key = Key.of(1);
            Value value = Value.of(1);

            distributedCache.invalidate(key); // nothing held here, so nothing is distributed at the moment
            distributedCache.put(key, value);

            sleep(Duration.ofSeconds(2));

            assertThat(distributedCache.getIfPresent(key)).isEqualTo(value);
        }

        @DisplayName("Test that an invalidation is not reverted by a delayed echo of the population preceding it")
        @Test
        void test_DistributionMode_invalidation_is_not_reverted_by_delayed_own_population_echo() {
            // The population and the invalidation happen one after the other on the same cache instance, so the
            // invalidation is unambiguously the later one - but it leaves no value behind for it to be stamped on,
            // so there is nothing left here carrying an operation to compare a delayed echo of the population
            // against. This mode is the one to test it in: only where population is distributed is a cache entry
            // written for it that can be echoed back at all.
            // The echo is handed over rather than awaited, because the change stream applies it long before the
            // invalidation is even issued. What has to be reproduced is an event of the underlying store arriving
            // after a local removal that precedes it - a matter of when it is delivered, not of what it says
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withDistributionMode(POPULATION_AND_INVALIDATION),
                    DistributedCaffeine::build);

            Key key = Key.of(1);
            Value value = Value.of(1);

            distributedCache.put(key, value);

            // the cache entry written for the population, exactly as a cache instance is handed it
            CacheEntry<Key, Value> population;
            try (Stream<CacheEntry<Key, Value>> cacheEntries = getFailable(() ->
                    repositoryOf(distributedCache).streamCacheEntries(null, CACHED_GROUP, false))) {
                population = cacheEntries.findFirst().orElseThrow();
            }

            distributedCache.invalidate(key);

            assertThat(distributedCache.getIfPresent(key)).isNull();

            getInstanceRegistry(distributedCache).getCacheManager()
                    .receiveCacheEntries(List.of(population));

            assertThat(distributedCache.getIfPresent(key)).isNull();
        }

        @DisplayName("Test that an eviction is not upheld by the delayed echo of the population preceding it")
        @Test
        void test_DistributionMode_eviction_is_not_upheld_by_delayed_own_population_echo() {
            // The counterpart of the test above, and the reason its guard is limited to invalidations: a key this
            // cache instance no longer holds because Caffeine evicted it is restored by the very same delayed echo,
            // which is how a cache instance catches up on a key it dropped on its own while the data store still
            // backs it. A guard going by "not held here and published by me" instead of by what was invalidated
            // takes that away and lets the cache instances diverge
            int maximumSize = 1;

            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withDistributionMode(POPULATION_AND_INVALIDATION)
                            .withCaffeine(Caffeine.newBuilder()
                                    .maximumSize(maximumSize)),
                    DistributedCaffeine::build);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCache.put(key1, value1);

            // the cache entry written for the population, exactly as a cache instance is handed it
            CacheEntry<Key, Value> population;
            try (Stream<CacheEntry<Key, Value>> cacheEntries = getFailable(() ->
                    repositoryOf(distributedCache).streamCacheEntries(null, CACHED_GROUP, false))) {
                population = cacheEntries.findFirst().orElseThrow();
            }

            // Kept away from here on, because with a maximum size of one and both keys backed by the data store
            // the echoes would keep restoring whichever key was just evicted and evicting the other one - the very
            // behaviour under test, which cannot be observed while it is also being provoked
            Adapter<Key, Value> adapter = getInstanceRegistry(distributedCache).getAdapter();
            Synchronizer<Key, Value> synchronizer = readFieldValue(adapter, AbstractAdapter.class,
                    "synchronizer", Synchronizer.class);
            Receiver<Key, Value> receiver = injectSpy(synchronizer, AbstractSynchronizer.class,
                    "receiver", Receiver.class);
            doNothing().when(receiver).receiveCacheEntries(anyList());

            distributedCache.put(key2, value2); // implicit eviction

            // driving the clean up along, like the other tests around eviction do: Caffeine performs it when it
            // gets round to it, which under load is not within the waiting duration
            await("eviction by size")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(maximumSize);
                        assertThat(distributedCache.getIfPresent(key1)).isNull();
                    });

            getInstanceRegistry(distributedCache).getCacheManager()
                    .receiveCacheEntries(List.of(population));

            assertThat(distributedCache.getIfPresent(key1)).isEqualTo(value1);
        }

        @DisplayName("Test that invalidating all reaches cache entries held by another cache instance")
        @Test
        void test_DistributionMode_invalidate_all_reaches_other_cache_instances() {
            // Without population being distributed each cache instance holds only what it populated itself, which is
            // what makes this the mode where invalidating all has something to reach that the calling cache instance
            // cannot enumerate: the second key below is held exclusively by the other cache instance and nothing in
            // the store says it exists, so no set of keys assembled here can cover it
            DistributedCache<Key, Value> distributedCacheA = createCache(
                    dc -> dc.withDistributionMode(INVALIDATION),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> distributedCacheB = createCache(
                    dc -> dc.withDistributionMode(INVALIDATION),
                    DistributedCaffeine::build);

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);

            distributedCacheA.put(key1, Value.of(1));
            distributedCacheB.put(key2, Value.of(2));

            distributedCacheA.invalidateAll();

            await("invalidation of all cache entries")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                        assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                    });
        }

        @DisplayName("Test that invalidating all reaches cache entries the calling cache instance no longer holds")
        @Test
        void test_DistributionMode_invalidate_all_reaches_stored_cache_entries() {
            // This mode excludes eviction from being distributed, so a cache entry evicted here locally stays cached
            // in the store and in the other cache instance - a key the calling cache instance cannot enumerate either,
            // this time although the store does know it. Emptying every cache instance is not enough while the store
            // keeps its own record of it, because that record is what a reactivation and a reload read
            // back, so the cached entries have to be gone from the store as well
            DistributedCache<Key, Value> distributedCacheA = createCache(
                    dc -> dc.withDistributionMode(POPULATION_AND_INVALIDATION)
                            .withCaffeine(Caffeine.newBuilder()
                                    .maximumSize(1)),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> distributedCacheB = createCache(
                    dc -> dc.withDistributionMode(POPULATION_AND_INVALIDATION),
                    DistributedCaffeine::build);

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);

            distributedCacheA.put(key1, Value.of(1));
            distributedCacheA.put(key2, Value.of(2));
            distributedCacheA.cleanUp(); // key1 is evicted here, which is not distributed in this mode

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                        assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(2)));
                    });

            distributedCacheA.invalidateAll();

            await("invalidation of all cache entries")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                        assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                        // both keys, the one evicted here included - and no cached entry left behind
                        assertThatDataStoreHasCounts(
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(2)),
                                Count.of(COMMAND, assertion -> assertion.isEqualTo(1)));
                    });
        }

        @DisplayName("Test that a repopulation is not reverted by invalidating all preceding it")
        @Test
        void test_DistributionMode_invalidate_all_does_not_revert_repopulation() {
            // same reasoning about the distribution mode as for the invalidation probes above: only without population
            // being distributed is losing the repopulation permanent, and therefore observable once everything has
            // settled. Invalidating all is ordered against the later actions of this cache instance exactly like an
            // invalidation by key, so what is put afterwards has to survive it being delivered back here
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withDistributionMode(INVALIDATION),
                    DistributedCaffeine::build);

            Key key = Key.of(1);
            Value value = Value.of(1);

            distributedCache.put(key, Value.of(0));
            distributedCache.invalidateAll();
            distributedCache.put(key, value);

            sleep(Duration.ofSeconds(2));

            assertThat(distributedCache.getIfPresent(key)).isEqualTo(value);
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

        @DisplayName("Test that an eviction racing a repopulation converges without reviving what it replaced")
        @Test
        void test_DistributionMode_eviction_racing_repopulation_converges() {
            // Same shape as the invalidation above, only that the removal preceding the repopulation is an eviction:
            // key1 is evicted to make room for key2 and is put again right after, so the repopulation is once more
            // unambiguously the later action of this very cache instance. An eviction is published asynchronously
            // though, so the executor decides when it is written while it takes place when the cache evicts. Delaying
            // the executor pins the interesting order deterministically instead of leaving it to how quickly the
            // executor happens to run: the eviction takes place before the repopulation and is written after it, so
            // the store is left with the eviction as its last word and the repopulation is lost.
            // That loss is accepted rather than fixed. It degrades an explicit put to a miss, which for a cache is
            // recoverable - the value is loaded or put again - and it is the same trade other distributed caches make
            // for the races of their own invalidation protocols. The alternatives are both worse: ordering the two
            // locally keeps the population in this cache instance and in no other one, which is a divergence rather
            // than a loss everybody agrees on, and preventing the write needs a conditional one in the store, which
            // is a lot of machinery for a recoverable miss.
            // So what is asserted here is what has to hold regardless of which of the two writes lands last: every
            // cache instance ends up agreeing, and none of them serves the value the repopulation replaced.
            DistributedCache<Key, Value> distributedCacheA = createCache(
                    dc -> dc.withDistributionMode(POPULATION_AND_INVALIDATION_AND_EVICTION)
                            .withCaffeine(Caffeine.newBuilder()
                                    .maximumSize(1)
                                    .executor(CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS))),
                    DistributedCaffeine::build);
            // population is distributed here, so the other cache instance follows the store for key1 and makes it
            // observable whether the two of them end up agreeing - which a single one never could
            DistributedCache<Key, Value> distributedCacheB = createCache(
                    dc -> dc.withDistributionMode(POPULATION_AND_INVALIDATION_AND_EVICTION),
                    DistributedCaffeine::build);

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);
            Value replacedValue = Value.of(1);
            Value repopulatedValue = Value.of(11);

            distributedCacheA.put(key1, replacedValue);
            distributedCacheA.put(key2, Value.of(2));
            distributedCacheA.cleanUp(); // key1 is evicted here, which is what gets distributed - asynchronously
            distributedCacheA.put(key1, repopulatedValue);
            distributedCacheA.cleanUp();

            // no awaiting: the point is not that something arrives eventually but what everything has settled on, and
            // awaiting would be satisfied by a state passed through on the way there
            sleep(Duration.ofSeconds(2));

            Value fromA = distributedCacheA.getIfPresent(key1);
            Value fromB = distributedCacheB.getIfPresent(key1);

            assertThat(fromA).isNotEqualTo(replacedValue);
            assertThat(fromB).isNotEqualTo(replacedValue);
            assertThat(fromA).isEqualTo(fromB);
        }

        @DisplayName("Test that an entry expiring while synchronization is stopped is not distributed as an eviction")
        @Test
        void test_DistributionMode_expired_entry_is_not_distributed_when_starting_synchronization() {
            // An entry whose expiration has passed counts as gone for every read while it stays resident until
            // maintenance reclaims it, and a cache instance that served locally while synchronization was stopped has
            // no reason to have run any. Such an entry cannot be reached any more either: asMap(),
            // getIfPresentQuietly() and even the expiration policy's oldest() all hide it, and cleanUp() reclaims it
            // only once its expiration lies more than the coarsest expiration bucket in the past. The eviction the
            // underlying cache eventually reports for it is delivered asynchronously, so it can arrive once
            // synchronization has been started again - and distributing it then would drop a cache entry from every
            // other cache instance which the data store still holds as cached and which nobody removed.
            // What prevents that is stopping synchronization marking every entry it can still reach, so that the
            // eviction arriving later is recognized as belonging to the time before synchronization was started again
            // rather than to what happened after it. Which is why this test asserts that starting it leaves the other
            // cache instance and the data store exactly as they were.
            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withDistributionMode(POPULATION_AND_INVALIDATION_AND_EVICTION)
                            .withCaffeine(Caffeine.newBuilder()
                                    .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration())))
                            // what this test asserts is that starting synchronization leaves the data store as it
                            // was, which presupposes that it holds the cache entry at all
                            .withPersistence(configurer -> configurer
                                    .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency));
            DistributedCache<Key, Value> distributedCacheA = createCache(
                    cacheBuilder,
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> distributedCacheB = createCache(
                    cacheBuilder,
                    DistributedCaffeine::build);

            Key key = Key.of(1);
            Value value = Value.of(1);

            distributedCacheA.put(key, value);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(distributedCacheB.getIfPresent(key)).isEqualTo(value));

            distributedCacheB.distributedPolicy().stopSynchronization();

            // expiring it while synchronization is stopped is what leaves the entry behind: nothing reads or writes
            // this cache instance afterwards, so no maintenance of its own reclaims it before it is started again
            distributedCacheB.policy().expireVariably().orElseThrow().setExpiresAfter(key, Duration.ZERO);

            // the state the assertions below are about, asserted rather than assumed so that this test fails loudly
            // instead of becoming vacuous should an expired entry ever stop being resident
            assertThat(distributedCacheB.asMap()).doesNotContainKey(key);
            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);

            distributedCacheB.distributedPolicy().startSynchronization();

            // no awaiting: the point is not that something arrives eventually but what everything has settled on, and
            // awaiting would be satisfied by a state passed through on the way there
            sleep(Duration.ofSeconds(2));

            assertThat(distributedCacheA.getIfPresent(key)).isEqualTo(value);
            assertThat(distributedCacheB.getIfPresent(key)).isEqualTo(value);
            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
        }

        @DisplayName("Test that pruning evicted cache entries does not remove an entry elsewhere")
        @Test
        void test_DistributionMode_pruning_evicted_cache_entries_does_not_remove_entry_elsewhere() {
            // Same pair of cache instances as above, only with evicted cache entries bounded so tightly that
            // maintenance prunes what the eviction put there. Pruning transitions the status rather than deleting the
            // document, so unlike a delete it reaches every cache instance on the change stream - which must not be
            // an invalidation, because the other cache instance still holds the cache entry and this distribution
            // mode excludes exactly that: one cache instance's local eviction removing it everywhere.
            DistributedCache<Key, Value> expiringDistributedCache = createCache(
                    dc -> dc.withDistributionMode(POPULATION_AND_INVALIDATION)
                            .withCaffeine(Caffeine.newBuilder()
                                    .expireAfterWrite(Duration.ofMillis(500)))
                            // cache residency is unavailable in a mode excluding evictions, so retention is bounded
                            // by time instead - generously, since pruning the evicted tier is what is under test
                            .withPersistence(configurer -> configurer
                                    .withCachedEntries(cachedEntries -> cachedEntries
                                            .withMaximumTime(Duration.ofDays(1)))
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumTime(Duration.ofMillis(1)))),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> retainingDistributedCache = createCache(
                    dc -> dc.withDistributionMode(POPULATION_AND_INVALIDATION)
                            .withCaffeine(Caffeine.newBuilder()
                                    .maximumSize(10))
                            .withPersistence(configurer -> configurer
                                    .withCachedEntries(cachedEntries -> cachedEntries
                                            .withMaximumTime(Duration.ofDays(1)))
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumTime(Duration.ofMillis(1)))),
                    DistributedCaffeine::build);

            Key key = Key.of(1);
            Value value = Value.of(1);

            expiringDistributedCache.put(key, value);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(retainingDistributedCache.getIfPresent(key)).isEqualTo(value));

            await("eviction of the expiring cache instance")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(1))));

            // only the pruning, without the sweep processMaintenance() would run after it, so that what
            // pruning leaves behind can be observed before it is removed from the data store
            invokeMethod(getInstanceRegistry(expiringDistributedCache).getMaintenanceWorker(),
                    InternalMaintenanceWorker.class, "processEvictedEntryPersistenceByTime",
                    List.of(Duration.class), List.of(Duration.ZERO.minus(Duration.ofMillis(1))));

            assertThatDataStoreHasCounts(
                    Count.of(STALE, assertion -> assertion.isEqualTo(1)));

            // no awaiting: the point is not that something arrives eventually but what everything has settled on, and
            // awaiting would be satisfied by a state passed through on the way there
            sleep(Duration.ofSeconds(2));

            assertThat(retainingDistributedCache.getIfPresent(key)).isEqualTo(value);
        }

        @DisplayName("Test refresh")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_refresh(CacheFactory<Key, Value> cacheFactory) throws Exception {
            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(@NonNull Key key) throws Exception {
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
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("loaded"));
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2)
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("loaded"));
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
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("loaded"));
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isEqualTo(reloadedValue2)
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("reloaded"));
                            assertThat(distributedLoadingCacheB.getIfPresent(key1)).isEqualTo(reloadedValue1)
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("reloaded"));
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2)
                                    .satisfies(value -> assertThat(value.getName()).isEqualTo("loaded"));
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
                public Value load(@NonNull Key key) throws Exception {
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
                            assertThat(distributedLoadingCacheA.getIfPresent(key1))
                                    .satisfies(value -> {
                                        assertThat(value.getId()).isEqualTo(key1.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(distributedLoadingCacheA.getIfPresent(key2))
                                    .satisfies(value -> {
                                        assertThat(value.getId()).isEqualTo(key2.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(distributedLoadingCacheB.getIfPresent(key1))
                                    .satisfies(value -> {
                                        assertThat(value.getId()).isEqualTo(key1.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(distributedLoadingCacheB.getIfPresent(key2))
                                    .satisfies(value -> {
                                        assertThat(value.getId()).isEqualTo(key2.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_REFRESHED_AFTER_WRITE, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1))
                                    .satisfies(value -> {
                                        assertThat(value.getId()).isEqualTo(key1.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isNull();
                            assertThat(distributedLoadingCacheB.getIfPresent(key1)).isNull();
                            assertThat(distributedLoadingCacheB.getIfPresent(key2))
                                    .satisfies(value -> {
                                        assertThat(value.getId()).isEqualTo(key2.getId());
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

        @DisplayName("Test persistence of evicted entries by size")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_evicted_entry_persistence_by_size(CacheFactory<Key, Value> cacheFactory) throws Exception {
            int maximumSize = 1;
            int retainedMaximumSize = 2;

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .evictionListener(evictionListener)
                                    .maximumSize(maximumSize))
                            .withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumSize(retainedMaximumSize)
                                            // just to test the distinction in logic
                                            .withMaximumTime(FOREVER.getDuration())
                                            .withLoadingStrategies(CACHE_LOADER)));

            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(@NonNull Key key) throws Exception {
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(1)));
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(1)));
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(2)));
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(1)));
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(2)));
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(1)));
                        }
                    });

            // create more cache entries (retained by size) than the maximum size allows
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromStore(key4, false)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue4));
                            assertThat(distributedPolicy.getFromStore(key4, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue4));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(3)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key4)).isEqualTo(loadedValue4);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNull();
                            assertThat(distributedPolicy.getFromStore(key4, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key4, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(2)));
                        }
                    });

            processMaintenance();

            if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                        Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(retainedMaximumSize)));
            } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                    distributionMode.equals(INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(retainedMaximumSize)));
            }

            // test cache without loading strategy
            DistributedLoadingCache<Key, Value> distributedLoadingCacheWithoutLoadingStrategy = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    dc -> dc.withPersistence(configurer -> configurer
                            .withEvictedEntries(DistributedCaffeine.EvictedEntryPersistenceConfigurer::withLoadingStrategies)),
                    dc -> dc.build(cacheLoader));

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded but not from store"))
                    .when(cacheLoader).load(any(Key.class));

            Value loadedFromStoreValue = distributedPolicy.getFromStore(key1, true).getValue();
            Value notFoundValue = distributedLoadingCacheWithoutLoadingStrategy.getIfPresent(key1);
            Value loadedButNotFromStoreValue = distributedLoadingCacheWithoutLoadingStrategy.get(key1);

            verify(cacheLoader, times(5)).load(any(Key.class));

            assertThat(loadedFromStoreValue).isEqualTo(loadedValue1);
            assertThat(notFoundValue).isNull();
            assertThat(loadedButNotFromStoreValue).isNotNull()
                    .satisfies(value -> assertThat(value.getName()).isEqualTo("loaded but not from store"));
        }

        @DisplayName("Test invalidation of a cache entry only the underlying store still holds")
        @Test
        void test_EvictedEntryPersistence_invalidation_of_passivated_cache_entry() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .maximumSize(1))
                            .withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries ->
                                            evictedEntries.withMaximumSize(10))),
                    DistributedCaffeine::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            distributedCache.put(key1, value1);
            distributedCache.put(Key.of(2), Value.of(2));
            distributedCache.cleanUp(); // evicts key1, which is retained and stays reloadable

            await("passivation")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(distributedCache.getIfPresent(key1)).isNull();
                        assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull();
                    });

            // no cache instance holds it anymore, so this is precisely the case an invalidation used to skip - and
            // skipping it would leave the cache entry reloadable after having been invalidated
            distributedCache.invalidate(key1);

            await("invalidation")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNull());
        }

        @DisplayName("Test that invalidating all reaches a cache entry only the underlying store still holds")
        @Test
        void test_EvictedEntryPersistence_invalidate_all_of_passivated_cache_entry() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .maximumSize(1))
                            .withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries ->
                                            evictedEntries.withMaximumSize(10))),
                    DistributedCaffeine::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            Key key1 = Key.of(1);

            distributedCache.put(key1, Value.of(1));
            distributedCache.put(Key.of(2), Value.of(2));
            distributedCache.cleanUp(); // evicts key1, which is retained and stays reloadable

            await("passivation")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(distributedCache.getIfPresent(key1)).isNull();
                        assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull();
                    });

            // no cache instance holds this one, so emptying every one of them cannot reach it: what keeps it
            // reloadable is the record the store has of it, and only clearing that stops it from coming back
            distributedCache.invalidateAll();

            await("invalidation of all cache entries")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNull());
        }

        @DisplayName("Test that cached entries are not retained without a synchronization strategy")
        @Test
        void test_CachedEntryPersistence_cache_entries_are_not_retained() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            distributedCache.put(key1, value1);

            // the cache entry is written either way - that is how it is distributed at all. What differs is only
            // whether it is retained beyond that, which is why getFromStore reports nothing even now, while the
            // cache entry is demonstrably there: it answers what persistence keeps, not what a write left behind
            await("distribution to data store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
                        assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                    });

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(0)));
            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();

            // swept from the data store, but not withdrawn from the cache instances holding it: a delete is not
            // reported as a change stream event, so the cache keeps serving what it has
            assertThat(distributedCache.getIfPresent(key1)).isEqualTo(value1);
        }

        @DisplayName("Test that synchronization starts empty without a synchronization strategy")
        @Test
        void test_CachedEntryPersistence_synchronization_starts_empty() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            distributedCache.put(key1, value1);

            await("distribution to data store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(1))));

            distributedCache.distributedPolicy().stopSynchronization();
            distributedCache.distributedPolicy().startSynchronization();

            // nothing is read back, so nothing clears the marks and the cache is emptied rather than reconciled -
            // even though the data store still happens to hold the cache entry at this point
            assertThat(distributedCache.asMap()).isEmpty();
        }

        @DisplayName("Test persistence of cached entries with a cold start")
        @Test
        void test_CachedEntryPersistence_with_cold_start() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    // retained, but deliberately not read back - the one configuration where the underlying store
                    // outlives what any cache instance holds without anything taking ownership of it again
                    dc -> dc.withPersistence(configurer -> configurer
                            .withCachedEntries(cachedEntries -> cachedEntries
                                    .withMaximumTime(FOREVER.getDuration())
                                    .withColdStart())),
                    DistributedCaffeine::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            distributedCache.put(key1, value1);

            await("distribution to data store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(1))));

            distributedPolicy.stopSynchronization();
            distributedPolicy.startSynchronization();

            // the retention holds, so what is asserted here is the cold start alone and not that the cache entry
            // was swept: it is still there to be read, just not by this cache instance
            assertThat(distributedCache.asMap()).isEmpty();
            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
            assertThat(distributedPolicy.getFromStore(key1, false)).isNotNull();

            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
        }

        @DisplayName("Test that population is still distributed without a synchronization strategy")
        @Test
        void test_CachedEntryPersistence_population_is_still_distributed() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            distributedCache.put(key1, value1);

            // retaining cache entries and distributing them are different matters: without a synchronization
            // strategy the data store is only a medium, and warming other cache instances still works through it
            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(syncedDistributedCache.getIfPresent(key1)).isEqualTo(value1));
        }

        @DisplayName("Test evicted cache entry persistence without a synchronization strategy")
        @Test
        void test_CachedEntryPersistence_without_strategy_but_with_evicted_entry_persistence() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .maximumSize(1))
                            .withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumSize(10))),
                    DistributedCaffeine::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);

            distributedCache.put(key1, value1);
            distributedCache.put(key2, Value.of(2));
            distributedCache.cleanUp(); // evicts key1, which evicted entry persistence keeps reloadable

            await("passivation")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull());

            // read rather than asserted: a maximum size of one is degenerate enough for Caffeine to deny admission
            // to both entries, so how many end up evicted is none of this test's business - only that the sweep
            // below leaves however many there are untouched
            Repository<?, ?> repository = repositoryOf(distributedCache);
            long evictedCountBeforeMaintenance = getFailable(() ->
                    repository.countCacheEntries(EVICTED_RETAINED_GROUP));
            assertThat(evictedCountBeforeMaintenance).isPositive();

            processMaintenance();

            // the two tiers are retained independently: cached entries are swept along with the removals
            // ones, whereas evicted ones are kept for as long as their own bound allows - so the underlying store
            // ends up holding exactly what memory does not
            assertThatDataStoreHasCounts(
                    CountGrouped.of(CACHED_GROUP, assertion -> assertion.isEqualTo(0)),
                    CountGrouped.of(EVICTED_RETAINED_GROUP,
                            assertion -> assertion.isEqualTo(evictedCountBeforeMaintenance)));
            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull();
        }

        @DisplayName("Test persistence of cached entries with cache residency")
        @Test
        void test_CachedEntryPersistence_with_cache_residency() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    // no eviction policy, so nothing ever ends the residency this retention rests on - which is
                    // also why the distribution mode is not required to include evictions here
                    dc -> dc.withPersistence(configurer -> configurer
                            .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency)),
                    DistributedCaffeine::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            distributedCache.put(key1, value1);

            await("distribution to data store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(1))));

            // the very sweep that removes a cache entry which is not retained at all (see the test asserting that),
            // deliberately run with the same accelerated window, so what is asserted below is the retention itself
            // and not that the sweep happened to leave the cache entry alone for lack of time
            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
            assertThat(distributedPolicy.getFromStore(key1, false)).isNotNull();

            // and neither pruning by time nor by size applies, so repeating it changes nothing
            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
        }

        @DisplayName("Test persistence of cached entries by time")
        @Test
        void test_CachedEntryPersistence_by_time() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withPersistence(configurer -> configurer
                            .withCachedEntries(cachedEntries -> cachedEntries
                                    .withMaximumTime(Duration.ofMillis(1)))),
                    DistributedCaffeine::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            distributedCache.put(key1, value1);

            await("distribution to data store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(1))));

            // deliberately the variant that leaves the transient window alone, so that what removes the cache entry
            // below can only be pruning by time and not the sweep for cache entries that are not retained at all
            processMaintenance();

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(0)));
            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();

            // pruning is a delete, so it is not reported as a change stream event and the cache keeps serving
            assertThat(distributedCache.getIfPresent(key1)).isEqualTo(value1);
        }

        @DisplayName("Test persistence of cached entries by size")
        @Test
        void test_CachedEntryPersistence_by_size() {
            int maximumSize = 2;
            int numberOfCacheEntries = 5;

            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withPersistence(configurer -> configurer
                            .withCachedEntries(cachedEntries -> cachedEntries
                                    .withMaximumSize(maximumSize))),
                    DistributedCaffeine::build);

            // no eviction policy is configured, so every cache entry stays cached and the data store is bounded by
            // its own maximum size rather than by what the cache instance happens to hold
            IntStream.rangeClosed(1, numberOfCacheEntries)
                    .forEach(id -> distributedCache.put(Key.of(id), Value.of(id)));

            await("distribution to data store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(numberOfCacheEntries))));

            processMaintenance();

            // which cache entries are dropped follows from write order alone, so only the count is asserted here
            assertThatDataStoreHasCounts(
                    Count.of(CACHED, assertion -> assertion.isEqualTo(maximumSize)));
            assertThat(distributedCache.estimatedSize()).isEqualTo(numberOfCacheEntries);
        }

        @DisplayName("Test persistence of cached entries with cache residency and of evicted cache entries")
        @Test
        void test_CachedEntryPersistence_with_cache_residency_and_evicted_entry_persistence() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    // an eviction policy is what ends a residency, which is why the distribution mode has to
                    // include evictions here - and it is the very same eviction that hands a cache entry from the
                    // one tier over to the other
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .maximumSize(1))
                            .withPersistence(configurer -> configurer
                                    .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency)
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumSize(10))),
                    DistributedCaffeine::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);

            // Kept away from here on, because what an evicted cache entry leaves in the data store is also
            // delivered back, and the cache entry written for its population restores it - which puts the cache
            // over its maximum again and hands the residency of the other key over to the evicted tier while this
            // test is measuring both. What is under test is the two tiers, not that echo
            Adapter<Key, Value> adapter = getInstanceRegistry(distributedCache).getAdapter();
            Synchronizer<Key, Value> synchronizer = readFieldValue(adapter, AbstractAdapter.class,
                    "synchronizer", Synchronizer.class);
            Receiver<Key, Value> receiver = injectSpy(synchronizer, AbstractSynchronizer.class,
                    "receiver", Receiver.class);
            doNothing().when(receiver).receiveCacheEntries(anyList());

            distributedCache.put(key1, Value.of(1));
            distributedCache.put(key2, Value.of(2));
            distributedCache.cleanUp(); // evicts key1, which is what moves it into the other tier

            await("passivation")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull());

            // read rather than asserted, see the test above: with a maximum size of one it is Caffeine's business
            // how many cache entries end up evicted and how many stay resident alongside them
            Repository<?, ?> repository = repositoryOf(distributedCache);
            long cachedCountBeforeMaintenance = getFailable(() ->
                    repository.countCacheEntries(CACHED_GROUP));
            long evictedCountBeforeMaintenance = getFailable(() ->
                    repository.countCacheEntries(EVICTED_RETAINED_GROUP));
            assertThat(evictedCountBeforeMaintenance).isPositive();
            // what cache residency amounts to, whichever cache entries Caffeine admitted
            assertThat(cachedCountBeforeMaintenance).isEqualTo(distributedCache.estimatedSize());

            processMaintenance();

            // both retentions hold at once and neither reaches into the other: residency keeps what is still
            // cached, the evicted tier keeps what is not, and each answers for its own phase of a cache entry
            assertThatDataStoreHasCounts(
                    CountGrouped.of(CACHED_GROUP, assertion -> assertion.isEqualTo(cachedCountBeforeMaintenance)),
                    CountGrouped.of(EVICTED_RETAINED_GROUP,
                            assertion -> assertion.isEqualTo(evictedCountBeforeMaintenance)));
            assertThat(cachedCountBeforeMaintenance).isEqualTo(distributedCache.estimatedSize());
        }

        @DisplayName("Test persistence of cached entries by time and of evicted cache entries")
        @Test
        void test_CachedEntryPersistence_by_time_and_evicted_entry_persistence() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    // time-based rather than size-based eviction, so that it is this cache entry which is evicted
                    // and not whichever one Caffeine decides to deny admission to
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .expireAfterWrite(Duration.ofSeconds(2)))
                            .withPersistence(configurer -> configurer
                                    .withCachedEntries(cachedEntries -> cachedEntries
                                            .withMaximumTime(Duration.ofMillis(1)))
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumSize(10))),
                    DistributedCaffeine::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            distributedCache.put(key1, value1);

            await("distribution to data store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(1))));

            processMaintenance();

            // the cached tier is done with the cache entry, and because pruning it is a delete rather than a
            // transition, no change stream event reports that - so the cache instance goes on holding it
            assertThatDataStoreIsEmpty();
            assertThat(distributedCache.policy().getIfPresentQuietly(key1)).isEqualTo(value1);

            // and once it is evicted the other tier writes it anew: the two tiers are phases of the same cache
            // entry's life, so what the one stopped keeping the other one takes on, counting from the eviction
            await("passivation")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> {
                        assertThatDataStoreHasCounts(
                                Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(1)));
                        assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull();
                    });
        }

        @DisplayName("Test persistence of evicted entries by time")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_evicted_entry_persistence_by_time(CacheFactory<Key, Value> cacheFactory) throws Exception {
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheBuilder<Key, Value> cacheBuilder =
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .evictionListener(evictionListener)
                                    // variable expiration policy provides more control over evictions
                                    .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration())))
                            .withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumTime(FOREVER.getDuration())
                                            // just to test the distinction in logic
                                            .withMaximumSize(Integer.MAX_VALUE)
                                            .withLoadingStrategies(CACHE_LOADER)));

            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                @SuppressWarnings("RedundantThrows")
                public Value load(@NonNull Key key) throws Exception {
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(1)));
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(2)));
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(2)));
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(1)),
                                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(3)));
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
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromStore(key4, false)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue4));
                            assertThat(distributedPolicy.getFromStore(key4, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue4));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(2)),
                                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key4)).isEqualTo(loadedValue4);
                            assertThat(distributedPolicy.getFromStore(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromStore(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromStore(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(entry.getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromStore(key4, false)).isNull();
                            assertThat(distributedPolicy.getFromStore(key4, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(3)));
                        }
                    });

            processMaintenance();

            if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(CACHED_LOADED, assertion -> assertion.isEqualTo(2)),
                        Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(2)));
            } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                    || distributionMode.equals(INVALIDATION)) {
                assertThatDataStoreHasCounts(
                        Count.of(EVICTED_TIME_RETAINED, assertion -> assertion.isEqualTo(3)));
            }

            // test cache without loading strategy
            DistributedLoadingCache<Key, Value> distributedLoadingCacheWithoutLoadingStrategy = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    dc -> dc.withPersistence(configurer -> configurer
                            .withEvictedEntries(DistributedCaffeine.EvictedEntryPersistenceConfigurer::withLoadingStrategies)),
                    dc -> dc.build(cacheLoader));

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded but not from store"))
                    .when(cacheLoader).load(any(Key.class));

            Value loadedFromStoreValue = distributedPolicy.getFromStore(key2, true).getValue();
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
            // the second cache instance below is created after the first write, so synchronizing from the data
            // store is the only way it can arrive at that cache entry
            CacheBuilder<Key, Value> cacheBuilder = dc -> dc.withPersistence(configurer -> configurer
                            .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency));
            DistributedCache<Key, Value> distributedCache = createCache(
                    cacheBuilder,
                    DistributedCaffeine::build);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            Key key3 = Key.of(3);
            Value value3 = Value.of(3);

            distributedCache.put(key1, value1);

            DistributedCache<Key, Value> syncedDistributedCache = createCache(
                    cacheBuilder,
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

        @DisplayName("Test reconciliation and preservation of statistics on restart")
        @Test
        void test_DistributedCaffeine_restart_reconciles_and_preserves_stats() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder().recordStats())
                            // reconciling against the data store is what this test is about, so the cache entries
                            // have to be persisted there in the first place
                            .withPersistence(configurer -> configurer
                                    .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency)),
                    DistributedCaffeine::build);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCache.put(key1, value1);

            await("distribution to data store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(1))));

            // record a hit and a miss, so that a restart resetting the statistics would be noticed below
            assertThat(distributedCache.getIfPresent(key1)).isEqualTo(value1);
            assertThat(distributedCache.getIfPresent(key2)).isNull();
            CacheStats statsBeforeRestart = distributedCache.stats();
            assertThat(statsBeforeRestart.hitCount()).isEqualTo(1);
            assertThat(statsBeforeRestart.missCount()).isEqualTo(1);

            distributedCache.distributedPolicy().stopSynchronization();

            // key1 is deliberately left untouched, so it stays exactly what the data store holds, down to the
            // operation marker - which makes it indistinguishable from an echo of this instance's own write once
            // synchronization resumes, and would have it dropped if the marker alone decided what to keep.
            // key2 is written while stopped, so it never reaches the store and has nothing to back it afterwards
            distributedCache.put(key2, value2);

            distributedCache.distributedPolicy().startSynchronization();

            // reconciliation completes before synchronization is reported as started, so no waiting is needed here
            assertThat(distributedCache.asMap())
                    .containsExactlyInAnyOrderEntriesOf(Map.of(key1, value1));

            // the cache instance itself survives a restart, so statistics continue instead of starting over
            assertThat(distributedCache.stats().hitCount()).isEqualTo(statsBeforeRestart.hitCount());
            assertThat(distributedCache.stats().missCount()).isEqualTo(statsBeforeRestart.missCount());
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
                        // identity checks (self-echo filter)
                        assertThat(distributedCache.getIfPresent(key1)).isSameAs(value1);
                        assertThat(syncedDistributedCache.getIfPresent(key1)).isNotSameAs(value1);
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)));
                    });

            // the store itself rather than the persistence view: nothing is retained here, the cache entry is
            // only written so that it can be distributed
            Repository<Key, Value> repository = distributedCache.distributedPolicy()
                    .getAdapter().getRepository().orElseThrow();
            Supplier<Instant> cachedTimestamp = () -> getFailable(() -> {
                try (Stream<CacheEntry<Key, Value>> cacheEntryStream =
                             repository.streamCacheEntries(null, Set.of(CACHED), false)) {
                    return cacheEntryStream.findFirst().orElseThrow().getTimestamp();
                }
            });
            Instant timestamp = cachedTimestamp.get();

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
                        // identity checks (self-echo filter)
                        assertThat(distributedCache.getIfPresent(key1)).isSameAs(value1);
                        assertThat(syncedDistributedCache.getIfPresent(key1)).isNotSameAs(value1);
                        assertThat(cachedTimestamp.get()).isAfter(timestamp);
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, assertion -> assertion.isEqualTo(1)),
                                // invalidating a key this cache instance does not hold still reaches the
                                // underlying store: whether it is held here says nothing about the other
                                // cache instances, which would otherwise keep serving it
                                Count.of(INVALIDATED, assertion -> assertion.isEqualTo(1)));
                    });

            // the cache entry written for the absent key is kept for distribution only and does not accumulate
            processMaintenance();

            assertThatDataStoreIsEmpty();
        }

        @DisplayName("Test Adapter")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Adapter() throws Exception {
            Set<CacheEntry<Key, Value>> receivedCacheEntries = new HashSet<>();
            @SuppressWarnings("Convert2Lambda")
            Receiver<Key, Value> receiver = spy(new Receiver<Key, Value>() {
                @Override
                public void receiveCacheEntries(@NonNull List<CacheEntry<Key, Value>> cacheEntries) {
                    receivedCacheEntries.addAll(cacheEntries);
                }
            });

            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Adapter<Key, Value> adapter = distributedCache.distributedPolicy().getAdapter();
            adapter.setReceiver(receiver);
            Repository<Key, Value> repository = adapter.getRepository().orElseThrow();

            assertThat(adapter.isActivated()).isTrue();

            CacheEntry<Key, Value> insertCacheEntry1 = CacheEntry.of(
                    "hash1",
                    "op1",
                    Key.of(1),
                    Value.of(1),
                    CACHED,
                    Instant.now());
            CacheEntry<Key, Value> insertCacheEntry2 = CacheEntry.of(
                    "hash2",
                    "op2",
                    Key.of(2),
                    Value.of(2),
                    CACHED,
                    Instant.now());

            repository.publishCacheEntries(Set.of(insertCacheEntry1, insertCacheEntry2));

            CacheEntry<Key, Value> updateCacheEntry1 = CacheEntry.of(
                    insertCacheEntry1.getHash(),
                    insertCacheEntry1.getOperation(),
                    insertCacheEntry1.getKey(),
                    insertCacheEntry1.getValue(),
                    insertCacheEntry1.getStatus(),
                    Instant.now());
            CacheEntry<Key, Value> updateCacheEntry2 = CacheEntry.of(
                    insertCacheEntry2.getHash(),
                    insertCacheEntry2.getOperation(),
                    insertCacheEntry2.getKey(),
                    insertCacheEntry2.getValue(),
                    insertCacheEntry2.getStatus(),
                    Instant.now());

            repository.publishCacheEntries(Set.of(updateCacheEntry1, updateCacheEntry2));

            List<io.github.oberhoff.distributedcaffeine.adapter.CacheEntry<Key, Value>> foundCacheEntries = new ArrayList<>();
            try (Stream<io.github.oberhoff.distributedcaffeine.adapter.CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(null, null, false)) {
                stream.forEach(foundCacheEntries::add);
            }

            await("receiving")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        verify(receiver, times(4))
                                .receiveCacheEntries(anyList());
                        assertThat(receivedCacheEntries)
                                .containsExactlyInAnyOrder(
                                        insertCacheEntry1, insertCacheEntry2,
                                        updateCacheEntry1, updateCacheEntry2);
                    });


            assertThat(foundCacheEntries).hasSize(2)
                    .containsExactlyInAnyOrder(updateCacheEntry1, updateCacheEntry2);
            assertThat(repository.countCacheEntries(null))
                    .isEqualTo(2);

            repository.deleteCacheEntries(null, null, null);

            assertThat(repository.countCacheEntries(null))
                    .isEqualTo(0);

            adapter.deactivate();
            assertThat(adapter.isActivated()).isFalse();

            // repository methods, filters and their combinations:
            // (operating directly on the repository, which works independently of the synchronizer being activated,
            // so the assertions below are not disturbed by change stream events)

            // timestamps in the past (and spaced apart) so that a status update - which refreshes the timestamp to the
            // real 'now' - reliably produces a newer timestamp than these seeded ones
            Instant now = Instant.now();
            Instant timestamp1 = now.minusSeconds(30);
            Instant timestamp2 = now.minusSeconds(20);
            Instant timestamp3 = now.minusSeconds(10);

            // cache entries covering statuses, hashes and timestamps (all within the scope of this repository, which
            // is the only one it can address)
            CacheEntry<Key, Value> cachedEntry1 = CacheEntry.of(
                    "h1", "op1", Key.of(1), Value.of(1), CACHED, timestamp1);
            CacheEntry<Key, Value> cachedEntry2 = CacheEntry.of(
                    "h2", "op2", Key.of(2), Value.of(2), CACHED, timestamp2);
            CacheEntry<Key, Value> invalidatedEntry3 = CacheEntry.of(
                    "h3", "op3", Key.of(3), Value.of(3), INVALIDATED, timestamp3);

            repository.publishCacheEntries(Set.of(cachedEntry1, cachedEntry2, invalidatedEntry3));

            assertThat(repository.countCacheEntries(null)).isEqualTo(3);

            // countCacheEntries filtered by statuses
            assertThat(repository.countCacheEntries(Set.of(CACHED))).isEqualTo(2);
            assertThat(repository.countCacheEntries(Set.of(INVALIDATED))).isEqualTo(1);
            assertThat(repository.countCacheEntries(Set.of(CACHED, INVALIDATED))).isEqualTo(3);
            assertThat(repository.countCacheEntries(Set.of(EVICTED_SIZE))).isEqualTo(0);

            // streamCacheEntries unfiltered
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(null, null, false)) {
                assertThat(stream.toList())
                        .containsExactlyInAnyOrder(cachedEntry1, cachedEntry2, invalidatedEntry3);
            }

            // streamCacheEntries filtered by hashes
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("h1", "h2"), null, false)) {
                assertThat(stream.toList())
                        .containsExactlyInAnyOrder(cachedEntry1, cachedEntry2);
            }

            // streamCacheEntries filtered by statuses
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(null, Set.of(CACHED), false)) {
                assertThat(stream.toList())
                        .containsExactlyInAnyOrder(cachedEntry1, cachedEntry2);
            }

            // streamCacheEntries filtered by hashes and statuses combined
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("h1", "h2", "h3"), Set.of(INVALIDATED), false)) {
                assertThat(stream.toList())
                        .containsExactly(invalidatedEntry3);
            }

            // streamCacheEntries ordered ascending by timestamp
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(null, null, true)) {
                assertThat(stream.toList())
                        .containsExactly(cachedEntry1, cachedEntry2, invalidatedEntry3);
            }

            // streamCacheEntryMetadata returns everything except key and value, which are the fields it exists to
            // avoid reading (and deserializing) at all
            List<CacheEntryMetadata> cacheEntryMetadata;
            try (Stream<CacheEntryMetadata> stream =
                         repository.streamCacheEntryMetadata(Set.of("h1"), null, false)) {
                cacheEntryMetadata = stream.toList();
            }
            assertThat(cacheEntryMetadata).hasSize(1);
            assertThat(cacheEntryMetadata.get(0))
                    .satisfies(metadata -> {
                        assertThat(metadata.getHash()).isEqualTo("h1");
                        assertThat(metadata.getOperation()).isEqualTo("op1");
                        assertThat(metadata.getStatus()).isEqualTo(CACHED);
                        assertThat(metadata.getTimestamp()).isEqualTo(timestamp1.truncatedTo(MILLIS));
                    })
                    // the metadata of a cache entry is unrelated to the cache entry it belongs to, so the two are never
                    // equal - and a stream of metadata can never turn out to be one of complete cache entries
                    .isNotEqualTo(cachedEntry1)
                    .isEqualTo(CacheEntryMetadata.of("h1", "op1", CACHED, timestamp1));

            // streamCacheEntryMetadata applies the same filters and ordering as streamCacheEntries
            try (Stream<CacheEntryMetadata> stream = repository.streamCacheEntryMetadata(null, Set.of(CACHED), true)) {
                assertThat(stream.map(CacheEntryMetadata::getHash).toList())
                        .containsExactly("h1", "h2");
            }

            // reading a document that is no cache entry is reported before it is skipped, so the warnings expected
            // for the two documents seeded below are captured (and asserted) instead of ending up - with their stack
            // traces - in the test output
            CaptureLogger loggerMongoRepository = CaptureLoggerFactory
                    .getCaptureLogger("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoRepository");
            loggerMongoRepository.startCapturing();

            // a document carrying a key and a value that cannot be deserialized is what tells the two streams apart:
            // it is no cache entry (skipped, logged and left out), while its metadata is returned - which it could only
            // be if reading metadata does not touch the payload at all
            mongoClient.getDatabase(DATABASE_NAME).getCollection(getCollectionName())
                    .insertOne(new Document()
                            .append(CacheEntry.Field.HASH.toString(), "broken")
                            .append(CacheEntry.Field.OPERATION.toString(), "op4")
                            .append(CacheEntry.Field.KEY.toString(), "not a serialized key")
                            .append(CacheEntry.Field.VALUE.toString(), "not a serialized value")
                            .append(CacheEntry.Field.STATUS.toString(), CACHED.toString())
                            .append(CacheEntry.Field.TIMESTAMP.toString(), timestamp1)
                            .append(DiscriminatorAware.DISCRIMINATOR_FIELD, DEFAULT_DISCRIMINATOR));
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("broken"), null, false)) {
                assertThat(stream.toList()).isEmpty();
            }
            try (Stream<CacheEntryMetadata> stream =
                         repository.streamCacheEntryMetadata(Set.of("broken"), null, false)) {
                assertThat(stream.toList())
                        .singleElement()
                        .isEqualTo(CacheEntryMetadata.of("broken", "op4", CACHED, timestamp1));
            }
            repository.deleteCacheEntries(Set.of("broken"), null, null);

            // a document not carrying what even metadata cannot do without (no status here) is no cache entry and no
            // metadata of one either, so both streams skip it
            mongoClient.getDatabase(DATABASE_NAME).getCollection(getCollectionName())
                    .insertOne(new Document()
                            .append(CacheEntry.Field.HASH.toString(), "incomplete")
                            .append(CacheEntry.Field.TIMESTAMP.toString(), timestamp1)
                            .append(DiscriminatorAware.DISCRIMINATOR_FIELD, DEFAULT_DISCRIMINATOR));
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("incomplete"), null, false)) {
                assertThat(stream.toList()).isEmpty();
            }
            try (Stream<CacheEntryMetadata> stream =
                         repository.streamCacheEntryMetadata(Set.of("incomplete"), null, false)) {
                assertThat(stream.toList()).isEmpty();
            }
            repository.deleteCacheEntries(Set.of("incomplete"), null, null);

            // every skipped document is reported, the incomplete one twice because both streams skip it
            assertThat(loggerMongoRepository.getLoggingEvents()).hasSize(3)
                    .allSatisfy(loggingEvent -> assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN))
                    .satisfiesOnlyOnce(loggingEvent -> {
                        assertThat(loggingEvent.getMessage())
                                .startsWith("Reading of cache entry failed")
                                .contains("hash=broken");
                        assertThat(loggingEvent.getThrowable())
                                .isExactlyInstanceOf(IllegalStateException.class)
                                .hasMessage("No Serializer found for deserializing value of type String");
                    })
                    .satisfiesOnlyOnce(loggingEvent -> {
                        assertThat(loggingEvent.getMessage())
                                .startsWith("Reading of cache entry failed")
                                .contains("hash=incomplete");
                        assertThat(loggingEvent.getThrowable())
                                .isExactlyInstanceOf(NullPointerException.class)
                                .hasMessage("status cannot be null");
                    })
                    .satisfiesOnlyOnce(loggingEvent -> {
                        assertThat(loggingEvent.getMessage())
                                .startsWith("Reading of cache entry metadata failed")
                                .contains("hash=incomplete");
                        assertThat(loggingEvent.getThrowable())
                                .isExactlyInstanceOf(NullPointerException.class)
                                .hasMessage("status cannot be null");
                    });

            loggerMongoRepository.stopCapturing();

            // updateStatusOfCacheEntries updates the status, clears the operation and refreshes the timestamp
            repository.updateStatusOfCacheEntries(Set.of("h1"), Set.of(CACHED), null, INVALIDATED);

            List<CacheEntry<Key, Value>> updatedEntries;
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("h1"), null, false)) {
                updatedEntries = stream.toList();
            }
            assertThat(updatedEntries).hasSize(1);
            CacheEntry<Key, Value> updatedEntry = updatedEntries.get(0);
            assertThat(updatedEntry.getStatus()).isEqualTo(INVALIDATED);
            assertThat(updatedEntry.getOperation()).isNull();          // operation cleared
            assertThat(updatedEntry.getKey()).isEqualTo(Key.of(1));    // key and value preserved
            assertThat(updatedEntry.getValue()).isEqualTo(Value.of(1));
            assertThat(updatedEntry.getTimestamp()).isAfter(timestamp3); // timestamp refreshed to (a recent) now
            assertThat(repository.countCacheEntries(Set.of(CACHED))).isEqualTo(1);
            assertThat(repository.countCacheEntries(Set.of(INVALIDATED))).isEqualTo(2);

            // updateStatusOfCacheEntries filtered by olderThan updates only entries older than the given timestamp,
            // cachedEntry2 (timestamp2) is updated, invalidatedEntry3 (timestamp3, not older) and the just-refreshed
            // 'h1' entry (recent) are not
            repository.updateStatusOfCacheEntries(null, null, timestamp3, EVICTED_SIZE);
            assertThat(repository.countCacheEntries(Set.of(EVICTED_SIZE))).isEqualTo(1);

            // deleteCacheEntries filtered by hashes
            repository.deleteCacheEntries(Set.of("h2"), null, null);
            assertThat(repository.countCacheEntries(null)).isEqualTo(2);
            assertThat(repository.countCacheEntries(Set.of(EVICTED_SIZE))).isEqualTo(0);

            // deleteCacheEntries filtered by statuses
            repository.deleteCacheEntries(null, Set.of(INVALIDATED), null);
            assertThat(repository.countCacheEntries(null)).isEqualTo(0);

            // deleteCacheEntries filtered by olderThan
            repository.publishCacheEntries(Set.of(
                    CacheEntry.of("old", "op1", Key.of(10), Value.of(10), CACHED, Instant.now().minusSeconds(10)),
                    CacheEntry.of("new", "op2", Key.of(11), Value.of(11), CACHED, Instant.now())));
            assertThat(repository.countCacheEntries(null)).isEqualTo(2);
            repository.deleteCacheEntries(null, null, Instant.now().minusSeconds(5));
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(null, null, false)) {
                assertThat(stream.toList())
                        .hasSize(1)
                        .allSatisfy(entry -> assertThat(entry.getHash()).isEqualTo("new"));
            }

            repository.deleteCacheEntries(null, null, null);
            assertThat(repository.countCacheEntries(null)).isEqualTo(0);
        }

        @DisplayName("Test Adapter with a collection shared across caches")
        @Test
        void test_Adapter_with_shared_collection() throws Exception {
            String collectionName = getCollectionName();

            // four caches on one and the same collection: two of them share the discriminator 'a' (so they are
            // expected to synchronize with each other), one uses 'b' and one chooses none, which puts it in the
            // default scope
            DistributedCache<Key, Value> cacheA1 = createCache(
                    MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, collectionName)
                            .withDiscriminator("a").build(),
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheA2 = createCache(
                    MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, collectionName)
                            .withDiscriminator("a").build(),
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheB = createCache(
                    MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, collectionName)
                            .withDiscriminator("b").build(),
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheInDefaultScope = createCache(
                    MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, collectionName).build(),
                    CacheBuilder.identity(), DistributedCaffeine::build);

            // a discriminator gone missing is reported instead of silently placing the cache in a scope of its own,
            // where it would neither synchronize with the caches it was meant to nor say so
            assertThatThrownBy(() ->
                    MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, collectionName).withDiscriminator(null))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("discriminator cannot be null");
            Stream.of("", " ", "\t\n").forEach(blank ->
                    assertThatThrownBy(() ->
                            MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, collectionName)
                                    .withDiscriminator(blank))
                            .isExactlyInstanceOf(IllegalArgumentException.class)
                            .hasMessage("discriminator cannot be blank"));

            // every cache has a discriminator, so the identifier always carries one
            assertThat(cacheA1.distributedPolicy().getAdapter().getIdentifier())
                    .isEqualTo(String.join(":", "mongodb", DATABASE_NAME, collectionName, "a"));
            assertThat(cacheInDefaultScope.distributedPolicy().getAdapter().getIdentifier())
                    .isEqualTo(String.join(":", "mongodb", DATABASE_NAME, collectionName, DEFAULT_DISCRIMINATOR));

            // the same key is populated in every scope, each with a value of its own
            Key key = Key.of(1);
            cacheA1.put(key, Value.of(1));
            cacheB.put(key, Value.of(2));
            cacheInDefaultScope.put(key, Value.of(3));

            // synchronization happens within a discriminator ...
            await("synchronization within the shared discriminator")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> assertThat(cacheA2.getIfPresent(key)).isEqualTo(Value.of(1)));

            // ... and not across discriminators: the events of the other caches passed through the same change
            // stream, so having seen the one above means the others had their chance as well
            assertThat(cacheB.getIfPresent(key)).isEqualTo(Value.of(2));
            assertThat(cacheInDefaultScope.getIfPresent(key)).isEqualTo(Value.of(3));

            // uniqueness in the store is (discriminator + hash), so the same key coexists once per scope
            assertThat(mongoClient.getDatabase(DATABASE_NAME).getCollection(collectionName).countDocuments())
                    .isEqualTo(3);

            // and every repository addresses its own scope only
            assertThat(repositoryOf(cacheA1).countCacheEntries(null)).isEqualTo(1);
            assertThat(repositoryOf(cacheB).countCacheEntries(null)).isEqualTo(1);
            assertThat(repositoryOf(cacheInDefaultScope).countCacheEntries(null)).isEqualTo(1);

            // deleting within one scope leaves the others untouched
            repositoryOf(cacheB).deleteCacheEntries(null, null, null);
            assertThat(repositoryOf(cacheB).countCacheEntries(null)).isEqualTo(0);
            assertThat(repositoryOf(cacheA1).countCacheEntries(null)).isEqualTo(1);
            assertThat(repositoryOf(cacheInDefaultScope).countCacheEntries(null)).isEqualTo(1);
        }

        @DisplayName("Test Repository queries are served by an index")
        @Test
        void test_Repository_queries_avoid_collection_scans() throws Exception {
            String collectionName = getCollectionName();
            // both scopes populated, so that the discriminator actually discriminates instead of matching every
            // document - and one of them in the default scope, which an index has to serve like any other
            DistributedCache<Key, Value> cacheWithDiscriminator = createCache(
                    MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, collectionName)
                            .withDiscriminator("d1").build(),
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheInDefaultScope = createCache(
                    MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, collectionName).build(),
                    CacheBuilder.identity(), DistributedCaffeine::build);

            // enough documents, spread over statuses and timestamps, that the planner has something to choose
            // between rather than trivially scanning a handful. Plan selection is cost-based, so it could in
            // principle turn over at a size no test wants to seed - probed separately up to 50k documents and
            // across scopes holding 1% to 100% of the collection, where it does not
            Status[] statuses = Status.values();
            for (Repository<Key, Value> repository : List.of(
                    repositoryOf(cacheWithDiscriminator), repositoryOf(cacheInDefaultScope))) {
                repository.publishCacheEntries(IntStream.range(0, 500)
                        .mapToObj(i -> CacheEntry.of("h" + i, "op" + i, Key.of(i), Value.of(i),
                                statuses[i % statuses.length], Instant.now().minusSeconds(i)))
                        .collect(toSet()));
            }

            Instant deadline = Instant.now().minusSeconds(100);
            Set<String> hashes = Set.of("h1", "h2");

            for (Repository<Key, Value> repository : List.of(
                    repositoryOf(cacheWithDiscriminator), repositoryOf(cacheInDefaultScope))) {
                // every filter combination the repository can produce, including those no internal caller currently
                // issues but the SPI permits (a discriminator on its own used to scan the collection)
                assertThatQueryIsIndexed(repository, collectionName, null, null, null, false);
                assertThatQueryIsIndexed(repository, collectionName, null, null, deadline, false);
                assertThatQueryIsIndexed(repository, collectionName, hashes, null, null, false);
                assertThatQueryIsIndexed(repository, collectionName, hashes, CACHED_GROUP, null, false);
                assertThatQueryIsIndexed(repository, collectionName, null, CACHED_GROUP, null, false);
                assertThatQueryIsIndexed(repository, collectionName, null, CACHED_GROUP, deadline, false);
                // ordered by timestamp, as synchronizing cache entries on activation does
                assertThatQueryIsIndexed(repository, collectionName, null, CACHED_GROUP, null, true);
            }
        }

        // asserts against the winning plan only - rejected plans name stages that are never executed. The filter is
        // taken from the repository itself rather than rebuilt here, so that this cannot drift from what is queried
        private void assertThatQueryIsIndexed(Repository<Key, Value> repository, String collectionName,
                                              Set<String> hashes, Set<Status> statuses, Instant olderThan,
                                              boolean orderByTimestampAsc) throws Exception {
            Bson filter = invokeMethod(repository,
                    Class.forName("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoRepository"),
                    "getFilter", List.of(Set.class, Set.class, Instant.class),
                    Arrays.asList(hashes, statuses, olderThan));
            FindIterable<Document> findIterable = mongoClient.getDatabase(DATABASE_NAME)
                    .getCollection(collectionName)
                    .find(filter);
            if (orderByTimestampAsc) {
                findIterable = findIterable.sort(Sorts.ascending(CacheEntry.Field.TIMESTAMP.toString()));
            }
            String winningPlan = findIterable.explain(ExplainVerbosity.QUERY_PLANNER)
                    .get("queryPlanner", Document.class)
                    .get("winningPlan", Document.class)
                    .toJson();
            assertThat(winningPlan)
                    .describedAs("%nWinning plan for filter %s", filter)
                    .doesNotContain("COLLSCAN");
        }

        @DisplayName("Test MaintenanceWorker")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_MaintenanceWorker_fails_and_retries() {
            int maximumSize = 1;
            int retainedMaximumSize = 2;

            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);

            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder()
                                    .maximumSize(maximumSize))
                            .withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumSize(retainedMaximumSize))),
                    DistributedCaffeine::build);

            // inject a spy into the maintenance worker (to provoke a failure later) and shorten the (otherwise
            // minute-long) maintenance interval so the scheduled maintenance runs frequently enough to be observed
            // within the test's waiting duration; (re)activate to apply it - from here the maintenance worker runs
            // continuously in the background (as it does in production, only faster)
            InternalMaintenanceWorker<Key, Value> maintenanceWorker = getInstanceRegistry(distributedCache)
                    .getMaintenanceWorker();
            InternalCacheManager<Key, Value> cacheManager = injectSpy(maintenanceWorker, InternalMaintenanceWorker.class,
                    "cacheManager", InternalCacheManager.class);
            writeFieldValue(maintenanceWorker, InternalMaintenanceWorker.class,
                    "MAINTENANCE_INTERVAL", Duration.ofMillis(100));
            maintenanceWorker.deactivate();
            maintenanceWorker.activate();

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);
            Key key3 = Key.of(3);
            Key key4 = Key.of(4);
            Key key5 = Key.of(5);
            Value value = Value.of(0);

            distributedCache.put(key1, value);

            await("caching")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(maximumSize))));

            // create retained-by-size entries up to (but not exceeding) the retained maximum size;
            // the background maintenance runs continuously but has nothing to prune yet
            distributedCache.put(key2, value); // implicit eviction

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(maximumSize)),
                            Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(1))));

            distributedCache.put(key3, value); // implicit eviction

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(maximumSize)),
                            Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(retainedMaximumSize))));

            loggerDistributedCaffeine.startCapturing();

            // provoke failure
            doThrow(new IllegalStateException()).when(cacheManager).cleanup();

            // the maintenance worker kicks in in the background, fails, and reschedules itself with a retry warning
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

            // create more retained-by-size entries than the retained maximum size allows;
            // the background maintenance keeps failing, so the overflow is left unpruned
            distributedCache.put(key4, value); // implicit eviction

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(maximumSize)),
                            Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(retainedMaximumSize + 1))));

            distributedCache.put(key5, value); // implicit eviction

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(maximumSize)),
                            Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(retainedMaximumSize + 2))));

            // fix failure
            doCallRealMethod().when(cacheManager).cleanup();

            // with the failure fixed, the background maintenance recovers on its own (no explicit trigger) and prunes
            // retained-by-size entries down to the retained maximum size; this distribution mode distributes
            // evictions, so residency is a property of every cache instance alike and the pruned overflow is
            // invalidated (and only later removed as distribution-only, so it still lingers within the waiting duration)
            await("recovery")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, assertion -> assertion.isEqualTo(maximumSize)),
                            Count.of(EVICTED_SIZE_RETAINED, assertion -> assertion.isEqualTo(retainedMaximumSize)),
                            Count.of(INVALIDATED, assertion -> assertion.isEqualTo(2))));
        }

        @DisplayName("Test MongoSynchronizer")
        @Test
        @EnabledIf("isMongo")
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_MongoSynchronizer_fails_and_retries() throws Exception {
            // early (fail-fast) failure: watching change streams requires majority read concern, so building a cache
            // whose collection uses a local read concern fails immediately (without retrying)
            try (MongoClient failFastMongoClient = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(mongoContainer.getReplicaSetUrl()))
                    .readConcern(ReadConcern.LOCAL)
                    .build())) {
                MongoAdapter<Key, Value> localReadConcernAdapter = MongoAdapter
                        .newBuilder(failFastMongoClient, DATABASE_NAME, getCollectionName())
                        .build();
                assertThatThrownBy(() -> DistributedCaffeine.newBuilder(localReadConcernAdapter).build())
                        .isExactlyInstanceOf(MongoClientException.class)
                        .hasMessageStartingWith("Watching change streams failed")
                        .hasMessageNotContaining("Retrying")
                        .cause()
                        .isExactlyInstanceOf(MongoCommandException.class)
                        .hasMessageContainingAll(ReadConcernLevel.LOCAL.getValue(), ReadConcernLevel.MAJORITY.getValue());
            }

            // both the "watching failed" and the "deserializing failed" warnings are logged under the MongoSynchronizer
            // class (which is package-private, so it is captured by its fully-qualified name)
            CaptureLogger loggerMongoSynchronizer = CaptureLoggerFactory
                    .getCaptureLogger("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoSynchronizer");

            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            DistributedCache<Key, Value> syncedDistributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);

            // reach the synced instance's change stream watcher to provoke inbound-processing failures in the background;
            // the watcher applies inbound changes via its receiver and deserializes them with its value serializer
            Adapter<Key, Value> syncedAdapter = getInstanceRegistry(syncedDistributedCache).getAdapter();
            Synchronizer<Key, Value> syncedSynchronizer = readFieldValue(syncedAdapter, AbstractAdapter.class,
                    "synchronizer", Synchronizer.class);
            Receiver<Key, Value> syncedReceiver = injectSpy(syncedSynchronizer, AbstractSynchronizer.class,
                    "receiver", Receiver.class);
            Serializer<Value, ?> syncedValueSerializer = injectSpy(syncedSynchronizer, AbstractSynchronizer.class,
                    "valueSerializer", Serializer.class);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            Key key3 = Key.of(3);
            Value value3 = Value.of(3);
            Key key4 = Key.of(4);
            Value value4 = Value.of(4);

            // a position to resume watching from must exist before any event has arrived, otherwise a cursor failing
            // in an idle period would resume at "now" and silently skip whatever is written while watching is down.
            // The server reports one for every polled batch, so it appears without anything having happened
            AtomicReference<?> resumeToken = readFieldValue(syncedSynchronizer,
                    syncedSynchronizer.getClass(), "resumeToken", AtomicReference.class);

            await("resume position while idle")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(resumeToken.get()).isNotNull());

            Object resumeTokenWhileIdle = resumeToken.get();

            // baseline: the change stream watcher synchronizes changes from the other instance in the background
            distributedCache.put(key1, value1);

            await("synchronization")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(syncedDistributedCache.getIfPresent(key1)).isEqualTo(value1));

            // and it advances as events are applied, so a failure resumes after the last one instead of repeating it
            assertThat(resumeToken.get()).isNotEqualTo(resumeTokenWhileIdle);

            // watching change streams fails and retries
            loggerMongoSynchronizer.startCapturing();

            // provoke failure in the inbound apply step of the synced instance's watcher
            doThrow(new IllegalStateException()).when(syncedReceiver).receiveCacheEntries(any());

            distributedCache.put(key2, value2);

            await("failure")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        List<LoggingEvent> loggingEvents = loggerMongoSynchronizer.getLoggingEvents();
                        assertThat(loggingEvents).isNotEmpty();
                        assertThat(loggingEvents).allMatch(loggingEvent ->
                                loggingEvent.getLevel().equals(Level.WARN)
                                        && loggingEvent.getMessage().startsWith("Watching change streams failed")
                                        && loggingEvent.getMessage().endsWith("Retrying..."));
                    });

            loggerMongoSynchronizer.stopCapturing();

            // while watching fails, the synced instance does not receive the update
            assertThat(syncedDistributedCache.getIfPresent(key2)).isNull();

            // fix failure: the watcher recovers on its own and applies the missed update
            doCallRealMethod().when(syncedReceiver).receiveCacheEntries(any());

            await("recovery")
                    .atMost(WAITING_DURATION.plusSeconds(10)) // retry delay is increased on failure
                    .untilAsserted(() -> assertThat(syncedDistributedCache.getIfPresent(key2)).isEqualTo(value2));

            // reading an inbound cache entry fails and is skipped (without failing the watcher)
            loggerMongoSynchronizer.startCapturing();

            // provoke failure when deserializing inbound cache entries
            doThrow(new IllegalStateException()).when(syncedValueSerializer).deserialize(any());

            distributedCache.put(key3, value3);

            await("failure")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        List<LoggingEvent> loggingEvents = loggerMongoSynchronizer.getLoggingEvents();
                        assertThat(loggingEvents).isNotEmpty();
                        assertThat(loggingEvents).allMatch(loggingEvent ->
                                loggingEvent.getLevel().equals(Level.WARN)
                                        && loggingEvent.getMessage().startsWith("Reading of cache entry failed")
                                        && loggingEvent.getMessage().endsWith("Skipping..."));
                    });

            loggerMongoSynchronizer.stopCapturing();

            // the entry that failed to deserialize is skipped and not applied (the watcher keeps running)
            assertThat(syncedDistributedCache.getIfPresent(key3)).isNull();

            // fix failure: subsequent inbound cache entries are synchronized again
            doCallRealMethod().when(syncedValueSerializer).deserialize(any());

            distributedCache.put(key4, value4);

            await("recovery")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(syncedDistributedCache.getIfPresent(key4)).isEqualTo(value4));

            // a failed activation must not poison later activations: the throwable recorded while activating is
            // cleared by deactivate() only, which InternalInstanceRegistry.deactivate() skips while the cache does
            // not count as activated - precisely the state a failed activation leaves behind. Simulate that state by
            // planting a throwable while deactivated (the synchronizer is package-private in another package, so its
            // class is reached via getClass()) and assert that activating still succeeds and resumes synchronizing.
            Key key5 = Key.of(5);
            Value value5 = Value.of(5);

            syncedDistributedCache.distributedPolicy().stopSynchronization();

            AtomicReference<Throwable> failFastThrowable = readFieldValue(syncedSynchronizer,
                    syncedSynchronizer.getClass(), "failFastThrowable", AtomicReference.class);
            failFastThrowable.set(new IllegalStateException("stale activation failure"));

            assertThatNoException().isThrownBy(() ->
                    syncedDistributedCache.distributedPolicy().startSynchronization());

            distributedCache.put(key5, value5);

            await("recovery after a failed activation")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(syncedDistributedCache.getIfPresent(key5)).isEqualTo(value5));

            // a retry attempt scheduled before deactivation must not start watching again. The retry policy decides
            // whether to abort at failure time only, so an attempt already queued behind a delay still runs after
            // deactivate() - simulated here by invoking the watcher directly while deactivated. It has to return
            // without watching: otherwise it would report itself activated (leaving the adapter activated while the
            // rest of the cache is deactivated) and enter a loop that never ends, so the next activation would join
            // a future that can never complete
            Key key6 = Key.of(6);
            Value value6 = Value.of(6);

            syncedDistributedCache.distributedPolicy().stopSynchronization();

            CompletableFuture<Void> staleWatchAttempt = CompletableFuture.runAsync(() ->
                    invokeMethod(syncedSynchronizer, syncedSynchronizer.getClass(),
                            "processChangeStreams", List.of(), List.of()));

            assertThat(staleWatchAttempt).succeedsWithin(WAITING_DURATION);
            assertThat(syncedSynchronizer.isActivated()).isFalse();

            // and activating afterwards still works, without joining a never-completing watcher
            assertThatNoException().isThrownBy(() ->
                    syncedDistributedCache.distributedPolicy().startSynchronization());

            distributedCache.put(key6, value6);

            await("recovery after a stale watch attempt")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(syncedDistributedCache.getIfPresent(key6)).isEqualTo(value6));
        }

        @DisplayName("Stress test synchronization from data store")
        @Test
        @SuppressWarnings("FutureReturnValueIgnored") // background load, awaited through loopCondition/loopCounter
        void stress_test_DistributedCaffeine_synchronization_from_data_store() throws Exception {
            int maximumSize = runsOnGitHub(10_000, 100_000);
            int retainedMaximumSize = maximumSize / 100;
            int numberOfOperations = 10_000;

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> removalListener = mock(RemovalListener.class);
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                public Value load(@NonNull Key key) {
                    return nextInt(10) == 0
                            ? null
                            : Value.of(key.getId(), nameWithMillisAndPrefixes("load"));
                }

                @Override
                public @NonNull Map<? extends Key, ? extends Value> loadAll(@NonNull Set<? extends Key> keys) {
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
                                // a reactivated cache instance synchronizing from the data store is the whole
                                // subject of this test
                                .withPersistence(configurer -> configurer
                                        .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency)
                                        .withEvictedEntries(evictedEntries -> evictedEntries
                                                .withMaximumSize(retainedMaximumSize)
                                                .withLoadingStrategies(CACHE_LOADER))),
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
                                CountGrouped.of(EVICTED_RETAINED_GROUP, assertion -> assertion.isGreaterThanOrEqualTo(retainedMaximumSize)));
                    });

            await("maintenance")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process maintenance", this::processMaintenance)
                    .untilAsserted(() ->
                            assertThatDataStoreHasCounts(
                                    CountGrouped.of(DISTRIBUTION_ONLY_GROUP, assertion -> assertion.isEqualTo(0)),
                                    CountGrouped.of(EVICTED_RETAINED_GROUP, assertion -> assertion.isEqualTo(retainedMaximumSize))));

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
        @SuppressWarnings("FutureReturnValueIgnored") // delayed executor shutdown, deliberately not awaited
        void stress_test_DistributedCaffeine_thread_safety(String valueSource) throws Exception {
            int maximumSize = 100;
            int retainedMaximumSize = maximumSize / 10;
            int numberOfOperations = 1_000;
            int levelOfParallelism = runsOnGitHub(3, 10);

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> removalListener = mock(RemovalListener.class);
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                public Value load(@NonNull Key key) {
                    return nextInt(10) == 0
                            ? null
                            : Value.of(key.getId(), nameWithMillisAndPrefixes("load"));
                }

                @Override
                public @NonNull Map<? extends Key, ? extends Value> loadAll(@NonNull Set<? extends Key> keys) {
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
                                // these stress tests stop and start synchronization midway and then assert that
                                // the cache instances converge, which is what synchronizing from the data store
                                // does - and they assert a count of cached entries there, which presupposes
                                // that those are retained rather than swept once distributed
                                .withPersistence(configurer -> configurer
                                        .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency)
                                        .withEvictedEntries(evictedEntries -> evictedEntries
                                                .withMaximumSize(retainedMaximumSize)
                                                .withLoadingStrategies(CACHE_LOADER))),
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
                                sleep(Duration.ofSeconds(1));
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
                                CountGrouped.of(EVICTED_RETAINED_GROUP, assertion -> assertion.isGreaterThanOrEqualTo(retainedMaximumSize)));
                    });

            await("maintenance")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process maintenance", this::processMaintenance)
                    .untilAsserted(() ->
                            assertThatDataStoreHasCounts(
                                    CountGrouped.of(DISTRIBUTION_ONLY_GROUP, assertion -> assertion.isEqualTo(0)),
                                    CountGrouped.of(EVICTED_RETAINED_GROUP, assertion -> assertion.isEqualTo(retainedMaximumSize))));

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
            int retainedMaximumSize = maximumSize / 2;
            int numberOfOperations = 10_000;
            int levelOfParallelism = runsOnGitHub(3, 10);

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> removalListener = mock(RemovalListener.class);
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                public Value load(@NonNull Key key) {
                    return nextInt(10) == 0
                            ? null
                            : Value.of(key.getId(), nameWithMillisAndPrefixes("load"));
                }

                @Override
                public @NonNull Map<? extends Key, ? extends Value> loadAll(@NonNull Set<? extends Key> keys) {
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
                                // these stress tests stop and start synchronization midway and then assert that
                                // the cache instances converge, which is what synchronizing from the data store
                                // does - and they assert a count of cached entries there, which presupposes
                                // that those are retained rather than swept once distributed
                                .withPersistence(configurer -> configurer
                                        .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency)
                                        .withEvictedEntries(evictedEntries -> evictedEntries
                                                .withMaximumSize(retainedMaximumSize)
                                                .withLoadingStrategies(CACHE_LOADER))),
                        dc -> dc.build(cacheLoader));
                return (DistributedLoadingCache<Key, Value>) cache;
            };

            List<DistributedLoadingCache<Key, Value>> distributedLoadingCaches = new ArrayList<>();
            List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

            IntStream.rangeClosed(1, levelOfParallelism).forEach(cacheIndex ->
                    completableFutures.add(CompletableFuture.runAsync(() -> {
                        sleep(Duration.ofSeconds(1).multipliedBy(min(10, cacheIndex - 1)));
                        AtomicLong ticker = new AtomicLong(0);
                        DistributedLoadingCache<Key, Value> distributedLoadingCache = cacheSupplier.apply(ticker);
                        distributedLoadingCaches.add(distributedLoadingCache);
                        IntStream.rangeClosed(1, numberOfOperations).forEach(operationIndex -> {
                            executeRandomOperation(distributedLoadingCache, maximumSize);
                            if (operationIndex == numberOfOperations / 2) {
                                distributedLoadingCache.distributedPolicy().stopSynchronization();
                                sleep(Duration.ofSeconds(1).multipliedBy(min(10, cacheIndex)));
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
                                CountGrouped.of(EVICTED_RETAINED_GROUP, assertion -> assertion.isEqualTo(0)),
                                CountGrouped.of(INVALIDATED_GROUP, assertion -> assertion.isGreaterThanOrEqualTo(retainedMaximumSize)));
                    });

            await("maintenance")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast("process maintenance", this::processMaintenance)
                    .untilAsserted(() ->
                            assertThatDataStoreHasCounts(
                                    CountGrouped.of(DISTRIBUTION_ONLY_GROUP, assertion -> assertion.isEqualTo(0)),
                                    CountGrouped.of(EVICTED_RETAINED_GROUP, assertion -> assertion.isEqualTo(0))));

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

        boolean isMongo;

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

            isMongo = dockerImageName.asCanonicalNameString().toLowerCase(Locale.ROOT).contains("mongo");
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
                            format("with %s.%s", DistributionMode.class.getSimpleName(), distributionMode.name()),
                            dc -> {
                                DistributedCaffeine<Key, Value> builder = dc.withDistributionMode(distributionMode);
                                // bounded by time rather than by cache residency, because tests using this provider
                                // add eviction policies of their own and residency would then require the mode to
                                // include evictions, which half of these deliberately do not
                                return distributionMode.isPopulationConsidered()
                                        ? builder.withPersistence(configurer -> configurer
                                                .withCachedEntries(cachedEntries -> cachedEntries
                                                        .withMaximumTime(Duration.ofDays(1))))
                                        : builder;
                            }));
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
            MongoAdapter<K, V> mongoAdapter = MongoAdapter
                    .newBuilder(mongoClient, DATABASE_NAME, getCollectionName())
                    .build();
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
                // minus one milli as operator for data store is '<' not '<='
                Duration distributionDuration = Duration.ZERO.minus(Duration.ofMillis(1));
                invokeMethod(maintenanceWorker, InternalMaintenanceWorker.class,
                        "processMaintenance", List.of(Duration.class), List.of(distributionDuration));
            });
        }

        void cleanUp() {
            distributedCacheInstances.forEach(DistributedCache::cleanUp);
        }

        void assertThatDataStoreIsEmpty() {
            assertThatDataStoreHasCounts(new Count[0]);
        }

        @SuppressWarnings("ReturnValueIgnored") // the assertion runs inside apply(), its result is of no interest
        void assertThatDataStoreHasCounts(Count... counts) {
            Repository<?, ?> repository = getInstanceRegistry(distributedCacheInstances.stream()
                    .findFirst()
                    .orElseThrow())
                    .getAdapter()
                    .getRepository()
                    .orElseThrow();
            Map<Status, Count> statusToCount = Stream.of(counts)
                    .collect(toMap(Count::status, Function.identity()));
            Stream.of(Status.values())
                    .map(status -> statusToCount.getOrDefault(status,
                            Count.of(status, assertion -> assertion.isEqualTo(0))))
                    .forEach(count -> count.assertion()
                            .apply(assertThat(getFailable(() ->
                                    repository.countCacheEntries(Set.of(count.status()))))
                                    .describedAs("%nCount for %s", count.status().name())));
        }

        @SuppressWarnings("ReturnValueIgnored") // the assertion runs inside apply(), its result is of no interest
        void assertThatDataStoreHasCounts(CountGrouped... countsGrouped) {
            Repository<?, ?> repository = getInstanceRegistry(distributedCacheInstances.stream()
                    .findFirst()
                    .orElseThrow())
                    .getAdapter()
                    .getRepository()
                    .orElseThrow();
            Stream.of(countsGrouped)
                    .forEach(count -> count.assertion()
                            .apply(assertThat(getFailable(() ->
                                    repository.countCacheEntries(count.statuses())))
                                    .describedAs("%nCount for %s", count.statuses())));
        }

        <K, V> InternalInstanceRegistry<K, V> getInstanceRegistry(DistributedCache<K, V> distributedCache) {
            return ((InternalDistributedCache<K, V>) distributedCache).instanceRegistry;
        }

        <K, V> Repository<K, V> repositoryOf(DistributedCache<K, V> distributedCache) {
            return distributedCache.distributedPolicy().getAdapter().getRepository().orElseThrow();
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

        boolean isMongo() {
            return isMongo;
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

        @SuppressWarnings("unused")
        <K, V> void printMongoCollection(Status... statuses) {
            AtomicInteger counter = new AtomicInteger(0);
            Repository<?, ?> repository = distributedCacheInstances.stream()
                    .findFirst()
                    .map(this::getInstanceRegistry)
                    .map(InternalInstanceRegistry::getAdapter)
                    .flatMap(Adapter::getRepository)
                    .orElseThrow();
            Set<Status> statusesOrNull = statuses.length == 0
                    ? null
                    : Set.of(statuses);
            try (Stream<? extends CacheEntry<?, ?>> cacheEntryStream = getFailable(() ->
                    repository.streamCacheEntries(null, statusesOrNull, false))) {
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

        @NullUnmarked
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

            @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
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
