/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Updates;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.Builder;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeineIntegrationTests.DistributedCaffeineIntegrationTestInstance.DockerImage;
import io.github.oberhoff.distributedcaffeine.DistributedPolicy.CacheEntry;
import io.github.oberhoff.distributedcaffeine.common.DistributedCaffeineCommonTestInstance;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import io.github.oberhoff.distributedcaffeine.common.logging.CaptureLogger;
import io.github.oberhoff.distributedcaffeine.common.logging.CaptureLoggerFactory;
import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.ForySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.assertj.core.api.AbstractLongAssert;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
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
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.platform.commons.util.ReflectionUtils.HierarchyTraversalMode;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributedCaffeineIntegrationTests.DistributedCaffeineIntegrationTestInstance.RUNS_ON_GITHUB;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.EXPIRES;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.HASH;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.STALE;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.STATUS;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED_GROUP;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_EXTENDED_GROUP;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_SIZE;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_SIZE_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_TIME;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_TIME_EXTENDED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.INVALIDATED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.INVALIDATED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.entry;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.runFailable;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.time.temporal.ChronoUnit.FOREVER;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCollection;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.params.ParameterizedInvocationConstants.ARGUMENTS_WITH_NAMES_PLACEHOLDER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
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

    @SuppressWarnings({"squid:S5838", "squid:S5778", "squid:S5961"})
    abstract static class DistributedCaffeineIntegration extends DistributedCaffeineIntegrationTestInstance {

        @DisplayName("Test put() and getIfPresent()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedCache_put_getIfPresent(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
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
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allCaches.forEach(cache -> {
                            assertThat(cache.estimatedSize()).isEqualTo(1);
                            assertThat(cache.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(cache.getIfPresent(Key.of(0))).isNull();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
                    });
        }

        @DisplayName("Test putAll() and getAllPresent()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedCache_putAll_getAllPresent(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(0)));
                    });
        }

        @DisplayName("Test get() and getAll()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedCache_get_getAll(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(7), s -> s.isEqualTo(0)));
                    });
        }

        @DisplayName("Test invalidate() and invalidateAll()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedCache_invalidate_invalidateAll(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(5), s -> s.isEqualTo(0)));
                    });

            featureParityCaches.forEach(cache ->
                    cache.invalidate(key1));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(4), s -> s.isEqualTo(1)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(1)));
                    });

            featureParityCaches.forEach(cache ->
                    cache.invalidateAll(map2to3.keySet()));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allCaches.forEach(cache -> {
                            assertThat(cache.estimatedSize()).isEqualTo(2);
                            assertThat(cache.getIfPresent(key1)).isNull();
                            assertThat(cache.getAllPresent(map2to3.keySet())).isEmpty();
                            assertThat(cache.getAllPresent(map4to5.keySet()))
                                    .containsAllEntriesOf(map4to5);
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(3)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(3)));
                    });

            featureParityCaches.forEach(Cache::invalidateAll);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allCaches.forEach(cache -> {
                            assertThat(cache.estimatedSize()).isEqualTo(0);
                            assertThat(cache.getIfPresent(key1)).isNull();
                            assertThat(cache.getAllPresent(map2to3.keySet())).isEmpty();
                            assertThat(cache.getAllPresent(map4to5.keySet())).isEmpty();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(5)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(5)));
                    });
        }

        @DisplayName("Test stats()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_DistributedCache_stats(CacheFactory<Key, Value> cacheFactory) {
            Caffeine<Object, Object> caffeineBuilder = Caffeine.newBuilder()
                    .recordStats();

            CacheBuilder<Key, Value> cacheBuilder =
                    b -> b.withCaffeineBuilder(caffeineBuilder);

            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);
            Cache<Key, Value> caffeineCache = caffeineBuilder
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
            Set<Cache<Key, Value>> featureParityCaches = Set.of(distributedCache, caffeineCache);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            EqualResult<Key, Value> statsResult = new EqualResult<>();

            featureParityCaches.forEach(cache -> {
                cache.put(key1, value1);
                cache.getIfPresent(key1);
                cache.getIfPresent(Key.of(0));
                statsResult.setObject(cache.stats());
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allCaches.forEach(cache -> {
                            assertThat(statsResult.<CacheStats>getObject().hitCount()).isOne();
                            assertThat(statsResult.<CacheStats>getObject().missCount()).isOne();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
                    });
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
                    b -> b.build(cacheLoader));
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    b -> b.build(cacheLoader));
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

            InternalCacheLoader<Key, Value> internalCacheLoader = getDistributedCaffeine(distributedLoadingCache).getCacheLoader();
            assertThatExceptionOfType(IllegalAccessException.class).isThrownBy(() -> internalCacheLoader.load(_null()));
            assertThatExceptionOfType(IllegalAccessException.class).isThrownBy(() -> internalCacheLoader.loadAll(_null()));
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(3), s -> s.isEqualTo(0)));
                    });

            int levelOfParallelism = 10;
            AtomicInteger counter = new AtomicInteger(0);

            doAnswer(invocation -> {
                await(Duration.ofMillis(100));
                return Value.of(counter.incrementAndGet(), "counted");
            }).when(cacheLoader).load(key1);

            featureParityCaches.forEach(loadingCache -> {
                loadingCache.invalidate(key1);
                IntStream.rangeClosed(1, levelOfParallelism)
                        .mapToObj(i -> CompletableFuture.runAsync(() ->
                                loadingCache.get(key1), executorService))
                        .toList() // intermediate step to ensure concurrency
                        .forEach(CompletableFuture::join);
                counter.set(0);
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() ->
                            allCaches.forEach(loadingCache ->
                                    assertThat(loadingCache.getIfPresent(key1)).isNotNull()
                                            .satisfies(value -> assertThat(requireNonNull(value).getId()).isLessThan(levelOfParallelism))
                                            .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("counted"))));
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
                    b -> b.build(cacheLoader));
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    b -> b.build(cacheLoader));
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

            InternalCacheLoader<Key, Value> internalCacheLoader = getDistributedCaffeine(distributedLoadingCache).getCacheLoader();
            assertThatExceptionOfType(IllegalAccessException.class).isThrownBy(() -> internalCacheLoader.load(_null()));
            assertThatExceptionOfType(IllegalAccessException.class).isThrownBy(() -> internalCacheLoader.loadAll(_null()));
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(6), s -> s.isEqualTo(0)));
                    });
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
                    b -> b.build(cacheLoader));
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    b -> b.build(cacheLoader));
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED_REFRESHED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                Count.of(CACHED_REFRESHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
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
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            assertThat(loadingCache.estimatedSize()).isEqualTo(0);
                            assertThat(loadingCache.getIfPresent(key1)).isEqualTo(refreshedValue1.getValue())
                                    .isNull();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                Count.of(CACHED_REFRESHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                Count.of(INVALIDATED_REFRESHED, f -> f.isEqualTo(0), s -> s.isEqualTo(1)));
                    });

            int levelOfParallelism = 10;
            AtomicInteger counter = new AtomicInteger(0);

            doAnswer(invocation -> {
                await(Duration.ofMillis(100));
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
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() ->
                            allCaches.forEach(loadingCache ->
                                    assertThat(loadingCache.getIfPresent(key1)).isNotNull()
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
                    b -> b.build(cacheLoader));
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    b -> b.build(cacheLoader));
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED_REFRESHED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                Count.of(CACHED_REFRESHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
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
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allCaches.forEach(loadingCache -> {
                            assertThat(loadingCache.estimatedSize()).isEqualTo(0);
                            assertThat(loadingCache.getAllPresent(keys1))
                                    .containsAllEntriesOf(refreshedMap1.getMap())
                                    .isUnmodifiable()
                                    .isEmpty();
                        });
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                Count.of(CACHED_REFRESHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                Count.of(INVALIDATED_REFRESHED, f -> f.isEqualTo(0), s -> s.isEqualTo(1)));
                    });

            int levelOfParallelism = 10;
            AtomicInteger counter = new AtomicInteger(0);

            doAnswer(invocation -> {
                await(Duration.ofMillis(100));
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
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() ->
                            allCaches.forEach(loadingCache ->
                                    assertThat(loadingCache.getIfPresent(key1)).isNotNull()
                                            .satisfies(value -> assertThat(requireNonNull(value).getId()).isLessThan(levelOfParallelism))
                                            .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("counted"))));
        }

        @DisplayName("Test put(), putIfAbsent(), putAll() and get() via asMap()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_put_putIfAbsent_putAll_get(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(4), s -> s.isEqualTo(1)));
                    });
        }

        @DisplayName("Test replace()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_replace(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(2)));
                    });
        }

        @DisplayName("Test remove(), contains*() and clear()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_remove_contains_clear(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(2)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
                    });

            featureParityMaps.forEach(Map::clear);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allMaps.forEach(map ->
                                assertThat(map).isEmpty());
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(3)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(3)));
                    });
        }

        @DisplayName("Test keySet()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_keySet(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(5)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(5)));
                    });

            // noinspection All
            featureParityMaps.forEach(map ->
                    map.keySet().clear());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allMaps.forEach(map ->
                                assertThat(map.keySet()).isEmpty());
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(6)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(6)));
                    });
        }

        @DisplayName("Test values()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_values(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(5)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(5)));
                    });

            // noinspection All
            featureParityMaps.forEach(map ->
                    map.values().clear());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allMaps.forEach(map ->
                                assertThat(map.values()).isEmpty());
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(6)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(6)));
                    });
        }

        @DisplayName("Test entrySet()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_entrySet(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            Cache<Key, Value> caffeineCache = Caffeine.newBuilder()
                    .build();

            Set<Cache<Key, Value>> allCaches = Set.of(distributedCache, syncedDistributedCache, caffeineCache);
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(5)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(5)));
                    });

            featureParityMaps.forEach(map ->
                    map.entrySet().iterator().next().setValue(Value.of(6, "write through")));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allMaps.forEach(map ->
                                assertThat(map.get(entry6.getKey())).isEqualTo(Value.of(6, "write through")));
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(6)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(5)));
                    });

            // noinspection All
            featureParityMaps.forEach(map ->
                    map.entrySet().clear());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allMaps.forEach(map ->
                                assertThat(map.entrySet()).isEmpty());
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(7)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(6)));
                    });
        }

        @DisplayName("Test policy()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_Policy(CacheFactory<Key, Value> cacheFactory) {
            Caffeine<Object, Object> caffeineBuilder = Caffeine.newBuilder()
                    .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration()));

            CacheBuilder<Key, Value> cacheBuilder =
                    b -> b.withCaffeineBuilder(caffeineBuilder);

            DistributedCache<Key, Value> distributedCache = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);
            Cache<Key, Value> caffeineCache = caffeineBuilder
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(3), s -> s.isEqualTo(1)));
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
                    .failFast(() -> speedUpAssertions(allCaches))
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
                                Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(4)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(1)),
                                Count.of(EVICTED_TIME, f -> f.isEqualTo(0), s -> s.isEqualTo(6)));
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
                    .failFast(() -> speedUpAssertions(allCaches))
                    .untilAsserted(() -> {
                        allCaches.forEach(cache ->
                                assertThat(cache.estimatedSize()).isEqualTo(1));
                        allPolicies.forEach(policy ->
                                assertThat(policy.getIfPresentQuietly(key1)).isEqualTo(value1));
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(4)),
                                Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(1)),
                                Count.of(EVICTED_TIME, f -> f.isEqualTo(0), s -> s.isEqualTo(6)));
                    });
        }

        @DisplayName("Test distributedPolicy()")
        @Test
        void test_DistributedPolicy() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                    .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration())))
                            .withExtendedPersistence(FOREVER.getDuration()),
                    Builder::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            assertThat(distributedPolicy.getKeySerializer())
                    .isInstanceOfAny(ByteArraySerializer.class, StringSerializer.class, JsonSerializer.class);
            assertThat(distributedPolicy.getValueSerializer())
                    .isInstanceOfAny(ByteArraySerializer.class, StringSerializer.class, JsonSerializer.class);

            Key manipulatedKey = Key.of(0);
            Value manipulatedValue = Value.of(0); //will never be returned
            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            assertThatNullPointerException().isThrownBy(() -> distributedPolicy.getFromMongo(_null(), true));
            assertThatNullPointerException().isThrownBy(() -> distributedPolicy.getAllFromMongo(_null(), true));
            assertThatNullPointerException().isThrownBy(() -> distributedPolicy.getAllFromMongo(_set(null), true));

            VarExpiration<Key, Value> varExpiration = distributedCache.policy().expireVariably().orElseThrow();

            varExpiration.put(manipulatedKey, manipulatedValue, Duration.ofHours(1));

            // create inconsistencies in relation to hash values
            distributedPolicy.getMongoCollection()
                    .updateMany(Filters.empty(), Updates.set(HASH.toString(), key1.hashCode()));

            varExpiration.put(key1, value1, Duration.ofHours(1));
            varExpiration.put(key1, value1, Duration.ofHours(1));
            varExpiration.put(key2, value2, Duration.ZERO); // fast eviction

            // test equals() and hashCode() and toString()
            CacheEntry<Key, Value> cacheEntry1 = distributedPolicy.getFromMongo(key1, true);
            CacheEntry<Key, Value> cacheEntry2 = distributedPolicy.getFromMongo(key2, true);
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
                    .failFast(() -> speedUpAssertions(distributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedPolicy.getFromMongo(key1, false)).isNotNull();
                        assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull();
                        assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                        assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull();
                        assertThat(distributedPolicy.getAllFromMongo(Set.of(key1, key2), false))
                                .containsOnly(cacheEntry1)
                                .allSatisfy(entry -> assertThat(entry.getId()).isNotBlank())
                                .allSatisfy(entry -> assertThat(entry.getKey()).isEqualTo(key1))
                                .allSatisfy(entry -> assertThat(entry.getValue()).isEqualTo(value1))
                                .allSatisfy(entry -> assertThat(entry.isEvicted()).isFalse());
                        assertThat(distributedPolicy.getAllFromMongo(Set.of(key1, key2), true))
                                .containsOnly(cacheEntry1, cacheEntry2);
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(2)),
                                Count.of(EVICTED_TIME_EXTENDED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
                    });
        }


        @DisplayName("Test population")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_population(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCacheA = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> distributedCacheB = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);

            DistributionMode distributionMode = getDistributedCaffeine(distributedCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);
            distributedCacheB.put(key2, value2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
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
                                    Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(0)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            distributedCacheA.put(key2, value2);
            distributedCacheB.put(key1, value1);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
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
                                    Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedCacheA, distributedCacheB));
        }

        @DisplayName("Test invalidation")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_invalidation(CacheFactory<Key, Value> cacheFactory) {
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> removalListener = mock(RemovalListener.class);

            CacheBuilder<Key, Value> cacheBuilder =
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .removalListener(removalListener));

            DistributedCache<Key, Value> distributedCacheA = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);
            DistributedCache<Key, Value> distributedCacheB = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);

            DistributionMode distributionMode = getDistributedCaffeine(distributedCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);
            distributedCacheB.put(key2, value2);

            verifyNoInteractions(removalListener);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
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
                                    Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(0)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            distributedCacheA.invalidate(key2);
            distributedCacheB.invalidate(key1);

            await("invalidation")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                    Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            distributedCacheA.invalidate(key1);
            distributedCacheB.invalidate(key2);

            await("invalidation")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                    Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(INVALIDATED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
                        }
                    });

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedCacheA, distributedCacheB));
        }

        @DisplayName("Test eviction by size")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_eviction_by_size(CacheFactory<Key, Value> cacheFactory) {
            int maximumSize = 1;

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheBuilder<Key, Value> cacheBuilder =
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .evictionListener(evictionListener)
                            .maximumSize(maximumSize));

            DistributedCache<Key, Value> distributedCacheA = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);
            DistributedCache<Key, Value> distributedCacheB = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);

            DistributionMode distributionMode = getDistributedCaffeine(distributedCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);

            verifyNoInteractions(evictionListener);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            distributedCacheB.put(key2, value2); // implicit eviction

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE, f -> f.isEqualTo(0), s -> s.isBetween(1L, 2L)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            distributedCacheA.put(key2, value2);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(2)),
                                    Count.of(EVICTED_SIZE, f -> f.isEqualTo(0), s -> s.isBetween(1L, 2L)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE, f -> f.isEqualTo(0), s -> s.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            distributedCacheB.put(key1, value1); // implicit eviction

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(3)),
                                    Count.of(EVICTED_SIZE, f -> f.isEqualTo(0), s -> s.isBetween(2L, 4L)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(3)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedCacheA, distributedCacheB));
        }

        @DisplayName("Test eviction by time")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_eviction_by_time(CacheFactory<Key, Value> cacheFactory) {
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheBuilder<Key, Value> cacheBuilder =
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .evictionListener(evictionListener)
                            // variable expiration policy provides more control over evictions
                            .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration())));

            DistributedCache<Key, Value> distributedCacheA = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);
            DistributedCache<Key, Value> distributedCacheB = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);

            DistributionMode distributionMode = getDistributedCaffeine(distributedCacheA).getDistributionMode();
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
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            distributedCacheB.put(key2, value2);
            // explicit eviction
            varExpirationA.setExpiresAfter(key1, Duration.ZERO);
            varExpirationB.setExpiresAfter(key1, Duration.ZERO);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1)),
                                    Count.of(EVICTED_TIME, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME, f -> f.isEqualTo(0), s -> s.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            distributedCacheA.put(key2, value2);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(2)),
                                    Count.of(EVICTED_TIME, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME, f -> f.isEqualTo(0), s -> s.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            distributedCacheB.put(key1, value1);
            // explicit eviction
            varExpirationA.setExpiresAfter(key2, Duration.ZERO);
            varExpirationB.setExpiresAfter(key2, Duration.ZERO);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(3)),
                                    Count.of(EVICTED_TIME, f -> f.isEqualTo(0), s -> s.isEqualTo(4)));
                        } else if (distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(3)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME, f -> f.isEqualTo(0), s -> s.isEqualTo(3)));
                        } else if (distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedCacheA, distributedCacheB));
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
                    b -> b.build(cacheLoader));
            DistributedLoadingCache<Key, Value> distributedLoadingCacheB = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    CacheBuilder.identity(),
                    b -> b.build(cacheLoader));

            DistributionMode distributionMode = getDistributedCaffeine(distributedLoadingCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded"))
                    .when(cacheLoader).load(any(Key.class));

            Value loadedValue1 = distributedLoadingCacheA.refresh(key1).join();
            Value loadedValue2 = distributedLoadingCacheB.refresh(key2).join();

            verify(cacheLoader, times(2)).load(any(Key.class));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                                    Count.of(CACHED_REFRESHED, f -> f.isEqualTo(2), s -> s.isEqualTo(0)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1)
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("loaded"));
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2)
                                    .satisfies(value -> assertThat(requireNonNull(value).getName()).isEqualTo("loaded"));
                            assertThatDataStoreHasCounts(Count.isEmpty());
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                                    Count.of(CACHED_REFRESHED, f -> f.isEqualTo(2), s -> s.isEqualTo(2)));
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
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            doAnswer(invocation -> null)
                    .when(cacheLoader).load(any(Key.class));

            distributedLoadingCacheA.refresh(key1).join();
            distributedLoadingCacheB.refreshAll(Set.of(key2)).join();

            verify(cacheLoader, times(6)).load(any(Key.class));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED_REFRESHED, f -> f.isEqualTo(0), s -> s.isEqualTo(4)),
                                    Count.of(INVALIDATED_REFRESHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(INVALIDATED_REFRESHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
                        }
                    });

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedLoadingCacheA, distributedLoadingCacheB));
        }

        @DisplayName("Test refresh after write")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_refresh_after_write(CacheFactory<Key, Value> cacheFactory) throws Exception {
            CacheBuilder<Key, Value> cacheBuilder =
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
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
                    b -> b.build(cacheLoader));
            DistributedLoadingCache<Key, Value> distributedLoadingCacheB = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    b -> b.build(cacheLoader));

            DistributionMode distributionMode = getDistributedCaffeine(distributedLoadingCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedLoadingCacheA.put(key1, value1);
            distributedLoadingCacheB.put(key2, value2);

            verifyNoInteractions(cacheLoader);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            // get value quietly to not trigger refresh
                            assertThat(distributedLoadingCacheA.policy().getIfPresentQuietly(key1)).isEqualTo(value1);
                            assertThat(distributedLoadingCacheA.policy().getIfPresentQuietly(key2)).isEqualTo(value2);
                            assertThat(distributedLoadingCacheB.policy().getIfPresentQuietly(key1)).isEqualTo(value1);
                            assertThat(distributedLoadingCacheB.policy().getIfPresentQuietly(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(0)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            // get value quietly to not trigger refresh
                            assertThat(distributedLoadingCacheA.policy().getIfPresentQuietly(key1)).isEqualTo(value1);
                            assertThat(distributedLoadingCacheB.policy().getIfPresentQuietly(key2)).isEqualTo(value2);
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "refreshed"))
                    .when(cacheLoader).load(any(Key.class));

            distributedLoadingCacheA.getIfPresent(key1);
            distributedLoadingCacheB.getIfPresent(key2);

            await("asynchronous refreshes")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            verify(cacheLoader, times(2)).load(any(Key.class)));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            // get value quietly to not trigger refresh
                            assertThat(requireNonNull(distributedLoadingCacheA.policy().getIfPresentQuietly(key1)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key1.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(requireNonNull(distributedLoadingCacheA.policy().getIfPresentQuietly(key2)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key2.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(requireNonNull(distributedLoadingCacheB.policy().getIfPresentQuietly(key1)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key1.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(requireNonNull(distributedLoadingCacheB.policy().getIfPresentQuietly(key2)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key2.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                    Count.of(CACHED_REFRESHED_AFTER_WRITE, f -> f.isEqualTo(2), s -> s.isEqualTo(0)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            // get value quietly to not trigger refresh
                            assertThat(requireNonNull(distributedLoadingCacheA.policy().getIfPresentQuietly(key1)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key1.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThat(distributedLoadingCacheA.policy().getIfPresentQuietly(key2)).isNull();
                            assertThat(distributedLoadingCacheB.policy().getIfPresentQuietly(key1)).isNull();
                            assertThat(requireNonNull(distributedLoadingCacheB.policy().getIfPresentQuietly(key2)))
                                    .satisfies(value -> {
                                        assertThat(requireNonNull(value).getId()).isEqualTo(key2.getId());
                                        assertThat(value.getName()).isEqualTo("refreshed");
                                    });
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            doAnswer(invocation -> null)
                    .when(cacheLoader).load(any(Key.class));

            distributedLoadingCacheA.getIfPresent(key1);
            distributedLoadingCacheB.getIfPresent(key2);

            await("asynchronous refreshes")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            verify(cacheLoader, times(4)).load(any(Key.class)));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                    Count.of(CACHED_REFRESHED_AFTER_WRITE, f -> f.isEqualTo(0), s -> s.isEqualTo(2)),
                                    Count.of(INVALIDATED_REFRESHED_AFTER_WRITE, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(
                                    Count.of(INVALIDATED_REFRESHED_AFTER_WRITE, f -> f.isEqualTo(0), s -> s.isEqualTo(2)));
                        }
                    });

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedLoadingCacheA, distributedLoadingCacheB));
        }

        @DisplayName("Test synchronization")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_synchronization(CacheFactory<Key, Value> cacheFactory) {
            DistributedCache<Key, Value> distributedCacheA = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);

            DistributionMode distributionMode = getDistributedCaffeine(distributedCacheA).getDistributionMode();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);
            distributedCacheA.put(key2, value2);

            DistributedCache<Key, Value> distributedCacheB = cacheFactory.create(
                    CacheBuilder.identity(),
                    Builder::build);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedCacheB.asMap());
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(0)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedCacheA, distributedCacheB));
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
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                    .evictionListener(evictionListener)
                                    .maximumSize(maximumSize))
                            .withExtendedPersistence(extendedMaximumSize)
                            // just to test the distinction in logic
                            .withExtendedPersistence(FOREVER.getDuration());

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
                    b -> b.buildWithExtendedPersistence(cacheLoader));
            DistributedLoadingCache<Key, Value> distributedLoadingCacheB = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    b -> b.buildWithExtendedPersistence(cacheLoader));

            DistributionMode distributionMode = getDistributedCaffeine(distributedLoadingCacheA).getDistributionMode();
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(maximumSize), s -> s.isEqualTo(0)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNull();
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            Value loadedValue2 = distributedLoadingCacheB.get(key2); // implicit eviction

            verify(cacheLoader, times(2)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(maximumSize), s -> s.isEqualTo(1)),
                                    Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(1), s -> s.isBetween(0L, 1L)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNull();
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            distributedLoadingCacheA.get(key1); // implicit eviction

            verifyNoMoreInteractions(cacheLoader);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(maximumSize), s -> s.isEqualTo(2)),
                                    Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(1), s -> s.isBetween(1L, 3L)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNull();
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            Value loadedValue3 = distributedLoadingCacheB.get(key3); // implicit eviction

            verify(cacheLoader, times(3)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(maximumSize), s -> s.isEqualTo(3)),
                                    Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(extendedMaximumSize), s -> s.isBetween(1L, 4L)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
                        }
                    });

            // create inconsistencies in relation to not stale cache entries but prevent instant replace by cache manager
            getDistributedCaffeine(distributedLoadingCacheA).getCacheManager().manageCleanUp(Duration.ZERO);
            getDistributedCaffeine(distributedLoadingCacheB).getCacheManager().manageCleanUp(Duration.ZERO);
            distributedLoadingCacheA.get(key1); // implicit eviction

            verifyNoMoreInteractions(cacheLoader);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(maximumSize), s -> s.isEqualTo(4)),
                                    Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(extendedMaximumSize), s -> s.isBetween(2L, 6L)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
                        }
                    });

            // create more cache entries (extended by size) than the maximum size allows
            Value loadedValue4 = distributedLoadingCacheA.get(key4); // implicit eviction

            verify(cacheLoader, times(4)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key4)).isEqualTo(loadedValue4);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromMongo(key4, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue4));
                            assertThat(distributedPolicy.getFromMongo(key4, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue4));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(maximumSize), s -> s.isEqualTo(5)),
                                    Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(extendedMaximumSize), s -> s.isBetween(3L, 8L)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(maximumSize);
                            assertThat(distributedLoadingCacheA.getIfPresent(key4)).isEqualTo(loadedValue4);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key4, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key4, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(extendedMaximumSize), s -> s.isEqualTo(0)));
                        }
                    });

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedLoadingCacheA, distributedLoadingCacheB));

            // test caches without special semantics (regarding loading missing values from store)
            DistributedLoadingCache<Key, Value> distributedLoadingCacheWithoutSpecialSemantics = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    b -> b.build(cacheLoader));

            DistributedCache<Key, Value> distributedCacheWithoutSpecialSemantics = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded but not from store"))
                    .when(cacheLoader).load(any(Key.class));

            Value loadedFromStoreValue = requireNonNull(distributedPolicy.getFromMongo(key1, true)).getValue();
            Value loadedButNotFromStoreValue = distributedLoadingCacheWithoutSpecialSemantics.get(key1);
            Value notFoundValue = distributedCacheWithoutSpecialSemantics.getIfPresent(key1);

            verify(cacheLoader, times(5)).load(any(Key.class));

            assertThat(loadedFromStoreValue).isEqualTo(loadedValue1);
            assertThat(loadedButNotFromStoreValue).isNotNull()
                    .satisfies(value -> assertThat(value.getName()).isEqualTo("loaded but not from store"));
            assertThat(notFoundValue).isNull();
        }

        @DisplayName("Test extended persistence by time")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_DistributionMode_extended_persistence_by_time(CacheFactory<Key, Value> cacheFactory) throws Exception {
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheBuilder<Key, Value> cacheBuilder =
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                    .evictionListener(evictionListener)
                                    .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration())))
                            .withExtendedPersistence(FOREVER.getDuration())
                            // just to test the distinction in logic
                            .withExtendedPersistence(Integer.MAX_VALUE);

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
                    b -> b.buildWithExtendedPersistence(cacheLoader));
            DistributedLoadingCache<Key, Value> distributedLoadingCacheB = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    b -> b.buildWithExtendedPersistence(cacheLoader));

            DistributionMode distributionMode = getDistributedCaffeine(distributedLoadingCacheA).getDistributionMode();
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNull();
                            assertThatDataStoreHasCounts(Count.isEmpty());
                        }
                    });

            Value loadedValue2 = distributedLoadingCacheB.get(key2);
            // explicit eviction
            varExpirationA.setExpiresAfter(key1, Duration.ZERO);
            varExpirationB.setExpiresAfter(key1, Duration.ZERO);

            verify(cacheLoader, times(2)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1)),
                                    Count.of(EVICTED_TIME_EXTENDED, f -> f.isEqualTo(1), s -> s.isEqualTo(1)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(loadedValue2);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_EXTENDED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
                        }
                    });

            distributedLoadingCacheA.get(key1);
            // explicit eviction
            varExpirationA.setExpiresAfter(key2, Duration.ZERO);
            varExpirationB.setExpiresAfter(key2, Duration.ZERO);

            verifyNoMoreInteractions(cacheLoader);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(2)),
                                    Count.of(EVICTED_TIME_EXTENDED, f -> f.isEqualTo(1), s -> s.isEqualTo(3)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_EXTENDED, f -> f.isEqualTo(2), s -> s.isEqualTo(0)));
                        }
                    });

            Value loadedValue3 = distributedLoadingCacheB.get(key3);
            // explicit eviction
            varExpirationA.setExpiresAfter(key1, Duration.ZERO);
            varExpirationB.setExpiresAfter(key1, Duration.ZERO);

            verify(cacheLoader, times(3)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(3)),
                                    Count.of(EVICTED_TIME_EXTENDED, f -> f.isEqualTo(2), s -> s.isEqualTo(4)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.getIfPresent(key3)).isEqualTo(loadedValue3);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_EXTENDED, f -> f.isEqualTo(2), s -> s.isEqualTo(1)));
                        }
                    });

            // create inconsistencies in relation to not stale cache entries but prevent instant replace by cache manager
            getDistributedCaffeine(distributedLoadingCacheA).getCacheManager().manageCleanUp(Duration.ZERO);
            getDistributedCaffeine(distributedLoadingCacheB).getCacheManager().manageCleanUp(Duration.ZERO);
            distributedLoadingCacheA.get(key1);
            // explicit eviction
            varExpirationA.setExpiresAfter(key3, Duration.ZERO);
            varExpirationB.setExpiresAfter(key3, Duration.ZERO);

            verifyNoMoreInteractions(cacheLoader);

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(4)),
                                    Count.of(EVICTED_TIME_EXTENDED, f -> f.isEqualTo(2), s -> s.isEqualTo(6)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_EXTENDED, f -> f.isEqualTo(3), s -> s.isEqualTo(1)));
                        }
                    });

            Value loadedValue4 = distributedLoadingCacheA.get(key4);
            // explicit eviction
            varExpirationA.setExpiresAfter(key3, Duration.ZERO);
            varExpirationB.setExpiresAfter(key3, Duration.ZERO);

            verify(cacheLoader, times(4)).load(any(Key.class));

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
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
                    .failFast(() -> speedUpAssertions(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode.equals(POPULATION_AND_INVALIDATION_AND_EVICTION)
                                || distributionMode.equals(POPULATION_AND_INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key4)).isEqualTo(loadedValue4);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyInAnyOrderEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromMongo(key4, false)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue4));
                            assertThat(distributedPolicy.getFromMongo(key4, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue4));
                            assertThatDataStoreHasCounts(
                                    Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(4)),
                                    Count.of(EVICTED_TIME_EXTENDED, f -> f.isEqualTo(2), s -> s.isEqualTo(6)));
                        } else if (distributionMode.equals(INVALIDATION_AND_EVICTION) ||
                                distributionMode.equals(INVALIDATION)) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(loadedValue1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key4)).isEqualTo(loadedValue4);
                            assertThat(distributedPolicy.getFromMongo(key1, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key1, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue1));
                            assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key2, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue2));
                            assertThat(distributedPolicy.getFromMongo(key3, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key3, true)).isNotNull()
                                    .satisfies(entry -> assertThat(requireNonNull(entry).getValue()).isEqualTo(loadedValue3));
                            assertThat(distributedPolicy.getFromMongo(key4, false)).isNull();
                            assertThat(distributedPolicy.getFromMongo(key4, true)).isNull();
                            assertThatDataStoreHasCounts(
                                    Count.of(EVICTED_TIME_EXTENDED, f -> f.isEqualTo(3), s -> s.isEqualTo(1)));
                        }
                    });

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedLoadingCacheA, distributedLoadingCacheB));

            // test caches without special semantics (regarding loading missing values from store)
            DistributedLoadingCache<Key, Value> distributedLoadingCacheWithoutSpecialSemantics = (DistributedLoadingCache<Key, Value>) cacheFactory.create(
                    cacheBuilder,
                    b -> b.build(cacheLoader));

            DistributedCache<Key, Value> distributedCacheWithoutSpecialSemantics = cacheFactory.create(
                    cacheBuilder,
                    Builder::build);

            doAnswer(invocation -> Value.of(invocation.<Key>getArgument(0).getId(), "loaded but not from store"))
                    .when(cacheLoader).load(any(Key.class));

            Value loadedFromStoreValue = requireNonNull(distributedPolicy.getFromMongo(key2, true)).getValue();
            Value loadedButNotFromStoreValue = distributedLoadingCacheWithoutSpecialSemantics.get(key2);
            Value notFoundValue = distributedCacheWithoutSpecialSemantics.getIfPresent(key2);

            verify(cacheLoader, times(5)).load(any(Key.class));

            assertThat(loadedFromStoreValue).isEqualTo(loadedValue2);
            assertThat(loadedButNotFromStoreValue).isNotNull()
                    .satisfies(value -> assertThat(value.getName()).isEqualTo("loaded but not from store"));
            assertThat(notFoundValue).isNull();
        }

        @DisplayName("Test synchronization")
        @Test
        void test_DistributedCaffeine_synchronization() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    Builder::build);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            distributedCache.put(key1, value1);
            // create stale cache entry
            distributedCache.put(key1, value1);

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCache))
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1))));

            // create inconsistencies in relation to not stale cache entries
            distributedCache.distributedPolicy().getMongoCollection().updateMany(Filters.empty(),
                    Updates.combine(
                            Updates.set(STATUS.toString(), CACHED.toString()),
                            Updates.set(EXPIRES.toString(), null)));

            assertThatDataStoreHasCounts(
                    Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1)));

            // corrects inconsistencies in relation to not stale cache entries implicitly
            DistributedCache<Key, Value> syncedDistributedCache = createCache(
                    CacheBuilder.identity(),
                    Builder::build);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(distributedCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedCache.asMap());
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1)));
                    });

            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            distributedCache.put(key2, value2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedCache.asMap());
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(1)));
                    });

            syncedDistributedCache.distributedPolicy().stopSynchronization();

            Value overwrittenValue = Value.of(1, "overwritten");
            Key key3 = Key.of(3);
            Value value3 = Value.of(3);
            syncedDistributedCache.put(key1, overwrittenValue);
            syncedDistributedCache.invalidate(key2);
            syncedDistributedCache.put(key3, value3);

            await("no synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCache.asMap().entrySet())
                                .has(matching(not(hasItems(syncedDistributedCache.asMap().entrySet()))));
                        assertThat(syncedDistributedCache.asMap().entrySet())
                                .has(matching(not(hasItems(distributedCache.asMap().entrySet()))));
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(1)));
                    });

            syncedDistributedCache.distributedPolicy().startSynchronization();

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedCache.asMap());
                        assertThatDataStoreHasCounts(
                                Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(1)));
                    });
        }

        @DisplayName("Test same value instance handling and invalidation of already absent value")
        @Test
        void test_DistributedCaffeine_same_value_and_already_absent() {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    Builder::build);
            DistributedCache<Key, Value> syncedDistributedCache = createCache(
                    CacheBuilder.identity(),
                    Builder::build);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);

            distributedCache.put(key1, value1);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCache, syncedDistributedCache))
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
                                Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(0)));
                    });

            distributedCache.put(key1, value1); // same value instance but should create stale entry
            distributedCache.invalidate(Key.of(0, "not present"));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> speedUpAssertions(distributedCache))
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1))));
        }

        @DisplayName("Test ChangeStreamWatcher")
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
        }

        @DisplayName("Test MaintenanceWorker")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_MaintenanceWorker_fails_and_retries() {
            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);

            DistributedCache<Key, Value> distributedCache = createCache(
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                    .maximumSize(1))
                            .withExtendedPersistence(1),
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
                            Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(1))));

            // create inconsistencies in relation to not stale cache entries but prevent instant replace by cache manager
            getDistributedCaffeine(distributedCache).getCacheManager().manageCleanUp(Duration.ZERO);
            distributedCache.put(key2, value2);

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(2)),
                            Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(1), s -> s.isEqualTo(0))));

            // create more cache entries (extended by size) than the maximum size allows
            distributedCache.put(key1, value1);

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(3)),
                            Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(1), s -> s.isEqualTo(1))));

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
                            Count.of(CACHED, f -> f.isEqualTo(2), s -> s.isEqualTo(3)),
                            Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(1), s -> s.isEqualTo(1))));

            // create inconsistencies in relation to not stale cache entries but prevent instant replace by cache manager
            getDistributedCaffeine(distributedCache).getCacheManager().manageCleanUp(Duration.ZERO);
            distributedCache.put(key2, value2);

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, f -> f.isEqualTo(3), s -> s.isEqualTo(3)),
                            Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(2), s -> s.isEqualTo(1))));

            // create more cache entries (extended by size) than the maximum size allows
            distributedCache.put(key1, value1);

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, f -> f.isEqualTo(4), s -> s.isEqualTo(3)),
                            Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(3), s -> s.isEqualTo(1))));

            // fix failure
            doCallRealMethod().when(toBeMarkedAsStale).stream();

            await("maintenance")
                    .atMost(WAITING_DURATION.plusSeconds(10)) // retry delay is increased on failure
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            Count.of(CACHED, f -> f.isEqualTo(1), s -> s.isEqualTo(6)),
                            Count.of(EVICTED_SIZE_EXTENDED, f -> f.isEqualTo(1), s -> s.isEqualTo(3))));
        }

        @DisplayName("Test MongoRepository")
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
        }

        @DisplayName("Stress test synchronization from data store")
        @Test
        void stress_test_DistributedCaffeine_synchronization_from_data_store() throws Exception {
            int maximumSize = runsOnGitHub(10_000, 100_000);
            int extendedMaximumSize = (int) (maximumSize * .9);
            int numberOfOperations = 10_000;

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> removalListener = mock(RemovalListener.class);
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                public Value load(Key key) {
                    return nextInt(2) == 1
                            ? Value.of(key.getId(), nameWithMillisAndPrefixes("load"))
                            : null;
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
                        b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                        .executor(executorService)
                                        .removalListener(removalListener)
                                        .evictionListener(evictionListener)
                                        .maximumSize(maximumSize)
                                        .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration())))
                                .withExtendedPersistence(extendedMaximumSize),
                        b -> b.buildWithExtendedPersistence(cacheLoader));
                return (DistributedLoadingCache<Key, Value>) cache;
            };

            DistributedLoadingCache<Key, Value> distributedLoadingCache = cacheSupplier.get();

            Map<Key, Value> keyValueMap = IntStream.rangeClosed(1, maximumSize)
                    .boxed()
                    .collect(toMap(Key::of, i -> Value.of(i, nameWithMillisAndPrefixes("init"))));
            distributedLoadingCache.putAll(keyValueMap);

            assertThatDataStoreHasCounts(
                    CountGrouped.of(CACHED_GROUP, f -> f.isEqualTo(keyValueMap.size()), s -> s.isEqualTo(0)));

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
                    .untilAtomic(loopCounter, lessThanOrEqualTo(0));

            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = cacheSupplier.get();

            loopCondition.set(false);

            await("synchronization between cache instances")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast(() -> speedUpAssertions(distributedLoadingCache, syncedDistributedLoadingCache))
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedLoadingCache.asMap());
                        assertThatDataStoreHasCounts(
                                CountGrouped.of(CACHED_GROUP, f -> f.isEqualTo(distributedLoadingCache.estimatedSize()), s -> s.isGreaterThanOrEqualTo(0)),
                                CountGrouped.of(EVICTED_EXTENDED_GROUP, f -> f.isBetween(1L, (long) extendedMaximumSize), s -> s.isGreaterThanOrEqualTo(0)));
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
                    .untilAtomic(loopCounter, lessThanOrEqualTo(0));

            syncedDistributedLoadingCache.distributedPolicy().startSynchronization();

            loopCondition.set(false);

            await("synchronization between cache instances")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast(() -> speedUpAssertions(distributedLoadingCache, syncedDistributedLoadingCache))
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedLoadingCache.asMap());
                        assertThatDataStoreHasCounts(
                                CountGrouped.of(CACHED_GROUP, f -> f.isEqualTo(distributedLoadingCache.estimatedSize()), s -> s.isGreaterThanOrEqualTo(0)),
                                CountGrouped.of(EVICTED_EXTENDED_GROUP, f -> f.isBetween(1L, (long) extendedMaximumSize), s -> s.isGreaterThanOrEqualTo(0)));
                    });

            await("cache manager maintenance")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedLoadingCache, syncedDistributedLoadingCache));

            verify(removalListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPLICIT));
            verify(removalListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.REPLACED));
            verify(removalListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
            verify(removalListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
            verifyNoMoreInteractions(removalListener);

            verify(evictionListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
            verify(evictionListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
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

        @DisplayName("Stress test with multiple threads")
        @Test
        void stress_test_DistributedCaffeine_multiple_threads() throws Exception {
            int maximumSize = 100;
            int extendedMaximumSize = (int) (maximumSize * .5);
            Duration refreshAfterWrite = Duration.ofSeconds(10);
            int numberOfOperations = 10_000;
            int levelOfParallelism = runsOnGitHub(5, 10);

            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> removalListener = mock(RemovalListener.class);
            @SuppressWarnings("unchecked")
            RemovalListener<Key, Value> evictionListener = mock(RemovalListener.class);

            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<>() {
                @Override
                public Value load(Key key) {
                    return nextInt(2) == 1
                            ? Value.of(key.getId(), nameWithMillisAndPrefixes("load"))
                            : null;
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
                        b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                        .removalListener(removalListener)
                                        .evictionListener(evictionListener)
                                        .executor(executorService)
                                        .maximumSize(maximumSize)
                                        .expireAfter(Expiry.creating((key, value) -> FOREVER.getDuration()))
                                        .refreshAfterWrite(refreshAfterWrite))
                                .withExtendedPersistence(extendedMaximumSize),
                        b -> b.buildWithExtendedPersistence(cacheLoader));
                return (DistributedLoadingCache<Key, Value>) cache;
            };

            // first cache only watches passively
            DistributedLoadingCache<Key, Value> firstDistributedLoadingCache = cacheSupplier.get();
            List<DistributedLoadingCache<Key, Value>> distributedLoadingCaches =
                    new ArrayList<>(List.of(firstDistributedLoadingCache));
            List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

            IntStream.rangeClosed(1, levelOfParallelism).forEach(cacheIndex ->
                    completableFutures.add(CompletableFuture.runAsync(() -> {
                        await(Duration.ofMillis(1_000).multipliedBy(Math.min(10, cacheIndex)));
                        DistributedLoadingCache<Key, Value> distributedLoadingCache = cacheSupplier.get();
                        distributedLoadingCaches.add(distributedLoadingCache);
                        IntStream.rangeClosed(1, numberOfOperations).forEach(operationIndex -> {
                            executeRandomOperation(distributedLoadingCache, maximumSize);
                            if (operationIndex == numberOfOperations / 2) {
                                distributedLoadingCache.distributedPolicy().stopSynchronization();
                                await(Duration.ofMillis(1_000).multipliedBy(Math.min(10, cacheIndex)));
                                distributedLoadingCache.distributedPolicy().startSynchronization();
                            }
                        });
                    }, executorService)));

            CompletableFuture.allOf(completableFutures.toArray(CompletableFuture[]::new)).join();

            await("synchronization between cache instances")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast(() -> speedUpAssertions(distributedLoadingCaches))
                    .untilAsserted(() -> {
                        IntStream.range(0, distributedLoadingCaches.size() - 1).forEach(i ->
                                assertThat(distributedLoadingCaches.get(i).asMap())
                                        .describedAs(() -> format("%s (%s) vs. %s (%s)",
                                                i, distributedLoadingCaches.get(i),
                                                i + 1, distributedLoadingCaches.get(i + 1)))
                                        .containsExactlyInAnyOrderEntriesOf(distributedLoadingCaches.get(i + 1).asMap()));
                        assertThatDataStoreHasCounts(
                                CountGrouped.of(CACHED_GROUP, f -> f.isEqualTo(firstDistributedLoadingCache.estimatedSize()), s -> s.isGreaterThanOrEqualTo(0)),
                                CountGrouped.of(EVICTED_EXTENDED_GROUP, f -> f.isBetween(1L, (long) extendedMaximumSize), s -> s.isGreaterThanOrEqualTo(0)));
                    });

            await("cache manager maintenance")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedLoadingCaches));

            distributedLoadingCaches.forEach(distributedCache ->
                    completableFutures.add(CompletableFuture
                            .runAsync(distributedCache::invalidateAll, executorService)));

            CompletableFuture.allOf(completableFutures.toArray(CompletableFuture[]::new)).join();

            await("synchronization between cache instances")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast(() -> speedUpAssertions(distributedLoadingCaches))
                    .untilAsserted(() -> {
                        assertThatCollection(distributedLoadingCaches)
                                .allMatch(distributedCache -> distributedCache.estimatedSize() == 0);
                        assertThatDataStoreHasCounts(
                                CountGrouped.of(CACHED_GROUP, f -> f.isEqualTo(firstDistributedLoadingCache.estimatedSize()), s -> s.isGreaterThanOrEqualTo(0)),
                                CountGrouped.of(EVICTED_EXTENDED_GROUP, f -> f.isBetween(1L, (long) extendedMaximumSize), s -> s.isGreaterThanOrEqualTo(0)));
                    });

            await("cache manager maintenance")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .untilAsserted(() -> assertThatMaintenanceIsDone(distributedLoadingCaches));

            verify(removalListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPLICIT));
            verify(removalListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.REPLACED));
            verify(removalListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
            verify(removalListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
            verifyNoMoreInteractions(removalListener);

            verify(evictionListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.SIZE));
            verify(evictionListener, atLeastOnce()).onRemoval(any(Key.class), any(Value.class), eq(RemovalCause.EXPIRED));
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
        MongoDatabase mongoDatabase;

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
            this.mongoDatabase = mongoClient.getDatabase(DATABASE_NAME);
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
                            Builder::withForySerializer),
                    new DistributedCaffeineConfiguration<>(
                            "with Fory Serializer (Class)",
                            b -> b.withCustomKeySerializer(new ForySerializer<>(Key.class))
                                    .withCustomValueSerializer(new ForySerializer<>(Value.class))),
                    new DistributedCaffeineConfiguration<>(
                            "with Java Object Serializer",
                            Builder::withJavaObjectSerializer),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (BSON, Class)",
                            b -> b.withJsonSerializer(Key.class, Value.class, true)),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (JSON, Class)",
                            b -> b.withJsonSerializer(new ObjectMapper(), Key.class, Value.class, false)),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (BSON, TypeReference)",
                            b -> b.withJsonSerializer(
                                    new TypeReference<>() {
                                    },
                                    new TypeReference<>() {
                                    },
                                    true)),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (JSON, TypeReference)",
                            b -> b.withJsonSerializer(new ObjectMapper(),
                                    new TypeReference<>() {
                                    },
                                    new TypeReference<>() {
                                    },
                                    false))
            );
        }

        Stream<DistributedCaffeineConfiguration<Key, Value>> createDistributedCaffeineConfigurationsWithDifferentDistributionModes() {
            return Stream.of(DistributionMode.values())
                    .map(distributionMode -> new DistributedCaffeineConfiguration<>(
                            format("with %s.%s", distributionMode.getClass().getSimpleName(), distributionMode.name()),
                            b -> b.withDistributionMode(distributionMode)));
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
            String collectionName = format("%s_%05d", testInfo.getTestMethod().orElseThrow().getName(), testCounter.get());
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);
            return createCache(mongoCollection, cacheBuilder, cacheConstructor);
        }

        @SafeVarargs
        final <K, V> void speedUpAssertions(Cache<K, V>... caches) {
            speedUpAssertions(Set.of(caches));
        }

        <K, V> void speedUpAssertions(Collection<? extends Cache<K, V>> caches) {
            // speed up using parallel stream
            caches.stream().parallel().forEach(cache -> {
                cache.cleanUp();
                if (cache instanceof DistributedCache<K, V> distributedCache) {
                    DistributedCaffeine<K, V> distributedCaffeine = getDistributedCaffeine(distributedCache);
                    InternalMaintenanceWorker<K, V> maintenanceWorker = distributedCaffeine.getMaintenanceWorker();
                    Collection<ObjectId> toBeMarkedAsStale = readFieldValue(maintenanceWorker, InternalMaintenanceWorker.class,
                            "toBeMarkedAsStale", Collection.class);
                    Set<Integer> maybeToBeMarkedAsStaleForExtendedPersistence = readFieldValue(maintenanceWorker, InternalMaintenanceWorker.class,
                            "maybeToBeMarkedAsStaleForExtendedPersistence", Set.class);
                    AtomicBoolean checkToBeMarkedAsStaleForExtendedPersistenceBySize = readFieldValue(maintenanceWorker, InternalMaintenanceWorker.class,
                            "checkToBeMarkedAsStaleForExtendedPersistenceBySize", AtomicBoolean.class);
                    while (!toBeMarkedAsStale.isEmpty()
                            || !maybeToBeMarkedAsStaleForExtendedPersistence.isEmpty()
                            || checkToBeMarkedAsStaleForExtendedPersistenceBySize.get()) {
                        invokeMethod(maintenanceWorker, InternalMaintenanceWorker.class,
                                "processMaintenance", List.of(), List.of());
                    }
                }
            });
        }

        void assertThatDataStoreHasCounts(Count... counts) {
            MongoCollection<Document> mongoCollection = distributedCacheInstances.stream()
                    .findFirst()
                    .orElseThrow()
                    .distributedPolicy()
                    .getMongoCollection();
            Map<Status, Count> statusToCount = Stream.of(counts)
                    .collect(toMap(Count::status, Function.identity()));
            String fresh = "fresh";
            String stale = "stale";
            Stream.of(Status.values())
                    .map(status -> statusToCount.getOrDefault(status,
                            Count.of(status, f -> f.isEqualTo(0), s -> s.isEqualTo(0))))
                    .forEach(count -> List.of(fresh, stale).forEach(label -> {
                        boolean isStale = label.equals(stale);
                        Bson filter = Filters.and(
                                Filters.eq(STATUS.toString(), count.status().toString()),
                                Filters.eq(STALE.toString(), isStale));
                        (isStale ? count.stale() : count.fresh())
                                .apply(assertThat(mongoCollection.countDocuments(filter))
                                        .describedAs(format("%n --> count '%s' for '%s'", label, count.status())));
                    }));
        }

        void assertThatDataStoreHasCounts(CountGrouped... countsGrouped) {
            MongoCollection<Document> mongoCollection = distributedCacheInstances.stream()
                    .findFirst()
                    .orElseThrow()
                    .distributedPolicy()
                    .getMongoCollection();
            String fresh = "fresh";
            String stale = "stale";
            Stream.of(countsGrouped).forEach(countGrouped -> List.of(fresh, stale).forEach(label -> {
                boolean isStale = label.equals(stale);
                Bson filter = Filters.and(
                        Filters.in(STATUS.toString(), Stream.of(countGrouped.statuses())
                                .map(Objects::toString)
                                .collect(toSet())),
                        Filters.eq(STALE.toString(), isStale));
                (isStale ? countGrouped.stale() : countGrouped.fresh())
                        .apply(assertThat(mongoCollection.countDocuments(filter))
                                .describedAs(format("%n --> count '%s' for '%s'", label,
                                        Arrays.toString(countGrouped.statuses()))));
            }));
        }

        @SafeVarargs
        final <K, V> void assertThatMaintenanceIsDone(DistributedCache<K, V>... distributedCaches) {
            assertThatMaintenanceIsDone(Set.of(distributedCaches));
        }

        <K, V> void assertThatMaintenanceIsDone(Collection<? extends DistributedCache<K, V>> distributedCaches) {
            distributedCaches.stream().parallel()
                    .forEach(distributedCache -> {
                        DistributedCaffeine<K, V> distributedCaffeine = getDistributedCaffeine(distributedCache);
                        InternalCacheManager<K, V> cacheManager = distributedCaffeine.getCacheManager();
                        InternalMaintenanceWorker<K, V> maintenanceWorker = distributedCaffeine.getMaintenanceWorker();
                        Map<K, InternalCacheDocument<K, V>> latest = readFieldValue(cacheManager, InternalCacheManager.class,
                                "latest", Map.class);
                        Map<K, InternalCacheDocument<K, V>> buffer = readFieldValue(cacheManager, InternalCacheManager.class,
                                "buffer", Map.class);
                        Map<K, Set<InternalCacheDocument<K, V>>> balance = readFieldValue(cacheManager, InternalCacheManager.class,
                                "balance", Map.class);
                        Set<ObjectId> toBeMarkedAsStale = readFieldValue(maintenanceWorker, InternalMaintenanceWorker.class,
                                "toBeMarkedAsStale", Set.class);
                        Set<Integer> maybeToBeMarkedAsStaleForExtendedPersistence = readFieldValue(maintenanceWorker, InternalMaintenanceWorker.class,
                                "maybeToBeMarkedAsStaleForExtendedPersistence", Set.class);
                        AtomicBoolean checkToBeMarkedAsStaleForExtendedPersistenceBySize = readFieldValue(maintenanceWorker, InternalMaintenanceWorker.class,
                                "checkToBeMarkedAsStaleForExtendedPersistenceBySize", AtomicBoolean.class);
                        // speed up assertions
                        speedUpAssertions(distributedCache);
                        distributedCaffeine.getCacheManager().manageCleanUp(Duration.ZERO);
                        // assertions
                        if (distributedCaffeine.getDistributionMode().isPopulationConsidered()) {
                            // workaround as comparing by size does not seem to work reliably with time-based evictions
                            boolean hasEvictionPolicyByTime = readFieldValue(cacheManager, InternalCacheManager.class,
                                    "hasEvictionPolicyByTime", Boolean.class);
                            if (hasEvictionPolicyByTime) {
                                assertThat(latest.entrySet().stream()
                                        .collect(toMap(Entry::getKey, entry -> entry.getValue().getValue())))
                                        .containsExactlyInAnyOrderEntriesOf(distributedCache.asMap());
                            } else {
                                assertThat(latest).hasSize((int) distributedCache.estimatedSize());
                            }
                        } else {
                            assertThat(latest).isEmpty();
                        }
                        assertThat(buffer).isEmpty();
                        assertThat(balance).isEmpty();
                        assertThat(toBeMarkedAsStale).isEmpty();
                        assertThat(maybeToBeMarkedAsStaleForExtendedPersistence).isEmpty();
                        assertThat(checkToBeMarkedAsStaleForExtendedPersistenceBySize).isFalse();
                    });
        }

        <K, V> DistributedCaffeine<K, V> getDistributedCaffeine(DistributedCache<K, V> distributedCache) {
            return ((InternalDistributedCache<K, V>) distributedCache).distributedCaffeine;
        }

        void executeRandomOperation(DistributedCache<Key, Value> distributedCache, int cacheSize) {
            List<Runnable> operations = listOperations(distributedCache, cacheSize);
            operations.get(nextInt(operations.size())).run();
        }

        List<Runnable> listOperations(DistributedCache<Key, Value> distributedCache, int cacheSize) {
            List<Runnable> operations = new ArrayList<>();
            operations.add(() -> {
                int addId = nextInt(cacheSize, 2 * cacheSize);
                distributedCache.put(Key.of(addId),
                        Value.of(addId, nameWithMillisAndPrefixes("add")));
            });
            operations.add(() -> {
                int addId1 = nextInt(cacheSize, cacheSize + cacheSize / 2);
                int addId2 = nextInt(cacheSize + cacheSize / 2, 2 * cacheSize);
                distributedCache.putAll(Map.of(
                        Key.of(addId1),
                        Value.of(addId1, nameWithMillisAndPrefixes("add")),
                        Key.of(addId2),
                        Value.of(addId2, nameWithMillisAndPrefixes("add"))));
            });
            operations.add(() -> {
                int updateId = nextInt(cacheSize);
                distributedCache.put(Key.of(updateId),
                        Value.of(updateId, nameWithMillisAndPrefixes("update")));
            });
            operations.add(() -> {
                int updateId1 = nextInt(0, cacheSize / 2);
                int updateId2 = nextInt(cacheSize / 2, cacheSize);
                distributedCache.putAll(Map.of(
                        Key.of(updateId1),
                        Value.of(updateId1, nameWithMillisAndPrefixes("update")),
                        Key.of(updateId2),
                        Value.of(updateId2, nameWithMillisAndPrefixes("update"))));
            });
            operations.add(() -> {
                int removeId = nextInt(cacheSize);
                distributedCache.invalidate(Key.of(removeId));
            });
            operations.add(() -> {
                int removeId1 = nextInt(0, cacheSize / 2);
                int removeId2 = nextInt(cacheSize / 2, cacheSize);
                distributedCache.invalidateAll(Set.of(
                        Key.of(removeId1),
                        Key.of(removeId2)));
            });
            operations.add(() -> {
                int expireId = nextInt(cacheSize);
                distributedCache.policy().expireVariably().orElseThrow().setExpiresAfter(Key.of(expireId), Duration.ZERO);
            });
            if (distributedCache instanceof DistributedLoadingCache<Key, Value> distributedLoadingCache) {
                operations.add(() -> {
                    int loadId = nextInt(cacheSize, 2 * cacheSize);
                    distributedLoadingCache.get(Key.of(loadId));
                });
                operations.add(() -> {
                    int loadId1 = nextInt(cacheSize, cacheSize + cacheSize / 2);
                    int loadId2 = nextInt(cacheSize + cacheSize / 2, 2 * cacheSize);
                    distributedLoadingCache.getAll(Set.of(
                            Key.of(loadId1),
                            Key.of(loadId2)));
                });
                operations.add(() -> {
                    int refreshId = nextInt(0, cacheSize);
                    distributedLoadingCache.refresh(Key.of(refreshId));
                });
                operations.add(() -> {
                    int refreshId1 = nextInt(0, cacheSize / 2);
                    int refreshId2 = nextInt(cacheSize / 2, cacheSize);
                    distributedLoadingCache.refreshAll(Set.of(
                            Key.of(refreshId1),
                            Key.of(refreshId2)));
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
            String millis = String.valueOf(System.currentTimeMillis());
            return prefix.isBlank()
                    ? millis
                    : String.join(delimiter, prefix, millis);
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
        <K, V> void printMongoCollection(Bson... filters) {
            AtomicInteger counter = new AtomicInteger(0);
            distributedCacheInstances.stream()
                    .findFirst()
                    .map(this::getDistributedCaffeine)
                    .map(DistributedCaffeine::getMongoRepository)
                    .orElseThrow()
                    .consumeCacheDocumentsGroupedByKeyInReverseOrder(filters.length == 0
                                    ? Filters.empty()
                                    : Filters.and(filters),
                            stream -> stream
                                    .flatMap(Set::stream)
                                    .sorted()
                                    .forEach(cacheDocument ->
                                            System.out.printf("%05d %s%n", counter.incrementAndGet(), cacheDocument)));
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

        record Count(Status status, Function<AbstractLongAssert<?>, AbstractLongAssert<?>> fresh,
                     Function<AbstractLongAssert<?>, AbstractLongAssert<?>> stale) {

            static Count of(Status status,
                            Function<AbstractLongAssert<?>, AbstractLongAssert<?>> fresh,
                            Function<AbstractLongAssert<?>, AbstractLongAssert<?>> stale) {
                return new Count(status, fresh, stale);
            }

            static Count[] isEmpty() {
                return new Count[]{};
            }
        }

        record CountGrouped(Status[] statuses, Function<AbstractLongAssert<?>, AbstractLongAssert<?>> fresh,
                            Function<AbstractLongAssert<?>, AbstractLongAssert<?>> stale) {

            static CountGrouped of(Status[] statuses,
                                   Function<AbstractLongAssert<?>, AbstractLongAssert<?>> fresh,
                                   Function<AbstractLongAssert<?>, AbstractLongAssert<?>> stale) {
                return new CountGrouped(statuses, fresh, stale);
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
