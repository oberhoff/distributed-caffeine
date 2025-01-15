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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Policy.VarExpiration;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.github.oberhoff.distributedcaffeine.DistributedPolicy.CacheEntry;
import io.github.oberhoff.distributedcaffeine.serializer.FurySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.SerializerException;
import org.assertj.core.util.introspection.CaseFormatUtils;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.platform.commons.util.ReflectionUtils.HierarchyTraversalMode;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.images.AbstractImagePullPolicy;
import org.testcontainers.images.ImageData;
import org.testcontainers.utility.DockerImageName;

import java.lang.System.Logger;
import java.lang.reflect.Field;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributedCaffeineTest.AbstractDistributedCaffeineTestBase.LATEST_ONLY;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.CACHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.EVICTED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.EXPIRES;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.HASH;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.ID;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.ORPHANED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.STATUS;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.TOUCHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.VALUE;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCollection;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_WITH_NAMES_PLACEHOLDER;

@DisplayName("Distributed Caffeine Test Suite")
final class DistributedCaffeineTest {

    @DisplayName("MongoDB 4.0.0")
    @Nested
    @Tag("mongo:4.0.0")
    @DisabledIfSystemProperty(named = LATEST_ONLY, matches = "true")
    final class Mongo_4_0_0 extends AbstractDistributedCaffeineTest {
    }

    @DisplayName("MongoDB 4.latest")
    @Nested
    @Tag("mongo:4")
    final class Mongo_4_latest extends AbstractDistributedCaffeineTest {
    }

    @DisplayName("MongoDB 5.0.0")
    @Nested
    @Tag("mongo:5.0.0")
    @DisabledIfSystemProperty(named = LATEST_ONLY, matches = "true")
    final class Mongo_5_0_0 extends AbstractDistributedCaffeineTest {
    }

    @DisplayName("MongoDB 5.latest")
    @Nested
    @Tag("mongo:5")
    final class Mongo_5_latest extends AbstractDistributedCaffeineTest {
    }

    @DisplayName("MongoDB 6.0.1")
    @Nested
    @Tag("mongo:6.0.1") // mongo:6.0.0 is not available
    @DisabledIfSystemProperty(named = LATEST_ONLY, matches = "true")
    final class Mongo_6_0_1 extends AbstractDistributedCaffeineTest {
    }

    @DisplayName("MongoDB 6.latest")
    @Nested
    @Tag("mongo:6")
    final class Mongo_6_latest extends AbstractDistributedCaffeineTest {
    }

    @DisplayName("MongoDB 7.0.0")
    @Nested
    @Tag("mongo:7.0.0")
    @DisabledIfSystemProperty(named = LATEST_ONLY, matches = "true")
    final class Mongo_7_0_0 extends AbstractDistributedCaffeineTest {
    }

    @DisplayName("MongoDB 7.latest")
    @Nested
    @Tag("mongo:7")
    final class Mongo_7_latest extends AbstractDistributedCaffeineTest {
    }

    @DisplayName("MongoDB 8.0.0")
    @Nested
    @Tag("mongo:8.0.0")
    @DisabledIfSystemProperty(named = LATEST_ONLY, matches = "true")
    final class Mongo_8_0_0 extends AbstractDistributedCaffeineTest {
    }

    @DisplayName("MongoDB 8.latest")
    @Nested
    @Tag("mongo:8")
    final class Mongo_8_latest extends AbstractDistributedCaffeineTest {
    }

    @TestMethodOrder(MethodName.class)
    abstract static class AbstractDistributedCaffeineTest extends AbstractDistributedCaffeineTestBase {

        @DisplayName("Test put() and getIfPresent()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentSerializers")
        void test_DistributedCache_put_getIfPresent(CacheBiFunction<Key, Value> cacheBiFunction) {
            DistributedCache<Key, Value> distributedCache = cacheBiFunction.apply(null, null);
            DistributedCache<Key, Value> syncedDistributedCache = cacheBiFunction.apply(null, null);

            assertThat(distributedCache.estimatedSize()).isZero();
            assertThat(syncedDistributedCache.estimatedSize()).isZero();
            assertThat(countMongoStatus(distributedCache)).isZero();

            Key key = Key.of(1);
            Value value = Value.of(1);
            distributedCache.put(key, value);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(distributedCache.getIfPresent(key)).isEqualTo(value);
                        assertThat(syncedDistributedCache.getIfPresent(key)).isEqualTo(value);
                        assertThat(distributedCache.getIfPresent(Key.of(0, "not present"))).isNull();
                        assertThat(syncedDistributedCache.getIfPresent(Key.of(0, "not present"))).isNull();
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(1);
                    });
        }

        @DisplayName("Test putAll() and getAllPresent()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentSerializers")
        void test_DistributedCache_putAll_getAllPresent(CacheBiFunction<Key, Value> cacheBiFunction) {
            DistributedCache<Key, Value> distributedCache = cacheBiFunction.apply(null, null);
            DistributedCache<Key, Value> syncedDistributedCache = cacheBiFunction.apply(null, null);

            assertThat(distributedCache.estimatedSize()).isZero();
            assertThat(syncedDistributedCache.estimatedSize()).isZero();
            assertThat(countMongoStatus(distributedCache)).isZero();

            Map<Key, Value> keyValueMap = Map.of(
                    Key.of(1), Value.of(1),
                    Key.of(2), Value.of(2));
            distributedCache.putAll(keyValueMap);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCache.getAllPresent(keyValueMap.keySet())).containsAllEntriesOf(keyValueMap);
                        assertThat(syncedDistributedCache.getAllPresent(keyValueMap.keySet())).containsAllEntriesOf(keyValueMap);
                        assertThat(distributedCache.getAllPresent(Set.of(Key.of(0, "not present")))).isEmpty();
                        assertThat(syncedDistributedCache.getAllPresent(Set.of(Key.of(0, "not present")))).isEmpty();
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(2);
                    });
        }

        @DisplayName("Test get() and getAll()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentSerializers")
        void test_DistributedCache_get_getAll(CacheBiFunction<Key, Value> cacheBiFunction) {
            DistributedCache<Key, Value> distributedCache = cacheBiFunction.apply(null, null);
            DistributedCache<Key, Value> syncedDistributedCache = cacheBiFunction.apply(null, null);

            assertThat(distributedCache.estimatedSize()).isZero();
            assertThat(syncedDistributedCache.estimatedSize()).isZero();
            assertThat(countMongoStatus(distributedCache)).isZero();

            Key key = Key.of(1);
            Value value = Value.of(1);
            distributedCache.get(key, mappedKey -> value);

            Map<Key, Value> keyValueMap = Map.of(
                    Key.of(2), Value.of(2),
                    Key.of(3), Value.of(3));
            distributedCache.getAll(keyValueMap.keySet(), mappedKeys -> keyValueMap);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(3);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(3);
                        assertThat(distributedCache.getIfPresent(key)).isEqualTo(value);
                        assertThat(syncedDistributedCache.getIfPresent(key)).isEqualTo(value);
                        assertThat(distributedCache.getAllPresent(keyValueMap.keySet())).containsAllEntriesOf(keyValueMap);
                        assertThat(syncedDistributedCache.getAllPresent(keyValueMap.keySet())).containsAllEntriesOf(keyValueMap);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(3);
                    });
        }

        @DisplayName("Test invalidate() and invalidateAll()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentSerializers")
        void test_DistributedCache_invalidate_invalidateAll(CacheBiFunction<Key, Value> cacheBiFunction) {
            DistributedCache<Key, Value> distributedCache = cacheBiFunction.apply(null, null);
            DistributedCache<Key, Value> syncedDistributedCache = cacheBiFunction.apply(null, null);

            assertThat(distributedCache.estimatedSize()).isZero();
            assertThat(syncedDistributedCache.estimatedSize()).isZero();
            assertThat(countMongoStatus(distributedCache)).isZero();

            Key key = Key.of(1);
            Value value = Value.of(1);
            distributedCache.put(key, value);

            Map<Key, Value> keyValueMap1 = Map.of(
                    Key.of(2), Value.of(2),
                    Key.of(3), Value.of(3));
            distributedCache.putAll(keyValueMap1);

            Map<Key, Value> keyValueMap2 = Map.of(
                    Key.of(4), Value.of(4),
                    Key.of(5), Value.of(5));
            distributedCache.putAll(keyValueMap2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(5);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(5);
                        assertThat(distributedCache.getIfPresent(key)).isEqualTo(value);
                        assertThat(syncedDistributedCache.getIfPresent(key)).isEqualTo(value);
                        assertThat(distributedCache.getAllPresent(keyValueMap1.keySet())).containsAllEntriesOf(keyValueMap1);
                        assertThat(syncedDistributedCache.getAllPresent(keyValueMap1.keySet())).containsAllEntriesOf(keyValueMap1);
                        assertThat(distributedCache.getAllPresent(keyValueMap2.keySet())).containsAllEntriesOf(keyValueMap2);
                        assertThat(syncedDistributedCache.getAllPresent(keyValueMap2.keySet())).containsAllEntriesOf(keyValueMap2);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(5);
                    });

            distributedCache.invalidate(key);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(4);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(4);
                        assertThat(distributedCache.getAllPresent(keyValueMap1.keySet())).containsAllEntriesOf(keyValueMap1);
                        assertThat(syncedDistributedCache.getAllPresent(keyValueMap1.keySet())).containsAllEntriesOf(keyValueMap1);
                        assertThat(distributedCache.getAllPresent(keyValueMap2.keySet())).containsAllEntriesOf(keyValueMap2);
                        assertThat(syncedDistributedCache.getAllPresent(keyValueMap2.keySet())).containsAllEntriesOf(keyValueMap2);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(4);
                    });

            distributedCache.invalidateAll(keyValueMap1.keySet());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCache.getAllPresent(keyValueMap2.keySet())).containsAllEntriesOf(keyValueMap2);
                        assertThat(syncedDistributedCache.getAllPresent(keyValueMap2.keySet())).containsAllEntriesOf(keyValueMap2);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(2);
                    });

            distributedCache.invalidateAll();

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isZero();
                        assertThat(syncedDistributedCache.estimatedSize()).isZero();
                        assertThat(countMongoStatus(distributedCache, CACHED)).isZero();
                    });
        }

        @DisplayName("Test get() and getAll()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentSerializers")
        void test_DistributedLoadingCache_get_getAll(CacheBiFunction<Key, Value> cacheBiFunction) {
            CacheLoader<Key, Value> cacheLoader = key -> Value.of(key.getId());

            DistributedLoadingCache<Key, Value> distributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheBiFunction.apply(null, cacheLoader);
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheBiFunction.apply(null, cacheLoader);

            assertThat(distributedLoadingCache.estimatedSize()).isZero();
            assertThat(syncedDistributedLoadingCache.estimatedSize()).isZero();
            assertThat(countMongoStatus(distributedLoadingCache)).isZero();

            Key key = Key.of(1);
            Value loadedValue = Value.of(1);
            distributedLoadingCache.get(key);

            Map<Key, Value> keyValueMap = Map.of(
                    Key.of(2), Value.of(2),
                    Key.of(3), Value.of(3));
            distributedLoadingCache.getAll(keyValueMap.keySet());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedLoadingCache, syncedDistributedLoadingCache))
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(syncedDistributedLoadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(distributedLoadingCache.getIfPresent(key)).isEqualTo(loadedValue);
                        assertThat(syncedDistributedLoadingCache.getIfPresent(key)).isEqualTo(loadedValue);
                        assertThat(distributedLoadingCache.getAllPresent(keyValueMap.keySet())).containsAllEntriesOf(keyValueMap);
                        assertThat(syncedDistributedLoadingCache.getAllPresent(keyValueMap.keySet())).containsAllEntriesOf(keyValueMap);
                        assertThat(countMongoStatus(distributedLoadingCache, CACHED)).isEqualTo(3);
                    });

            Key keyTriggersNoLoading = Key.of(0, "triggers no loading");
            distributedLoadingCache.asMap().get(keyTriggersNoLoading);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedLoadingCache, syncedDistributedLoadingCache))
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(syncedDistributedLoadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(distributedLoadingCache.getIfPresent(keyTriggersNoLoading)).isNull();
                        assertThat(countMongoStatus(distributedLoadingCache, CACHED)).isEqualTo(3);
                    });
        }

        @DisplayName("Test refresh() and refreshAll()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentSerializers")
        void test_DistributedLoadingCache_refresh_refreshAll(CacheBiFunction<Key, Value> cacheBiFunction) {
            CacheLoader<Key, Value> cacheLoader = key -> Value.of(key.getId());

            DistributedLoadingCache<Key, Value> distributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheBiFunction.apply(null, cacheLoader);
            DistributedLoadingCache<Key, Value> syncedDistributedLoadingCache = (DistributedLoadingCache<Key, Value>) cacheBiFunction.apply(null, cacheLoader);

            assertThat(distributedLoadingCache.estimatedSize()).isZero();
            assertThat(syncedDistributedLoadingCache.estimatedSize()).isZero();
            assertThat(countMongoStatus(distributedLoadingCache)).isZero();

            // CacheLoader#load (async) is used if no entry exists

            Key key = Key.of(1);
            Value refreshedValue = Value.of(1);
            distributedLoadingCache.refresh(key);

            Map<Key, Value> keyValueMap = Map.of(
                    Key.of(2), Value.of(2),
                    Key.of(3), Value.of(3));
            distributedLoadingCache.refreshAll(keyValueMap.keySet());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedLoadingCache, syncedDistributedLoadingCache))
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(syncedDistributedLoadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(distributedLoadingCache.getIfPresent(key)).isEqualTo(refreshedValue);
                        assertThat(syncedDistributedLoadingCache.getIfPresent(key)).isEqualTo(refreshedValue);
                        assertThat(distributedLoadingCache.getAllPresent(keyValueMap.keySet()))
                                .containsAllEntriesOf(keyValueMap);
                        assertThat(syncedDistributedLoadingCache.getAllPresent(keyValueMap.keySet()))
                                .containsAllEntriesOf(keyValueMap);
                        assertThat(countMongoStatus(distributedLoadingCache, CACHED)).isEqualTo(3);
                    });

            // CacheLoader#reload (async) is used if entry already exists

            Map<Key, Value> keyValueMapToBeOverwritten = Map.of(
                    Key.of(1), Value.of(1, "toBeOverwritten"),
                    Key.of(2), Value.of(2, "toBeOverwritten"),
                    Key.of(3), Value.of(3, "toBeOverwritten"));
            distributedLoadingCache.putAll(keyValueMapToBeOverwritten);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedLoadingCache, syncedDistributedLoadingCache))
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(syncedDistributedLoadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(distributedLoadingCache.getAllPresent(keyValueMapToBeOverwritten.keySet()))
                                .containsAllEntriesOf(keyValueMapToBeOverwritten);
                        assertThat(syncedDistributedLoadingCache.getAllPresent(keyValueMapToBeOverwritten.keySet()))
                                .containsAllEntriesOf(keyValueMapToBeOverwritten);
                        assertThat(countMongoStatus(distributedLoadingCache, CACHED)).isEqualTo(3);
                    });

            distributedLoadingCache.refresh(key);
            distributedLoadingCache.refreshAll(keyValueMap.keySet());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedLoadingCache, syncedDistributedLoadingCache))
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(syncedDistributedLoadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(distributedLoadingCache.getIfPresent(key)).isEqualTo(refreshedValue);
                        assertThat(syncedDistributedLoadingCache.getIfPresent(key)).isEqualTo(refreshedValue);
                        assertThat(distributedLoadingCache.getAllPresent(keyValueMap.keySet())).containsAllEntriesOf(keyValueMap);
                        assertThat(syncedDistributedLoadingCache.getAllPresent(keyValueMap.keySet())).containsAllEntriesOf(keyValueMap);
                        assertThat(countMongoStatus(distributedLoadingCache, CACHED)).isEqualTo(3);
                    });
        }

        @DisplayName("Test put(), putIfAbsent() and putAll() via asMap()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentSerializers")
        void test_ConcurrentMap_put_putIfAbsent_putAll(CacheBiFunction<Key, Value> cacheBiFunction) {
            DistributedCache<Key, Value> distributedCache = cacheBiFunction.apply(null, null);
            DistributedCache<Key, Value> syncedDistributedCache = cacheBiFunction.apply(null, null);

            ConcurrentMap<Key, Value> distributedCacheMap = distributedCache.asMap();
            ConcurrentMap<Key, Value> syncedDistributedCacheMap = syncedDistributedCache.asMap();

            assertThat(distributedCacheMap).isEmpty();
            assertThat(syncedDistributedCacheMap).isEmpty();
            assertThat(countMongoStatus(distributedCache)).isZero();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            distributedCacheMap.put(key1, value1);

            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            distributedCacheMap.putIfAbsent(key2, value2);
            distributedCacheMap.putIfAbsent(key2, Value.of(0, "not absent"));

            Map<Key, Value> keyValueMap = Map.of(
                    Key.of(3), Value.of(3),
                    Key.of(4), Value.of(4));
            distributedCacheMap.putAll(keyValueMap);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(4);
                        assertThat(syncedDistributedCacheMap).hasSize(4);
                        assertThat(distributedCacheMap).containsEntry(key1, value1);
                        assertThat(syncedDistributedCacheMap).containsEntry(key1, value1);
                        assertThat(distributedCacheMap).containsEntry(key2, value2);
                        assertThat(syncedDistributedCacheMap).containsEntry(key2, value2);
                        assertThat(distributedCacheMap).containsAllEntriesOf(keyValueMap);
                        assertThat(syncedDistributedCacheMap).containsAllEntriesOf(keyValueMap);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(4);
                    });
        }

        @DisplayName("Test compute(), computeIfAbsent() and computeIfPresent() via asMap()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentSerializers")
        void test_ConcurrentMap_compute_computeIfAbsent_computeIfPresent(CacheBiFunction<Key, Value> cacheBiFunction) {
            DistributedCache<Key, Value> distributedCache = cacheBiFunction.apply(null, null);
            DistributedCache<Key, Value> syncedDistributedCache = cacheBiFunction.apply(null, null);

            ConcurrentMap<Key, Value> distributedCacheMap = distributedCache.asMap();
            ConcurrentMap<Key, Value> syncedDistributedCacheMap = syncedDistributedCache.asMap();

            assertThat(distributedCacheMap).isEmpty();
            assertThat(syncedDistributedCacheMap).isEmpty();
            assertThat(countMongoStatus(distributedCache)).isZero();

            Key key1 = Key.of(1);
            Value computedValue1 = Value.of(1);
            distributedCacheMap.compute(key1, (k, v) -> Value.of(k.getId()));

            Key key2 = Key.of(2);
            Value computedValue2 = Value.of(2);
            distributedCacheMap.computeIfAbsent(key2, k -> Value.of(k.getId()));
            distributedCacheMap.computeIfAbsent(key2, k -> Value.of(0, "not absent"));

            Key key3 = Key.of(3);
            Value computedValue3 = Value.of(3);
            distributedCacheMap.computeIfPresent(key3, (k, v) -> Value.of(0, "not present"));
            distributedCacheMap.put(key3, Value.of(0, "present"));
            distributedCacheMap.computeIfPresent(key3, (k, v) -> Value.of(k.getId()));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(3);
                        assertThat(syncedDistributedCacheMap).hasSize(3);
                        assertThat(distributedCacheMap).containsEntry(key1, computedValue1);
                        assertThat(syncedDistributedCacheMap).containsEntry(key1, computedValue1);
                        assertThat(distributedCacheMap).containsEntry(key2, computedValue2);
                        assertThat(syncedDistributedCacheMap).containsEntry(key2, computedValue2);
                        assertThat(distributedCacheMap).containsEntry(key3, computedValue3);
                        assertThat(syncedDistributedCacheMap).containsEntry(key3, computedValue3);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(3);
                    });

            distributedCacheMap.compute(key1, (k, v) -> null);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(2);
                        assertThat(syncedDistributedCacheMap).hasSize(2);
                        assertThat(distributedCacheMap).doesNotContainKey(key1);
                        assertThat(syncedDistributedCacheMap).doesNotContainKey(key1);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(2);
                    });
        }

        @DisplayName("Test replace(), replaceAll() and merge() via asMap()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentSerializers")
        void test_ConcurrentMap_replace_replaceAll_merge(CacheBiFunction<Key, Value> cacheBiFunction) {
            DistributedCache<Key, Value> distributedCache = cacheBiFunction.apply(null, null);
            DistributedCache<Key, Value> syncedDistributedCache = cacheBiFunction.apply(null, null);

            ConcurrentMap<Key, Value> distributedCacheMap = distributedCache.asMap();
            ConcurrentMap<Key, Value> syncedDistributedCacheMap = syncedDistributedCache.asMap();

            assertThat(distributedCacheMap).isEmpty();
            assertThat(syncedDistributedCacheMap).isEmpty();
            assertThat(countMongoStatus(distributedCache)).isZero();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1, "to be replaced");
            Value replacedValue1 = Value.of(1);
            distributedCacheMap.put(key1, value1);

            Key key2 = Key.of(2);
            Value value2 = Value.of(2, "to be replaced");
            Value replacedValue2 = Value.of(2);
            distributedCacheMap.put(key2, value2);

            Map<Key, Value> keyValueMap = Map.of(
                    Key.of(3), Value.of(3, "to be replaced"),
                    Key.of(4), Value.of(4, "to be replaced"));
            Map<Key, Value> replacedKeyValueMap = Map.of(
                    Key.of(3), Value.of(3),
                    Key.of(4), Value.of(4));
            distributedCacheMap.putAll(keyValueMap);

            Key key5 = Key.of(5);
            Value value5 = Value.of(5, "to be merged (replaced)");
            Value replacedValue5 = Value.of(5);
            distributedCacheMap.put(key5, value5);

            Key key6 = Key.of(6);
            Value value6 = Value.of(6, "to be merged (removed)");
            distributedCacheMap.put(key6, value6);

            distributedCacheMap.replace(key1, replacedValue1);
            distributedCacheMap.replace(key2, value2, replacedValue2);
            distributedCacheMap.replaceAll((k, v) -> keyValueMap.containsKey(k) ? Value.of(k.getId()) : v);
            distributedCacheMap.merge(key5, value5, (k, v) -> replacedValue5);
            distributedCacheMap.merge(key6, value6, (k, v) -> null);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(5);
                        assertThat(syncedDistributedCacheMap).hasSize(5);
                        assertThat(distributedCacheMap).containsEntry(key1, replacedValue1);
                        assertThat(syncedDistributedCacheMap).containsEntry(key1, replacedValue1);
                        assertThat(distributedCacheMap).containsEntry(key2, replacedValue2);
                        assertThat(syncedDistributedCacheMap).containsEntry(key2, replacedValue2);
                        assertThat(distributedCacheMap).containsAllEntriesOf(replacedKeyValueMap);
                        assertThat(syncedDistributedCacheMap).containsAllEntriesOf(replacedKeyValueMap);
                        assertThat(distributedCacheMap).containsEntry(key5, replacedValue5);
                        assertThat(syncedDistributedCacheMap).containsEntry(key5, replacedValue5);
                        assertThat(distributedCacheMap).doesNotContainKey(key6);
                        assertThat(syncedDistributedCacheMap).doesNotContainKey(key6);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(5);
                    });
        }

        @DisplayName("Test keySet(), values() and entrySet() via asMap()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentSerializers")
        void test_ConcurrentMap_keySet_values_entrySet(CacheBiFunction<Key, Value> cacheBiFunction) {
            DistributedCache<Key, Value> distributedCache = cacheBiFunction.apply(null, null);
            DistributedCache<Key, Value> syncedDistributedCache = cacheBiFunction.apply(null, null);

            ConcurrentMap<Key, Value> distributedCacheMap = distributedCache.asMap();
            ConcurrentMap<Key, Value> syncedDistributedCacheMap = syncedDistributedCache.asMap();

            assertThat(distributedCacheMap).isEmpty();
            assertThat(syncedDistributedCacheMap).isEmpty();
            assertThat(countMongoStatus(distributedCache)).isZero();

            Key exceptionKey1 = Key.of(1, "exception");
            Value exceptionValue1 = Value.of(1, "exception");
            Key exceptionKey2 = Key.of(2, "exception");
            Value exceptionValue2 = Value.of(2, "exception");
            Entry<Key, Value> exceptionEntry1 = new SimpleEntry<>(exceptionKey1, exceptionValue1);
            Entry<Key, Value> exceptionEntry2 = new SimpleEntry<>(exceptionKey2, exceptionValue2);

            assertThatThrownBy(() -> distributedCacheMap.keySet().add(exceptionKey1))
                    .isInstanceOf(UnsupportedOperationException.class);

            assertThatThrownBy(() -> distributedCacheMap.keySet().addAll(Set.of(exceptionKey1, exceptionKey2)))
                    .isInstanceOf(UnsupportedOperationException.class);

            assertThatThrownBy(() -> distributedCacheMap.values().add(exceptionValue1))
                    .isInstanceOf(UnsupportedOperationException.class);

            assertThatThrownBy(() -> distributedCacheMap.values().addAll(Set.of(exceptionValue1, exceptionValue2)))
                    .isInstanceOf(UnsupportedOperationException.class);

            assertThatThrownBy(() -> distributedCacheMap.entrySet().add(exceptionEntry1))
                    .isInstanceOf(UnsupportedOperationException.class);

            assertThatThrownBy(() -> distributedCacheMap.entrySet().addAll(Set.of(exceptionEntry1, exceptionEntry2)))
                    .isInstanceOf(UnsupportedOperationException.class);

            Map<Key, Value> keyValueMap = IntStream.rangeClosed(1, 6)
                    .boxed()
                    .collect(Collectors.toMap(Key::of, Value::of));
            distributedCacheMap.putAll(keyValueMap);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(6);
                        assertThat(syncedDistributedCacheMap).hasSize(6);
                        assertThat(distributedCacheMap).containsAllEntriesOf(keyValueMap);
                        assertThat(syncedDistributedCacheMap).containsAllEntriesOf(keyValueMap);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(6);
                    });

            distributedCacheMap.remove(Key.of(1));
            distributedCacheMap.values().remove(Value.of(2));
            distributedCacheMap.entrySet().remove(new SimpleEntry<>(Key.of(3), Value.of(3)));
            distributedCacheMap.keySet().removeAll(Set.of(Key.of(4)));
            distributedCacheMap.values().removeAll(Set.of(Value.of(5)));
            distributedCacheMap.entrySet().removeAll(Set.of(new SimpleEntry<>(Key.of(6), Value.of(6))));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).isEmpty();
                        assertThat(syncedDistributedCacheMap).isEmpty();
                        assertThat(countMongoStatus(distributedCache, CACHED)).isZero();
                    });

            distributedCacheMap.putAll(keyValueMap);
            keyValueMap.remove(Key.of(1));
            distributedCacheMap.keySet().retainAll(keyValueMap.keySet());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(keyValueMap.size());
                        assertThat(syncedDistributedCacheMap).hasSize(keyValueMap.size());
                        assertThat(distributedCacheMap).containsAllEntriesOf(keyValueMap);
                        assertThat(syncedDistributedCacheMap).containsAllEntriesOf(keyValueMap);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(keyValueMap.size());
                    });

            keyValueMap.remove(Key.of(2));
            distributedCacheMap.values().retainAll(keyValueMap.values());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(keyValueMap.size());
                        assertThat(syncedDistributedCacheMap).hasSize(keyValueMap.size());
                        assertThat(distributedCacheMap).containsAllEntriesOf(keyValueMap);
                        assertThat(syncedDistributedCacheMap).containsAllEntriesOf(keyValueMap);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(keyValueMap.size());
                    });

            keyValueMap.remove(Key.of(3));
            distributedCacheMap.entrySet().retainAll(keyValueMap.entrySet());

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(keyValueMap.size());
                        assertThat(syncedDistributedCacheMap).hasSize(keyValueMap.size());
                        assertThat(distributedCacheMap).containsAllEntriesOf(keyValueMap);
                        assertThat(syncedDistributedCacheMap).containsAllEntriesOf(keyValueMap);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(keyValueMap.size());
                    });

            distributedCacheMap.entrySet().forEach(entry -> entry.setValue(Value.of(entry.getKey().getId(), "write through")));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(3);
                        assertThat(syncedDistributedCacheMap).hasSize(3);
                        assertThat(distributedCacheMap).containsValues(Value.of(4, "write through"),
                                Value.of(5, "write through"), Value.of(6, "write through"));
                        assertThat(syncedDistributedCacheMap).containsValues(Value.of(4, "write through"),
                                Value.of(5, "write through"), Value.of(6, "write through"));
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(3);
                    });
        }

        @DisplayName("Test remove() and clear() via asMap()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentSerializers")
        void test_ConcurrentMap_remove_clear(CacheBiFunction<Key, Value> cacheBiFunction) {
            DistributedCache<Key, Value> distributedCache = cacheBiFunction.apply(null, null);
            DistributedCache<Key, Value> syncedDistributedCache = cacheBiFunction.apply(null, null);

            ConcurrentMap<Key, Value> distributedCacheMap = distributedCache.asMap();
            ConcurrentMap<Key, Value> syncedDistributedCacheMap = syncedDistributedCache.asMap();

            assertThat(distributedCacheMap).isEmpty();
            assertThat(syncedDistributedCacheMap).isEmpty();
            assertThat(countMongoStatus(distributedCache)).isZero();

            Key key = Key.of(1);
            Value value = Value.of(1);
            distributedCacheMap.put(key, value);

            Map<Key, Value> keyValueMap1 = Map.of(
                    Key.of(2), Value.of(2),
                    Key.of(3), Value.of(3));
            distributedCacheMap.putAll(keyValueMap1);

            Map<Key, Value> keyValueMap2 = Map.of(
                    Key.of(4), Value.of(4),
                    Key.of(5), Value.of(5));
            distributedCacheMap.putAll(keyValueMap2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(5);
                        assertThat(syncedDistributedCacheMap).hasSize(5);
                        assertThat(distributedCacheMap).containsEntry(key, value);
                        assertThat(syncedDistributedCacheMap).containsEntry(key, value);
                        assertThat(distributedCacheMap).containsAllEntriesOf(keyValueMap1);
                        assertThat(syncedDistributedCacheMap).containsAllEntriesOf(keyValueMap1);
                        assertThat(distributedCacheMap).containsAllEntriesOf(keyValueMap2);
                        assertThat(syncedDistributedCacheMap).containsAllEntriesOf(keyValueMap2);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(5);
                    });

            distributedCacheMap.remove(key);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(4);
                        assertThat(syncedDistributedCacheMap).hasSize(4);
                        assertThat(distributedCacheMap).containsAllEntriesOf(keyValueMap1);
                        assertThat(syncedDistributedCacheMap).containsAllEntriesOf(keyValueMap1);
                        assertThat(distributedCacheMap).containsAllEntriesOf(keyValueMap2);
                        assertThat(syncedDistributedCacheMap).containsAllEntriesOf(keyValueMap2);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(4);
                    });

            keyValueMap1.forEach(distributedCacheMap::remove);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).hasSize(2);
                        assertThat(syncedDistributedCacheMap).hasSize(2);
                        assertThat(distributedCacheMap).containsAllEntriesOf(keyValueMap2);
                        assertThat(syncedDistributedCacheMap).containsAllEntriesOf(keyValueMap2);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(2);
                    });

            distributedCacheMap.clear();

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheMap).isEmpty();
                        assertThat(syncedDistributedCacheMap).isEmpty();
                        assertThat(countMongoStatus(distributedCache, CACHED)).isZero();
                    });
        }

        @DisplayName("Test population")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentDistributionModes")
        void test_DistributedCaffeine_population_with_different_distribution_modes(CacheBiFunction<Key, Value> cacheBiFunction) {
            Supplier<BuilderFunction<Key, Value>> builderFunctionSupplier = () ->
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .maximumSize(2));

            DistributedCache<Key, Value> distributedCacheA = cacheBiFunction.apply(builderFunctionSupplier.get(), null);
            DistributedCache<Key, Value> distributedCacheB = cacheBiFunction.apply(builderFunctionSupplier.get(), null);

            DistributedCaffeine<Key, Value> distributedCaffeine = getDistributedCaffeine(distributedCacheA);
            DistributionMode distributionMode = readFieldValue(distributedCaffeine, DistributedCaffeine.class,
                    "distributionMode", DistributionMode.class);

            assertThat(distributedCacheA.estimatedSize()).isZero();
            assertThat(distributedCacheB.estimatedSize()).isZero();
            assertThat(countMongoStatus(distributedCacheA)).isZero();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);
            distributedCacheB.put(key2, value2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                                || distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyEntriesOf(distributedCacheB.asMap());
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        } else if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        }
                    });

            distributedCacheA.put(key2, value2);
            distributedCacheB.put(key1, value1);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                                || distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyEntriesOf(distributedCacheB.asMap());
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        } else if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyEntriesOf(distributedCacheB.asMap());
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        }
                    });

            deleteMongoExpires(distributedCacheA);

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(isCacheManagerMaintained(distributedCacheA, distributedCacheB)).isTrue());
        }

        @DisplayName("Test invalidation")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentDistributionModes")
        void test_DistributedCaffeine_invalidation_with_different_distribution_modes(CacheBiFunction<Key, Value> cacheBiFunction) {
            Supplier<BuilderFunction<Key, Value>> builderFunctionSupplier = () ->
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .maximumSize(2));

            DistributedCache<Key, Value> distributedCacheA = cacheBiFunction.apply(builderFunctionSupplier.get(), null);
            DistributedCache<Key, Value> distributedCacheB = cacheBiFunction.apply(builderFunctionSupplier.get(), null);

            DistributedCaffeine<Key, Value> distributedCaffeine = getDistributedCaffeine(distributedCacheA);
            DistributionMode distributionMode = readFieldValue(distributedCaffeine, DistributedCaffeine.class,
                    "distributionMode", DistributionMode.class);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);
            distributedCacheB.put(key2, value2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                                || distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyEntriesOf(distributedCacheB.asMap());
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        } else if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        }
                    });

            distributedCacheA.invalidate(key2);
            distributedCacheB.invalidate(key1);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                                || distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        } else if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        }
                    });

            distributedCacheA.invalidate(key1);
            distributedCacheB.invalidate(key2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                                || distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        } else if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        }
                    });

            deleteMongoExpires(distributedCacheA);

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(isCacheManagerMaintained(distributedCacheA, distributedCacheB)).isTrue());
        }

        @DisplayName("Test eviction")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentDistributionModes")
        void test_DistributedCaffeine_eviction_with_different_distribution_modes(CacheBiFunction<Key, Value> cacheBiFunction) {
            Supplier<BuilderFunction<Key, Value>> builderFunctionSupplier = () ->
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .maximumSize(1));

            DistributedCache<Key, Value> distributedCacheA = cacheBiFunction.apply(builderFunctionSupplier.get(), null);
            DistributedCache<Key, Value> distributedCacheB = cacheBiFunction.apply(builderFunctionSupplier.get(), null);

            DistributedCaffeine<Key, Value> distributedCaffeine = getDistributedCaffeine(distributedCacheA);
            DistributionMode distributionMode = readFieldValue(distributedCaffeine, DistributedCaffeine.class,
                    "distributionMode", DistributionMode.class);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);
            await("end of operation to prevent interference")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                                || distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key1)).isNotSameAs(value1);
                        }
                    });
            distributedCacheB.put(key2, value2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(1);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(1);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            // population and eviction sometimes interfere here, but without any (side) effects
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isBetween(1L, 2L);
                        } else if (distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(1);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(1);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        } else if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        }
                    });

            distributedCacheA.put(key2, value2);
            await("end of operation to prevent interference")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                                || distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedCacheB.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheB.getIfPresent(key2)).isNotSameAs(value2);
                        }
                    });
            distributedCacheB.put(key1, value1);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(1);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(3);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            // population and eviction sometimes interfere here, but without any (side) effects
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isBetween(2L, 4L);
                        } else if (distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(1);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(3);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        } else if (distributionMode == INVALIDATION_AND_EVICTION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(2);
                        } else if (distributionMode == INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2);
                            assertThat(distributedCacheB.getIfPresent(key1)).isEqualTo(value1);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        }
                    });

            deleteMongoExpires(distributedCacheA);

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(isCacheManagerMaintained(distributedCacheA, distributedCacheB)).isTrue());
        }

        @DisplayName("Test synchronization")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentDistributionModes")
        void test_DistributedCaffeine_synchronization_with_different_distribution_modes(CacheBiFunction<Key, Value> cacheBiFunction) {
            Supplier<BuilderFunction<Key, Value>> builderFunctionSupplier = () ->
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .maximumSize(2));

            DistributedCache<Key, Value> distributedCacheA = cacheBiFunction.apply(builderFunctionSupplier.get(), null);

            DistributedCaffeine<Key, Value> distributedCaffeine = getDistributedCaffeine(distributedCacheA);
            DistributionMode distributionMode = readFieldValue(distributedCaffeine, DistributedCaffeine.class,
                    "distributionMode", DistributionMode.class);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            distributedCacheA.put(key1, value1);
            distributedCacheA.put(key2, value2);

            DistributedCache<Key, Value> distributedCacheB = cacheBiFunction.apply(builderFunctionSupplier.get(), null);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheA, distributedCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                                || distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheA.asMap())
                                    .containsExactlyEntriesOf(distributedCacheB.asMap());
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        } else if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedCacheA, EVICTED)).isEqualTo(0);
                        }
                    });

            deleteMongoExpires(distributedCacheA);

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(isCacheManagerMaintained(distributedCacheA, distributedCacheB)).isTrue());
        }

        @DisplayName("Test refresh")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheBiFunctionsWithDifferentDistributionModes")
        void test_DistributedCaffeine_refresh_with_different_distribution_modes(CacheBiFunction<Key, Value> cacheBiFunction) {
            Supplier<BuilderFunction<Key, Value>> builderFunctionSupplier = () ->
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .maximumSize(2));

            AtomicBoolean returnNull = new AtomicBoolean(false);
            AtomicInteger idCounter = new AtomicInteger(0);
            CacheLoader<Key, Value> cacheLoader = key -> returnNull.get()
                    ? null
                    : Value.of(idCounter.incrementAndGet());

            DistributedLoadingCache<Key, Value> distributedLoadingCacheA = (DistributedLoadingCache<Key, Value>) cacheBiFunction.apply(builderFunctionSupplier.get(), cacheLoader);
            DistributedLoadingCache<Key, Value> distributedLoadingCacheB = (DistributedLoadingCache<Key, Value>) cacheBiFunction.apply(builderFunctionSupplier.get(), cacheLoader);

            DistributedCaffeine<Key, Value> distributedCaffeine = getDistributedCaffeine(distributedLoadingCacheA);
            DistributionMode distributionMode = readFieldValue(distributedCaffeine, DistributedCaffeine.class,
                    "distributionMode", DistributionMode.class);

            assertThat(distributedLoadingCacheA.estimatedSize()).isZero();
            assertThat(distributedLoadingCacheB.estimatedSize()).isZero();
            assertThat(countMongoStatus(distributedLoadingCacheA)).isZero();

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);

            distributedLoadingCacheA.refresh(key1);
            await("end of operation to prevent interference")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(Value.of(1));
                        }
                    });
            distributedLoadingCacheB.refresh(key2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                                || distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(countMongoStatus(distributedLoadingCacheA, CACHED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedLoadingCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, EVICTED)).isEqualTo(0);
                        } else if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(1);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(Value.of(1));
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(Value.of(2));
                            assertThat(countMongoStatus(distributedLoadingCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, EVICTED)).isEqualTo(0);
                        }
                    });

            distributedLoadingCacheA.refreshAll(Set.of(key2));
            await("end of operation to prevent interference")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isEqualTo(Value.of(3));
                        }
                    });
            distributedLoadingCacheB.refreshAll(Set.of(key1));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                                || distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheA.asMap())
                                    .containsExactlyEntriesOf(distributedLoadingCacheB.asMap());
                            assertThat(countMongoStatus(distributedLoadingCacheA, CACHED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedLoadingCacheA, ORPHANED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedLoadingCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, EVICTED)).isEqualTo(0);
                        } else if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(2);
                            assertThat(distributedLoadingCacheA.getIfPresent(key1)).isEqualTo(Value.of(1));
                            assertThat(distributedLoadingCacheA.getIfPresent(key2)).isEqualTo(Value.of(3));
                            assertThat(distributedLoadingCacheB.getIfPresent(key1)).isEqualTo(Value.of(4));
                            assertThat(distributedLoadingCacheB.getIfPresent(key2)).isEqualTo(Value.of(2));
                            assertThat(countMongoStatus(distributedLoadingCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, INVALIDATED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, EVICTED)).isEqualTo(0);
                        }
                    });

            returnNull.set(true); // turn refreshes to invalidations

            distributedLoadingCacheA.refresh(key1);
            distributedLoadingCacheB.refresh(key2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedLoadingCacheA, distributedLoadingCacheB))
                    .untilAsserted(() -> {
                        if (distributionMode == POPULATION_AND_INVALIDATION_AND_EVICTION
                                || distributionMode == POPULATION_AND_INVALIDATION) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, ORPHANED)).isEqualTo(4);
                            assertThat(countMongoStatus(distributedLoadingCacheA, INVALIDATED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedLoadingCacheA, EVICTED)).isEqualTo(0);
                        } else if (distributionMode == INVALIDATION_AND_EVICTION
                                || distributionMode == INVALIDATION) {
                            assertThat(distributedLoadingCacheA.estimatedSize()).isEqualTo(0);
                            assertThat(distributedLoadingCacheB.estimatedSize()).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, CACHED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, ORPHANED)).isEqualTo(0);
                            assertThat(countMongoStatus(distributedLoadingCacheA, INVALIDATED)).isEqualTo(2);
                            assertThat(countMongoStatus(distributedLoadingCacheA, EVICTED)).isEqualTo(0);
                        }
                    });

            deleteMongoExpires(distributedLoadingCacheA);

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(isCacheManagerMaintained(distributedLoadingCacheA, distributedLoadingCacheB)).isTrue());
        }

        @DisplayName("Test MongoDB synchronization")
        @Test
        void test_DistributedCaffeine_MongoDB_synchronization() {
            String collectionName = getCollectionNameWithSuffix("cacheMongoSynchronization");

            DistributedCache<Key, Value> distributedCache = createDistributedCache(collectionName);

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            distributedCache.put(key1, value1);
            // create orphaned cache entry
            distributedCache.put(key1, Value.of(1)); // new value instance

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache))
                    .untilAsserted(() -> assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(1));

            // create inconsistencies in relation to not orphaned cache entries
            distributedCache.distributedPolicy().getMongoCollection().updateMany(Filters.empty(),
                    Updates.combine(Updates.set(STATUS, CACHED), Updates.set(EXPIRES, null)));

            assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(2);

            // corrects inconsistencies in relation to not orphaned cache entries implicitly
            DistributedCache<Key, Value> syncedDistributedCache = createDistributedCache(collectionName);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(distributedCache.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(syncedDistributedCache.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(1);
                    });

            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            distributedCache.put(key2, value2);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCache.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(syncedDistributedCache.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(distributedCache.getIfPresent(key2)).isEqualTo(value2);
                        assertThat(syncedDistributedCache.getIfPresent(key2)).isEqualTo(value2);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(2);
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
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCache.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(syncedDistributedCache.getIfPresent(key1)).isEqualTo(overwrittenValue);
                        assertThat(distributedCache.getIfPresent(key2)).isEqualTo(value2);
                        assertThat(syncedDistributedCache.getIfPresent(key2)).isNull();
                        assertThat(syncedDistributedCache.getIfPresent(key3)).isEqualTo(value3);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(2);
                    });

            syncedDistributedCache.distributedPolicy().startSynchronization();

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCache.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(syncedDistributedCache.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(distributedCache.getIfPresent(key2)).isEqualTo(value2);
                        assertThat(syncedDistributedCache.getIfPresent(key2)).isEqualTo(value2);
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(2);
                    });
        }

        @DisplayName("Test removal and eviction listener")
        @Test
        void test_DistributedCaffeine_removal_and_eviction_listener() {
            String collectionName = getCollectionNameWithSuffix("cacheEvictionListener");

            AtomicReference<RemovalCause> removalCause = new AtomicReference<>(null);
            AtomicReference<RemovalCause> evictionCause = new AtomicReference<>(null);

            RemovalListener<Key, Value> removalListener = (key, value, cause) ->
                    removalCause.set(cause);
            RemovalListener<Key, Value> evictionListener = (key, value, cause) ->
                    evictionCause.set(cause);

            DistributedCache<Key, Value> distributedCacheWithListeners = createDistributedCache(collectionName,
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .maximumSize(1)
                            .removalListener(removalListener)
                            .evictionListener(evictionListener)));

            assertThat(removalCause).hasNullValue();
            assertThat(evictionCause).hasNullValue();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            distributedCacheWithListeners.put(key1, value1);
            distributedCacheWithListeners.put(key2, value2);
            distributedCacheWithListeners.invalidateAll();

            await("invocation of listeners")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheWithListeners))
                    .untilAsserted(() -> {
                        assertThat(removalCause).hasValue(RemovalCause.EXPLICIT);
                        assertThat(evictionCause).hasValue(RemovalCause.SIZE);
                    });
        }

        @DisplayName("Test policy()")
        @Test
        void test_DistributedCaffeine_policy() {
            String collectionName = getCollectionNameWithSuffix("cachePolicy");

            Expiry<Key, Value> expiry = new Expiry<>() {

                @Override
                public long expireAfterCreate(Key key, Value value, long currentTime) {
                    return Long.MAX_VALUE;
                }

                @Override
                public long expireAfterUpdate(Key key, Value value, long currentTime, long currentDuration) {
                    return Long.MAX_VALUE;
                }

                @Override
                public long expireAfterRead(Key key, Value value, long currentTime, long currentDuration) {
                    return Long.MAX_VALUE;
                }
            };

            DistributedCache<Key, Value> distributedCacheWithVarExpiration = createDistributedCache(collectionName,
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .expireAfter(expiry)));

            DistributedCache<Key, Value> syncedDistributedCacheWithVarExpiration = createDistributedCache(collectionName,
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .expireAfter(expiry)));

            VarExpiration<Key, Value> varExpiration = distributedCacheWithVarExpiration.policy().expireVariably().orElseThrow();

            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            varExpiration.put(key1, value1, Duration.ofHours(1));

            Key key2 = Key.of(2);
            Value value2 = Value.of(2);
            varExpiration.putIfAbsent(key2, value2, Duration.ofHours(1));
            varExpiration.putIfAbsent(key2, Value.of(0, "not absent"), Duration.ofHours(1));

            Key key3 = Key.of(3);
            Value computedValue3 = Value.of(3);
            varExpiration.compute(key3, (k, v) -> Value.of(k.getId()), Duration.ofHours(1));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheWithVarExpiration, syncedDistributedCacheWithVarExpiration))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheWithVarExpiration.estimatedSize()).isEqualTo(3);
                        assertThat(syncedDistributedCacheWithVarExpiration.estimatedSize()).isEqualTo(3);
                        assertThat(distributedCacheWithVarExpiration.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(syncedDistributedCacheWithVarExpiration.getIfPresent(key1)).isEqualTo(value1);
                        assertThat(distributedCacheWithVarExpiration.getIfPresent(key2)).isEqualTo(value2);
                        assertThat(syncedDistributedCacheWithVarExpiration.getIfPresent(key2)).isEqualTo(value2);
                        assertThat(distributedCacheWithVarExpiration.getIfPresent(key3)).isEqualTo(computedValue3);
                        assertThat(syncedDistributedCacheWithVarExpiration.getIfPresent(key3)).isEqualTo(computedValue3);
                        assertThat(countMongoStatus(distributedCacheWithVarExpiration, CACHED)).isEqualTo(3);
                    });

            varExpiration.compute(key1, (k, v) -> null, Duration.ofHours(1));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCacheWithVarExpiration, syncedDistributedCacheWithVarExpiration))
                    .untilAsserted(() -> {
                        assertThat(distributedCacheWithVarExpiration.estimatedSize()).isEqualTo(2);
                        assertThat(syncedDistributedCacheWithVarExpiration.estimatedSize()).isEqualTo(2);
                        assertThat(distributedCacheWithVarExpiration.getIfPresent(key1)).isNull();
                        assertThat(syncedDistributedCacheWithVarExpiration.getIfPresent(key1)).isNull();
                        assertThat(countMongoStatus(distributedCacheWithVarExpiration, CACHED)).isEqualTo(2);
                    });
        }

        @DisplayName("Test distributedPolicy()")
        @Test
        void test_DistributedCaffeine_distributedPolicy() {
            String collectionName = getCollectionNameWithSuffix("cacheDistributedPolicy");

            Expiry<Key, Value> expiry = new Expiry<>() {

                @Override
                public long expireAfterCreate(Key key, Value value, long currentTime) {
                    return Long.MAX_VALUE;
                }

                @Override
                public long expireAfterUpdate(Key key, Value value, long currentTime, long currentDuration) {
                    return Long.MAX_VALUE;
                }

                @Override
                public long expireAfterRead(Key key, Value value, long currentTime, long currentDuration) {
                    return Long.MAX_VALUE;
                }
            };

            DistributedCache<Key, Value> distributedCache = createDistributedCache(collectionName,
                    b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                            .expireAfter(expiry)));
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            assertThat(distributedPolicy.getMongoCollection().getNamespace().getCollectionName())
                    .isEqualTo(collectionName);

            Key manipulatedKey = Key.of(0);
            Value manipulatedValue = Value.of(0);
            Key key1 = Key.of(1);
            Value value1 = Value.of(1);
            Key key2 = Key.of(2);
            Value value2 = Value.of(2);

            VarExpiration<Key, Value> varExpiration = distributedCache.policy().expireVariably().orElseThrow();

            varExpiration.put(manipulatedKey, manipulatedValue, Duration.ofHours(1));

            // create inconsistencies in relation to hash values
            distributedPolicy.getMongoCollection()
                    .updateMany(Filters.empty(), Updates.set(HASH, key1.hashCode()));

            varExpiration.put(key1, value1, Duration.ofHours(1));
            distributedCache.invalidate(key1);
            varExpiration.put(key1, value1, Duration.ofHours(1));
            varExpiration.put(key2, value2, Duration.ZERO); // evict fast

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedPolicy.getFromMongo(key2, false)).isNull();
                        assertThat(distributedPolicy.getFromMongo(key2, true).getValue()).isEqualTo(value2);
                        assertThat(distributedPolicy.getAllFromMongo(Set.of(key1, key2), false).stream()
                                .collect(Collectors.toMap(CacheEntry::getKey, CacheEntry::getValue)))
                                .containsExactlyInAnyOrderEntriesOf(Map.of(key1, value1));
                        assertThat(distributedPolicy.getAllFromMongo(Set.of(key1, key2), true).stream()
                                .collect(Collectors.toMap(CacheEntry::getKey, CacheEntry::getValue)))
                                .containsExactlyInAnyOrderEntriesOf(Map.of(key1, value1, key2, value2));
                    });

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache))
                    .untilAsserted(() -> assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(2));
        }

        @DisplayName("Test direct manipulation")
        @Test
        void test_DistributedCaffeine_direct_manipulation() {
            String collectionName = getCollectionNameWithSuffix("cacheDirectManipulation");

            DistributedCache<Key, Value> distributedCache = createDistributedCache(collectionName);

            DistributedCaffeine<Key, Value> distributedCaffeine = getDistributedCaffeine(distributedCache);
            MongoCollection<Document> mongoCollection = distributedCaffeine.getMongoCollection();
            InternalMongoRepository<Key, Value> mongoRepository = distributedCaffeine.getMongoRepository();

            Key key = Key.of(1);
            Value value = Value.of(1);

            ObjectId objectId = new ObjectId();
            Bson update = invokeMethod(mongoRepository, InternalMongoRepository.class,
                    "toMongoUpdate", List.of(Key.class, Value.class, String.class), List.of(key, value, CACHED));

            mongoCollection.updateOne(Filters.eq(ID, objectId), update, new UpdateOptions().upsert(true));

            await("distribution via change streams")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(distributedCache.getIfPresent(key)).isEqualTo(value);
                        assertThat(countMongoStatus(distributedCache)).isEqualTo(1);
                        assertThat(isCacheManagerMaintained(distributedCache)).isTrue();
                    });

            Value updatedValue = Value.of(1, "updatedValue");
            Serializer<Value, ?> valueSerializer = getDistributedCaffeine(distributedCache).getValueSerializer();
            try {
                update = Updates.set(VALUE, valueSerializer.serialize(updatedValue));
            } catch (SerializerException ignored) {
            }

            mongoCollection.updateOne(Filters.eq(ID, objectId), update);

            await("distribution via change streams")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(distributedCache.getIfPresent(key)).isEqualTo(updatedValue);
                        assertThat(countMongoStatus(distributedCache)).isEqualTo(1);
                        assertThat(isCacheManagerMaintained(distributedCache)).isTrue();
                    });

            mongoCollection.deleteMany(Filters.empty());

            await("distribution via change streams")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isZero();
                        assertThat(distributedCache.getIfPresent(key)).isNull();
                        assertThat(countMongoStatus(distributedCache)).isZero();
                        assertThat(isCacheManagerMaintained(distributedCache)).isTrue();
                    });
        }

        @DisplayName("Test that no unnecessary operations")
        @Test
        void test_DistributedCaffeine_no_unnecessary_operations() {
            String collectionName = getCollectionNameWithSuffix("cacheNoUnnecessaryOperations");

            DistributedCache<Key, Value> distributedCache = createDistributedCache(collectionName);
            DistributedCache<Key, Value> syncedDistributedCache = createDistributedCache(collectionName);

            Key key = Key.of(1);
            Value value = Value.of(1);
            distributedCache.put(key, value);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .failFast(() -> cleanUp(distributedCache, syncedDistributedCache))
                    .untilAsserted(() -> {
                        assertThat(distributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(syncedDistributedCache.estimatedSize()).isEqualTo(1);
                        assertThat(distributedCache.getIfPresent(key)).isEqualTo(value);
                        assertThat(syncedDistributedCache.getIfPresent(key)).isEqualTo(value);
                        assertThat(distributedCache.getIfPresent(Key.of(0, "not present"))).isNull();
                        assertThat(syncedDistributedCache.getIfPresent(Key.of(0, "not present"))).isNull();
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(1);
                        // identity checks
                        assertThat(distributedCache.getIfPresent(key)).isSameAs(value);
                        assertThat(syncedDistributedCache.getIfPresent(key)).isNotSameAs(value);
                    });

            distributedCache.put(key, value); // same value instance
            distributedCache.invalidate(Key.of(0, "not present"));

            await("synchronization between cache instances")
                    .pollInterval(WAITING_DURATION) // wait for (no) synchronization
                    .untilAsserted(() -> {
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(1);
                        assertThat(countMongoStatus(distributedCache, ORPHANED)).isEqualTo(0);
                        assertThat(countMongoStatus(distributedCache, INVALIDATED)).isEqualTo(0);
                    });

            distributedCache.put(key, Value.of(value.getId())); // new value instance

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(countMongoStatus(distributedCache, CACHED)).isEqualTo(1);
                        assertThat(countMongoStatus(distributedCache, ORPHANED)).isEqualTo(1);
                    });
        }

        @DisplayName("Test that weak or soft references are not supported")
        @Test
        void test_DistributedCaffeine_weak_or_soft_references_not_supported() {
            String collectionName = getCollectionNameWithSuffix("cacheWeakOrSoftReferencesNotSupported");

            assertThatThrownBy(() ->
                    createDistributedCache(collectionName,
                            b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                    .weakKeys()
                                    .weakValues())))
                    .isInstanceOf(DistributedCaffeineException.class)
                    .hasMessageStartingWith("The use of weak or soft references is not supported");
        }

        @DisplayName("Test that removal listener is not invoked if refresh returns old value")
        @Test
        void test_Caffeine_removal_listener_is_not_invoked_if_refresh_returns_old_value() {

            AtomicInteger removalInvocations = new AtomicInteger();
            RemovalListener<Key, Value> removalListener = (key, value, removalCause) ->
                    removalInvocations.incrementAndGet();

            AtomicInteger loadInvocations = new AtomicInteger();
            AtomicInteger asyncLoadInvocations = new AtomicInteger();
            AtomicInteger asyncReloadInvocations = new AtomicInteger();
            CacheLoader<Key, Value> cacheLoader = new CacheLoader<>() {
                @Override
                public Value load(Key key) {
                    loadInvocations.incrementAndGet();
                    return Value.of(key.getId(), format("load_%s_%s", System.currentTimeMillis(), key.getId()));
                }

                @Override
                public CompletableFuture<? extends Value> asyncLoad(Key key, Executor executor) {
                    asyncLoadInvocations.incrementAndGet();
                    return CompletableFuture.completedFuture(load(key));
                }

                @Override
                public CompletableFuture<? extends Value> asyncReload(Key key, Value oldValue, Executor executor) {
                    asyncReloadInvocations.incrementAndGet();
                    return CompletableFuture.completedFuture(oldValue);
                }
            };

            LoadingCache<Key, Value> loadingCache = Caffeine.newBuilder()
                    .removalListener(removalListener)
                    .build(cacheLoader);

            Key key = Key.of(1);
            Set<Key> keys = Set.of(Key.of(2), Key.of(3));

            loadingCache.refresh(key);
            loadingCache.refreshAll(keys);

            await("refresh (initial load)")
                    .failFast(loadingCache::cleanUp)
                    .pollInterval(WAITING_DURATION) // wait for (no) async removal listener
                    .untilAsserted(() -> {
                        assertThat(loadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(removalInvocations).hasValue(0);
                        assertThat(loadInvocations).hasValue(3);
                        assertThat(asyncLoadInvocations).hasValue(3);
                        assertThat(asyncReloadInvocations).hasValue(0);
                    });

            loadingCache.refresh(key);
            loadingCache.refreshAll(keys);

            await("refresh (reload)")
                    .failFast(loadingCache::cleanUp)
                    .pollInterval(WAITING_DURATION) // wait for (no) async removal listener
                    .untilAsserted(() -> {
                        assertThat(loadingCache.estimatedSize()).isEqualTo(3);
                        assertThat(removalInvocations).hasValue(0);
                        assertThat(loadInvocations).hasValue(3);
                        assertThat(asyncLoadInvocations).hasValue(3);
                        assertThat(asyncReloadInvocations).hasValue(3);
                    });
        }

        @DisplayName("Test ChangeStreamWatcher")
        @Test
        void test_ChangeStreamWatcher() {
            String collectionName = getCollectionNameWithSuffix("cacheChangeStreamWatcher");

            // test early failure
            assertThatThrownBy(() ->
                    createDistributedCache(collectionName,
                            b -> b.withCustomKeySerializer(new Serializer<>() {
                                @Override
                                public Object serialize(Object object) {
                                    return null;
                                }

                                @Override
                                public Object deserialize(Object value) {
                                    return null;
                                }
                            })))
                    .isInstanceOf(DistributedCaffeineException.class)
                    .hasMessageStartingWith("Watching change streams failed")
                    .cause()
                    .hasMessageStartingWith("Unknown serializer");

            DistributedCache<Key, Value> distributedCache = createDistributedCache(collectionName);
            distributedCache.put(Key.of(1), Value.of(1));

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(isCacheManagerMaintained(distributedCache)).isTrue());

            // test retry on failure

            DistributedCaffeine<Key, Value> distributedCaffeine = getDistributedCaffeine(distributedCache);
            InternalChangeStreamWatcher<Key, Value> changeStreamWatcher = readFieldValue(distributedCaffeine, DistributedCaffeine.class,
                    "changeStreamWatcher", InternalChangeStreamWatcher.class);

            List<String> logs = new ArrayList<>();
            Logger logger = new Logger() {
                @Override
                public String getName() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean isLoggable(Level level) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
                    logs.add(msg);
                }

                @Override
                public void log(Level level, ResourceBundle bundle, String format, Object... params) {
                    throw new UnsupportedOperationException();
                }
            };

            // provoke failure
            writeFieldValue(changeStreamWatcher, InternalChangeStreamWatcher.class, "logger", logger);
            AtomicReference<BsonTimestamp> operationTime = readFieldValue(changeStreamWatcher, InternalChangeStreamWatcher.class,
                    "operationTime", AtomicReference.class);
            writeFieldValue(changeStreamWatcher, InternalChangeStreamWatcher.class, "operationTime", null);

            distributedCache.put(Key.of(1), Value.of(1)); // new value instance

            await("failure")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(logs.size()).isPositive();
                        assertThat(logs).allMatch(log -> log.contains("Watching change streams failed"));
                    });

            await("cache manager maintenance")
                    .pollInterval(WAITING_DURATION) // wait for (no) cache manager maintenance
                    .untilAsserted(() ->
                            assertThat(isCacheManagerMaintained(distributedCache)).isFalse());

            // fix failure
            writeFieldValue(changeStreamWatcher, InternalChangeStreamWatcher.class, "operationTime", operationTime);

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION.plusSeconds(10)) // retry delay is increased on failure
                    .untilAsserted(() ->
                            assertThat(isCacheManagerMaintained(distributedCache)).isTrue());

            // provoke failure
            InternalDocumentConverter<Key, Value> documentConverter = readFieldValue(changeStreamWatcher, InternalChangeStreamWatcher.class,
                    "documentConverter", InternalDocumentConverter.class);
            writeFieldValue(changeStreamWatcher, InternalChangeStreamWatcher.class, "documentConverter", null);

            logs.clear();
            distributedCache.put(Key.of(1), Value.of(1)); // new value instance

            await("failure")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(logs.size()).isPositive();
                        assertThat(logs).allMatch(log -> log.contains("Deserializing of cache entry failed"));
                    });

            // fix failure
            writeFieldValue(changeStreamWatcher, InternalChangeStreamWatcher.class, "documentConverter", documentConverter);

            await("cache manager maintenance")
                    .atMost(WAITING_DURATION.plusSeconds(10)) // retry delay is increased on failure
                    .untilAsserted(() ->
                            assertThat(isCacheManagerMaintained(distributedCache)).isTrue());

            // deactivate synchronization
            distributedCache.distributedPolicy().stopSynchronization();

            distributedCache.put(Key.of(1), Value.of(1)); // new value instance

            await("insert")
                    .pollInterval(WAITING_DURATION) // wait for (no) insert
                    .untilAsserted(() ->
                            assertThat(countMongoStatus(distributedCache)).isEqualTo(3));

            @SuppressWarnings("resource")
            ExecutorService executorService = readFieldValue(changeStreamWatcher, InternalChangeStreamWatcher.class,
                    "executorService", ExecutorService.class);

            await("shut down")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(executorService.isShutdown()).isTrue());

            // activate synchronization
            distributedCache.distributedPolicy().startSynchronization();

            distributedCache.put(Key.of(1), Value.of(1)); // new value instance

            await("insert")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(countMongoStatus(distributedCache)).isEqualTo(4));
        }

        @DisplayName("Test MaintenanceWorker")
        @Test
        void test_MaintenanceWorker() {
            String collectionName = getCollectionNameWithSuffix("cacheMaintenanceWorker");

            DistributedCache<Key, Value> distributedCache = createDistributedCache(collectionName);
            distributedCache.put(Key.of(1), Value.of(1));

            // create orphaned cache entry
            distributedCache.put(Key.of(1), Value.of(1)); // new value instance

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(countMongoStatus(distributedCache, ORPHANED)).isEqualTo(1));

            deleteMongoExpires(distributedCache);

            await("deletion")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(countMongoStatus(distributedCache, ORPHANED)).isZero());

            // create orphaned cache entry
            distributedCache.put(Key.of(1), Value.of(1)); // new value instance

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(countMongoStatus(distributedCache, ORPHANED)).isEqualTo(1));

            // test retry on failure

            InternalMaintenanceWorker<Key, Value> maintenanceWorker = getDistributedCaffeine(distributedCache).getMaintenanceWorker();

            List<String> logs = new ArrayList<>();
            Logger logger = new Logger() {
                @Override
                public String getName() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean isLoggable(Level level) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
                    logs.add(msg);
                }

                @Override
                public void log(Level level, ResourceBundle bundle, String format, Object... params) {
                    throw new UnsupportedOperationException();
                }
            };

            // provoke failure
            writeFieldValue(maintenanceWorker, InternalMaintenanceWorker.class, "logger", logger);
            PriorityBlockingQueue<ObjectId> toBeOrphaned = readFieldValue(maintenanceWorker, InternalMaintenanceWorker.class,
                    "toBeOrphaned", PriorityBlockingQueue.class);
            writeFieldValue(maintenanceWorker, InternalMaintenanceWorker.class, "toBeOrphaned", null);

            await("failure")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(logs.size()).isPositive();
                        assertThat(logs).allMatch(log -> log.contains("Maintenance failed"));
                    });

            deleteMongoExpires(distributedCache);

            await("deletion")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(countMongoStatus(distributedCache, ORPHANED)).isZero());

            // fix failure
            writeFieldValue(maintenanceWorker, InternalMaintenanceWorker.class, "toBeOrphaned", toBeOrphaned);

            // create orphaned cache entry
            distributedCache.put(Key.of(1), Value.of(1)); // new value instance

            await("maintenance")
                    .atMost(WAITING_DURATION.plusSeconds(10)) // retry delay is increased on failure
                    .untilAsserted(() ->
                            assertThat(countMongoStatus(distributedCache, ORPHANED)).isEqualTo(1));

            // deactivate synchronization
            distributedCache.distributedPolicy().stopSynchronization();

            // create orphaned cache entry
            distributedCache.put(Key.of(1), Value.of(1)); // new value instance

            await("maintenance")
                    .pollInterval(WAITING_DURATION) // wait for (no) maintenance
                    .untilAsserted(() ->
                            assertThat(countMongoStatus(distributedCache, ORPHANED)).isEqualTo(1));

            @SuppressWarnings("resource")
            ExecutorService executorService = readFieldValue(maintenanceWorker, InternalMaintenanceWorker.class,
                    "executorService", ExecutorService.class);

            await("shut down")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(executorService.isShutdown()).isTrue());

            // activate synchronization
            distributedCache.distributedPolicy().startSynchronization();

            // create orphaned cache entry
            distributedCache.put(Key.of(1), Value.of(1)); // new value instance

            await("maintenance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() ->
                            assertThat(countMongoStatus(distributedCache, ORPHANED)).isEqualTo(2));
        }

        @DisplayName("Stress test synchronization from MongoDB")
        @Test
        void stress_test_DistributedCaffeine_synchronization_from_MongoDB() {
            final Duration EXTENDED_WAITING_DURATION = WAITING_DURATION.multipliedBy(100);
            final Duration EXTENDED_POLL_INTERVAL = Duration.ofSeconds(1);

            String collectionName = getCollectionNameWithSuffix("cacheStressSynchronizationFromMongo");
            int cacheSize = 100_000;

            Supplier<DistributedLoadingCache<Key, Value>> cacheSupplier = () -> {
                CacheLoader<Key, Value> cacheLoader = key -> nextInt(2) == 1
                        ? Value.of(key.getId(), format("load_%s_%s", System.currentTimeMillis(), key.getId()))
                        : null;
                return createDistributedLoadingCache(collectionName,
                        b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                .maximumSize(cacheSize)),
                        cacheLoader);
            };

            DistributedLoadingCache<Key, Value> distributedLoadingCache = cacheSupplier.get();

            Map<Key, Value> keyValueMap = IntStream.rangeClosed(1, cacheSize)
                    .boxed()
                    .collect(Collectors.toMap(Key::of, Value::of));
            distributedLoadingCache.putAll(keyValueMap);

            assertThat(countMongoStatus(distributedLoadingCache, CACHED)).isEqualTo(keyValueMap.size());

            AtomicBoolean loopCondition = new AtomicBoolean(true);
            AtomicInteger loopCounter = new AtomicInteger(10_000);

            CompletableFuture.runAsync(() -> {
                while (loopCondition.get()) {
                    executeRandomOperation(distributedLoadingCache, cacheSize);
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
                    .failFast(() -> cleanUp(distributedLoadingCache, syncedDistributedLoadingCache))
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedLoadingCache.asMap());
                        assertThat(countMongoStatus(distributedLoadingCache, CACHED))
                                .isEqualTo(distributedLoadingCache.estimatedSize());
                    });

            syncedDistributedLoadingCache.distributedPolicy().stopSynchronization();

            IntStream.rangeClosed(1, 10_000).forEach(operationIndex ->
                    executeRandomOperation(syncedDistributedLoadingCache, cacheSize));

            loopCondition.set(true);
            loopCounter.set(10_000);

            CompletableFuture.runAsync(() -> {
                while (loopCondition.get()) {
                    executeRandomOperation(distributedLoadingCache, cacheSize);
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
                    .failFast(() -> cleanUp(distributedLoadingCache, syncedDistributedLoadingCache))
                    .untilAsserted(() -> {
                        assertThat(distributedLoadingCache.asMap())
                                .containsExactlyInAnyOrderEntriesOf(syncedDistributedLoadingCache.asMap());
                        assertThat(countMongoStatus(distributedLoadingCache, CACHED))
                                .isEqualTo(distributedLoadingCache.estimatedSize());
                    });

            deleteMongoExpires(distributedLoadingCache);

            await("cache manager maintenance")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .untilAsserted(() ->
                            assertThat(isCacheManagerMaintained(distributedLoadingCache, syncedDistributedLoadingCache)).isTrue());
        }

        @DisplayName("Stress test with multiple threads")
        @Test
        void stress_test_DistributedCaffeine_multiple_threads() {
            final Duration EXTENDED_WAITING_DURATION = WAITING_DURATION.multipliedBy(100);
            final Duration EXTENDED_POLL_INTERVAL = Duration.ofSeconds(1);

            String collectionName = getCollectionNameWithSuffix("cacheStressMultiThread");
            int cacheSize = 100;

            Supplier<DistributedLoadingCache<Key, Value>> cacheSupplier = () -> {
                CacheLoader<Key, Value> cacheLoader = key -> nextInt(2) == 1
                        ? Value.of(key.getId(), format("load_%s_%s", System.currentTimeMillis(), key.getId()))
                        : null;
                return createDistributedLoadingCache(collectionName,
                        b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                .maximumSize(cacheSize)),
                        cacheLoader);
            };

            // first cache only watches passively
            DistributedLoadingCache<Key, Value> firstDistributedLoadingCache = cacheSupplier.get();
            List<DistributedLoadingCache<Key, Value>> distributedLoadingCaches =
                    new ArrayList<>(List.of(firstDistributedLoadingCache));
            List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

            IntStream.rangeClosed(1, 10).forEach(cacheIndex ->
                    completableFutures.add(CompletableFuture.runAsync(() -> {
                        await(Duration.ofMillis(1_000).multipliedBy(cacheIndex));
                        DistributedLoadingCache<Key, Value> distributedLoadingCache = cacheSupplier.get();
                        distributedLoadingCaches.add(distributedLoadingCache);
                        IntStream.rangeClosed(1, 10_000).forEach(operationIndex -> {
                            executeRandomOperation(distributedLoadingCache, cacheSize);
                            if (operationIndex == 1_000) {
                                distributedLoadingCache.distributedPolicy().stopSynchronization();
                                await(Duration.ofMillis(1_000).multipliedBy(cacheIndex));
                                distributedLoadingCache.distributedPolicy().startSynchronization();
                            }
                        });
                    }, executorService)));

            CompletableFuture.allOf(completableFutures.toArray(CompletableFuture[]::new)).join();

            long expiredCount = countMongoStatus(firstDistributedLoadingCache, INVALIDATED, EVICTED);

            await("synchronization between cache instances")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast(() -> cleanUp(distributedLoadingCaches.<DistributedCache<Key, Value>>toArray(DistributedCache[]::new)))
                    .untilAsserted(() -> {
                        IntStream.range(0, distributedLoadingCaches.size() - 1).forEach(i ->
                                assertThat(distributedLoadingCaches.get(i).asMap())
                                        .describedAs(() -> format("%s (%s) vs. %s (%s)",
                                                i, distributedLoadingCaches.get(i),
                                                i + 1, distributedLoadingCaches.get(i + 1)))
                                        .containsExactlyInAnyOrderEntriesOf(distributedLoadingCaches.get(i + 1).asMap()));
                        assertThat(countMongoStatus(firstDistributedLoadingCache, CACHED))
                                .isEqualTo(firstDistributedLoadingCache.estimatedSize());
                    });

            await("automatic deletion via expiresAt index")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .untilAsserted(() ->
                            assertThat(countMongoStatus(firstDistributedLoadingCache, INVALIDATED, EVICTED))
                                    .isLessThan(expiredCount));

            deleteMongoExpires(firstDistributedLoadingCache);

            await("cache manager maintenance")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .untilAsserted(() ->
                            assertThat(isCacheManagerMaintained(distributedLoadingCaches
                                    .<DistributedCache<Key, Value>>toArray(DistributedCache[]::new))).isTrue());

            distributedLoadingCaches.forEach(distributedCache ->
                    completableFutures.add(CompletableFuture
                            .runAsync(distributedCache::invalidateAll, executorService)));

            CompletableFuture.allOf(completableFutures.toArray(CompletableFuture[]::new)).join();

            await("synchronization between cache instances")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .failFast(() -> cleanUp(distributedLoadingCaches.<DistributedCache<Key, Value>>toArray(DistributedCache[]::new)))
                    .untilAsserted(() -> {
                        assertThatCollection(distributedLoadingCaches)
                                .allMatch(distributedCache -> distributedCache.estimatedSize() == 0);
                        assertThat(countMongoStatus(firstDistributedLoadingCache, CACHED))
                                .isEqualTo(firstDistributedLoadingCache.estimatedSize());
                    });

            deleteMongoExpires(firstDistributedLoadingCache);

            await("cache manager maintenance and empty collection")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .pollInterval(EXTENDED_POLL_INTERVAL)
                    .untilAsserted(() -> {
                        assertThat(isCacheManagerMaintained(distributedLoadingCaches
                                .<DistributedCache<Key, Value>>toArray(DistributedCache[]::new))).isTrue();
                        assertThat(countMongoStatus(firstDistributedLoadingCache)).isZero();
                    });
        }
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    abstract static class AbstractDistributedCaffeineTestBase {

        static final String LATEST_ONLY = "latestOnly";
        static final String DATABASE_NAME = "distributedCaffeineTestSuiteDatabase";
        static final Duration WAITING_DURATION = Duration.ofSeconds(5);

        MongoDBContainer mongoContainer;
        MongoClient mongoClient;
        MongoDatabase mongoDatabase;

        AtomicLong collectionNameSuffix;
        ExecutorService executorService;
        SecureRandom secureRandom;

        Collection<DistributedCache<?, ?>> distributedCacheInstances;

        @BeforeAll
        void beforeAll() {
            String tag = Optional.ofNullable(getClass().getAnnotation(Tag.class))
                    .map(Tag::value)
                    .orElseThrow();
            String displayName = Optional.ofNullable(getClass().getAnnotation(DisplayName.class))
                    .map(DisplayName::value)
                    .map(value -> value.replace(' ', '-'))
                    .map(String::toLowerCase)
                    .orElseThrow();
            DockerImageName dockerImageName = DockerImageName.parse(tag);

            mongoContainer = new MongoDBContainer(dockerImageName)
                    .withCreateContainerCmdModifier(cmd -> cmd.withName(displayName))
                    // images with latest tag should be pulled periodically to get the newest version
                    .withImagePullPolicy(new AbstractImagePullPolicy() {
                        @Override
                        protected boolean shouldPullCached(DockerImageName imageName, ImageData localImageData) {
                            Duration maxAge = Duration.ofDays(30);
                            boolean isLatest = !imageName.getVersionPart().contains(".");
                            boolean isOutdated = Instant.now().isAfter(localImageData.getCreatedAt().plus(maxAge));
                            return isLatest && isOutdated;
                        }
                    });
            mongoContainer.start();
            mongoClient = MongoClients.create(mongoContainer.getConnectionString());
            mongoDatabase = mongoClient.getDatabase(DATABASE_NAME);

            collectionNameSuffix = new AtomicLong(0);
            executorService = Executors.newCachedThreadPool(runnable ->
                    new Thread(runnable, Thread.class.getSimpleName()
                            .concat(DistributedCaffeineTest.class.getSimpleName())
                            .concat(String.valueOf(runnable.hashCode()))));
            secureRandom = new SecureRandom();
        }

        @AfterAll
        void afterAll() {
            mongoClient.close();
            mongoContainer.stop();
            executorService.shutdownNow();
        }

        @BeforeEach
        void beforeEach() {
            distributedCacheInstances = new HashSet<>();
        }

        @AfterEach
        void afterEach() {
            // stop synchronization (release database connections)
            distributedCacheInstances.forEach(distributedCache ->
                    distributedCache.distributedPolicy().stopSynchronization());
            // invalidate all cache entries (only after synchronization is already stopped for all caches)
            distributedCacheInstances.forEach(Cache::invalidateAll);
            distributedCacheInstances.clear();
        }

        Stream<Arguments> provideCacheBiFunctionsWithDifferentSerializers() {
            Stream<DistributedCaffeineConfiguration<Key, Value>> distributedCaffeineConfigurations = createDistributedCaffeineConfigurationWithDifferentSerializers();
            return createNamedCacheBiFunctionsForParametrizedTests(distributedCaffeineConfigurations)
                    .map(Arguments::of);
        }

        Stream<Arguments> provideCacheBiFunctionsWithDifferentDistributionModes() {
            Stream<DistributedCaffeineConfiguration<Key, Value>> distributedCaffeineConfigurations = createDistributedCaffeineConfigurationsWithDifferentDistributionModes();
            return createNamedCacheBiFunctionsForParametrizedTests(distributedCaffeineConfigurations)
                    .map(Arguments::of);
        }

        private Stream<DistributedCaffeineConfiguration<Key, Value>> createDistributedCaffeineConfigurationWithDifferentSerializers() {
            return Stream.of(
                    new DistributedCaffeineConfiguration<>(
                            "with Fury Serializer",
                            "cacheWithFurySerializer",
                            DistributedCaffeine.Builder::withFurySerializer),
                    new DistributedCaffeineConfiguration<>(
                            "with Fury Serializer (Class)",
                            "cacheWithFurySerializerClass",
                            b -> b.withCustomKeySerializer(new FurySerializer<>(Key.class))
                                    .withCustomValueSerializer(new FurySerializer<>(Value.class))),
                    new DistributedCaffeineConfiguration<>(
                            "with Java Object Serializer",
                            "cacheWithJavaObjectSerializer",
                            DistributedCaffeine.Builder::withJavaObjectSerializer),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (BSON, Class)",
                            "cacheWithJsonSerializerBsonClass",
                            b -> b.withJsonSerializer(Key.class, Value.class, true)),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (JSON, Class)",
                            "cacheWithJsonSerializerStringClass",
                            b -> b.withJsonSerializer(new ObjectMapper(), Key.class, Value.class, false)),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (BSON, TypeReference)",
                            "cacheWithJsonSerializerBsonTypeReference",
                            b -> b.withJsonSerializer(
                                    new TypeReference<>() {
                                    },
                                    new TypeReference<>() {
                                    },
                                    true)),
                    new DistributedCaffeineConfiguration<>(
                            "with Jackson Serializer (JSON, TypeReference)",
                            "cacheWithJsonSerializerStringTypeReference",
                            b -> b.withJsonSerializer(new ObjectMapper(),
                                    new TypeReference<>() {
                                    },
                                    new TypeReference<>() {
                                    },
                                    false))
            );
        }

        private Stream<DistributedCaffeineConfiguration<Key, Value>> createDistributedCaffeineConfigurationsWithDifferentDistributionModes() {
            return Stream.of(DistributionMode.values())
                    .map(distributionMode -> {
                        String distributionModeNameCamelCase = CaseFormatUtils.toCamelCase(distributionMode.name());
                        return new DistributedCaffeineConfiguration<>(
                                format("with %s.%s", distributionMode.getClass().getSimpleName(), distributionMode.name()),
                                format("cacheWith%s%s", distributionMode.getClass().getSimpleName(),
                                        distributionModeNameCamelCase.substring(0, 1).toUpperCase()
                                                .concat(distributionModeNameCamelCase.substring(1))),
                                b -> b.withDistributionMode(distributionMode));
                    });
        }

        private <K, V> Stream<Named<CacheBiFunction<K, V>>> createNamedCacheBiFunctionsForParametrizedTests(Stream<DistributedCaffeineConfiguration<K, V>> distributedCaffeineConfigurations) {
            return distributedCaffeineConfigurations
                    .map(distributedCaffeineConfiguration -> {
                        String collectionNameWithSuffix = getCollectionNameWithSuffix(distributedCaffeineConfiguration.getCollectionName());
                        CacheBiFunction<K, V> cacheBiFunction = (builderFunction, cacheLoader) -> {
                            List<BuilderFunction<K, V>> builderFunctions = Stream.of(builderFunction, distributedCaffeineConfiguration.getBuilderFunction())
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList());
                            return createCacheInstance(collectionNameWithSuffix, builderFunctions, cacheLoader);
                        };
                        return Named.of(distributedCaffeineConfiguration.getDisplayName(), cacheBiFunction);
                    });
        }

        private <K, V> DistributedCache<K, V> createCacheInstance(String collectionName, List<BuilderFunction<K, V>> builderFunctions, CacheLoader<K, V> cacheLoader) {
            DistributedCaffeine.Builder<K, V> distributedCaffeineBuilder = DistributedCaffeine.newBuilder(mongoDatabase.getCollection(collectionName));
            for (Function<DistributedCaffeine.Builder<K, V>, DistributedCaffeine.Builder<K, V>> builderFunction : builderFunctions) {
                distributedCaffeineBuilder = builderFunction.apply(distributedCaffeineBuilder);
            }
            DistributedCache<K, V> distributedCache = isNull(cacheLoader)
                    ? distributedCaffeineBuilder.build()
                    : distributedCaffeineBuilder.build(cacheLoader);
            distributedCacheInstances.add(distributedCache);
            return distributedCache;
        }

        <K, V> DistributedCache<K, V> createDistributedCache(String collectionName) {
            return createCacheInstance(collectionName, List.of(), null);
        }

        <K, V> DistributedCache<K, V> createDistributedCache(String collectionName, BuilderFunction<K, V> builderFunction) {
            return createCacheInstance(collectionName, List.of(builderFunction), null);
        }

        <K, V> DistributedLoadingCache<K, V> createDistributedLoadingCache(String collectionName, BuilderFunction<K, V> builderFunction, CacheLoader<K, V> cacheLoader) {
            return (DistributedLoadingCache<K, V>) createCacheInstance(collectionName, List.of(builderFunction), cacheLoader);
        }

        String getCollectionNameWithSuffix(String collectionName) {
            return format("%s_%05d", collectionName, collectionNameSuffix.incrementAndGet());
        }

        @SafeVarargs
        final <K, V> void cleanUp(DistributedCache<K, V>... distributedCaches) {
            // speed up assertions
            Stream.of(distributedCaches).forEach(distributedCache -> {
                distributedCache.cleanUp();
                InternalMaintenanceWorker<K, V> maintenanceWorker = getDistributedCaffeine(distributedCache).getMaintenanceWorker();
                PriorityBlockingQueue<ObjectId> toBeOrphaned = readFieldValue(maintenanceWorker, InternalMaintenanceWorker.class,
                        "toBeOrphaned", PriorityBlockingQueue.class);
                while (!toBeOrphaned.isEmpty()) {
                    invokeMethod(maintenanceWorker, InternalMaintenanceWorker.class,
                            "processMaintenance", List.of(), List.of());
                }
            });
        }

        @SafeVarargs
        final <K, V> boolean isCacheManagerMaintained(DistributedCache<K, V>... distributedCaches) {
            return Stream.of(distributedCaches)
                    .allMatch(distributedCache -> {
                        DistributedCaffeine<K, V> distributedCaffeine = getDistributedCaffeine(distributedCache);
                        boolean isCachedSupported = distributedCaffeine.isSupportedByDistributionMode(CACHED);
                        InternalCacheManager<K, V> cacheManager = distributedCaffeine.getCacheManager();
                        ConcurrentMap<?, ?> latest = readFieldValue(cacheManager, InternalCacheManager.class,
                                "latest", ConcurrentMap.class);
                        ConcurrentMap<?, ?> buffer = readFieldValue(cacheManager, InternalCacheManager.class,
                                "buffer", ConcurrentMap.class);
                        ConcurrentMap<?, ?> balance = readFieldValue(cacheManager, InternalCacheManager.class,
                                "balance", ConcurrentMap.class);
                        Set<?> ignore = readFieldValue(cacheManager, InternalCacheManager.class,
                                "ignore", Set.class);
                        return (isCachedSupported
                                ? latest.size() == distributedCache.estimatedSize()
                                : latest.isEmpty())
                                && buffer.isEmpty() && balance.isEmpty() && ignore.isEmpty();
                    });
        }

        <K, V> DistributedCaffeine<K, V> getDistributedCaffeine(DistributedCache<K, V> distributedCache) {
            return ((InternalDistributedCache<K, V>) distributedCache).distributedCaffeine;
        }

        <K, V> long countMongoStatus(DistributedCache<K, V> distributedCache, String... statuses) {
            Bson filter = isNull(statuses) || statuses.length == 0
                    ? Filters.empty()
                    : Filters.in(STATUS, statuses);
            return distributedCache.distributedPolicy().getMongoCollection().countDocuments(filter);
        }

        <K, V> void deleteMongoExpires(DistributedCache<K, V> distributedCache) {
            Bson filter = Filters.ne(EXPIRES, null);
            distributedCache.distributedPolicy().getMongoCollection().deleteMany(filter);
        }

        void executeRandomOperation(DistributedCache<Key, Value> distributedCache, int cacheSize) {
            List<Runnable> operations = listOperations(distributedCache, cacheSize);
            operations.get(nextInt(operations.size())).run();
        }

        private List<Runnable> listOperations(DistributedCache<Key, Value> distributedCache, int cacheSize) {
            long currentTimeMillis = System.currentTimeMillis();
            List<Runnable> operations = new ArrayList<>();
            operations.add(() -> {
                int addId = nextInt(cacheSize, 2 * cacheSize);
                distributedCache.put(Key.of(addId),
                        Value.of(addId, format("add_%s_%s", currentTimeMillis, addId)));
            });
            operations.add(() -> {
                int addId1 = nextInt(cacheSize, cacheSize + cacheSize / 2);
                int addId2 = nextInt(cacheSize + cacheSize / 2, 2 * cacheSize);
                distributedCache.putAll(Map.of(
                        Key.of(addId1),
                        Value.of(addId1, format("add_%s_%s", currentTimeMillis, addId1)),
                        Key.of(addId2),
                        Value.of(addId2, format("add_%s_%s", currentTimeMillis, addId2))));
            });
            operations.add(() -> {
                int updateId = nextInt(cacheSize);
                distributedCache.put(Key.of(updateId),
                        Value.of(updateId, format("update_%s_%s", currentTimeMillis, updateId)));
            });
            operations.add(() -> {
                int updateId1 = nextInt(0, cacheSize / 2);
                int updateId2 = nextInt(cacheSize / 2, cacheSize);
                distributedCache.putAll(Map.of(
                        Key.of(updateId1),
                        Value.of(updateId1, format("update_%s_%s", currentTimeMillis, updateId1)),
                        Key.of(updateId2),
                        Value.of(updateId2, format("update_%s_%s", currentTimeMillis, updateId2))));
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
            if (distributedCache instanceof DistributedLoadingCache) {
                DistributedLoadingCache<Key, Value> distributedLoadingCache = (DistributedLoadingCache<Key, Value>) distributedCache;
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

        void await(Duration duration) {
            await("duration")
                    .pollInterval(duration)
                    .timeout(duration.plusSeconds(1)) // timeout must be greater than the poll delay
                    .until(() -> true);
        }

        ConditionFactory await(String alias) {
            return Awaitility.await(alias)
                    .pollExecutorService(executorService);
        }

        @SuppressWarnings("unused")
        <K, V> void printMongoCollection(DistributedCache<K, V> distributedCache, Bson filter) {
            AtomicInteger counter = new AtomicInteger();
            distributedCache.distributedPolicy().getMongoCollection().find()
                    .filter(isNull(filter) ? Filters.empty() : filter)
                    .sort(Sorts.orderBy(Sorts.ascending(TOUCHED)))
                    .forEach(document -> System.out.printf("%07d %s%n", counter.incrementAndGet(), document));
        }

        @SuppressWarnings("unchecked")
        <T, R> R readFieldValue(Object instanceObject, Class<T> instanceClass, String fieldName, Class<? super R> fieldClass) {
            return (R) ReflectionUtils.tryToReadFieldValue(instanceClass, fieldName, instanceClass.cast(instanceObject))
                    .toOptional()
                    .filter(fieldClass::isInstance)
                    .orElseThrow();
        }

        <T> void writeFieldValue(Object instanceObject, Class<T> instanceClass, String fieldName, Object fieldValue) {
            Predicate<Field> fieldPredicate = field -> field.getName().equals(fieldName);
            Field field = ReflectionUtils.streamFields(instanceClass, fieldPredicate, HierarchyTraversalMode.TOP_DOWN)
                    .findFirst()
                    .orElseThrow();
            ReflectionUtils.makeAccessible(field);
            try {
                field.set(instanceObject, fieldValue);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        <T, R> R invokeMethod(Object instanceObject, Class<T> instanceClass, String methodName, List<Class<?>> parameterClasses, List<Object> parameterObjects) {
            return (R) ReflectionUtils.invokeMethod(
                    ReflectionUtils.findMethod(instanceClass, methodName, parameterClasses.toArray(Class[]::new))
                            .orElseThrow(NoSuchMethodError::new),
                    instanceObject, parameterObjects.toArray(Object[]::new));
        }

        interface BuilderFunction<K, V> extends Function<DistributedCaffeine.Builder<K, V>, DistributedCaffeine.Builder<K, V>> {

            @Override
            DistributedCaffeine.Builder<K, V> apply(DistributedCaffeine.Builder<K, V> builder);
        }

        interface CacheBiFunction<K, V> extends BiFunction<BuilderFunction<K, V>, CacheLoader<K, V>, DistributedCache<K, V>> {

            @Override
            DistributedCache<K, V> apply(BuilderFunction<K, V> builderFunction, CacheLoader<K, V> cacheLoader);
        }

        static class DistributedCaffeineConfiguration<K, V> {

            private final String displayName;
            private final String collectionName;
            private final BuilderFunction<K, V> builderFunction;

            DistributedCaffeineConfiguration(String displayName, String collectionName, BuilderFunction<K, V> builderFunction) {
                this.displayName = displayName;
                this.collectionName = collectionName;
                this.builderFunction = builderFunction;
            }

            String getDisplayName() {
                return displayName;
            }

            String getCollectionName() {
                return collectionName;
            }

            BuilderFunction<K, V> getBuilderFunction() {
                return builderFunction;
            }
        }
    }
}
