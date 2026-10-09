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
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import com.mongodb.ConnectionString;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.CachedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedPolicy.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractSynchronizer;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntryMetadata;
import io.github.oberhoff.distributedcaffeine.adapter.DiscriminatorAware;
import io.github.oberhoff.distributedcaffeine.adapter.Publisher;
import io.github.oberhoff.distributedcaffeine.adapter.Receiver;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.adapter.Repository.Order;
import io.github.oberhoff.distributedcaffeine.adapter.Synchronizer;
import io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoAdapter.WatcherSharingMode;
import io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresAdapter.ListenerSharingMode;
import io.github.oberhoff.distributedcaffeine.common.DistributedCaffeineCommonTestInstance;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import io.github.oberhoff.distributedcaffeine.common.logging.CaptureLogger;
import io.github.oberhoff.distributedcaffeine.common.logging.CaptureLoggerFactory;
import io.github.oberhoff.distributedcaffeine.serializer.ForySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JavaObjectSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
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
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.core.type.TypeReference;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer.LoadingStrategy.CACHE_LOADER;
import static io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer.LoadingStrategy.MAPPING_FUNCTION;
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
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.DISTRIBUTION_ONLY_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_RETAINED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE_RETAINED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME_RETAINED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED_REFRESHED_AFTER_WRITE;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.STALE;
import static io.github.oberhoff.distributedcaffeine.adapter.DiscriminatorAware.DEFAULT_DISCRIMINATOR;
import static io.github.oberhoff.distributedcaffeine.adapter.Repository.Order.ASCENDING;
import static io.github.oberhoff.distributedcaffeine.adapter.Repository.Order.DESCENDING;
import static io.github.oberhoff.distributedcaffeine.adapter.Repository.Order.UNORDERED;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.time.temporal.ChronoUnit.FOREVER;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
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
import static org.mockito.Mockito.when;

@DisplayName("Distributed Caffeine Integration Test Suite")
final class DistributedCaffeineIntegrationTests {

    @Nested
    @DisplayName("MongoDB 4.2.0")
    @DockerImage("mongo:4.2.0") // oldest version supported by the latest driver
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_4_2_0 extends MongoIntegration {
    }

    @Nested
    @DisplayName("MongoDB 4.latest")
    @DockerImage("mongo:4")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_4_latest extends MongoIntegration {
    }

    @Nested
    @DisplayName("MongoDB 5.0.0")
    @DockerImage("mongo:5.0.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_5_0_0 extends MongoIntegration {
    }

    @Nested
    @DisplayName("MongoDB 5.latest")
    @DockerImage("mongo:5")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_5_latest extends MongoIntegration {
    }

    @Nested
    @DisplayName("MongoDB 6.0.1")
    @DockerImage("mongo:6.0.1") // mongo:6.0.0 is not available
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_6_0_1 extends MongoIntegration {
    }

    @Nested
    @DisplayName("MongoDB 6.latest")
    @DockerImage("mongo:6")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_6_latest extends MongoIntegration {
    }

    @Nested
    @DisplayName("MongoDB 7.0.0")
    @DockerImage("mongo:7.0.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_7_0_0 extends MongoIntegration {
    }

    @Nested
    @DisplayName("MongoDB 7.latest")
    @DockerImage("mongo:7")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_7_latest extends MongoIntegration {
    }

    @Nested
    @DisplayName("MongoDB 8.0.0")
    @DockerImage("mongo:8.0.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_8_0_0 extends MongoIntegration {
    }

    @Nested
    @DisplayName("MongoDB 8.latest")
    @DockerImage("mongo:8")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_8_latest extends MongoIntegration {
    }

    @Nested
    @DisplayName("MongoDB 9.0.2")
    @DockerImage("mongo:9.0.2") // mongo:9.0.0 is not available
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Mongo_9_0_2 extends MongoIntegration {
    }

    @Nested
    @DisplayName("MongoDB 9.latest")
    @DockerImage("mongo:9")
    final class Mongo_9_latest extends MongoIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 9.5.25")
    @DockerImage("postgres:9.5.25") // oldest version offering all features needed
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_9_5_25 extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 9.latest")
    @DockerImage("postgres:9")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_9_latest extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 10.0")
    @DockerImage("postgres:10.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_10_0 extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 10.latest")
    @DockerImage("postgres:10")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_10_latest extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 11.0")
    @DockerImage("postgres:11.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_11_0 extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 11.latest")
    @DockerImage("postgres:11")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_11_latest extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 12.0")
    @DockerImage("postgres:12.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_12_0 extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 12.latest")
    @DockerImage("postgres:12")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_12_latest extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 13.0")
    @DockerImage("postgres:13.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_13_0 extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 13.latest")
    @DockerImage("postgres:13")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_13_latest extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 14.0")
    @DockerImage("postgres:14.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_14_0 extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 14.latest")
    @DockerImage("postgres:14")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_14_latest extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 15.0")
    @DockerImage("postgres:15.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_15_0 extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 15.latest")
    @DockerImage("postgres:15")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_15_latest extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 16.0")
    @DockerImage("postgres:16.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_16_0 extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 16.latest")
    @DockerImage("postgres:16")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_16_latest extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 17.0")
    @DockerImage("postgres:17.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_17_0 extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 17.latest")
    @DockerImage("postgres:17")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_17_latest extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 18.0")
    @DockerImage("postgres:18.0")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    final class Postgres_18_0 extends PostgresIntegration {
    }

    @Nested
    @DisplayName("PostgreSQL 18.latest")
    @DockerImage("postgres:18")
    final class Postgres_18_latest extends PostgresIntegration {
    }

    @SuppressWarnings({"java:S5838", "java:S5778", "java:S5961", "ResultOfMethodCallIgnored", "DataFlowIssue"})
    abstract static class CommonIntegration extends DistributedCaffeineIntegrationTestInstance {
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
                public Value load(@NonNull Key key) {
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

        @DisplayName("Test that starting synchronization recovers a partially activated cache instance")
        @Test
        void test_DistributedCaffeine_starting_synchronization_recovers_partial_activation() {
            // The check guarding activation requires all three components, so a cache instance whose adapter was
            // stopped on its own - which its public API allows - no longer counts as activated and starting
            // synchronization runs the whole body again, over components that never stopped. Activating the
            // maintenance worker a second time joins the worker future of the first activation, which completes
            // only once that worker is deactivated, so it waits for something that cannot happen while it is
            // holding the synchronization lock - and with that lock held no cache operation of this instance can
            // proceed either.
            // Started on a separate thread for exactly that reason: waiting for it here would park the thread
            // running this test inside that lock, and nothing after it - including tearing the instance down
            // again - could run.
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            InternalInstanceRegistry<Key, Value> instanceRegistry = getInstanceRegistry(distributedCache);
            InternalMaintenanceWorker<Key, Value> maintenanceWorker = instanceRegistry.getMaintenanceWorker();

            assertThat(instanceRegistry.isActivated()).isTrue();

            instanceRegistry.getAdapter().deactivate();

            // the state this is about, asserted rather than assumed: partially activated, so the check guarding
            // activation lets the body through while the maintenance worker is still running
            assertThat(instanceRegistry.isActivated()).isFalse();
            assertThat(maintenanceWorker.isActivated()).isTrue();

            CompletableFuture<Void> startSynchronization = CompletableFuture.runAsync(() ->
                    distributedCache.distributedPolicy().startSynchronization());

            try {
                assertThat(startSynchronization).succeedsWithin(WAITING_DURATION);
                assertThat(instanceRegistry.isActivated()).isTrue();
            } finally {
                // whatever the outcome above, the thread must not stay parked in the lock: deactivating cancels
                // the very future it is joining, which fails that activation and unwinds it - which is also what
                // its rollback is for, so the instance is left stopped rather than half started
                maintenanceWorker.deactivate();
                await("release of the parked activation")
                        .atMost(WAITING_DURATION)
                        .untilAsserted(() -> assertThat(startSynchronization).isDone());
            }
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

        @DisplayName("Test replace() and replaceAll()")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentSerializers")
        void test_ConcurrentMap_replace_replaceAll(CacheFactory<Key, Value> cacheFactory) {
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
            // replaceAll derives each new value from the one in place, so what it leaves behind still witnesses
            // what replace() put there
            Value replacedAllValue1 = Value.of(1, "replaced all");
            Value replacedAllValue2 = Value.of(2, "replaced all");

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
                assertThatNullPointerException().isThrownBy(() -> map.replaceAll(_null()));

                replacedValue1x1.setValue(map.replace(key1, value1));
                replacedBool2x1.setObject(map.replace(key2, toBeReplacedValue, value2));
                map.put(key1, toBeReplacedValue);
                map.put(key2, toBeReplacedValue);
                replacedValue1x2.setValue(map.replace(key1, value1));
                replacedBool2x2.setObject(map.replace(key2, toBeReplacedValue, value2));
                replacedBool2x3.setObject(map.replace(key2, toBeReplacedValue, value2));

                // the one mutator of the map view that is NOT overridden: it comes from ConcurrentMap's default,
                // which iterates the entry set and calls replace() for each entry - so it takes the
                // synchronization lock once per entry and distributes each replacement on its own, unlike
                // removeAll and retainAll beside it, which take the lock once and invalidate as a set
                map.replaceAll((key, value) -> Value.of(value.getId(), "replaced all"));
            });

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        allMaps.forEach(map -> {
                            assertThat(map).hasSize(2);
                            assertThat(map.get(key1)).isEqualTo(replacedAllValue1)
                                    .isNotEqualTo(replacedValue1x1.getValue())
                                    .isNotEqualTo(replacedValue1x2.getValue());
                            assertThat(replacedValue1x1.getValue()).isNull();
                            assertThat(replacedBool2x1.<Boolean>getObject()).isFalse();
                            assertThat(map.get(key2)).isEqualTo(replacedAllValue2);
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
        @SuppressWarnings("EqualsIncompatibleType")
            // comparing unrelated types is the point of the equals() contract test
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
                    repositoryOf(distributedCache).streamCacheEntries(null, CACHED_GROUP, UNORDERED))) {
                population = cacheEntries.findFirst().orElseThrow();
            }

            distributedCache.invalidate(key);

            assertThat(distributedCache.getIfPresent(key)).isNull();

            getInstanceRegistry(distributedCache).getCacheManager()
                    .receiveCacheEntries(List.of(population));

            assertThat(distributedCache.getIfPresent(key)).isNull();
        }

        @DisplayName("Test that a population is not reverted by a delayed cache entry of another cache instance")
        @Test
        void test_DistributionMode_population_is_not_reverted_by_delayed_foreign_cache_entry() {
            // A cache entry of another cache instance, written before this one populated the key and delivered
            // after it. Nothing it carries says which of the two came first - its operation belongs to another
            // cache instance and is not comparable here - so the one thing that can decide it is that this cache
            // instance has not seen its own population come back yet.
            // The echo is kept away rather than raced against: the receiver of the synchronizer is stubbed out, so
            // what the change stream delivers never reaches the cache and the population stays unconfirmed
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withDistributionMode(POPULATION_AND_INVALIDATION),
                    DistributedCaffeine::build);

            Key key = Key.of(1);
            Value staleValue = Value.of(1, "stale");
            Value value = Value.of(1);

            Adapter<Key, Value> adapter = getInstanceRegistry(distributedCache).getAdapter();
            Synchronizer<Key, Value> synchronizer = readFieldValue(adapter, AbstractAdapter.class,
                    "synchronizer", Synchronizer.class);
            Receiver<Key, Value> receiver = injectSpy(synchronizer, AbstractSynchronizer.class,
                    "receiver", Receiver.class);
            doNothing().when(receiver).receiveCacheEntries(anyList());

            distributedCache.put(key, value);

            // the hash of the cache entry written for the population, so the delayed one addresses the same key
            String hash;
            try (Stream<CacheEntry<Key, Value>> cacheEntries = getFailable(() ->
                    repositoryOf(distributedCache).streamCacheEntries(null, CACHED_GROUP, UNORDERED))) {
                hash = cacheEntries.findFirst().orElseThrow().getHash();
            }

            // the cache entry written for the population, to be handed over as its echo afterwards
            CacheEntry<Key, Value> population;
            try (Stream<CacheEntry<Key, Value>> cacheEntries = getFailable(() ->
                    repositoryOf(distributedCache).streamCacheEntries(null, CACHED_GROUP, UNORDERED))) {
                population = cacheEntries.findFirst().orElseThrow();
            }

            CacheEntry<Key, Value> foreign = CacheEntry.of(
                    hash,
                    "otherCacheInstance:1",
                    key,
                    staleValue,
                    CACHED,
                    Instant.now());

            getInstanceRegistry(distributedCache).getCacheManager()
                    .receiveCacheEntries(List.of(foreign));

            // the delayed cache entry takes the key while the population is still unconfirmed
            assertThat(distributedCache.getIfPresent(key)).isEqualTo(staleValue);

            getInstanceRegistry(distributedCache).getCacheManager()
                    .receiveCacheEntries(List.of(population));

            // and the echo of the population restores it, so the divergence lasts until the echo arrives
            assertThat(distributedCache.getIfPresent(key)).isEqualTo(value);
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
                    repositoryOf(distributedCache).streamCacheEntries(null, CACHED_GROUP, UNORDERED))) {
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
                    repository.countCacheEntries(EVICTED_RETAINED_GROUP, null));
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
                    repository.countCacheEntries(CACHED_GROUP, null));
            long evictedCountBeforeMaintenance = getFailable(() ->
                    repository.countCacheEntries(EVICTED_RETAINED_GROUP, null));
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

        @DisplayName("Test persistence of evicted entries by size")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_EvictedEntryPersistence_by_size(CacheFactory<Key, Value> cacheFactory) throws Exception {
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
                                            .withMaximumTime(FOREVER.getDuration())));
            // deliberately without a loading strategy: what this test pins is what the underlying store retains,
            // while reading it back again is what the loading strategy tests below are for

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
        }

        @DisplayName("Test that concurrent maintenance does not prune evicted entries below their maximum size")
        @Test
            // Every cache instance runs its own maintenance against the same records, so two runs overlapping is the
            // ordinary case rather than a corner of it. Pruning that counts first and then selects how many it counted
            // beyond the maximum prunes once per run: the second one still acts on the first one's count
        void test_EvictedEntryPersistence_by_size_with_concurrent_maintenance() throws Exception {
            int retainedMaximumSize = 2;
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder().maximumSize(1))
                            .withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumSize(retainedMaximumSize))),
                    DistributedCaffeine::build);

            // the own echo restoring an evicted key would move the counts this test is about
            Adapter<Key, Value> adapter = getInstanceRegistry(distributedCache).getAdapter();
            Synchronizer<Key, Value> synchronizer = readFieldValue(adapter, AbstractAdapter.class,
                    "synchronizer", Synchronizer.class);
            Receiver<Key, Value> receiver = injectSpy(synchronizer, AbstractSynchronizer.class,
                    "receiver", Receiver.class);
            doNothing().when(receiver).receiveCacheEntries(anyList());

            // apart in time, because pruning keeps whatever shares the timestamp it cuts at - evictions within the
            // same millisecond would leave more than the maximum behind by design, which is not what this is about
            for (int id = 1; id <= 4; id++) {
                distributedCache.put(Key.of(id), Value.of(id));
                distributedCache.cleanUp();
                sleep(Duration.ofMillis(10));
            }

            await("eviction")
                    .atMost(WAITING_DURATION)
                    .failFast("process clean up", this::cleanUp)
                    .untilAsserted(() -> assertThatDataStoreHasCounts(
                            CountGrouped.of(CACHED_GROUP, assertion -> assertion.isEqualTo(1)),
                            CountGrouped.of(EVICTED_RETAINED_GROUP, assertion -> assertion.isEqualTo(3))));

            // the second run starts its pruning between the first one's count and its selection
            InternalMaintenanceWorker<Key, Value> maintenanceWorker =
                    getInstanceRegistry(distributedCache).getMaintenanceWorker();
            Repository<Key, Value> repository = injectSpy(maintenanceWorker, InternalMaintenanceWorker.class,
                    "repository", Repository.class);
            AtomicBoolean interleaved = new AtomicBoolean(false);
            doAnswer(invocation -> {
                Object count = invocation.callRealMethod();
                if (interleaved.compareAndSet(false, true)) {
                    invokeMethod(maintenanceWorker, InternalMaintenanceWorker.class,
                            "processEvictedEntryPersistenceBySize", List.of(), List.of());
                }
                return count;
            }).when(repository).countCacheEntries(EVICTED_RETAINED_GROUP, null);

            invokeMethod(maintenanceWorker, InternalMaintenanceWorker.class,
                    "processEvictedEntryPersistenceBySize", List.of(), List.of());

            assertThat(interleaved).isTrue();
            assertThatDataStoreHasCounts(
                    CountGrouped.of(CACHED_GROUP, assertion -> assertion.isEqualTo(1)),
                    CountGrouped.of(EVICTED_RETAINED_GROUP, assertion -> assertion.isEqualTo(retainedMaximumSize)));
        }

        @DisplayName("Test that pruning evicted entries by size keeps exactly the maximum when timestamps tie at the cut")
        @Test
            // Equal timestamps are routine - a store keeping milliseconds only, a transition stamping everything it
            // changes alike - and their order among each other is unspecified, so which of them stay must not
            // depend on it: pruning again, as the next run of any cache instance does, has to change nothing
        void test_EvictedEntryPersistence_by_size_with_timestamps_tied_at_the_cut() throws Exception {
            int retainedMaximumSize = 2;
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder().maximumSize(1))
                            .withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumSize(retainedMaximumSize))),
                    DistributedCaffeine::build);
            Repository<Key, Value> repository = repositoryOf(distributedCache);

            // the newest one, three sharing the timestamp the cut falls on, and an older one
            Instant cut = Instant.now().truncatedTo(MILLIS);
            repository.publishCacheEntries(List.of(
                    CacheEntry.of("h5", null, Key.of(5), Value.of(5), EVICTED_SIZE_RETAINED, cut.plusSeconds(1)),
                    CacheEntry.of("h4", null, Key.of(4), Value.of(4), EVICTED_SIZE_RETAINED, cut),
                    CacheEntry.of("h3", null, Key.of(3), Value.of(3), EVICTED_SIZE_RETAINED, cut),
                    CacheEntry.of("h2", null, Key.of(2), Value.of(2), EVICTED_SIZE_RETAINED, cut),
                    CacheEntry.of("h1", null, Key.of(1), Value.of(1), EVICTED_SIZE_RETAINED, cut.minusSeconds(1))));

            InternalMaintenanceWorker<Key, Value> maintenanceWorker =
                    getInstanceRegistry(distributedCache).getMaintenanceWorker();
            for (int run = 1; run <= 2; run++) {
                invokeMethod(maintenanceWorker, InternalMaintenanceWorker.class,
                        "processEvictedEntryPersistenceBySize", List.of(), List.of());

                try (Stream<CacheEntryMetadata> retained =
                             repository.streamCacheEntryMetadata(null, EVICTED_RETAINED_GROUP, UNORDERED)) {
                    // the newest one, and of those tied the one first by hash
                    assertThat(retained.map(CacheEntryMetadata::getHash))
                            .as("retained after run %d", run)
                            .containsExactlyInAnyOrder("h5", "h2");
                }
            }
        }

        @DisplayName("Test that retained evicted entries are not reloaded without a loading strategy")
        @Test
            // No loading strategy is the default - loadingStrategies starts out empty - so a retained evicted entry
            // stays in the store and is reached only through getFromStore or getAllFromStore. Asking the cache for it
            // loads it afresh instead, which the value says: what the loader returns is stamped
        void test_EvictedEntryPersistence_without_a_loading_strategy_does_not_read_the_store() throws Exception {
            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<Key, Value>() {
                @Override
                public Value load(@NonNull Key key) {
                    return Value.of(key.getId(), "loaded");
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCache =
                    (DistributedLoadingCache<Key, Value>) this.<Key, Value>createCache(
                            dc -> dc.withCaffeine(Caffeine.newBuilder().maximumSize(1))
                                    .withPersistence(configurer -> configurer
                                            .withEvictedEntries(evictedEntries -> evictedEntries
                                                    .withMaximumSize(10))),
                            dc -> dc.build(cacheLoader));
            Key key = Key.of(1);

            // evicted by taking the room away rather than by writing until something gives, so which entry goes
            // is not Caffeine's admission decision to make
            distributedLoadingCache.put(key, Value.of(1, "written"));
            distributedLoadingCache.policy().eviction().orElseThrow().setMaximum(0);
            distributedLoadingCache.cleanUp();

            await("the eviction being retained in the store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(distributedLoadingCache.distributedPolicy()
                            .getFromStore(key, true))
                            .isNotNull()
                            .extracting(CacheEntry::getStatus)
                            .isEqualTo(EVICTED_SIZE_RETAINED));

            assertThat(distributedLoadingCache.get(key))
                    .as("loaded afresh rather than taken from the store")
                    .isEqualTo(Value.of(1, "loaded"));
            verify(cacheLoader, times(1)).load(key);
        }

        @DisplayName("Test that the mapping function strategy reads the store first and maps only what it misses")
        @Test
            // The same order the cache loader strategy states, for the entry point a cache built without a cache
            // loader has instead - and the only one it has, so without this strategy its retained evicted cache
            // entries are reachable through getFromStore alone
        void test_EvictedEntryPersistence_with_the_mapping_function_strategy_reads_the_store_first() {
            @SuppressWarnings({"Convert2Lambda", "java:S9357"})
            Function<Key, Value> mappingFunction = spy(new Function<Key, Value>() {
                @Override
                public Value apply(Key key) {
                    return Value.of(key.getId(), "mapped");
                }
            });

            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder().maximumSize(1))
                            .withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumSize(10)
                                            .withLoadingStrategies(MAPPING_FUNCTION))),
                    DistributedCaffeine::build);
            Key key = Key.of(1);

            // evicted by taking the room away rather than by writing until something gives, so which entry goes
            // is not Caffeine's admission decision to make
            distributedCache.put(key, Value.of(1, "written"));
            distributedCache.policy().eviction().orElseThrow().setMaximum(0);
            distributedCache.cleanUp();

            await("the eviction being retained in the store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(distributedCache.distributedPolicy()
                            .getFromStore(key, true))
                            .isNotNull()
                            .extracting(CacheEntry::getStatus)
                            .isEqualTo(EVICTED_SIZE_RETAINED));

            assertThat(distributedCache.get(key, mappingFunction))
                    .as("the value that was written, so it came back from the store")
                    .isEqualTo(Value.of(1, "written"));
            verifyNoInteractions(mappingFunction);

            // and the fallback: a key the store never held is what the mapping function is actually for
            Key unknown = Key.of(99);
            assertThat(distributedCache.get(unknown, mappingFunction)).isEqualTo(Value.of(99, "mapped"));
            verify(mappingFunction, times(1)).apply(unknown);
        }

        @DisplayName("Test that the mapping function strategy applies a bulk mapping function to the remainder only")
        @Test
            // getAll is read in one go just like loadAll is: the mapping function sees what the store could not
            // supply and is not applied at all when that leaves nothing, which is what the second read asserts by
            // the function being untouched while both values still arrive
        void test_EvictedEntryPersistence_with_the_mapping_function_strategy_maps_the_remainder() {
            @SuppressWarnings({"Convert2Lambda", "java:S9357"})
            Function<Set<? extends Key>, Map<Key, Value>> mappingFunction =
                    spy(new Function<Set<? extends Key>, Map<Key, Value>>() {
                        @Override
                        public Map<Key, Value> apply(Set<? extends Key> keys) {
                            Map<Key, Value> keyToValue = new LinkedHashMap<>();
                            keys.forEach(key -> keyToValue.put(key, Value.of(key.getId(), "mapped")));
                            return keyToValue;
                        }
                    });

            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder().maximumSize(10))
                            .withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumSize(10)
                                            .withLoadingStrategies(MAPPING_FUNCTION))),
                    DistributedCaffeine::build);
            Key key1 = Key.of(1);
            Key key2 = Key.of(2);
            Key unknown = Key.of(99);

            // evicted by taking the room away rather than by writing until something gives, so which entries go
            // is not Caffeine's admission decision to make
            distributedCache.put(key1, Value.of(1, "written"));
            distributedCache.put(key2, Value.of(2, "written"));
            distributedCache.policy().eviction().orElseThrow().setMaximum(0);
            distributedCache.cleanUp();
            await("the evictions being retained in the store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> Stream.of(key1, key2).forEach(key ->
                            assertThat(distributedCache.distributedPolicy()
                                    .getFromStore(key, true))
                                    .isNotNull()
                                    .extracting(CacheEntry::getStatus)
                                    .isEqualTo(EVICTED_SIZE_RETAINED)));

            assertThat(distributedCache.getAll(List.of(key1, key2, unknown), mappingFunction))
                    .as("the two the store held plus the one it did not")
                    .isEqualTo(Map.of(
                            key1, Value.of(1, "written"),
                            key2, Value.of(2, "written"),
                            unknown, Value.of(99, "mapped")));
            verify(mappingFunction, times(1)).apply(Set.of(unknown));

            // what the read above put back was evicted again right away, so the store holds the same two once more
            distributedCache.cleanUp();
            await("the evictions being retained in the store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> Stream.of(key1, key2).forEach(key ->
                            assertThat(distributedCache.distributedPolicy()
                                    .getFromStore(key, true))
                                    .isNotNull()
                                    .extracting(CacheEntry::getStatus)
                                    .isEqualTo(EVICTED_SIZE_RETAINED)));

            assertThat(distributedCache.getAll(List.of(key1, key2), mappingFunction))
                    .isEqualTo(Map.of(key1, Value.of(1, "written"), key2, Value.of(2, "written")));
            verifyNoMoreInteractions(mappingFunction);
        }

        @DisplayName("Test that the mapping function strategy does not reach the computing methods of the map view")
        @Test
        void test_EvictedEntryPersistence_with_the_mapping_function_strategy_does_not_reach_the_map_view() {
            // The map view answers from the content of that map alone, which is both its own contract and what
            // Caffeine does with a cache loader - so the function is applied to a key the store holds, and the
            // value says which of the two it came from
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withCaffeine(Caffeine.newBuilder().maximumSize(1))
                            .withPersistence(configurer -> configurer
                                    .withEvictedEntries(evictedEntries -> evictedEntries
                                            .withMaximumSize(10)
                                            .withLoadingStrategies(MAPPING_FUNCTION))),
                    DistributedCaffeine::build);
            Key key = Key.of(1);

            // evicted by taking the room away rather than by writing until something gives, so which entry goes
            // is not Caffeine's admission decision to make
            distributedCache.put(key, Value.of(1, "written"));
            distributedCache.policy().eviction().orElseThrow().setMaximum(0);
            distributedCache.cleanUp();
            await("the eviction being retained in the store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(distributedCache.distributedPolicy()
                            .getFromStore(key, true))
                            .isNotNull()
                            .extracting(CacheEntry::getStatus)
                            .isEqualTo(EVICTED_SIZE_RETAINED));

            assertThat(distributedCache.asMap().computeIfAbsent(key, k -> Value.of(k.getId(), "computed")))
                    .as("computed afresh rather than taken from the store")
                    .isEqualTo(Value.of(1, "computed"));
        }

        @DisplayName("Test that the cache loader strategy reads the store first and loads only what it misses")
        @Test
            // The documented order, stated here rather than left to a loader invocation count that does not move:
            // the cache loader is invoked only for what the store could not supply
        void test_EvictedEntryPersistence_with_the_cache_loader_strategy_reads_the_store_first() throws Exception {
            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<Key, Value>() {
                @Override
                public Value load(@NonNull Key key) {
                    return Value.of(key.getId(), "loaded");
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCache =
                    (DistributedLoadingCache<Key, Value>) this.<Key, Value>createCache(
                            dc -> dc.withCaffeine(Caffeine.newBuilder().maximumSize(1))
                                    .withPersistence(configurer -> configurer
                                            .withEvictedEntries(evictedEntries -> evictedEntries
                                                    .withMaximumSize(10)
                                                    .withLoadingStrategies(CACHE_LOADER))),
                            dc -> dc.build(cacheLoader));
            Key key = Key.of(1);

            // evicted by taking the room away rather than by writing until something gives, so which entry goes
            // is not Caffeine's admission decision to make
            distributedLoadingCache.put(key, Value.of(1, "written"));
            distributedLoadingCache.policy().eviction().orElseThrow().setMaximum(0);
            distributedLoadingCache.cleanUp();

            await("the eviction being retained in the store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(distributedLoadingCache.distributedPolicy()
                            .getFromStore(key, true))
                            .isNotNull()
                            .extracting(CacheEntry::getStatus)
                            .isEqualTo(EVICTED_SIZE_RETAINED));

            assertThat(distributedLoadingCache.get(key))
                    .as("the value that was written, so it came back from the store")
                    .isEqualTo(Value.of(1, "written"));
            verifyNoInteractions(cacheLoader);

            // and the fallback: a key the store never held is what the loader is actually for
            Key unknown = Key.of(99);
            assertThat(distributedLoadingCache.get(unknown)).isEqualTo(Value.of(99, "loaded"));
            verify(cacheLoader, times(1)).load(unknown);
        }

        @DisplayName("Test that the cache loader strategy loads only the remainder of a bulk load")
        @Test
            // the bulk counterpart, which getAll takes through loadAll: the store is read once for the whole set
            // and the cache loader is invoked for what it could not supply - or not at all when that leaves nothing
        void test_EvictedEntryPersistence_with_the_cache_loader_strategy_loads_the_remainder() throws Exception {
            @SuppressWarnings("Convert2Lambda")
            CacheLoader<Key, Value> cacheLoader = spy(new CacheLoader<Key, Value>() {
                @Override
                public Value load(@NonNull Key key) {
                    return Value.of(key.getId(), "loaded");
                }
            });

            DistributedLoadingCache<Key, Value> distributedLoadingCache =
                    (DistributedLoadingCache<Key, Value>) this.<Key, Value>createCache(
                            dc -> dc.withCaffeine(Caffeine.newBuilder().maximumSize(10))
                                    .withPersistence(configurer -> configurer
                                            .withEvictedEntries(evictedEntries -> evictedEntries
                                                    .withMaximumSize(10)
                                                    .withLoadingStrategies(CACHE_LOADER))),
                            dc -> dc.build(cacheLoader));
            Key key1 = Key.of(1);
            Key key2 = Key.of(2);
            Key unknown = Key.of(99);

            // evicted by taking the room away rather than by writing until something gives, so which entries go
            // is not Caffeine's admission decision to make
            distributedLoadingCache.put(key1, Value.of(1, "written"));
            distributedLoadingCache.put(key2, Value.of(2, "written"));
            distributedLoadingCache.policy().eviction().orElseThrow().setMaximum(0);
            distributedLoadingCache.cleanUp();
            await("the evictions being retained in the store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> Stream.of(key1, key2).forEach(key ->
                            assertThat(distributedLoadingCache.distributedPolicy()
                                    .getFromStore(key, true))
                                    .isNotNull()
                                    .extracting(CacheEntry::getStatus)
                                    .isEqualTo(EVICTED_SIZE_RETAINED)));

            assertThat(distributedLoadingCache.getAll(List.of(key1, key2, unknown)))
                    .as("the two the store held plus the one it did not")
                    .isEqualTo(Map.of(
                            key1, Value.of(1, "written"),
                            key2, Value.of(2, "written"),
                            unknown, Value.of(99, "loaded")));
            verify(cacheLoader, times(1)).load(unknown);

            // what the read above put back was evicted again right away, so the store holds the same two once more
            distributedLoadingCache.cleanUp();
            await("the evictions being retained in the store")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> Stream.of(key1, key2).forEach(key ->
                            assertThat(distributedLoadingCache.distributedPolicy()
                                    .getFromStore(key, true))
                                    .isNotNull()
                                    .extracting(CacheEntry::getStatus)
                                    .isEqualTo(EVICTED_SIZE_RETAINED)));

            assertThat(distributedLoadingCache.getAll(List.of(key1, key2)))
                    .isEqualTo(Map.of(key1, Value.of(1, "written"), key2, Value.of(2, "written")));
            verifyNoMoreInteractions(cacheLoader);
        }

        @DisplayName("Test persistence of evicted entries by time")
        @ParameterizedTest(name = ARGUMENTS_WITH_NAMES_PLACEHOLDER)
        @MethodSource("provideCacheFactoriesWithDifferentDistributionModes")
        void test_EvictedEntryPersistence_by_time(CacheFactory<Key, Value> cacheFactory) throws Exception {
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
                                            .withMaximumSize(Integer.MAX_VALUE)));
            // deliberately without a loading strategy: what this test pins is what the underlying store retains,
            // while reading it back again is what the loading strategy tests above are for

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

        @DisplayName("Test that a cache bounded by weight has its evictions retained and pruned by record count")
        @Test
        void test_EvictedEntryPersistence_of_a_weight_bounded_cache_is_pruned_by_count() {
            // Two bounds in different units, deliberately. Caffeine bounds the cache by total weight, which
            // measures the live object on this heap; the store tier bounds what is kept by number of records,
            // because a serialized record's cost is unrelated to that weight and the pruning reads metadata only,
            // never the key and value it would have to deserialize to weigh them
            int retainedMaximumSize = 2;
            CacheBuilder<Key, Value> cacheBuilder = dc -> dc
                    .withCaffeine(Caffeine.newBuilder()
                            .maximumWeight(50)
                            .weigher((Weigher<Key, Value>) (key, value) -> key.getId()))
                    .withPersistence(configurer -> configurer
                            .withEvictedEntries(evictedEntries -> evictedEntries
                                    .withMaximumSize(retainedMaximumSize)));

            DistributedCache<Key, Value> distributedCache = createCache(cacheBuilder, DistributedCaffeine::build);
            DistributedPolicy<Key, Value> distributedPolicy = distributedCache.distributedPolicy();

            // each key weighs its own id, so no two of them fit under a maximum weight of 50, while four entries
            // would never breach a maximum SIZE of 50 - only the weigher can be what evicts here
            List<Key> keys = List.of(Key.of(30), Key.of(31), Key.of(32), Key.of(33));
            keys.forEach(key -> distributedCache.put(key, Value.of(key.getId())));
            distributedCache.cleanUp();

            // An eviction the weigher caused is retained exactly as one a maximum size would have caused. Which
            // entries Caffeine chooses to evict is its own admission decision and is not the same every time -
            // what is under test is the bound the store applies afterwards, not that choice
            await("more evictions reaching the store than it is allowed to keep")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(distributedPolicy.getAllFromStore(keys, true))
                            .filteredOn(cacheEntry -> cacheEntry.getStatus() == EVICTED_SIZE_RETAINED)
                            .hasSizeGreaterThan(retainedMaximumSize));

            processMaintenance();

            // pruned to a count of records rather than to a weight, whatever those records weigh
            assertThat(distributedPolicy.getAllFromStore(keys, true))
                    .filteredOn(cacheEntry -> cacheEntry.getStatus() == EVICTED_SIZE_RETAINED)
                    .hasSize(retainedMaximumSize);
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
                             repository.streamCacheEntries(null, Set.of(CACHED), UNORDERED)) {
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

        @DisplayName("Test that writes stop waiting for a store that keeps failing, and resume when it answers")
        @Test
        void test_CacheManager_stops_writing_to_a_store_that_keeps_failing() throws Exception {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            // A store that is gone is a store whose writes take the driver's timeout before they fail, and that is
            // what this stands in for: the delay is what the breaker exists to stop paying, and a real outage
            // would make every test here wait it out
            Duration storeTimeout = Duration.ofMillis(500);
            InternalCacheManager<Key, Value> cacheManager = getInstanceRegistry(distributedCache).getCacheManager();
            Publisher<Key, Value> publisher = injectSpy(cacheManager, InternalCacheManager.class,
                    "publisher", Publisher.class);
            AtomicBoolean failing = new AtomicBoolean(true);
            doAnswer(invocation -> {
                if (failing.get()) {
                    sleep(storeTimeout);
                    throw new IllegalStateException("provoked");
                }
                return invocation.callRealMethod();
            }).when(publisher).publishCacheEntries(anyList());

            // the writes that find out: each pays the timeout, and each reports the store rather than the breaker
            for (int write = 1; write <= 3; write++) {
                int key = write;
                assertThatThrownBy(() -> distributedCache.put(Key.of(key), Value.of(key)))
                        .as("write %d, which is still asking the store", key)
                        .hasMessage("provoked");
            }

            // and from here it is not asked again: the write fails well inside the time asking would have taken
            Instant before = Instant.now();
            assertThatThrownBy(() -> distributedCache.put(Key.of(4), Value.of(4)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("because the last 3 attempts to contact it failed");
            assertThat(Duration.between(before, Instant.now()))
                    .as("a refused write must not wait for the store it is refusing to ask")
                    .isLessThan(storeTimeout);

            // nothing of it reached the store, and nothing of it was kept locally either
            verify(publisher, times(3)).publishCacheEntries(anyList());
            assertThat(distributedCache.getIfPresent(Key.of(4))).isNull();

            // the store answers again, and the write after the suspension is what finds out - no probe of its own
            failing.set(false);

            await("writes resuming once the store answers")
                    .atMost(WAITING_DURATION)
                    // the suspension has to run out before anything is attempted again, so the writes until then
                    // are refused rather than failing an assertion - which is the behaviour under test, not a
                    // reason to stop waiting
                    .ignoreExceptionsInstanceOf(IllegalStateException.class)
                    .untilAsserted(() -> {
                        distributedCache.put(Key.of(5), Value.of(5));
                        assertThat(distributedCache.getIfPresent(Key.of(5))).isEqualTo(Value.of(5));
                    });
        }

        @DisplayName("Test that invalidating all stops waiting for a store that keeps failing")
        @Test
        void test_CacheManager_stops_sweeping_a_store_that_keeps_failing() throws Exception {
            // the sweep runs only where population is distributed and there is a store keeping a record of it,
            // which is what makes invalidating all reach more than the keys this cache instance happens to hold
            DistributedCache<Key, Value> distributedCache = createCache(
                    dc -> dc.withPersistence(configurer -> configurer
                            .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency)),
                    DistributedCaffeine::build);

            Duration storeTimeout = Duration.ofMillis(500);
            Repository<Key, Value> repository = injectSpy(
                    getInstanceRegistry(distributedCache).getCacheManager(),
                    InternalCacheManager.class, "repository", Repository.class);
            doAnswer(invocation -> {
                sleep(storeTimeout);
                throw new IllegalStateException("provoked");
            }).when(repository).updateStatusOfCacheEntries(any(), anySet(), any(), any());

            // The sweep runs before anything is published, so this is the only place its failure can be counted -
            // exactly as on the loading path. Left unguarded it would pay the timeout on every call, forever
            for (int sweep = 1; sweep <= 3; sweep++) {
                assertThatThrownBy(distributedCache::invalidateAll)
                        .as("sweep %d, which is still asking the store", sweep)
                        .hasMessageContaining("provoked");
            }

            Instant before = Instant.now();
            assertThatThrownBy(distributedCache::invalidateAll)
                    .hasMessageContaining("because the last 3 attempts to contact it failed");
            assertThat(Duration.between(before, Instant.now()))
                    .as("invalidating all must not wait for a store the cache has stopped contacting")
                    .isLessThan(storeTimeout);

            verify(repository, times(3)).updateStatusOfCacheEntries(any(), anySet(), any(), any());
        }

        @DisplayName("Test that reads of the store on the loading path also stop waiting for a store that fails")
        @Test
        void test_CacheLoader_stops_reading_a_store_that_keeps_failing() throws Exception {
            // the strategy that reads the store before it calls the cache loader, which is the configuration where
            // every miss touches the store - and where nothing else would ever notice that it is gone
            CacheLoader<Key, Value> cacheLoader = key -> Value.of(key.getId(), "loaded");
            DistributedLoadingCache<Key, Value> loadingCache =
                    (DistributedLoadingCache<Key, Value>) this.<Key, Value>createCache(
                            dc -> dc.withCaffeine(Caffeine.newBuilder().maximumSize(1))
                                    .withPersistence(configurer -> configurer
                                            .withEvictedEntries(evictedEntries -> evictedEntries
                                                    .withMaximumSize(10)
                                                    .withLoadingStrategies(CACHE_LOADER))),
                            dc -> dc.build(cacheLoader));

            Duration storeTimeout = Duration.ofMillis(500);
            Repository<Key, Value> repository = injectSpy(getInstanceRegistry(loadingCache).getCacheManager(),
                    InternalCacheManager.class, "repository", Repository.class);
            doAnswer(invocation -> {
                sleep(storeTimeout);
                throw new IllegalStateException("provoked");
            }).when(repository).streamCacheEntries(anySet(), anySet(), any(Order.class));

            // the misses that find out: the read fails before anything is published, so this is the only place the
            // failure can be counted at all
            for (int miss = 1; miss <= 3; miss++) {
                int key = miss;
                assertThatThrownBy(() -> loadingCache.get(Key.of(key)))
                        .as("miss %d, which is still asking the store", key)
                        .hasMessageContaining("provoked");
            }

            // and from here the store is left alone, so the miss fails without waiting for it
            Instant before = Instant.now();
            assertThatThrownBy(() -> loadingCache.get(Key.of(4)))
                    .hasMessageContaining("because the last 3 attempts to contact it failed");
            assertThat(Duration.between(before, Instant.now()))
                    .as("a miss must not wait for a store the cache has stopped contacting")
                    .isLessThan(storeTimeout);

            verify(repository, times(3)).streamCacheEntries(anySet(), anySet(), any(Order.class));
        }

        @DisplayName("Test that synchronization reports being degraded while the store does not answer")
        @Test
        void test_DistributedPolicy_reports_a_store_that_does_not_answer() throws Exception {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            assertThat(distributedCache.distributedPolicy().getSynchronizationState())
                    .isEqualTo(SynchronizationState.SYNCHRONIZED);

            InternalCacheManager<Key, Value> cacheManager = getInstanceRegistry(distributedCache).getCacheManager();
            Publisher<Key, Value> publisher = injectSpy(cacheManager, InternalCacheManager.class,
                    "publisher", Publisher.class);
            AtomicBoolean failing = new AtomicBoolean(true);
            doAnswer(invocation -> {
                if (failing.get()) {
                    throw new IllegalStateException("provoked");
                }
                return invocation.callRealMethod();
            }).when(publisher).publishCacheEntries(anyList());

            // failing writes alone are not a state: a cache instance whose store is merely slow or unlucky is
            // still synchronized, and only giving up on the store is what this reports
            assertThatThrownBy(() -> distributedCache.put(Key.of(1), Value.of(1))).hasMessage("provoked");
            assertThat(distributedCache.distributedPolicy().getSynchronizationState())
                    .isEqualTo(SynchronizationState.SYNCHRONIZED);

            assertThatThrownBy(() -> distributedCache.put(Key.of(2), Value.of(2))).hasMessage("provoked");
            assertThatThrownBy(() -> distributedCache.put(Key.of(3), Value.of(3))).hasMessage("provoked");

            assertThat(distributedCache.distributedPolicy().getSynchronizationState())
                    .as("the store has stopped being contacted, which is what degraded means")
                    .isEqualTo(SynchronizationState.DEGRADED);

            // reads are served throughout, which is what distinguishes this from being stopped
            distributedCache.policy().getIfPresentQuietly(Key.of(1));

            // and it recovers on its own, without synchronization being started again
            failing.set(false);

            await("the state returning once the store answers")
                    .atMost(WAITING_DURATION)
                    .ignoreExceptionsInstanceOf(IllegalStateException.class)
                    .untilAsserted(() -> {
                        distributedCache.put(Key.of(4), Value.of(4));
                        assertThat(distributedCache.distributedPolicy().getSynchronizationState())
                                .isEqualTo(SynchronizationState.SYNCHRONIZED);
                    });

            // stopping is reported as stopping, whatever the store has been doing
            distributedCache.distributedPolicy().stopSynchronization();
            assertThat(distributedCache.distributedPolicy().getSynchronizationState())
                    .isEqualTo(SynchronizationState.STOPPED);
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

            // kept capturing until the failure is fixed below: maintenance retries on a fixed interval, so every
            // cycle until then fails and reports it - which belongs to what this test provokes rather than in the
            // output of the suite

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
            loggerDistributedCaffeine.stopCapturing();

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

        @DisplayName("Test Adapter")
        @Test
        void test_Adapter() throws Exception {
            Set<CacheEntry<Key, Value>> receivedCacheEntries = new HashSet<>();
            Receiver<Key, Value> receiver = spy(new Receiver<Key, Value>() {
                @Override
                public void receiveCacheEntries(@NonNull List<CacheEntry<Key, Value>> cacheEntries) {
                    receivedCacheEntries.addAll(cacheEntries);
                }

                @Override
                public void receiveSynchronizationRestart() {
                    // nothing to record: this test drives the adapter directly and never interrupts it
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

            // awaited before updating them, because what a cache entry says when it arrives is what the store
            // holds at that moment: where the events carry their own payload both versions arrive either way,
            // while a store whose notification only names the record is read when the notification is taken, so
            // publishing twice in a row would leave the first version unobservable. Both distribute every publish,
            // which is what this asserts
            await("receiving of the published cache entries")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(receivedCacheEntries)
                            .contains(insertCacheEntry1, insertCacheEntry2));

            repository.publishCacheEntries(Set.of(updateCacheEntry1, updateCacheEntry2));

            List<io.github.oberhoff.distributedcaffeine.adapter.CacheEntry<Key, Value>> foundCacheEntries = new ArrayList<>();
            try (Stream<io.github.oberhoff.distributedcaffeine.adapter.CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(null, null, UNORDERED)) {
                stream.forEach(foundCacheEntries::add);
            }

            await("receiving")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> {
                        // the number of calls is not fixed: whatever the watcher has already polled is handed
                        // over as one batch, so the four cache entries below arrive in between one and four of them
                        verify(receiver, atLeastOnce())
                                .receiveCacheEntries(anyList());
                        assertThat(receivedCacheEntries)
                                .containsExactlyInAnyOrder(
                                        insertCacheEntry1, insertCacheEntry2,
                                        updateCacheEntry1, updateCacheEntry2);
                    });


            assertThat(foundCacheEntries).hasSize(2)
                    .containsExactlyInAnyOrder(updateCacheEntry1, updateCacheEntry2);
            assertThat(repository.countCacheEntries(null, null))
                    .isEqualTo(2);

            repository.deleteCacheEntries(null, null, null);

            assertThat(repository.countCacheEntries(null, null))
                    .isEqualTo(0);

            adapter.deactivate();
            assertThat(adapter.isActivated()).isFalse();

            // repository methods, filters and their combinations:
            // (operating directly on the repository, which works independently of the synchronizer being activated,
            // so the assertions below are not disturbed by change stream events)

            // timestamps in the past (and spaced apart) so that a status update - which refreshes the timestamp to the
            // real 'now' - reliably produces a newer timestamp than these seeded ones. At millisecond precision,
            // which is the coarsest any store here keeps, so that what comes back compares equal to what went in
            // rather than to what one store happened to round it to
            Instant now = Instant.now().truncatedTo(MILLIS);
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

            assertThat(repository.countCacheEntries(null, null)).isEqualTo(3);

            // countCacheEntries filtered by statuses
            assertThat(repository.countCacheEntries(Set.of(CACHED), null)).isEqualTo(2);
            assertThat(repository.countCacheEntries(Set.of(INVALIDATED), null)).isEqualTo(1);
            assertThat(repository.countCacheEntries(Set.of(CACHED, INVALIDATED), null)).isEqualTo(3);
            assertThat(repository.countCacheEntries(Set.of(EVICTED_SIZE), null)).isEqualTo(0);

            // streamCacheEntries unfiltered
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(null, null, UNORDERED)) {
                assertThat(stream.toList())
                        .containsExactlyInAnyOrder(cachedEntry1, cachedEntry2, invalidatedEntry3);
            }

            // streamCacheEntries filtered by hashes
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("h1", "h2"), null, UNORDERED)) {
                assertThat(stream.toList())
                        .containsExactlyInAnyOrder(cachedEntry1, cachedEntry2);
            }

            // streamCacheEntries filtered by statuses
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(null, Set.of(CACHED), UNORDERED)) {
                assertThat(stream.toList())
                        .containsExactlyInAnyOrder(cachedEntry1, cachedEntry2);
            }

            // streamCacheEntries filtered by hashes and statuses combined
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("h1", "h2", "h3"), Set.of(INVALIDATED), UNORDERED)) {
                assertThat(stream.toList())
                        .containsExactly(invalidatedEntry3);
            }

            // streamCacheEntries ordered ascending by timestamp
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(null, null, ASCENDING)) {
                assertThat(stream.toList())
                        .containsExactly(cachedEntry1, cachedEntry2, invalidatedEntry3);
            }

            // streamCacheEntryMetadata returns everything except key and value, which are the fields it exists to
            // avoid reading (and deserializing) at all
            List<CacheEntryMetadata> cacheEntryMetadata;
            try (Stream<CacheEntryMetadata> stream =
                         repository.streamCacheEntryMetadata(Set.of("h1"), null, UNORDERED)) {
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
            try (Stream<CacheEntryMetadata> stream = repository.streamCacheEntryMetadata(null, Set.of(CACHED), ASCENDING)) {
                assertThat(stream.map(CacheEntryMetadata::getHash).toList())
                        .containsExactly("h1", "h2");
            }

            // updateStatusOfCacheEntries updates the status, clears the operation and refreshes the timestamp
            repository.updateStatusOfCacheEntries(Set.of("h1"), Set.of(CACHED), null, INVALIDATED);

            List<CacheEntry<Key, Value>> updatedEntries;
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("h1"), null, UNORDERED)) {
                updatedEntries = stream.toList();
            }
            assertThat(updatedEntries).hasSize(1);
            CacheEntry<Key, Value> updatedEntry = updatedEntries.get(0);
            assertThat(updatedEntry.getStatus()).isEqualTo(INVALIDATED);
            assertThat(updatedEntry.getOperation()).isNull();          // operation cleared
            assertThat(updatedEntry.getKey()).isEqualTo(Key.of(1));    // key and value preserved
            assertThat(updatedEntry.getValue()).isEqualTo(Value.of(1));
            assertThat(updatedEntry.getTimestamp()).isAfter(timestamp3); // timestamp refreshed to (a recent) now
            assertThat(repository.countCacheEntries(Set.of(CACHED), null)).isEqualTo(1);
            assertThat(repository.countCacheEntries(Set.of(INVALIDATED), null)).isEqualTo(2);

            // updateStatusOfCacheEntries filtered by olderThan updates only entries older than the given timestamp,
            // cachedEntry2 (timestamp2) is updated, invalidatedEntry3 (timestamp3, not older) and the just-refreshed
            // 'h1' entry (recent) are not
            repository.updateStatusOfCacheEntries(null, null, timestamp3, EVICTED_SIZE);
            assertThat(repository.countCacheEntries(Set.of(EVICTED_SIZE), null)).isEqualTo(1);

            // deleteCacheEntries filtered by hashes
            repository.deleteCacheEntries(Set.of("h2"), null, null);
            assertThat(repository.countCacheEntries(null, null)).isEqualTo(2);
            assertThat(repository.countCacheEntries(Set.of(EVICTED_SIZE), null)).isEqualTo(0);

            // deleteCacheEntries filtered by statuses
            repository.deleteCacheEntries(null, Set.of(INVALIDATED), null);
            assertThat(repository.countCacheEntries(null, null)).isEqualTo(0);

            // deleteCacheEntries filtered by olderThan
            repository.publishCacheEntries(Set.of(
                    CacheEntry.of("old", "op1", Key.of(10), Value.of(10), CACHED, Instant.now().minusSeconds(10)),
                    CacheEntry.of("new", "op2", Key.of(11), Value.of(11), CACHED, Instant.now())));
            assertThat(repository.countCacheEntries(null, null)).isEqualTo(2);
            repository.deleteCacheEntries(null, null, Instant.now().minusSeconds(5));
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(null, null, UNORDERED)) {
                assertThat(stream.toList())
                        .hasSize(1)
                        .allSatisfy(entry -> assertThat(entry.getHash()).isEqualTo("new"));
            }

            repository.deleteCacheEntries(null, null, null);
            assertThat(repository.countCacheEntries(null, null)).isEqualTo(0);
        }

        @DisplayName("Test Adapter with a dataset shared across caches")
        @Test
        void test_Adapter_with_shared_dataset() throws Exception {
            // four caches on one and the same dataset: two of them share the discriminator 'a' (so they are
            // expected to synchronize with each other), one uses 'b' and one chooses none, which puts it in the
            // default scope
            DistributedCache<Key, Value> cacheA1 = createCache(
                    "a", CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheA2 = createCache(
                    "a", CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheB = createCache(
                    "b", CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheInDefaultScope = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

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

            // every repository addresses its own scope only
            assertThat(repositoryOf(cacheA1).countCacheEntries(null, null)).isEqualTo(1);
            assertThat(repositoryOf(cacheB).countCacheEntries(null, null)).isEqualTo(1);
            assertThat(repositoryOf(cacheInDefaultScope).countCacheEntries(null, null)).isEqualTo(1);

            // deleting within one scope leaves the others untouched
            repositoryOf(cacheB).deleteCacheEntries(null, null, null);
            assertThat(repositoryOf(cacheB).countCacheEntries(null, null)).isEqualTo(0);
            assertThat(repositoryOf(cacheA1).countCacheEntries(null, null)).isEqualTo(1);
            assertThat(repositoryOf(cacheInDefaultScope).countCacheEntries(null, null)).isEqualTo(1);
        }

        @DisplayName("Test that cache entries are published, read back, filtered and ordered")
        @Test
        void test_Repository_publishes_and_reads_cache_entries() throws Exception {
            Repository<Key, Value> repository = repositoryFor(null);

            // timestamps in the past and spaced apart, so that a status update - which refreshes the timestamp to the
            // real 'now' - reliably produces a newer one. Truncated to the coarsest resolution any store keeps, so
            // that the round-trip compares equal rather than nearly so wherever this runs
            Instant now = Instant.now().truncatedTo(MILLIS);
            Instant timestamp1 = now.minusSeconds(30);
            Instant timestamp2 = now.minusSeconds(20);
            Instant timestamp3 = now.minusSeconds(10);

            CacheEntry<Key, Value> cachedEntry1 = CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, timestamp1);
            CacheEntry<Key, Value> cachedEntry2 = CacheEntry.of("h2", "op2", Key.of(2), Value.of(2), CACHED, timestamp2);
            CacheEntry<Key, Value> invalidatedEntry3 =
                    CacheEntry.of("h3", "op3", Key.of(3), null, INVALIDATED, timestamp3);

            repository.publishCacheEntries(List.of(cachedEntry1, cachedEntry2, invalidatedEntry3));

            assertThat(repository.countCacheEntries(null, null)).isEqualTo(3);
            assertThat(repository.countCacheEntries(Set.of(CACHED), null)).isEqualTo(2);

            // unfiltered, then by hashes, by statuses, by both, and ordered
            try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(null, null, UNORDERED)) {
                assertThat(stream.toList())
                        .containsExactlyInAnyOrder(cachedEntry1, cachedEntry2, invalidatedEntry3);
            }
            try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(Set.of("h1", "h2"), null, UNORDERED)) {
                assertThat(stream.toList()).containsExactlyInAnyOrder(cachedEntry1, cachedEntry2);
            }
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("h1", "h2", "h3"), Set.of(INVALIDATED), UNORDERED)) {
                assertThat(stream.toList()).containsExactly(invalidatedEntry3);
            }
            try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(null, null, ASCENDING)) {
                assertThat(stream.toList()).containsExactly(cachedEntry1, cachedEntry2, invalidatedEntry3);
            }

            // an invalidated cache entry carries no value, which has to survive the round-trip as null rather than as
            // something that fails to deserialize
            try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(Set.of("h3"), null, UNORDERED)) {
                assertThat(stream.toList())
                        .singleElement()
                        .satisfies(cacheEntry -> {
                            assertThat(cacheEntry.getKey()).isEqualTo(Key.of(3));
                            assertThat(cacheEntry.getValue()).isNull();
                        });
            }

            // publishing the same hash again replaces what was there, which is the uniqueness every store has to
            // enforce for a hash within a scope, by a primary key or by a unique index
            repository.publishCacheEntries(List.of(
                    CacheEntry.of("h1", "op9", Key.of(1), Value.of(9), CACHED, timestamp1)));
            assertThat(repository.countCacheEntries(null, null)).isEqualTo(3);
            try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(Set.of("h1"), null, UNORDERED)) {
                assertThat(stream.toList())
                        .singleElement()
                        .satisfies(cacheEntry -> assertThat(cacheEntry.getValue()).isEqualTo(Value.of(9)));
            }
        }

        @DisplayName("Test that metadata is read without touching key and value")
        @Test
        void test_Repository_reads_metadata_only() throws Exception {
            Repository<Key, Value> repository = repositoryFor(null);

            Instant timestamp = Instant.now().truncatedTo(MILLIS).minusSeconds(30);
            CacheEntry<Key, Value> cachedEntry = CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, timestamp);
            repository.publishCacheEntries(List.of(cachedEntry));

            try (Stream<CacheEntryMetadata> stream = repository.streamCacheEntryMetadata(Set.of("h1"), null, UNORDERED)) {
                assertThat(stream.toList())
                        .singleElement()
                        // the metadata of a cache entry is unrelated to the cache entry it belongs to, so the two are
                        // never equal
                        .isNotEqualTo(cachedEntry)
                        .isEqualTo(CacheEntryMetadata.of("h1", "op1", CACHED, timestamp));
            }
        }

        @DisplayName("Test that a status update sets the status, clears the operation and refreshes the timestamp")
        @Test
        void test_Repository_updates_status() throws Exception {
            Repository<Key, Value> repository = repositoryFor(null);

            Instant now = Instant.now().truncatedTo(MILLIS);
            Instant timestamp1 = now.minusSeconds(30);
            Instant timestamp3 = now.minusSeconds(10);
            repository.publishCacheEntries(List.of(
                    CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, timestamp1),
                    CacheEntry.of("h3", "op3", Key.of(3), Value.of(3), CACHED, timestamp3)));

            repository.updateStatusOfCacheEntries(Set.of("h1"), Set.of(CACHED), null, INVALIDATED);

            try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(Set.of("h1"), null, UNORDERED)) {
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
            assertThat(repository.countCacheEntries(Set.of(EVICTED_SIZE), null)).isEqualTo(1);
            assertThat(repository.countCacheEntries(Set.of(INVALIDATED), null)).isEqualTo(1);
        }

        @DisplayName("Test that deleting is filtered by hashes, statuses and age")
        @Test
        void test_Repository_deletes_cache_entries() throws Exception {
            Repository<Key, Value> repository = repositoryFor(null);

            Instant now = Instant.now().truncatedTo(MILLIS);
            repository.publishCacheEntries(List.of(
                    CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, now.minusSeconds(30)),
                    CacheEntry.of("h2", "op2", Key.of(2), Value.of(2), INVALIDATED, now.minusSeconds(20)),
                    CacheEntry.of("h3", "op3", Key.of(3), Value.of(3), CACHED, now)));

            repository.deleteCacheEntries(Set.of("h1"), null, null);
            assertThat(repository.countCacheEntries(null, null)).isEqualTo(2);

            repository.deleteCacheEntries(null, Set.of(INVALIDATED), null);
            assertThat(repository.countCacheEntries(null, null)).isEqualTo(1);

            repository.deleteCacheEntries(null, null, now.minusSeconds(5));
            assertThat(repository.countCacheEntries(null, null)).isEqualTo(1);

            repository.deleteCacheEntries(null, null, null);
            assertThat(repository.countCacheEntries(null, null)).isEqualTo(0);
        }

        @DisplayName("Test that every repository addresses its own discriminator only")
        @Test
        void test_Repository_scopes_by_discriminator() throws Exception {
            Repository<Key, Value> repositoryA = repositoryFor("a");
            Repository<Key, Value> repositoryB = repositoryFor("b");

            Instant timestamp = Instant.now().truncatedTo(MILLIS);
            // the same hash in both scopes, which is allowed precisely because what is unique carries the
            // discriminator alongside the hash
            repositoryA.publishCacheEntries(List.of(
                    CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, timestamp)));
            repositoryB.publishCacheEntries(List.of(
                    CacheEntry.of("h1", "op1", Key.of(1), Value.of(2), CACHED, timestamp)));

            assertThat(repositoryA.countCacheEntries(null, null)).isEqualTo(1);
            assertThat(repositoryB.countCacheEntries(null, null)).isEqualTo(1);
            try (Stream<CacheEntry<Key, Value>> stream = repositoryA.streamCacheEntries(null, null, UNORDERED)) {
                assertThat(stream.toList())
                        .singleElement()
                        .satisfies(cacheEntry -> assertThat(cacheEntry.getValue()).isEqualTo(Value.of(1)));
            }

            // and deleting within one scope leaves the other untouched
            repositoryB.deleteCacheEntries(null, null, null);
            assertThat(repositoryB.countCacheEntries(null, null)).isEqualTo(0);
            assertThat(repositoryA.countCacheEntries(null, null)).isEqualTo(1);
        }

        @DisplayName("Stress test synchronization from data store")
        @Test
        @SuppressWarnings("FutureReturnValueIgnored")
            // background load, awaited through loopCondition/loopCounter
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
                                                .withLoadingStrategies(MAPPING_FUNCTION, CACHE_LOADER))),
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

        @SuppressWarnings("FutureReturnValueIgnored")
            // delayed executor shutdown, deliberately not awaited
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
                                                .withLoadingStrategies(MAPPING_FUNCTION, CACHE_LOADER))),
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
                                                .withLoadingStrategies(MAPPING_FUNCTION, CACHE_LOADER))),
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

        // A TCP proxy that can stop carrying anything on the connections it holds without closing them: the
        // bytes sent into such a connection are accepted and dropped, and nothing comes back. It can also let a
        // few more bytes through before doing so, which cuts a message off in the middle. Connections accepted
        // afterwards are carried as usual
        static final class SilencingProxy implements AutoCloseable {

            private final ServerSocket serverSocket;
            private final String targetHost;
            private final int targetPort;
            private final Executor executor;
            private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
            private final Set<Budget> budgets = ConcurrentHashMap.newKeySet();
            private volatile boolean silencingNewConnections;

            SilencingProxy(String targetHost, int targetPort, Executor executor) throws IOException {
                this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
                this.targetHost = targetHost;
                this.targetPort = targetPort;
                this.executor = executor;
                executor.execute(this::accept);
            }

            int getPort() {
                return serverSocket.getLocalPort();
            }

            void silenceOpenConnections() {
                silenceOpenConnectionsAfter(0);
            }

            // connections accepted from now on are silent from the start, as long as this is on - so that whoever
            // tries to reconnect meanwhile keeps failing, as during an outage that lasts
            void silenceNewConnections(boolean silencing) {
                silencingNewConnections = silencing;
            }

            // whichever direction they travel in, so this is for a moment in which only one side is expected to
            // send anything
            void silenceOpenConnectionsAfter(long bytes) {
                budgets.forEach(budget -> budget.limit(bytes));
            }

            private void accept() {
                while (!serverSocket.isClosed()) {
                    try {
                        Socket client = serverSocket.accept();
                        Socket server = new Socket(targetHost, targetPort);
                        sockets.add(client);
                        sockets.add(server);
                        Budget budget = new Budget();
                        if (silencingNewConnections) {
                            budget.limit(0);
                        }
                        budgets.add(budget);
                        executor.execute(() -> pump(client, server, budget));
                        executor.execute(() -> pump(server, client, budget));
                    } catch (IOException e) {
                        // closed, which is how accepting ends
                    }
                }
            }

            private void pump(Socket from, Socket to, Budget budget) {
                byte[] buffer = new byte[8192];
                try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                    int read;
                    while ((read = in.read(buffer)) >= 0) {
                        int allowed = budget.take(read);
                        if (allowed > 0) {
                            out.write(buffer, 0, allowed);
                            out.flush();
                        }
                    }
                } catch (IOException e) {
                    // either side closed, which is how pumping ends
                } finally {
                    // a silenced connection stays open, because a reset is exactly what it must not receive
                    if (!budget.isLimited()) {
                        closeQuietly(from);
                        closeQuietly(to);
                    }
                }
            }

            private static void closeQuietly(Socket socket) {
                try {
                    socket.close();
                } catch (IOException e) {
                    // nothing left to do about it
                }
            }

            @Override
            public void close() throws IOException {
                serverSocket.close();
                sockets.forEach(SilencingProxy::closeQuietly);
            }

            // how many more bytes a connection carries, shared by both of its directions - unlimited until limited
            private static final class Budget {

                private long remaining = -1;

                synchronized void limit(long bytes) {
                    remaining = bytes;
                }

                synchronized boolean isLimited() {
                    return remaining >= 0;
                }

                synchronized int take(int read) {
                    if (remaining < 0) {
                        return read;
                    }
                    int allowed = (int) Math.min(read, remaining);
                    remaining -= allowed;
                    return allowed;
                }
            }
        }

    }

    @SuppressWarnings({"java:S5838", "java:S5778", "java:S5961"})
    abstract static class MongoIntegration extends CommonIntegration {
        static final String DATABASE_NAME = "distributedCaffeineDatabase";

        MongoDBContainer mongoContainer;

        MongoClient mongoClient;

        @DisplayName("Test that the adapter names and separates its scope by discriminator")
        @Test
        void test_Adapter_scopes_by_discriminator() {
            String collectionName = getDatasetName();

            DistributedCache<Key, Value> cacheA = createCache(
                    "a", CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheInDefaultScope = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            // every cache has a discriminator, so the identifier always carries one
            assertThat(cacheA.distributedPolicy().getAdapter().getIdentifier())
                    .isEqualTo(String.join(":", "mongodb", DATABASE_NAME, collectionName, "a"));
            assertThat(cacheInDefaultScope.distributedPolicy().getAdapter().getIdentifier())
                    .isEqualTo(String.join(":", "mongodb", DATABASE_NAME, collectionName, DEFAULT_DISCRIMINATOR));

            // uniqueness in the store is (discriminator + hash), so the same key coexists once per scope
            Key key = Key.of(1);
            cacheA.put(key, Value.of(1));
            cacheInDefaultScope.put(key, Value.of(2));

            assertThat(mongoClient.getDatabase(DATABASE_NAME).getCollection(collectionName).countDocuments())
                    .isEqualTo(2);
        }

        @DisplayName("Test that documents which are no cache entries are reported and skipped")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Repository_skips_documents_that_are_no_cache_entries() throws Exception {
            DistributedCache<Key, Value> distributedCache = createCache(
                    CacheBuilder.identity(),
                    DistributedCaffeine::build);
            Adapter<Key, Value> adapter = distributedCache.distributedPolicy().getAdapter();
            Repository<Key, Value> repository = adapter.getRepository().orElseThrow();

            // operating directly on the repository, which works independently of the synchronizer being activated,
            // so that nothing the change stream delivers is counted among the warnings asserted below
            adapter.deactivate();

            // millisecond precision, which is what the store keeps, so the metadata read back compares equal
            Instant timestamp = Instant.now().truncatedTo(MILLIS);

            // reading a document that is no cache entry is reported before it is skipped, so the warnings expected
            // for the two documents seeded below are captured (and asserted) instead of ending up - with their stack
            // traces - in the test output
            CaptureLogger loggerMongoRepository = CaptureLoggerFactory
                    .getCaptureLogger("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoRepository");
            loggerMongoRepository.startCapturing();

            // a document carrying a key and a value that cannot be deserialized is what tells the two streams apart:
            // it is no cache entry (skipped, logged and left out), while its metadata is returned - which it could only
            // be if reading metadata does not touch the payload at all
            mongoClient.getDatabase(DATABASE_NAME).getCollection(getDatasetName())
                    .insertOne(new Document()
                            .append(CacheEntry.Field.HASH.toString(), "broken")
                            .append(CacheEntry.Field.OPERATION.toString(), "op4")
                            .append(CacheEntry.Field.KEY.toString(), "not a serialized key")
                            .append(CacheEntry.Field.VALUE.toString(), "not a serialized value")
                            .append(CacheEntry.Field.STATUS.toString(), CACHED.toString())
                            .append(CacheEntry.Field.TIMESTAMP.toString(), timestamp)
                            .append(DiscriminatorAware.DISCRIMINATOR_FIELD, DEFAULT_DISCRIMINATOR));
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("broken"), null, UNORDERED)) {
                assertThat(stream.toList()).isEmpty();
            }
            try (Stream<CacheEntryMetadata> stream =
                         repository.streamCacheEntryMetadata(Set.of("broken"), null, UNORDERED)) {
                assertThat(stream.toList())
                        .singleElement()
                        .isEqualTo(CacheEntryMetadata.of("broken", "op4", CACHED, timestamp));
            }
            repository.deleteCacheEntries(Set.of("broken"), null, null);

            // a document not carrying what even metadata cannot do without (no status here) is no cache entry and no
            // metadata of one either, so both streams skip it
            mongoClient.getDatabase(DATABASE_NAME).getCollection(getDatasetName())
                    .insertOne(new Document()
                            .append(CacheEntry.Field.HASH.toString(), "incomplete")
                            .append(CacheEntry.Field.TIMESTAMP.toString(), timestamp)
                            .append(DiscriminatorAware.DISCRIMINATOR_FIELD, DEFAULT_DISCRIMINATOR));
            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("incomplete"), null, UNORDERED)) {
                assertThat(stream.toList()).isEmpty();
            }
            try (Stream<CacheEntryMetadata> stream =
                         repository.streamCacheEntryMetadata(Set.of("incomplete"), null, UNORDERED)) {
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
        }

        @DisplayName("Test that every query the repository issues is served by an index")
        @Test
        void test_Repository_queries_avoid_collection_scans() throws Exception {
            String collectionName = getDatasetName();
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
                assertThatQueryIsIndexed(repository, collectionName, null, null, null, UNORDERED);
                assertThatQueryIsIndexed(repository, collectionName, null, null, deadline, UNORDERED);
                assertThatQueryIsIndexed(repository, collectionName, hashes, null, null, UNORDERED);
                assertThatQueryIsIndexed(repository, collectionName, hashes, CACHED_GROUP, null, UNORDERED);
                assertThatQueryIsIndexed(repository, collectionName, null, CACHED_GROUP, null, UNORDERED);
                assertThatQueryIsIndexed(repository, collectionName, null, CACHED_GROUP, deadline, UNORDERED);
                // ordered, as synchronizing cache entries on activation reads oldest first and pruning by size
                // reads newest first
                assertThatQueryIsIndexed(repository, collectionName, null, CACHED_GROUP, null, ASCENDING);
                assertThatQueryIsIndexed(repository, collectionName, null, EVICTED_RETAINED_GROUP, null, DESCENDING);
            }
        }

        @DisplayName("Test that a binary JSON value is stored as a document rather than as a string")
        @Test
        void test_Repository_stores_binary_json_as_a_document() throws Exception {
            // What storeAsBinaryJson is for: the value is handed to the store in the store's own representation,
            // so somebody looking at the records reads them and can query inside them. Stored as a string it
            // round-trips just as well and nothing in the cache behaves differently - only the records stop being
            // legible, which no other test would notice
            Value value = Value.of(1);
            publishWith(new JacksonSerializer<>(Value.class, true), "asDocument", value);
            publishWith(new JacksonSerializer<>(Value.class, false), "asString", value);

            MongoCollection<Document> collection = mongoClient.getDatabase(DATABASE_NAME)
                    .getCollection(getDatasetName());

            assertThat(requireNonNull(collection.find(Filters.eq(DiscriminatorAware.DISCRIMINATOR_FIELD,
                    "asDocument")).first()).get(CacheEntry.Field.VALUE.toString()))
                    .isInstanceOf(Document.class);
            assertThat(requireNonNull(collection.find(Filters.eq(DiscriminatorAware.DISCRIMINATOR_FIELD,
                    "asString")).first()).get(CacheEntry.Field.VALUE.toString()))
                    .isInstanceOf(String.class);

            // and what was stored as a document can be queried inside, which is the whole of the difference
            assertThat(collection.find(Filters.eq(
                    CacheEntry.Field.VALUE.toString().concat(".name"), "value")).into(new ArrayList<>()))
                    .singleElement()
                    .satisfies(document -> assertThat(document.getString(DiscriminatorAware.DISCRIMINATOR_FIELD))
                            .isEqualTo("asDocument"));
        }

        @DisplayName("Test failure handling while watching change streams")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_fails_and_retries() throws Exception {
            // early (fail-fast) failure: watching change streams requires majority read concern, so building a cache
            // whose collection uses a local read concern fails immediately (without retrying)
            try (MongoClient failFastMongoClient = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(mongoContainer.getReplicaSetUrl()))
                    .readConcern(ReadConcern.LOCAL)
                    .build())) {
                MongoAdapter<Key, Value> localReadConcernAdapter = MongoAdapter
                        .newBuilder(failFastMongoClient, DATABASE_NAME, getDatasetName())
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
            AtomicReference<?> resumeToken = resumeTokenOf(syncedSynchronizer);

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

            // receiving fails and retries - for this cache instance alone, without the cursor it shares failing
            loggerMongoSynchronizer.startCapturing();

            // restarting reconciles the cache against the data store, and reports that it did - captured for
            // the same reason and asserted below, because that reconcile is what the recovery under test consists of
            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);
            loggerDistributedCaffeine.startCapturing();

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
                                        && loggingEvent.getMessage().startsWith("Receiving change stream events failed")
                                        && loggingEvent.getMessage().endsWith("Retrying..."));
                    });

            loggerMongoSynchronizer.stopCapturing();

            // while receiving fails, the synced instance does not receive the update
            assertThat(syncedDistributedCache.getIfPresent(key2)).isNull();

            // fix failure: the cache instance recovers on its own, by reconciling against the data store
            doCallRealMethod().when(syncedReceiver).receiveCacheEntries(any());

            await("recovery")
                    .atMost(WAITING_DURATION.plusSeconds(10)) // retry delay is increased on failure
                    .untilAsserted(() -> assertThat(syncedDistributedCache.getIfPresent(key2)).isEqualTo(value2));

            // and it did not just carry on: restarting reconciled the cache against the data store, which is what
            // makes an entry missed while receiving was down good again
            assertThat(loggerDistributedCaffeine.getLoggingEvents())
                    .anySatisfy(loggingEvent -> {
                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                        assertThat(loggingEvent.getMessage())
                                .startsWith("Synchronization was interrupted for cache at");
                    });
            loggerDistributedCaffeine.stopCapturing();

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

            // Deactivating and activating again subscribes anew: every activation gets a subscription of its own and
            // never waits for one left over from before, which is what keeps a failed or stale one from blocking it
            Key key5 = Key.of(5);
            Value value5 = Value.of(5);

            syncedDistributedCache.distributedPolicy().stopSynchronization();
            assertThat(syncedSynchronizer.isActivated()).isFalse();

            assertThatNoException().isThrownBy(() ->
                    syncedDistributedCache.distributedPolicy().startSynchronization());
            assertThat(syncedSynchronizer.isActivated()).isTrue();

            distributedCache.put(key5, value5);

            await("recovery after activating again")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(syncedDistributedCache.getIfPresent(key5)).isEqualTo(value5));
        }

        @DisplayName("Test that an invalidation swept while the watcher is down is not lost")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_invalidation_swept_while_watcher_is_down_is_not_lost() {
            // An invalidation reaches the other cache instances as an update of the document the population left
            // behind, and change streams are watched with UPDATE_LOOKUP, so what is delivered is that document as it
            // stands when the event is polled - not as it stood when the update happened. Maintenance deletes it a
            // distribution duration after the write, because without persistence it only ever existed to carry the
            // removal. A watcher that was down for longer than that therefore polls an event whose document is
            // already gone: the lookup finds nothing, the pipeline's match on the discriminator drops the event on
            // the server, and nothing arrives at all - not even something recognizable as missing. Only the
            // reconcile that resuming performs can make the invalidation good.
            // Being down is engineered rather than waited for: the watcher of cacheA is pointed at a client that is
            // closed, and a cache instance on a collection it does not watch yet has it reopen, which then keeps
            // failing - with the resume position pinned before the invalidation - while the invalidation is written
            // and swept. cacheA has a client of its own, so that the watcher taken down is its alone
            try (MongoClient ownMongoClient = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(mongoContainer.getReplicaSetUrl()))
                    .applicationName(getDatasetName())
                    .build());
                 MongoClient closedMongoClient = MongoClients.create(mongoContainer.getReplicaSetUrl())) {
                closedMongoClient.close();
                DistributedCache<Key, Value> distributedCacheA = createCache(
                        MongoAdapter.newBuilder(ownMongoClient, DATABASE_NAME, getDatasetName()).build(),
                        CacheBuilder.identity(), DistributedCaffeine::build);
                DistributedCache<Key, Value> distributedCacheB = createCache(
                        CacheBuilder.identity(), DistributedCaffeine::build);
                try {
                    Key key1 = Key.of(1);
                    Value value1 = Value.of(1);

                    distributedCacheB.put(key1, value1);

                    await("synchronization between cache instances")
                            .atMost(WAITING_DURATION)
                            .untilAsserted(() -> assertThat(distributedCacheA.getIfPresent(key1)).isEqualTo(value1));

                    // the watcher reports every failed attempt, so the provoked ones below are captured and
                    // asserted instead of ending up, with their stack traces, in the test output
                    CaptureLogger loggerMongoWatcher = CaptureLoggerFactory
                            .getCaptureLogger("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoWatcher");
                    loggerMongoWatcher.startCapturing();
                    // resuming reconciles the cache against the data store, and reports that it did
                    CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                            .getCaptureLogger(DistributedCaffeine.class);
                    loggerDistributedCaffeine.startCapturing();

                    Object watcher = watcherOf(readFieldValue(distributedCacheA.distributedPolicy().getAdapter(),
                            AbstractAdapter.class, "synchronizer", Synchronizer.class));
                    Object mongoDatabase = readFieldValue(watcher, watcher.getClass(), "mongoDatabase", Object.class);
                    writeFieldValue(watcher, watcher.getClass(), "mongoDatabase",
                            closedMongoClient.getDatabase(DATABASE_NAME));
                    Adapter<Key, Value> reopening = MongoAdapter.newBuilder(ownMongoClient, DATABASE_NAME,
                            "c_" + getDatasetName().substring(2)).build();
                    Synchronizer<?, ?> reopeningSynchronizer = readFieldValue(reopening, AbstractAdapter.class,
                            "synchronizer", Synchronizer.class);
                    writeFieldValue(reopeningSynchronizer, reopeningSynchronizer.getClass(), "activationTimeout",
                            Duration.ofSeconds(1));
                    assertThatThrownBy(() -> createCache(reopening, CacheBuilder.identity(),
                            DistributedCaffeine::build))
                            .isExactlyInstanceOf(MongoClientException.class);
                    await("watcher down")
                            .atMost(WAITING_DURATION)
                            .until(() -> !loggerMongoWatcher.getLoggingEvents().isEmpty());

                    distributedCacheB.invalidate(key1);

                    // what maintenance does a distribution duration later, done now: a negative duration puts the
                    // deadline ahead of every write, so nothing is left for the watcher to find once it watches again
                    invokeMethod(getInstanceRegistry(distributedCacheB).getMaintenanceWorker(),
                            InternalMaintenanceWorker.class, "processNotRetained",
                            List.of(Duration.class), List.of(Duration.ZERO.minus(Duration.ofMillis(1))));

                    assertThatDataStoreIsEmpty();

                    writeFieldValue(watcher, watcher.getClass(), "mongoDatabase", mongoDatabase);

                    // something written once watching works again arrives - asserted first, so that a watcher which
                    // never recovered at all cannot make the assertions below pass
                    Key key2 = Key.of(2);
                    Value value2 = Value.of(2);
                    distributedCacheB.put(key2, value2);

                    await("recovery")
                            .atMost(WAITING_DURATION.plusSeconds(10)) // retry delay is increased on failure
                            .untilAsserted(() -> assertThat(distributedCacheA.getIfPresent(key2)).isEqualTo(value2));

                    assertThat(distributedCacheA.getIfPresent(key1)).isNull();
                    assertThat(distributedCacheB.getIfPresent(key1)).isNull();

                    // the watcher really did fail and say so, which is what the recovery above is a recovery from
                    assertThat(loggerMongoWatcher.getLoggingEvents())
                            .anySatisfy(loggingEvent -> {
                                assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                assertThat(loggingEvent.getMessage()).startsWith("Watching change streams failed");
                            });
                    loggerMongoWatcher.stopCapturing();

                    // and the invalidation above survived because resuming reconciled the cache against the data
                    // store - the only step that can drop a cache entry whose removal was never delivered
                    assertThat(loggerDistributedCaffeine.getLoggingEvents())
                            .anySatisfy(loggingEvent -> {
                                assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                assertThat(loggingEvent.getMessage())
                                        .startsWith("Synchronization was interrupted for cache at");
                            });
                    loggerDistributedCaffeine.stopCapturing();
                } finally {
                    // torn down here rather than after the test, because its client is closed on the way out
                    distributedCacheA.distributedPolicy().stopSynchronization();
                    distributedCacheA.invalidateAll();
                    distributedCacheInstances.remove(distributedCacheA);
                }
            }
        }

        @DisplayName("Test that a watching connection that silently stops carrying anything is noticed and replaced")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_replaces_a_watching_connection_that_went_silent() throws Exception {
            // cacheB reaches the server through a proxy that can go silent on the connections it is carrying while
            // keeping them open - what a failover moving the server's address or an expired NAT entry leaves a
            // client with: nothing is reset, nothing arrives. Its client uses the driver's defaults, which set no
            // limit on how long an answer may take, and connects directly, so that the members of the replica set
            // behind the proxy are not discovered and reached around it
            try (SilencingProxy proxy = new SilencingProxy(
                    mongoContainer.getHost(), mongoContainer.getMappedPort(27017), executorService);
                 MongoClient proxiedMongoClient = MongoClients.create(format(
                         "mongodb://localhost:%d/?directConnection=true", proxy.getPort()))) {
                DistributedCache<Key, Value> cacheA = createCache(CacheBuilder.identity(), DistributedCaffeine::build);
                Adapter<Key, Value> adapterB = MongoAdapter
                        .newBuilder(proxiedMongoClient, DATABASE_NAME, getDatasetName()).build();
                // the limit on watching shortened before activation, so that noticing takes seconds rather than
                // the production limit - the test is about the noticing, not about how long it takes
                Synchronizer<?, ?> synchronizerB = readFieldValue(adapterB, AbstractAdapter.class,
                        "synchronizer", Synchronizer.class);
                writeFieldValue(synchronizerB, synchronizerB.getClass(), "watcherTimeout", Duration.ofSeconds(2));
                DistributedCache<Key, Value> cacheB = createCache(adapterB, CacheBuilder.identity(),
                        DistributedCaffeine::build);
                try {
                    cacheA.put(Key.of(1), Value.of(1));

                    await("synchronization through the proxy")
                            .atMost(WAITING_DURATION)
                            .untilAsserted(() -> assertThat(cacheB.getIfPresent(Key.of(1))).isEqualTo(Value.of(1)));

                    CaptureLogger loggerMongoWatcher = CaptureLoggerFactory
                            .getCaptureLogger("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoWatcher");
                    loggerMongoWatcher.startCapturing();
                    CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                            .getCaptureLogger(DistributedCaffeine.class);
                    loggerDistributedCaffeine.startCapturing();

                    // from here on, whatever cacheB had open carries nothing in either direction, while a connection
                    // opened afterwards goes through - the server is reachable again, the old connections do not know
                    proxy.silenceOpenConnections();

                    cacheA.put(Key.of(2), Value.of(2));

                    // the change went to a connection that no longer delivers, so the value can only arrive by the
                    // watcher noticing, watching anew and reconciling
                    await("recovery from a watching connection that went silent")
                            .atMost(EXTENDED_WAITING_DURATION)
                            .untilAsserted(() -> assertThat(cacheB.getIfPresent(Key.of(2))).isEqualTo(Value.of(2)));

                    assertThat(loggerMongoWatcher.getLoggingEvents())
                            .anySatisfy(loggingEvent -> {
                                assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                assertThat(loggingEvent.getMessage()).startsWith("Watching change streams failed");
                            });
                    loggerMongoWatcher.stopCapturing();

                    assertThat(loggerDistributedCaffeine.getLoggingEvents())
                            .anySatisfy(loggingEvent -> {
                                assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                assertThat(loggingEvent.getMessage())
                                        .startsWith("Synchronization was interrupted for cache at");
                            });
                    loggerDistributedCaffeine.stopCapturing();
                } finally {
                    // torn down here rather than after the test, because its client is closed on the way out
                    cacheB.distributedPolicy().stopSynchronization();
                    distributedCacheInstances.remove(cacheB);
                }
            }
        }

        @DisplayName("Test that watching a single collection survives the collection being dropped")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_keeps_watching_a_collection_that_is_dropped() {
            // A cursor on a single collection - which is what the instance and collection modes watch with - is ended
            // by the server once its collection is dropped. Whoever recreates it then writes into a collection that
            // nothing watches any more, unless the watcher notices that its cursor is gone. A cursor on the whole
            // database is not ended by dropping one of its collections.
            // What is written before the watcher watches anew can only arrive through the reconcile that follows,
            // and persisting cached entries is what gives that reconcile something to restore
            for (WatcherSharingMode sharingMode : List.of(WatcherSharingMode.INSTANCE, WatcherSharingMode.COLLECTION)) {
                DistributedCache<Key, Value> cacheA = createCache(CacheBuilder.identity(),
                        dc -> dc.withPersistence(configurer -> configurer
                                        .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                                .build());
                DistributedCache<Key, Value> cacheB = createCache(
                        MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, getDatasetName())
                                .withWatcherSharingMode(sharingMode)
                                .build(),
                        CacheBuilder.identity(),
                        dc -> dc.withPersistence(configurer -> configurer
                                        .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                                .build());

                CaptureLogger loggerMongoWatcher = CaptureLoggerFactory
                        .getCaptureLogger("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoWatcher");
                loggerMongoWatcher.startCapturing();
                CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                        .getCaptureLogger(DistributedCaffeine.class);
                loggerDistributedCaffeine.startCapturing();

                cacheA.put(Key.of(1), Value.of(1));

                await("synchronization before the drop with " + sharingMode)
                        .atMost(WAITING_DURATION)
                        .untilAsserted(() -> assertThat(cacheB.getIfPresent(Key.of(1))).isEqualTo(Value.of(1)));

                mongoClient.getDatabase(DATABASE_NAME).getCollection(getDatasetName()).drop();

                cacheA.put(Key.of(2), Value.of(2));

                await("synchronization after the drop with " + sharingMode)
                        .atMost(Duration.ofSeconds(30))
                        .untilAsserted(() -> assertThat(cacheB.getIfPresent(Key.of(2))).isEqualTo(Value.of(2)));

                // because the watcher noticed that its cursor had been ended, rather than by chance
                assertThat(loggerMongoWatcher.getLoggingEvents())
                        .anySatisfy(loggingEvent -> {
                            assertThat(loggingEvent.getMessage()).startsWith("Watching change streams failed");
                            assertThat(loggingEvent.getThrowable())
                                    .hasMessageStartingWith("Change stream was ended by the server");
                        });
                loggerMongoWatcher.stopCapturing();
                loggerDistributedCaffeine.stopCapturing();
                cacheA.distributedPolicy().stopSynchronization();
                cacheB.distributedPolicy().stopSynchronization();
            }
        }

        @DisplayName("Test that a cache instance joining a cursor being replaced waits for it, or gives up in time")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_joins_a_shared_cursor_while_it_is_being_replaced() throws Exception {
            // The cache instances watching share a client of their own, so that the watcher taken down is theirs
            // alone: it is pointed at a client that is closed, so that replacing its cursor keeps failing for as long
            // as that lasts. Reading and writing go on as usual
            try (MongoClient ownMongoClient = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(mongoContainer.getReplicaSetUrl()))
                    .applicationName(getDatasetName())
                    .build());
                 MongoClient closedMongoClient = MongoClients.create(mongoContainer.getReplicaSetUrl())) {
                closedMongoClient.close();
                DistributedCache<Key, Value> cacheA = createCache(CacheBuilder.identity(), DistributedCaffeine::build);
                DistributedCache<Key, Value> cacheB1 = createCache(watchingAdapter(ownMongoClient),
                        CacheBuilder.identity(), DistributedCaffeine::build);
                List<DistributedCache<Key, Value>> watching = new ArrayList<>(List.of(cacheB1));
                try {
                    cacheA.put(Key.of(1), Value.of(1));

                    await("synchronization before the outage")
                            .atMost(WAITING_DURATION)
                            .untilAsserted(() -> assertThat(cacheB1.getIfPresent(Key.of(1))).isEqualTo(Value.of(1)));

                    // every failed attempt to replace the cursor is reported, and so is the reconcile once it is
                    CaptureLogger loggerMongoWatcher = CaptureLoggerFactory
                            .getCaptureLogger("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoWatcher");
                    loggerMongoWatcher.startCapturing();
                    CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                            .getCaptureLogger(DistributedCaffeine.class);
                    loggerDistributedCaffeine.startCapturing();

                    Object watcher = watcherOf(readFieldValue(cacheB1.distributedPolicy().getAdapter(),
                            AbstractAdapter.class, "synchronizer", Synchronizer.class));
                    Object mongoDatabase = readFieldValue(watcher, watcher.getClass(), "mongoDatabase", Object.class);
                    writeFieldValue(watcher, watcher.getClass(), "mongoDatabase",
                            closedMongoClient.getDatabase(DATABASE_NAME));

                    // A cache instance on a collection the cursor does not watch yet has it reopened - by the watcher
                    // itself, which keeps failing while pointed at the closed client. Being the one whose activation
                    // timeout runs out first, it gives up, and says why
                    Adapter<Key, Value> adapterB3 = MongoAdapter.newBuilder(ownMongoClient, DATABASE_NAME,
                            "c_" + getDatasetName().substring(2)).build();
                    Synchronizer<?, ?> synchronizerB3 = readFieldValue(adapterB3, AbstractAdapter.class,
                            "synchronizer", Synchronizer.class);
                    writeFieldValue(synchronizerB3, synchronizerB3.getClass(), "activationTimeout",
                            Duration.ofSeconds(1));
                    assertThatThrownBy(() -> createCache(adapterB3, CacheBuilder.identity(),
                            DistributedCaffeine::build))
                            .isExactlyInstanceOf(MongoClientException.class)
                            .hasMessageStartingWith("Watching change streams failed for cache at")
                            .cause()
                            .isInstanceOf(MongoTimeoutException.class);
                    await("cursor being replaced")
                            .atMost(WAITING_DURATION)
                            .until(() -> !loggerMongoWatcher.getLoggingEvents().isEmpty());

                    // while one whose timeout lasts waits for the cursor to come back
                    Adapter<Key, Value> adapterB2 = watchingAdapter(ownMongoClient);
                    Synchronizer<?, ?> synchronizerB2 = readFieldValue(adapterB2, AbstractAdapter.class,
                            "synchronizer", Synchronizer.class);
                    writeFieldValue(synchronizerB2, synchronizerB2.getClass(), "activationTimeout",
                            Duration.ofSeconds(60));
                    CompletableFuture<DistributedCache<Key, Value>> joining = CompletableFuture.supplyAsync(() ->
                            createCache(adapterB2, CacheBuilder.identity(), DistributedCaffeine::build), executorService);
                    sleep(Duration.ofSeconds(2));
                    assertThat(joining).isNotDone();

                    writeFieldValue(watcher, watcher.getClass(), "mongoDatabase", mongoDatabase);

                    DistributedCache<Key, Value> cacheB2 = joining.get(60, TimeUnit.SECONDS);
                    watching.add(cacheB2);

                    cacheA.put(Key.of(2), Value.of(2));

                    await("synchronization of the instance that joined while the cursor was being replaced")
                            .atMost(WAITING_DURATION)
                            .untilAsserted(() -> assertThat(cacheB2.getIfPresent(Key.of(2))).isEqualTo(Value.of(2)));
                    loggerMongoWatcher.stopCapturing();
                    loggerDistributedCaffeine.stopCapturing();
                } finally {
                    // torn down here rather than after the test, because their client is closed on the way out
                    watching.forEach(cache -> {
                        cache.distributedPolicy().stopSynchronization();
                        distributedCacheInstances.remove(cache);
                    });
                }
            }
        }

        @DisplayName("Test that sharing at instance level gives every cache instance a cursor of its own")
        @Test
        void test_Synchronizer_shares_watchers_by_instance() {
            assertWatchersShared(WatcherSharingMode.INSTANCE, 3);
        }

        @DisplayName("Test that sharing at collection level gives the cache instances of a collection one cursor")
        @Test
        void test_Synchronizer_shares_watchers_by_collection() {
            assertWatchersShared(WatcherSharingMode.COLLECTION, 2);
        }

        @DisplayName("Test that sharing at database level gives all cache instances one cursor")
        @Test
        void test_Synchronizer_shares_watchers_by_database() {
            assertWatchersShared(WatcherSharingMode.DATABASE, 1);
        }

        @DisplayName("Test that a cache instance failing to apply what it received leaves the others on its cursor alone")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_isolates_a_cache_instance_that_fails_to_apply() {
            // all three share a cursor, which is the default
            DistributedCache<Key, Value> cacheA = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheB = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheC = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            CaptureLogger loggerMongoWatcher = CaptureLoggerFactory
                    .getCaptureLogger("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoWatcher");
            loggerMongoWatcher.startCapturing();
            CaptureLogger loggerMongoSynchronizer = CaptureLoggerFactory
                    .getCaptureLogger("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoSynchronizer");
            loggerMongoSynchronizer.startCapturing();
            // the failing cache instance recovers by reconciling, which it reports
            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);
            loggerDistributedCaffeine.startCapturing();

            InternalCacheManager<Key, Value> cacheManager = getInstanceRegistry(cacheB).getCacheManager();
            AtomicBoolean failing = new AtomicBoolean(true);
            cacheB.distributedPolicy().getAdapter().setReceiver(new Receiver<>() {

                @Override
                public void receiveCacheEntries(@NonNull List<CacheEntry<Key, Value>> cacheEntries) {
                    if (failing.get()) {
                        throw new IllegalStateException("provoked");
                    }
                    cacheManager.receiveCacheEntries(cacheEntries);
                }

                @Override
                public void receiveSynchronizationRestart() {
                    cacheManager.receiveSynchronizationRestart();
                }
            });

            cacheA.put(Key.of(1), Value.of(1));

            // the cache instance next to the failing one receives as usual, through the very same cursor
            await("synchronization next to a failing cache instance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(cacheC.getIfPresent(Key.of(1))).isEqualTo(Value.of(1)));
            await("failure of the cache instance that fails to apply")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(loggerMongoSynchronizer.getLoggingEvents())
                            .anySatisfy(loggingEvent -> {
                                assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                assertThat(loggingEvent.getMessage())
                                        .startsWith("Receiving change stream events failed");
                                assertThat(loggingEvent.getThrowable()).hasMessage("provoked");
                            }));

            failing.set(false);
            cacheA.put(Key.of(2), Value.of(2));

            // and the failing one receives again once it no longer fails - including what it failed on, which it
            // kept and applies after reconciling
            await("synchronization once applying no longer fails")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .untilAsserted(() -> {
                        assertThat(cacheB.getIfPresent(Key.of(1))).isEqualTo(Value.of(1));
                        assertThat(cacheB.getIfPresent(Key.of(2))).isEqualTo(Value.of(2));
                    });

            // without the cursor ever having been given up for it
            assertThat(loggerMongoWatcher.getLoggingEvents()).isEmpty();
            loggerMongoWatcher.stopCapturing();
            loggerMongoSynchronizer.stopCapturing();
            loggerDistributedCaffeine.stopCapturing();
        }

        @DisplayName("Test that a cache instance slow to apply what it received does not hold up the others")
        @Test
        void test_Synchronizer_does_not_hold_up_others_behind_a_slow_cache_instance() {
            // all three share a cursor, which is the default
            DistributedCache<Key, Value> cacheA = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheB = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheC = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            // cacheB does not get past applying until released - which, if applying happened on the thread that
            // holds the cursor, would leave nobody else on the cursor receiving anything either
            InternalCacheManager<Key, Value> cacheManager = getInstanceRegistry(cacheB).getCacheManager();
            CountDownLatch released = new CountDownLatch(1);
            cacheB.distributedPolicy().getAdapter().setReceiver(new Receiver<>() {

                @Override
                public void receiveCacheEntries(@NonNull List<CacheEntry<Key, Value>> cacheEntries) {
                    try {
                        released.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    cacheManager.receiveCacheEntries(cacheEntries);
                }

                @Override
                public void receiveSynchronizationRestart() {
                    cacheManager.receiveSynchronizationRestart();
                }
            });

            try {
                cacheA.put(Key.of(1), Value.of(1));
                cacheA.put(Key.of(2), Value.of(2));

                await("synchronization next to a cache instance that does not get past applying")
                        .atMost(WAITING_DURATION)
                        .untilAsserted(() -> {
                            assertThat(cacheC.getIfPresent(Key.of(1))).isEqualTo(Value.of(1));
                            assertThat(cacheC.getIfPresent(Key.of(2))).isEqualTo(Value.of(2));
                        });
                assertThat(cacheB.getIfPresent(Key.of(1))).isNull();
            } finally {
                released.countDown();
            }

            // and the slow one catches up once it gets past applying
            await("synchronization of the slow cache instance once released")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(cacheB.getIfPresent(Key.of(2))).isEqualTo(Value.of(2)));
        }

        @DisplayName("Test that a cache instance falling too far behind is reconciled instead")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_reconciles_a_cache_instance_that_falls_too_far_behind() throws Exception {
            // Persisting cached entries is what makes the recovery warm: reconciling keeps what the store confirms,
            // and without a store that retains them there is nothing to confirm against
            DistributedCache<Key, Value> cacheA = createCache(CacheBuilder.identity(),
                    dc -> dc.withPersistence(configurer -> configurer
                                    .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                            .build());
            Adapter<Key, Value> adapterB = createAdapter();
            // a limit low enough to be passed by a handful of writes, set before activation
            Synchronizer<?, ?> synchronizerB = readFieldValue(adapterB, AbstractAdapter.class,
                    "synchronizer", Synchronizer.class);
            writeFieldValue(synchronizerB, synchronizerB.getClass(), "pendingLimit", 5);
            DistributedCache<Key, Value> cacheB = createCache(adapterB, CacheBuilder.identity(),
                    dc -> dc.withPersistence(configurer -> configurer
                                    .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                            .build());

            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);
            loggerDistributedCaffeine.startCapturing();

            // cacheB applies the first thing it receives only once released, so that everything after it piles up
            InternalCacheManager<Key, Value> cacheManager = getInstanceRegistry(cacheB).getCacheManager();
            CountDownLatch released = new CountDownLatch(1);
            CountDownLatch held = new CountDownLatch(1);
            cacheB.distributedPolicy().getAdapter().setReceiver(new Receiver<>() {

                @Override
                public void receiveCacheEntries(@NonNull List<CacheEntry<Key, Value>> cacheEntries) {
                    held.countDown();
                    try {
                        released.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    cacheManager.receiveCacheEntries(cacheEntries);
                }

                @Override
                public void receiveSynchronizationRestart() {
                    cacheManager.receiveSynchronizationRestart();
                }
            });

            cacheA.put(Key.of(0), Value.of(0));
            assertThat(held.await(WAITING_DURATION.toMillis(), TimeUnit.MILLISECONDS)).isTrue();

            // more than the limit, while cacheB is still busy with the first
            List<Key> keys = IntStream.rangeClosed(1, 20).mapToObj(Key::of).toList();
            keys.forEach(key -> cacheA.put(key, Value.of(key.getId())));
            released.countDown();

            await("synchronization of everything despite falling behind")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> keys.forEach(key ->
                            assertThat(cacheB.getIfPresent(key)).isEqualTo(Value.of(key.getId()))));

            // and it got there by reconciling, rather than by applying everything that piled up
            assertThat(loggerDistributedCaffeine.getLoggingEvents())
                    .anySatisfy(loggingEvent -> {
                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                        assertThat(loggingEvent.getMessage()).startsWith("Synchronization was interrupted for cache at");
                    });
            loggerDistributedCaffeine.stopCapturing();
        }

        private Adapter<Key, Value> watchingAdapter(MongoClient watchingMongoClient) {
            return MongoAdapter.newBuilder(watchingMongoClient, DATABASE_NAME, getDatasetName()).build();
        }

        // Three cache instances watching through a client of their own - two on one collection, told apart by their
        // discriminators, and one on another collection - so that the change stream cursors that client holds are
        // the ones they share. Each of them has a peer on the shared client to be written to, so that what they
        // receive has to come through the cursor they share
        private void assertWatchersShared(WatcherSharingMode sharingMode, int expectedCursors) {
            String collectionName = getDatasetName();
            // still bounded, and still ending in the counter that makes it unique
            String otherCollectionName = "b_" + collectionName.substring(2);
            try (MongoClient watchingMongoClient = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(mongoContainer.getReplicaSetUrl()))
                    .applicationName(collectionName)
                    .build())) {
                List<DistributedCache<Key, Value>> watching = new ArrayList<>();
                List<DistributedCache<Key, Value>> writing = new ArrayList<>();
                List<List<String>> scopes = List.of(List.of(collectionName, "a"), List.of(collectionName, "b"),
                        List.of(otherCollectionName, "a"));
                try {
                    for (List<String> scope : scopes) {
                        watching.add(createCache(MongoAdapter.newBuilder(watchingMongoClient, DATABASE_NAME,
                                                scope.get(0))
                                        .withDiscriminator(scope.get(1))
                                        .withWatcherSharingMode(sharingMode)
                                        .build(),
                                CacheBuilder.identity(), DistributedCaffeine::build));
                        writing.add(createCache(MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, scope.get(0))
                                        .withDiscriminator(scope.get(1))
                                        .build(),
                                CacheBuilder.identity(), DistributedCaffeine::build));
                    }

                    for (int index = 0; index < scopes.size(); index++) {
                        writing.get(index).put(Key.of(index), Value.of(index));
                    }
                    for (int index = 0; index < scopes.size(); index++) {
                        DistributedCache<Key, Value> cache = watching.get(index);
                        Key key = Key.of(index);
                        Value value = Value.of(index);
                        await("synchronization over the cursor shared at " + sharingMode)
                                .atMost(WAITING_DURATION)
                                .untilAsserted(() -> assertThat(cache.getIfPresent(key)).isEqualTo(value));
                    }

                    // counted only now, long after the last cache instance joined, so that no cursor is being
                    // reopened wider while counting
                    assertThat(countChangeStreamCursors(collectionName)).isEqualTo(expectedCursors);
                } finally {
                    // torn down here rather than after the test, because their client is closed on the way out
                    watching.forEach(cache -> {
                        cache.distributedPolicy().stopSynchronization();
                        cache.invalidateAll();
                        distributedCacheInstances.remove(cache);
                    });
                }

                // and a cursor is given up once its last cache instance stops
                await("cursors given up")
                        .atMost(WAITING_DURATION)
                        .until(() -> countChangeStreamCursors(collectionName) == 0);
            }
        }

        // The change stream cursors that the client of the given application name holds on the test's database,
        // collected over a second. One look is not enough: a cursor passing between being polled and lying idle in
        // that very moment is listed as neither, so a single look can come up short - but not ten of them
        private int countChangeStreamCursors(String applicationName) {
            Set<Object> cursorIds = new HashSet<>();
            for (int look = 0; look < 10; look++) {
                changeStreamCursorsOf(applicationName).forEach(cursor -> cursorIds.add(cursor.get("cursorId")));
                sleep(Duration.ofMillis(100));
            }
            return cursorIds.size();
        }

        private List<Document> changeStreamCursorsOf(String applicationName) {
            List<Document> operations = mongoClient.getDatabase("admin")
                    .aggregate(List.of(new Document("$currentOp",
                            new Document("allUsers", true).append("idleCursors", true))))
                    .into(new ArrayList<>());
            List<Document> cursors = new ArrayList<>();
            for (Document operation : operations) {
                Document cursor = operation.get("cursor", Document.class);
                String namespace = operation.getString("ns");
                if (applicationName.equals(operation.getString("appName")) && nonNull(cursor) && nonNull(namespace)
                        && namespace.startsWith(DATABASE_NAME + ".")
                        && String.valueOf(cursor.get("originatingCommand")).contains("$changeStream")) {
                    cursors.add(new Document("ns", namespace).append("cursorId", cursor.get("cursorId")));
                }
            }
            return cursors;
        }

        private void publishWith(Serializer<Value, ?> valueSerializer, String discriminator, Value value)
                throws Exception {
            MongoAdapter<Key, Value> adapter = MongoAdapter
                    .newBuilder(mongoClient, DATABASE_NAME, getDatasetName())
                    .withDiscriminator(discriminator)
                    .build();
            adapter.setKeySerializer(new JavaObjectSerializer<>());
            adapter.setValueSerializer(valueSerializer);
            adapter.getRepository().orElseThrow().publishCacheEntries(List.of(
                    CacheEntry.of("h1", "op1", Key.of(1), value, CACHED, Instant.now().truncatedTo(MILLIS))));
        }

        // asserts against the winning plan only - rejected plans name stages that are never executed. The filter is
        // taken from the repository itself rather than rebuilt here, so that this cannot drift from what is queried
        private void assertThatQueryIsIndexed(Repository<Key, Value> repository, String collectionName,
                                              Set<String> hashes, Set<Status> statuses, Instant olderThan,
                                              Order order) throws Exception {
            Bson filter = invokeMethod(repository,
                    Class.forName("io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoRepository"),
                    "getFilter", List.of(Set.class, Set.class, Instant.class),
                    Arrays.asList(hashes, statuses, olderThan));
            FindIterable<Document> findIterable = mongoClient.getDatabase(DATABASE_NAME)
                    .getCollection(collectionName)
                    .find(filter);
            String timestamp = CacheEntry.Field.TIMESTAMP.toString();
            switch (order) {
                case ASCENDING -> findIterable = findIterable.sort(Sorts.ascending(timestamp));
                case DESCENDING -> findIterable = findIterable.sort(Sorts.descending(timestamp));
                case UNORDERED -> {
                    // nothing to sort by
                }
            }
            String winningPlan = findIterable.explain(ExplainVerbosity.QUERY_PLANNER)
                    .get("queryPlanner", Document.class)
                    .get("winningPlan", Document.class)
                    .toJson();
            assertThat(winningPlan)
                    .describedAs("%nWinning plan for filter %s", filter)
                    .doesNotContain("COLLSCAN");
            // An ordered read by status is what maintenance prunes with, over a group as large as it is configured
            // to be: sorting it in memory would cost that much memory on the server, and fail outright beyond the
            // server's limit for it. The index has to deliver the order itself, merged across the statuses
            if (order != Order.UNORDERED && isNull(hashes) && nonNull(statuses)) {
                assertThat(winningPlan)
                        .describedAs("%nWinning plan for filter %s ordered %s", filter, order)
                        .doesNotContain("\"SORT\"");
            }
        }

        @Override
        void startStore(DockerImageName dockerImageName, String displayName) {
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

        @Override
        void stopStore() {
            this.mongoClient.close();
            this.mongoContainer.stop();
        }

        // before MongoDB 4.4 the fully qualified namespace is capped at 120 bytes, and a collection name is
        // derived from a test method name, which can be longer than what the database name leaves for it. Bounded
        // from the front like the table name of the PostgreSQL tests above, and for the same reason: the tests of
        // a group share their prefix, so the tail is what tells them apart, and the counter alone already makes it
        // unique
        @Override
        String getDatasetName() {
            int maximumLength = 120 - DATABASE_NAME.length() - 1;
            String name = super.getDatasetName();
            return name.length() <= maximumLength ? name : name.substring(name.length() - maximumLength);
        }

        // the position the watcher a synchronizer currently subscribes through resumes from - shared by every
        // cache instance on that watcher, which is why it is reached through the subscription
        AtomicReference<?> resumeTokenOf(Synchronizer<?, ?> synchronizer) {
            Object watcher = watcherOf(synchronizer);
            return readFieldValue(watcher, watcher.getClass(), "resumeToken", AtomicReference.class);
        }

        // the watcher a synchronizer currently subscribes through
        Object watcherOf(Synchronizer<?, ?> synchronizer) {
            AtomicReference<?> subscription = readFieldValue(synchronizer, synchronizer.getClass(), "subscription",
                    AtomicReference.class);
            Object activeSubscription = requireNonNull(subscription.get());
            return readFieldValue(activeSubscription, activeSubscription.getClass(), "watcher", Object.class);
        }

        @Override
        <K, V> Adapter<K, V> createAdapter() {
            return MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, getDatasetName()).build();
        }

        @Override
        <K, V> Adapter<K, V> createAdapter(String discriminator) {
            return MongoAdapter.newBuilder(mongoClient, DATABASE_NAME, getDatasetName())
                    .withDiscriminator(discriminator)
                    .build();
        }

    }

    @SuppressWarnings({"java:S5838", "java:S5778", "SqlNoDataSourceInspection", "SqlSourceToSinkFlow"})
    abstract static class PostgresIntegration extends CommonIntegration {
        // whichever index serves it, for a predicate so broad that preferring one of them would be the wrong plan
        private static final Set<String> ANY_INDEX = Set.of("Index");

        // what the adapter sends a notification with, spelled as it spells it
        private static final String NOTIFY_STATEMENT = "SELECT pg_notify(?, ?)";

        static final String SCHEMA_NAME = "public";

        PostgreSQLContainer postgresContainer;

        DataSource dataSource;

        @DisplayName("Test that the adapter names and separates its scope by discriminator")
        @Test
        void test_Adapter_scopes_by_discriminator() throws Exception {
            String tableName = getDatasetName();

            DistributedCache<Key, Value> cacheA = createCache(
                    "a", CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheInDefaultScope = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            // every cache has a discriminator, so the identifier always carries one - and the identifier is what
            // the notification channel is derived from and what every log line names the cache by
            assertThat(cacheA.distributedPolicy().getAdapter().getIdentifier())
                    .isEqualTo(String.join(":", "postgresql", SCHEMA_NAME, tableName, "a"));
            assertThat(cacheInDefaultScope.distributedPolicy().getAdapter().getIdentifier())
                    .isEqualTo(String.join(":", "postgresql", SCHEMA_NAME, tableName, DEFAULT_DISCRIMINATOR));

            // uniqueness in the store is (discriminator, hash), which is exactly the primary key, so the same
            // key coexists once per scope
            Key key = Key.of(1);
            cacheA.put(key, Value.of(1));
            cacheInDefaultScope.put(key, Value.of(2));

            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(format(
                         "SELECT count(*) FROM \"%s\".\"%s\"", SCHEMA_NAME, tableName));
                 ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getLong(1)).isEqualTo(2);
            }
        }

        @DisplayName("Test that a name PostgreSQL would need quoting for is used exactly as it was given")
        @Test
        void test_Adapter_uses_a_name_that_needs_quoting() throws Exception {
            // The names a plain-identifier rule would refuse, and which a schema this adapter has to live with may
            // well already use: a table some migration tool hyphenated, a name with a space, one carrying a quote
            // of its own. What makes them safe is the quoting every statement puts them in, not their shape - an
            // embedded quote is doubled on the way in, so it cannot end the identifier it sits in
            String tableName = "cache-entries \"odd\"";
            PostgresAdapter<Key, Value> adapter = PostgresAdapter
                    .newBuilder(dataSource, SCHEMA_NAME, tableName)
                    .build();
            adapter.setKeySerializer(new JavaObjectSerializer<>());
            adapter.setValueSerializer(new JacksonSerializer<>(Value.class, false));
            Repository<Key, Value> repository = adapter.getRepository().orElseThrow();

            // the table is created, written and read back under that name like any other
            repository.publishCacheEntries(List.of(CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED,
                    Instant.now().truncatedTo(MICROS))));

            try (Stream<CacheEntry<Key, Value>> cacheEntries = repository.streamCacheEntries(null, null, UNORDERED)) {
                assertThat(cacheEntries.toList())
                        .singleElement()
                        .satisfies(cacheEntry -> assertThat(cacheEntry.getValue()).isEqualTo(Value.of(1)));
            }

            // and it really is the name that was asked for, rather than one the server made of it
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM "
                         + "information_schema.tables WHERE table_schema = ? AND table_name = ?")) {
                statement.setString(1, SCHEMA_NAME);
                statement.setString(2, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getInt(1)).isEqualTo(1);
                }
            }
        }

        @DisplayName("Test that a timestamp is one and the same instant whatever time zone writes or reads it")
        @Test
        void test_Repository_keeps_timestamps_absolute() throws Exception {
            Repository<Key, Value> repository = repositoryFor(null);

            // microseconds, which is the resolution the column keeps, and a value carrying more than that
            Instant exact = Instant.parse("2026-07-01T12:34:56.123456Z");
            Instant finerThanTheColumn = Instant.parse("2026-07-01T12:34:56.123456789Z");

            TimeZone originalTimeZone = TimeZone.getDefault();
            try {
                // written from a machine well ahead of UTC...
                TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"));
                repository.publishCacheEntries(List.of(
                        CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, exact),
                        CacheEntry.of("h2", "op2", Key.of(2), Value.of(2), CACHED, finerThanTheColumn)));

                // ...and read back on one well behind it, which changes nothing: the column is timestamptz, so what
                // it holds is a point in time and not a reading of a clock, and neither machine's zone is part of it
                TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
                try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(Set.of("h1"), null, UNORDERED)) {
                    assertThat(stream.toList())
                            .singleElement()
                            .satisfies(cacheEntry -> assertThat(cacheEntry.getTimestamp()).isEqualTo(exact));
                }

                // and what the store holds is that instant in UTC, rather than either machine's reading of it
                try (Connection connection = dataSource.getConnection();
                     PreparedStatement statement = connection.prepareStatement(format(
                             "SELECT \"%s\" AT TIME ZONE 'UTC' FROM \"%s\".\"%s\" WHERE hash = 'h1'",
                             CacheEntry.Field.TIMESTAMP, SCHEMA_NAME, getDatasetName()))) {
                    try (ResultSet resultSet = statement.executeQuery()) {
                        assertThat(resultSet.next()).isTrue();
                        assertThat(resultSet.getString(1)).isEqualTo("2026-07-01 12:34:56.123456");
                    }
                }

                // Anything finer than the column is rounded to what it can hold, rather than truncated - which is
                // worth knowing but costs nothing: MongoDB keeps its dates to the millisecond, a thousand times
                // coarser, and what the engine does with a timestamp is order and age it
                try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(Set.of("h2"), null, UNORDERED)) {
                    assertThat(stream.toList())
                            .singleElement()
                            .satisfies(cacheEntry -> assertThat(cacheEntry.getTimestamp())
                                    .isEqualTo(Instant.parse("2026-07-01T12:34:56.123457Z")));
                }
            } finally {
                TimeZone.setDefault(originalTimeZone);
            }
        }

        @DisplayName("Test that records which are no cache entries are reported and skipped, while their metadata still reads")
        @Test
        void test_Repository_skips_records_that_are_no_cache_entries() throws Exception {
            Repository<Key, Value> repository = repositoryFor(null);
            Instant timestamp = Instant.now().truncatedTo(MICROS).minusSeconds(30);

            // reading a record that is no cache entry is reported before it is skipped, so the warning is
            // captured and asserted instead of ending up, with its stack trace, in the test output
            CaptureLogger loggerPostgresRepository = CaptureLoggerFactory.getCaptureLogger(
                    "io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresRepository");
            loggerPostgresRepository.startCapturing();

            // a record whose key and value cannot be deserialized is no cache entry - skipped, logged and left
            // out - while its metadata is returned, which it could only be if reading metadata never touches the
            // payload. Unlike MongoDB there is no counterpart for a record missing what metadata needs: the
            // columns the metadata is read from are NOT NULL, so the schema refuses such a row outright
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(format(
                         "INSERT INTO \"%s\".\"%s\" (discriminator, hash, operation, key_binary, value_text, "
                                 + "status, timestamp) VALUES (?, 'broken', 'op4', ?, 'not a serialized value', "
                                 + "?, ?)",
                         SCHEMA_NAME, getDatasetName()))) {
                statement.setString(1, DEFAULT_DISCRIMINATOR);
                statement.setBytes(2, "not a serialized key".getBytes(StandardCharsets.UTF_8));
                statement.setString(3, CACHED.toString());
                statement.setObject(4, timestamp.atOffset(ZoneOffset.UTC));
                statement.executeUpdate();
            }

            try (Stream<CacheEntry<Key, Value>> stream =
                         repository.streamCacheEntries(Set.of("broken"), null, UNORDERED)) {
                assertThat(stream.toList()).isEmpty();
            }
            try (Stream<CacheEntryMetadata> stream =
                         repository.streamCacheEntryMetadata(Set.of("broken"), null, UNORDERED)) {
                assertThat(stream.toList())
                        .singleElement()
                        .isEqualTo(CacheEntryMetadata.of("broken", "op4", CACHED, timestamp));
            }

            assertThat(loggerPostgresRepository.getLoggingEvents())
                    .singleElement()
                    .satisfies(loggingEvent -> {
                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                        assertThat(loggingEvent.getMessage()).startsWith("Reading of cache entry failed");
                        assertThat(loggingEvent.getThrowable()).isNotNull();
                    });
            loggerPostgresRepository.stopCapturing();
        }

        @DisplayName("Test that the repository streams unbounded reads in batches and reads by hash in one go")
        @Test
            // In autocommit mode the driver reads a whole result into memory before handing out the first row, so a
            // stream would be lazy in name only. Fetching in batches needs a transaction instead, which is what can be
            // seen from outside: the connection sits in it between batches - and must not once the stream is closed,
            // since an open one keeps vacuum from collecting what died meanwhile. A read by hash is bounded by the
            // hashes asked for, so it has nothing to fetch in batches and must not pay the transaction's round trip:
            // its connection is never seen in one, not even with the stream still open
        void test_Repository_streams_in_batches() throws Exception {
            Repository<Key, Value> repository = repositoryFor(null);
            // more than one batch, so that the stream cannot have been read to the end with its first one
            Instant timestamp = Instant.now().truncatedTo(MICROS);
            List<CacheEntry<Key, Value>> cacheEntries = new ArrayList<>();
            for (int index = 0; index < 2_500; index++) {
                cacheEntries.add(CacheEntry.of("h" + index, "op", Key.of(index), Value.of(index),
                        EVICTED_SIZE_RETAINED, timestamp.minusMillis(index)));
            }
            repository.publishCacheEntries(cacheEntries);

            for (boolean metadataOnly : List.of(false, true)) {
                try (Stream<?> stream = metadataOnly
                        ? repository.streamCacheEntryMetadata(null, EVICTED_RETAINED_GROUP, DESCENDING)
                        : repository.streamCacheEntries(null, null, UNORDERED)) {
                    assertThat(stream.iterator().next()).isNotNull();
                    assertThat(countReadingInTransaction())
                            .as("reading in a transaction while open (metadata only: %s)", metadataOnly)
                            .isEqualTo(1);
                }
                assertThat(countReadingInTransaction())
                        .as("reading in a transaction once closed (metadata only: %s)", metadataOnly)
                        .isZero();
            }

            // as many hashes as delivering reads at most at once, and all of them have to come back
            Set<String> hashes = IntStream.range(0, 500)
                    .mapToObj(index -> "h" + index)
                    .collect(toSet());
            for (boolean metadataOnly : List.of(false, true)) {
                try (Stream<?> stream = metadataOnly
                        ? repository.streamCacheEntryMetadata(hashes, null, UNORDERED)
                        : repository.streamCacheEntries(hashes, null, UNORDERED)) {
                    Iterator<?> iterator = stream.iterator();
                    assertThat(iterator.next()).isNotNull();
                    assertThat(countReadingInTransaction())
                            .as("reading by hash in a transaction while open (metadata only: %s)", metadataOnly)
                            .isZero();
                    int read = 1;
                    for (; iterator.hasNext(); iterator.next()) {
                        read++;
                    }
                    assertThat(read).isEqualTo(hashes.size());
                }
            }
        }

        @DisplayName("Test that every query the repository issues is served by the index meant for it")
        @Test
        void test_Repository_queries_avoid_sequential_scans() throws Exception {
            // both scopes populated, so that the discriminator actually discriminates instead of matching every
            // row - and one of them the default scope, which an index has to serve like any other
            Repository<Key, Value> repositoryWithDiscriminator = repositoryFor("d1");
            Repository<Key, Value> repositoryInDefaultScope = repositoryFor(null);

            // enough rows, spread over timestamps and with a rare status among them, that every predicate below is
            // selective enough for an index to be the better plan rather than merely a permitted one: on a handful
            // of rows, or for a filter matching most of them, PostgreSQL reads the table and is right to
            Instant timestamp = Instant.now().truncatedTo(MICROS);
            for (Repository<Key, Value> repository : List.of(repositoryWithDiscriminator, repositoryInDefaultScope)) {
                List<CacheEntry<Key, Value>> cacheEntries = new ArrayList<>();
                for (int index = 0; index < 2_000; index++) {
                    cacheEntries.add(CacheEntry.of("h" + index, "op", Key.of(index), Value.of(index),
                            index % 400 == 0 ? EVICTED_SIZE_RETAINED : CACHED, timestamp.minusSeconds(index)));
                }
                repository.publishCacheEntries(cacheEntries);
            }

            Set<String> hashes = Set.of("h1", "h2");
            Set<Status> statuses = Set.of(EVICTED_SIZE_RETAINED);
            Instant deadline = timestamp.minusSeconds(1_990);

            try (Connection connection = dataSource.getConnection()) {
                // the planner needs statistics before it can prefer anything
                try (Statement statement = connection.createStatement()) {
                    statement.execute(format("ANALYZE \"%s\".\"%s\"", SCHEMA_NAME, getDatasetName()));
                }

                // Which index is meant for which query is read off the table rather than spelled out here, so that
                // renaming one cannot quietly turn the assertions below into assertions about nothing. Naming the
                // index is what gives them teeth: every predicate leads with the discriminator, and so does the
                // primary key, so "some index was used" is satisfied by the wrong index just as well as by the right
                Map<String, String> indexes = readIndexes(connection);
                assertThat(indexes).hasSize(2);
                String byHash = indexNameOf(indexes, false);
                String byStatusAndTimestamp = indexNameOf(indexes, true);

                // a query narrowed from both sides can be answered from either end, and which end is cheaper is
                // the planner's judgement rather than a property of the table
                Set<String> eitherIndex = Set.of(byHash, byStatusAndTimestamp);

                for (Repository<Key, Value> repository : List.of(repositoryWithDiscriminator,
                        repositoryInDefaultScope)) {
                    // asking by hash is the primary key's own question
                    assertThatQueryIsIndexed(connection, repository, hashes, null, null, UNORDERED, Set.of(byHash));
                    assertThatQueryIsIndexed(connection, repository, hashes, statuses, null, UNORDERED, eitherIndex);
                    assertThatQueryIsIndexed(connection, repository, hashes, null, deadline, UNORDERED, eitherIndex);
                    assertThatQueryIsIndexed(connection, repository, hashes, statuses, deadline, UNORDERED,
                            eitherIndex);

                    // and asking by status or by age is what the second index is there for, the two together being
                    // what maintenance sweeps with - which the primary key cannot answer at all
                    assertThatQueryIsIndexed(connection, repository, null, statuses, null, UNORDERED,
                            Set.of(byStatusAndTimestamp));
                    assertThatQueryIsIndexed(connection, repository, null, null, deadline, UNORDERED,
                            Set.of(byStatusAndTimestamp));
                    assertThatQueryIsIndexed(connection, repository, null, statuses, deadline, UNORDERED,
                            Set.of(byStatusAndTimestamp));

                    // ordered, as synchronizing cache entries on activation reads - which is the one
                    // shape that never carries an age to filter by
                    assertThatQueryIsIndexed(connection, repository, hashes, null, null, ASCENDING, Set.of(byHash));
                    assertThatQueryIsIndexed(connection, repository, hashes, statuses, null, ASCENDING, eitherIndex);
                    assertThatQueryIsIndexed(connection, repository, null, statuses, null, ASCENDING,
                            Set.of(byStatusAndTimestamp));
                    // and newest first, as pruning by size reads
                    assertThatQueryIsIndexed(connection, repository, null, statuses, null, DESCENDING,
                            Set.of(byStatusAndTimestamp));
                }

                // What remains is the discriminator on its own, which counting a scope and reading it whole ask -
                // and there a sequential scan is not a missing index but the right plan, because the filter matches
                // most of the table. All that can be asserted of those is that an index path exists at all, which
                // is what pricing sequential scans out of the way asks. Set for the transaction rather than for the
                // session, because the connection goes back into a pool afterwards and would otherwise carry the
                // setting over to whoever gets it next
                connection.setAutoCommit(false);
                try (Statement statement = connection.createStatement()) {
                    statement.execute("SET LOCAL enable_seqscan = off");
                    for (Repository<Key, Value> repository : List.of(repositoryWithDiscriminator,
                            repositoryInDefaultScope)) {
                        assertThatQueryIsIndexed(connection, repository, null, null, null, UNORDERED, ANY_INDEX);
                        assertThatQueryIsIndexed(connection, repository, null, null, null, ASCENDING, ANY_INDEX);
                    }
                } finally {
                    // nothing was written, and the setting goes with the transaction that carried it
                    connection.rollback();
                    connection.setAutoCommit(true);
                }
            }
        }

        @DisplayName("Test that a role which may only read and write can use a table it was not allowed to create")
        @Test
        void test_Repository_uses_a_table_it_may_not_create() throws Exception {
            // created by somebody who may, which is what a migration is
            repositoryFor(null);

            String role = "r" + Integer.toHexString(getDatasetName().hashCode());
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute(format("CREATE ROLE %s LOGIN PASSWORD 'secret'", role));
                statement.execute(format("GRANT USAGE ON SCHEMA \"%s\" TO %s", SCHEMA_NAME, role));
                // reading and writing only - no CREATE on the schema and no ownership of the table, which is how
                // an application role is commonly granted
                statement.execute(format("GRANT SELECT, INSERT, UPDATE, DELETE ON \"%s\".\"%s\" TO %s",
                        SCHEMA_NAME, getDatasetName(), role));
            }

            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(postgresContainer.getJdbcUrl());
            hikariConfig.setUsername(role);
            hikariConfig.setPassword("secret");
            hikariConfig.setMaximumPoolSize(2);
            try (HikariDataSource restricted = new HikariDataSource(hikariConfig)) {
                PostgresAdapter<Key, Value> adapter = PostgresAdapter
                        .newBuilder(restricted, SCHEMA_NAME, getDatasetName())
                        .build();
                adapter.setKeySerializer(new JavaObjectSerializer<>());
                adapter.setValueSerializer(new JacksonSerializer<>(Value.class, false));
                Repository<Key, Value> repository = adapter.getRepository().orElseThrow();

                // constructing it is what used to fail, because CREATE TABLE IF NOT EXISTS is refused on the
                // privilege before PostgreSQL ever looks at whether the table is there
                repository.publishCacheEntries(List.of(CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED,
                        Instant.now().truncatedTo(MICROS))));
                assertThat(repository.countCacheEntries(null, null)).isEqualTo(1);
            }
        }

        @DisplayName("Test that a table shaped for another version is reported at construction, not at the first write")
        @Test
        void test_Repository_rejects_a_table_of_another_shape() throws Exception {
            // the shape this library had before the JSON column was renamed, which CREATE TABLE IF NOT EXISTS
            // would leave exactly as it is
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute(format("CREATE TABLE \"%s\".\"%s\" (discriminator text NOT NULL, "
                                + "hash text NOT NULL, operation text, key_binary bytea, key_text text, "
                                + "key_json jsonb, value_binary bytea, value_text text, value_json jsonb, "
                                + "status text NOT NULL, timestamp timestamptz NOT NULL)",
                        SCHEMA_NAME, getDatasetName()));
            }

            assertThatThrownBy(() -> repositoryFor(null))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("is not shaped as this version expects")
                    .hasMessageContaining("key_jsonb expected as jsonb but missing")
                    .hasMessageContaining("value_jsonb expected as jsonb but missing");
        }

        @DisplayName("Test that each serializer stores into a column of its own, so that records stay legible")
        @Test
        void test_Repository_uses_a_column_per_serializer() throws Exception {
            // the same value written three ways, by three caches sharing the table under discriminators of their own
            publish("binary", new JavaObjectSerializer<>());
            publish("text", new JacksonSerializer<>(Value.class, false));
            publish("jsonb", new JacksonSerializer<>(Value.class, true));

            assertThat(columnsOf("binary")).containsExactly("value_binary");
            assertThat(columnsOf("text")).containsExactly("value_text");
            assertThat(columnsOf("jsonb")).containsExactly("value_jsonb");

            // and what a JSON serializer wrote is the store's own representation, so it can be read and queried as
            // such rather than decoded first - which is the whole point of not putting everything into one column
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(format(
                         "SELECT value_jsonb ->> 'name' FROM \"%s\".\"%s\" WHERE discriminator = 'jsonb'",
                         SCHEMA_NAME, getDatasetName()))) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getString(1)).isEqualTo("value");
                }
            }
        }

        @DisplayName("Test that tables whose index names would collide once truncated each keep an index of their own")
        @Test
        void test_Repository_keeps_index_names_apart() throws Exception {
            // A pair chosen so that appending the index suffix and truncating at the identifier length the server
            // allows yields one and the same name for both, which it does because the suffix opens with "_status" and
            // the longer table is named after the shorter one plus exactly that. Both are distinct tables, and both
            // are short enough that nothing truncates the table names themselves
            String shorter = "t".repeat(55);
            String longer = shorter + "_status";

            PostgresAdapter<Key, Value> shorterAdapter = PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, shorter)
                    .build();
            PostgresAdapter<Key, Value> longerAdapter = PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, longer)
                    .build();

            assertThat(shorterAdapter.getRepository()).isPresent();
            assertThat(longerAdapter.getRepository()).isPresent();
            // one index from the primary key and one from the adapter, for each of the two tables - where a shared
            // name would have left the second table with the primary key alone, and silently, because creating an
            // index that is already there is not an error
            assertThat(indexCountOf(shorter)).isEqualTo(2);
            assertThat(indexCountOf(longer)).isEqualTo(2);
        }

        @DisplayName("Test that a write the server rolled back for reasons other than its own is simply made again")
        @Test
        void test_Repository_retries_a_rolled_back_write() throws Exception {
            // "40P01" is what a deadlock reaches a client as, which is what concurrent maintenance runs into: the
            // server rolls one of the two transactions back, and nothing of it was committed
            AtomicInteger connections = new AtomicInteger();
            AtomicBoolean rollbackNext = new AtomicBoolean();
            DataSource rollingBack = rollingBackOnce(rollbackNext, "40P01", connections);
            Repository<Key, Value> repository = repositoryUsing(rollingBack);

            int before = connections.get();
            rollbackNext.set(true);
            repository.publishCacheEntries(List.of(
                    CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, Instant.now().truncatedTo(MICROS))));

            // one attempt that was rolled back and one that went through, and the caller saw no failure at all
            assertThat(connections.get() - before).isEqualTo(2);
            try (Stream<CacheEntry<Key, Value>> cacheEntries = repository.streamCacheEntries(null, null, UNORDERED)) {
                assertThat(cacheEntries).hasSize(1);
            }
        }

        @DisplayName("Test that a write that failed for its own reasons is reported rather than made again")
        @Test
        void test_Repository_does_not_retry_a_plain_failure() throws Exception {
            // "42P01" is a table that is not there, which asking again cannot mend
            AtomicInteger connections = new AtomicInteger();
            AtomicBoolean failNext = new AtomicBoolean();
            DataSource failing = rollingBackOnce(failNext, "42P01", connections);
            Repository<Key, Value> repository = repositoryUsing(failing);

            int before = connections.get();
            failNext.set(true);
            assertThatThrownBy(() -> repository.publishCacheEntries(List.of(
                    CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, Instant.now().truncatedTo(MICROS)))))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("provoked");

            assertThat(connections.get() - before).isEqualTo(1);
        }

        @DisplayName("Test that a write whose announcement failed leaves neither a record nor a notification")
        @Test
        void test_Repository_rolls_back_a_write_it_could_not_announce() throws Exception {
            // A notification carries only what its writer announces, so a record kept while its announcement was
            // lost is one no other cache instance ever learns of - which is why the write and the notification of
            // it share a transaction. Failing inside that transaction is what shows it: before the notification
            // was sent, and after it was sent but before the commit that would make it deliverable
            Repository<Key, Value> reading = repositoryFor(null);
            AtomicBoolean refuseNext = new AtomicBoolean();
            PostgresAdapter<Key, Value> adapter = adapterUsing(refusing(refuseNext,
                    (method, arguments) -> "prepareStatement".equals(method)
                            && NOTIFY_STATEMENT.equals(arguments[0])));
            Repository<Key, Value> repository = adapter.getRepository().orElseThrow();
            String channel = invokeMethod(null,
                    Class.forName("io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresChannel"),
                    "channelOf", List.of(String.class), List.of(adapter.getIdentifier()));

            try (Connection listener = dataSource.getConnection()) {
                // for the same reason the adapter does: a pooled connection may arrive still subscribed
                execute(listener, "UNLISTEN *");
                execute(listener, "LISTEN " + channel);
                // and dropping what it had already been handed, for the same reason the adapter does it: this
                // connection comes from a pool, so a cache instance of an earlier test may have left notifications
                // queued on the session, and unsubscribing stops what comes next rather than discarding those
                drain(listener);
                try {
                    // the announcement cannot be sent at all
                    refuseNext.set(true);
                    assertThatThrownBy(() -> repository.publishCacheEntries(List.of(CacheEntry.of("h1", "op1",
                            Key.of(1), Value.of(1), CACHED, Instant.now().truncatedTo(MICROS)))))
                            .isInstanceOf(SQLException.class)
                            .hasMessageContaining("provoked");

                    assertThat(reading.countCacheEntries(null, null)).isEqualTo(0);
                    assertThat(notificationsOf(listener, 1)).isEmpty();

                    // and the same once it has been sent, which is the half a rollback has to reach into: the
                    // server holds a notification until the transaction that issued it commits, so rolling back
                    // takes it with the record rather than leaving it to be delivered on its own
                    PostgresAdapter<Key, Value> uncommitting = adapterUsing(refusing(refuseNext,
                            (method, arguments) -> "commit".equals(method)));
                    refuseNext.set(true);
                    assertThatThrownBy(() -> uncommitting.getRepository().orElseThrow()
                            .publishCacheEntries(List.of(CacheEntry.of("h2", "op2", Key.of(2), Value.of(2),
                                    CACHED, Instant.now().truncatedTo(MICROS)))))
                            .isInstanceOf(SQLException.class)
                            .hasMessageContaining("provoked");

                    assertThat(reading.countCacheEntries(null, null)).isEqualTo(0);
                    assertThat(notificationsOf(listener, 1)).isEmpty();

                    // and with nothing refused both arrive, which is what makes the assertions above say that
                    // nothing was delivered rather than that nothing was listening
                    repository.publishCacheEntries(List.of(CacheEntry.of("h3", "op3", Key.of(3), Value.of(3),
                            CACHED, Instant.now().truncatedTo(MICROS))));

                    assertThat(reading.countCacheEntries(null, null)).isEqualTo(1);
                    assertThat(notificationsOf(listener, 1))
                            .singleElement()
                            .satisfies(payload -> assertThat(payload).contains("h3"));
                } finally {
                    // the connection goes back to the pool, and a pooled connection still listening would hand
                    // another test notifications it never asked for
                    execute(listener, "UNLISTEN " + channel);
                }
            }
        }

        @DisplayName("Test that a population reaches another cache instance on the same table")
        @Test
        void test_Synchronizer_distributes_a_population() {
            DistributedCache<Key, Value> cacheA = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheB = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            Key key = Key.of(1);
            Value value = Value.of(1);

            cacheA.put(key, value);

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(cacheB.getIfPresent(key)).isEqualTo(value));
        }

        @DisplayName("Test that an invalidation reaches another cache instance")
        @Test
        void test_Synchronizer_distributes_an_invalidation() {
            DistributedCache<Key, Value> cacheA = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheB = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

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

        @DisplayName("Test that a batch of cache entries arrives as a batch")
        @Test
        void test_Synchronizer_distributes_a_batch() {
            DistributedCache<Key, Value> cacheA = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheB = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            List<Key> keys = IntStream.rangeClosed(1, 200)
                    .mapToObj(Key::of)
                    .toList();
            keys.forEach(key -> cacheA.put(key, Value.of(key.getId())));

            await("synchronization of every cache entry")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(cacheB.estimatedSize()).isEqualTo(keys.size()));
            assertThat(cacheB.getIfPresent(Key.of(200))).isEqualTo(Value.of(200));
        }

        @DisplayName("Test that a status transition made in the store reaches another cache instance")
        @Test
        void test_Synchronizer_distributes_a_status_transition() throws Exception {
            DistributedCache<Key, Value> cacheA = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheB = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

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

        @DisplayName("Test that cache instances of different discriminators do not hear each other")
        @Test
        void test_Synchronizer_separates_discriminators() {
            DistributedCache<Key, Value> cacheA = createCache("a",
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheB = createCache("b",
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheA2 = createCache("a",
                    CacheBuilder.identity(), DistributedCaffeine::build);

            Key key = Key.of(1);

            cacheA.put(key, Value.of(1));

            await("synchronization within the shared discriminator")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(cacheA2.getIfPresent(key)).isEqualTo(Value.of(1)));

            // the other scope had its chance on the same table while that one arrived
            assertThat(cacheB.getIfPresent(key)).isNull();
        }

        @DisplayName("Test that a cache instance that failed to apply what it received recovers by reconciling")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_recovers_a_cache_instance_that_failed_to_apply() {
            // Persisting cached entries is what makes the recovery warm: reconciling keeps what the store confirms,
            // and without a store that retains them there is nothing to confirm against
            DistributedCache<Key, Value> cacheA = createCache(CacheBuilder.identity(),
                    dc -> dc.withPersistence(configurer -> configurer
                                    .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                            .build());
            DistributedCache<Key, Value> cacheB = createCache(CacheBuilder.identity(),
                    dc -> dc.withPersistence(configurer -> configurer
                                    .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                            .build());

            Key key1 = Key.of(1);
            Key key2 = Key.of(2);

            cacheA.put(key1, Value.of(1));

            await("synchronization between cache instances")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(cacheB.getIfPresent(key1)).isEqualTo(Value.of(1)));

            // Failing the apply drops what was taken for it and has this cache instance alone recover by restarting -
            // without the session, which other cache instances may share, being given up for it. Handing over cache
            // entries keeps failing throughout, so the only way the value below can arrive is the reconcile that
            // restarting performs, which is exactly what is under test here
            // every failed attempt is reported, so the provoked ones below are captured and asserted instead of
            // ending up, with their stack traces, in the test output
            CaptureLogger loggerPostgresSynchronizer = CaptureLoggerFactory.getCaptureLogger(
                    "io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresSynchronizer");
            loggerPostgresSynchronizer.startCapturing();

            // restarting reconciles the cache against the data store, and reports that it did - captured for
            // the same reason and asserted below, because that reconcile is what the recovery under test consists of
            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);
            loggerDistributedCaffeine.startCapturing();

            InternalCacheManager<Key, Value> cacheManager = getInstanceRegistry(cacheB).getCacheManager();
            AtomicBoolean failing = new AtomicBoolean(true);
            cacheB.distributedPolicy().getAdapter().setReceiver(new Receiver<>() {

                @Override
                public void receiveCacheEntries(@NonNull List<CacheEntry<Key, Value>> cacheEntries) {
                    if (failing.get()) {
                        throw new IllegalStateException("provoked");
                    }
                    cacheManager.receiveCacheEntries(cacheEntries);
                }

                @Override
                public void receiveSynchronizationRestart() {
                    cacheManager.receiveSynchronizationRestart();
                }
            });

            cacheA.put(key2, Value.of(2));

            await("recovery of what was published while not listening")
                    .atMost(EXTENDED_WAITING_DURATION) // the retry delay grows with every failure
                    .untilAsserted(() -> assertThat(cacheB.getIfPresent(key2)).isEqualTo(Value.of(2)));

            // the listener really did fail and say so, which is what the recovery above is a recovery from
            assertThat(loggerPostgresSynchronizer.getLoggingEvents())
                    .anySatisfy(loggingEvent -> {
                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                        assertThat(loggingEvent.getMessage()).startsWith("Receiving notifications failed");
                        assertThat(loggingEvent.getThrowable()).hasMessage("provoked");
                    });
            loggerPostgresSynchronizer.stopCapturing();

            // and what brought the value back is the reconcile that restarting performs, which says so itself
            assertThat(loggerDistributedCaffeine.getLoggingEvents())
                    .anySatisfy(loggingEvent -> {
                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                        assertThat(loggingEvent.getMessage())
                                .startsWith("Synchronization was interrupted for cache at");
                    });
            loggerDistributedCaffeine.stopCapturing();

            failing.set(false);
        }

        @DisplayName("Test that a listening connection that silently stops carrying anything is noticed and replaced")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_replaces_a_listening_connection_that_went_silent() throws Exception {
            // cacheB reaches the server through a proxy that can go silent on the connections it is carrying while
            // keeping them open - what a failover moving the server's address or an expired NAT entry leaves a
            // client with: nothing is reset, nothing arrives, and a connection that only waits never finds out
            try (SilencingProxy proxy = new SilencingProxy(postgresContainer.getHost(),
                    postgresContainer.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT), executorService)) {
                try (HikariDataSource proxiedDataSource = createProxiedDataSource(proxy)) {
                    // Persisting cached entries is what makes the recovery warm, as in the test above
                    DistributedCache<Key, Value> cacheA = createCache(CacheBuilder.identity(),
                            dc -> dc.withPersistence(configurer -> configurer
                                            .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                                    .build());
                    // the heartbeat shortened before activation, so that noticing takes seconds rather than the
                    // production interval and timeout - the test is about the noticing, not about how long it takes
                    Adapter<Key, Value> adapterB = PostgresAdapter.newBuilder(
                            proxiedDataSource, SCHEMA_NAME, getDatasetName()).build();
                    Synchronizer<?, ?> synchronizerB = readFieldValue(adapterB, AbstractAdapter.class,
                            "synchronizer", Synchronizer.class);
                    writeFieldValue(synchronizerB, synchronizerB.getClass(), "heartbeatInterval", Duration.ofSeconds(1));
                    writeFieldValue(synchronizerB, synchronizerB.getClass(), "heartbeatTimeout", Duration.ofSeconds(1));
                    DistributedCache<Key, Value> cacheB = createCache(adapterB,
                            CacheBuilder.identity(),
                            dc -> dc.withPersistence(configurer -> configurer
                                            .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                                    .build());
                    try {
                        Key key1 = Key.of(1);
                        Key key2 = Key.of(2);

                        cacheA.put(key1, Value.of(1));

                        await("synchronization through the proxy")
                                .atMost(WAITING_DURATION)
                                .untilAsserted(() -> assertThat(cacheB.getIfPresent(key1)).isEqualTo(Value.of(1)));

                        // the replacement failure is reported, and so is the reconcile that follows it - captured
                        // and asserted rather than left in the test output
                        CaptureLogger loggerPostgresSynchronizer = CaptureLoggerFactory.getCaptureLogger(
                                "io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresListener");
                        loggerPostgresSynchronizer.startCapturing();
                        CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                                .getCaptureLogger(DistributedCaffeine.class);
                        loggerDistributedCaffeine.startCapturing();

                        // from here on, whatever cacheB had open carries nothing in either direction, while a
                        // connection opened afterwards goes through - the server is reachable again, the old
                        // session just does not know it
                        proxy.silenceOpenConnections();

                        cacheA.put(key2, Value.of(2));

                        // the notification went to a connection that no longer delivers, so the value can only
                        // arrive by the listener noticing, listening anew and reconciling
                        await("recovery from a listening connection that went silent")
                                .atMost(EXTENDED_WAITING_DURATION)
                                .untilAsserted(() -> assertThat(cacheB.getIfPresent(key2)).isEqualTo(Value.of(2)));

                        assertThat(loggerPostgresSynchronizer.getLoggingEvents())
                                .anySatisfy(loggingEvent -> {
                                    assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                    assertThat(loggingEvent.getMessage())
                                            .startsWith("Listening for notifications failed");
                                    assertThat(loggingEvent.getThrowable())
                                            .hasMessageStartingWith("Listening connection did not respond");
                                });
                        loggerPostgresSynchronizer.stopCapturing();

                        assertThat(loggerDistributedCaffeine.getLoggingEvents())
                                .anySatisfy(loggingEvent -> {
                                    assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                    assertThat(loggingEvent.getMessage())
                                            .startsWith("Synchronization was interrupted for cache at");
                                });
                        loggerDistributedCaffeine.stopCapturing();
                    } finally {
                        // torn down here rather than after the test, because the data source it uses is closed
                        // on the way out of this method
                        cacheB.distributedPolicy().stopSynchronization();
                        cacheB.invalidateAll();
                        distributedCacheInstances.remove(cacheB);
                    }
                }
            }
        }

        @DisplayName("Test that a listening connection stuck inside the driver in the middle of a message is aborted")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_aborts_a_listening_connection_stuck_in_the_middle_of_a_message() throws Exception {
            // The driver waits for the first byte of a message with a timeout, and for the rest of it without one.
            // A connection that stops carrying anything after the first byte of a notification therefore holds the
            // listening thread inside the driver for good - which neither the poll timeout nor the heartbeat, run
            // by that same thread, can do anything about
            try (SilencingProxy proxy = new SilencingProxy(postgresContainer.getHost(),
                    postgresContainer.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT), executorService)) {
                try (HikariDataSource proxiedDataSource = createProxiedDataSource(proxy)) {
                    DistributedCache<Key, Value> cacheA = createCache(CacheBuilder.identity(),
                            dc -> dc.withPersistence(configurer -> configurer
                                            .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                                    .build());
                    Adapter<Key, Value> adapterB = PostgresAdapter.newBuilder(
                            proxiedDataSource, SCHEMA_NAME, getDatasetName()).build();
                    Synchronizer<?, ?> synchronizerB = readFieldValue(adapterB, AbstractAdapter.class,
                            "synchronizer", Synchronizer.class);
                    // the watchdog shortened so that it fires within seconds, and the heartbeat put out of reach,
                    // so that the watchdog is the only way out - which is what this test is about
                    writeFieldValue(synchronizerB, synchronizerB.getClass(), "watchdogTimeout", Duration.ofSeconds(2));
                    writeFieldValue(synchronizerB, synchronizerB.getClass(), "heartbeatInterval", Duration.ofDays(1));
                    DistributedCache<Key, Value> cacheB = createCache(adapterB,
                            CacheBuilder.identity(),
                            dc -> dc.withPersistence(configurer -> configurer
                                            .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                                    .build());
                    try {
                        Key key1 = Key.of(1);
                        Key key2 = Key.of(2);

                        cacheA.put(key1, Value.of(1));

                        await("synchronization through the proxy")
                                .atMost(WAITING_DURATION)
                                .untilAsserted(() -> assertThat(cacheB.getIfPresent(key1)).isEqualTo(Value.of(1)));

                        CaptureLogger loggerPostgresSynchronizer = CaptureLoggerFactory.getCaptureLogger(
                                "io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresListener");
                        loggerPostgresSynchronizer.startCapturing();
                        CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                                .getCaptureLogger(DistributedCaffeine.class);
                        loggerDistributedCaffeine.startCapturing();

                        // the next thing the listening connection is sent is the notification below, and it gets
                        // to read the first byte of it and no more
                        proxy.silenceOpenConnectionsAfter(1);

                        cacheA.put(key2, Value.of(2));

                        await("recovery from a listening connection stuck in the middle of a message")
                                .atMost(Duration.ofSeconds(30))
                                .untilAsserted(() -> assertThat(cacheB.getIfPresent(key2)).isEqualTo(Value.of(2)));

                        assertThat(loggerPostgresSynchronizer.getLoggingEvents())
                                .anySatisfy(loggingEvent -> {
                                    assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                    assertThat(loggingEvent.getMessage())
                                            .startsWith("Listening for notifications failed");
                                    assertThat(loggingEvent.getThrowable())
                                            .hasMessageStartingWith("Listening connection was stuck inside the driver");
                                });
                        loggerPostgresSynchronizer.stopCapturing();

                        assertThat(loggerDistributedCaffeine.getLoggingEvents())
                                .anySatisfy(loggingEvent -> {
                                    assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                    assertThat(loggingEvent.getMessage())
                                            .startsWith("Synchronization was interrupted for cache at");
                                });
                        loggerDistributedCaffeine.stopCapturing();
                    } finally {
                        cacheB.distributedPolicy().stopSynchronization();
                        cacheB.invalidateAll();
                        distributedCacheInstances.remove(cacheB);
                    }
                }
            }
        }

        @DisplayName("Test that the adapter listens through the listener data source while writing through the other")
        @Test
        void test_Synchronizer_listens_through_a_separate_listener_data_source() {
            try (HikariDataSource listenerDataSource = createDataSource(postgresContainer.getDatabaseName())) {
                DistributedCache<Key, Value> cacheA = createCache(
                        CacheBuilder.identity(), DistributedCaffeine::build);
                DistributedCache<Key, Value> cacheB = createCache(
                        PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, getDatasetName())
                                .withListenerDataSource(listenerDataSource)
                                .build(),
                        CacheBuilder.identity(), DistributedCaffeine::build);
                try {
                    // the listening connection is the one cacheB holds from its listener data source, and it is
                    // all it takes from there - reading and writing go through the data source of the test
                    assertThat(listenerDataSource.getHikariPoolMXBean().getActiveConnections()).isEqualTo(1);

                    cacheA.put(Key.of(1), Value.of(1));

                    await("synchronization towards the instance listening through its listener data source")
                            .atMost(WAITING_DURATION)
                            .untilAsserted(() -> assertThat(cacheB.getIfPresent(Key.of(1))).isEqualTo(Value.of(1)));

                    cacheB.put(Key.of(2), Value.of(2));

                    await("synchronization from the instance listening through its listener data source")
                            .atMost(WAITING_DURATION)
                            .untilAsserted(() -> assertThat(cacheA.getIfPresent(Key.of(2))).isEqualTo(Value.of(2)));

                    assertThat(listenerDataSource.getHikariPoolMXBean().getActiveConnections()).isEqualTo(1);
                } finally {
                    // torn down here rather than after the test, because the listener data source is closed on the
                    // way out of this method
                    cacheB.distributedPolicy().stopSynchronization();
                    cacheB.invalidateAll();
                    distributedCacheInstances.remove(cacheB);
                }
            }
        }

        @DisplayName("Test that starting synchronization fails when what is written does not reach the listener")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_refuses_a_listener_that_does_not_receive_what_is_written() throws Exception {
            // A listener on another database of the same server: LISTEN is accepted there, and nothing written
            // through the data source ever arrives, because notifications do not cross databases. That is what a
            // pooler in transaction mode looks like from the client as well - a subscription accepted on a session
            // that then is not the one delivering - without needing a pooler to show it
            String otherDatabase = format("other_%05d", testCounter.get());
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("CREATE DATABASE " + otherDatabase);
            }
            try (HikariDataSource listenerDataSource = createDataSource(otherDatabase)) {
                Adapter<Key, Value> adapter = PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, getDatasetName())
                        .withListenerDataSource(listenerDataSource)
                        .build();
                // the probe shortened before activation, so that giving up takes a second rather than the
                // production timeout - what is under test is that it gives up, not how long it waits first
                Synchronizer<?, ?> synchronizer = readFieldValue(adapter, AbstractAdapter.class,
                        "synchronizer", Synchronizer.class);
                writeFieldValue(synchronizer, synchronizer.getClass(), "probeTimeout", Duration.ofSeconds(1));

                assertThatThrownBy(() -> createCache(adapter, CacheBuilder.identity(), DistributedCaffeine::build))
                        .isInstanceOf(IllegalStateException.class)
                        .rootCause()
                        .hasMessageStartingWith("A notification sent through the data source did not reach the "
                                + "listening connection")
                        .hasMessageContaining("withListenerDataSource");

                // and the listening connection it gave up on went back to the pool rather than being kept
                assertThat(listenerDataSource.getHikariPoolMXBean().getActiveConnections()).isZero();
            } finally {
                try (Connection connection = dataSource.getConnection();
                     Statement statement = connection.createStatement()) {
                    statement.execute("DROP DATABASE IF EXISTS " + otherDatabase);
                }
            }
        }

        @DisplayName("Test that sharing at instance level gives every cache instance a session of its own")
        @Test
        void test_Synchronizer_shares_sessions_by_instance() {
            assertSessionsShared(ListenerSharingMode.INSTANCE, 3);
        }

        @DisplayName("Test that sharing at table level gives the cache instances of a table one session")
        @Test
        void test_Synchronizer_shares_sessions_by_table() {
            assertSessionsShared(ListenerSharingMode.TABLE, 2);
        }

        @DisplayName("Test that sharing at database level gives all cache instances one session")
        @Test
        void test_Synchronizer_shares_sessions_by_database() {
            assertSessionsShared(ListenerSharingMode.DATABASE, 1);
        }

        @DisplayName("Test that a cache instance failing to apply what it received leaves the others on its session alone")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_isolates_a_cache_instance_that_fails_to_apply() {
            // all three share a session, which is the default
            DistributedCache<Key, Value> cacheA = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheB = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheC = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            CaptureLogger loggerPostgresListener = CaptureLoggerFactory.getCaptureLogger(
                    "io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresListener");
            loggerPostgresListener.startCapturing();
            CaptureLogger loggerPostgresSynchronizer = CaptureLoggerFactory.getCaptureLogger(
                    "io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresSynchronizer");
            loggerPostgresSynchronizer.startCapturing();
            // the failing cache instance recovers by reconciling, which it reports
            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);
            loggerDistributedCaffeine.startCapturing();

            InternalCacheManager<Key, Value> cacheManager = getInstanceRegistry(cacheB).getCacheManager();
            AtomicBoolean failing = new AtomicBoolean(true);
            cacheB.distributedPolicy().getAdapter().setReceiver(new Receiver<>() {

                @Override
                public void receiveCacheEntries(@NonNull List<CacheEntry<Key, Value>> cacheEntries) {
                    if (failing.get()) {
                        throw new IllegalStateException("provoked");
                    }
                    cacheManager.receiveCacheEntries(cacheEntries);
                }

                @Override
                public void receiveSynchronizationRestart() {
                    cacheManager.receiveSynchronizationRestart();
                }
            });

            cacheA.put(Key.of(1), Value.of(1));

            // the cache instance next to the failing one receives as usual, over the very same session
            await("synchronization next to a failing cache instance")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(cacheC.getIfPresent(Key.of(1))).isEqualTo(Value.of(1)));
            await("failure of the cache instance that fails to apply")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(loggerPostgresSynchronizer.getLoggingEvents())
                            .anySatisfy(loggingEvent -> {
                                assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                                assertThat(loggingEvent.getMessage()).startsWith("Receiving notifications failed");
                                assertThat(loggingEvent.getThrowable()).hasMessage("provoked");
                            }));

            failing.set(false);
            cacheA.put(Key.of(2), Value.of(2));

            // and the failing one receives again once it no longer fails
            await("synchronization once applying no longer fails")
                    .atMost(EXTENDED_WAITING_DURATION)
                    .untilAsserted(() -> assertThat(cacheB.getIfPresent(Key.of(2))).isEqualTo(Value.of(2)));

            // without the session ever having been given up for it
            assertThat(loggerPostgresListener.getLoggingEvents()).isEmpty();
            loggerPostgresListener.stopCapturing();
            loggerPostgresSynchronizer.stopCapturing();
            loggerDistributedCaffeine.stopCapturing();
        }

        @DisplayName("Test that a cache instance joining a session being replaced waits for it, or gives up in time")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_joins_a_shared_session_while_it_is_being_replaced() throws Exception {
            // The cache instances read and write directly and only listen through the proxy, so that the outage hits
            // the shared listening session alone - which keeps failing to be replaced for as long as it lasts
            try (SilencingProxy proxy = new SilencingProxy(postgresContainer.getHost(),
                    postgresContainer.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT), executorService);
                 HikariDataSource listenerDataSource = createProxiedDataSource(proxy)) {
                DistributedCache<Key, Value> cacheA = createCache(CacheBuilder.identity(), DistributedCaffeine::build);
                // the session is opened by this cache instance, so the heartbeat is shortened on it
                Adapter<Key, Value> adapterB1 = listeningAdapter(listenerDataSource);
                Synchronizer<?, ?> synchronizerB1 = readFieldValue(adapterB1, AbstractAdapter.class,
                        "synchronizer", Synchronizer.class);
                writeFieldValue(synchronizerB1, synchronizerB1.getClass(), "heartbeatInterval", Duration.ofSeconds(1));
                writeFieldValue(synchronizerB1, synchronizerB1.getClass(), "heartbeatTimeout", Duration.ofSeconds(1));
                DistributedCache<Key, Value> cacheB1 = createCache(adapterB1, CacheBuilder.identity(),
                        DistributedCaffeine::build);
                List<DistributedCache<Key, Value>> listening = new ArrayList<>(List.of(cacheB1));
                try {
                    cacheA.put(Key.of(1), Value.of(1));

                    await("synchronization through the proxy")
                            .atMost(WAITING_DURATION)
                            .untilAsserted(() -> assertThat(cacheB1.getIfPresent(Key.of(1))).isEqualTo(Value.of(1)));

                    // every failed attempt to replace the session is reported, and so is the reconcile once it is
                    CaptureLogger loggerPostgresListener = CaptureLoggerFactory.getCaptureLogger(
                            "io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresListener");
                    loggerPostgresListener.startCapturing();
                    CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                            .getCaptureLogger(DistributedCaffeine.class);
                    loggerDistributedCaffeine.startCapturing();

                    proxy.silenceNewConnections(true);
                    proxy.silenceOpenConnections();

                    await("session being replaced")
                            .atMost(Duration.ofSeconds(10))
                            .until(() -> !loggerPostgresListener.getLoggingEvents().isEmpty());

                    // a cache instance whose activation timeout runs out first gives up, and says why
                    Adapter<Key, Value> adapterB3 = listeningAdapter(listenerDataSource);
                    Synchronizer<?, ?> synchronizerB3 = readFieldValue(adapterB3, AbstractAdapter.class,
                            "synchronizer", Synchronizer.class);
                    writeFieldValue(synchronizerB3, synchronizerB3.getClass(), "activationTimeout",
                            Duration.ofSeconds(1));
                    assertThatThrownBy(() -> createCache(adapterB3, CacheBuilder.identity(),
                            DistributedCaffeine::build))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageStartingWith("Listening for notifications failed for cache at")
                            .cause()
                            .isInstanceOf(TimeoutException.class);

                    // while one whose timeout lasts waits for the session to come back
                    Adapter<Key, Value> adapterB2 = listeningAdapter(listenerDataSource);
                    Synchronizer<?, ?> synchronizerB2 = readFieldValue(adapterB2, AbstractAdapter.class,
                            "synchronizer", Synchronizer.class);
                    writeFieldValue(synchronizerB2, synchronizerB2.getClass(), "activationTimeout",
                            Duration.ofSeconds(60));
                    CompletableFuture<DistributedCache<Key, Value>> joining = CompletableFuture.supplyAsync(() ->
                            createCache(adapterB2, CacheBuilder.identity(), DistributedCaffeine::build), executorService);
                    sleep(Duration.ofSeconds(2));
                    assertThat(joining).isNotDone();

                    proxy.silenceNewConnections(false);

                    DistributedCache<Key, Value> cacheB2 = joining.get(60, TimeUnit.SECONDS);
                    listening.add(cacheB2);

                    cacheA.put(Key.of(2), Value.of(2));

                    await("synchronization of the instance that joined while the session was being replaced")
                            .atMost(WAITING_DURATION)
                            .untilAsserted(() -> assertThat(cacheB2.getIfPresent(Key.of(2))).isEqualTo(Value.of(2)));
                    loggerPostgresListener.stopCapturing();
                    loggerDistributedCaffeine.stopCapturing();
                } finally {
                    // torn down here rather than after the test, because the listener data source is closed on the
                    // way out of this method
                    listening.forEach(cache -> {
                        cache.distributedPolicy().stopSynchronization();
                        distributedCacheInstances.remove(cache);
                    });
                }
            }
        }

        @DisplayName("Test that a cache instance slow to apply what it received does not hold up the others")
        @Test
        void test_Synchronizer_does_not_hold_up_others_behind_a_slow_cache_instance() {
            // all three share a session, which is the default
            DistributedCache<Key, Value> cacheA = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheB = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);
            DistributedCache<Key, Value> cacheC = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            // cacheB does not get past applying until released - which, if applying happened on the thread that
            // holds the session, would leave nobody else on the session receiving anything either
            InternalCacheManager<Key, Value> cacheManager = getInstanceRegistry(cacheB).getCacheManager();
            CountDownLatch released = new CountDownLatch(1);
            cacheB.distributedPolicy().getAdapter().setReceiver(new Receiver<>() {

                @Override
                public void receiveCacheEntries(@NonNull List<CacheEntry<Key, Value>> cacheEntries) {
                    try {
                        released.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    cacheManager.receiveCacheEntries(cacheEntries);
                }

                @Override
                public void receiveSynchronizationRestart() {
                    cacheManager.receiveSynchronizationRestart();
                }
            });

            try {
                cacheA.put(Key.of(1), Value.of(1));
                cacheA.put(Key.of(2), Value.of(2));

                await("synchronization next to a cache instance that does not get past applying")
                        .atMost(WAITING_DURATION)
                        .untilAsserted(() -> {
                            assertThat(cacheC.getIfPresent(Key.of(1))).isEqualTo(Value.of(1));
                            assertThat(cacheC.getIfPresent(Key.of(2))).isEqualTo(Value.of(2));
                        });
                assertThat(cacheB.getIfPresent(Key.of(1))).isNull();
            } finally {
                released.countDown();
            }

            // and the slow one catches up once it gets past applying
            await("synchronization of the slow cache instance once released")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(cacheB.getIfPresent(Key.of(2))).isEqualTo(Value.of(2)));
        }

        @DisplayName("Test that cache instances joining a shared session wake it rather than wait for its polls")
        @Test
        void test_Synchronizer_joins_a_shared_session_without_waiting_for_its_polls() {
            DistributedCache<Key, Value> cacheA = createCache(
                    CacheBuilder.identity(), DistributedCaffeine::build);

            // The driver holds the connection for the whole of a poll, so a subscription can only be carried out
            // once the poll under way is over - and one joining right after the previous one was confirmed arrives
            // just as the next poll begins, so without being woken it waits out all of it. Measured for ten joining
            // one after the other, each with everything building a cache instance takes: one second without waking,
            // a fifth of one with it. The bound sits between the two with room on either side
            long startedAt = System.nanoTime();
            List<DistributedCache<Key, Value>> joining = new ArrayList<>();
            for (int index = 0; index < 10; index++) {
                joining.add(createCache(CacheBuilder.identity(), DistributedCaffeine::build));
            }
            Duration joiningTook = Duration.ofNanos(System.nanoTime() - startedAt);
            assertThat(joiningTook).isLessThan(Duration.ofMillis(600));

            cacheA.put(Key.of(1), Value.of(1));

            joining.forEach(cache -> await("synchronization of every cache instance that joined")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> assertThat(cache.getIfPresent(Key.of(1))).isEqualTo(Value.of(1))));
        }

        @DisplayName("Test that a cache instance falling too far behind is reconciled instead")
        @Test
        @ResourceLock(LOGGER_RESOURCE_LOCK)
        void test_Synchronizer_reconciles_a_cache_instance_that_falls_too_far_behind() throws Exception {
            // Persisting cached entries is what makes the recovery warm: reconciling keeps what the store confirms,
            // and without a store that retains them there is nothing to confirm against
            DistributedCache<Key, Value> cacheA = createCache(CacheBuilder.identity(),
                    dc -> dc.withPersistence(configurer -> configurer
                                    .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                            .build());
            Adapter<Key, Value> adapterB = createAdapter();
            // a limit low enough to be passed by a handful of writes, set before activation
            Synchronizer<?, ?> synchronizerB = readFieldValue(adapterB, AbstractAdapter.class,
                    "synchronizer", Synchronizer.class);
            writeFieldValue(synchronizerB, synchronizerB.getClass(), "pendingLimit", 5);
            DistributedCache<Key, Value> cacheB = createCache(adapterB, CacheBuilder.identity(),
                    dc -> dc.withPersistence(configurer -> configurer
                                    .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                            .build());

            CaptureLogger loggerDistributedCaffeine = CaptureLoggerFactory
                    .getCaptureLogger(DistributedCaffeine.class);
            loggerDistributedCaffeine.startCapturing();

            // cacheB applies the first thing it receives only once released, so that everything after it piles up
            InternalCacheManager<Key, Value> cacheManager = getInstanceRegistry(cacheB).getCacheManager();
            CountDownLatch released = new CountDownLatch(1);
            CountDownLatch held = new CountDownLatch(1);
            cacheB.distributedPolicy().getAdapter().setReceiver(new Receiver<>() {

                @Override
                public void receiveCacheEntries(@NonNull List<CacheEntry<Key, Value>> cacheEntries) {
                    held.countDown();
                    try {
                        released.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    cacheManager.receiveCacheEntries(cacheEntries);
                }

                @Override
                public void receiveSynchronizationRestart() {
                    cacheManager.receiveSynchronizationRestart();
                }
            });

            cacheA.put(Key.of(0), Value.of(0));
            assertThat(held.await(WAITING_DURATION.toMillis(), TimeUnit.MILLISECONDS)).isTrue();

            // more than the limit, while cacheB is still busy with the first
            List<Key> keys = IntStream.rangeClosed(1, 20).mapToObj(Key::of).toList();
            keys.forEach(key -> cacheA.put(key, Value.of(key.getId())));
            released.countDown();

            await("synchronization of everything despite falling behind")
                    .atMost(WAITING_DURATION)
                    .untilAsserted(() -> keys.forEach(key ->
                            assertThat(cacheB.getIfPresent(key)).isEqualTo(Value.of(key.getId()))));

            // and it got there by reconciling, rather than by reading back everything that piled up
            assertThat(loggerDistributedCaffeine.getLoggingEvents())
                    .anySatisfy(loggingEvent -> {
                        assertThat(loggingEvent.getLevel()).isEqualTo(Level.WARN);
                        assertThat(loggingEvent.getMessage()).startsWith("Synchronization was interrupted for cache at");
                    });
            loggerDistributedCaffeine.stopCapturing();
        }

        @DisplayName("Test that what the adapter puts into one notification is a payload the server accepts")
        @Test
        void test_Synchronizer_payloads_stay_within_what_the_server_accepts() throws Exception {
            // Enough hashes of the length the hasher produces to need more than one notification, chunked by the
            // adapter itself rather than by a number repeated here: what is under test is that its chunking fits
            // through the server, not that two constants agree with each other. Nothing else would catch the
            // chunk size being raised, because the batches the other tests send stay well inside one payload
            List<String> hashes = IntStream.range(0, 2000)
                    .mapToObj(index -> format("%032x", index))
                    .toList();
            List<String> payloads = invokeMethod(null,
                    Class.forName("io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresChannel"),
                    "payloadsOf", List.of(List.class), List.of(hashes));
            assertThat(payloads).hasSizeGreaterThan(1);

            String channel = "distributed_caffeine_payload_bound";
            try (Connection listener = dataSource.getConnection();
                 Connection writer = dataSource.getConnection()) {
                // for the same reason the adapter does: a pooled connection may arrive still subscribed
                execute(listener, "UNLISTEN *");
                execute(listener, "LISTEN " + channel);
                // unsubscribing stops what comes next, not what this session was already handed
                drain(listener);
                try {
                    for (String payload : payloads) {
                        notifyPayload(writer, channel, payload);
                    }
                    assertThat(notificationsOf(listener, payloads.size()))
                            .containsExactlyElementsOf(payloads);

                    // and the limit that the chunk size leaves room beneath is where it is documented to be,
                    // which is what makes that room a margin rather than a guess. The server refuses an over-long
                    // payload at the call, so this is what the adapter would run into if it ever stopped chunking
                    assertThatThrownBy(() -> notifyPayload(writer, channel, "x".repeat(8000)))
                            .isInstanceOf(SQLException.class);
                } finally {
                    // the connection goes back to the pool, and a pooled connection still listening would hand
                    // another test notifications it never asked for
                    execute(listener, "UNLISTEN " + channel);
                }
            }
        }

        // Statements against this test's own table that are reading in a transaction, counted from another
        // connection
        private long countReadingInTransaction() throws SQLException {
            String readingInTransaction = format("SELECT count(*) FROM pg_stat_activity "
                    + "WHERE state = 'idle in transaction' AND query LIKE '%%\"%s\"%%'", getDatasetName());
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(readingInTransaction)) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }

        // Three cache instances listening through a pool of their own - two on one table, told apart by their
        // discriminators, and one on another table - so that the connections the pool has out are the sessions
        // they listen on. Each of them has a peer on the shared data source to be written to, so that what they
        // receive has to come through the session they share
        private void assertSessionsShared(ListenerSharingMode sharing, int expectedSessions) {
            String tableName = getDatasetName();
            // still bounded, and still ending in the counter that makes it unique
            String otherTableName = "b_" + tableName.substring(2);
            try (HikariDataSource listenerDataSource = createDataSource(postgresContainer.getDatabaseName())) {
                List<DistributedCache<Key, Value>> listening = new ArrayList<>();
                List<DistributedCache<Key, Value>> writing = new ArrayList<>();
                List<List<String>> scopes = List.of(List.of(tableName, "a"), List.of(tableName, "b"),
                        List.of(otherTableName, "a"));
                try {
                    for (List<String> scope : scopes) {
                        listening.add(createCache(PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, scope.get(0))
                                        .withDiscriminator(scope.get(1))
                                        .withListenerDataSource(listenerDataSource)
                                        .withListenerSharingMode(sharing)
                                        .build(),
                                CacheBuilder.identity(), DistributedCaffeine::build));
                        writing.add(createCache(PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, scope.get(0))
                                        .withDiscriminator(scope.get(1))
                                        .build(),
                                CacheBuilder.identity(), DistributedCaffeine::build));
                    }

                    // a session is held from the moment its first cache instance is activated, and only sessions are
                    // taken from this pool - reading and writing go through the shared data source
                    assertThat(listenerDataSource.getHikariPoolMXBean().getActiveConnections())
                            .isEqualTo(expectedSessions);

                    for (int index = 0; index < scopes.size(); index++) {
                        writing.get(index).put(Key.of(index), Value.of(index));
                    }
                    for (int index = 0; index < scopes.size(); index++) {
                        DistributedCache<Key, Value> cache = listening.get(index);
                        Key key = Key.of(index);
                        Value value = Value.of(index);
                        await("synchronization over the session shared at " + sharing)
                                .atMost(WAITING_DURATION)
                                .untilAsserted(() -> assertThat(cache.getIfPresent(key)).isEqualTo(value));
                    }
                } finally {
                    // torn down here rather than after the test, because the listener data source is closed on the
                    // way out of this method
                    listening.forEach(cache -> {
                        cache.distributedPolicy().stopSynchronization();
                        cache.invalidateAll();
                        distributedCacheInstances.remove(cache);
                    });
                }

                // and a session is given up once its last cache instance stops
                await("sessions given up")
                        .atMost(WAITING_DURATION)
                        .until(() -> listenerDataSource.getHikariPoolMXBean().getActiveConnections() == 0);
            }
        }

        private Adapter<Key, Value> listeningAdapter(DataSource listenerDataSource) {
            return PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, getDatasetName())
                    .withListenerDataSource(listenerDataSource)
                    .build();
        }

        // asserts against the access path only - what a plan costs is the planner's business, not this table's.
        // Both the predicate and the binding behind it are taken from the repository rather than rebuilt here, so
        // that this cannot drift from what is actually queried. Naming more than one index accepts whichever of
        // them the planner picked, for a predicate that genuinely has more than one right answer
        private void assertThatQueryIsIndexed(Connection connection, Repository<Key, Value> repository,
                                              Set<String> hashes, Set<Status> statuses, Instant olderThan,
                                              Order order, Set<String> servedBy)
                throws Exception {
            Class<?> repositoryClass = Class
                    .forName("io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresRepository");
            String sql;
            if (order != Order.UNORDERED) {
                // what reads the store back carries the order and never an age, so it is built as a whole
                sql = invokeMethod(repository, repositoryClass, "select",
                        List.of(String.class, Set.class, Set.class, Order.class),
                        Arrays.asList("1", hashes, statuses, order));
            } else {
                String where = invokeMethod(repository, repositoryClass, "where",
                        List.of(Set.class, Set.class, Instant.class),
                        Arrays.asList(hashes, statuses, olderThan));
                sql = format("SELECT 1 FROM \"%s\".\"%s\" %s", SCHEMA_NAME, getDatasetName(), where);
            }

            StringBuilder plan = new StringBuilder();
            try (PreparedStatement statement = connection.prepareStatement("EXPLAIN " + sql)) {
                invokeMethod(repository, repositoryClass, "bind",
                        List.of(Connection.class, PreparedStatement.class, int.class, Set.class, Set.class,
                                Instant.class),
                        Arrays.asList(connection, statement, 1, hashes, statuses,
                                order != Order.UNORDERED ? null : olderThan));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        plan.append(resultSet.getString(1)).append('\n');
                    }
                }
            }

            String plans = plan.toString();
            assertThat(plans)
                    .describedAs("%nPlan for %s", sql)
                    .doesNotContain("Seq Scan");
            assertThat(servedBy)
                    .describedAs("%nPlan for %s%n%s", sql, plans)
                    .anyMatch(plans::contains);
        }

        // read off the table rather than rebuilt from the naming convention the adapter follows, which is exactly
        // where the two could drift apart
        private Map<String, String> readIndexes(Connection connection) throws SQLException {
            Map<String, String> indexes = new LinkedHashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = ? AND tablename = ?")) {
                statement.setString(1, SCHEMA_NAME);
                statement.setString(2, getDatasetName());
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        indexes.put(resultSet.getString(1), resultSet.getString(2));
                    }
                }
            }
            return indexes;
        }

        // the one covering the status is the one maintenance sweeps through, the other one is the primary key.
        // Read from the column list of the definition rather than from the whole of it, because the index name
        // sits in there too - and with the quotes taken off, because PostgreSQL prints them only where a name
        // needs them, which "timestamp" does and "status" does not
        private String indexNameOf(Map<String, String> indexes, boolean coveringStatus) {
            return indexes.entrySet().stream()
                    .filter(index -> {
                        String definition = index.getValue().replace("\"", "");
                        String columns = definition.substring(definition.lastIndexOf('(') + 1);
                        return columns.contains(CacheEntry.Field.STATUS.toString()) == coveringStatus;
                    })
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElseThrow();
        }

        private void execute(Connection connection, String sql) throws SQLException {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }

        // through pg_notify rather than the NOTIFY statement, so that channel and payload are parameters instead
        // of literals a caller would have to quote itself - which is what the adapter does as well
        private void notifyPayload(Connection connection, String channel, String payload) throws SQLException {
            try (PreparedStatement statement = connection.prepareStatement("SELECT pg_notify(?, ?)")) {
                statement.setString(1, channel);
                statement.setString(2, payload);
                statement.execute();
            }
        }

        // What a pooled connection was handed before this test borrowed it, thrown away so that what arrives
        // afterwards is this test's own. The adapter does the same on every connection it takes over
        private void drain(Connection connection) throws SQLException {
            PGConnection pgConnection = connection.unwrap(PGConnection.class);
            PGNotification[] stale;
            do {
                stale = pgConnection.getNotifications(1);
            } while (stale != null && stale.length > 0);
        }

        private List<String> notificationsOf(Connection connection, int expected) throws SQLException {
            List<String> payloads = new ArrayList<>();
            Instant deadline = Instant.now().plus(WAITING_DURATION);
            while (payloads.size() < expected && Instant.now().isBefore(deadline)) {
                PGNotification[] notifications = connection.unwrap(PGConnection.class).getNotifications(100);
                if (notifications != null) {
                    Stream.of(notifications).map(PGNotification::getParameter).forEach(payloads::add);
                }
            }
            return payloads;
        }

        private long indexCountOf(String table) throws Exception {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT count(*) FROM pg_indexes WHERE schemaname = ? AND tablename = ?")) {
                statement.setString(1, SCHEMA_NAME);
                statement.setString(2, table);
                try (ResultSet resultSet = statement.executeQuery()) {
                    assertThat(resultSet.next()).isTrue();
                    return resultSet.getLong(1);
                }
            }
        }

        private void publish(String discriminator, Serializer<Value, ?> valueSerializer) throws Exception {
            PostgresAdapter<Key, Value> adapter = PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, getDatasetName())
                    .withDiscriminator(discriminator)
                    .build();
            adapter.setKeySerializer(new JavaObjectSerializer<>());
            adapter.setValueSerializer(valueSerializer);
            adapter.getRepository().orElseThrow().publishCacheEntries(List.of(
                    CacheEntry.of("h1", "op1", Key.of(1), Value.of(1), CACHED, Instant.now().truncatedTo(MICROS))));
        }

        // which of the three value columns the record actually filled
        private List<String> columnsOf(String discriminator) throws Exception {
            List<String> columns = new java.util.ArrayList<>();
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(format(
                         "SELECT value_binary, value_text, value_jsonb FROM \"%s\".\"%s\" WHERE discriminator = ?",
                         SCHEMA_NAME, getDatasetName()))) {
                statement.setString(1, discriminator);
                try (ResultSet resultSet = statement.executeQuery()) {
                    assertThat(resultSet.next()).isTrue();
                    for (String column : List.of("value_binary", "value_text", "value_jsonb")) {
                        if (resultSet.getObject(column) != null) {
                            columns.add(column);
                        }
                    }
                }
            }
            return columns;
        }

        // Hands out the real connections, except that the next one asked for once the flag is set fails the way the
        // server fails a statement - which is the only way to put a caller in front of a given SQL state on purpose,
        // since what provokes a real deadlock is two transactions meeting at a moment neither of them decides
        private DataSource rollingBackOnce(AtomicBoolean failNext, String sqlState, AtomicInteger connections)
                throws SQLException {
            DataSource failing = mock(DataSource.class);
            when(failing.getConnection()).thenAnswer(invocation -> {
                connections.incrementAndGet();
                if (failNext.compareAndSet(true, false)) {
                    throw new SQLException("provoked", sqlState);
                }
                return dataSource.getConnection();
            });
            return failing;
        }

        private Repository<Key, Value> repositoryUsing(DataSource source) {
            return adapterUsing(source).getRepository().orElseThrow();
        }

        private PostgresAdapter<Key, Value> adapterUsing(DataSource source) {
            PostgresAdapter<Key, Value> adapter = PostgresAdapter.newBuilder(source, SCHEMA_NAME, getDatasetName()).build();
            adapter.setKeySerializer(new JavaObjectSerializer<>());
            adapter.setValueSerializer(new JacksonSerializer<>(Value.class, false));
            return adapter;
        }

        // A data source whose connections refuse one call, so that a failure can be placed INSIDE the transaction
        // rather than before it - which is where refusing the connection itself cannot reach. Everything else
        // passes through to a real connection, so the write really does happen before the refusal and really is
        // rolled back by it
        private DataSource refusing(AtomicBoolean refuseNext, BiPredicate<String, Object[]> refused)
                throws SQLException {
            DataSource refusing = mock(DataSource.class);
            when(refusing.getConnection()).thenAnswer(dataSourceInvocation -> {
                Connection connection = dataSource.getConnection();
                return Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[]{Connection.class}, (proxy, method, arguments) -> {
                            if (refused.test(method.getName(), requireNonNullElse(arguments, new Object[0]))
                                    && refuseNext.compareAndSet(true, false)) {
                                throw new SQLException("provoked", "42601");
                            }
                            try {
                                return method.invoke(connection, arguments);
                            } catch (InvocationTargetException e) {
                                // the caller is owed what the connection threw, not the news that it was reached
                                // by reflection
                                throw e.getCause();
                            }
                        });
            });
            return refusing;
        }

        // A pool that reaches the test's server through the proxy, set up the way a pool facing a network that can
        // go silent has to be anyway. Without the driver's timeouts, a connection that is being opened at the
        // moment the proxy goes silent hangs in its handshake for good - and with it the one thread the pool opens
        // connections on, so that nothing borrowed from it ever arrives again (seen as a 1-in-10 flake). And it is
        // filled before it is handed out, so that no connection is being opened at that moment to begin with
        HikariDataSource createProxiedDataSource(SilencingProxy proxy) {
            HikariConfig hikariConfig = createHikariConfig("localhost", proxy.getPort(),
                    postgresContainer.getDatabaseName());
            hikariConfig.setMinimumIdle(5);
            // the pooled connections go silent along with the listening one, and are told apart from live ones by
            // validating them on the way out of the pool - quickly, so that doing so is not the test
            hikariConfig.setValidationTimeout(1_000);
            hikariConfig.addDataSourceProperty("loginTimeout", "5");
            hikariConfig.addDataSourceProperty("socketTimeout", "5");
            HikariDataSource proxiedDataSource = new HikariDataSource(hikariConfig);
            await("filling of the pool")
                    .atMost(WAITING_DURATION)
                    .until(() -> proxiedDataSource.getHikariPoolMXBean().getTotalConnections() == 5);
            return proxiedDataSource;
        }

        // a pool of its own on the test's server, for a test that needs a data source next to the shared one
        HikariDataSource createDataSource(String databaseName) {
            return new HikariDataSource(createHikariConfig(postgresContainer.getHost(),
                    postgresContainer.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT), databaseName));
        }

        // a small pool on the test's server, reached at the given address - directly or through a proxy
        private HikariConfig createHikariConfig(String host, int port, String databaseName) {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(format("jdbc:postgresql://%s:%d/%s", host, port, databaseName));
            hikariConfig.setUsername(postgresContainer.getUsername());
            hikariConfig.setPassword(postgresContainer.getPassword());
            hikariConfig.setMaximumPoolSize(5);
            return hikariConfig;
        }

        @Override
        void startStore(DockerImageName dockerImageName, String displayName) {
            this.postgresContainer = new PostgreSQLContainer(dockerImageName)
                    .withCreateContainerCmdModifier(cmd -> cmd.withName(displayName));
            this.postgresContainer.start();
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(postgresContainer.getJdbcUrl());
            hikariConfig.setUsername(postgresContainer.getUsername());
            hikariConfig.setPassword(postgresContainer.getPassword());
            // every cache instance holds one of these for as long as it listens, so the pool has to carry the
            // caches of a test as well as the connections their operations borrow
            hikariConfig.setMaximumPoolSize(30);
            this.dataSource = new HikariDataSource(hikariConfig);
        }

        @Override
        void stopStore() {
            ((HikariDataSource) this.dataSource).close();
            this.postgresContainer.stop();
        }

        @Override
        <K, V> Adapter<K, V> createAdapter() {
            return PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, getDatasetName()).build();
        }

        @Override
        <K, V> Adapter<K, V> createAdapter(String discriminator) {
            return PostgresAdapter.newBuilder(dataSource, SCHEMA_NAME, getDatasetName())
                    .withDiscriminator(discriminator)
                    .build();
        }

        // a table name is an identifier and cannot be as long as a test method name, so the name is bounded -
        // the counter alone already makes it unique, which is what the truncation relies on
        @Override
        String getDatasetName() {
            String name = super.getDatasetName();
            return name.length() <= 60 ? name : name.substring(name.length() - 60);
        }

    }

    abstract static class DistributedCaffeineIntegrationTestInstance extends DistributedCaffeineCommonTestInstance {

        static final String RUNS_ON_GITHUB = "runsOnGitHub";
        static final String LOGGER_RESOURCE_LOCK = "logger";
        static final Duration WAITING_DURATION = Duration.ofSeconds(3);
        static final Duration EXTENDED_WAITING_DURATION = Duration.ofMinutes(3);
        static final Duration EXTENDED_POLL_INTERVAL = Duration.ofSeconds(1);

        SecureRandom secureRandom;

        // What a store brings: a container of its own, whatever a cache instance needs to reach it, and the
        // adapter built on that. Everything else a test does goes through the SPI and reads the same either way,
        // which is what makes one suite answer for both
        abstract void startStore(DockerImageName dockerImageName, String displayName);

        abstract void stopStore();

        abstract <K, V> Adapter<K, V> createAdapter();

        abstract <K, V> Adapter<K, V> createAdapter(String discriminator);

        // through the adapter rather than the repository directly, because wiring the serializers and the
        // discriminator is what an adapter does - and a repository is package-private in its adapter's package.
        // A byte array serializer for the key and a string one for the value, so that both encodings a store has
        // to carry are exercised at once
        Repository<Key, Value> repositoryFor(String discriminator) {
            Adapter<Key, Value> adapter = isNull(discriminator)
                    ? createAdapter()
                    : createAdapter(discriminator);
            adapter.setKeySerializer(new JavaObjectSerializer<>());
            adapter.setValueSerializer(new JacksonSerializer<>(Value.class, false));
            return adapter.getRepository().orElseThrow();
        }

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

            startStore(dockerImageName, displayName);
        }

        @AfterAll
        void afterAll() {
            stopStore();
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
            return createCache(createAdapter(), cacheBuilder, cacheConstructor);
        }

        // every cache of a test shares its collection, so this is what puts two of them in the same scope or in
        // different ones - the store-specific half of that, so a test about the scoping itself does not have to name
        // the store
        <K, V> DistributedCache<K, V> createCache(String discriminator, CacheBuilder<K, V> cacheBuilder,
                                                  CacheConstructor<K, V> cacheConstructor) {
            return createCache(createAdapter(discriminator), cacheBuilder, cacheConstructor);
        }

        // what the store calls a dataset of its own - a collection in MongoDB, a table in PostgreSQL - named
        // after the test that owns it, so that tests cannot see each other's records
        String getDatasetName() {
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

        @SuppressWarnings("ReturnValueIgnored")
            // the assertion runs inside apply(), its result is of no interest
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
                                    repository.countCacheEntries(Set.of(count.status()), null)))
                                    .describedAs("%nCount for %s", count.status().name())));
        }

        @SuppressWarnings("ReturnValueIgnored")
            // the assertion runs inside apply(), its result is of no interest
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
                                    repository.countCacheEntries(count.statuses(), null)))
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
                    repository.streamCacheEntries(null, statusesOrNull, UNORDERED))) {
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
