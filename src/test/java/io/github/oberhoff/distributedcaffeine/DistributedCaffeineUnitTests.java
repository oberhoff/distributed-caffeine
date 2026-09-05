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
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
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
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntryMetadata;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.adapter.Synchronizer;
import io.github.oberhoff.distributedcaffeine.common.DistributedCaffeineCommonTestInstance;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;
import io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.CachedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.Configurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.PersistenceConfigurer;

import static io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer.LoadingStrategy.CACHE_LOADER;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.adapter.Repository.DEFAULT_DISCRIMINATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@DisplayName("Distributed Caffeine Unit Test Suite")
final class DistributedCaffeineUnitTests {

    @Nested
    @DisplayName("Test builder and configurers")
    final class BuilderUnit extends DistributedCaffeineUnitTestInstance {

        @DisplayName("that arguments and states are checked")
        @Test
        @SuppressWarnings({"unchecked", "java:S5778", "java:S5961"})
        void test_Builder_checks_on_arguments_and_states() {
            Adapter<Key, Value> adapter = mock(Adapter.class);

            assertThatThrownBy(() ->
                    DistributedCaffeine.newBuilder(_null()))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("adapter cannot be null");

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
            when(adapter.getRepository()).thenReturn(repository);
            // answered rather than returned, so that every synchronization gets a stream of its own instead of
            // re-consuming one that an earlier one already closed
            when(repository.streamCacheEntries(any(), any(), anyBoolean()))
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
    @DisplayName("Test Caffeine")
    @SuppressWarnings("java:S5838")
    final class CaffeineUnit extends DistributedCaffeineUnitTestInstance {

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
    @DisplayName("Test Hasher")
    final class HasherUnit extends DistributedCaffeineUnitTestInstance {

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

            repository.upsertCacheEntries(List.of(cacheEntry("hash1", 1), cacheEntry("hash2", 2)));

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
        void test_MongoRepository_bulk_upsert_propagates_other_errors() throws Exception {
            MongoClient mongoClient = mock(MongoClient.class, RETURNS_DEEP_STUBS);
            MongoCollection<Document> mongoCollection = mongoCollectionOf(mongoClient);
            Repository<Key, Value> repository = repositoryOf(mongoClient);

            MongoBulkWriteException validationException = bulkWriteExceptionOf(
                    new BulkWriteError(121, "Document failed validation", new BsonDocument(), 0));
            when(mongoCollection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
                    .thenThrow(validationException);

            assertThatThrownBy(() -> repository.upsertCacheEntries(List.of(cacheEntry("hash1", 1))))
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
            try (Stream<CacheEntryMetadata> stream = repository.streamCacheEntryMetadata(null, null, false)) {
                assertThat(stream).isEmpty();
            }
            verify(findIterable, times(1)).projection(projectionCaptor.capture());
            assertThat(projectionCaptor.getValue().toBsonDocument().keySet())
                    .containsExactlyInAnyOrder(
                            CacheEntry.Field.HASH.toString(), CacheEntry.Field.OPERATION.toString(),
                            CacheEntry.Field.STATUS.toString(), CacheEntry.Field.TIMESTAMP.toString())
                    .doesNotContain(CacheEntry.Field.KEY.toString(), CacheEntry.Field.VALUE.toString());

            // a cache entry, in contrast, is read with all of its fields
            try (Stream<CacheEntry<Key, Value>> stream = repository.streamCacheEntries(null, null, false)) {
                assertThat(stream).isEmpty();
            }
            verify(findIterable, times(2)).projection(projectionCaptor.capture());
            assertThat(projectionCaptor.getValue().toBsonDocument().keySet())
                    .containsExactlyInAnyOrder(Stream.of(CacheEntry.Field.values())
                            .map(CacheEntry.Field::toString)
                            .toArray(String[]::new));
        }

        private MongoCollection<Document> mongoCollectionOf(MongoClient mongoClient) {
            // deep stubs return the same collection mock the repository resolves for these names
            return mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME);
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
    @DisplayName("Test MongoSynchronizer")
    final class MongoSynchronizerUnit extends DistributedCaffeineUnitTestInstance {

        private static final String DATABASE_NAME = "database";
        private static final String COLLECTION_NAME = "collection";
        private static final String MONGO_SYNCHRONIZER_CLASS_NAME =
                "io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoSynchronizer";

        @DisplayName("that watching resumes after the position reported while no events occurred")
        @Test
        void test_MongoSynchronizer_resumes_after_position_reported_while_idle() throws Exception {
            // the position the server reports for a polled batch, here an empty one because nothing has happened
            BsonDocument tokenWhileIdle = new BsonDocument("_data", new BsonString("tokenWhileIdle"));

            MongoClient mongoClient = mock(MongoClient.class, RETURNS_DEEP_STUBS);
            ChangeStreamIterable<Document> changeStreamIterable = mock();
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = mock();

            when(mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME).watch(anyList()))
                    .thenReturn(changeStreamIterable);
            when(changeStreamIterable.fullDocument(any())).thenReturn(changeStreamIterable);
            when(changeStreamIterable.resumeAfter(any())).thenReturn(changeStreamIterable);
            when(changeStreamIterable.cursor()).thenReturn(cursor);
            when(cursor.getResumeToken()).thenReturn(tokenWhileIdle);
            // an idle poll delivering no event, then watching fails - so no event was ever there to take a position
            // from, which is the situation an operation time cannot cover (it only ever comes from an event)
            MongoException connectionLost = new MongoException("connection lost");
            when(cursor.tryNext()).thenReturn(null).thenThrow(connectionLost);

            Synchronizer<Key, Value> synchronizer = synchronizerOf(mongoClient);
            startWatching(synchronizer);

            // the first attempt has nothing to resume from, so it watches from wherever the stream currently is
            assertThatThrownBy(() -> processChangeStreams(synchronizer))
                    .isSameAs(connectionLost);
            verify(changeStreamIterable, never()).resumeAfter(any());

            // the retry must not start over at "now", which would skip everything written while watching was down.
            // It resumes strictly after the position the failed attempt saw while idle
            assertThatThrownBy(() -> processChangeStreams(synchronizer))
                    .isSameAs(connectionLost);
            verify(changeStreamIterable, times(1)).resumeAfter(tokenWhileIdle);
        }

        // MongoSynchronizer and its watch loop are package-private in another package, so both are reached
        // reflectively. The constructor only resolves the collection, which the deep stubs absorb
        @SuppressWarnings("unchecked")
        private Synchronizer<Key, Value> synchronizerOf(MongoClient mongoClient) throws Exception {
            Constructor<?> constructor = Class.forName(MONGO_SYNCHRONIZER_CLASS_NAME)
                    .getDeclaredConstructor(MongoClient.class, String.class, String.class);
            constructor.setAccessible(true);
            Synchronizer<Key, Value> synchronizer = (Synchronizer<Key, Value>)
                    constructor.newInstance(mongoClient, DATABASE_NAME, COLLECTION_NAME);
            // wiring an adapter would normally do, which constructing the synchronizer directly skips
            synchronizer.setDiscriminator(DEFAULT_DISCRIMINATOR);
            return synchronizer;
        }

        // a freshly constructed synchronizer counts as stopped and would refuse to watch, so it is moved into the
        // state a pending activation leaves behind (without starting the asynchronous machinery around it)
        // unchecked: the state is held in an AtomicReference of a package-private enum, which cannot be named here, so
        // reading the field yields a raw reference and setting the constant found by name is unverifiable for javac
        @SuppressWarnings("unchecked")
        private void startWatching(Synchronizer<Key, Value> synchronizer) {
            Object starting = Stream.of(readFieldValue(synchronizer, synchronizer.getClass(), "watchState",
                            AtomicReference.class).get().getClass().getEnumConstants())
                    .filter(watchState -> watchState.toString().equals("STARTING"))
                    .findFirst()
                    .orElseThrow(NoSuchFieldError::new);
            readFieldValue(synchronizer, synchronizer.getClass(), "watchState", AtomicReference.class)
                    .set(starting);
        }

        private void processChangeStreams(Synchronizer<Key, Value> synchronizer) {
            invokeMethod(synchronizer, synchronizer.getClass(), "processChangeStreams", List.of(), List.of());
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
        @SuppressWarnings("EqualsIncompatibleType") // comparing unrelated types is the point of the equals() contract test
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

    abstract static class DistributedCaffeineUnitTestInstance extends DistributedCaffeineCommonTestInstance {
    }
}
