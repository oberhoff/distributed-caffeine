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
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.Builder;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import io.github.oberhoff.distributedcaffeine.common.DistributedCaffeineCommonTestInstance;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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
                            b -> b.withCaffeineBuilder(_null()),
                            Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("caffeineBuilder cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            b -> b.withHashProvider(_null()),
                            Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("hashProvider cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            b -> b.withDistributionMode(_null()),
                            Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("distributionMode cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            b -> b.withSerializers(configurer -> configurer
                                    .withKeySerializer(_null())),
                            Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("keySerializer cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            b -> b.withSerializers(configurer -> configurer
                                    .withValueSerializer(_null())),
                            Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("valueSerializer cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            b -> b.withSerializers(configurer ->
                                    configurer.withKeySerializer(new Serializer<>() {
                                        @Override
                                        public Object serialize(Key object) {
                                            return _null();
                                        }

                                        @Override
                                        public Key deserialize(Object value) {
                                            return _null();
                                        }
                                    })),
                            Builder::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Serializers must implement one of the following interfaces: "
                            .concat("ByteArraySerializer, StringSerializer, JsonSerializer"));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            b -> b.withSerializers(configurer ->
                                    configurer.withValueSerializer(new Serializer<>() {
                                        @Override
                                        public Object serialize(Value object) {
                                            return _null();
                                        }

                                        @Override
                                        public Value deserialize(Object value) {
                                            return _null();
                                        }
                                    })),
                            Builder::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Serializers must implement one of the following interfaces: "
                            .concat("ByteArraySerializer, StringSerializer, JsonSerializer"));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            b -> b.withExtendedPersistence(configurer -> configurer
                                    .withMaximumSize(0)),
                            Builder::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("maximumSize must be positive");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            b -> b.withExtendedPersistence(configurer -> configurer
                                    .withMaximumTime(_null())),
                            Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("maximumTime cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            b -> b.withExtendedPersistence(configurer -> configurer
                                    .withMaximumTime(Duration.ZERO)),
                            Builder::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("maximumTime must be positive");

            Stream.<CacheConstructor<Key, Value>>of(Builder::build, b -> b.build(key -> null))
                    .forEach(cacheConstructor -> assertThatThrownBy(() ->
                            createCache(adapter,
                                    b -> b.withExtendedPersistence(configurer -> configurer
                                            .withMaximumSize(1)),
                                    cacheConstructor))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("If extended persistence is configured, at least one eviction strategy must be set"));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                            .maximumSize(1))
                                    .withExtendedPersistence(configurer -> configurer
                                            .withMaximumSize(1)
                                            .withLoadingStrategy(true)),
                            Builder::build))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("If extended persistence is configured and loading strategy for cache loader is enabled, "
                            .concat("cache must be build as loading cache"));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            CacheBuilder.identity(),
                            b -> b.build(_null())))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("cacheLoader cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                    .weakKeys()
                                    .weakValues()),
                            Builder::build))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("The use of weak or soft references is not supported");
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
                public CompletableFuture<? extends Value> asyncLoad(Key key, Executor executor) {
                    return CompletableFuture.completedFuture(load(key));
                }

                @Override
                public CompletableFuture<? extends Value> asyncReload(Key key, Value oldValue, Executor executor) {
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
            field.setAccessible(false);

            Cache<Integer, Integer> integerCache = (Cache<Integer, Integer>) caffeine.build();

            integerCache.put(0, 1);
            assertThat(integerCache.getIfPresent(0)).isEqualTo(1);
            integerCache.invalidateAll();
            integerCache.cleanUp();
            assertThat(integerCache.estimatedSize()).isEqualTo(0);

            assertThat(removalCount).hasValue(2);
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
                    .withMessage("Hasher is not being used correctly");
        }
    }

    abstract static class DistributedCaffeineUnitTestInstance extends DistributedCaffeineCommonTestInstance {
    }
}
