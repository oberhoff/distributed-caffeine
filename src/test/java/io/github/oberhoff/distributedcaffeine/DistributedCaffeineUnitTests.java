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

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.mongodb.client.MongoCollection;
import io.github.oberhoff.distributedcaffeine.common.DistributedCaffeineCommonTestInstance;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    @DisplayName("Test Builder")
    final class BuilderUnit extends DistributedCaffeineUnitTestInstance {

        @DisplayName("that arguments and states are checked")
        @Test
        @SuppressWarnings({"unchecked", "java:S5778", "java:S5961"})
        void test_Builder_checks_on_arguments_and_states() {
            @SuppressWarnings("unchecked")
            MongoCollection<Document> mongoCollection = mock(MongoCollection.class);

            assertThatThrownBy(() ->
                    DistributedCaffeine.newBuilder(_null()))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("mongoCollection cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withCaffeineBuilder(_null()),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("caffeineBuilder cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withDistributionMode(_null()),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("distributionMode cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withForySerializer(_null()),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("registerClasses cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withJsonSerializer(_null(), _null(), (Class<Object>) _null(), true),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("objectMapper cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withJsonSerializer(new ObjectMapper(), _null(), (Class<Object>) _null(), true),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("keyClass cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withJsonSerializer(new ObjectMapper(), Object.class, _null(), true),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("valueClass cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withJsonSerializer(_null(), _null(), (TypeReference<Object>) _null(), true),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("objectMapper cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withJsonSerializer(new ObjectMapper(), _null(), (TypeReference<Object>) _null(), true),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("keyTypeReference cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withJsonSerializer(new ObjectMapper(), new TypeReference<>() {
                            }, _null(), true),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("valueTypeReference cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withJsonSerializer(_null(), (Class<Object>) _null(), true),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("keyClass cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withJsonSerializer(Object.class, _null(), true),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("valueClass cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withJsonSerializer(_null(), (TypeReference<Object>) _null(), true),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("keyTypeReference cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withJsonSerializer(new TypeReference<>() {
                            }, _null(), true),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("valueTypeReference cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withCustomKeySerializer(_null()),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("keySerializer cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withCustomValueSerializer(_null()),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("valueSerializer cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withCustomKeySerializer(new Serializer<>() {
                                @Override
                                public Object serialize(Object object) {
                                    return null;
                                }

                                @Override
                                public Object deserialize(Object value) {
                                    return null;
                                }
                            }),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Custom serializer must implement one of the following interfaces: "
                            .concat("ByteArraySerializer, StringSerializer, JsonSerializer"));

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withCustomValueSerializer(new Serializer<>() {
                                @Override
                                public Object serialize(Object object) {
                                    return null;
                                }

                                @Override
                                public Object deserialize(Object value) {
                                    return null;
                                }
                            }),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Custom serializer must implement one of the following interfaces: "
                            .concat("ByteArraySerializer, StringSerializer, JsonSerializer"));

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withExtendedPersistence(0),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("maximumSize must be positive");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withExtendedPersistence(_null()),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("maximumTime cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withExtendedPersistence(Duration.ZERO),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("maximumTime must be positive");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            CacheBuilder.identity(),
                            b -> b.build(_null())))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("cacheLoader cannot be null");

            Stream.<CacheConstructor<Key, Value>>of(DistributedCaffeine.Builder::buildWithExtendedPersistence,
                            b -> b.buildWithExtendedPersistence(key -> null))
                    .forEach(cacheConstructor -> assertThatThrownBy(() ->
                            createCache(mongoCollection,
                                    CacheBuilder.identity(),
                                    DistributedCaffeine.Builder::buildWithExtendedPersistence))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("If no extended persistence size and no extended persistence time is set, "
                                    .concat("'build(...)' must be used")));

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withExtendedPersistence(1),
                            b -> b.buildWithExtendedPersistence(_null())))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("cacheLoader cannot be null");

            assertThatThrownBy(() ->
                    createCache(mongoCollection,
                            b -> b.withCaffeineBuilder(Caffeine.newBuilder()
                                    .weakKeys()
                                    .weakValues()),
                            DistributedCaffeine.Builder::build))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("The use of weak or soft references is not supported");

            Stream.<CacheConstructor<Key, Value>>of(DistributedCaffeine.Builder::build, b -> b.build(key -> null),
                            DistributedCaffeine.Builder::buildWithExtendedPersistence, b -> b.buildWithExtendedPersistence(key -> null))
                    .forEach(cacheConstructor -> assertThatThrownBy(() ->
                            createCache(mongoCollection,
                                    b -> b.withExtendedPersistence(1),
                                    cacheConstructor))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("If an extended persistence size or an extended persistence time is set, "
                                    .concat("at least one eviction policy must be configured.")));
        }
    }

    @Nested
    @DisplayName("Test Caffeine")
    @SuppressWarnings("java:S5838")
    final class CaffeineUnit extends DistributedCaffeineUnitTestInstance {

        @DisplayName("that removal listener is not invoked if refresh returns old value")
        @Test
        void test_removal_listener_is_not_invoked_if_refresh_returns_old_value() {
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
    }

    abstract static class DistributedCaffeineUnitTestInstance extends DistributedCaffeineCommonTestInstance {
    }
}
