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
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
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
                                        public Object serialize(Key object) {
                                            return _null();
                                        }

                                        @Override
                                        public Key deserialize(Object value) {
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
                                        public Object serialize(Value object) {
                                            return _null();
                                        }

                                        @Override
                                        public Value deserialize(Object value) {
                                            return _null();
                                        }
                                    })),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Serializers must implement one of the following interfaces: "
                            .concat("ByteArraySerializer, StringSerializer, JsonSerializer"));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withExtendedPersistence(configurer -> configurer
                                    .withMaximumSize(0)),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("maximumSize must be positive");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withExtendedPersistence(configurer -> configurer
                                    .withMaximumTime(_null())),
                            DistributedCaffeine::build))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("maximumTime cannot be null");

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withExtendedPersistence(configurer -> configurer
                                    .withMaximumTime(Duration.ZERO)),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("maximumTime must be positive");

            Stream.<CacheConstructor<Key, Value>>of(DistributedCaffeine::build, dc -> dc.build(key -> null))
                    .forEach(cacheConstructor -> assertThatThrownBy(() ->
                            createCache(adapter,
                                    dc -> dc.withExtendedPersistence(configurer -> configurer
                                            .withMaximumSize(1)),
                                    cacheConstructor))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("If extended persistence is configured, "
                                    .concat("at least one eviction strategy must be set")));

            assertThatThrownBy(() ->
                    createCache(adapter,
                            dc -> dc.withCaffeine(Caffeine.newBuilder()
                                            .maximumSize(1))
                                    .withExtendedPersistence(configurer -> configurer
                                            .withMaximumSize(1)
                                            .withLoadingStrategy(true)),
                            DistributedCaffeine::build))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("If extended persistence is configured and loading strategy for cache loader is enabled, "
                            .concat("cache must be build as loading cache"));

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
            when(repository.streamCacheEntries(any(), any(), any(), anyBoolean()))
                    .thenAnswer(invocation -> Stream.empty());
            return adapter;
        }

        @DisplayName("that an adapter already in use is rejected")
        @Test
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
                    .setRetriever(any());

            // an own adapter for each cache instance is what the rejection asks for, so that has to work
            assertThat(createCache(mockAdapter("database.other"), CacheBuilder.identity(),
                    DistributedCaffeine::build))
                    .isNotNull();
        }

        @DisplayName("that an adapter is released again if constructing fails")
        @Test
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
            InternalHasher<Object> hasher = new InternalHasher<>((key, hasherSupplier) -> "provided");

            // over the types hashed out of the box as well as over keys hashing themselves, so that configuring
            // one is enough to take over hashing entirely
            assertThat(hasher.getHash("key")).isEqualTo("provided");
            assertThat(hasher.getHash(Key.of(1))).isEqualTo("provided");
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
            return CacheEntry.of(hash, id, Key.of(id), Value.of(id), Status.CACHED, Instant.now());
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

    abstract static class DistributedCaffeineUnitTestInstance extends DistributedCaffeineCommonTestInstance {
    }
}
