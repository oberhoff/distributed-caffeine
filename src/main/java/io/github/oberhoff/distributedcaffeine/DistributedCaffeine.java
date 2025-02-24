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
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status;
import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.FurySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JavaObjectSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.lang.System.Logger;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.STATUS;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EXTENDED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.ORPHANED;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

/**
 * Distributed Caffeine is a {@link Caffeine}-based distributed cache using MongoDB change streams for near real-time
 * synchronization between multiple cache instances, especially across different machines.
 * <p>
 * Cache instances can be configured and constructed using a builder returned by
 * {@link #newBuilder(MongoCollection)}. A cache instance can be of type {@link DistributedCache}
 * (extends {@link Cache}) or of type {@link DistributedLoadingCache} (extends {@link LoadingCache}).
 * <p>
 * <b>Attention:</b> To ensure the integrity of distributed synchronization between cache instances, the following
 * minor restrictions apply:
 * <ul>
 *      <li>Reference-based eviction using Caffeine's weak or soft references for keys or values is not supported. Even
 *      for the use of Caffeine (stand-alone), it is advised to use the more predictable size- or time-based eviction
 *      instead.</li>
 * </ul>
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public final class DistributedCaffeine<K, V> {

    private final Logger logger;

    private final MongoCollection<Document> mongoCollection;
    private final DistributionMode distributionMode;
    private final Serializer<K, ?> keySerializer;
    private final Serializer<V, ?> valueSerializer;
    private final ExtendedPersistenceConfig extendedPersistenceConfig;
    private final Cache<K, V> cache;

    private final InternalSynchronizationLock synchronizationLock;
    private final InternalObjectIdGenerator objectIdGenerator;

    private final InternalDocumentConverter<K, V> documentConverter;
    private final InternalMongoRepository<K, V> mongoRepository;
    private final InternalCacheManager<K, V> cacheManager;
    private final InternalChangeStreamWatcher<K, V> changeStreamWatcher;
    private final InternalMaintenanceWorker<K, V> maintenanceWorker;

    private DistributedCaffeine(Builder<K, V> builder) {
        this.logger = System.getLogger(getClass().getName());

        this.mongoCollection = builder.mongoCollection;
        this.distributionMode = builder.distributionMode;
        this.keySerializer = builder.keySerializer;
        this.valueSerializer = builder.valueSerializer;
        this.extendedPersistenceConfig = new ExtendedPersistenceConfig(
                builder.extendedPersistenceSize, builder.extendedPersistenceTime);
        this.cache = builder.cache;

        this.synchronizationLock = new InternalSynchronizationLock();
        this.objectIdGenerator = new InternalObjectIdGenerator();

        this.documentConverter = new InternalDocumentConverter<>();
        this.mongoRepository = new InternalMongoRepository<>();
        this.cacheManager = new InternalCacheManager<>();
        this.changeStreamWatcher = new InternalChangeStreamWatcher<>();
        this.maintenanceWorker = new InternalMaintenanceWorker<>();

        Stream.of(builder.wrappedRemovalListener, builder.wrappedEvictionListener, builder.wrappedCacheLoader,
                        this.documentConverter, this.mongoRepository, this.cacheManager, this.changeStreamWatcher,
                        this.maintenanceWorker)
                .filter(Objects::nonNull)
                .forEach(lazyInitializer -> lazyInitializer.initialize(this));

        activate();
    }

    /**
     * Returns a new builder for configuring and constructing cache instances. For example, the builder can be finalized
     * with {@link Builder#build()} to construct a cache instance of type {@link DistributedCache} (extends
     * {@link Cache}) or with {@link Builder#build(CacheLoader)} to construct a loading cache instance of type
     * {@link DistributedLoadingCache} (extends {@link LoadingCache}).
     * <p>
     * <b>Note:</b> A call to this method must use additional generic type parameters for key and value directly
     * before the method name like in the following example:
     * <pre>
     * DistributedCache&#60;Key, Value&#62; distributedCache = DistributedCaffeine.&#60;Key, Value&#62;newBuilder(mongoCollection)
     *     ...
     *     .build();
     * </pre>
     *
     * @param mongoCollection the MongoDB collection used for distributed synchronization between cache instances
     * @param <K>             the key type of the cache
     * @param <V>             the value type of the cache
     * @return builder for configuring and constructing cache instances
     */
    public static <K, V> Builder<K, V> newBuilder(MongoCollection<Document> mongoCollection) {
        return new Builder<>(mongoCollection);
    }

    /**
     * Builder for configuring and constructing cache instances of type {@link DistributedCache} (extends {@link Cache})
     * or of type {@link DistributedLoadingCache} (extends {@link LoadingCache}). To construct a builder,
     * {@link DistributedCaffeine#newBuilder(MongoCollection)} must be used.
     *
     * @param <K> the key type of the cache
     * @param <V> the value type of the cache
     */
    public static final class Builder<K, V> {

        private final MongoCollection<Document> mongoCollection;

        private Caffeine<Object, Object> caffeineBuilder;
        private DistributionMode distributionMode;
        private Serializer<K, ?> keySerializer;
        private Serializer<V, ?> valueSerializer;
        private Integer extendedPersistenceSize;
        private Duration extendedPersistenceTime;
        private Cache<K, V> cache;

        private InternalRemovalListener<K, V> wrappedRemovalListener;
        private InternalEvictionListener<K, V> wrappedEvictionListener;
        private InternalCacheLoader<K, V> wrappedCacheLoader;

        private Builder(MongoCollection<Document> mongoCollection) {
            requireNonNull(mongoCollection, "mongoCollection cannot be null");
            this.mongoCollection = mongoCollection;
        }

        /**
         * Specifies the Caffeine builder to be used for configuring the Caffeine cache used internally. This
         * configuration also begins with a builder returned by invoking its own {@code newBuilder()} method, but
         * without finalizing it by invoking one of its own {@code build(...)} methods. Instead, this construction is
         * done internally by the outer {@code build...(...)} methods, as in the following example:
         * <pre>
         * DistributedCache&#60;Key, Value&#62; distributedCache = DistributedCaffeine.&#60;Key, Value&#62;newBuilder(mongoCollection)
         *     .withCaffeineBuilder(Caffeine.newBuilder()
         *         .maximumSize(10_000)
         *         .expireAfterWrite(Duration.ofMinutes(5)))
         *     .build();
         * </pre>
         * <b>Note:</b> A default (empty) Caffeine configuration is used as default if this builder method is
         * skipped.
         * <p>
         * <b>Attention:</b> To ensure the integrity of distributed synchronization between cache instances, the
         * following minor restrictions apply:
         * <ul>
         *      <li>Reference-based eviction using Caffeine's weak or soft references for keys or values is not
         *      supported. Even for the use of Caffeine (stand-alone), it is advised to use the more predictable size-
         *      or time-based eviction instead.</li>
         * </ul>
         *
         * @param caffeineBuilder Caffeine builder instance without final build step
         * @return a builder instance for chaining additional methods
         */
        @SuppressWarnings("unchecked")
        public Builder<K, V> withCaffeineBuilder(Caffeine<?, ?> caffeineBuilder) {
            requireNonNull(caffeineBuilder, "caffeineBuilder cannot be null");
            this.caffeineBuilder = (Caffeine<Object, Object>) caffeineBuilder;
            return this;
        }

        /**
         * Specifies the mode used for distributed synchronization between cache instances.
         * <p>
         * <b>Note:</b> {@link DistributionMode#POPULATION_AND_INVALIDATION_AND_EVICTION} is the default mode if this
         * method is skipped.
         *
         * @param distributionMode distribution mode used for distributed synchronization
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withDistributionMode(DistributionMode distributionMode) {
            requireNonNull(distributionMode, "distributionMode cannot be null");
            this.distributionMode = distributionMode;
            return this;
        }

        /**
         * Specifies that cache entries are serialized with byte array representation using <i>Apache Fury</i> when
         * stored in the MongoDB collection.
         * <p>
         * <b>Note:</b> This is the default serializer if builder methods for serializers are skipped.
         *
         * @param registerClasses optional class of the object (with additional classes of nested objects) to serialize
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withFurySerializer(Class<?>... registerClasses) {
            this.keySerializer = new FurySerializer<>(registerClasses);
            this.valueSerializer = new FurySerializer<>(registerClasses);
            return this;
        }

        /**
         * Specifies that cache entries are serialized with byte array representation using <i>Java Object
         * Serialization</i> when stored in the MongoDB collection. Objects to serialize must implement the
         * {@link java.io.Serializable} interface.
         * <p>
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fury</i> is the default serializer
         * if builder methods for serializers are skipped.
         *
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withJavaObjectSerializer() {
            this.keySerializer = new JavaObjectSerializer<>();
            this.valueSerializer = new JavaObjectSerializer<>();
            return this;
        }

        /**
         * Specifies that cache entries are serialized with JSON representation (encoded as String or BSON) using
         * <i>Jackson</i> when stored in the MongoDB collection. If a default object mapper is sufficient,
         * {@link Builder#withJsonSerializer(Class, Class, boolean)} can be used instead.
         * <p>
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fury</i> is the default serializer
         * if builder methods for serializers are skipped.
         *
         * @param objectMapper the customized object mapper
         * @param keyClass     the class of the key object to serialize
         * @param valueClass   the class of the value object to serialize
         * @param storeAsBson  {@code true} for BSON encoding or {@code false} for string encoding
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withJsonSerializer(ObjectMapper objectMapper,
                                                Class<? super K> keyClass,
                                                Class<? super V> valueClass,
                                                boolean storeAsBson) {
            requireNonNull(objectMapper, "objectMapper cannot be null");
            requireNonNull(keyClass, "keyClass cannot be null");
            requireNonNull(valueClass, "valueClass cannot be null");
            this.keySerializer = new JacksonSerializer<>(objectMapper, keyClass, storeAsBson);
            this.valueSerializer = new JacksonSerializer<>(objectMapper, valueClass, storeAsBson);
            return this;
        }

        /**
         * Specifies that cache entries are serialized with JSON representation (encoded as String or BSON) using
         * <i>Jackson</i> when stored in the MongoDB collection. If a default object mapper is sufficient,
         * {@link Builder#withJsonSerializer(TypeReference, TypeReference, boolean)} can be used instead.
         * <p>
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fury</i> is the default serializer
         * if builder methods for serializers are skipped.
         *
         * @param objectMapper       the customized object mapper
         * @param keyTypeReference   the type reference of the key object
         * @param valueTypeReference the type reference of the value object
         * @param storeAsBson        {@code true} for BSON encoding or {@code false} for string encoding
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withJsonSerializer(ObjectMapper objectMapper,
                                                TypeReference<K> keyTypeReference,
                                                TypeReference<V> valueTypeReference,
                                                boolean storeAsBson) {
            requireNonNull(objectMapper, "objectMapper cannot be null");
            requireNonNull(keyTypeReference, "keyTypeReference cannot be null");
            requireNonNull(valueTypeReference, "valueTypeReference cannot be null");
            this.keySerializer = new JacksonSerializer<>(objectMapper, keyTypeReference, storeAsBson);
            this.valueSerializer = new JacksonSerializer<>(objectMapper, valueTypeReference, storeAsBson);
            return this;
        }

        /**
         * Specifies that cache entries are serialized with JSON representation (encoded as String or BSON) using
         * <i>Jackson</i> when stored in the MongoDB collection. If a customized object mapper is required,
         * {@link Builder#withJsonSerializer(ObjectMapper, Class, Class, boolean)} can be used instead.
         * <p>
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fury</i> is the default serializer
         * if builder methods for serializers are skipped.
         *
         * @param keyClass    the class of the key object to serialize
         * @param valueClass  the class of the value object to serialize
         * @param storeAsBson {@code true} for BSON encoding or {@code false} for string encoding
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withJsonSerializer(Class<? super K> keyClass,
                                                Class<? super V> valueClass,
                                                boolean storeAsBson) {
            requireNonNull(keyClass, "keyClass cannot be null");
            requireNonNull(valueClass, "valueClass cannot be null");
            this.keySerializer = new JacksonSerializer<>(keyClass, storeAsBson);
            this.valueSerializer = new JacksonSerializer<>(valueClass, storeAsBson);
            return this;
        }

        /**
         * Specifies that cache entries are serialized with JSON representation (encoded as String or BSON) using
         * <i>Jackson</i> when stored in the MongoDB collection. If a customized object mapper is required,
         * {@link Builder#withJsonSerializer(ObjectMapper, TypeReference, TypeReference, boolean)} can be used instead.
         * <p>
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fury</i> is the default serializer
         * if builder methods for serializers are skipped.
         *
         * @param keyTypeReference   the type reference of the key object
         * @param valueTypeReference the type reference of the value object
         * @param storeAsBson        {@code true} for BSON encoding or {@code false} for string encoding
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withJsonSerializer(TypeReference<K> keyTypeReference,
                                                TypeReference<V> valueTypeReference,
                                                boolean storeAsBson) {
            requireNonNull(keyTypeReference, "keyTypeReference cannot be null");
            requireNonNull(valueTypeReference, "valueTypeReference cannot be null");
            this.keySerializer = new JacksonSerializer<>(keyTypeReference, storeAsBson);
            this.valueSerializer = new JacksonSerializer<>(valueTypeReference, storeAsBson);
            return this;
        }

        /**
         * Specifies a custom serializer to be used for serializing key objects. Custom serializers must implement one
         * of the following interfaces:
         * <ul>
         *     <li>{@link ByteArraySerializer} for serializing an object to a byte array representation</li>
         *     <li>{@link StringSerializer} for serializing an object to a string representation</li>
         *     <li>{@link JsonSerializer} for serializing an object to a JSON representation (encoded as String or BSON)
         *     </li>
         * </ul>
         * <p>
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fury</i> is the default serializer
         * if builder methods for serializers are skipped.
         *
         * @param keySerializer the custom serializer for key objects
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withCustomKeySerializer(Serializer<K, ?> keySerializer) {
            requireNonNull(keySerializer, "keySerializer cannot be null");
            ensureInstanceOfSerializer(keySerializer);
            this.keySerializer = keySerializer;
            return this;
        }

        /**
         * Specifies a custom serializer to be used for serializing value objects. Custom serializers must implement one
         * of the following interfaces:
         * <ul>
         *     <li>{@link ByteArraySerializer} for serializing an object to a byte array representation</li>
         *     <li>{@link StringSerializer} for serializing an object to a string representation</li>
         *     <li>{@link JsonSerializer} for serializing an object to a JSON representation (encoded as String or BSON)
         *     </li>
         * </ul>
         * <p>
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fury</i> is the default serializer
         * if builder methods for serializers are skipped.
         *
         * @param valueSerializer the custom serializer for value objects
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withCustomValueSerializer(Serializer<V, ?> valueSerializer) {
            requireNonNull(valueSerializer, "valueSerializer cannot be null");
            ensureInstanceOfSerializer(valueSerializer);
            this.valueSerializer = valueSerializer;
            return this;
        }

        private void ensureInstanceOfSerializer(Serializer<?, ?> serializer) {
            List<Class<?>> serializers = List.of(
                    ByteArraySerializer.class, StringSerializer.class, JsonSerializer.class);
            if (serializers.stream()
                    .noneMatch(serializerClass -> serializerClass.isInstance(serializer))) {
                throw new IllegalArgumentException(format(
                        "Custom serializer must implement one of the following interfaces: %s",
                        serializers.stream()
                                .map(Class::getSimpleName)
                                .collect(Collectors.joining(", "))));
            }
        }

        /**
         * Specifies the maximum size for the extended persistence up to which recently evicted cache entries will
         * remain in the MongoDB collection and may be reloaded on demand using a variant of
         * {@link DistributedLoadingCache} instance with special semantics.
         * <p>
         * Special semantics means, that the methods {@link DistributedLoadingCache#get(Object)} and
         * {@link DistributedLoadingCache#getAll(Iterable)} will only invoke the cache loader for missing cache entries
         * that could not be reloaded from the MongoDB collection before.
         * <p>
         * <b>Note:</b> If extended persistence is configured, at least one eviction policy must be configured and only
         * {@code buildWithExtendedPersistence(...)} methods can be used to construct a variant of a
         * {@link DistributedLoadingCache} instance with special semantics.
         *
         * @param maximumSize the maximum size for the extended persistence, must be positive
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withExtendedPersistence(Integer maximumSize) {
            requireNonNull(maximumSize, "maximumSize cannot be null");
            if (maximumSize <= 0) {
                throw new IllegalArgumentException("maximumSize must be positive");
            }
            this.extendedPersistenceSize = maximumSize;
            return this;
        }

        /**
         * Specifies the maximum amount of time for the extended persistence that recently evicted cache entries will
         * remain in the MongoDB collection and may be reloaded on demand using a variant of a
         * {@link DistributedLoadingCache} instance with special semantics.
         * <p>
         * Special semantics means, that the methods {@link DistributedLoadingCache#get(Object)} and
         * {@link DistributedLoadingCache#getAll(Iterable)} will only invoke the cache loader for missing cache entries
         * that could not be reloaded from the MongoDB collection before.
         * <p>
         * <b>Note:</b> If extended persistence is configured, at least one eviction policy must be configured and only
         * {@code buildWithExtendedPersistence(...)} methods can be used to construct a variant of a
         * {@link DistributedLoadingCache} instance with special semantics.
         *
         * @param maximumTime the maximum amount of time for the extended persistence, must be positive
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withExtendedPersistence(Duration maximumTime) {
            requireNonNull(maximumTime, "maximumTime cannot be null");
            if (maximumTime.compareTo(Duration.ZERO) <= 0) {
                throw new IllegalArgumentException("maximumTime must be positive");
            }
            this.extendedPersistenceTime = maximumTime;
            return this;
        }

        /**
         * Constructs a {@link DistributedCache} instance (extends {@link Cache}) (similar to {@link Caffeine#build()}).
         *
         * @return the new distributed cache instance
         */
        public DistributedCache<K, V> build() {
            ensureNoExtendedPersistenceConfiguration();
            DistributedCaffeine<K, V> distributedCaffeine = buildCommon(Caffeine::build);
            InternalDistributedCache<K, V> distributedCache = new InternalDistributedCache<>();
            distributedCache.initialize(distributedCaffeine);
            return distributedCache;
        }

        /**
         * Constructs a {@link DistributedLoadingCache} instance (extends {@link LoadingCache}) (similar to
         * {@link Caffeine#build(CacheLoader)}).
         *
         * @param cacheLoader the cache loader used to obtain new values
         * @return the new distributed loading cache instance
         */
        public DistributedLoadingCache<K, V> build(CacheLoader<K, V> cacheLoader) {
            ensureNoExtendedPersistenceConfiguration();
            requireNonNull(cacheLoader, "cacheLoader cannot be null");
            wrappedCacheLoader = new InternalCacheLoader<>(cacheLoader);
            DistributedCaffeine<K, V> distributedCaffeine = buildCommon(caffeine ->
                    caffeine.build(wrappedCacheLoader));
            InternalDistributedLoadingCache<K, V> distributedLoadingCache = new InternalDistributedLoadingCache<>();
            distributedLoadingCache.initialize(distributedCaffeine);
            return distributedLoadingCache;
        }

        private void ensureNoExtendedPersistenceConfiguration() {
            if (nonNull(extendedPersistenceTime) || nonNull(extendedPersistenceSize)) {
                throw new IllegalStateException(
                        "If an extended persistence size or an extended persistence time is set, "
                                .concat("'buildWithExtendedPersistence(...)' must be used"));
            }
        }

        /**
         * Constructs a variant of a {@link DistributedLoadingCache} instance with special semantics regarding extended
         * persistence which can be configured by size using {@link #withExtendedPersistence(Integer)} or by
         * time using {@link #withExtendedPersistence(Duration)}.
         * <p>
         * Special semantics means, that the methods {@link DistributedLoadingCache#get(Object)} and
         * {@link DistributedLoadingCache#getAll(Iterable)} will only invoke the cache loader for missing cache entries
         * that could not be reloaded from the MongoDB collection before.
         * <p>
         * This method is a shortcut for {@link #buildWithExtendedPersistence(CacheLoader)} using an implicit cache
         * loader that always returns {@code null} values.
         * <p>
         * <b>Note:</b> Before constructing a variant of a {@link DistributedLoadingCache} instance with special
         * semantics, extended persistence and at least one eviction policy must be configured.
         *
         * @return the new distributed loading cache instance with special semantics
         */
        public DistributedLoadingCache<K, V> buildWithExtendedPersistence() {
            ensureExtendedPersistenceConfiguration();
            return buildWithExtendedPersistence(key -> null); // noop cache loader
        }

        /**
         * Constructs a variant of a {@link DistributedLoadingCache} instance with special semantics regarding extended
         * persistence which can be configured by size using {@link #withExtendedPersistence(Integer)} or by
         * time using {@link #withExtendedPersistence(Duration)}.
         * <p>
         * Special semantics means, that the methods {@link DistributedLoadingCache#get(Object)} and
         * {@link DistributedLoadingCache#getAll(Iterable)} will only invoke the cache loader for missing cache entries
         * that could not be reloaded from the MongoDB collection before.
         * <p>
         * If a cache loader is required that always returns {@code null} values,
         * {@link #buildWithExtendedPersistence()} can be used instead.
         * <p>
         * <b>Note:</b> Before constructing a variant of a {@link DistributedLoadingCache} instance with special
         * semantics, extended persistence and at least one eviction policy must be configured.
         *
         * @param cacheLoader the cache loader used to obtain new values
         * @return the new distributed loading cache instance with special semantics
         */
        public DistributedLoadingCache<K, V> buildWithExtendedPersistence(CacheLoader<K, V> cacheLoader) {
            ensureExtendedPersistenceConfiguration();
            requireNonNull(cacheLoader, "cacheLoader cannot be null");
            wrappedCacheLoader = new InternalCacheLoader<>(cacheLoader);
            DistributedCaffeine<K, V> distributedCaffeine = buildCommon(caffeine -> {
                LoadingCache<K, V> loadingCache = caffeine.build(wrappedCacheLoader);
                Policy<K, V> policy = loadingCache.policy();
                if (Stream.of(policy.eviction(), policy.expireAfterAccess(), policy.expireAfterWrite(),
                                policy.expireVariably())
                        .allMatch(Optional::isEmpty)) {
                    throw new IllegalStateException(
                            "If an extended persistence size or an extended persistence time is set, "
                                    .concat("at least one eviction policy must be configured."));
                }
                return loadingCache;
            });
            InternalDistributedLoadingCache<K, V> distributedLoadingCache = new InternalDistributedLoadingCache<>();
            distributedLoadingCache.initialize(distributedCaffeine);
            return distributedLoadingCache;
        }

        private void ensureExtendedPersistenceConfiguration() {
            if (Objects.isNull(extendedPersistenceTime) && Objects.isNull(extendedPersistenceSize)) {
                throw new IllegalStateException(
                        "If no extended persistence size and no extended persistence time is set, "
                                .concat("'build(...)' must be used"));
            }
        }

        @SuppressWarnings("unchecked")
        private DistributedCaffeine<K, V> buildCommon(Function<Caffeine<Object, Object>, Cache<K, V>> build) {
            // use default Caffeine builder if no customized builder is set
            Caffeine<Object, Object> caffeine = Optional.ofNullable(this.caffeineBuilder)
                    .orElseGet(Caffeine::newBuilder);

            // throw exception if weak or soft references are configured
            boolean hasWeakOrSoftReferences;
            try {
                Method isStrongKeysMethod = caffeine.getClass().getDeclaredMethod("isStrongKeys");
                Method isStrongValuesMethod = caffeine.getClass().getDeclaredMethod("isStrongValues");
                isStrongKeysMethod.setAccessible(true);
                isStrongValuesMethod.setAccessible(true);
                hasWeakOrSoftReferences = !((Boolean) isStrongKeysMethod.invoke(caffeine)
                        && (Boolean) isStrongValuesMethod.invoke(caffeine));
                isStrongKeysMethod.setAccessible(false);
                isStrongValuesMethod.setAccessible(false);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            if (hasWeakOrSoftReferences) {
                throw new IllegalStateException("The use of weak or soft references is not supported");
            }

            // inject wrapped removal and eviction listener
            try {
                Field removalListenerField = caffeine.getClass().getDeclaredField("removalListener");
                Field evictionListenerField = caffeine.getClass().getDeclaredField("evictionListener");
                removalListenerField.setAccessible(true);
                evictionListenerField.setAccessible(true);
                RemovalListener<K, V> removalListener = (RemovalListener<K, V>) removalListenerField.get(caffeine);
                RemovalListener<K, V> evictionListener = (RemovalListener<K, V>) evictionListenerField.get(caffeine);
                RemovalListener<K, V> noopListener = (key, value, removalCause) -> {
                };
                wrappedRemovalListener = new InternalRemovalListener<>(nonNull(removalListener)
                        ? removalListener
                        : noopListener);
                wrappedEvictionListener = new InternalEvictionListener<>(nonNull(evictionListener)
                        ? evictionListener
                        : noopListener);
                removalListenerField.set(caffeine, wrappedRemovalListener);
                evictionListenerField.set(caffeine, wrappedEvictionListener);
                removalListenerField.setAccessible(false);
                evictionListenerField.setAccessible(false);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            // set defaults
            if (isNull(this.distributionMode)) {
                this.distributionMode = POPULATION_AND_INVALIDATION_AND_EVICTION;
            }
            if (isNull(this.keySerializer)) {
                this.keySerializer = new FurySerializer<>();
            }
            if (isNull(this.valueSerializer)) {
                this.valueSerializer = new FurySerializer<>();
            }

            // build final Caffeine cache instance
            this.cache = build.apply(caffeine);

            // construct and return DistributedCaffeine instance
            return new DistributedCaffeine<>(this);
        }
    }

    void activate() {
        cacheManager.activate();
        changeStreamWatcher.activate();
        maintenanceWorker.activate();
        // synchronization after watching so that no changes are missed
        synchronizeCacheWithMongoCollection();
    }

    void deactivate() {
        if (isActivated()) {
            maintenanceWorker.deactivate();
            changeStreamWatcher.deactivate();
            cacheManager.deactivate();
        }
    }

    boolean isActivated() {
        return cacheManager.isActivated()
                && changeStreamWatcher.isActivated()
                && maintenanceWorker.isActivated();
    }

    void processOutboundInsert(Map<? extends K, ? extends V> map, Status status,
                               boolean manage, boolean originConscious) {
        if (status.matches(distributionMode)) {
            manage = manage && CACHED.matches(distributionMode);
            cacheManager.manageOutboundInsert(map, status, manage, originConscious);
        }
    }

    void processInboundInsert(InternalCacheDocument<K, V> cacheDocument, boolean isChangeStream) {
        if (cacheDocument.getStatus().matches(distributionMode)) {
            synchronizationLock.lock();
            try {
                boolean manage = CACHED.matches(distributionMode);
                cacheManager.manageInboundInsert(cacheDocument, manage, isChangeStream);
            } finally {
                synchronizationLock.unlock();
            }
        }
    }

    void processInboundUpdate(InternalCacheDocument<K, V> cacheDocument) {
        if (CACHED.matches(distributionMode)) {
            synchronizationLock.lock();
            try {
                cacheManager.manageInboundUpdate(cacheDocument);
            } finally {
                synchronizationLock.unlock();
            }
        }
    }

    void processInboundDelete(ObjectId objectId) {
        if (CACHED.matches(distributionMode)) {
            synchronizationLock.lock();
            try {
                cacheManager.manageInboundDelete(objectId);
            } finally {
                synchronizationLock.unlock();
            }
        }
    }

    void processInboundFailure(ObjectId objectId) {
        if (CACHED.matches(distributionMode)) {
            synchronizationLock.lock();
            try {
                cacheManager.manageInboundFailure(objectId);
            } finally {
                synchronizationLock.unlock();
            }
        }
    }

    V putDistributed(K key, V value) {
        putAllDistributed(Map.of(key, value));
        return value;
    }

    Map<? extends K, ? extends V> putAllDistributed(Map<? extends K, ? extends V> map) {
        processOutboundInsert(map, CACHED, true, true);
        return map;
    }

    V putDistributedRefresh(K key, V newValue, V oldValue) {
        if (isActivated()) {
            if (CACHED.matches(distributionMode)) {
                processOutboundInsert(Map.of(key, newValue), CACHED, false, false);
                // return old value which does not change the cache and does not trigger the removal listener
                return oldValue;
            } else {
                return newValue;
            }
        } else {
            return newValue;
        }
    }

    K invalidateDistributed(K key) {
        invalidateAllDistributed(Set.of(key));
        return key;
    }

    Set<K> invalidateAllDistributed(Set<K> keys) {
        Map<K, V> map = new HashMap<>(); // allows null values
        keys.forEach(key -> map.put(key, null));
        processOutboundInsert(map, INVALIDATED, true, true);
        return keys;
    }

    V invalidateDistributedRefresh(K key, V oldValue) {
        if (isActivated()) {
            if (INVALIDATED.matches(distributionMode)) {
                Map<K, V> map = new HashMap<>(); // allows null values
                map.put(key, null);
                processOutboundInsert(map, INVALIDATED, false, false);
                // return old value which does not change the cache and does not trigger the removal listener
                return oldValue;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    void evictDistributed(K key, V value, RemovalCause removalCause) {
        // distribution of reference-based evictions is not supported (yet)
        if (removalCause != RemovalCause.COLLECTED) {
            Status status = extendedPersistenceConfig.isExtendedPersistence()
                    ? EXTENDED
                    : EVICTED;
            processOutboundInsert(Map.of(key, value), status, false, true);
        }
        // manage replacement if populations are distributed, but evictions are not distributed
        if (CACHED.matches(distributionMode) && !EVICTED.matches(distributionMode)) {
            cacheManager.manageReplacement(key, value, removalCause);
        }
    }

    private void synchronizeCacheWithMongoCollection() {
        if (CACHED.matches(distributionMode)) {
            Bson filter = Filters.ne(STATUS.toString(), ORPHANED.toString());
            try (Stream<InternalCacheDocument<K, V>> cacheDocumentStream =
                         mongoRepository.streamCacheDocuments(filter)) {
                cacheDocumentStream
                        .collect(Collectors.groupingBy(InternalCacheDocument::getKey))
                        .forEach((k, cacheDocuments) -> {
                            List<InternalCacheDocument<K, V>> sortedCacheDocuments = cacheDocuments.stream()
                                    .sorted(Comparator.reverseOrder())
                                    .collect(Collectors.toCollection(ArrayList::new));
                            if (!sortedCacheDocuments.isEmpty()) {
                                // newest cache entry must be treated in the same way as a distributed inbound insert
                                processInboundInsert(sortedCacheDocuments.remove(0), false);
                                // correct any inconsistencies (if any) in relation to (not yet) orphaned cache entries
                                sortedCacheDocuments.forEach(cacheManager::manageReplacement);
                            }
                        });
            }
            // remove cache entries which are not managed (e.g. if synchronization is started after it was stopped)
            synchronizationLock.lock();
            try {
                cacheManager.manageSynchronization();
            } finally {
                synchronizationLock.unlock();
            }
        }
    }

    Logger getLogger() {
        return requireNonNull(logger);
    }

    MongoCollection<Document> getMongoCollection() {
        return requireNonNull(mongoCollection);
    }

    DistributionMode getDistributionMode() {
        return requireNonNull(distributionMode);
    }

    Serializer<K, ?> getKeySerializer() {
        return requireNonNull(keySerializer);
    }

    Serializer<V, ?> getValueSerializer() {
        return requireNonNull(valueSerializer);
    }

    ExtendedPersistenceConfig getExtendedPersistenceConfig() {
        return requireNonNull(extendedPersistenceConfig);
    }

    Cache<K, V> getCache() {
        return requireNonNull(cache);
    }

    InternalSynchronizationLock getSynchronizationLock() {
        return requireNonNull(synchronizationLock);
    }

    InternalObjectIdGenerator getObjectIdGenerator() {
        return requireNonNull(objectIdGenerator);
    }

    InternalDocumentConverter<K, V> getDocumentConverter() {
        return requireNonNull(documentConverter);
    }

    InternalMongoRepository<K, V> getMongoRepository() {
        return requireNonNull(mongoRepository);
    }

    InternalCacheManager<K, V> getCacheManager() {
        return requireNonNull(cacheManager);
    }

    InternalChangeStreamWatcher<K, V> getChangeStreamWatcher() {
        return requireNonNull(changeStreamWatcher);
    }

    InternalMaintenanceWorker<K, V> getMaintenanceWorker() {
        return requireNonNull(maintenanceWorker);
    }

    static class ExtendedPersistenceConfig {

        private final Integer extendedPersistenceSize;
        private final Duration extendedPersistenceTime;

        public ExtendedPersistenceConfig(Integer extendedPersistenceSize, Duration extendedPersistenceTime) {
            this.extendedPersistenceSize = extendedPersistenceSize;
            this.extendedPersistenceTime = extendedPersistenceTime;
        }

        public Integer getExtendedPersistenceSize() {
            return extendedPersistenceSize;
        }

        public Duration getExtendedPersistenceTime() {
            return extendedPersistenceTime;
        }

        public boolean isExtendedPersistence() {
            return nonNull(extendedPersistenceSize) || nonNull(extendedPersistenceTime);
        }
    }

    interface LazyInitializer<K, V> {

        void initialize(DistributedCaffeine<K, V> distributedCaffeine);
    }
}
