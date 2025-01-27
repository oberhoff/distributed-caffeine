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
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.ForySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JavaObjectSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.bson.Document;

import java.lang.System.Logger;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.DISCRIMINATOR;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.ORIGIN;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.STALE;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.runFailable;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

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
 * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
 */
@SuppressWarnings("squid:S1452")
public final class DistributedCaffeine<K, V> {

    private final Logger logger;

    private final MongoCollection<Document> mongoCollection;
    private final DistributionMode distributionMode;
    private final Serializer<K, ?> keySerializer;
    private final Serializer<V, ?> valueSerializer;
    private final InternalExtendedPersistence extendedPersistence;
    private final InternalCacheLoader<K, V> cacheLoader;
    private final Executor executor;
    private final Cache<K, V> cache;

    private final InternalDocumentConverter<K, V> documentConverter;
    private final InternalMongoRepository<K, V> mongoRepository;
    private final InternalCacheManager<K, V> cacheManager;
    private final InternalChangeStreamWatcher<K, V> changeStreamWatcher;
    private final InternalMaintenanceWorker<K, V> maintenanceWorker;
    private final InternalSynchronizationLock synchronizationLock;
    private final Long origin;

    private DistributedCaffeine(Builder<K, V> builder) {
        this.logger = System.getLogger(getClass().getName());

        this.mongoCollection = builder.mongoCollection;
        this.distributionMode = builder.distributionMode;
        this.keySerializer = builder.keySerializer;
        this.valueSerializer = builder.valueSerializer;
        this.extendedPersistence = new InternalExtendedPersistence(
                builder.extendedPersistenceSize, builder.extendedPersistenceTime, builder.extendedPersistenceLoader);
        this.cacheLoader = builder.cacheLoader;
        this.executor = builder.executor;
        this.cache = builder.cache;

        this.documentConverter = new InternalDocumentConverter<>();
        this.mongoRepository = new InternalMongoRepository<>();
        this.cacheManager = new InternalCacheManager<>();
        this.changeStreamWatcher = new InternalChangeStreamWatcher<>();
        this.maintenanceWorker = new InternalMaintenanceWorker<>();
        this.synchronizationLock = new InternalSynchronizationLock();
        this.origin = new SecureRandom().nextLong();

        Stream.of(builder.removalListener, builder.evictionListener, builder.cacheLoader, this.documentConverter,
                        this.mongoRepository, this.cacheManager, this.changeStreamWatcher, this.maintenanceWorker)
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
     * Exemplary usage:
     * <pre>
     * DistributedCache&#60;Key, Value&#62; distributedCache = DistributedCaffeine.newBuilder(mongoCollection)
     *     ...
     *     .build();
     * </pre>
     *
     * @param mongoCollection the MongoDB collection used for distributed synchronization between cache instances
     * @param <K>             the key type of the cache
     * @param <V>             the value type of the cache
     * @return builder for configuring and constructing cache instances
     * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
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
        private boolean extendedPersistenceLoader;
        private Cache<K, V> cache;

        private InternalRemovalListener<K, V> removalListener;
        private InternalEvictionListener<K, V> evictionListener;
        private Executor executor;
        private InternalCacheLoader<K, V> cacheLoader;

        private Builder(MongoCollection<Document> mongoCollection) {
            requireNonNull(mongoCollection, "mongoCollection cannot be null");
            this.mongoCollection = mongoCollection;
        }

        /**
         * Specifies the Caffeine builder to be used for configuring the Caffeine cache used internally. This
         * configuration also begins with a builder returned by invoking its own {@code newBuilder()} method, but
         * without finalizing it by invoking one of its own {@code build(...)} methods. Instead, this construction is
         * done internally by the outer {@code build...(...)} methods.
         * <p>
         * Exemplary usage:
         * <pre>
         * DistributedCache&#60;Key, Value&#62; distributedCache = DistributedCaffeine.newBuilder(mongoCollection)
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
         * @param caffeineBuilder Caffeine builder instance without a final build step
         * @return a builder instance for chaining additional methods
         * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
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
         * Specifies that cache entries are serialized with byte array representation using <i>Apache Fory</i> when
         * stored in the MongoDB collection.
         * <p>
         * <b>Note:</b> This is the default serializer if builder methods for serializers are skipped.
         *
         * @param registerClasses optional class of the object (with additional classes of nested objects) to serialize
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withForySerializer(Class<?>... registerClasses) {
            this.keySerializer = new ForySerializer<>(registerClasses);
            this.valueSerializer = new ForySerializer<>(registerClasses);
            return this;
        }

        /**
         * Specifies that cache entries are serialized with byte array representation using <i>Java Object
         * Serialization</i> when stored in the MongoDB collection. Objects to serialize must implement the
         * {@link java.io.Serializable} interface.
         * <p>
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fory</i> is the default serializer
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
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fory</i> is the default serializer
         * if builder methods for serializers are skipped.
         *
         * @param objectMapper      the customized object mapper
         * @param keyClass          the class of the key object to serialize
         * @param valueClass        the class of the value object to serialize
         * @param storeAsBinaryJson {@code true} for BSON encoding or {@code false} for string encoding
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withJsonSerializer(ObjectMapper objectMapper,
                                                Class<K> keyClass,
                                                Class<V> valueClass,
                                                boolean storeAsBinaryJson) {
            requireNonNull(objectMapper, "objectMapper cannot be null");
            requireNonNull(keyClass, "keyClass cannot be null");
            requireNonNull(valueClass, "valueClass cannot be null");
            this.keySerializer = new JacksonSerializer<>(objectMapper, keyClass, storeAsBinaryJson);
            this.valueSerializer = new JacksonSerializer<>(objectMapper, valueClass, storeAsBinaryJson);
            return this;
        }

        /**
         * Specifies that cache entries are serialized with JSON representation (encoded as String or BSON) using
         * <i>Jackson</i> when stored in the MongoDB collection. If a default object mapper is sufficient,
         * {@link Builder#withJsonSerializer(TypeReference, TypeReference, boolean)} can be used instead.
         * <p>
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fory</i> is the default serializer
         * if builder methods for serializers are skipped.
         *
         * @param objectMapper       the customized object mapper
         * @param keyTypeReference   the type reference of the key object
         * @param valueTypeReference the type reference of the value object
         * @param storeAsBinaryJson  {@code true} for BSON encoding or {@code false} for string encoding
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withJsonSerializer(ObjectMapper objectMapper,
                                                TypeReference<K> keyTypeReference,
                                                TypeReference<V> valueTypeReference,
                                                boolean storeAsBinaryJson) {
            requireNonNull(objectMapper, "objectMapper cannot be null");
            requireNonNull(keyTypeReference, "keyTypeReference cannot be null");
            requireNonNull(valueTypeReference, "valueTypeReference cannot be null");
            this.keySerializer = new JacksonSerializer<>(objectMapper, keyTypeReference, storeAsBinaryJson);
            this.valueSerializer = new JacksonSerializer<>(objectMapper, valueTypeReference, storeAsBinaryJson);
            return this;
        }

        /**
         * Specifies that cache entries are serialized with JSON representation (encoded as String or BSON) using
         * <i>Jackson</i> when stored in the MongoDB collection. If a customized object mapper is required,
         * {@link Builder#withJsonSerializer(ObjectMapper, Class, Class, boolean)} can be used instead.
         * <p>
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fory</i> is the default serializer
         * if builder methods for serializers are skipped.
         *
         * @param keyClass          the class of the key object to serialize
         * @param valueClass        the class of the value object to serialize
         * @param storeAsBinaryJson {@code true} for BSON encoding or {@code false} for string encoding
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withJsonSerializer(Class<K> keyClass,
                                                Class<V> valueClass,
                                                boolean storeAsBinaryJson) {
            requireNonNull(keyClass, "keyClass cannot be null");
            requireNonNull(valueClass, "valueClass cannot be null");
            this.keySerializer = new JacksonSerializer<>(keyClass, storeAsBinaryJson);
            this.valueSerializer = new JacksonSerializer<>(valueClass, storeAsBinaryJson);
            return this;
        }

        /**
         * Specifies that cache entries are serialized with JSON representation (encoded as String or BSON) using
         * <i>Jackson</i> when stored in the MongoDB collection. If a customized object mapper is required,
         * {@link Builder#withJsonSerializer(ObjectMapper, TypeReference, TypeReference, boolean)} can be used instead.
         * <p>
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fory</i> is the default serializer
         * if builder methods for serializers are skipped.
         *
         * @param keyTypeReference   the type reference of the key object
         * @param valueTypeReference the type reference of the value object
         * @param storeAsBinaryJson  {@code true} for BSON encoding or {@code false} for string encoding
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withJsonSerializer(TypeReference<K> keyTypeReference,
                                                TypeReference<V> valueTypeReference,
                                                boolean storeAsBinaryJson) {
            requireNonNull(keyTypeReference, "keyTypeReference cannot be null");
            requireNonNull(valueTypeReference, "valueTypeReference cannot be null");
            this.keySerializer = new JacksonSerializer<>(keyTypeReference, storeAsBinaryJson);
            this.valueSerializer = new JacksonSerializer<>(valueTypeReference, storeAsBinaryJson);
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
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fory</i> is the default serializer
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
         * <b>Note:</b> A serializer with byte array representation using <i>Apache Fory</i> is the default serializer
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
                                .collect(joining(", "))));
            }
        }

        /**
         * Specifies the maximum size for the extended persistence up to which recently evicted cache entries will
         * remain in the MongoDB collection and may be reloaded on demand.
         * <p>
         * Cache entries with extended persistence can be reloaded using a variant of a {@link DistributedLoadingCache}
         * instance with special semantics which can be constructed using {@link #buildWithExtendedPersistence()} or
         * {@link #buildWithExtendedPersistence(CacheLoader)}. Alternatively,
         * {@link DistributedPolicy#getFromMongo(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromMongo(Iterable, boolean)} can be used to load those cache entries directly
         * from the MongoDB collection bypassing this cache instance.
         * <p>
         * <b>Note:</b> If extended persistence is configured, at least one eviction policy must be configured.
         *
         * @param maximumSize the maximum size for the extended persistence (must be positive)
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
         * remain in the MongoDB collection and may be reloaded on demand.
         * <p>
         * Cache entries with extended persistence can be reloaded using a variant of a {@link DistributedLoadingCache}
         * instance with special semantics which can be constructed using {@link #buildWithExtendedPersistence()} or
         * {@link #buildWithExtendedPersistence(CacheLoader)}. Alternatively,
         * {@link DistributedPolicy#getFromMongo(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromMongo(Iterable, boolean)} can be used to load those cache entries directly
         * from the MongoDB collection bypassing this cache instance.
         * <p>
         * <b>Note:</b> If extended persistence is configured, at least one eviction policy must be configured.
         *
         * @param maximumTime the maximum amount of time for the extended persistence (must be positive)
         * @return a builder instance for chaining additional methods
         */
        public Builder<K, V> withExtendedPersistence(Duration maximumTime) {
            requireNonNull(maximumTime, "maximumTime cannot be null");
            if (maximumTime.isZero() || maximumTime.isNegative()) {
                throw new IllegalArgumentException("maximumTime must be positive");
            }
            this.extendedPersistenceTime = maximumTime;
            return this;
        }

        /**
         * Constructs a {@link DistributedCache} (extends {@link Cache}) instance (similar to {@link Caffeine#build()}).
         *
         * @param <K1> the key type of the cache (same as {@link K})
         * @param <V1> the value type of the cache (same as {@link V})
         * @return the new distributed cache instance
         */
        @SuppressWarnings("unchecked")
        public <K1 extends K, V1 extends V> DistributedCache<K1, V1> build() {
            DistributedCaffeine<K, V> distributedCaffeine = buildCommon(Caffeine::build);
            InternalDistributedCache<K, V> distributedCache = new InternalDistributedCache<>();
            distributedCache.initialize(distributedCaffeine);
            return (DistributedCache<K1, V1>) distributedCache;
        }

        /**
         * Constructs a {@link DistributedLoadingCache} (extends {@link LoadingCache}) instance (similar to
         * {@link Caffeine#build(CacheLoader)}).
         *
         * @param cacheLoader the cache loader used to obtain new values
         * @param <K1>        the key type of the cache (same as {@link K})
         * @param <V1>        the value type of the cache (same as {@link V})
         * @return the new distributed loading cache instance
         */
        @SuppressWarnings("unchecked")
        public <K1 extends K, V1 extends V> DistributedLoadingCache<K1, V1> build(
                CacheLoader<? super K1, ? super V1> cacheLoader) {
            requireNonNull(cacheLoader, "cacheLoader cannot be null");
            this.cacheLoader = new InternalCacheLoader<>((CacheLoader<K, V>) cacheLoader);
            DistributedCaffeine<K, V> distributedCaffeine = buildCommon(caffeine -> caffeine.build(this.cacheLoader));
            InternalDistributedLoadingCache<K, V> distributedLoadingCache = new InternalDistributedLoadingCache<>();
            distributedLoadingCache.initialize(distributedCaffeine);
            return (DistributedLoadingCache<K1, V1>) distributedLoadingCache;
        }

        /**
         * Constructs a variant of a {@link DistributedLoadingCache} (extends {@link LoadingCache}) instance with
         * special semantics regarding extended persistence which can be configured by size using
         * {@link #withExtendedPersistence(Integer)} or by time using {@link #withExtendedPersistence(Duration)}.
         * <p>
         * Special semantics means that a provided {@link CacheLoader} is only invoked to obtain missing cache entries
         * if these could not be reloaded from the MongoDB collection beforehand.
         * <p>
         * This method is a shortcut for {@link #buildWithExtendedPersistence(CacheLoader)} using an implicit cache
         * loader that always returns {@code null} values.
         * <p>
         * <b>Note:</b> Before constructing a variant of a {@link DistributedLoadingCache} instance with special
         * semantics, extended persistence and at least one eviction policy must be configured.
         *
         * @param <K1> the key type of the cache (same as {@link K})
         * @param <V1> the value type of the cache  (same as {@link V})
         * @return the new distributed loading cache instance with special semantics
         */
        public <K1 extends K, V1 extends V> DistributedLoadingCache<K1, V1> buildWithExtendedPersistence() {
            return buildWithExtendedPersistence(key -> null);
        }

        /**
         * Constructs a variant of a {@link DistributedLoadingCache} (extends {@link LoadingCache}) instance with
         * special semantics regarding extended persistence which can be configured by size using
         * {@link #withExtendedPersistence(Integer)} or by time using {@link #withExtendedPersistence(Duration)}.
         * <p>
         * Special semantics means that a provided {@link CacheLoader} is only invoked to obtain missing cache entries
         * if these could not be reloaded from the MongoDB collection beforehand.
         * <p>
         * If a cache loader is required that always returns {@code null} values,
         * {@link #buildWithExtendedPersistence()} can be used instead.
         * <p>
         * <b>Note:</b> Before constructing a variant of a {@link DistributedLoadingCache} instance with special
         * semantics, extended persistence and at least one eviction policy must be configured.
         *
         * @param cacheLoader the cache loader used to obtain new values
         * @param <K1>        the key type of the cache (same as {@link K})
         * @param <V1>        the value type of the cache (same as {@link V})
         * @return the new distributed loading cache instance with special semantics
         */
        @SuppressWarnings("unchecked")
        public <K1 extends K, V1 extends V> DistributedLoadingCache<K1, V1> buildWithExtendedPersistence(
                CacheLoader<? super K1, ? super V1> cacheLoader) {
            requireNonNull(cacheLoader, "cacheLoader cannot be null");
            if (isNull(extendedPersistenceTime) && isNull(extendedPersistenceSize)) {
                throw new IllegalStateException(
                        "If no extended persistence size and no extended persistence time is set, "
                                .concat("'build(...)' must be used"));
            }
            this.cacheLoader = new InternalCacheLoader<>((CacheLoader<K, V>) cacheLoader);
            this.extendedPersistenceLoader = true;
            DistributedCaffeine<K, V> distributedCaffeine = buildCommon(caffeine -> caffeine.build(this.cacheLoader));
            InternalDistributedLoadingCache<K, V> distributedLoadingCache = new InternalDistributedLoadingCache<>();
            distributedLoadingCache.initialize(distributedCaffeine);
            return (DistributedLoadingCache<K1, V1>) distributedLoadingCache;
        }

        @SuppressWarnings({"unchecked", "squid:S3011"})
        private DistributedCaffeine<K, V> buildCommon(Function<Caffeine<Object, Object>, Cache<K, V>> build) {
            // use default Caffeine builder if no customized builder is set
            Caffeine<Object, Object> caffeine = Optional.ofNullable(this.caffeineBuilder)
                    .orElseGet(Caffeine::newBuilder);

            // throw exception if weak or soft references are configured
            boolean hasWeakOrSoftReferences;
            Method isStrongKeysMethod = getFailable(() ->
                    caffeine.getClass().getDeclaredMethod("isStrongKeys"));
            Method isStrongValuesMethod = getFailable(() ->
                    caffeine.getClass().getDeclaredMethod("isStrongValues"));
            isStrongKeysMethod.setAccessible(true);
            isStrongValuesMethod.setAccessible(true);
            hasWeakOrSoftReferences = !((Boolean) getFailable(() -> isStrongKeysMethod.invoke(caffeine))
                    || (Boolean) getFailable(() -> isStrongValuesMethod.invoke(caffeine)));
            isStrongKeysMethod.setAccessible(false);
            isStrongValuesMethod.setAccessible(false);
            if (hasWeakOrSoftReferences) {
                throw new IllegalStateException("The use of weak or soft references is not supported");
            }

            // inject removal and eviction listener
            Field removalListenerField = getFailable(() ->
                    caffeine.getClass().getDeclaredField("removalListener"));
            Field evictionListenerField = getFailable(() ->
                    caffeine.getClass().getDeclaredField("evictionListener"));
            removalListenerField.setAccessible(true);
            evictionListenerField.setAccessible(true);
            RemovalListener<K, V> caffeineRemovalListener = getFailable(() ->
                    (RemovalListener<K, V>) removalListenerField.get(caffeine));
            RemovalListener<K, V> caffeineEvictionListener = getFailable(() ->
                    (RemovalListener<K, V>) evictionListenerField.get(caffeine));
            RemovalListener<K, V> noopListener = (key, value, removalCause) -> {
            };
            this.removalListener = new InternalRemovalListener<>(nonNull(caffeineRemovalListener)
                    ? caffeineRemovalListener
                    : noopListener);
            this.evictionListener = new InternalEvictionListener<>(nonNull(caffeineEvictionListener)
                    ? caffeineEvictionListener
                    : noopListener);
            runFailable(() -> removalListenerField.set(caffeine, removalListener));
            runFailable(() -> evictionListenerField.set(caffeine, evictionListener));
            removalListenerField.setAccessible(false);
            evictionListenerField.setAccessible(false);

            // extract executor
            Field executorField = getFailable(() ->
                    caffeine.getClass().getDeclaredField("executor"));
            executorField.setAccessible(true);
            this.executor = Optional.ofNullable(
                            getFailable(() -> (Executor) executorField.get(caffeine)))
                    .orElseGet(ForkJoinPool::commonPool);
            executorField.setAccessible(false);

            // set defaults
            if (isNull(this.distributionMode)) {
                this.distributionMode = POPULATION_AND_INVALIDATION_AND_EVICTION;
            }
            if (isNull(this.keySerializer)) {
                this.keySerializer = new ForySerializer<>();
            }
            if (isNull(this.valueSerializer)) {
                this.valueSerializer = new ForySerializer<>();
            }

            // build final Caffeine cache instance
            this.cache = build.apply(caffeine);

            // throw exception if extended persistence is configured but eviction policies are missing
            Policy<K, V> policy = cache.policy();
            if ((nonNull(extendedPersistenceSize) || nonNull(extendedPersistenceTime))
                    && Stream.of(policy.eviction(), policy.expireAfterAccess(), policy.expireAfterWrite(),
                            policy.expireVariably())
                    .allMatch(Optional::isEmpty)) {
                throw new IllegalStateException(
                        "If an extended persistence size or an extended persistence time is set, "
                                .concat("at least one eviction policy must be configured."));
            }

            // construct and return DistributedCaffeine instance
            return new DistributedCaffeine<>(this);
        }
    }

    void activate() {
        if (!isActivated()) {
            // migrations can be removed in future releases
            mongoCollection.updateMany(
                    Filters.exists(DISCRIMINATOR.toString(), false),
                    Updates.set(DISCRIMINATOR.toString(), null));
            mongoCollection.updateMany(
                    Filters.exists(ORIGIN.toString(), false),
                    Updates.set(ORIGIN.toString(), 0L));
            mongoCollection.updateMany(
                    Filters.exists(STALE.toString(), false),
                    Updates.set(STALE.toString(), false));
            synchronizationLock.runLocked(() -> {
                cacheManager.activate();
                changeStreamWatcher.activate();
                maintenanceWorker.activate();
            });
            // synchronization after watching so that no changes are missed
            cacheManager.synchronizeCacheFromStore();
        }
    }

    void deactivate() {
        if (isActivated()) {
            synchronizationLock.runLocked(() -> {
                maintenanceWorker.deactivate();
                changeStreamWatcher.deactivate();
                cacheManager.deactivate();
            });
        }
    }

    boolean isActivated() {
        return cacheManager.isActivated()
                && changeStreamWatcher.isActivated()
                && maintenanceWorker.isActivated();
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

    InternalExtendedPersistence getExtendedPersistence() {
        return requireNonNull(extendedPersistence);
    }

    InternalCacheLoader<K, V> getCacheLoader() {
        return requireNonNull(cacheLoader);
    }

    Executor getExecutor() {
        return requireNonNull(executor);
    }

    Cache<K, V> getCache() {
        return requireNonNull(cache);
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

    InternalSynchronizationLock getSynchronizationLock() {
        return requireNonNull(synchronizationLock);
    }

    Long getOrigin() {
        return requireNonNull(origin);
    }
}
