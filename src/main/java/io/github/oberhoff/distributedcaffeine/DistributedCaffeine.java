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
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import io.github.oberhoff.distributedcaffeine.hasher.HashProvider;
import io.github.oberhoff.distributedcaffeine.hasher.Hashable;
import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.ForySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JavaObjectSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.runFailable;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Starting point for configuring and constructing Distributed Caffeine cache instances using a builder pattern instance
 * returned by {@link #newBuilder(Adapter)}.
 * <p>
 * Cache instances can be of type {@link DistributedCache} (extends {@link Cache}) or of type
 * {@link DistributedLoadingCache} (extends {@link LoadingCache}).
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
@NullMarked
public final class DistributedCaffeine<K, V> {

    private final Adapter<K, V> adapter;
    private Caffeine<Object, Object> caffeine;
    private InternalHasher<K> hasher;
    private DistributionMode distributionMode;
    private SerializersConfigurer<K, V> serializersConfigurer;
    private ExtendedPersistenceConfigurer extendedPersistenceConfigurer;

    private DistributedCaffeine(Adapter<K, V> adapter) {
        this.adapter = adapter;
        // set defaults
        this.caffeine = Caffeine.newBuilder();
        this.hasher = new InternalHasher<>(null);
        this.distributionMode = POPULATION_AND_INVALIDATION_AND_EVICTION;
        this.serializersConfigurer = new SerializersConfigurer<>();
        this.extendedPersistenceConfigurer = new ExtendedPersistenceConfigurer();
    }

    /**
     * Returns a new builder pattern instance for configuring and constructing cache instances. For example, the builder
     * pattern instance can be finalized with {@link DistributedCaffeine#build()} to construct a cache instance of type
     * {@link DistributedCache} (extends {@link Cache}) or with {@link DistributedCaffeine#build(CacheLoader)} to
     * construct a loading cache instance of type {@link DistributedLoadingCache} (extends {@link LoadingCache}).
     * <p>
     * Exemplary usage:
     * <pre>
     * DistributedCache&#60;Key, Value&#62; distributedCache = DistributedCaffeine.newBuilder(adapter)
     *     ...
     *     .build();
     * </pre>
     *
     * @param adapter the adapter used for distributed synchronization between cache instances
     * @param <K>     the key type of the cache
     * @param <V>     the value type of the cache
     * @return builder pattern instance for configuring and constructing cache instances
     * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
     */
    public static <K, V> DistributedCaffeine<K, V> newBuilder(Adapter<K, V> adapter) {
        requireNonNull(adapter, "adapter cannot be null");
        return new DistributedCaffeine<>(adapter);
    }

    /**
     * Specifies the configuration of the Caffeine cache used internally. This configuration also begins with a builder
     * pattern instance returned by invoking its own {@code newBuilder()} method, but without finalizing it by invoking
     * one of its own {@code build(...)} methods. Instead, this construction is done internally by the outer
     * {@code build(...)} methods.
     * <p>
     * Exemplary usage:
     * <pre>
     * DistributedCache&#60;Key, Value&#62; distributedCache = DistributedCaffeine.newBuilder(adapter)
     *     .withCaffeine(Caffeine.newBuilder()
     *         .maximumSize(10_000)
     *         .expireAfterWrite(Duration.ofMinutes(5)))
     *     .build();
     * </pre>
     * <b>Note:</b> An "empty" Caffeine configuration is used as default if this method is skipped.
     * <p>
     * <b>Attention:</b> To ensure the integrity of distributed synchronization between cache instances, the
     * following minor restrictions apply:
     * <ul>
     *      <li>Reference-based eviction using Caffeine's weak or soft references for keys or values is not
     *      supported. Even for the use of Caffeine (stand-alone), it is advised to use the more predictable size-
     *      or time-based eviction instead.</li>
     * </ul>
     *
     * @param caffeine Caffeine builder pattern instance without a final build step
     * @return a builder pattern instance for chaining additional methods
     * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
     */
    @SuppressWarnings("unchecked")
    public DistributedCaffeine<K, V> withCaffeine(Caffeine<?, ?> caffeine) {
        requireNonNull(caffeine, "caffeine cannot be null");
        this.caffeine = (Caffeine<Object, Object>) caffeine;
        return this;
    }

    /**
     * Specifies the hash provider used for generating hashes for key objects. Alternatively, key objects can
     * implement the {@link Hashable} interface, in which case this method can be skipped.
     * <p>
     * Exemplary usage:
     * <pre>
     * ...
     * .withHashProvider((hasher, key) -> hasher.get()
     *     .putUuid(key.getId())
     *     .putLong(key.getVersion())
     *     .putString(key.getName())
     *     .put...
     *     .getHash())
     * ...
     * </pre>
     * <b>Note:</b> A specified hash provider always takes precedence, even if key objects implement the
     * {@link Hashable} interface (implementation is mandatory if this method is skipped).
     *
     * @param hashProvider hash provider used for generating hashes for the given key using the given hasher
     * @return a builder pattern instance for chaining additional methods
     */
    public DistributedCaffeine<K, V> withHashProvider(HashProvider<K> hashProvider) {
        requireNonNull(hashProvider, "hashProvider cannot be null");
        this.hasher = new InternalHasher<>(hashProvider);
        return this;
    }

    /**
     * Specifies the mode used for distributed synchronization between cache instances.
     * <p>
     * <b>Note:</b> {@link DistributionMode#POPULATION_AND_INVALIDATION_AND_EVICTION} is used as default if this
     * method is skipped.
     *
     * @param distributionMode distribution mode used for distributed synchronization
     * @return a builder pattern instance for chaining additional methods
     */
    public DistributedCaffeine<K, V> withDistributionMode(DistributionMode distributionMode) {
        requireNonNull(distributionMode, "distributionMode cannot be null");
        this.distributionMode = distributionMode;
        return this;
    }

    /**
     * Specifies the serializers used for serializing key and value objects via the given configurer.
     * <p>
     * Exemplary usage:
     * <pre>
     * ...
     * .withSerializers(configurer -> configurer
     *     .withKeySerializer(new KeySerializer())
     *     .withValueSerializer(new ValueSerializer()))
     * ...
     * </pre>
     * <b>Note:</b> {@link ForySerializer} is used as default for serializing key and value objects if this method
     * is skipped.
     *
     * @param configurer configurer for serializers
     * @return a builder pattern instance for chaining additional methods
     */
    public DistributedCaffeine<K, V> withSerializers(Configurer<SerializersConfigurer<K, V>> configurer) {
        requireNonNull(configurer, "configurer cannot be null");
        this.serializersConfigurer = configurer.apply(this.serializersConfigurer);
        return this;
    }

    /**
     * Specifies extended persistence via the given configurer.
     * <p>
     * Exemplary usage:
     * <pre>
     * ...
     * .withExtendedPersistence(configurer -> configurer
     *     .withMaximumSize(1_000_000)
     *     .withMaximumTime(Duration.ofDays(10)))
     * ...
     * </pre>
     * <b>Note:</b> No extended persistence is used if this method is skipped.
     *
     * @param configurer configurer for extended persistence
     * @return a builder pattern instance for chaining additional methods
     */
    public DistributedCaffeine<K, V> withExtendedPersistence(Configurer<ExtendedPersistenceConfigurer> configurer) {
        requireNonNull(configurer, "configurer cannot be null");
        this.extendedPersistenceConfigurer = configurer.apply(this.extendedPersistenceConfigurer);
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
        Supplier<InternalInstanceRegistry<K, V>> instanceRegistrySupplier = () ->
                buildCommon(Caffeine::build, null);
        InternalInstanceRegistry<K, V> instanceRegistry = instanceRegistrySupplier.get();
        instanceRegistry.setInstanceRegistrySupplier(instanceRegistrySupplier);
        InternalDistributedCache<K, V> distributedCache = new InternalDistributedCache<>();
        instanceRegistry.initializeLazy(distributedCache);
        instanceRegistry.activate();
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
        Supplier<InternalInstanceRegistry<K, V>> instanceRegistrySupplier = () -> {
            InternalCacheLoader<K, V> internalCacheLoader = new InternalCacheLoader<>((CacheLoader<K, V>) cacheLoader);
            return buildCommon(c -> c.build(internalCacheLoader), internalCacheLoader);
        };
        InternalInstanceRegistry<K, V> instanceRegistry = instanceRegistrySupplier.get();
        instanceRegistry.setInstanceRegistrySupplier(instanceRegistrySupplier);
        InternalDistributedLoadingCache<K, V> distributedLoadingCache = new InternalDistributedLoadingCache<>();
        instanceRegistry.initializeLazy(distributedLoadingCache);
        instanceRegistry.activate();
        return (DistributedLoadingCache<K1, V1>) distributedLoadingCache;
    }

    @SuppressWarnings({"unchecked", "java:S3011"})
    private InternalInstanceRegistry<K, V> buildCommon(
            Function<Caffeine<Object, Object>, Cache<InternalKey<K>, InternalValue<V>>> build,
            @Nullable InternalCacheLoader<K, V> cacheLoader) {
        InternalInstanceRegistry<K, V> instanceRegistry = new InternalInstanceRegistry<>();
        instanceRegistry.setAdapter(this.adapter);
        instanceRegistry.setHasher(this.hasher);
        instanceRegistry.setDistributionMode(this.distributionMode);
        instanceRegistry.setSerializersConfigurer(this.serializersConfigurer);
        instanceRegistry.setExtendedPersistenceConfigurer(this.extendedPersistenceConfigurer);
        instanceRegistry.setCacheLoader(instanceRegistry.initializeLazy(cacheLoader));

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
        instanceRegistry.setRemovalListener(
                instanceRegistry.initializeLazy(new InternalRemovalListener<>(nonNull(caffeineRemovalListener)
                        ? caffeineRemovalListener
                        : noopListener)));
        instanceRegistry.setEvictionListener(
                instanceRegistry.initializeLazy(new InternalEvictionListener<>(nonNull(caffeineEvictionListener)
                        ? caffeineEvictionListener
                        : noopListener)));
        runFailable(() -> removalListenerField.set(caffeine, instanceRegistry.getRemovalListener()));
        runFailable(() -> evictionListenerField.set(caffeine, instanceRegistry.getEvictionListener()));

        // inject expiry if set
        Field expiryField = getFailable(() ->
                caffeine.getClass().getDeclaredField("expiry"));
        expiryField.setAccessible(true);
        Expiry<K, V> caffeineExpiry = getFailable(() ->
                (Expiry<K, V>) expiryField.get(caffeine));
        if (nonNull(caffeineExpiry)) {
            runFailable(() -> expiryField.set(caffeine, new InternalExpiry<>(caffeineExpiry)));
        }

        // inject weigher if set
        Field weigherField = getFailable(() ->
                caffeine.getClass().getDeclaredField("weigher"));
        weigherField.setAccessible(true);
        Weigher<K, V> caffeineWeigher = getFailable(() ->
                (Weigher<K, V>) weigherField.get(caffeine));
        if (nonNull(caffeineWeigher)) {
            runFailable(() -> weigherField.set(caffeine, new InternalWeigher<>(caffeineWeigher)));
        }

        // inject scheduler if not set or disabled (necessary for eviction listener reliability)
        Field schedulerField = getFailable(() ->
                caffeine.getClass().getDeclaredField("scheduler"));
        schedulerField.setAccessible(true);
        Scheduler caffeineScheduler = getFailable(() ->
                (Scheduler) schedulerField.get(caffeine));
        Scheduler scheduler = (isNull(caffeineScheduler) || caffeineScheduler == Scheduler.disabledScheduler())
                ? Scheduler.systemScheduler()
                : caffeineScheduler;
        runFailable(() -> schedulerField.set(caffeine, new InternalScheduler(scheduler)));

        // extract executor
        Field executorField = getFailable(() ->
                caffeine.getClass().getDeclaredField("executor"));
        executorField.setAccessible(true);
        instanceRegistry.setExecutor(Optional.ofNullable(getFailable(() ->
                        (Executor) executorField.get(caffeine)))
                .orElseGet(ForkJoinPool::commonPool));

        // extract statsCounter (lazy) and replace if necessary
        Field statsCounterSupplierField = getFailable(() ->
                caffeine.getClass().getDeclaredField("statsCounterSupplier"));
        statsCounterSupplierField.setAccessible(true);
        Supplier<StatsCounter> caffeineStatsCounterSupplier = getFailable(() ->
                (Supplier<StatsCounter>) statsCounterSupplierField.get(caffeine));
        if (nonNull(caffeineStatsCounterSupplier)) {
            Supplier<StatsCounter> statsCounterSupplier = () -> {
                StatsCounter caffeineStatsCounter = caffeineStatsCounterSupplier.get();
                instanceRegistry.setStatsCounter(caffeineStatsCounter);
                // reset caffeine (lazy) after supplier was invoked
                runFailable(() -> statsCounterSupplierField.set(caffeine, caffeineStatsCounterSupplier));
                return caffeineStatsCounter;
            };
            runFailable(() -> statsCounterSupplierField.set(caffeine, statsCounterSupplier));
        } else {
            instanceRegistry.setStatsCounter(StatsCounter.disabledStatsCounter());
        }

        // build final Caffeine cache instance (switched to internal key and value representation)
        instanceRegistry.setCache(build.apply(caffeine));

        // validate configurers
        this.serializersConfigurer.validate(instanceRegistry.getCache());
        this.extendedPersistenceConfigurer.validate(instanceRegistry.getCache());

        // reset caffeine
        runFailable(() -> removalListenerField.set(caffeine, caffeineRemovalListener));
        runFailable(() -> evictionListenerField.set(caffeine, caffeineEvictionListener));
        // stats counter is reset lazy and scheduler cannot be reset

        return instanceRegistry;
    }

    /**
     * Configurer to specify the serializers used for serializing key and value objects.
     *
     * @param <K> the key type of the cache
     * @param <V> the value type of the cache
     * @author Andreas Oberhoff
     */
    @NullMarked
    public static final class SerializersConfigurer<K, V> {

        private Serializer<K, ?> keySerializer;
        private Serializer<V, ?> valueSerializer;

        private SerializersConfigurer() {
            this.keySerializer = new ForySerializer<>();
            this.valueSerializer = new ForySerializer<>();
        }

        /**
         * Specifies a serializer to be used for serializing key objects.
         * <p>
         * Already built-in serializers:
         * <ul>
         *      <li>{@link ForySerializer}</li>
         *      <li>{@link JacksonSerializer}</li>
         *      <li>{@link JavaObjectSerializer}</li>
         * </ul>
         * <p>
         * If custom serializers are required, they must either extend one of the aforementioned serializers or
         * implement one of the following interfaces:
         * <ul>
         *      <li>{@link ByteArraySerializer} for serializing an object to a byte array representation</li>
         *      <li>{@link StringSerializer} for serializing an object to a string representation</li>
         *      <li>{@link JsonSerializer} for serializing an object to a JSON representation (encoded as String or
         *      BSON)</li>
         * </ul>
         * <p>
         * <b>Note:</b> {@link ForySerializer} is used as default if this method is skipped.
         *
         * @param keySerializer the custom serializer for key objects
         * @return a configurer instance for chaining additional methods
         */
        public SerializersConfigurer<K, V> withKeySerializer(Serializer<K, ?> keySerializer) {
            requireNonNull(keySerializer, "keySerializer cannot be null");
            this.keySerializer = keySerializer;
            return this;
        }

        /**
         * Specifies a serializer to be used for serializing value objects.
         * <p>
         * Already built-in serializers:
         * <ul>
         *      <li>{@link ForySerializer}</li>
         *      <li>{@link JacksonSerializer}</li>
         *      <li>{@link JavaObjectSerializer}</li>
         * </ul>
         * <p>
         * If custom serializers are required, they must either extend one of the aforementioned serializers or
         * implement one of the following interfaces:
         * <ul>
         *      <li>{@link ByteArraySerializer} for serializing an object to a byte array representation</li>
         *      <li>{@link StringSerializer} for serializing an object to a string representation</li>
         *      <li>{@link JsonSerializer} for serializing an object to a JSON representation (encoded as String or
         *      BSON)</li>
         * </ul>
         * <p>
         * <b>Note:</b> {@link ForySerializer} is used as default if this method is skipped.
         *
         * @param valueSerializer the custom serializer for value objects
         * @return a configurer instance for chaining additional methods
         */
        public SerializersConfigurer<K, V> withValueSerializer(Serializer<V, ?> valueSerializer) {
            requireNonNull(valueSerializer, "valueSerializer cannot be null");
            this.valueSerializer = valueSerializer;
            return this;
        }

        @SuppressWarnings("java:S1452")
        Serializer<K, ?> getKeySerializer() {
            return keySerializer;
        }

        @SuppressWarnings("java:S1452")
        Serializer<V, ?> getValueSerializer() {
            return valueSerializer;
        }

        @SuppressWarnings({"unused", "java:S1172"})
        void validate(Cache<?, ?> cache) {
            List<Class<?>> serializers = List.of(
                    ByteArraySerializer.class, StringSerializer.class, JsonSerializer.class);
            Stream.of(getKeySerializer(), getValueSerializer()).forEach(serializer -> {
                if (serializers.stream()
                        .noneMatch(serializerClass -> serializerClass.isInstance(serializer))) {
                    throw new IllegalArgumentException(format(
                            "Serializers must implement one of the following interfaces: %s",
                            serializers.stream()
                                    .map(Class::getSimpleName)
                                    .collect(joining(", "))));
                }
            });
        }
    }

    /**
     * Configurer to specify extended persistence.
     *
     * @author Andreas Oberhoff
     */
    @NullMarked
    public static final class ExtendedPersistenceConfigurer {

        private @Nullable Integer maximumSize;
        private @Nullable Duration maximumTime;
        private boolean cacheLoaderStrategy;

        private ExtendedPersistenceConfigurer() {
            this.cacheLoaderStrategy = false;
        }

        /**
         * Specifies the maximum size for the extended persistence up to which recently evicted cache entries will
         * remain in the underlying store and may be reloaded on demand.
         * <p>
         * Cache entries with extended persistence can be reloaded using loading strategies configured by
         * {@link #withLoadingStrategy(boolean)}.
         * <p>
         * Alternatively, {@link DistributedPolicy#getFromStore(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromStore(Iterable, boolean)} can be used to load those cache entries directly
         * from the underlying store bypassing this cache instance.
         * <p>
         * <b>Note:</b> If extended persistence is configured, at least one eviction policy must be configured.
         *
         * @param maximumSize the maximum size for the extended persistence (must be positive)
         * @return a configurer instance for chaining additional methods
         */
        public ExtendedPersistenceConfigurer withMaximumSize(int maximumSize) {
            if (maximumSize <= 0) {
                throw new IllegalArgumentException("maximumSize must be positive");
            }
            this.maximumSize = maximumSize;
            return this;
        }

        /**
         * Specifies the maximum amount of time for the extended persistence that recently evicted cache entries will
         * remain in the underlying store and may be reloaded on demand.
         * <p>
         * Cache entries with extended persistence can be reloaded using loading strategies configured by
         * {@link #withLoadingStrategy(boolean)}.
         * <p>
         * Alternatively, {@link DistributedPolicy#getFromStore(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromStore(Iterable, boolean)} can be used to load those cache entries directly
         * from the underlying store bypassing this cache instance.
         * <p>
         * <b>Note:</b> If extended persistence is configured, at least one eviction policy must be configured.
         *
         * @param maximumTime the maximum amount of time for the extended persistence (must be positive)
         * @return a configurer instance for chaining additional methods
         */
        public ExtendedPersistenceConfigurer withMaximumTime(Duration maximumTime) {
            requireNonNull(maximumTime, "maximumTime cannot be null");
            if (maximumTime.isZero() || maximumTime.isNegative()) {
                throw new IllegalArgumentException("maximumTime must be positive");
            }
            this.maximumTime = maximumTime;
            return this;
        }

        /**
         * Specifies loading strategies used to reload evicted cache entries with extended persistence on demand.
         * <p>
         * Loading strategy for cache loader can be enabled using {@code cacheLoaderStrategy}. This means that a
         * provided {@link CacheLoader} is only invoked to obtain missing cache entries if these could not be reloaded
         * from the underlying store beforehand.
         * <p>
         * By default, no loading strategies are enabled.
         * <p>
         * Alternatively, {@link DistributedPolicy#getFromStore(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromStore(Iterable, boolean)} can be used to load those cache entries directly
         * from the underlying store bypassing this cache instance.
         * <p>
         * <b>Note:</b> If extended persistence is configured, at least one eviction policy must be configured.
         *
         * @param cacheLoaderStrategy {@code true} to enable loading strategy for cache loader, otherwise {@code false}
         * @return a configurer instance for chaining additional methods
         */
        public ExtendedPersistenceConfigurer withLoadingStrategy(boolean cacheLoaderStrategy) {
            this.cacheLoaderStrategy = cacheLoaderStrategy;
            return this;
        }

        // TODO withInvalidationStrategy

        Optional<Integer> getMaximumSize() {
            return Optional.ofNullable(maximumSize);
        }

        Optional<Duration> getMaximumTime() {
            return Optional.ofNullable(maximumTime);
        }

        boolean isConfigured() {
            return Stream.of(getMaximumSize(), getMaximumTime())
                    .anyMatch(Optional::isPresent);
        }

        boolean hasCacheLoaderStrategy() {
            return isConfigured() && cacheLoaderStrategy;
        }

        void validate(Cache<?, ?> cache) {
            if (isConfigured()) {
                Policy<?, ?> policy = cache.policy();
                if (Stream.of(policy.eviction(), policy.expireAfterAccess(), policy.expireAfterWrite(),
                                policy.expireVariably())
                        .allMatch(Optional::isEmpty)) {
                    throw new IllegalStateException(
                            "If extended persistence is configured, at least one eviction strategy must be set");
                }
                if (cacheLoaderStrategy && !(cache instanceof LoadingCache)) {
                    throw new IllegalStateException(
                            "If extended persistence is configured and loading strategy for cache loader is enabled, "
                                    .concat("cache must be build as loading cache"));
                }
            }
        }
    }

    /**
     * Functional interface to apply a configurer to another configurer of the same type.
     *
     * @param <T> the type of the configurer
     * @author Andreas Oberhoff
     */
    @NullMarked
    @FunctionalInterface
    public interface Configurer<T> extends UnaryOperator<T> {
    }
}
