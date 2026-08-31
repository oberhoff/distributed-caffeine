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
import org.apache.fory.config.ForyBuilder;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer.LoadingStrategy.CACHE_LOADER;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailableOrNull;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.runFailable;
import static java.lang.String.format;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedSet;
import static java.util.Locale.ROOT;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableSet;

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
@SuppressWarnings("java:S1192")
public final class DistributedCaffeine<K, V> {

    // Caffeine exposes no public API to inspect or replace these before build(), so they are accessed reflectively.
    // The handles are resolved once here (the builder class is stable within a JVM) instead of on every build, and
    // resolution fails fast with a clear message if a Caffeine upgrade renames or removes a member - turning what
    // would be a cryptic NoSuchFieldException deep inside build() into an explicit "incompatible version" error.
    private static final Method IS_STRONG_KEYS_METHOD = caffeineMethod("isStrongKeys");
    private static final Method IS_STRONG_VALUES_METHOD = caffeineMethod("isStrongValues");
    private static final Field REMOVAL_LISTENER_FIELD = caffeineField("removalListener");
    private static final Field EVICTION_LISTENER_FIELD = caffeineField("evictionListener");
    private static final Field EXPIRY_FIELD = caffeineField("expiry");
    private static final Field WEIGHER_FIELD = caffeineField("weigher");
    private static final Field SCHEDULER_FIELD = caffeineField("scheduler");
    private static final Field EXECUTOR_FIELD = caffeineField("executor");
    private static final Field STATS_COUNTER_SUPPLIER_FIELD = caffeineField("statsCounterSupplier");

    // an adapter owns the single change stream feeding one cache instance and is wired to that instance while
    // building, so it belongs to exactly one of them. Handing the same one to a second build rewires it, which
    // redirects the change stream of the first cache into the second (leaving the first silently blind) and hangs
    // the second build outright, because activating an adapter that is already watching joins a watcher that only
    // completes once it stops.
    // Claimed here rather than in AbstractAdapter so that adapters implementing the interface directly are covered
    // too, and held for as long as the cache instance exists, because that instance can be restarted at any time
    // (a claim is only given up again when constructing fails, see buildClaimed()). The weak keys let an entry go
    // once the application itself drops the adapter, and no value is held for it, so nothing references the key
    // back and keeps it alive. Note that this identifies adapters by equals(), which no adapter overrides, so it
    // is identity in practice
    private static final Set<Adapter<?, ?>> CLAIMED_ADAPTERS = synchronizedSet(newSetFromMap(new WeakHashMap<>()));

    private final Adapter<K, V> adapter;
    private Caffeine<Object, Object> caffeine;
    private InternalHasher<K> hasher;
    private DistributionMode distributionMode;
    private SerializersConfigurer<K, V> serializersConfigurer;
    private PersistenceConfigurer persistenceConfigurer;

    private DistributedCaffeine(Adapter<K, V> adapter) {
        this.adapter = adapter;
        // set defaults
        this.caffeine = Caffeine.newBuilder();
        this.hasher = new InternalHasher<>(null);
        this.distributionMode = POPULATION_AND_INVALIDATION_AND_EVICTION;
        this.serializersConfigurer = new SerializersConfigurer<>();
        this.persistenceConfigurer = new PersistenceConfigurer();
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
     * <b>Attention:</b> An adapter belongs to exactly one cache instance and cannot be shared between them.
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
     * <b>Attention:</b> To ensure the integrity of distributed synchronization between cache instances, the following
     * minor restrictions apply:
     * <ul>
     *      <li>Reference-based eviction using Caffeine's weak or soft references for keys or values is not supported.
     *      Even for the use of Caffeine (stand-alone), it is advised to use the more predictable size- or time-based
     *      eviction instead.</li>
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
     * Specifies the mode used for distributed synchronization between cache instances.
     * <p>
     * <b>Note:</b> {@link DistributionMode#POPULATION_AND_INVALIDATION_AND_EVICTION} is used as default if this method
     * is skipped.
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
     * Specifies the hash provider used for generating hashes for key objects. This method can be skipped for keys of
     * type {@link String}, {@link Long}, {@link Integer} or {@link UUID}, which are hashed out of the box, and for key
     * objects implementing the {@link Hashable} interface.
     * <p>
     * Exemplary usage:
     * <pre>
     * ...
     * .withHashProvider((key, hasher) -> hasher.get()
     *     .putUUID(key.getId())
     *     .putLong(key.getVersion())
     *     .putString(key.getName())
     *     .put...
     *     .getHash())
     * ...
     * </pre>
     * <b>Note:</b> A specified hash provider always takes precedence over the alternatives listed above.
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
     * <b>Note:</b> {@link ForySerializer} is used as default for serializing key and value objects if this method is
     * skipped.
     * <p>
     * <b>Attention:</b> Using the default {@link ForySerializer}, class registration is not enforced by default, which
     * means that arbitrary classes can be deserialized. Values should therefore only be deserialized from a trusted
     * data store. If strict enforcement is required, {@link ForySerializer#ForySerializer(ForyBuilder, Class[])} can be
     * used with a Fory builder enforcing this.
     *
     * @param configurer configurer for serializers
     * @return a builder pattern instance for chaining additional methods
     */
    public DistributedCaffeine<K, V> withSerializers(Configurer<SerializersConfigurer<K, V>> configurer) {
        requireNonNull(configurer, "configurer cannot be null");
        this.serializersConfigurer = requireNonNull(configurer.apply(this.serializersConfigurer),
                "configurer cannot return null");
        return this;
    }

    /**
     * Specifies persistence of cache entries in the underlying store via the given configurer.
     * <p>
     * Persistence is configured separately for the two kinds of cache entries the underlying store can retain:
     * <ul>
     *     <li>{@link PersistenceConfigurer#withCachedEntries(Configurer)} for cache entries that are currently
     *     cached, which can be synchronized into a cache instance when synchronization starts</li>
     *     <li>{@link PersistenceConfigurer#withEvictedEntries(Configurer)} for cache entries that have recently been
     *     evicted, which may be reloaded on demand</li>
     * </ul>
     * <p>
     * Exemplary usage:
     * <pre>
     * ...
     * .withPersistence(configurer -> configurer
     *     .withCachedEntries(cachedEntries -> cachedEntries
     *         .withCacheResidency())
     *     .withEvictedEntries(evictedEntries -> evictedEntries
     *         .withMaximumSize(1_000_000)
     *         .withMaximumTime(Duration.ofDays(10))))
     * ...
     * </pre>
     * <b>Note:</b> No persistence is used if this method is skipped.
     *
     * @param configurer configurer for persistence
     * @return a builder pattern instance for chaining additional methods
     */
    public DistributedCaffeine<K, V> withPersistence(Configurer<PersistenceConfigurer> configurer) {
        requireNonNull(configurer, "configurer cannot be null");
        this.persistenceConfigurer = requireNonNull(configurer.apply(this.persistenceConfigurer),
                "configurer cannot return null");
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
        return buildClaimed(() -> {
            InternalInstanceRegistry<K, V> instanceRegistry = buildCommon(Caffeine::build, null);
            InternalDistributedCache<K, V> distributedCache = new InternalDistributedCache<>();
            instanceRegistry.initialize(distributedCache);
            instanceRegistry.activate();
            return (DistributedCache<K1, V1>) distributedCache;
        });
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
        InternalCacheLoader<K, V> internalCacheLoader = new InternalCacheLoader<>((CacheLoader<K, V>) cacheLoader);
        return buildClaimed(() -> {
            @SuppressWarnings("NullableProblems")
            InternalInstanceRegistry<K, V> instanceRegistry =
                    buildCommon(c -> c.build(internalCacheLoader), internalCacheLoader);
            InternalDistributedLoadingCache<K, V> distributedLoadingCache = new InternalDistributedLoadingCache<>();
            instanceRegistry.initialize(distributedLoadingCache);
            instanceRegistry.activate();
            return (DistributedLoadingCache<K1, V1>) distributedLoadingCache;
        });
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
        instanceRegistry.setCachedEntryPersistenceConfigurer(
                this.persistenceConfigurer.getCachedEntryPersistenceConfigurer());
        instanceRegistry.setEvictedEntryPersistenceConfigurer(
                this.persistenceConfigurer.getEvictedEntryPersistenceConfigurer());
        instanceRegistry.setCacheLoader(cacheLoader);

        // throw exception if weak or soft references are configured
        boolean hasWeakOrSoftReferences =
                !(Boolean) getFailable(() -> IS_STRONG_KEYS_METHOD.invoke(caffeine))
                        || !(Boolean) getFailable(() -> IS_STRONG_VALUES_METHOD.invoke(caffeine));
        if (hasWeakOrSoftReferences) {
            throw new IllegalStateException("The use of weak or soft references is not supported");
        }

        // inject removal and eviction listener (reset later)
        RemovalListener<K, V> caffeineRemovalListener = getFailableOrNull(() ->
                (@Nullable RemovalListener<K, V>) REMOVAL_LISTENER_FIELD.get(caffeine));
        RemovalListener<K, V> caffeineEvictionListener = getFailableOrNull(() ->
                (@Nullable RemovalListener<K, V>) EVICTION_LISTENER_FIELD.get(caffeine));
        RemovalListener<K, V> noopListener = (key, value, removalCause) -> {
        };
        instanceRegistry.setRemovalListener(
                new InternalRemovalListener<>(nonNull(caffeineRemovalListener)
                        ? caffeineRemovalListener
                        : noopListener));
        instanceRegistry.setEvictionListener(
                new InternalEvictionListener<>(nonNull(caffeineEvictionListener)
                        ? caffeineEvictionListener
                        : noopListener));
        runFailable(() -> REMOVAL_LISTENER_FIELD.set(caffeine, instanceRegistry.getRemovalListener()));
        runFailable(() -> EVICTION_LISTENER_FIELD.set(caffeine, instanceRegistry.getEvictionListener()));

        // inject expiry if set (reset later)
        Expiry<K, V> caffeineExpiry = getFailableOrNull(() ->
                (@Nullable Expiry<K, V>) EXPIRY_FIELD.get(caffeine));
        if (nonNull(caffeineExpiry)) {
            runFailable(() -> EXPIRY_FIELD.set(caffeine, new InternalExpiry<>(caffeineExpiry)));
        }

        // inject weigher if set (reset later)
        Weigher<K, V> caffeineWeigher = getFailableOrNull(() ->
                (@Nullable Weigher<K, V>) WEIGHER_FIELD.get(caffeine));
        if (nonNull(caffeineWeigher)) {
            runFailable(() -> WEIGHER_FIELD.set(caffeine, new InternalWeigher<>(caffeineWeigher)));
        }

        // inject scheduler if not set or disabled (necessary for eviction listener reliability)
        Scheduler caffeineScheduler = getFailableOrNull(() ->
                (@Nullable Scheduler) SCHEDULER_FIELD.get(caffeine));
        if (!(caffeineScheduler instanceof InternalScheduler)) {
            Scheduler scheduler = (isNull(caffeineScheduler) || Scheduler.disabledScheduler().equals(caffeineScheduler))
                    ? Scheduler.systemScheduler()
                    : caffeineScheduler;
            runFailable(() -> SCHEDULER_FIELD.set(caffeine, new InternalScheduler(scheduler)));
        }

        // extract executor
        instanceRegistry.setExecutor(Optional.ofNullable(getFailableOrNull(() ->
                        (Executor) EXECUTOR_FIELD.get(caffeine)))
                .orElseGet(ForkJoinPool::commonPool));

        // extract statsCounter (lazy) and replace if necessary
        Supplier<StatsCounter> caffeineStatsCounterSupplier = getFailableOrNull(() ->
                (Supplier<StatsCounter>) STATS_COUNTER_SUPPLIER_FIELD.get(caffeine));
        if (nonNull(caffeineStatsCounterSupplier)) {
            Supplier<StatsCounter> statsCounterSupplier = () -> {
                StatsCounter caffeineStatsCounter = caffeineStatsCounterSupplier.get();
                instanceRegistry.setStatsCounter(caffeineStatsCounter);
                // reset caffeine (lazy) after supplier was invoked
                runFailable(() -> STATS_COUNTER_SUPPLIER_FIELD.set(caffeine, caffeineStatsCounterSupplier));
                return caffeineStatsCounter;
            };
            runFailable(() -> STATS_COUNTER_SUPPLIER_FIELD.set(caffeine, statsCounterSupplier));
        } else {
            instanceRegistry.setStatsCounter(StatsCounter.disabledStatsCounter());
        }

        // build final Caffeine cache instance (switched to internal key and value representation)
        instanceRegistry.setCache(build.apply(caffeine));

        // every field the components read is assigned by now, so they are initialized once here
        instanceRegistry.initializeComponents();

        // validate configurers
        this.serializersConfigurer.validate(instanceRegistry.getCache());
        this.persistenceConfigurer.validate(instanceRegistry.getCache(), this.distributionMode);

        // reset caffeine
        runFailable(() -> REMOVAL_LISTENER_FIELD.set(caffeine, caffeineRemovalListener));
        runFailable(() -> EVICTION_LISTENER_FIELD.set(caffeine, caffeineEvictionListener));
        runFailable(() -> EXPIRY_FIELD.set(caffeine, caffeineExpiry));
        runFailable(() -> WEIGHER_FIELD.set(caffeine, caffeineWeigher));
        // stats counter is reset lazy and scheduler cannot be reset

        return instanceRegistry;
    }

    // claims the adapter for the cache instance about to be constructed, see CLAIMED_ADAPTERS. The claim is released
    // again if that instance does not come up, so that the very same adapter can be handed to another attempt. That
    // matters most when activation is what failed, for example because the store is not reachable yet: nothing is
    // watching then, and the next attempt overwrites the wiring of the abandoned one anyway
    private <T> T buildClaimed(Supplier<T> build) {
        if (!CLAIMED_ADAPTERS.add(this.adapter)) {
            throw new IllegalStateException(format("The adapter for cache at '%s' is already in use by another cache "
                    + "instance, every cache instance requires its own adapter", this.adapter.getIdentifier()));
        }
        try {
            return build.get();
        } catch (Exception e) {
            CLAIMED_ADAPTERS.remove(this.adapter);
            throw e;
        }
    }

    @SuppressWarnings("java:S3011")
    private static Field caffeineField(String name) {
        try {
            Field field = Caffeine.class.getDeclaredField(name);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw incompatibleCaffeine(Field.class.getSimpleName().toLowerCase(ROOT), name, e);
        }
    }

    @SuppressWarnings("java:S3011")
    private static Method caffeineMethod(String name) {
        try {
            Method method = Caffeine.class.getDeclaredMethod(name);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException e) {
            throw incompatibleCaffeine(Method.class.getSimpleName().toLowerCase(ROOT), name, e);
        }
    }

    private static IllegalStateException incompatibleCaffeine(String memberKind, String name, Throwable cause) {
        return new IllegalStateException("Incompatible Caffeine version: expected %s '%s' on '%s' was not found. "
                .concat("Distributed Caffeine accesses Caffeine internals does not support this Caffeine version.")
                .formatted(memberKind, name, Caffeine.class.getName()), cause);
    }

    /**
     * Configurer to specify the serializers used for serializing key and value objects.
     *
     * @param <K> the key type of the cache
     * @param <V> the value type of the cache
     * @author Andreas Oberhoff
     */
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
         * <p>
         * <b>Attention:</b> Using the default {@link ForySerializer}, class registration is not enforced by default,
         * which means that arbitrary classes can be deserialized. Values should therefore only be deserialized from a
         * trusted data store. If strict enforcement is required,
         * {@link ForySerializer#ForySerializer(ForyBuilder, Class[])} can be used with a Fory builder enforcing this.
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
         * <p>
         * <b>Attention:</b> Using the default {@link ForySerializer}, class registration is not enforced by default,
         * which means that arbitrary classes can be deserialized. Values should therefore only be deserialized from a
         * trusted data store. If strict enforcement is required,
         * {@link ForySerializer#ForySerializer(ForyBuilder, Class[])} can be used with a Fory builder enforcing this.
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
            Stream.of(getKeySerializer(), getValueSerializer())
                    .forEach(serializer -> {
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
     * Configurer to specify persistence of cache entries to retain them (conditionally) in the underlying store
     * (separately for cached and evicted entries).
     *
     * @author Andreas Oberhoff
     */
    public static final class PersistenceConfigurer {

        private CachedEntryPersistenceConfigurer cachedEntryPersistenceConfigurer;
        private EvictedEntryPersistenceConfigurer evictedEntryPersistenceConfigurer;

        private PersistenceConfigurer() {
            this.cachedEntryPersistenceConfigurer = new CachedEntryPersistenceConfigurer();
            this.evictedEntryPersistenceConfigurer = new EvictedEntryPersistenceConfigurer();
        }

        /**
         * Specifies persistence of cached entries via the given configurer.
         *
         * @param configurer configurer for persistence of cached entries
         * @return a configurer instance for chaining additional methods
         */
        public PersistenceConfigurer withCachedEntries(Configurer<CachedEntryPersistenceConfigurer> configurer) {
            requireNonNull(configurer, "configurer cannot be null");
            this.cachedEntryPersistenceConfigurer = requireNonNull(
                    configurer.apply(this.cachedEntryPersistenceConfigurer),
                    "configurer cannot return null");
            return this;
        }

        /**
         * Specifies persistence of evicted entries via the given configurer.
         *
         * @param configurer configurer for persistence of evicted entries
         * @return a configurer instance for chaining additional methods
         */
        public PersistenceConfigurer withEvictedEntries(Configurer<EvictedEntryPersistenceConfigurer> configurer) {
            requireNonNull(configurer, "configurer cannot be null");
            this.evictedEntryPersistenceConfigurer = requireNonNull(
                    configurer.apply(this.evictedEntryPersistenceConfigurer),
                    "configurer cannot return null");
            return this;
        }

        CachedEntryPersistenceConfigurer getCachedEntryPersistenceConfigurer() {
            return cachedEntryPersistenceConfigurer;
        }

        EvictedEntryPersistenceConfigurer getEvictedEntryPersistenceConfigurer() {
            return evictedEntryPersistenceConfigurer;
        }

        void validate(Cache<?, ?> cache, DistributionMode distributionMode) {
            cachedEntryPersistenceConfigurer.validate(cache, distributionMode);
            evictedEntryPersistenceConfigurer.validate(cache);
        }

        private static boolean hasEvictionPolicy(Cache<?, ?> cache) {
            Policy<?, ?> policy = cache.policy();
            return Stream.of(policy.eviction(), policy.expireAfterAccess(), policy.expireAfterWrite(),
                            policy.expireVariably())
                    .anyMatch(Optional::isPresent);
        }
    }

    /**
     * Configurer to specify persistence of cached entries to retain them (conditionally) in the underlying store.
     *
     * @author Andreas Oberhoff
     */
    public static final class CachedEntryPersistenceConfigurer {

        private @Nullable Integer maximumSize;
        private @Nullable Duration maximumTime;
        private boolean cacheResidency;
        private boolean coldStart;

        private CachedEntryPersistenceConfigurer() {
            // noop
        }

        /**
         * Specifies the persistence of cached entries so that all of them are retained in the underlying store (unless
         * invalidated or evicted).
         * <p>
         * All retained cache entries are synchronized back into this cache as warm-up.
         * <p>
         * Alternatively, {@link DistributedPolicy#getFromStore(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromStore(Iterable, boolean)} can be used to load those cache entries directly
         * from the underlying store bypassing this cache instance.
         * <p>
         * <b>Note:</b> This is mutually exclusive with {@link #withMaximumSize(int)} and
         * {@link #withMaximumTime(Duration)}, which bound retention independently of whether cache entries are still
         * cached.
         * <p>
         * <b>Note:</b> If persistence of cached entries is configured, the configured {@link DistributionMode} must
         * include population, and eviction as well if an eviction policy is configured.
         *
         * @return a configurer instance for chaining additional methods
         */
        public CachedEntryPersistenceConfigurer withCacheResidency() {
            this.cacheResidency = true;
            return this;
        }

        /**
         * Specifies the maximum size for the persistence of cached entries up to which the most recently cached ones
         * are retained in the underlying store (if not invalidated or evicted).
         * <p>
         * Retained cache entries are synchronized back into this cache as warm-up (unless a cold start is configured
         * explicitly using {@link #withColdStart()}).
         * <p>
         * Alternatively, {@link DistributedPolicy#getFromStore(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromStore(Iterable, boolean)} can be used to load those cache entries directly
         * from the underlying store bypassing this cache instance.
         * <p>
         * <b>Note:</b> This is mutually exclusive with {@link #withCacheResidency()} and can be combined with
         * {@link #withMaximumTime(Duration)}.
         * <p>
         * <b>Note:</b> If persistence of cached entries is configured, the configured {@link DistributionMode} must
         * include population.
         *
         * @param maximumSize the maximum size for the persistence of cached entries (must be positive)
         * @return a configurer instance for chaining additional methods
         */
        public CachedEntryPersistenceConfigurer withMaximumSize(int maximumSize) {
            if (maximumSize <= 0) {
                throw new IllegalArgumentException("maximumSize must be positive");
            }
            this.maximumSize = maximumSize;
            return this;
        }

        /**
         * Specifies the maximum amount of time for the persistence of cached entries within they are retained in the
         * underlying store (if not invalidated or evicted).
         * <p>
         * Retained cache entries are synchronized back into this cache as warm-up (unless a cold start is configured
         * explicitly using {@link #withColdStart()}).
         * <p>
         * Alternatively, {@link DistributedPolicy#getFromStore(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromStore(Iterable, boolean)} can be used to load those cache entries directly
         * from the underlying store bypassing this cache instance.
         * <p>
         * <b>Note:</b> This is mutually exclusive with {@link #withCacheResidency()} and can be combined with
         * {@link #withMaximumSize(int)}.
         * <p>
         * <b>Note:</b> If persistence of cached entries is configured, the configured {@link DistributionMode} must
         * include population.
         *
         * @param maximumTime the maximum amount of time for the persistence of cached entries (must be positive)
         * @return a configurer instance for chaining additional methods
         */
        public CachedEntryPersistenceConfigurer withMaximumTime(Duration maximumTime) {
            requireNonNull(maximumTime, "maximumTime cannot be null");
            if (maximumTime.isZero() || maximumTime.isNegative()) {
                throw new IllegalArgumentException("maximumTime must be positive");
            }
            this.maximumTime = maximumTime;
            return this;
        }

        /**
         * Specifies that a cache instance starts empty instead of synchronizing retained cache entries back into this
         * cache as warm-up (which otherwise is the default).
         * <p>
         * Alternatively, {@link DistributedPolicy#getFromStore(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromStore(Iterable, boolean)} can be used to load those cache entries directly
         * from the underlying store bypassing this cache instance.
         * <p>
         * <b>Note:</b> This is mutually exclusive with {@link #withCacheResidency()}, which cannot be honored
         * without reading those cache entries back, because nothing else takes ownership of them again.
         *
         * @return a configurer instance for chaining additional methods
         */
        public CachedEntryPersistenceConfigurer withColdStart() {
            this.coldStart = true;
            return this;
        }

        Optional<Integer> getMaximumSize() {
            return Optional.ofNullable(maximumSize);
        }

        Optional<Duration> getMaximumTime() {
            return Optional.ofNullable(maximumTime);
        }

        boolean isConfigured() {
            return cacheResidency || Stream.of(getMaximumSize(), getMaximumTime())
                    .anyMatch(Optional::isPresent);
        }

        // reading back is what retaining is for, so it follows from being configured rather than from a
        // setting of its own - and a cold start declined on a tier that retains nothing stays inert
        boolean hasInitialSynchronizationStrategy() {
            return isConfigured() && !coldStart;
        }

        void validate(Cache<?, ?> cache, DistributionMode distributionMode) {
            if (isConfigured()) {
                if (!distributionMode.isPopulationConsidered()) {
                    throw new IllegalStateException(
                            "If persistence of cached entries is configured, "
                                    .concat("the distribution mode must include population"));
                }
                if (cacheResidency && Stream.of(getMaximumSize(), getMaximumTime())
                        .anyMatch(Optional::isPresent)) {
                    throw new IllegalStateException(
                            "If persistence of cached entries is configured, cache residency must not be "
                                    .concat("combined with a maximum size or a maximum amount of time"));
                }
                // residency can only be honored if leaving a cache instance is recorded at all, which an eviction
                // is only if the distribution mode includes it - a cache that cannot evict never poses the question
                if (cacheResidency && PersistenceConfigurer.hasEvictionPolicy(cache)
                        && !distributionMode.isEvictionConsidered()) {
                    throw new IllegalStateException(
                            "If persistence of cached entries is configured with cache residency and an "
                                    .concat("eviction policy is set, the distribution mode must include evictions"));
                }
                // last of the residency checks, so that the more fundamental ones above are reported first.
                // Cache residency alone is the one retention that is neither bounded nor reclaimable: what a cache
                // instance leaves behind when it stops is not an eviction, so nothing transitions those cache
                // entries and no maximum size or amount of time ages them out either. Reading them back is what
                // takes ownership of them again, and because that reads every retained cache entry rather than the
                // ones this cache instance wrote, a single one starting up adopts what all of them left
                if (cacheResidency && coldStart) {
                    throw new IllegalStateException(
                            "If persistence of cached entries is configured with cache residency, "
                                    .concat("a cold start must not be specified"));
                }
            }
        }
    }

    /**
     * Configurer to specify persistence of evicted entries to retain them (conditionally) in the underlying store.
     *
     * @author Andreas Oberhoff
     */
    public static final class EvictedEntryPersistenceConfigurer {

        /**
         * Loading strategies used to reload retained evicted cache entries on demand.
         *
         * @author Andreas Oberhoff
         */
        public enum LoadingStrategy {

            /**
             * Loading strategy for a provided {@link CacheLoader} that is only invoked to obtain missing cache entries
             * if these could not be reloaded from the underlying store beforehand.
             */
            CACHE_LOADER
        }

        private @Nullable Integer maximumSize;
        private @Nullable Duration maximumTime;
        private Set<LoadingStrategy> loadingStrategies;

        private EvictedEntryPersistenceConfigurer() {
            this.loadingStrategies = Set.of();
        }

        /**
         * Specifies the maximum size for the persistence of evicted entries up to which the most recently evicted ones
         * are retained in the underlying store (unless invalidated) and may be reloaded on demand.
         * <p>
         * Retained evicted cache entries can be reloaded using loading strategies configured by
         * {@link #withLoadingStrategies(LoadingStrategy...)}.
         * <p>
         * Alternatively, {@link DistributedPolicy#getFromStore(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromStore(Iterable, boolean)} can be used to load those cache entries directly
         * from the underlying store bypassing this cache instance.
         * <p>
         * <b>Note:</b> If persistence of evicted entries is configured, at least one eviction policy must be
         * configured.
         *
         * @param maximumSize the maximum size for the persistence of evicted entries (must be positive)
         * @return a configurer instance for chaining additional methods
         */
        public EvictedEntryPersistenceConfigurer withMaximumSize(int maximumSize) {
            if (maximumSize <= 0) {
                throw new IllegalArgumentException("maximumSize must be positive");
            }
            this.maximumSize = maximumSize;
            return this;
        }

        /**
         * Specifies the maximum amount of time for the persistence of evicted entries within they are retained in the
         * underlying store (unless invalidated) and may be reloaded on demand.
         * <p>
         * Retained evicted cache entries can be reloaded using loading strategies configured by
         * {@link #withLoadingStrategies(LoadingStrategy...)}.
         * <p>
         * Alternatively, {@link DistributedPolicy#getFromStore(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromStore(Iterable, boolean)} can be used to load those cache entries directly
         * from the underlying store bypassing this cache instance.
         * <p>
         * <b>Note:</b> If persistence of evicted entries is configured, at least one eviction policy must be
         * configured.
         *
         * @param maximumTime the maximum amount of time for the persistence of evicted entries (must be positive)
         * @return a configurer instance for chaining additional methods
         */
        public EvictedEntryPersistenceConfigurer withMaximumTime(Duration maximumTime) {
            requireNonNull(maximumTime, "maximumTime cannot be null");
            if (maximumTime.isZero() || maximumTime.isNegative()) {
                throw new IllegalArgumentException("maximumTime must be positive");
            }
            this.maximumTime = maximumTime;
            return this;
        }

        /**
         * Specifies loading strategies used to reload retained evicted cache entries on demand.
         * <p>
         * By default, no loading strategies are enabled, which is also expressed by passing no strategy at all.
         * <p>
         * Alternatively, {@link DistributedPolicy#getFromStore(Object, boolean)} or
         * {@link DistributedPolicy#getAllFromStore(Iterable, boolean)} can be used to load those cache entries directly
         * from the underlying store bypassing this cache instance.
         * <p>
         * <b>Note:</b> If persistence of evicted entries is configured, at least one eviction policy must be
         * configured.
         *
         * @param loadingStrategies the loading strategies to enable (must not be null or contain null)
         * @return a configurer instance for chaining additional methods
         */
        public EvictedEntryPersistenceConfigurer withLoadingStrategies(LoadingStrategy... loadingStrategies) {
            requireNonNull(loadingStrategies, "loadingStrategies cannot be null");
            this.loadingStrategies = Stream.of(loadingStrategies)
                    .map(loadingStrategy -> requireNonNull(loadingStrategy,
                            "loadingStrategies cannot contain null"))
                    .collect(toUnmodifiableSet());
            return this;
        }

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

        // see the cached tier: a loading strategy without a retention is rejected below
        boolean hasCacheLoaderStrategy() {
            return loadingStrategies.contains(CACHE_LOADER);
        }

        void validate(Cache<?, ?> cache) {
            // see the corresponding check for cached entries: a loading strategy on its own retains nothing,
            // so there would be nothing to read back even though reading back is what it asks for
            if (!loadingStrategies.isEmpty() && !isConfigured()) {
                throw new IllegalStateException(
                        "If a loading strategy is enabled, persistence of evicted entries must be "
                                .concat("configured with a maximum size or a maximum amount of time"));
            }
            if (isConfigured()) {
                if (!PersistenceConfigurer.hasEvictionPolicy(cache)) {
                    throw new IllegalStateException(
                            "If persistence of evicted entries is configured, "
                                    .concat("at least one eviction strategy must be set"));
                }
                if (hasCacheLoaderStrategy() && !(cache instanceof LoadingCache)) {
                    throw new IllegalStateException(
                            "If persistence of evicted entries is configured and loading strategy "
                                    .concat("for cache loader is enabled, cache must be built as loading cache"));
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
    @FunctionalInterface
    public interface Configurer<T> extends UnaryOperator<T> {
    }
}
