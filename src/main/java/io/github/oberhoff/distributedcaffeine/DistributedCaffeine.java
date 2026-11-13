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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.CacheLoader;
import io.github.oberhoff.distributedcaffeine.hasher.HashProvider;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeScan;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;

/**
 * The builder of a distributed cache. Each instance keeps a local Caffeine cache; a miss reads the shared store before
 * the source (the loader or mapping function, if any), and a change reaches the copies of the other instances.
 * Configured with Caffeine's builder methods and the store's settings, built as any of Caffeine's cache variants. The
 * two levels split the work:
 * <ol>
 *     <li>The store holds the values: its maximum age decides how long a value is valid, on every instance; its
 *     maximum size, how many values it keeps.</li>
 *     <li>Each instance holds copies: Caffeine's settings decide which copies it keeps and for how long, never beyond
 *     their values' validity.</li>
 *     <li>When a copy goes, the instance asks the store, and only if the store has no valid value, the source.</li>
 * </ol>
 * <pre>
 * {@code DistributedLoadingCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
 *     .withCaffeine(caffeine -> caffeine
 *         .expireAfterWrite(Duration.ofMinutes(5))
 *         .maximumSize(10_000))
 *     .withStore(store -> store
 *         .withMaximumAge(Duration.ofHours(1))
 *         .withMaximumSize(1_000_000))
 *     .build(key -> loadExpensiveValue(key));
 * }</pre>
 *
 * @param <K> the key type, fixed by the adapter
 * @param <V> the value type, fixed by the adapter
 * @author Andreas Oberhoff
 */
public final class DistributedCaffeine<K, V> {

    private static final String LOADER_CANNOT_BE_NULL = "loader cannot be null";

    private final Adapter<K, V> adapter;
    private final CaffeineConfigurer<K, V> caffeine = new CaffeineConfigurer<>();
    private final StoreConfigurer store = new StoreConfigurer();
    private final SerializersConfigurer<K, V> serializers = new SerializersConfigurer<>();
    private @Nullable HashProvider<K> hashProvider;
    private @Nullable DistributionMode distributionMode;
    private @Nullable Duration negativeCachingExpiry;
    private int loadAttempts = InternalCore.LOAD_ATTEMPTS;
    private InternalEnvironment environment = InternalEnvironment.REAL;

    private DistributedCaffeine(Adapter<K, V> adapter) {
        this.adapter = adapter;
    }

    /**
     * Starts a builder for a cache stored where the adapter says.
     *
     * @param adapter the adapter, used for this one cache only
     * @param <K>     the key type
     * @param <V>     the value type
     * @return the builder
     */
    public static <K, V> DistributedCaffeine<K, V> newBuilder(Adapter<K, V> adapter) {
        return new DistributedCaffeine<>(requireNonNull(adapter, "adapter cannot be null"));
    }

    /**
     * Configures this instance's local level with Caffeine's builder methods, which keep their meaning for the local
     * copies (see {@link CaffeineConfigurer}): a copy that goes sends the instance back to the store, not to the
     * source. May be called more than once; settings accumulate, and a setting made twice is rejected as in Caffeine.
     *
     * @param configurer the configuration
     * @return this builder
     */
    public DistributedCaffeine<K, V> withCaffeine(Configurer<CaffeineConfigurer<K, V>> configurer) {
        configure(configurer, caffeine);
        return this;
    }

    /**
     * Configures the shared store: its bounds, all optional, and how the cache works with it.
     *
     * @param configurer the configuration
     * @return this builder
     */
    public DistributedCaffeine<K, V> withStore(Configurer<StoreConfigurer> configurer) {
        configure(configurer, store);
        return this;
    }

    /**
     * Sets how instances learn about changes made elsewhere; {@link DistributionMode#INVALIDATE} by default.
     *
     * @param distributionMode the distribution mode
     * @return this builder
     */
    public DistributedCaffeine<K, V> withDistributionMode(DistributionMode distributionMode) {
        requireNonNull(distributionMode, "distributionMode cannot be null");
        if (this.distributionMode != null) {
            throw new IllegalStateException("distributionMode was already set");
        }
        this.distributionMode = distributionMode;
        return this;
    }

    /**
     * Configures how keys and values are serialized for the store; Fory for both by default (see
     * {@link SerializersConfigurer}, also for hardening).
     *
     * @param configurer the configuration
     * @return this builder
     */
    public DistributedCaffeine<K, V> withSerializers(Configurer<SerializersConfigurer<K, V>> configurer) {
        configure(configurer, serializers);
        return this;
    }

    /**
     * Sets how keys are hashed, for key types other than {@code String}, {@code Long}, {@code Integer} and {@code UUID}
     * that do not implement {@code Hashable}. Keys that are equal must have the same hash.
     *
     * @param hashProvider the hash provider
     * @return this builder
     */
    public DistributedCaffeine<K, V> withHashProvider(HashProvider<K> hashProvider) {
        requireNonNull(hashProvider, "hashProvider cannot be null");
        if (this.hashProvider != null) {
            throw new IllegalStateException("hashProvider was already set");
        }
        this.hashProvider = hashProvider;
        return this;
    }

    /**
     * Remembers, for the given time, keys whose load found nothing, so that repeated requests for a missing key reach
     * neither the store nor the source. A value written through any instance ends the absence at once (without
     * distribution, only one written through this instance); a value that appears only in the source is found after
     * this time. An absence belongs to the key: it answers every later load of it, whichever loader or mapping
     * function that load would call. Up to 100,000 keys per instance. Off by default, as Caffeine caches no missing
     * value.
     *
     * @param expiry how long an absence is remembered (positive; keep it short)
     * @return this builder
     */
    public DistributedCaffeine<K, V> withNegativeCaching(Duration expiry) {
        requireNonNull(expiry, "expiry cannot be null");
        if (expiry.isNegative() || expiry.isZero()) {
            throw new IllegalArgumentException("the negative caching expiry must be positive");
        }
        if (negativeCachingExpiry != null) {
            throw new IllegalStateException("negative caching was already configured");
        }
        this.negativeCachingExpiry = expiry;
        return this;
    }

    /**
     * Builds a cache; connects and checks the store synchronously and fails fast.
     *
     * @param <K1> the key type of the cache: the adapter's, or a subtype the assignment names
     * @param <V1> the value type of the cache: the adapter's, or a subtype the assignment names
     * @return the cache
     */
    public <K1 extends K, V1 extends V> DistributedCache<K1, V1> build() {
        return this.<K1, V1>narrowed().buildCache();
    }

    /**
     * Builds a loading cache; connects and checks the store synchronously and fails fast.
     *
     * @param loader the loader of values missing from the store
     * @param <K1>   the key type of the cache: the adapter's, or a subtype the assignment names
     * @param <V1>   the value type of the cache: the adapter's, or a subtype the assignment names
     * @return the cache
     */
    public <K1 extends K, V1 extends V> DistributedLoadingCache<K1, V1> build(CacheLoader<? super K1, V1> loader) {
        return this.<K1, V1>narrowed().buildLoading(loader);
    }

    /**
     * Builds an asynchronous cache; connects and checks the store synchronously and fails fast.
     *
     * @param <K1> the key type of the cache: the adapter's, or a subtype the assignment names
     * @param <V1> the value type of the cache: the adapter's, or a subtype the assignment names
     * @return the cache
     */
    public <K1 extends K, V1 extends V> DistributedAsyncCache<K1, V1> buildAsync() {
        return this.<K1, V1>narrowed().buildAsyncCache();
    }

    /**
     * Builds an asynchronous loading cache; connects and checks the store synchronously and fails fast.
     *
     * @param loader the loader of values missing from the store
     * @param <K1>   the key type of the cache: the adapter's, or a subtype the assignment names
     * @param <V1>   the value type of the cache: the adapter's, or a subtype the assignment names
     * @return the cache
     */
    public <K1 extends K, V1 extends V> DistributedAsyncLoadingCache<K1, V1> buildAsync(
            CacheLoader<? super K1, V1> loader) {
        requireNonNull(loader, LOADER_CANNOT_BE_NULL);
        return this.<K1, V1>narrowed().buildAsyncFrom(loader);
    }

    /**
     * Builds an asynchronous loading cache; connects and checks the store synchronously and fails fast.
     *
     * @param loader the loader of values missing from the store
     * @param <K1>   the key type of the cache: the adapter's, or a subtype the assignment names
     * @param <V1>   the value type of the cache: the adapter's, or a subtype the assignment names
     * @return the cache
     */
    public <K1 extends K, V1 extends V> DistributedAsyncLoadingCache<K1, V1> buildAsync(
            AsyncCacheLoader<? super K1, V1> loader) {
        requireNonNull(loader, LOADER_CANNOT_BE_NULL);
        return this.<K1, V1>narrowed().buildAsyncFromAsync(loader);
    }

    // the builder typed with the types the caller's assignment names, as Caffeine's build methods do: the adapter's
    // types, or subtypes of them when the adapter was built without its own (Object). Unchecked but safe: the cache
    // holds only the keys and values its callers pass, and the settings made before accept the wider types
    @SuppressWarnings("unchecked")
    private <K1 extends K, V1 extends V> DistributedCaffeine<K1, V1> narrowed() {
        return (DistributedCaffeine<K1, V1>) this;
    }

    private DistributedCache<K, V> buildCache() {
        validate(false);
        return new InternalDistributedCache<>(core(null));
    }

    private DistributedLoadingCache<K, V> buildLoading(CacheLoader<? super K, V> loader) {
        requireNonNull(loader, LOADER_CANNOT_BE_NULL);
        validate(true);
        InternalCore.SingleLoader<K, V> single = loader::load;
        // the loader's key type is a supertype of K, so the keys it returns are only known to be of that type; as in
        // Caffeine, they are taken as K
        @SuppressWarnings("unchecked")
        InternalCore.BulkLoader<K, V> bulk = overridesLoadAll(loader)
                ? keys -> (Map<? extends K, ? extends V>) loader.loadAll(keys)
                : InternalCore.perKey(single);
        InternalCore<K, V> core = core(new InternalCore.Loaders<>(single, loader::reload, null, null, null));
        return new InternalDistributedLoadingCache<>(core, bulk);
    }

    private DistributedAsyncCache<K, V> buildAsyncCache() {
        validate(false);
        return new InternalDistributedAsyncCache<>(core(null), caffeine.executor());
    }

    private DistributedAsyncLoadingCache<K, V> buildAsyncFrom(CacheLoader<? super K, V> loader) {
        validate(true);
        return buildAsyncLoading(loader, overridesLoadAll(loader));
    }

    private DistributedAsyncLoadingCache<K, V> buildAsyncFromAsync(AsyncCacheLoader<? super K, V> loader) {
        validate(true);
        return buildAsyncLoading(loader, overridesAsyncLoadAll(loader));
    }

    // the user's loader runs on Caffeine's executor, as in Caffeine (a CacheLoader's async methods run its
    // synchronous ones there); the asynchronous paths wait for its futures without holding a thread, the
    // synchronous view (synchronous()) waits in the caller's thread
    @SuppressWarnings("unchecked")
    private DistributedAsyncLoadingCache<K, V> buildAsyncLoading(AsyncCacheLoader<? super K, V> loader, boolean bulk) {
        Executor executor = caffeine.executor();
        InternalCore.AsyncSingleLoader<K, V> asyncSingle = key -> loader.asyncLoad(key, executor);
        InternalCore.AsyncBulkLoader<K, V> asyncBulk = bulk
                ? keys -> (CompletableFuture<? extends Map<? extends K, ? extends V>>) loader
                .asyncLoadAll(keys, executor)
                : null;
        InternalCore.AsyncReloader<K, V> asyncReloader = (key, oldValue) -> loader.asyncReload(key, oldValue, executor);
        InternalCore.SingleLoader<K, V> single = key ->
                InternalCore.await(requireNonNull(asyncSingle.load(key), "asyncLoad returned null"));
        InternalCore.BulkLoader<K, V> bulkLoader = asyncBulk != null
                ? keys -> InternalCore.await(requireNonNull(asyncBulk.loadAll(keys), "asyncLoadAll returned null"))
                : InternalCore.perKey(single);
        InternalCore.Reloader<K, V> reloader = (key, oldValue) ->
                InternalCore.await(requireNonNull(asyncReloader.reload(key, oldValue), "asyncReload returned null"));
        InternalCore<K, V> core = core(new InternalCore.Loaders<>(single, reloader, asyncSingle, asyncBulk,
                asyncReloader));
        return new InternalDistributedAsyncLoadingCache<>(core, bulkLoader, executor);
    }

    private void validate(boolean loading) {
        if (caffeine.isRefreshing() && !loading) {
            throw new IllegalStateException("refreshAfterWrite requires a loading cache, as in Caffeine");
        }
        if (store().maximumAgeForStoreOnly()) {
            validateStoreOnly();
        } else {
            validateLocalExpiry("expireAfterWrite", caffeine.expireAfterWrite());
            validateLocalExpiry("expireAfterAccess", caffeine.expireAfterAccess());
        }
    }

    // a fixed local expiry longer than the store's maximum age would never apply, as every copy goes at the maximum age
    // first (a copy is read no earlier than installed, and installed no earlier than its value's write): the first
    // level meant to outlive the second, which decides how long a value is valid, almost certainly a mistake, so
    // rejected here rather than lowered quietly. At runtime, a maximum age or expiry set so still lowers it
    // (InternalExpiry): instances change one after another, and a staggered change must not fail. A user's Expiry has
    // no duration known before it runs; it is lowered per copy
    private void validateLocalExpiry(String name, @Nullable Duration duration) {
        Duration maximumAge = store().maximumAge();
        if (duration != null && duration.compareTo(maximumAge) > 0) {
            throw new IllegalStateException(name + " (" + duration + ") is longer than the store's maximum age ("
                    + maximumAge + "), which decides how long a value is valid: shorten it, or use "
                    + "withMaximumAgeForStoreOnly() for local copies meant to outlive the store's values");
        }
    }

    // the maximum age for the store only (StoreConfigurer#withMaximumAgeForStoreOnly)
    private void validateStoreOnly() {
        if (store().unboundedAge()) {
            throw new IllegalStateException("withMaximumAgeForStoreOnly() requires withMaximumAge(...)");
        }
        // without messages every held value counts as trusted, which the atomic methods rely on (InternalCore#read)
        if (distributionMode() == DistributionMode.NONE) {
            throw new IllegalStateException("withMaximumAgeForStoreOnly() requires distribution, not "
                    + "DistributionMode.NONE");
        }
        // a trusted holder knows of every write well before the store lets go of its row (InternalCore#read): a
        // heartbeat interval of at most a minute, three intervals within the tombstones' retention (only reached
        // by an interval set that long), and a maximum age of at least five intervals. Told from the maximum age's
        // side, the setting the user chose: with the default interval, at least 50 seconds
        Duration heartbeat = store().heartbeatInterval();
        if (heartbeat.compareTo(InternalMaintenance.STORE_ONLY_HEARTBEAT_LIMIT) > 0) {
            throw new IllegalStateException("withMaximumAgeForStoreOnly() requires a heartbeat interval of at most "
                    + InternalMaintenance.STORE_ONLY_HEARTBEAT_LIMIT + ", not " + heartbeat);
        }
        Duration minimum = InternalMaintenance.storeOnlyMinimumAge(heartbeat);
        if (store().maximumAge().compareTo(minimum) < 0) {
            throw new IllegalStateException("withMaximumAgeForStoreOnly() requires a maximum age of at least five "
                    + "heartbeat intervals (" + minimum + "), not " + store().maximumAge()
                    + ": set a longer maximum age or a shorter heartbeat interval");
        }
    }

    // how long size pruning spares a row (Store#pruneToSize): at least long enough for a slower load's write on
    // "missing" not to apply over a newer row pruned in the meantime (a fixed floor, for every change source, also
    // without distribution); polled, also the window in which a poll can still find a write: the poll interval with its
    // jitter and the poll's own time (rounded up to twice the interval) plus the overlap for writes committed after
    // their time. A feed needs nothing more: its messages carry their versions, not a reference to the row (VISION.md,
    // "In-memory store profiles", dimension 0)
    // With the maximum age for the store only, at least three heartbeat intervals: a trusted holder has heard every
    // write older than that (two intervals with their jitter), so a row pruned is no write it may not know of, and the
    // atomic methods decide on its held copy (InternalCore#read)
    Duration pruneGrace(boolean polled) {
        Duration window = store().pollInterval().multipliedBy(2).plus(InternalPollingStore.POLL_OVERLAP);
        Duration grace = polled && window.compareTo(InternalMaintenance.PRUNE_GRACE) > 0 ? window
                : InternalMaintenance.PRUNE_GRACE;
        Duration heard = store().heartbeatInterval().multipliedBy(3);
        return store().maximumAgeForStoreOnly() && heard.compareTo(grace) > 0 ? heard : grace;
    }

    private static <T> void configure(Configurer<T> configurer, T configured) {
        requireNonNull(requireNonNull(configurer, "configurer cannot be null").apply(configured),
                "configurer cannot return null");
    }

    // creates the store (each of its operations bounded by the operation timeout) and builds the core; a failure
    // leaves nothing behind. A store not reached (a retryable failure) starts the cache without it, unless
    // withFailFast(): a stand-in until the core connects the store in the background (VISION.md, "Starting without the
    // store"). Then the name lacks what an adapter learns from the server (the product behind a Redis client)
    // whatever fails, what was claimed is released before it is rethrown; created is the adapter's store once
    // assigned, which Sonar does not take as non-null
    @SuppressWarnings({"java:S1181", "java:S4449"})
    private InternalCore<K, V> core(InternalCore.@Nullable Loaders<K, V> loaders) {
        InternalClaims.claim(adapter);
        Store created = null;
        try {
            InternalSerializer<K> keySerializer = serializers.keySerializer();
            InternalSerializer<V> valueSerializer = serializers.valueSerializer();
            Executor executor = requireNonNullElseGet(store().executor(), InternalExecutors::defaultStoreExecutor);
            StoreSettings settings = new StoreSettings(keySerializer.format(), valueSerializer.format(),
                    store().operationTimeout());
            InternalPendingStore.Created attempt = InternalPendingStore.create(adapter, settings, store().failFast(),
                    environment);
            created = attempt.store();
            // where the cache lives, as its log messages begin with it; taken once the store exists, as an adapter may
            // name the server's product only after asking it (Valkey or Kvrocks behind a Redis client, say)
            String name = String.valueOf(adapter);
            InternalCore.Connector connector = attempt.connector(() -> connect(settings, name, executor),
                    store().unboundedAge() && !caffeine.hasExpiry());
            if (connector == null) {
                created = distributing(created, name, executor);
            }
            // no copy outlives the store's maximum age, also where no message drops it (none sent, or one lost); with
            // the maximum age for the store only, the copies expire as Caffeine's expiry says, except those no store
            // confirmed (InternalExpiry#unconfirmedMaximumAge)
            boolean forStoreOnly = store().maximumAgeForStoreOnly();
            return new InternalCore<>(name, created,
                    caffeine.caffeine(forStoreOnly ? StoreConfigurer.UNBOUNDED_AGE : store().maximumAge(),
                            store().maximumAge()),
                    caffeine.internalExpiry(), new InternalHasher<>(hashProvider), keySerializer,
                    valueSerializer, store().maximumAge(), forStoreOnly, executor, caffeine.refreshAfterWrite(),
                    loaders, distributionMode(), negativeCachingExpiry, store().maximumSize(),
                    store().maintenanceInterval(), pruneGrace(created instanceof InternalPollingStore),
                    store().heartbeatInterval(), loadAttempts,
                    InternalStatsCounter.of(caffeine.isRecordingStats()),
                    () -> InternalClaims.release(adapter), connector, environment);
        } catch (RuntimeException | Error e) {
            if (created != null) {
                created.close();
            }
            InternalClaims.release(adapter);
            throw e;
        }
    }

    // the connect of a cache started without its store, in the background (InternalCore#attemptConnect): the store
    // created and made ready as build() does; a store created but not usable as configured is closed again
    private InternalCore.Connected connect(StoreSettings settings, String name, Executor executor) {
        Store created = InternalReachability.create(adapter, settings, environment);
        try {
            Store distributing = distributing(created, name, executor);
            return new InternalCore.Connected(distributing, pruneGrace(distributing instanceof InternalPollingStore));
        } catch (RuntimeException e) {
            created.close();
            throw e;
        }
    }

    // what the core subscribes to, decided by the store created, in one place for build() and for a connect later
    // (VISION.md, "Starting without the store", design step 2): its own feed if it has one, or else its scan, polled
    // by the core (VISION.md, "The polling feed"); unchanged without distribution. A future choice of the user (polling
    // forced, a message bus) plugs in here
    private Store distributing(Store created, String name, Executor executor) {
        if (distributionMode() == DistributionMode.NONE || created instanceof ChangeFeed) {
            return created;
        }
        if (!(created instanceof ChangeScan scan)) {
            throw new IllegalStateException(adapter + " offers no change feed and no change scan, so it supports "
                    + "only DistributionMode.NONE, not " + distributionMode());
        }
        if (store().pollInterval().multipliedBy(2).compareTo(store().heartbeatInterval()) > 0) {
            throw new IllegalStateException("The poll interval " + store().pollInterval() + " must be at most half "
                    + "the heartbeat interval " + store().heartbeatInterval()
                    + ", or the heartbeat takes the pause between polls for a silent feed");
        }
        Duration overlap = InternalPollingStore.POLL_OVERLAP;
        return new InternalPollingStore(name, created, scan, store().pollInterval(), overlap,
                InternalMaintenance.TOMBSTONE_RETENTION.minus(overlap), store().operationTimeout(), executor,
                environment);
    }

    // as Caffeine decides whether to load in bulk: only if the loader overrides loadAll
    private static boolean overridesLoadAll(CacheLoader<?, ?> loader) {
        try {
            return loader.getClass().getMethod("loadAll", Set.class).getDeclaringClass() != CacheLoader.class;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static boolean overridesAsyncLoadAll(AsyncCacheLoader<?, ?> loader) {
        try {
            return loader.getClass().getMethod("asyncLoadAll", Set.class, Executor.class).getDeclaringClass()
                    != AsyncCacheLoader.class;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    // for the core, once it builds caches

    CaffeineConfigurer<K, V> caffeine() {
        return caffeine;
    }

    StoreConfigurer store() {
        return store;
    }

    @Nullable HashProvider<K> hashProvider() {
        return hashProvider;
    }

    DistributionMode distributionMode() {
        return distributionMode != null ? distributionMode : DistributionMode.INVALIDATE;
    }

    @Nullable Duration negativeCachingExpiry() {
        return negativeCachingExpiry;
    }

    // the time, sleeping, scheduling and randomness of the cache (InternalEnvironment): a simulation of the tests',
    // whose time they move, for the paths that wait for time; not public
    DistributedCaffeine<K, V> withEnvironment(InternalEnvironment environment) {
        this.environment = requireNonNull(environment, "environment cannot be null");
        return this;
    }

    // how often a load is attempted before it returns its value uncached (InternalCore#load): raised by the
    // linearizability tests only, above the writes their scenarios hold, so that they check every case but the one
    // documented exception; not public
    DistributedCaffeine<K, V> withLoadAttempts(int loadAttempts) {
        if (loadAttempts < 1) {
            throw new IllegalArgumentException("loadAttempts must be positive");
        }
        this.loadAttempts = loadAttempts;
        return this;
    }
}
