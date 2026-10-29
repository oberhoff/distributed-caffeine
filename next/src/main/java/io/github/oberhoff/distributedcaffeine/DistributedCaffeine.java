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
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

/**
 * The builder of a distributed cache: a Caffeine cache per instance, read through a shared store.
 * <pre>{@code
 * DistributedLoadingCache<String, Order> orders = DistributedCaffeine.newBuilder(adapter)
 *     .withCaffeine(caffeine -> caffeine
 *         .maximumSize(10_000)
 *         .expireAfterWrite(Duration.ofMinutes(5)))
 *     .withStore(store -> store
 *         .withMaximumAge(Duration.ofHours(1))
 *         .withMaximumSize(1_000_000))
 *     .build(key -> repository.load(key));
 * }</pre>
 *
 * @param <K> the key type, fixed by the adapter
 * @param <V> the value type, fixed by the adapter
 */
public final class DistributedCaffeine<K, V> {

    static final Duration DEFAULT_BUILD_TIMEOUT = Duration.ofSeconds(30);
    private static final String LOADER_CANNOT_BE_NULL = "loader cannot be null";

    private final Adapter<K, V> adapter;
    private final CaffeineConfigurer<K, V> caffeine = new CaffeineConfigurer<>();
    private final StoreConfigurer store = new StoreConfigurer();
    private final SerializersConfigurer<K, V> serializers = new SerializersConfigurer<>();
    private @Nullable HashProvider<K> hashProvider;
    private @Nullable DistributionMode distributionMode;
    private @Nullable Duration absentCachingExpiry;
    private Duration buildTimeout = DEFAULT_BUILD_TIMEOUT;
    private @Nullable Executor storeExecutor;
    private int loadAttempts = InternalCore.LOAD_ATTEMPTS;

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
     * Configures this instance's local level with Caffeine's builder methods, which keep their meaning (see
     * {@link CaffeineConfigurer}). May be called more than once; settings accumulate, and a setting made twice is
     * rejected as in Caffeine.
     *
     * @param configurer the configuration
     * @return this builder
     */
    public DistributedCaffeine<K, V> withCaffeine(Configurer<CaffeineConfigurer<K, V>> configurer) {
        configure(configurer, caffeine);
        return this;
    }

    /**
     * Configures the bounds of the shared store; a maximum age and a maximum size are required.
     *
     * @param configurer the configuration
     * @return this builder
     */
    public DistributedCaffeine<K, V> withStore(Configurer<StoreConfigurer> configurer) {
        configure(configurer, store);
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
     * Sets how keys are hashed, for key types that neither are built-in ({@code String}, {@code Long},
     * {@code Integer}, {@code UUID}) nor implement {@code Hashable}. Keys that are {@code equals} must produce the
     * same hash.
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
     * Sets how instances learn about changes made elsewhere; {@link DistributionMode#INVALIDATE} by default. With
     * {@link DistributionMode#NONE} and no {@code expireAfterWrite} or {@code expireAfter}, local copies expire after
     * the store's maximum age.
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
     * Remembers keys whose load found nothing, for the given time, in a small local set, so that repeated requests
     * for a key that does not exist reach neither the store nor the source. A value written through the cache
     * anywhere ends the absence at once; a value appearing only in the source is found after this time.
     *
     * @param expiry how long an absence is remembered (positive; keep it short)
     * @return this builder
     */
    public DistributedCaffeine<K, V> withAbsentCaching(Duration expiry) {
        requireNonNull(expiry, "expiry cannot be null");
        if (expiry.isNegative() || expiry.isZero()) {
            throw new IllegalArgumentException("expiry must be positive");
        }
        if (absentCachingExpiry != null) {
            throw new IllegalStateException("absent caching was already configured");
        }
        this.absentCachingExpiry = expiry;
        return this;
    }

    /**
     * Sets how long {@code build} may take to connect and check the store before it fails. Optional: 30 seconds by
     * default.
     *
     * @param buildTimeout the timeout (positive)
     * @return this builder
     */
    public DistributedCaffeine<K, V> withBuildTimeout(Duration buildTimeout) {
        requireNonNull(buildTimeout, "buildTimeout cannot be null");
        if (buildTimeout.isNegative() || buildTimeout.isZero()) {
            throw new IllegalArgumentException("buildTimeout must be positive");
        }
        this.buildTimeout = buildTimeout;
        return this;
    }

    /**
     * Sets the executor for the library's blocking store I/O (e.g. virtual threads on Java 21 or later). Optional:
     * a library-owned pool of daemon threads by default. Owned and closed by the caller. It must run each task on
     * another thread than the one submitting it (not {@code Runnable::run}): some loads are started by Caffeine under
     * its lock of the key, and their store I/O must not run under it.
     *
     * @param storeExecutor the executor
     * @return this builder
     */
    public DistributedCaffeine<K, V> withStoreExecutor(Executor storeExecutor) {
        this.storeExecutor = requireNonNull(storeExecutor, "storeExecutor cannot be null");
        return this;
    }

    /**
     * Builds a cache; connects and checks the store synchronously and fails fast.
     *
     * @return the cache
     */
    public DistributedCache<K, V> build() {
        validate(false);
        return new InternalDistributedCache<>(core(null));
    }

    /**
     * Builds a loading cache; connects and checks the store synchronously and fails fast.
     *
     * @param loader the loader of values missing from the store
     * @return the cache
     */
    public DistributedLoadingCache<K, V> build(CacheLoader<? super K, V> loader) {
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

    /**
     * Builds an asynchronous cache; connects and checks the store synchronously and fails fast.
     *
     * @return the cache
     */
    public DistributedAsyncCache<K, V> buildAsync() {
        validate(false);
        return new InternalDistributedAsyncCache<>(core(null), caffeine.executor());
    }

    /**
     * Builds an asynchronous loading cache; connects and checks the store synchronously and fails fast.
     *
     * @param loader the loader of values missing from the store
     * @return the cache
     */
    public DistributedAsyncLoadingCache<K, V> buildAsync(CacheLoader<? super K, V> loader) {
        requireNonNull(loader, LOADER_CANNOT_BE_NULL);
        validate(true);
        return buildAsyncLoading(loader, overridesLoadAll(loader));
    }

    /**
     * Builds an asynchronous loading cache; connects and checks the store synchronously and fails fast.
     *
     * @param loader the loader of values missing from the store
     * @return the cache
     */
    public DistributedAsyncLoadingCache<K, V> buildAsync(AsyncCacheLoader<? super K, V> loader) {
        requireNonNull(loader, LOADER_CANNOT_BE_NULL);
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
        if (store.maximumAge() == null || store.maximumSize() == -1) {
            throw new IllegalStateException("The store's maximum age and maximum size are required: "
                    + "withStore(store -> store.withMaximumAge(...).withMaximumSize(...))");
        }
        if (caffeine.isRefreshing() && !loading) {
            throw new IllegalStateException("refreshAfterWrite requires a loading cache, as in Caffeine");
        }
    }

    // how long size pruning spares a row (Store#pruneToSize): at least long enough for a slower load's write on
    // "missing" not to apply over a newer row pruned meanwhile (a fixed floor, for every change source, also without
    // distribution); polled, also the window in which a poll can still find a write: the poll interval with its jitter
    // and the poll's own time (rounded up to twice the interval) plus the overlap for writes committed after their
    // time. A feed needs nothing more: its messages carry their versions, not a reference to the row (VISION.md,
    // "In-memory store profiles", dimension 0)
    private Duration pruneGrace(boolean polled) {
        Duration window = store().pollInterval().multipliedBy(2).plus(InternalPollingStore.OVERLAP);
        return polled && window.compareTo(InternalMaintenance.PRUNE_GRACE) > 0 ? window
                : InternalMaintenance.PRUNE_GRACE;
    }

    private static <T> void configure(Configurer<T> configurer, T configured) {
        requireNonNull(requireNonNull(configurer, "configurer cannot be null").apply(configured),
                "configurer cannot return null");
    }

    // connects to the store (within the build timeout) and builds the core; a failure leaves nothing behind
    @SuppressWarnings("java:S1181") // whatever fails, what was claimed is released before it is rethrown
    private InternalCore<K, V> core(InternalCore.@Nullable Loaders<K, V> loaders) {
        InternalClaims.claim(adapter);
        Store created = null;
        try {
            InternalSerializer<K> keySerializer = serializers.keySerializer();
            InternalSerializer<V> valueSerializer = serializers.valueSerializer();
            Executor executor = storeExecutor != null ? storeExecutor : InternalExecutors.defaultStoreExecutor();
            created = createStore(new StoreSettings(keySerializer.format(), valueSerializer.format(),
                    store().timeout()), executor);
            if (distributionMode() != DistributionMode.NONE && !(created instanceof ChangeFeed)) {
                if (!(created instanceof ChangeScan scan)) {
                    throw new IllegalStateException(adapter + " offers no change feed and no change scan, so it "
                            + "supports only DistributionMode.NONE, not " + distributionMode());
                }
                // polled by the core (VISION.md, "The polling feed")
                if (store().pollInterval().multipliedBy(2).compareTo(store().heartbeatInterval()) > 0) {
                    throw new IllegalStateException("The poll interval " + store().pollInterval() + " must be at "
                            + "most half the heartbeat interval " + store().heartbeatInterval()
                            + ", or the heartbeat takes the pause between polls for a silent feed");
                }
                created = new InternalPollingStore(created, scan, store().pollInterval(), InternalPollingStore.OVERLAP,
                        InternalMaintenance.TOMBSTONE_RETENTION.minus(InternalPollingStore.OVERLAP), store().timeout(),
                        executor);
            }
            // no copy outlives the store's maximum age, also where no message drops it (none sent, or one lost)
            return new InternalCore<>(created, caffeine.caffeine(requireNonNull(store().maximumAge())),
                    new InternalHasher<>(hashProvider), keySerializer,
                    valueSerializer, requireNonNull(store().maximumAge()), executor, caffeine.refreshAfterWrite(),
                    loaders, distributionMode(), absentCachingExpiry, store().maximumSize(),
                    store().maintenanceInterval(), pruneGrace(created instanceof InternalPollingStore),
                    store().heartbeatInterval(), loadAttempts,
                    InternalStatsCounter.of(caffeine.isRecordingStats()),
                    () -> InternalClaims.release(adapter));
        } catch (RuntimeException | Error e) {
            if (created != null) {
                created.close();
            }
            InternalClaims.release(adapter);
            throw e;
        }
    }

    @SuppressWarnings("FutureReturnValueIgnored") // the callback closing a late store is attached for its side effect
    private Store createStore(StoreSettings settings, Executor executor) {
        CompletableFuture<Store> future = CompletableFuture.supplyAsync(
                () -> adapter.createStore(settings), executor);
        try {
            return future.get(buildTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            future.whenComplete((late, failure) -> {
                if (late != null) {
                    late.close();
                }
            });
            throw new StoreException("The store did not answer within " + buildTimeout, e, false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StoreException("Interrupted while connecting to the store", e, false);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new StoreException("Connecting to the store failed", requireNonNullElse(e.getCause(), e), false);
        }
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

    @Nullable Duration absentCachingExpiry() {
        return absentCachingExpiry;
    }

    Duration buildTimeout() {
        return buildTimeout;
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

    @Nullable Executor storeExecutor() {
        return storeExecutor;
    }
}
