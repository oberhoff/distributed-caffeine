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
package io.github.oberhoff.distributedcaffeine.integration.spring;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.CaffeineSpec;
import io.github.oberhoff.distributedcaffeine.DistributedAsyncCache;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.hasher.HashProvider;
import io.github.oberhoff.distributedcaffeine.hasher.Hashable;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.cache.interceptor.SimpleKey;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A Spring {@link CaffeineCacheManager} whose caches are distributed: a drop-in that keeps Spring's caching
 * annotations and the manager's settings, with each cache built from a {@link DistributedCaffeine} builder the
 * application supplies by the cache's name, usually with the name as the cache's collection, table or keyspace. For
 * example, with MongoDB (each cache a collection; any other adapter works the same way):
 * <pre>{@code
 * @Bean
 * DistributedCaffeineCacheManager cacheManager(MongoClient mongoClient) {
 *     return new DistributedCaffeineCacheManager(name -> DistributedCaffeine.newBuilder(MongoAdapter
 *             .newBuilder(mongoClient, databaseName, name)
 *             // or all caches in one collection, told apart by name:
 *             // .newBuilder(mongoClient, databaseName, collectionName).withDiscriminator(name)
 *             .build())
 *         .withCaffeine(caffeine -> caffeine.maximumSize(10_000))
 *         .withStore(store -> store.withMaximumAge(Duration.ofHours(1))));
 * }
 * }</pre>
 * Differences to {@link CaffeineCacheManager}:
 * <ul>
 *     <li>Caches are configured on the builders: {@link #setCaffeine(Caffeine)}, {@link #setCaffeineSpec(CaffeineSpec)}
 *     and {@link #setCacheSpecification(String)} are not supported.</li>
 *     <li>Keys of Spring's default key generator are hashed by default: a single argument as the cache hashes it, and
 *     a {@link SimpleKey} (several arguments, or none) by its parts, each a {@link String}, {@link Long},
 *     {@link Integer}, {@link UUID}, an {@link Enum enum}, a {@link Hashable} or {@code null}. For other types, set a
 *     hash provider on the builder ({@link DistributedCaffeine#withHashProvider(HashProvider)}): it is asked only for
 *     keys, and parts, of other types.</li>
 *     <li>Values are serialized into the store, and {@code null} values ({@link #setAllowNullValues(boolean)}, on by
 *     default) as Spring's marker.</li>
 *     <li>The manager closes the caches it built: on shutdown, and once Spring drops one ({@link #removeCache(String)},
 *     {@link #resetCaches()}, a setter rebuilding the caches, or a custom cache registered under its name).</li>
 * </ul>
 *
 * @author Andreas Oberhoff
 */
public class DistributedCaffeineCacheManager extends CaffeineCacheManager implements DisposableBean {

    // the builder's hash provider, read to fill in the SimpleKey hashing around it: the builder has no getter, and
    // refuses a second provider. Our own class in the same artifact; looked up once, so a renamed field fails on the
    // first use of the manager
    private static final String HASH_PROVIDER_FIELD = "hashProvider";
    private static final VarHandle HASH_PROVIDER;

    static {
        try {
            HASH_PROVIDER = MethodHandles.privateLookupIn(DistributedCaffeine.class, MethodHandles.lookup())
                    .findVarHandle(DistributedCaffeine.class, HASH_PROVIDER_FIELD, HashProvider.class);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("the builder's field " + HASH_PROVIDER_FIELD + " was not found", e);
        }
    }

    private final Function<String, DistributedCaffeine<Object, Object>> builders;
    // what close the caches built, by name: Spring keeps its caches in a private map and drops them without closing
    private final Map<String, Runnable> built = new ConcurrentHashMap<>();
    private final AtomicReference<@Nullable AsyncCacheLoader<Object, Object>> loader = new AtomicReference<>();

    /**
     * Constructs a manager that creates caches by name on first use.
     *
     * @param builders the builder of each cache, by its name; a new builder, with a new adapter, on every call
     */
    public DistributedCaffeineCacheManager(Function<String, DistributedCaffeine<Object, Object>> builders) {
        this.builders = requireNonNull(builders, "builders cannot be null");
    }

    /**
     * Not supported: configure the caches on the builders ({@link DistributedCaffeine#withCaffeine}).
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void setCaffeine(Caffeine<Object, Object> caffeine) {
        throw unsupported("setCaffeine");
    }

    /**
     * Not supported: configure the caches on the builders ({@link DistributedCaffeine#withCaffeine}).
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void setCaffeineSpec(CaffeineSpec caffeineSpec) {
        throw unsupported("setCaffeineSpec");
    }

    /**
     * Not supported: configure the caches on the builders ({@link DistributedCaffeine#withCaffeine}).
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void setCacheSpecification(String cacheSpecification) {
        throw unsupported("setCacheSpecification");
    }

    private static UnsupportedOperationException unsupported(String method) {
        // a builder or spec handed over could not be applied: refused, not ignored
        return new UnsupportedOperationException(("%s is not supported by %s: configure the caches on the builders it "
                + "is constructed with (withCaffeine)").formatted(method,
                DistributedCaffeineCacheManager.class.getSimpleName()));
    }

    // Spring keeps the loader in a private field: remembered here first, since the setter rebuilds the caches

    @Override
    public void setCacheLoader(CacheLoader<Object, Object> cacheLoader) {
        loader.set(requireNonNull(cacheLoader, "cacheLoader cannot be null"));
        super.setCacheLoader(cacheLoader);
    }

    @Override
    public void setAsyncCacheLoader(AsyncCacheLoader<Object, Object> cacheLoader) {
        loader.set(requireNonNull(cacheLoader, "cacheLoader cannot be null"));
        super.setAsyncCacheLoader(cacheLoader);
    }

    @Override
    @SuppressWarnings("resource") // closed once Spring drops it, or on shutdown (track)
    protected Cache<Object, Object> createNativeCaffeineCache(String name) {
        DistributedCaffeine<Object, Object> builder = builder(name);
        @Nullable AsyncCacheLoader<Object, Object> cacheLoader = loader.get();
        if (cacheLoader == null) {
            DistributedCache<Object, Object> cache = builder.build();
            return track(name, cache, cache::close);
        }
        if (cacheLoader instanceof CacheLoader<Object, Object> regularCacheLoader) {
            DistributedCache<Object, Object> cache = builder.build(regularCacheLoader);
            return track(name, cache, cache::close);
        }
        throw new IllegalStateException(
                "Cannot create regular Caffeine Cache with async-only cache loader: " + cacheLoader);
    }

    @Override
    @SuppressWarnings("resource") // closed once Spring drops it, or on shutdown (track)
    protected AsyncCache<Object, Object> createAsyncCaffeineCache(String name) {
        DistributedCaffeine<Object, Object> builder = builder(name);
        @Nullable AsyncCacheLoader<Object, Object> cacheLoader = loader.get();
        DistributedAsyncCache<Object, Object> cache;
        if (cacheLoader == null) {
            cache = builder.buildAsync();
        } else if (cacheLoader instanceof CacheLoader<Object, Object> regularCacheLoader) {
            cache = builder.buildAsync(regularCacheLoader);
        } else {
            cache = builder.buildAsync(cacheLoader);
        }
        return track(name, cache, cache::close);
    }

    private DistributedCaffeine<Object, Object> builder(String name) {
        DistributedCaffeine<Object, Object> builder = requireNonNull(builders.apply(name),
                () -> "the builder of cache " + name + " cannot be null");
        @SuppressWarnings("unchecked")
        HashProvider<Object> own = (HashProvider<Object>) HASH_PROVIDER.get(builder);
        if (own == null) {
            builder.withHashProvider(new SimpleKeyHashProvider());
        } else if (!(own instanceof SimpleKeyHashProvider)) {
            // the builder's own provider asked only for what the SimpleKey hashing cannot hash itself
            HASH_PROVIDER.set(builder, new SimpleKeyHashProvider(own));
        }
        return builder;
    }

    // a cache built for a name replaces the one built before, which Spring is about to drop
    private <C> C track(String name, C cache, Runnable closer) {
        Runnable previous = built.put(name, closer);
        if (previous != null) {
            previous.run();
        }
        return cache;
    }

    @Override
    public void removeCache(String name) {
        super.removeCache(name);
        closeBuilt(name);
    }

    @Override
    public void registerCustomCache(String name, Cache<Object, Object> cache) {
        super.registerCustomCache(name, cache);
        closeBuilt(name);
    }

    @Override
    public void registerCustomCache(String name, AsyncCache<Object, Object> cache) {
        super.registerCustomCache(name, cache);
        closeBuilt(name);
    }

    @Override
    public void resetCaches() {
        super.resetCaches();
        // Spring drops the caches it created on demand, keeping only those fixed by name and the custom ones
        Set<String> kept = Set.copyOf(getCacheNames());
        for (String name : Set.copyOf(built.keySet())) {
            if (!kept.contains(name)) {
                closeBuilt(name);
            }
        }
    }

    private void closeBuilt(String name) {
        Runnable closer = built.remove(name);
        if (closer != null) {
            closer.run();
        }
    }

    /**
     * Closes every cache the manager built.
     */
    @Override
    public void destroy() {
        for (String name : Set.copyOf(built.keySet())) {
            closeBuilt(name);
        }
    }
}
