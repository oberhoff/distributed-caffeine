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
import io.github.oberhoff.distributedcaffeine.CaffeineConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedAsyncCache;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.SerializersConfigurer;
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
 *                     .newBuilder(mongoClient, databaseName, name)
 *                     // or all caches in one collection, told apart by name:
 *                     // .newBuilder(mongoClient, databaseName, collectionName).withDiscriminator(name)
 *                     .build())
 *             .withCaffeine(caffeine -> caffeine.maximumSize(10_000))
 *             .withStore(store -> store.withMaximumAge(Duration.ofHours(1))));
 * }
 * }</pre>
 * Differences to {@link CaffeineCacheManager}:
 * <ul>
 *     <li>Caches are configured on the builders: {@link #setCaffeine(Caffeine)} is not supported. A spec
 *     ({@link #setCaffeineSpec(CaffeineSpec)}, {@link #setCacheSpecification(String)}, Spring Boot's
 *     {@code spring.cache.caffeine.spec}) applies to every builder after its own settings
 *     ({@link CaffeineConfigurer#withSpec(String)}); a setting made by both fails.</li>
 *     <li>Keys of Spring's default key generator are hashed by default: a single argument as the cache hashes it, and
 *     a {@link SimpleKey} (several arguments, or none) by its parts, each a {@link String}, {@link Long},
 *     {@link Integer}, {@link UUID}, an {@link Enum enum}, a {@link Hashable} or {@code null}. For other types, set a
 *     hash provider on the builder ({@link DistributedCaffeine#withHashProvider(HashProvider)}): it is asked only for
 *     keys, and parts, of other types.</li>
 *     <li>Keys are serialized for diagnostics, by Fory where the builder sets no key serializer
 *     ({@link DistributedCaffeine#withSerializers}), a {@link SimpleKey} by its parts.</li>
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
    // the builder's key serializer, read alike to set SimpleKeyForySerializer where none was set: the builder's
    // serializers, then their key serializer, whose type is package-private, so the field is looked up by name
    private static final String SERIALIZERS_FIELD = "serializers";
    private static final String KEY_SERIALIZER_FIELD = "keySerializer";
    private static final VarHandle SERIALIZERS;
    private static final VarHandle KEY_SERIALIZER;

    static {
        String field = HASH_PROVIDER_FIELD;
        try {
            HASH_PROVIDER = MethodHandles.privateLookupIn(DistributedCaffeine.class, MethodHandles.lookup())
                    .findVarHandle(DistributedCaffeine.class, HASH_PROVIDER_FIELD, HashProvider.class);
            field = SERIALIZERS_FIELD;
            SERIALIZERS = MethodHandles.privateLookupIn(DistributedCaffeine.class, MethodHandles.lookup())
                    .findVarHandle(DistributedCaffeine.class, SERIALIZERS_FIELD, SerializersConfigurer.class);
            field = KEY_SERIALIZER_FIELD;
            KEY_SERIALIZER = MethodHandles.privateLookupIn(SerializersConfigurer.class, MethodHandles.lookup())
                    .unreflectVarHandle(SerializersConfigurer.class.getDeclaredField(KEY_SERIALIZER_FIELD));
        } catch (ReflectiveOperationException | SecurityException e) {
            throw new IllegalStateException("The builder's field " + field + " was not found or is not accessible",
                    e);
        }
    }

    private final Function<String, DistributedCaffeine<Object, Object>> builders;
    // what close the caches built, by name: Spring keeps its caches in a private map and drops them without closing
    private final Map<String, Runnable> built = new ConcurrentHashMap<>();
    private final AtomicReference<@Nullable AsyncCacheLoader<Object, Object>> loader = new AtomicReference<>();
    // the spec every builder gets, if one was set
    private final AtomicReference<@Nullable CaffeineSpec> spec = new AtomicReference<>();
    // one for every builder without a key serializer: thread-safe, and its Fory costly to build
    private final SimpleKeyForySerializer keySerializer = new SimpleKeyForySerializer();

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
        // a builder handed over could not be applied: refused, not ignored
        throw new UnsupportedOperationException("setCaffeine is not supported by "
                + DistributedCaffeineCacheManager.class.getSimpleName()
                + ": configure the caches on the builders it is constructed with (withCaffeine)");
    }

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> Applies to every builder after its own settings
     * ({@link CaffeineConfigurer#withSpec(CaffeineSpec)}); a setting made by both fails when the cache is built.
     */
    @Override
    public void setCaffeineSpec(CaffeineSpec caffeineSpec) {
        // remembered first: Spring's setter rebuilds the caches, through this manager's builders
        spec.set(requireNonNull(caffeineSpec, "caffeineSpec cannot be null"));
        super.setCaffeineSpec(caffeineSpec);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <b>Distributed Caffeine:</b> Applies to every builder after its own settings
     * ({@link CaffeineConfigurer#withSpec(String)}); a setting made by both fails when the cache is built.
     */
    @Override
    public void setCacheSpecification(String cacheSpecification) {
        spec.set(CaffeineSpec.parse(requireNonNull(cacheSpecification, "cacheSpecification cannot be null")));
        super.setCacheSpecification(cacheSpecification);
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
        throw new IllegalStateException(("A synchronous cache cannot be built with an asynchronous cache loader (%s): "
                + "set a CacheLoader, or enable setAsyncCacheMode(true)").formatted(cacheLoader));
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
                () -> "The builder function returned no builder for cache " + name);
        @SuppressWarnings("unchecked")
        HashProvider<Object> own = (HashProvider<Object>) HASH_PROVIDER.get(builder);
        if (own == null) {
            builder.withHashProvider(new SimpleKeyHashProvider());
        } else if (!(own instanceof SimpleKeyHashProvider)) {
            // the builder's own provider asked only for what the SimpleKey hashing cannot hash itself
            HASH_PROVIDER.set(builder, new SimpleKeyHashProvider(own));
        }
        if (KEY_SERIALIZER.get((SerializersConfigurer<?, ?>) SERIALIZERS.get(builder)) == null) {
            builder.withSerializers(serializers -> serializers.withKeySerializer(keySerializer));
        }
        CaffeineSpec caffeineSpec = spec.get();
        if (caffeineSpec != null) {
            try {
                builder.withCaffeine(caffeine -> caffeine.withSpec(caffeineSpec));
            } catch (IllegalStateException e) {
                throw new IllegalStateException(("The Caffeine spec '%s' sets what the builder of cache %s sets "
                        + "already: %s").formatted(caffeineSpec.toParsableString(), name, e.getMessage()), e);
            }
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
        // every cache is closed, even if one fails to; the first failure is thrown for Spring to report, with the
        // others suppressed in it
        @Nullable RuntimeException failure = null;
        for (String name : Set.copyOf(built.keySet())) {
            try {
                closeBuilt(name);
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
