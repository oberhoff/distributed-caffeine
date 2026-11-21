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
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.CaffeineSpec;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.hasher.Hasher;
import io.github.oberhoff.distributedcaffeine.serializer.ForySerializer;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.CachingConfigurer;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.cache.interceptor.LoggingCacheErrorHandler;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The manager in a Spring context: two contexts on one store stand for two instances of an application.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class DistributedCaffeineCacheManagerTest {

    private final InMemoryDatabase database = new InMemoryDatabase();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    // each cache in a table of its own, by its name, as a discriminator would separate them in one table
    private DistributedCaffeine<Object, Object> builder(String name) {
        return DistributedCaffeine.newBuilder(settings -> database.openStore(name));
    }

    private DistributedCaffeineCacheManager manager() {
        DistributedCaffeineCacheManager manager = new DistributedCaffeineCacheManager(this::builder);
        closeables.add(manager::destroy);
        return manager;
    }

    // the source behind the cached methods, counting its calls, shared by every instance
    static final class Source {
        final AtomicInteger calls = new AtomicInteger();
    }

    static class Service {
        private final Source source;

        Service(Source source) {
            this.source = source;
        }

        @Cacheable("orders")
        public String order(String id) {
            source.calls.incrementAndGet();
            return "order " + id;
        }

        @Cacheable("orders")
        public String order(String id, int version) {
            source.calls.incrementAndGet();
            return "order " + id + " v" + version;
        }

        @Cacheable("totals")
        @SuppressWarnings("SameReturnValue") // a fixed result, which the other instance must be served from the store
        public String total() {
            source.calls.incrementAndGet();
            return "total";
        }

        @Cacheable("orders")
        @SuppressWarnings("SameReturnValue") // always null: a cached null, served as such by the other instance
        public @Nullable String missing(@SuppressWarnings("unused") String id) {
            source.calls.incrementAndGet();
            return null;
        }

        @Cacheable("orders")
        public void byDate(@SuppressWarnings("unused") LocalDate date) {
            source.calls.incrementAndGet();
        }

        @CacheEvict("orders")
        @SuppressWarnings("EmptyMethod") // the annotation evicts
        public void evict(@SuppressWarnings("unused") String id) {
            // evicts only
        }

        @Cacheable(value = "orders", sync = true)
        public String orderLoaded(String id) {
            source.calls.incrementAndGet();
            return "loaded order " + id;
        }

        @CacheEvict(value = "orders", allEntries = true)
        @SuppressWarnings("EmptyMethod") // the annotation evicts
        public void evictAll() {
            // evicts only
        }
    }

    @Configuration
    @EnableCaching
    static class Application {
        @Bean
        DistributedCaffeineCacheManager cacheManager(InMemoryDatabase database) {
            return new DistributedCaffeineCacheManager(
                    name -> DistributedCaffeine.newBuilder(settings -> database.openStore(name)));
        }

        @Bean
        Service service(Source source) {
            return new Service(source);
        }
    }

    // an application whose cached methods return futures: its caches in async mode, as Spring requires for them
    static class AsyncService {
        private final Source source;

        AsyncService(Source source) {
            this.source = source;
        }

        @Cacheable("futures")
        public CompletableFuture<String> order(String id) {
            source.calls.incrementAndGet();
            return CompletableFuture.supplyAsync(() -> "order " + id);
        }

        @Cacheable("futures")
        public CompletableFuture<String> order(String id, int version) {
            source.calls.incrementAndGet();
            return CompletableFuture.supplyAsync(() -> "order " + id + " v" + version);
        }
    }

    @Configuration
    @EnableCaching
    static class AsyncApplication {
        @Bean
        DistributedCaffeineCacheManager cacheManager(InMemoryDatabase database) {
            DistributedCaffeineCacheManager manager = new DistributedCaffeineCacheManager(
                    name -> DistributedCaffeine.newBuilder(settings -> database.openStore(name)));
            manager.setAsyncCacheMode(true);
            return manager;
        }

        @Bean
        AsyncService asyncService(Source source) {
            return new AsyncService(source);
        }
    }

    // an application whose cache errors are logged instead of failing the call
    @Configuration
    static class LoggingErrors implements CachingConfigurer {
        @Override
        public CacheErrorHandler errorHandler() {
            return new LoggingCacheErrorHandler();
        }
    }

    private Service instance(Source source, Class<?>... configurations) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.registerBean(InMemoryDatabase.class, () -> database);
        context.registerBean(Source.class, () -> source);
        context.register(Application.class);
        for (Class<?> configuration : configurations) {
            context.register(configuration);
        }
        context.refresh();
        closeables.add(context);
        return context.getBean(Service.class);
    }

    private AsyncService asyncInstance(Source source) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.registerBean(InMemoryDatabase.class, () -> database);
        context.registerBean(Source.class, () -> source);
        context.register(AsyncApplication.class);
        context.refresh();
        closeables.add(context);
        return context.getBean(AsyncService.class);
    }

    @Test
    void a_future_result_cached_by_one_instance_is_served_on_another() {
        Source source = new Source();
        AsyncService first = asyncInstance(source);
        AsyncService second = asyncInstance(source);

        assertThat(first.order("o1")).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("order o1");
        assertThat(first.order("o1", 2)).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("order o1 v2");
        assertThat(source.calls).hasValue(2);

        assertThat(second.order("o1")).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("order o1");
        assertThat(second.order("o1", 2)).succeedsWithin(Duration.ofSeconds(5)).isEqualTo("order o1 v2");
        assertThat(source.calls).as("every result from the store").hasValue(2);
    }

    @Test
    void a_result_cached_by_one_instance_is_served_on_another() {
        Source source = new Source();
        Service first = instance(source);
        Service second = instance(source);

        assertThat(first.order("o1")).isEqualTo("order o1");
        assertThat(first.order("o1", 2)).isEqualTo("order o1 v2");
        assertThat(first.total()).isEqualTo("total");
        assertThat(first.missing("o2")).isNull();
        assertThat(source.calls).hasValue(4);

        assertThat(second.order("o1")).isEqualTo("order o1");
        assertThat(second.order("o1", 2)).isEqualTo("order o1 v2");
        assertThat(second.total()).isEqualTo("total");
        assertThat(second.missing("o2")).isNull();
        assertThat(source.calls).as("every result from the store").hasValue(4);
    }

    @Test
    void an_eviction_reaches_every_instance() {
        Source source = new Source();
        Service first = instance(source);
        Service second = instance(source);
        first.order("o1");
        first.order("o2");
        second.order("o1");
        second.order("o2");

        // the eviction reaches the other instance's copies with the store's delivery
        first.evict("o1");
        await(() -> {
            second.order("o1");
            return source.calls.get() == 3;
        });
        assertThat(second.order("o2")).isEqualTo("order o2");
        assertThat(source.calls).as("only the evicted one again").hasValue(3);

        first.evictAll();
        await(() -> {
            second.order("o1");
            second.order("o2");
            return source.calls.get() == 5;
        });
    }

    @Test
    void in_an_outage_a_cached_method_fails_on_its_write_unless_it_loads_through_the_cache() {
        Source source = new Source();
        Service service = instance(source);
        service.order("held");

        database.setDown(true);

        assertThat(service.order("held")).as("a held copy is served").isEqualTo("order held");
        assertThatThrownBy(() -> service.order("o1")).isInstanceOf(StoreException.class);
        assertThat(service.orderLoaded("o2")).isEqualTo("loaded order o2");
        assertThat(service.orderLoaded("o2")).as("kept locally").isEqualTo("loaded order o2");
        assertThat(source.calls).hasValue(3);
    }

    @Test
    void in_an_outage_a_logging_error_handler_keeps_cached_methods_working() {
        Source source = new Source();
        Service service = instance(source, LoggingErrors.class);
        // the cache built before the outage: the store is down for its operations, and the error handler covers
        // those, not the cache's creation (a build whose store call fails for good fails, before any handler)
        service.evict("o1");

        database.setDown(true);

        assertThat(service.order("o1")).isEqualTo("order o1");
        assertThat(service.order("o1")).isEqualTo("order o1");
        assertThat(source.calls).as("not kept: the write failed").hasValue(2);
        service.evict("o1");
    }

    @Test
    void a_key_of_another_type_fails_naming_it() {
        Service service = instance(new Source());
        LocalDate date = LocalDate.of(2026, 11, 19);

        assertThatThrownBy(() -> service.byDate(date))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("java.time.LocalDate");
    }

    @Test
    void a_builders_own_hash_provider_hashes_only_the_other_types() {
        List<Object> asked = new ArrayList<>();
        DistributedCaffeineCacheManager manager = new DistributedCaffeineCacheManager(name -> builder(name)
                .withHashProvider((key, hasher) -> {
                    asked.add(key);
                    return hasher.get().putLong(((LocalDate) key).toEpochDay()).getHash();
                }));
        closeables.add(manager::destroy);
        org.springframework.cache.Cache cache = manager.getCache("orders");
        LocalDate date = LocalDate.of(2026, 11, 19);

        assertThat(cache).isNotNull();
        cache.put(date, "v");
        cache.put(new SimpleKey("a", date), "w");
        cache.put("a", "x");

        assertThat(cache.get(LocalDate.of(2026, 11, 19), String.class)).isEqualTo("v");
        assertThat(cache.get(new SimpleKey("a", date), String.class)).isEqualTo("w");
        assertThat(cache.get("a", String.class)).isEqualTo("x");
        assertThat(asked).as("asked for the dates only").containsOnly(date);
    }

    @Test
    void a_builders_simple_key_hash_provider_is_kept() {
        SimpleKeyHashProvider own = new SimpleKeyHashProvider();
        DistributedCaffeineCacheManager manager = new DistributedCaffeineCacheManager(
                name -> builder(name).withHashProvider(own));
        closeables.add(manager::destroy);
        org.springframework.cache.Cache cache = manager.getCache("orders");

        assertThat(cache).isNotNull();
        cache.put(new SimpleKey("a", 1), "v");
        assertThat(cache.get(new SimpleKey("a", 1), String.class)).isEqualTo("v");
    }

    // the key bytes stored for a key of the cache orders, as its row holds them
    private byte[] storedKey(Object key) {
        String hash = new SimpleKeyHashProvider().getHash(key, Hasher::new).toHex();
        Row row;
        try (Store store = database.openStore("orders")) {
            row = store.read(List.of(hash), Duration.ofDays(1)).get(hash);
        }
        assertThat(row).as("the row of %s", key).isNotNull();
        return row.key();
    }

    @Test
    void a_simple_key_is_stored_by_its_params_where_no_key_serializer_was_set() {
        org.springframework.cache.Cache cache = manager().getCache("orders");
        SimpleKey key = new SimpleKey("a", null, 7L);

        assertThat(cache).isNotNull();
        cache.put(key, "v");

        byte[] stored = storedKey(key);
        assertThat(new SimpleKeyForySerializer().deserialize(stored)).isEqualTo(key);
        assertThat(stored).as("not by Java serialization").isNotEqualTo(new ForySerializer<>().serialize(key));
    }

    @Test
    void a_builders_own_key_serializer_is_kept() {
        ForySerializer<Object> own = new ForySerializer<>();
        DistributedCaffeineCacheManager manager = new DistributedCaffeineCacheManager(name -> builder(name)
                .withSerializers(serializers -> serializers.withKeySerializer(own)));
        closeables.add(manager::destroy);
        org.springframework.cache.Cache cache = manager.getCache("orders");
        SimpleKey key = new SimpleKey("a", 7L);

        assertThat(cache).isNotNull();
        cache.put(key, "v");

        assertThat(storedKey(key)).isEqualTo(own.serialize(key));
    }

    @Test
    void a_builder_handed_over_is_refused() {
        DistributedCaffeineCacheManager manager = manager();
        Caffeine<Object, Object> caffeine = Caffeine.newBuilder();

        assertThatThrownBy(() -> manager.setCaffeine(caffeine))
                .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("withCaffeine");
    }

    @Test
    void a_spec_applies_to_every_cache_also_to_those_built_before() {
        DistributedCaffeineCacheManager manager = manager();
        Cache<Object, Object> before = nativeCache(manager, "before");

        manager.setCacheSpecification("maximumSize=5,recordStats");

        assertThat(nativeCache(manager, "before")).as("the cache built before, replaced").isNotSameAs(before);
        assertThatThrownBy(before::policy).as("and closed").isInstanceOf(IllegalStateException.class);
        for (String name : new String[]{"before", "after"}) {
            Cache<Object, Object> cache = nativeCache(manager, name);
            assertThat(cache.policy().eviction()).as(name).hasValueSatisfying(
                    eviction -> assertThat(eviction.getMaximum()).isEqualTo(5));
            assertThat(cache.policy().isRecordingStats()).as(name).isTrue();
        }

        manager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=7"));
        assertThat(nativeCache(manager, "after").policy().eviction()).hasValueSatisfying(
                eviction -> assertThat(eviction.getMaximum()).isEqualTo(7));
    }

    @Test
    void a_setting_made_by_both_the_spec_and_the_builder_fails_naming_the_cache() {
        DistributedCaffeineCacheManager manager = new DistributedCaffeineCacheManager(name -> builder(name)
                .withCaffeine(caffeine -> caffeine.maximumSize(10)));
        closeables.add(manager::destroy);
        manager.setCacheSpecification("maximumSize=5");

        assertThatThrownBy(() -> manager.getCache("orders")).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("orders").hasMessageContaining("maximumSize=5")
                .hasMessageContaining("maximum size was already set");
    }

    @Test
    void a_loader_and_the_async_mode_reach_the_caches() {
        DistributedCaffeineCacheManager manager = manager();
        manager.setCacheLoader(key -> "loaded " + key);
        assertThat(nativeCache(manager, "a").get("k", key -> "computed")).isEqualTo("computed");
        assertThat(((LoadingCache<Object, Object>) nativeCache(manager, "b"))
                .get("k")).isEqualTo("loaded k");

        manager.setAsyncCacheMode(true);
        org.springframework.cache.Cache cache = manager.getCache("c");
        assertThat(cache).isInstanceOf(CaffeineCache.class);
        assertThat(((AsyncLoadingCache<Object, Object>) ((CaffeineCache) cache).getAsyncCache()).get("k"))
                .succeedsWithin(Duration.ofSeconds(5)).isEqualTo("loaded k");
    }

    private static Cache<Object, Object> nativeCache(DistributedCaffeineCacheManager manager, String name) {
        org.springframework.cache.Cache cache = manager.getCache(name);
        assertThat(cache).isInstanceOf(CaffeineCache.class);
        return ((CaffeineCache) cache).getNativeCache();
    }

    @Test
    void a_cache_spring_drops_is_closed() {
        DistributedCaffeineCacheManager manager = manager();
        Cache<Object, Object> removed = nativeCache(manager, "removed");
        Cache<Object, Object> replaced = nativeCache(manager, "replaced");
        Cache<Object, Object> rebuilt = nativeCache(manager, "rebuilt");
        Cache<Object, Object> reset = nativeCache(manager, "reset");

        manager.removeCache("removed");
        manager.registerCustomCache("replaced", Caffeine.newBuilder().build());
        assertThatThrownBy(() -> removed.getIfPresent("k")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> replaced.getIfPresent("k")).isInstanceOf(IllegalStateException.class);
        assertThat(rebuilt.getIfPresent("k")).isNull();

        manager.setAllowNullValues(false);
        assertThatThrownBy(() -> rebuilt.getIfPresent("k")).as("rebuilt by the setter")
                .isInstanceOf(IllegalStateException.class);
        Cache<Object, Object> current = nativeCache(manager, "rebuilt");
        assertThat(current.getIfPresent("k")).isNull();

        manager.resetCaches();
        assertThatThrownBy(() -> reset.getIfPresent("k")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> current.getIfPresent("k")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void the_caches_are_closed_on_shutdown() {
        DistributedCaffeineCacheManager manager = manager();
        Cache<Object, Object> cache = nativeCache(manager, "a");
        AsyncCache<Object, Object> unused = Caffeine.newBuilder().buildAsync();
        manager.registerCustomCache("custom", unused);

        manager.destroy();

        assertThatThrownBy(() -> cache.getIfPresent("k")).isInstanceOf(IllegalStateException.class);
        assertThat(unused.synchronous().getIfPresent("k")).as("a custom cache is the application's").isNull();
    }
}
