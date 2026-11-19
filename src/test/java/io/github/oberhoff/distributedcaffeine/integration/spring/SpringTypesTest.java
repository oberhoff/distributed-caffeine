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

import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.serializer.JavaObjectSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.cache.interceptor.SimpleKeyGenerator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Settles how Spring's own types fare over a distributed cache, before the integration is built (VISION.md,
 * "Integrations: one subpackage per framework"): its {@code NullValue} marker as a value, which Spring recognizes by
 * identity, and its {@code SimpleKey} as a key. Each value crosses the store: written by one instance, read by
 * another that does not hold it.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class SpringTypesTest {


    private final InMemoryDatabase database = new InMemoryDatabase();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    // an adapter per instance, all on one table
    private Adapter<Object, Object> adapter() {
        return settings -> database.openStore("cache");
    }

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private CaffeineCache cache(UnaryOperator<DistributedCaffeine<Object, Object>> configuration) {
        var cache = configuration.apply(DistributedCaffeine.newBuilder(adapter())).build();
        closeables.add(cache);
        return new CaffeineCache("cache", cache, true);
    }

    private CaffeineCache asyncCache(UnaryOperator<DistributedCaffeine<Object, Object>> configuration) {
        var cache = configuration.apply(DistributedCaffeine.newBuilder(adapter())).buildAsync();
        closeables.add(cache);
        return new CaffeineCache("cache", cache, true);
    }

    @Test
    void a_null_result_crosses_the_store_with_the_default_serializer() {
        CaffeineCache writer = cache(UnaryOperator.identity());
        CaffeineCache reader = cache(UnaryOperator.identity());

        writer.put("k", null);

        ValueWrapper wrapper = reader.get("k");
        assertThat(wrapper).as("a cached null, not a miss").isNotNull();
        assertThat(wrapper.get()).isNull();
    }

    @Test
    void a_null_result_crosses_the_store_with_java_serialization() {
        UnaryOperator<DistributedCaffeine<Object, Object>> java = builder -> builder.withSerializers(
                serializers -> serializers.withValueSerializer(new JavaObjectSerializer<>()));
        CaffeineCache writer = cache(java);
        CaffeineCache reader = cache(java);

        writer.put("k", null);

        ValueWrapper wrapper = reader.get("k");
        assertThat(wrapper).as("a cached null, not a miss").isNotNull();
        assertThat(wrapper.get()).isNull();
    }

    @Test
    void a_null_result_crosses_the_store_in_async_mode() {
        CaffeineCache writer = asyncCache(UnaryOperator.identity());
        CaffeineCache reader = asyncCache(UnaryOperator.identity());

        writer.put("k", null);

        // async mode: a cached null completes with a wrapper of null, a miss gives no future at all
        CompletableFuture<?> future = reader.retrieve("k");
        assertThat(future).as("a cached null, not a miss").isNotNull().succeedsWithin(Duration.ofSeconds(5)).isInstanceOfSatisfying(ValueWrapper.class,
                wrapper -> assertThat(wrapper.get()).isNull());
    }

    @Test
    void the_key_generators_keys_take_the_built_in_hashing_only_when_single() {
        CaffeineCache cache = cache(UnaryOperator.identity());

        // one argument is the key as it is, several or none a SimpleKey
        Object single = SimpleKeyGenerator.generateKey("a");
        Object several = SimpleKeyGenerator.generateKey("a", 1);
        Object none = SimpleKeyGenerator.generateKey();
        cache.put(single, "v");

        assertThat(single).isEqualTo("a");
        assertThat(cache.get(single)).isNotNull();
        assertThatThrownBy(() -> cache.put(several, "v")).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SimpleKey");
        assertThatThrownBy(() -> cache.put(none, "v")).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SimpleKey");
    }

    @Test
    void a_simple_key_crosses_the_store_once_hashed() {
        UnaryOperator<DistributedCaffeine<Object, Object>> hashed = builder -> builder.withHashProvider(new SimpleKeyHashProvider());
        CaffeineCache writer = cache(hashed);
        CaffeineCache reader = cache(hashed);
        Object key = new SimpleKey("a", 1);

        // the key is serialized into the store too (for diagnostics), with the default serializer
        writer.put(key, "v");
        writer.put(SimpleKey.EMPTY, "empty");

        ValueWrapper wrapper = reader.get(new SimpleKey("a", 1));
        assertThat(wrapper).isNotNull();
        assertThat(wrapper.get()).isEqualTo("v");
        ValueWrapper empty = reader.get(SimpleKey.EMPTY);
        assertThat(empty).isNotNull();
        assertThat(empty.get()).isEqualTo("empty");
    }
}
