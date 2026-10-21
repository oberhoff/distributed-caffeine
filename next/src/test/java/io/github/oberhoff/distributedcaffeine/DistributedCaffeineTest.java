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

import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Format;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.StorageOnlyStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DistributedCaffeineTest {

    private final InMemoryDatabase database = new InMemoryDatabase();
    private final Adapter<String, String> adapter = settings -> database.openStore("cache");

    @Test
    void the_configuration_reaches_the_builder() {
        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter)
                .withCaffeine(caffeine -> caffeine.maximumSize(10))
                .withCaffeine(caffeine -> caffeine.expireAfterWrite(Duration.ofMinutes(1)))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100)
                        .withMaintenanceInterval(Duration.ofSeconds(30)).withHeartbeatInterval(Duration.ofSeconds(5))
                        .withTimeout(Duration.ofSeconds(3)))
                .withDistributionMode(DistributionMode.UPDATE)
                .withAbsentCaching(Duration.ofSeconds(10))
                .withBuildTimeout(Duration.ofSeconds(5));

        assertThat(builder.store().maximumAge()).isEqualTo(Duration.ofHours(1));
        assertThat(builder.store().maximumSize()).isEqualTo(100);
        assertThat(builder.store().maintenanceInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(builder.store().heartbeatInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(builder.store().timeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(builder.distributionMode()).isEqualTo(DistributionMode.UPDATE);
        assertThat(builder.absentCachingExpiry()).isEqualTo(Duration.ofSeconds(10));
        assertThat(builder.buildTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(builder.caffeine().toString()).contains("maximumSize=10");
    }

    @Test
    void defaults() {
        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter);

        assertThat(builder.store().maximumSize()).isEqualTo(-1);
        assertThat(builder.store().maintenanceInterval()).isEqualTo(Duration.ofMinutes(1));
        assertThat(builder.store().heartbeatInterval()).isEqualTo(Duration.ofSeconds(10));
        assertThat(builder.store().timeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(builder.distributionMode()).isEqualTo(DistributionMode.INVALIDATE);
        assertThat(builder.absentCachingExpiry()).isNull();
        assertThat(builder.buildTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(builder.storeExecutor()).isNull();
        assertThat(builder.hashProvider()).isNull();
    }

    @Test
    void the_maximum_age_and_size_are_required() {
        DistributedCaffeine<String, String> unbounded = DistributedCaffeine.newBuilder(adapter);
        DistributedCaffeine<String, String> withoutSize = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)));
        DistributedCaffeine<String, String> withoutAge = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumSize(10));

        assertThatThrownBy(unbounded::build).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("withMaximumAge").hasMessageContaining("withMaximumSize");
        assertThatThrownBy(withoutSize::build).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(withoutAge::build).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void refresh_requires_a_loading_cache() {
        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter)
                .withCaffeine(caffeine -> caffeine.refreshAfterWrite(1, TimeUnit.MINUTES))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100));

        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class).hasMessageContaining("refresh");
        assertThatThrownBy(builder::buildAsync).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void invalid_settings_are_rejected_at_once() {
        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter);

        assertThatThrownBy(() -> builder.withStore(store -> store.withMaximumAge(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.withStore(store -> store.withMaximumSize(0)))
                .isInstanceOf(IllegalArgumentException.class);
        Duration negative = Duration.ofSeconds(-1);
        assertThatThrownBy(() -> builder.withAbsentCaching(negative))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.withBuildTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.withStore(store -> store.withMaximumAge(Duration.ofMinutes(1))
                .withMaximumAge(Duration.ofMinutes(2)))).isInstanceOf(IllegalStateException.class);
        DistributedCaffeine<String, String> withProvider = builder.withHashProvider((key, hasher) -> "a");
        assertThatThrownBy(() -> withProvider.withHashProvider((key, hasher) -> "b"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void every_variant_builds() throws Exception {
        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100));

        try (DistributedCache<String, String> cache = builder.build()) {
            assertThat(cache).isNotInstanceOf(DistributedLoadingCache.class);
        }
        try (DistributedLoadingCache<String, String> cache = builder.build(key -> "v")) {
            assertThat(cache.get("k")).isEqualTo("v");
        }
        try (DistributedAsyncCache<String, String> cache = builder.buildAsync()) {
            assertThat(cache).isNotInstanceOf(DistributedAsyncLoadingCache.class);
        }
        try (DistributedAsyncLoadingCache<String, String> cache = builder.buildAsync(key -> "v")) {
            assertThat(cache.get("k").get()).isEqualTo("v");
        }
        try (DistributedAsyncLoadingCache<String, String> cache = builder.buildAsync(
                (key, executor) -> CompletableFuture.completedFuture("async"))) {
            assertThat(cache.get("k").get()).isEqualTo("v");
            assertThat(cache.get("other").get()).isEqualTo("async");
        }
    }

    // an adapter for a store without notifications: storage only, so no distribution

    @Test
    void a_store_without_a_change_feed_serves_a_cache_without_distribution() {
        // an adapter per cache
        try (DistributedCache<String, String> first = storageOnly(
                settings -> new StorageOnlyStore(database.openStore("cache")), DistributionMode.NONE)
                .build();
             DistributedCache<String, String> second = storageOnly(
                     settings -> new StorageOnlyStore(database.openStore("cache")),
                     DistributionMode.NONE).build()) {
            first.put("k", "v");

            assertThat(second.getIfPresent("k")).as("through the store").isEqualTo("v");
        }
    }

    @Test
    void a_store_without_a_change_feed_fails_a_build_with_distribution_and_is_closed() {
        AtomicBoolean closed = new AtomicBoolean();
        Adapter<String, String> storageOnly = settings ->
                new StorageOnlyStore(database.openStore("cache")) {
                    @Override
                    public void close() {
                        closed.set(true);
                        super.close();
                    }
                };

        for (DistributionMode mode : new DistributionMode[]{DistributionMode.INVALIDATE, DistributionMode.UPDATE}) {
            closed.set(false);
            DistributedCaffeine<String, String> builder = storageOnly(storageOnly, mode);

            assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no change feed").hasMessageContaining("DistributionMode.NONE");
            assertThat(closed).isTrue();
        }
    }

    private static DistributedCaffeine<String, String> storageOnly(Adapter<String, String> adapter,
                                                                    DistributionMode mode) {
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100))
                .withDistributionMode(mode);
    }

    // the store's settings are the cache's: the adapter receives them

    @Test
    void the_store_is_created_with_the_formats_and_the_timeout_the_cache_configured() {
        AtomicReference<StoreSettings> received = new AtomicReference<>();
        Adapter<String, String> recording = settings -> {
            received.set(settings);
            return database.openStore("cache");
        };
        try (DistributedCache<String, String> ignored = DistributedCaffeine.newBuilder(recording)
                // keys as text, values with Fory (the default), as bytes
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100)
                        .withTimeout(Duration.ofSeconds(7)))
                .build()) {
            assertThat(received.get()).isEqualTo(new StoreSettings(Format.TEXT, Format.BINARY, Duration.ofSeconds(7)));
        }
    }

    @Test
    void the_timeout_must_be_positive() {
        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter);

        assertThatThrownBy(() -> builder.withStore(store -> store.withTimeout(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new StoreSettings(Format.TEXT, Format.TEXT, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
