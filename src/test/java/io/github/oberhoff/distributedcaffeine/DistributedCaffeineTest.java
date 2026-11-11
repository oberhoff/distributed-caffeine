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
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.StorageOnlyStore;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Andreas Oberhoff
 */
@NullMarked
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
                        .withOperationTimeout(Duration.ofSeconds(3)))
                .withDistributionMode(DistributionMode.UPDATE)
                .withNegativeCaching(Duration.ofSeconds(10));

        assertThat(builder.store().maximumAge()).isEqualTo(Duration.ofHours(1));
        assertThat(builder.store().maximumSize()).isEqualTo(100);
        assertThat(builder.store().maintenanceInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(builder.store().heartbeatInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(builder.store().operationTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(builder.distributionMode()).isEqualTo(DistributionMode.UPDATE);
        assertThat(builder.negativeCachingExpiry()).isEqualTo(Duration.ofSeconds(10));
        assertThat(builder.caffeine().toString()).contains("maximumSize=10");
    }

    @Test
    void the_adapter_is_released_on_close_and_on_a_failed_build() {
        Adapter<String, String> open = settings -> database.openStore("cache");
        DistributedCache<String, String> cache = DistributedCaffeine.newBuilder(open)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100))
                .build();
        assertThat(InternalClaims.isClaimed(open)).isTrue();
        cache.close();
        assertThat(InternalClaims.isClaimed(open)).as("released on close").isFalse();

        Adapter<String, String> failing = settings -> {
            throw new IllegalStateException("cannot open");
        };
        DistributedCaffeine<String, String> failingBuilder = DistributedCaffeine.newBuilder(failing)
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100));
        assertThatThrownBy(failingBuilder::build).isInstanceOf(RuntimeException.class);
        assertThat(InternalClaims.isClaimed(failing)).as("released on a failed build").isFalse();
    }

    @Test
    void closing_empties_the_local_cache_without_notifying_the_removal_listener() {
        java.util.List<String> removed = new java.util.concurrent.CopyOnWriteArrayList<>();
        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter)
                .withCaffeine(caffeine -> caffeine.executor(Runnable::run)
                        .removalListener((key, value, cause) -> removed.add(key + "=" + value + ":" + cause)))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100));
        DistributedCache<String, String> cache = builder.build();
        cache.put("k1", "v1");
        cache.put("k2", "v2");
        cache.invalidate("k1");
        assertThat(removed).containsExactly("k1=v1:EXPLICIT");

        cache.close();

        assertThat(removed).as("nothing reported for the copies close() drops").containsExactly("k1=v1:EXPLICIT");
        // the mark belongs to the dropped copies, not to the builder: a cache built again reports as usual
        try (DistributedCache<String, String> again = builder.build()) {
            assertThat(again.get("k2", key -> "loaded")).isEqualTo("v2");
            again.invalidate("k2");
            assertThat(removed).containsExactly("k1=v1:EXPLICIT", "k2=v2:EXPLICIT");
        }
    }

    @Test
    void defaults() {
        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter);

        assertThat(builder.store().maximumAge()).isEqualTo(StoreConfigurer.UNBOUNDED_AGE);
        assertThat(builder.store().maximumSize()).isEqualTo(StoreConfigurer.UNBOUNDED_SIZE);
        assertThat(builder.store().maintenanceInterval()).isEqualTo(Duration.ofMinutes(1));
        assertThat(builder.store().heartbeatInterval()).isEqualTo(Duration.ofSeconds(10));
        assertThat(builder.store().operationTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(builder.distributionMode()).isEqualTo(DistributionMode.INVALIDATE);
        assertThat(builder.negativeCachingExpiry()).isNull();
        assertThat(builder.store().executor()).isNull();
        assertThat(builder.hashProvider()).isNull();
    }

    @Test
    void the_store_bounds_are_optional_and_an_age_beyond_the_cap_is_taken_as_unbounded() {
        DistributedCaffeine<String, String> forever = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumAge(ChronoUnit.FOREVER.getDuration()));
        DistributedCaffeine<String, String> atTheCap = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumAge(StoreConfigurer.UNBOUNDED_AGE));

        assertThat(forever.store().maximumAge()).isEqualTo(StoreConfigurer.UNBOUNDED_AGE);
        assertThat(atTheCap.store().maximumAge()).isEqualTo(Duration.ofNanos(Long.MAX_VALUE));
        assertThat(StoreConfigurer.UNBOUNDED_AGE.toDays()).as("about 292 years").isEqualTo(106_751);
        DistributedCache<String, String> cache = DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .build();
        cache.put("k", "v");
        assertThat(cache.getIfPresent("k")).isEqualTo("v");
        cache.close();
    }

    @Test
    void the_maximum_age_for_the_store_only_requires_an_age_distribution_and_a_short_heartbeat() {
        DistributedCaffeine<String, String> noAge = DistributedCaffeine.newBuilder(adapter)
                .withStore(StoreConfigurer::withMaximumAgeForStoreOnly);
        assertThatThrownBy(noAge::build).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires withMaximumAge");
        DistributedCaffeine<String, String> none = DistributedCaffeine.newBuilder(adapter)
                .withDistributionMode(DistributionMode.NONE)
                .withStore(store -> store.withMaximumAgeForStoreOnly().withMaximumAge(Duration.ofHours(1)));
        assertThatThrownBy(none::build).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("DistributionMode.NONE");
        DistributedCaffeine<String, String> fifth = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumAge(Duration.ofSeconds(30)).withMaximumAgeForStoreOnly());
        assertThatThrownBy(fifth::build).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("maximum age of at least five heartbeat intervals (PT50S), not PT30S");
        // a shorter heartbeat allows a shorter age; five default intervals are enough
        DistributedCaffeine<String, String> shorter = DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofSeconds(30)).withMaximumAgeForStoreOnly()
                        .withHeartbeatInterval(Duration.ofSeconds(6)));
        shorter.build().close();
        DistributedCaffeine<String, String> exact = DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofSeconds(50)).withMaximumAgeForStoreOnly());
        exact.build().close();
        DistributedCaffeine<String, String> minute = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumAgeForStoreOnly()
                        .withHeartbeatInterval(Duration.ofMinutes(2)));
        assertThatThrownBy(minute::build).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("heartbeat interval of at most PT1M");
    }

    @Test
    void the_maximum_age_for_the_store_only_spares_three_heartbeat_intervals_from_size_pruning() {
        DistributedCaffeine<String, String> storeOnly = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumAgeForStoreOnly()
                        .withHeartbeatInterval(Duration.ofSeconds(5)));
        DistributedCaffeine<String, String> usual = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1))
                        .withHeartbeatInterval(Duration.ofSeconds(5)));

        assertThat(storeOnly.pruneGrace(false)).isEqualTo(Duration.ofSeconds(15));
        assertThat(usual.pruneGrace(false)).isEqualTo(InternalMaintenance.PRUNE_GRACE);
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
        assertThatThrownBy(() -> builder.withNegativeCaching(negative))
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
    void the_store_is_created_with_the_formats_and_the_operation_timeout_the_cache_configured() {
        AtomicReference<@Nullable StoreSettings> received = new AtomicReference<>();
        Adapter<String, String> recording = settings -> {
            received.set(settings);
            return database.openStore("cache");
        };
        try (DistributedCache<String, String> ignored = DistributedCaffeine.newBuilder(recording)
                // keys as text, values with Fory (the default), as bytes
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(100)
                        .withOperationTimeout(Duration.ofSeconds(7)))
                .build()) {
            assertThat(received.get()).isEqualTo(new StoreSettings(Format.TEXT, Format.BINARY, Duration.ofSeconds(7)));
        }
    }

    @Test
    void the_operation_timeout_must_be_positive() {
        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter);

        assertThatThrownBy(() -> builder.withStore(store -> store.withOperationTimeout(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
        Duration negative = Duration.ofSeconds(-1);
        assertThatThrownBy(() -> new StoreSettings(Format.TEXT, Format.TEXT, negative))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
