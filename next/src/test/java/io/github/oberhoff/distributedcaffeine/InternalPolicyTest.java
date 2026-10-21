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

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Policy;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code policy()}: L1's policy translated to the user's keys and values, local to the instance; the writes of
 * {@code VarExpiration} go through L2.
 */
@Timeout(30)
class InternalPolicyTest {

    private static final Duration LONG = Duration.ofHours(1);

    private final InMemoryDatabase database = new InMemoryDatabase();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private DistributedCaffeine<String, String> builder() {
        Adapter<String, String> adapter = settings -> database.openStore("cache");
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000));
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    @Test
    void quiet_reads_are_local_and_unwrapped() {
        DistributedCache<String, String> first = track(builder().withCaffeine(CaffeineConfigurer::recordStats)
                .build());
        DistributedCache<String, String> second = track(builder().build());
        first.put("k", "v");

        assertThat(first.policy().isRecordingStats()).isTrue();
        assertThat(first.policy().getIfPresentQuietly("k")).isEqualTo("v");
        assertThat(first.stats().requestCount()).as("quietly: no stats").isZero();
        assertThat(second.policy().getIfPresentQuietly("k")).as("quietly: no store read").isNull();

        Policy.CacheEntry<String, String> entry = first.policy().getEntryIfPresentQuietly("k");
        assertThat(entry).isNotNull();
        assertThat(entry.getKey()).isEqualTo("k");
        assertThat(entry.getValue()).isEqualTo("v");
        assertThat(entry).isEqualTo(Map.entry("k", "v"));
        assertThat(first.policy().getEntryIfPresentQuietly("missing")).isNull();
    }

    @Test
    void eviction_translates_keys_and_values_and_sets_the_local_maximum() {
        DistributedCache<String, String> cache = track(builder().withCaffeine(caffeine -> caffeine.maximumSize(10))
                .build());
        cache.putAll(Map.of("a", "1", "b", "2", "c", "3"));
        cache.cleanUp();

        Policy.Eviction<String, String> eviction = cache.policy().eviction().orElseThrow();
        assertThat(eviction.isWeighted()).isFalse();
        assertThat(eviction.getMaximum()).isEqualTo(10);
        assertThat(eviction.coldest(10)).containsOnlyKeys("a", "b", "c").containsEntry("a", "1");
        assertThat(eviction.hottest(2)).hasSize(2);
        Set<String> coldest = eviction.coldest(stream -> stream.map(Map.Entry::getKey).collect(Collectors.toSet()));
        assertThat(coldest)
                .containsExactlyInAnyOrder("a", "b", "c");

        eviction.setMaximum(1);
        cache.cleanUp();
        assertThat(cache.estimatedSize()).isEqualTo(1);
        assertThat(cache.distributedPolicy().getAllFromStore(List.of("a", "b", "c"))).as("L1 only").hasSize(3);
    }

    @Test
    void weights_are_reported_per_key() {
        DistributedCache<String, String> cache = track(builder().withCaffeine(caffeine -> caffeine
                .maximumWeight(100).weigher((String key, String value) -> value.length())).build());
        cache.put("k", "four");
        cache.cleanUp();

        Policy.Eviction<String, String> eviction = cache.policy().eviction().orElseThrow();
        assertThat(eviction.isWeighted()).isTrue();
        assertThat(eviction.weightOf("k")).hasValue(4);
        assertThat(eviction.weightedSize()).hasValue(4);
        assertThat(eviction.coldestWeighted(10)).containsOnlyKeys("k");
    }

    @Test
    void fixed_expiration_reports_ages_and_changes_the_local_duration() {
        DistributedCache<String, String> cache = track(builder().withCaffeine(caffeine -> caffeine
                .expireAfterWrite(Duration.ofMinutes(5))).build());
        cache.put("k", "v");

        Policy.FixedExpiration<String, String> expiration = cache.policy().expireAfterWrite().orElseThrow();
        assertThat(expiration.getExpiresAfter()).isEqualTo(Duration.ofMinutes(5));
        assertThat(expiration.ageOf("k")).isPresent();
        assertThat(expiration.ageOf("missing")).isEmpty();
        assertThat(expiration.oldest(1)).containsOnlyKeys("k");
        long youngest = expiration.youngest(Stream::count);
        assertThat(youngest).isEqualTo(1);

        expiration.setExpiresAfter(Duration.ofMinutes(1));
        assertThat(expiration.getExpiresAfter()).isEqualTo(Duration.ofMinutes(1));
        assertThat(cache.policy().expireAfterAccess()).isEmpty();
        assertThat(cache.policy().expireVariably()).isEmpty();
    }

    @Test
    void variable_expiration_writes_through_the_store_with_a_local_duration() {
        DistributedCache<String, String> first = track(builder().withCaffeine(caffeine -> caffeine
                .expireAfter(Expiry.writing((String key, String value) -> Duration.ofMinutes(5)))).build());
        DistributedCache<String, String> second = track(builder().build());

        Policy.VarExpiration<String, String> expiration = first.policy().expireVariably().orElseThrow();
        assertThat(expiration.put("k", "v", Duration.ofMinutes(2))).isNull();
        assertThat(second.getIfPresent("k")).isEqualTo("v");
        assertThat(expiration.getExpiresAfter("k").orElseThrow()).isLessThanOrEqualTo(Duration.ofMinutes(2))
                .isGreaterThan(Duration.ofMinutes(1));

        assertThat(expiration.putIfAbsent("k", "other", Duration.ofMinutes(1))).isEqualTo("v");
        assertThat(expiration.compute("k", (key, current) -> current + "!", Duration.ofMinutes(3)))
                .isEqualTo("v!");
        assertThat(requireNonNull(first.distributedPolicy().getFromStore("k")).value()).isEqualTo("v!");
        assertThat(expiration.getExpiresAfter("k").orElseThrow()).isGreaterThan(Duration.ofMinutes(2));
        assertThat(expiration.oldest(5)).containsOnlyKeys("k");
    }

    @Test
    void refresh_after_write_follows_a_changed_duration() {
        DistributedLoadingCache<String, String> cache = track(builder().withCaffeine(caffeine -> caffeine
                .refreshAfterWrite(Duration.ofMinutes(5))).build(key -> "v"));
        assertThat(cache.get("k")).isEqualTo("v");

        Policy.FixedRefresh<String, String> refresh = cache.policy().refreshAfterWrite().orElseThrow();
        assertThat(refresh.getRefreshesAfter()).isEqualTo(Duration.ofMinutes(5));
        assertThat(refresh.ageOf("k")).isPresent();

        refresh.setRefreshesAfter(Duration.ofMinutes(1));
        assertThat(refresh.getRefreshesAfter()).isEqualTo(Duration.ofMinutes(1));
        InternalCore<String, String> core = ((InternalDistributedLoadingCache<String, String>) cache).core;
        assertThat(core.refreshAfterWrite()).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void refreshes_in_flight_are_unwrapped() throws Exception {
        CountDownLatch reloading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DistributedLoadingCache<String, String> cache = track(builder().build(new CacheLoader<>() {
            @Override
            public String load(String key) {
                return "v1";
            }

            @Override
            public String reload(String key, String oldValue) throws InterruptedException {
                reloading.countDown();
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                return "v2";
            }
        }));
        assertThat(cache.get("k")).isEqualTo("v1");
        cache.refresh("k");
        assertThat(reloading.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            Map<String, CompletableFuture<String>> refreshes = cache.policy().refreshes();
            assertThat(refreshes).containsOnlyKeys("k");
            release.countDown();
            assertThat(refreshes.get("k").get(5, TimeUnit.SECONDS)).isEqualTo("v2");
        } finally {
            release.countDown();
        }
    }
}
