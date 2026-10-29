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
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.ConcurrentStatsCounter;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class CaffeineConfigurerTest {

    // the store's maximum age the core builds with: above every expiry here, so that none is lowered
    private static final Duration MAXIMUM_AGE = Duration.ofHours(1);

    private static InternalKey<String> key(String key) {
        return new InternalHasher<String>(null).wrap(key);
    }

    private static InternalValue<String> value(String value) {
        return new InternalValue<>(value, 1L, 0);
    }

    @Test
    void the_weigher_sees_the_users_key_and_value() {
        CaffeineConfigurer<String, String> configurer = new CaffeineConfigurer<String, String>()
                .maximumWeight(100)
                .weigher((String key, String value) -> key.length() + value.length());
        Cache<InternalKey<String>, InternalValue<String>> cache = configurer.caffeine(MAXIMUM_AGE).build();

        cache.put(key("abc"), value("defgh"));
        cache.cleanUp();

        assertThat(cache.policy().eviction().orElseThrow().weightedSize().orElseThrow()).isEqualTo(8);
    }

    @Test
    void the_listeners_see_the_users_key_value_and_cause() {
        List<String> removed = new CopyOnWriteArrayList<>();
        CaffeineConfigurer<String, String> configurer = new CaffeineConfigurer<String, String>()
                .executor(Runnable::run)
                .removalListener((String key, String value, RemovalCause cause) -> removed.add(key + "=" + value + ":" + cause))
                .evictionListener((String key, String value, RemovalCause cause) -> removed.add("evicted " + key));
        Cache<InternalKey<String>, InternalValue<String>> cache = configurer.caffeine(MAXIMUM_AGE).build();

        cache.put(key("a"), value("1"));
        cache.put(key("a"), value("2"));
        cache.invalidate(key("a"));

        assertThat(removed).containsExactly("a=1:REPLACED", "a=2:EXPLICIT");
    }

    @Test
    void the_expiry_sees_the_users_key_and_value() {
        List<String> seen = new CopyOnWriteArrayList<>();
        CaffeineConfigurer<String, String> configurer = new CaffeineConfigurer<String, String>()
                .expireAfter(new Expiry<>() {
                    @Override
                    public long expireAfterCreate(String key, String value, long currentTime) {
                        seen.add("create " + key + "=" + value);
                        return TimeUnit.MINUTES.toNanos(1);
                    }

                    @Override
                    public long expireAfterUpdate(String key, String value, long currentTime, long currentDuration) {
                        seen.add("update " + key + "=" + value);
                        return currentDuration;
                    }

                    @Override
                    public long expireAfterRead(String key, String value, long currentTime, long currentDuration) {
                        seen.add("read " + key + "=" + value);
                        return currentDuration;
                    }
                });
        Cache<InternalKey<String>, InternalValue<String>> cache = configurer.caffeine(MAXIMUM_AGE).build();

        cache.put(key("a"), value("1"));
        cache.put(key("a"), value("2"));
        assertThat(requireNonNull(cache.getIfPresent(key("a"))).value()).isEqualTo("2");

        assertThat(seen).containsExactly("create a=1", "update a=2", "read a=2");
    }

    @Test
    void the_capacity_and_the_time_unit_forms_are_passed_to_caffeine() {
        CaffeineConfigurer<String, String> configurer = new CaffeineConfigurer<String, String>()
                .initialCapacity(64)
                .expireAfterWrite(2, TimeUnit.MINUTES)
                .expireAfterAccess(3, TimeUnit.MINUTES);

        assertThat(configurer.caffeine(MAXIMUM_AGE).toString()).contains("initialCapacity=64",
                "expireAfterWrite=" + TimeUnit.MINUTES.toNanos(2) + "ns",
                "expireAfterAccess=" + TimeUnit.MINUTES.toNanos(3) + "ns");
    }

    @Test
    void a_stats_counter_supplied_records_caffeines_stats() {
        StatsCounter counter = new ConcurrentStatsCounter();
        Cache<InternalKey<String>, InternalValue<String>> cache = new CaffeineConfigurer<String, String>()
                .recordStats(() -> counter)
                .caffeine(MAXIMUM_AGE).build();
        CaffeineConfigurer.takeCreatedStatsCounter(); // as the core does after building

        assertThat(cache.getIfPresent(key("a"))).isNull();

        assertThat(counter.snapshot().missCount()).isEqualTo(1);
    }

    @Test
    void the_scheduler_is_passed_to_caffeine() {
        AtomicInteger scheduled = new AtomicInteger();
        Cache<InternalKey<String>, InternalValue<String>> cache = new CaffeineConfigurer<String, String>()
                .scheduler((executor, command, delay, unit) -> {
                    scheduled.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                })
                .expireAfterWrite(Duration.ofMinutes(1))
                .caffeine(MAXIMUM_AGE).build();

        cache.put(key("a"), value("1"));
        cache.cleanUp();

        assertThat(scheduled).as("the next expiry is scheduled with it").hasPositiveValue();
    }

    @Test
    void caffeines_own_validation_applies() {
        CaffeineConfigurer<String, String> sized = new CaffeineConfigurer<String, String>().maximumSize(1);
        CaffeineConfigurer<String, String> fresh = new CaffeineConfigurer<>();
        CaffeineConfigurer<String, String> expiring = new CaffeineConfigurer<String, String>()
                .expireAfterWrite(Duration.ofMinutes(1));
        Expiry<String, String> expiry = new Expiry<>() {
            @Override
            public long expireAfterCreate(String key, String value, long currentTime) {
                return 0;
            }

            @Override
            public long expireAfterUpdate(String key, String value, long currentTime, long currentDuration) {
                return 0;
            }

            @Override
            public long expireAfterRead(String key, String value, long currentTime, long currentDuration) {
                return 0;
            }
        };

        assertThatThrownBy(() -> sized.maximumSize(2)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> sized.maximumWeight(2)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> fresh.maximumSize(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> expiring.expireAfter(expiry)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void weak_and_soft_references_are_rejected_with_a_clear_message() {
        CaffeineConfigurer<String, String> configurer = new CaffeineConfigurer<>();

        assertThatThrownBy(configurer::weakKeys).isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("hash");
        assertThatThrownBy(configurer::weakValues).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(configurer::softValues).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void stats_and_refresh_are_tracked_for_the_core() {
        CaffeineConfigurer<String, String> plain = new CaffeineConfigurer<>();
        CaffeineConfigurer<String, String> configured = new CaffeineConfigurer<String, String>()
                .recordStats()
                .refreshAfterWrite(1, TimeUnit.MINUTES);

        assertThat(plain.isRecordingStats()).isFalse();
        assertThat(plain.isRefreshing()).isFalse();
        assertThat(configured.isRecordingStats()).isTrue();
        assertThat(configured.isRefreshing()).isTrue();
    }

    @Test
    void settings_reach_the_local_level() {
        Cache<InternalKey<String>, InternalValue<String>> cache = new CaffeineConfigurer<String, String>()
                .maximumSize(5)
                .expireAfterAccess(Duration.ofMinutes(2))
                .recordStats()
                .caffeine(MAXIMUM_AGE).build();

        assertThat(cache.policy().eviction().orElseThrow().getMaximum()).isEqualTo(5);
        assertThat(cache.policy().expireAfterAccess().orElseThrow().getExpiresAfter()).isEqualTo(Duration.ofMinutes(2));
        assertThat(cache.policy().isRecordingStats()).isTrue();
    }
}
