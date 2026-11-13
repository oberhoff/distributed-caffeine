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

import com.github.benmanes.caffeine.cache.Ticker;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.LogCapture;
import io.github.oberhoff.distributedcaffeine.testing.StorageOnlyStore;
import io.github.oberhoff.distributedcaffeine.testing.StoreProfile;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A cache built while its store cannot be reached (VISION.md, "Starting without the store"): it starts as during an
 * outage, connects in the background, and then holds only what the store holds; {@code withFailFast()} throws instead.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(60)
@SuppressWarnings("resource") // every cache a test opens is closed after it (@AfterEach)
class InternalStartWithoutStoreTest {

    private final List<AutoCloseable> closeables = new ArrayList<>();
    // whether the restarted instance reaches the store: the others always do (a partial partition, the harder case)
    private final AtomicBoolean reachable = new AtomicBoolean();
    private final AtomicInteger attempts = new AtomicInteger();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    // the store as the restarted instance's client sees it: refused (retryable) until reachable
    private Adapter<String, String> restarted(InMemoryDatabase database) {
        return restarted(database::openStore);
    }

    private Adapter<String, String> restarted(Function<String, Store> open) {
        return settings -> {
            attempts.incrementAndGet();
            if (!reachable.get()) {
                throw new StoreException("Connection refused", null, false, true);
            }
            return open.apply("cache");
        };
    }

    private DistributedCaffeine<String, String> builder(Adapter<String, String> adapter) {
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1))
                        .withPollInterval(Duration.ofMillis(10)));
    }

    private DistributedLoadingCache<String, String> other(InMemoryDatabase database) {
        Adapter<String, String> adapter = settings -> database.openStore("cache");
        return track(builder(adapter).build(key -> "source"));
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    @Test
    void a_cache_built_without_its_store_starts_degraded_loading_from_the_source_and_refusing_writes() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (LogCapture log = new LogCapture()) {
            DistributedLoadingCache<String, String> cache = track(builder(restarted(database)).build(key -> "source"));

            assertThat(cache.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);
            assertThat(cache.get("k")).isEqualTo("source");
            assertThatThrownBy(() -> cache.put("k", "written")).isInstanceOf(StoreException.class);
            assertThat(log.count("The store is not available (Connection refused); the cache starts without it"))
                    .as(log.text()).isEqualTo(1);
            assertThat(log.count("kept until it is reached")).as("bounded by the maximum age").isZero();
            assertThat(log.count("Resuming the change feed failed")).as("quiet before the connect").isZero();
        }
    }

    @Test
    void once_connected_it_holds_what_the_store_holds_and_receives_the_others_writes() {
        connectsAndDistributes(new InMemoryDatabase());
    }

    // a store with a scan only: the connect polls it, as build() would have
    @Test
    void a_polled_store_is_polled_once_connected() {
        connectsAndDistributes(new InMemoryDatabase(StoreProfile.JDBC));
    }

    private void connectsAndDistributes(InMemoryDatabase database) {
        DistributedLoadingCache<String, String> other = other(database);
        other.put("k", "stored");
        try (LogCapture log = new LogCapture()) {
            DistributedLoadingCache<String, String> cache = track(builder(restarted(database)).build(key -> "source"));
            assertThat(cache.get("k")).as("the store not reached: the source").isEqualTo("source");

            reachable.set(true);
            await(() -> cache.distributedPolicy().getSynchronizationState() == SynchronizationState.SYNCHRONIZED);
            await(() -> "stored".equals(cache.get("k")));

            other.put("k", "newer");
            await(() -> "newer".equals(cache.getIfPresent("k")));
            cache.put("k", "mine");
            await(() -> "mine".equals(other.getIfPresent("k")));
            assertThat(log.count("The store was reached; the cache started without it is connected")).as(log.text())
                    .isEqualTo(1);
        }
    }

    @Test
    void with_fail_fast_the_build_throws_and_leaves_the_adapter_free() {
        InMemoryDatabase database = new InMemoryDatabase();
        Adapter<String, String> adapter = restarted(database);

        DistributedCaffeine<String, String> failingFast = builder(adapter).withStore(StoreConfigurer::withFailFast);
        assertThatThrownBy(() -> failingFast.build(key -> "source"))
                .isInstanceOf(StoreException.class).hasMessageContaining("Connection refused");

        reachable.set(true);
        DistributedLoadingCache<String, String> cache = track(builder(adapter).withStore(StoreConfigurer::withFailFast)
                .build(key -> "source"));
        assertThat(cache.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);
    }

    // a store that answered but cannot be used is a configuration error: never a start without it
    @Test
    void a_failure_that_is_not_retryable_still_fails_the_build() {
        Adapter<String, String> adapter = settings -> {
            throw new StoreException("Table cache was created by another version", false);
        };

        DistributedCaffeine<String, String> builder = builder(adapter);
        assertThatThrownBy(() -> builder.build(key -> "source")).isInstanceOf(StoreException.class)
                .hasMessageContaining("another version");
    }

    // found only once the store answers: logged as an error, the cache stays DEGRADED, the store created is closed
    @Test
    void a_store_unusable_as_configured_found_at_the_connect_leaves_the_cache_degraded() {
        InMemoryDatabase database = new InMemoryDatabase();
        AtomicInteger closed = new AtomicInteger();
        Adapter<String, String> adapter = restarted(table -> new StorageOnlyStore(database.openStore(table)) {
            @Override
            public void close() {
                closed.incrementAndGet();
                super.close();
            }
        });
        try (LogCapture log = new LogCapture()) {
            DistributedLoadingCache<String, String> cache = track(builder(adapter).build(key -> "source"));
            reachable.set(true);

            await(() -> log.count("The store was reached, but the cache cannot use it") > 0);
            assertThat(log.text()).contains("offers no change feed and no change scan");
            await(() -> closed.get() == 1);
            assertThat(cache.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);
            assertThat(cache.get("k")).isEqualTo("source");
        }
    }

    @Test
    void a_cache_closed_before_its_store_was_reached_stops_connecting() {
        InMemoryDatabase database = new InMemoryDatabase();
        DistributedLoadingCache<String, String> cache = builder(restarted(database)).build(key -> "source");
        await(() -> attempts.get() >= 2); // build() and one attempt in the background
        cache.close();
        pause(100); // an attempt running at the close ends meanwhile
        int made = attempts.get();

        reachable.set(true);
        pause(1_500);

        assertThat(attempts.get()).isEqualTo(made);
        assertThat(cache.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.CLOSED);
        // the adapter is free again
        track(builder(restarted(database)).build(key -> "source"));
    }

    @Test
    void without_a_maximum_age_or_an_expiry_the_warning_says_the_values_are_kept() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (LogCapture log = new LogCapture()) {
            track(DistributedCaffeine.newBuilder(restarted(database))
                    .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                            .withValueSerializer(StringSerializer.ofString()))
                    .build(key -> "source"));

            assertThat(log.count("the values loaded meanwhile are kept until it is reached")).as(log.text())
                    .isEqualTo(1);
        }
    }

    // with the maximum age for the store only, a copy no store confirmed still expires at the maximum age; the others
    // do not (InternalExpiry#unconfirmedMaximumAge)
    @Test
    void with_the_maximum_age_for_the_store_only_unconfirmed_copies_expire_at_the_maximum_age() {
        Duration age = Duration.ofMinutes(1);
        InternalExpiry<String, String> expiry = new InternalExpiry<>(StoreConfigurer.UNBOUNDED_AGE, age,
                Ticker.systemTicker(), Long.MAX_VALUE, InternalExpiry.NONE);
        InternalKey<String> key = new InternalHasher<String>(null).wrap("k");

        long unconfirmed = expiry.expireAfterCreate(key, new InternalValue<>("v", InternalVersion.UNCONFIRMED, -1), 0);
        long confirmed = expiry.expireAfterCreate(key, new InternalValue<>("v", 5, 0), 0);

        assertThat(unconfirmed).isEqualTo(age.toNanos());
        assertThat(confirmed).isEqualTo(Long.MAX_VALUE);

        expiry.unconfirmedMaximumAge(Duration.ofSeconds(30).toNanos());
        assertThat(expiry.expireAfterCreate(key, new InternalValue<>("v", InternalVersion.UNCONFIRMED, -1), 0))
                .as("set at runtime").isEqualTo(Duration.ofSeconds(30).toNanos());
    }
}
