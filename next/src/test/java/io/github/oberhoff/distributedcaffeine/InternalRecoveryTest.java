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
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import java.nio.charset.StandardCharsets;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Assumptions;
import io.github.oberhoff.distributedcaffeine.testing.StoreProfile;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Revalidation after gaps, the subscription interrupted, the store down (store guard), and maintenance.
 */
@NullMarked
@Timeout(60)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
public class InternalRecoveryTest {

    private static final Duration LONG = Duration.ofHours(1);

    private final CacheBackend backend = backend();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicInteger versionReads = new AtomicInteger();
    private final AtomicInteger storeCalls = new AtomicInteger();

    // the store under test: the in-memory store here, a real database in the subclasses
    protected CacheBackend backend() {
        return CacheBackend.of(new InMemoryDatabase());
    }

    @AfterEach
    void tearDown() throws Exception {
        InMemoryDatabase database = backend.inMemory();
        if (database != null) {
            database.setDown(false);
        }
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        backend.close();
    }

    // a cache whose store calls are counted
    private DistributedCaffeine<String, String> counted(Duration maximumAge, long maximumSize, Duration maintenance) {
        Adapter<String, String> adapter = settings -> new InternalCoreTest.DelegatingStore(
                backend.open(settings)) {
            @Override
            public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
                reads.incrementAndGet();
                storeCalls.incrementAndGet();
                return super.read(hashes, maxAge);
            }

            @Override
            public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
                versionReads.incrementAndGet();
                storeCalls.incrementAndGet();
                return super.readVersions(hashes, maxAge);
            }

            @Override
            public io.github.oberhoff.distributedcaffeine.spi.WriteResult write(
                    Collection<io.github.oberhoff.distributedcaffeine.spi.Write> writes) {
                storeCalls.incrementAndGet();
                return super.write(writes);
            }
        };
        return DistributedCaffeine.newBuilder(InternalCoreTest.offered(adapter))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaximumAge(maximumAge).withMaximumSize(maximumSize)
                        .withMaintenanceInterval(maintenance));
    }

    private DistributedCache<String, String> holder() {
        return track(counted(LONG, 1_000, LONG).build());
    }

    private DistributedCache<String, String> other() {
        Adapter<String, String> adapter = backend::open;
        return track(DistributedCaffeine.newBuilder(InternalCoreTest.offered(adapter))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaximumAge(LONG).withMaximumSize(1_000).withMaintenanceInterval(LONG))
                .build());
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private static InternalCore<String, String> core(DistributedCache<String, String> cache) {
        return ((InternalDistributedCache<String, String>) cache).core;
    }

    private static @Nullable String held(DistributedCache<String, String> cache, String key) {
        InternalCore<String, String> core = core(cache);
        CompletableFuture<InternalValue<String>> future = core.l1().asMap().get(core.wrap(key));
        InternalValue<String> value = future == null || !future.isDone() ? null : future.join();
        return value == null ? null : value.value();
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 10 s");
            }
            pause(5);
        }
    }

    // revalidation after a gap

    @Test
    void after_a_gap_an_unchanged_held_value_is_kept_by_a_version_read_only() {
        backend.requireFeed();
        DistributedCache<String, String> cache = holder();
        cache.put("k", "v");
        backend.induceGap();
        pause(100);
        await(() -> versionReads.get() > 0); // the background pass
        int valueReads = reads.get();

        assertThat(cache.getIfPresent("k")).isEqualTo("v");
        assertThat(cache.getIfPresent("k")).isEqualTo("v");

        assertThat(reads.get()).as("no value was read again").isEqualTo(valueReads);
        assertThat(held(cache, "k")).isEqualTo("v");
    }

    // the heartbeat: a feed that stops delivering without saying so

    private DistributedCache<String, String> beating(Duration interval) {
        Adapter<String, String> adapter = backend::open;
        return track(DistributedCaffeine.newBuilder(InternalCoreTest.offered(adapter))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaximumAge(LONG).withMaximumSize(1_000).withMaintenanceInterval(LONG)
                        .withHeartbeatInterval(interval))
                .withCaffeine(CaffeineConfigurer::recordStats)
                .build());
    }

    @Test
    void a_silent_feed_is_noticed_by_the_heartbeat_and_nothing_held_is_trusted_until_it_delivers_again() {
        backend.requireFeed();
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        DistributedCache<String, String> cache = beating(Duration.ofMillis(200));
        DistributedCache<String, String> other = other();
        other.put("k", "old");
        assertThat(cache.getIfPresent("k")).isEqualTo("old");
        pause(600); // beats delivered: no silence
        assertThat(cache.distributedPolicy().stats().interruptionCount()).isZero();
        assertThat(cache.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);

        // delivery stops without any error: the change is not announced, the copy would be served for good
        faults.pauseDelivery();
        other.put("k", "new");

        await(() -> cache.distributedPolicy().stats().interruptionCount() == 1);
        assertThat(cache.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);
        assertThat(cache.getIfPresent("k")).as("checked against the store").isEqualTo("new");

        // delivering again: the next beat ends the silence with a gap (after those of the resumes while silent)
        long gaps = cache.distributedPolicy().stats().gapCount();
        faults.resumeDelivery();
        await(() -> cache.distributedPolicy().stats().gapCount() > gaps);
        await(() -> cache.distributedPolicy().getSynchronizationState() == SynchronizationState.SYNCHRONIZED);
        assertThat(cache.distributedPolicy().stats().interruptionCount()).isEqualTo(1);
        assertThat(cache.distributedPolicy().stats().invalidatedMessageCount()).as("beats are not counted").isZero();
    }

    @Test
    void the_reserved_hash_of_the_heartbeat_is_refused_as_a_key_hash() {
        Adapter<String, String> adapter = backend::open;
        DistributedCache<String, String> cache = track(DistributedCaffeine.newBuilder(InternalCoreTest.offered(adapter))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaximumAge(LONG).withMaximumSize(1_000))
                .withHashProvider((key, hasher) -> key)
                .build());

        assertThatThrownBy(() -> cache.put(InternalHeartbeat.HASH, "v")).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reserved");
        cache.put("other", "v");
        assertThat(cache.getIfPresent("other")).isEqualTo("v");
    }

    @Test
    void after_a_gap_a_change_whose_message_was_lost_is_found_on_read() {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        DistributedCache<String, String> cache = holder();
        DistributedCache<String, String> other = other();
        other.put("k", "old");
        assertThat(cache.getIfPresent("k")).isEqualTo("old");
        faults.pauseDelivery();
        other.put("k", "new");

        faults.gap();
        faults.resumeDelivery();

        await(() -> "new".equals(cache.getIfPresent("k")));
    }

    @Test
    void after_a_gap_the_background_pass_drops_what_changed_without_any_read() {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        DistributedCache<String, String> cache = holder();
        DistributedCache<String, String> other = other();
        for (int i = 0; i < 50; i++) {
            other.put("k" + i, "old");
        }
        for (int i = 0; i < 50; i++) {
            assertThat(cache.getIfPresent("k" + i)).isEqualTo("old");
        }
        faults.pauseDelivery();
        for (int i = 0; i < 25; i++) {
            other.put("k" + i, "new");
        }

        faults.gap();
        faults.resumeDelivery();

        await(() -> held(cache, "k0") == null && held(cache, "k24") == null);
        for (int i = 25; i < 50; i++) {
            assertThat(held(cache, "k" + i)).isEqualTo("old");
        }
        assertThat(cache.estimatedSize()).isEqualTo(25);
    }

    // the subscription interrupted, the store answering

    @Test
    void while_the_subscription_is_down_every_access_checks_the_store() {
        backend.requireFeed();
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        DistributedCache<String, String> cache = holder();
        DistributedCache<String, String> other = other();
        other.put("k", "old");
        assertThat(cache.getIfPresent("k")).isEqualTo("old");

        faults.interruptSubscriptions();
        await(() -> cache.distributedPolicy().getSynchronizationState() == SynchronizationState.DEGRADED);
        other.put("k", "new");

        assertThat(cache.getIfPresent("k")).as("checked against the store although no message arrived").isEqualTo("new");
        int checks = versionReads.get();
        assertThat(cache.getIfPresent("k")).isEqualTo("new");
        assertThat(versionReads.get()).as("checked on every access").isGreaterThan(checks);

        faults.resumeSubscriptions(true);
        await(() -> cache.distributedPolicy().getSynchronizationState() == SynchronizationState.SYNCHRONIZED
                && core(cache).isQuiescent());
        int afterResume = versionReads.get();
        assertThat(cache.getIfPresent("k")).isEqualTo("new");
        assertThat(versionReads.get()).as("trusted again after the gap's check, no further check")
                .isEqualTo(afterResume);
    }

    @Test
    void a_resume_is_a_gap_also_when_what_was_missed_is_never_delivered() {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        DistributedCache<String, String> cache = holder();
        DistributedCache<String, String> other = other();
        other.put("k", "old");
        assertThat(cache.getIfPresent("k")).isEqualTo("old");
        faults.interruptSubscriptions();
        other.put("k", "new");

        faults.resumeSubscriptions(false);

        await(() -> cache.distributedPolicy().getSynchronizationState() == SynchronizationState.SYNCHRONIZED);
        await(() -> held(cache, "k") == null || "new".equals(held(cache, "k")));
        assertThat(cache.getIfPresent("k")).isEqualTo("new");
    }

    // the store down

    @Test
    void while_the_store_is_down_hits_are_served_misses_load_from_the_source_and_writes_are_refused() {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        DistributedLoadingCache<String, String> cache = track(counted(LONG, 1_000, LONG).build(key -> "source-" + key));
        cache.put("held", "v");

        faults.setDown(true);
        for (int i = 0; i < InternalStoreGuard.THRESHOLD; i++) {
            assertThat(cache.get("miss" + i)).isEqualTo("source-miss" + i);
        }
        await(() -> cache.distributedPolicy().getSynchronizationState() == SynchronizationState.DEGRADED);

        assertThat(cache.get("held")).isEqualTo("v");
        assertThat(cache.get("another")).isEqualTo("source-another");
        assertThatThrownBy(() -> cache.put("k", "v")).isInstanceOf(StoreException.class);
        assertThatThrownBy(() -> cache.invalidate("held")).isInstanceOf(StoreException.class);
        assertThat(held(cache, "held")).as("dropped locally anyway").isNull();
    }

    @Test
    void a_value_loaded_while_the_store_was_down_is_never_confirmed_by_a_stored_version() {
        Assumptions.assumeTrue(backend.inMemory() != null, "needs a store numbering its own versions");
        // a store with versions of its own (a sequence): its first row is at version 1, which the version of a value
        // no store confirmed must never equal, or a read or a message would confirm the wrong value
        InMemoryDatabase database = new InMemoryDatabase(StoreProfile.POSTGRESQL);
        Adapter<String, String> adapter = settings -> database.openStore("cache");
        DistributedLoadingCache<String, String> cache = track(DistributedCaffeine.newBuilder(InternalCoreTest.offered(adapter))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaximumAge(LONG).withMaximumSize(1_000).withMaintenanceInterval(LONG))
                .build(key -> "source"));
        Store other = track(database.openStore("cache"));
        database.setDown(true);
        assertThat(cache.get("k")).as("from the source alone, unconfirmed").isEqualTo("source");
        database.setDown(false);

        Write stored = Write.value(new InternalHasher<String>(null).wrap("k").hash(),
                "k".getBytes(StandardCharsets.UTF_8), "stored".getBytes(StandardCharsets.UTF_8), 99);
        WriteResult result = other.write(List.of(stored));

        assertThat(result.version(stored)).as("the store's first version").isOne();
        await(() -> "stored".equals(cache.get("k")));
    }

    @Test
    void the_open_guard_refuses_store_calls_without_waiting_for_the_store() {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        DistributedLoadingCache<String, String> cache = track(counted(LONG, 1_000, LONG).build(key -> "source-" + key));
        faults.setDown(true);
        for (int i = 0; i < InternalStoreGuard.THRESHOLD; i++) {
            assertThat(cache.get("miss" + i)).isEqualTo("source-miss" + i);
        }
        int calls = storeCalls.get();

        for (int i = 0; i < 20; i++) {
            assertThat(cache.get("refused" + i)).isEqualTo("source-refused" + i);
        }

        assertThat(storeCalls.get()).as("refused by the guard, the store is not called").isEqualTo(calls);
    }

    @Test
    void when_the_store_is_back_maintenance_closes_the_guard_and_source_only_values_are_replaced() {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        AtomicInteger loads = new AtomicInteger();
        DistributedLoadingCache<String, String> cache = track(counted(LONG, 1_000, Duration.ofMillis(100))
                .build(key -> key + "-" + loads.incrementAndGet()));
        faults.setDown(true);
        for (int i = 0; i < InternalStoreGuard.THRESHOLD; i++) {
            assertThat(cache.get("miss" + i)).isEqualTo("miss" + i + "-" + (i + 1));
        }
        assertThat(cache.get("k")).isEqualTo("k-4");
        assertThat(cache.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);

        faults.setDown(false);

        await(() -> cache.distributedPolicy().getSynchronizationState() == SynchronizationState.SYNCHRONIZED);
        assertThat(cache.get("k")).as("the source-only value is checked and loaded properly").isEqualTo("k-5");
        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).isEqualTo("k-5");
    }

    // maintenance

    @Test
    void maintenance_expires_values_everywhere() {
        DistributedCache<String, String> writer = track(counted(Duration.ofMillis(300), 1_000, Duration.ofMillis(100)).build());
        DistributedCache<String, String> holder = other();
        writer.put("k", "v");
        assertThat(holder.getIfPresent("k")).isEqualTo("v");

        await(() -> held(writer, "k") == null && held(holder, "k") == null);

        Store store = track(backend.open());
        await(() -> {
            Row row = store.read(List.of(new InternalHasher<String>(null).wrap("k").hash()), LONG)
                    .get(new InternalHasher<String>(null).wrap("k").hash());
            return row == null || row.state() == State.EXPIRED;
        });
    }

    @Test
    void maintenance_prunes_the_store_to_its_maximum_size() {
        DistributedCache<String, String> cache = track(counted(LONG, 5, Duration.ofMillis(100)).build());
        for (int i = 0; i < 12; i++) {
            cache.put("k" + i, "v");
            pause(2);
        }

        Store store = track(backend.open());
        List<String> hashes = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            hashes.add(new InternalHasher<String>(null).wrap("k" + i).hash());
        }
        await(() -> store.read(hashes, LONG).size() == 5);
        assertThat(store.read(hashes.subList(7, 12), LONG)).hasSize(5);
    }
}
