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
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distribution between instances: messages applied under the version rule, population, loads in flight, and the
 * absent set.
 */
@Timeout(30)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
public class InternalDistributionTest {

    private static final Duration LONG = Duration.ofHours(1);

    private final CacheBackend backend = backend();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        backend.close();
    }

    // the store under test: the in-memory store here, a real database in the subclasses
    protected CacheBackend backend() {
        return CacheBackend.of(new InMemoryDatabase());
    }

    private DistributedCaffeine<String, String> builder(Function<Store, Store> wrapping) {
        Adapter<String, String> adapter = settings -> wrapping.apply(backend.open(settings));
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000));
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private DistributedCache<String, String> plain() {
        return track(builder(Function.identity()).build());
    }

    private DistributedCache<String, String> populated() {
        return track(builder(Function.identity()).withDistributionMode(DistributionMode.UPDATE).build());
    }

    // what L1 holds for the key, without reading through the store
    private static @Nullable String held(DistributedCache<String, String> cache, String key) {
        InternalCore<String, String> core = ((InternalDistributedCache<String, String>) cache).core;
        CompletableFuture<InternalValue<String>> future = core.l1().asMap().get(core.wrap(key));
        InternalValue<String> value = future == null || !future.isDone() ? null : future.join();
        return value == null ? null : value.value();
    }

    private static void settle() {
        pause(300);
    }

    @Test
    void a_write_elsewhere_drops_the_held_older_value() {
        DistributedCache<String, String> first = plain();
        DistributedCache<String, String> second = plain();
        first.put("k", "old");
        assertThat(second.getIfPresent("k")).isEqualTo("old");

        first.put("k", "new");

        await(() -> held(second, "k") == null);
        assertThat(second.getIfPresent("k")).isEqualTo("new");
    }

    @Test
    void with_population_a_write_elsewhere_replaces_the_held_value_without_access() {
        DistributedCache<String, String> first = populated();
        DistributedCache<String, String> second = populated();
        first.put("k", "old");
        assertThat(second.getIfPresent("k")).isEqualTo("old");

        first.put("k", "new");

        await(() -> "new".equals(held(second, "k")));
    }

    @Test
    void a_population_read_never_replaces_a_newer_value_that_landed_meanwhile() throws Exception {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        CountDownLatch populating = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger readsSeen = new AtomicInteger();
        DistributedCache<String, String> holder = track(builder(store -> new InternalCoreTest.DelegatingStore(store) {
            @Override
            public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
                Map<String, Row> rows = super.read(hashes, maxAge);
                if (readsSeen.incrementAndGet() == 2) {
                    // the population read: hold it back after it read the older row
                    populating.countDown();
                    try {
                        assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return rows;
            }
        }).withDistributionMode(DistributionMode.UPDATE).build());
        DistributedCache<String, String> writer = populated();
        writer.put("k", "v1");
        assertThat(holder.getIfPresent("k")).isEqualTo("v1");

        writer.put("k", "v2");
        assertThat(populating.await(5, TimeUnit.SECONDS)).isTrue();
        // the echo of v3 would repair a wrong replacement: keep it back while the population read completes
        faults.pauseDelivery();
        holder.put("k", "v3");
        release.countDown();
        settle();

        assertThat(held(holder, "k")).isEqualTo("v3");
        faults.resumeDelivery();
    }

    // a load that blocks after it called the source, until released; answers what the source held when called
    private static final class BlockedLoad {
        final CountDownLatch called = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicReference<String> source = new java.util.concurrent.atomic.AtomicReference<>();
        final AtomicInteger calls = new AtomicInteger();

        String load() throws InterruptedException {
            String value = source.get();
            if (calls.incrementAndGet() == 1) {
                called.countDown();
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            }
            return value;
        }
    }

    @Test
    void no_absence_is_recorded_by_a_load_whose_future_was_dropped_by_invalidateAll() throws Exception {
        BlockedLoad blocked = new BlockedLoad();
        DistributedLoadingCache<String, String> loader = track(builder(Function.identity())
                .withAbsentCaching(Duration.ofMinutes(1)).build(key -> blocked.load()));
        DistributedCache<String, String> writer = plain();
        CompletableFuture<String> load = CompletableFuture.supplyAsync(() -> loader.get("k"));
        assertThat(blocked.called.await(5, TimeUnit.SECONDS)).isTrue();

        // drops the load's future: the write's message then finds nothing to mark
        loader.invalidateAll();
        writer.put("k", "v");
        settle();
        blocked.release.countDown();

        assertThat(load.get(5, TimeUnit.SECONDS)).isNull();
        assertThat(loader.getIfPresent("k")).as("no absence hides the value").isEqualTo("v");
    }

    @Test
    void no_absence_is_recorded_by_a_load_whose_future_was_dropped_by_an_invalidation() throws Exception {
        BlockedLoad blocked = new BlockedLoad();
        DistributedLoadingCache<String, String> loader = track(builder(Function.identity())
                .withAbsentCaching(Duration.ofMinutes(1)).build(key -> blocked.load()));
        DistributedCache<String, String> writer = plain();
        CompletableFuture<String> load = CompletableFuture.supplyAsync(() -> loader.get("k"));
        assertThat(blocked.called.await(5, TimeUnit.SECONDS)).isTrue();

        // drops the load's future (the epoch stays): the write's message then finds nothing to mark
        loader.invalidate("k");
        writer.put("k", "v");
        settle();
        blocked.release.countDown();

        assertThat(load.get(5, TimeUnit.SECONDS)).isNull();
        assertThat(loader.getIfPresent("k")).as("no absence hides the value").isEqualTo("v");
    }

    @Test
    @Disabled("a load overtaken by invalidateAll is accepted until its repair (VISION.md, \"Target design: "
            + "invalidateAll by the store's time, hidden on read\", and build plan, Later)")
    void a_load_of_a_missing_key_overtaken_by_invalidateAll_is_invalidated_everywhere() throws Exception {
        BlockedLoad blocked = new BlockedLoad();
        blocked.source.set("old");
        DistributedLoadingCache<String, String> loader = track(builder(Function.identity()).build(key -> blocked.load()));
        DistributedCache<String, String> invalidator = plain();
        DistributedCache<String, String> bystander = plain();
        CompletableFuture<String> load = CompletableFuture.supplyAsync(() -> loader.get("k"));
        assertThat(blocked.called.await(5, TimeUnit.SECONDS)).isTrue();

        // the source changes, then everything is invalidated, while the load holds the older value
        blocked.source.set("new");
        invalidator.invalidateAll();
        settle();
        blocked.release.countDown();
        // decided under the cut-off it read, before the invalidateAll: refused by a store checking the basis, so
        // loaded again ("new"); or written and hidden by its basis, the load answering its own (Caffeine's order "the
        // load, then invalidateAll"). Either way, every instance agrees
        String loaded = load.get(5, TimeUnit.SECONDS);
        assertThat(loaded).isIn("old", "new");
        String expected = loaded.equals("new") ? "new" : null;

        assertThat(invalidator.getIfPresent("k")).isEqualTo(expected);
        assertThat(bystander.getIfPresent("k")).isEqualTo(expected);
        assertThat(loader.getIfPresent("k")).as("held at the epoch before the invalidateAll: checked")
                .isEqualTo(expected);
    }

    @Test
    @Disabled("a load overtaken by invalidateAll is accepted until its repair (VISION.md, \"Target design: "
            + "invalidateAll by the store's time, hidden on read\", and build plan, Later)")
    void a_load_that_read_a_tombstone_overtaken_by_invalidateAll_is_hidden() throws Exception {
        BlockedLoad blocked = new BlockedLoad();
        blocked.source.set("old");
        DistributedLoadingCache<String, String> loader = track(builder(Function.identity()).build(key -> blocked.load()));
        DistributedCache<String, String> invalidator = plain();
        invalidator.invalidate("k");
        CompletableFuture<String> load = CompletableFuture.supplyAsync(() -> loader.get("k"));
        assertThat(blocked.called.await(5, TimeUnit.SECONDS)).isTrue();

        blocked.source.set("new");
        invalidator.invalidateAll();
        settle();
        blocked.release.countDown();

        // refused and loaded again, or the load's own and hidden by its basis
        String loaded = load.get(5, TimeUnit.SECONDS);
        assertThat(loaded).isIn("old", "new");
        assertThat(invalidator.getIfPresent("k")).isEqualTo(loaded.equals("new") ? "new" : null);
    }

    @Test
    void no_absence_is_recorded_by_a_load_overlapping_a_gap() throws Exception {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        BlockedLoad blocked = new BlockedLoad();
        DistributedLoadingCache<String, String> loader = track(builder(Function.identity())
                .withAbsentCaching(Duration.ofMinutes(1)).build(key -> blocked.load()));
        DistributedCache<String, String> writer = plain();
        CompletableFuture<String> load = CompletableFuture.supplyAsync(() -> loader.get("k"));
        assertThat(blocked.called.await(5, TimeUnit.SECONDS)).isTrue();

        // the write's message is lost in a gap
        faults.pauseDelivery();
        writer.put("k", "v");
        faults.gap();
        faults.resumeDelivery();
        await(() -> loader.distributedPolicy().getSynchronizationState() == SynchronizationState.SYNCHRONIZED);
        settle();
        blocked.release.countDown();

        assertThat(load.get(5, TimeUnit.SECONDS)).isNull();
        assertThat(loader.getIfPresent("k")).as("no absence hides the value").isEqualTo("v");
    }

    @Test
    void extra_keys_are_written_only_where_no_row_exists_and_never_over_a_newer_value() throws Exception {
        java.util.Map<String, String> source = new java.util.concurrent.ConcurrentHashMap<>(Map.of("a", "a1", "x", "x1"));
        CountDownLatch called = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DistributedLoadingCache<String, String> loader = track(builder(Function.identity()).build(new CacheLoader<>() {
            @Override
            public String load(String key) {
                return source.get(key);
            }

            @Override
            public Map<String, String> loadAll(java.util.Set<? extends String> keys) throws InterruptedException {
                Map<String, String> result = Map.of("a", source.get("a"), "x", source.get("x"));
                called.countDown();
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                return result;
            }
        }));
        DistributedCache<String, String> writer = plain();
        CompletableFuture<Map<String, String>> bulk = CompletableFuture.supplyAsync(() -> loader.getAll(List.of("a")));
        assertThat(called.await(5, TimeUnit.SECONDS)).isTrue();

        // the extra key changes after the loader read it: the source first, then the cache
        source.put("x", "x2");
        writer.invalidate("x");
        settle();
        release.countDown();

        assertThat(bulk.get(5, TimeUnit.SECONDS)).containsExactly(Map.entry("a", "a1"));
        assertThat(writer.distributedPolicy().getFromStore("x")).as("not written over the tombstone").isNull();
        assertThat(held(loader, "x")).isNull();
    }

    @Test
    void an_extra_key_held_after_a_newer_write_was_announced_is_dropped_again() {
        DistributedCache<String, String> writer = plain();
        DistributedLoadingCache<String, String> loader = track(builder(store -> new InternalCoreTest.DelegatingStore(store) {
            @Override
            public java.util.Set<String> write(Collection<io.github.oberhoff.distributedcaffeine.spi.Write> writes) {
                java.util.Set<String> conflicts = super.write(writes);
                if (writes.size() == 2 && writes.stream().noneMatch(
                        io.github.oberhoff.distributedcaffeine.spi.Write::isUnconditional)) {
                    // after the extra landed, before it is held: a newer write elsewhere, announced at once
                    writer.put("x", "newer");
                    settle();
                }
                return conflicts;
            }
        }).build(new CacheLoader<>() {
            @Override
            public String load(String key) {
                return key + "1";
            }

            @Override
            public Map<String, String> loadAll(java.util.Set<? extends String> keys) {
                return Map.of("a", "a1", "x", "x1");
            }
        }));

        assertThat(loader.getAll(List.of("a"))).isEqualTo(Map.of("a", "a1"));

        assertThat(held(loader, "x")).isNotEqualTo("x1");
        assertThat(loader.getIfPresent("x")).isEqualTo("newer");
    }

    @Test
    void population_never_adds_a_key_an_instance_does_not_hold() {
        DistributedCache<String, String> first = populated();
        DistributedCache<String, String> second = populated();

        first.put("k", "v");
        settle();

        assertThat(held(second, "k")).isNull();
        assertThat(second.estimatedSize()).isZero();
    }

    @Test
    void an_invalidation_elsewhere_drops_the_held_value() {
        DistributedCache<String, String> first = populated();
        DistributedCache<String, String> second = populated();
        first.put("k", "v");
        assertThat(second.getIfPresent("k")).isEqualTo("v");

        first.invalidate("k");

        await(() -> held(second, "k") == null);
        assertThat(second.getIfPresent("k")).isNull();
    }

    @Test
    void the_writers_own_message_leaves_its_value_alone() {
        AtomicInteger reads = new AtomicInteger();
        DistributedCache<String, String> writer = track(builder(store -> new InternalCoreTest.DelegatingStore(store) {
            @Override
            public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
                reads.incrementAndGet();
                return super.read(hashes, maxAge);
            }
        }).withDistributionMode(DistributionMode.UPDATE).build());

        writer.put("k", "v1");
        settle();
        reads.set(0);

        // a second write, so that its echo certainly finds the key held
        writer.put("k", "v2");
        settle();

        assertThat(held(writer, "k")).isEqualTo("v2");
        assertThat(reads).as("no population read for its own echo").hasValue(0);
    }

    @Test
    void duplicates_and_reordering_across_keys_still_converge() {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        faults.setDuplicateProbability(0.5);
        faults.setReorderAcrossKeys(true);
        DistributedCache<String, String> first = populated();
        DistributedCache<String, String> second = populated();
        for (int i = 0; i < 20; i++) {
            first.put("k" + i, "v0");
            assertThat(second.getIfPresent("k" + i)).isEqualTo("v0");
        }

        for (int round = 1; round <= 5; round++) {
            for (int i = 0; i < 20; i++) {
                first.put("k" + i, "v" + round);
            }
        }

        for (int i = 0; i < 20; i++) {
            String key = "k" + i;
            await(() -> "v5".equals(held(second, key)));
        }
    }

    @Test
    void invalidateAll_elsewhere_clears_the_held_values() {
        DistributedCache<String, String> first = plain();
        DistributedCache<String, String> second = plain();
        first.putAll(Map.of("a", "1", "b", "2"));
        assertThat(second.getAllPresent(List.of("a", "b"))).isEqualTo(Map.of("a", "1", "b", "2"));

        first.invalidateAll();

        await(() -> second.estimatedSize() == 0);
    }

    @Test
    void a_load_overtaken_by_a_message_is_not_kept() throws Exception {
        CountDownLatch readDone = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DistributedCache<String, String> first = plain();
        first.put("k", "old");
        DistributedCache<String, String> second = track(builder(store -> new InternalCoreTest.DelegatingStore(store) {
            @Override
            public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
                Map<String, Row> rows = super.read(hashes, maxAge);
                readDone.countDown();
                try {
                    assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return rows;
            }
        }).build());
        CompletableFuture<String> read = CompletableFuture.supplyAsync(() -> second.getIfPresent("k"));
        assertThat(readDone.await(5, TimeUnit.SECONDS)).isTrue();

        first.put("k", "new");
        settle();
        release.countDown();

        assertThat(read.get(5, TimeUnit.SECONDS)).as("the read itself started before the write").isEqualTo("old");
        await(() -> held(second, "k") == null);
    }

    @Test
    void absent_caching_answers_repeated_misses_without_store_or_source() {
        AtomicInteger loads = new AtomicInteger();
        AtomicInteger reads = new AtomicInteger();
        DistributedLoadingCache<String, String> cache = track(builder(store -> new InternalCoreTest.DelegatingStore(store) {
            @Override
            public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
                reads.incrementAndGet();
                return super.read(hashes, maxAge);
            }
        }).withAbsentCaching(Duration.ofMinutes(1)).build(key -> {
            loads.incrementAndGet();
            return null;
        }));

        assertThat(cache.get("missing")).isNull();
        assertThat(cache.get("missing")).isNull();
        assertThat(cache.getIfPresent("missing")).isNull();

        assertThat(loads).hasValue(1);
        assertThat(reads).hasValue(1);
    }

    @Test
    void a_write_anywhere_ends_an_absence_at_once() {
        AtomicInteger loads = new AtomicInteger();
        CacheLoader<String, String> loader = key -> loads.incrementAndGet() == 1 ? null : "found";
        DistributedLoadingCache<String, String> first = track(builder(Function.identity())
                .withAbsentCaching(Duration.ofMinutes(1)).build(loader));
        DistributedCache<String, String> second = plain();
        assertThat(first.get("k")).isNull();

        second.put("k", "written");

        await(() -> "written".equals(first.get("k")));
    }

    @Test
    void an_absence_ends_after_its_expiry() {
        AtomicInteger loads = new AtomicInteger();
        DistributedLoadingCache<String, String> cache = track(builder(Function.identity())
                .withAbsentCaching(Duration.ofMillis(100)).build(key -> loads.incrementAndGet() == 1 ? null : "now"));
        assertThat(cache.get("k")).isNull();

        pause(150);

        assertThat(cache.get("k")).isEqualTo("now");
    }

    @Test
    void a_write_during_a_load_that_finds_nothing_leaves_no_absence() throws Exception {
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        DistributedLoadingCache<String, String> first = track(builder(Function.identity())
                .withAbsentCaching(Duration.ofMinutes(1)).build(key -> {
                    if (loads.incrementAndGet() == 1) {
                        loading.countDown();
                        assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                        return null;
                    }
                    return "loaded";
                }));
        DistributedCache<String, String> second = plain();
        CompletableFuture<String> load = CompletableFuture.supplyAsync(() -> first.get("k"));
        assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();

        second.put("k", "written");
        settle();
        release.countDown();

        assertThat(load.get(5, TimeUnit.SECONDS)).isNull();
        assertThat(first.get("k")).as("no absence was recorded").isEqualTo("written");
    }

    @Test
    void an_expiry_elsewhere_drops_the_held_value() {
        DistributedCache<String, String> first = plain();
        first.put("k", "v");
        DistributedCache<String, String> second = plain();
        assertThat(second.getIfPresent("k")).isEqualTo("v");
        Store maintenance = track(backend.open());
        pause(60);

        assertThat(maintenance.expire(Duration.ofMillis(50), InternalVersion.fresh())).isEqualTo(1);

        await(() -> held(second, "k") == null && held(first, "k") == null);
    }
}
