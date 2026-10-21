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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.StorageOnlyStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The core's read and write paths against the reference store: several instances of one cache, each with an adapter
 * of its own on the same table. No distribution of messages yet (a later step), so an instance only learns about a
 * change elsewhere through the store.
 */
@Timeout(30)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
public class InternalCoreTest {

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

    private Adapter<String, String> adapter() {
        return adapter(backend);
    }

    private static Adapter<String, String> adapter(CacheBackend backend) {
        return backend::open;
    }

    private DistributedCaffeine<String, String> builder(Duration maximumAge) {
        return builder(backend, maximumAge);
    }

    private static DistributedCaffeine<String, String> builder(CacheBackend backend, Duration maximumAge) {
        return DistributedCaffeine.newBuilder(adapter(backend))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(maximumAge).withMaximumSize(1_000));
    }

    private DistributedLoadingCache<String, String> loading(CacheLoader<String, String> loader) {
        DistributedLoadingCache<String, String> cache = builder(LONG).build(loader);
        closeables.add(cache);
        return cache;
    }

    private DistributedCache<String, String> plain() {
        DistributedCache<String, String> cache = builder(LONG).build();
        closeables.add(cache);
        return cache;
    }

    private Store store() {
        Store store = backend.open();
        closeables.add(store);
        return store;
    }

    private static String hash(String key) {
        return new InternalHasher<String>(null).wrap(key).hash();
    }

    // reading through the store

    @Test
    void a_load_writes_the_store_and_another_instance_reads_it_instead_of_the_source() {
        AtomicInteger loads = new AtomicInteger();
        DistributedLoadingCache<String, String> first = builder(LONG).withCaffeine(CaffeineConfigurer::recordStats)
                .build(key -> "v-" + key + "-" + loads.incrementAndGet());
        closeables.add(first);
        DistributedLoadingCache<String, String> second = loading(key -> "v-" + key + "-" + loads.incrementAndGet());

        assertThat(first.get("k")).isEqualTo("v-k-1");
        assertThat(first.get("k")).isEqualTo("v-k-1");
        assertThat(second.get("k")).isEqualTo("v-k-1");

        assertThat(loads).hasValue(1);
        assertThat(first.stats().missCount()).isEqualTo(1);
        assertThat(first.stats().hitCount()).isEqualTo(1);
        assertThat(first.stats().loadSuccessCount()).isEqualTo(1);
    }

    @Test
    void getIfPresent_reads_through_the_store_but_never_loads() {
        DistributedCache<String, String> first = plain();
        DistributedCache<String, String> second = plain();
        first.put("k", "v");

        assertThat(second.getIfPresent("k")).isEqualTo("v");
        assertThat(second.getIfPresent("missing")).isNull();
        assertThat(second.estimatedSize()).isEqualTo(1);
    }

    @Test
    void get_with_a_mapping_function_loads_once_and_writes_through() {
        DistributedCache<String, String> first = plain();
        DistributedCache<String, String> second = plain();
        AtomicInteger calls = new AtomicInteger();

        assertThat(first.get("k", key -> "mapped-" + calls.incrementAndGet())).isEqualTo("mapped-1");
        assertThat(second.get("k", key -> "mapped-" + calls.incrementAndGet())).isEqualTo("mapped-1");
        assertThat(calls).hasValue(1);
    }

    @Test
    void the_loader_runs_in_the_callers_thread() {
        Thread caller = Thread.currentThread();
        Thread[] loading = new Thread[1];
        DistributedLoadingCache<String, String> cache = loading(key -> {
            loading[0] = Thread.currentThread();
            return "v";
        });

        assertThat(cache.get("k")).isEqualTo("v");

        assertThat(loading[0]).isSameAs(caller);
    }

    @Test
    void a_null_load_is_neither_cached_nor_written() {
        DistributedLoadingCache<String, String> cache = loading(key -> null);

        assertThat(cache.get("k")).isNull();

        assertThat(cache.estimatedSize()).isZero();
        assertThat(store().read(List.of(hash("k")), LONG).rows()).isEmpty();
    }

    @Test
    void a_failing_load_propagates_and_caches_nothing() {
        DistributedLoadingCache<String, String> cache = loading(key -> {
            throw new IllegalStateException("source down");
        });

        assertThatThrownBy(() -> cache.get("k")).isInstanceOf(IllegalStateException.class).hasMessage("source down");
        assertThat(cache.estimatedSize()).isZero();
    }

    @Test
    void a_value_older_than_the_maximum_age_is_not_served_but_loaded_again() {
        AtomicInteger loads = new AtomicInteger();
        DistributedLoadingCache<String, String> first = builder(Duration.ofMillis(100)).build(key -> "v" + loads.incrementAndGet());
        closeables.add(first);
        assertThat(first.get("k")).isEqualTo("v1");
        pause(150);
        DistributedLoadingCache<String, String> second = builder(Duration.ofMillis(100)).build(key -> "v" + loads.incrementAndGet());
        closeables.add(second);

        assertThat(second.get("k")).isEqualTo("v2");
        assertThat(requireNonNull(second.distributedPolicy().getFromStore("k")).value())
                .as("the fresh value replaced the old row")
                .isEqualTo("v2");
        assertThat(second.estimatedSize()).isEqualTo(1);
    }

    @Test
    void an_unreadable_row_is_a_miss_and_the_load_replaces_it() {
        Assumptions.assumeTrue(backend.inMemory() != null, "a real database refuses to store the unreadable bytes");
        store().put(List.of(new Entry(hash("k"), "k".getBytes(StandardCharsets.UTF_8), new byte[]{(byte) 0xff})));
        DistributedCache<String, Long> cache = DistributedCaffeine.<String, Long>newBuilder(backend::open)
                .withSerializers(serializers -> serializers.withValueSerializer(StringSerializer.ofLong()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10))
                .build();
        closeables.add(cache);

        assertThat(cache.getIfPresent("k")).isNull();
        assertThat(cache.get("k", key -> 42L)).isEqualTo(42L);
        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).isEqualTo(42L);
    }

    // one load per key and JVM

    @Test
    void concurrent_gets_of_one_key_share_one_load() throws Exception {
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        DistributedLoadingCache<String, String> cache = loading(key -> {
            loads.incrementAndGet();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return "v";
        });
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(executor.submit(() -> cache.get("k")));
            }
            pause(200);
            release.countDown();
            for (Future<String> future : futures) {
                assertThat(future.get()).isEqualTo("v");
            }
        } finally {
            executor.shutdownNow();
        }
        assertThat(loads).hasValue(1);
    }

    @Test
    void a_slow_load_does_not_block_other_keys() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        DistributedLoadingCache<String, String> cache = loading(key -> {
            if (key.equals("slow")) {
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            }
            return "v-" + key;
        });
        CompletableFuture<String> slow = CompletableFuture.supplyAsync(() -> cache.get("slow"));
        pause(100);

        long start = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            assertThat(cache.get("fast" + i)).isEqualTo("v-fast" + i);
        }

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
        release.countDown();
        assertThat(slow.get(5, TimeUnit.SECONDS)).isEqualTo("v-slow");
    }

    @Test
    void getIfPresent_answers_null_at_once_while_a_load_is_in_flight() throws Exception {
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DistributedLoadingCache<String, String> cache = loading(key -> {
            loading.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return "v";
        });
        CompletableFuture<String> load = CompletableFuture.supplyAsync(() -> cache.get("k"));
        assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();

        long start = System.nanoTime();
        assertThat(cache.getIfPresent("k")).as("as in Caffeine: not present yet").isNull();
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));

        release.countDown();
        assertThat(load.get(5, TimeUnit.SECONDS)).isEqualTo("v");
    }

    @Test
    void a_value_loaded_from_the_source_alone_on_a_failed_read_is_checked_on_the_next_read() {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        AtomicInteger loads = new AtomicInteger();
        DistributedLoadingCache<String, String> cache = loading(key -> "v" + loads.incrementAndGet());
        faults.failNextReads(1);

        assertThat(cache.get("k")).isEqualTo("v1");
        assertThat(store().read(List.of(hash("k")), LONG).rows()).as("the source-only value was not written").isEmpty();

        assertThat(cache.get("k")).as("not trusted: checked, found missing, loaded properly").isEqualTo("v2");
        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).isEqualTo("v2");
    }

    @Test
    void a_loader_requesting_its_own_key_fails_at_once() {
        AtomicReference<DistributedLoadingCache<String, String>> self = new AtomicReference<>();
        DistributedLoadingCache<String, String> cache = loading(key -> self.get().get(key));
        self.set(cache);

        assertThatThrownBy(() -> cache.get("k")).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Recursive load");
    }

    @Test
    void loads_waiting_for_each_other_across_threads_finish_instead_of_hanging() throws Exception {
        // each load asks for the other key once both loads are under way: whichever waits second would close the
        // cycle, so it loads the other key by itself instead of waiting (as the current generation does)
        CountDownLatch bothLoading = new CountDownLatch(2);
        AtomicReference<DistributedCache<String, String>> self = new AtomicReference<>();
        Function<String, String> askingForTheOther = key -> {
            bothLoading.countDown();
            try {
                assertThat(bothLoading.await(5, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            String other = key.equals("a") ? "b" : "a";
            return "outer " + key + " with " + self.get().get(other, inner -> "inner " + inner);
        };
        DistributedCache<String, String> cache = plain();
        self.set(cache);
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            Future<String> a = threads.submit(() -> cache.get("a", askingForTheOther));
            Future<String> b = threads.submit(() -> cache.get("b", askingForTheOther));

            assertThat(a.get(10, TimeUnit.SECONDS)).startsWith("outer a");
            assertThat(b.get(10, TimeUnit.SECONDS)).startsWith("outer b");
            assertThat(cache.getIfPresent("a")).isNotNull();
            assertThat(cache.getIfPresent("b")).isNotNull();
        } finally {
            threads.shutdownNow();
        }
    }

    // races between a load and a change elsewhere (ordering rule 6)

    @Test
    void a_put_elsewhere_during_a_load_wins_over_the_loaded_value() throws Exception {
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DistributedLoadingCache<String, String> first = loading(key -> {
            loading.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return "loaded, possibly outdated";
        });
        DistributedCache<String, String> second = plain();
        CompletableFuture<String> load = CompletableFuture.supplyAsync(() -> first.get("k"));
        assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();

        second.put("k", "newer");
        release.countDown();

        assertThat(load.get(5, TimeUnit.SECONDS)).isEqualTo("newer");
        assertThat(first.getIfPresent("k")).isEqualTo("newer");
        assertThat(requireNonNull(first.distributedPolicy().getFromStore("k")).value()).isEqualTo("newer");
    }

    @Test
    void an_invalidation_elsewhere_during_a_load_makes_it_load_again() throws Exception {
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DistributedLoadingCache<String, String> first = loading(key -> {
            if (loads.incrementAndGet() == 1) {
                loading.countDown();
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                return "outdated";
            }
            return "fresh";
        });
        DistributedCache<String, String> second = plain();
        CompletableFuture<String> load = CompletableFuture.supplyAsync(() -> first.get("k"));
        assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();

        second.invalidate("k");
        release.countDown();

        assertThat(load.get(5, TimeUnit.SECONDS)).isEqualTo("fresh");
        assertThat(requireNonNull(first.distributedPolicy().getFromStore("k")).value()).isEqualTo("fresh");
        assertThat(loads).hasValue(2);
    }

    @Test
    void invalidations_on_every_attempt_return_the_last_value_uncached() {
        Store other = store();
        DistributedLoadingCache<String, String> cache = loading(key -> {
            other.invalidate(List.of(hash(key)));
            return "outdated";
        });

        assertThat(cache.get("k")).isEqualTo("outdated");

        assertThat(cache.estimatedSize()).isZero();
        assertThat(store().read(List.of(hash("k")), LONG).rows().get(hash("k")).state()).isEqualTo(State.TOMBSTONE);
    }

    // writes

    @Test
    void put_writes_through_and_a_newer_held_value_is_never_replaced_by_an_older_one() {
        DistributedCache<String, String> first = plain();
        DistributedCache<String, String> second = plain();

        first.put("k", "a");
        second.put("k", "b");

        assertThat(first.getIfPresent("k")).as("its own write until the newer one is announced").isIn("a", "b");
        assertThat(requireNonNull(first.distributedPolicy().getFromStore("k")).value()).isEqualTo("b");
        assertThat(plain().getIfPresent("k")).isEqualTo("b");
    }

    @Test
    void a_slower_put_of_an_older_write_never_replaces_a_newer_held_value() throws Exception {
        CountDownLatch slowWritten = new CountDownLatch(1);
        CountDownLatch fastCommitted = new CountDownLatch(1);
        Adapter<String, String> delaying = settings -> new DelegatingStore(backend.open(settings)) {
            @Override
            public Map<String, Long> put(java.util.Collection<Entry> entries) {
                Map<String, Long> versions = super.put(entries);
                if (new String(entries.iterator().next().value(), StandardCharsets.UTF_8).equals("slow")) {
                    slowWritten.countDown();
                    try {
                        assertThat(fastCommitted.await(5, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return versions;
            }
        };
        DistributedCache<String, String> cache = DistributedCaffeine.newBuilder(delaying)
                .withSerializers(serializers -> serializers.withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10)).build();
        closeables.add(cache);

        CompletableFuture<Void> slow = CompletableFuture.runAsync(() -> cache.put("k", "slow"));
        assertThat(slowWritten.await(5, TimeUnit.SECONDS)).isTrue();
        cache.put("k", "fast");
        fastCommitted.countDown();
        slow.get(5, TimeUnit.SECONDS);

        assertThat(cache.getIfPresent("k")).as("the store applied fast after slow").isEqualTo("fast");
    }

    @Test
    void putAll_and_getAllPresent() {
        DistributedCache<String, String> first = plain();
        DistributedCache<String, String> second = plain();
        first.putAll(Map.of("a", "1", "b", "2"));

        assertThat(second.getAllPresent(List.of("b", "missing", "a"))).containsExactly(Map.entry("b", "2"), Map.entry("a", "1"));
    }

    @Test
    void invalidate_removes_everywhere_through_the_store() {
        AtomicInteger loads = new AtomicInteger();
        DistributedLoadingCache<String, String> first = loading(key -> "v" + loads.incrementAndGet());
        assertThat(first.get("k")).isEqualTo("v1");
        DistributedLoadingCache<String, String> second = loading(key -> "v" + loads.incrementAndGet());

        first.invalidate("k");

        assertThat(second.get("k")).isEqualTo("v2");
    }

    @Test
    void invalidateAll_tombstones_every_value() {
        DistributedCache<String, String> first = plain();
        first.putAll(Map.of("a", "1", "b", "2"));

        first.invalidateAll();

        assertThat(first.estimatedSize()).isZero();
        assertThat(plain().getAllPresent(List.of("a", "b"))).isEmpty();
    }

    @Test
    void a_put_with_an_unknown_outcome_drops_the_key_and_throws() {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        DistributedCache<String, String> cache = plain();
        cache.put("k", "old");
        faults.failNextWrites(1, true);

        assertThatThrownBy(() -> cache.put("k", "new")).isInstanceOf(StoreException.class);

        assertThat(cache.getIfPresent("k")).as("read through the store again").isEqualTo("new");
    }

    @Test
    void a_put_certainly_not_committed_keeps_the_held_value() {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        DistributedCache<String, String> cache = plain();
        cache.put("k", "old");
        faults.failNextWrites(1, false);

        assertThatThrownBy(() -> cache.put("k", "new")).isInstanceOf(StoreException.class);

        assertThat(cache.getIfPresent("k")).isEqualTo("old");
    }

    // several keys

    @Test
    void getAll_reads_the_store_once_and_loads_the_rest_in_bulk() {
        DistributedCache<String, String> first = plain();
        first.put("stored", "s");
        AtomicInteger bulkCalls = new AtomicInteger();
        Map<String, Set<String>> requested = new ConcurrentHashMap<>();
        DistributedLoadingCache<String, String> second = loading(new CacheLoader<>() {
            @Override
            public String load(String key) {
                throw new AssertionError("loadAll expected");
            }

            @Override
            public Map<String, String> loadAll(Set<? extends String> keys) {
                bulkCalls.incrementAndGet();
                requested.put("keys", Set.copyOf(keys));
                Map<String, String> result = new HashMap<>();
                keys.forEach(key -> result.put(key, "loaded-" + key));
                result.put("extra", "unasked");
                return result;
            }
        });

        Map<String, String> values = second.getAll(List.of("a", "stored", "b"));

        assertThat(values).containsExactly(Map.entry("a", "loaded-a"), Map.entry("stored", "s"), Map.entry("b", "loaded-b"));
        assertThat(bulkCalls).hasValue(1);
        assertThat(requested.get("keys")).containsExactlyInAnyOrder("a", "b");
        assertThat(second.getIfPresent("extra")).as("an extra key the store held no value for is kept").isEqualTo("unasked");
    }

    @Test
    void an_extra_key_never_overwrites_a_value_the_store_holds() {
        DistributedCache<String, String> first = plain();
        first.put("extra", "newer");
        DistributedLoadingCache<String, String> second = loading(new CacheLoader<>() {
            @Override
            public String load(String key) {
                return "loaded-" + key;
            }

            @Override
            public Map<String, String> loadAll(Set<? extends String> keys) {
                Map<String, String> result = new LinkedHashMap<>();
                keys.forEach(key -> result.put(key, "loaded-" + key));
                result.put("extra", "outdated");
                return result;
            }
        });

        assertThat(second.getAll(List.of("a"))).isEqualTo(Map.of("a", "loaded-a"));

        assertThat(requireNonNull(first.distributedPolicy().getFromStore("extra")).value()).isEqualTo("newer");
        assertThat(second.getIfPresent("extra")).isEqualTo("newer");
    }

    @Test
    void getAll_without_a_bulk_loader_loads_key_by_key() {
        AtomicInteger loads = new AtomicInteger();
        DistributedLoadingCache<String, String> cache = loading(key -> {
            loads.incrementAndGet();
            return "v-" + key;
        });

        assertThat(cache.getAll(List.of("a", "b", "c"))).hasSize(3);
        assertThat(loads).hasValue(3);
    }

    @Test
    void getAll_with_a_mapping_function() {
        DistributedCache<String, String> cache = plain();
        Function<Set<? extends String>, Map<String, String>> mapping = keys -> {
            Map<String, String> result = new HashMap<>();
            keys.forEach(key -> result.put(key, "m-" + key));
            return result;
        };

        assertThat(cache.getAll(List.of("a", "b"), mapping)).containsExactly(Map.entry("a", "m-a"), Map.entry("b", "m-b"));
        assertThat(plain().getAllPresent(List.of("a", "b"))).hasSize(2);
    }

    @Test
    @Timeout(120) // MongoDB writes one document per round trip
    void batches_far_larger_than_one_statement_are_written_read_and_removed_whole() {
        // beyond what one statement may carry (PostgreSQL binds at most 65,535 parameters) and the stores' chunks
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < 25_000; index++) {
            values.put("k" + index, "v" + index);
        }
        DistributedCache<String, String> writer = plain();
        DistributedCache<String, String> reader = plain();

        writer.putAll(values);
        assertThat(reader.getAllPresent(values.keySet())).isEqualTo(values);
        Map<String, String> loaded = reader.getAll(values.keySet(), keys -> Map.of());
        assertThat(loaded).isEqualTo(values);

        writer.invalidateAll(values.keySet());
        assertThat(reader.distributedPolicy().getAllFromStore(values.keySet())).isEmpty();
    }

    // refresh

    @Test
    void refresh_goes_to_the_source_and_writes_conditionally() throws Exception {
        AtomicInteger loads = new AtomicInteger();
        DistributedLoadingCache<String, String> cache = loading(key -> "v" + loads.incrementAndGet());
        assertThat(cache.get("k")).isEqualTo("v1");

        assertThat(cache.refresh("k").get(5, TimeUnit.SECONDS)).isEqualTo("v2");

        // Caffeine completes the refresh future first and updates its map in a callback after it
        await(() -> "v2".equals(cache.getIfPresent("k")));
        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).isEqualTo("v2");
    }

    @Test
    void a_refresh_overtaken_by_a_put_elsewhere_takes_the_newer_value() throws Exception {
        CountDownLatch reloading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DistributedLoadingCache<String, String> first = loading(new CacheLoader<>() {
            @Override
            public String load(String key) {
                return "initial";
            }

            @Override
            public String reload(String key, String oldValue) throws Exception {
                reloading.countDown();
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                return "reloaded, possibly outdated";
            }
        });
        assertThat(first.get("k")).isEqualTo("initial");
        CompletableFuture<String> refresh = first.refresh("k");
        assertThat(reloading.await(5, TimeUnit.SECONDS)).isTrue();

        plain().put("k", "newer");
        release.countDown();

        assertThat(refresh.get(5, TimeUnit.SECONDS)).isEqualTo("newer");
        assertThat(requireNonNull(first.distributedPolicy().getFromStore("k")).value()).isEqualTo("newer");
    }

    @Test
    void a_value_read_from_the_store_at_least_refreshAfterWrite_old_is_served_and_refreshed_in_the_background() {
        // the store's clock moves on alone, so that L1's own refreshAfterWrite never fires during the test
        Assumptions.assumeTrue(backend.inMemory() != null, "moves the in-memory store's clock");
        AtomicLong clockMicros = new AtomicLong(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        CacheBackend aging = CacheBackend.of(new InMemoryDatabase(clockMicros::get, 1));
        DistributedCache<String, String> writer = builder(aging, LONG).build();
        closeables.add(writer);
        writer.put("viaGet", "stored");
        writer.put("viaGetIfPresent", "stored");
        clockMicros.addAndGet(TimeUnit.MINUTES.toMicros(30));
        writer.put("young", "stored");
        AtomicInteger loads = new AtomicInteger();
        DistributedLoadingCache<String, String> reader = builder(aging, LONG)
                .withCaffeine(caffeine -> caffeine.refreshAfterWrite(Duration.ofMinutes(1)))
                .build(key -> {
                    loads.incrementAndGet();
                    return "fresh";
                });
        closeables.add(reader);

        assertThat(reader.get("viaGet")).as("served at once").isEqualTo("stored");
        assertThat(reader.getIfPresent("viaGetIfPresent")).as("served at once").isEqualTo("stored");
        assertThat(reader.get("young")).isEqualTo("stored");

        await(() -> "fresh".equals(reader.getIfPresent("viaGet"))
                && "fresh".equals(reader.getIfPresent("viaGetIfPresent")));
        assertThat(requireNonNull(reader.distributedPolicy().getFromStore("viaGet")).value()).isEqualTo("fresh");
        assertThat(requireNonNull(reader.distributedPolicy().getFromStore("viaGetIfPresent")).value())
                .isEqualTo("fresh");
        assertThat(reader.getIfPresent("young")).as("younger than refreshAfterWrite").isEqualTo("stored");
        assertThat(loads).hasValue(2);
    }

    @Test
    void refreshAll_removes_a_key_the_source_no_longer_has_everywhere() throws Exception {
        DistributedCache<String, String> other = plain();
        other.put("gone", "old");
        other.put("kept", "old");
        DistributedLoadingCache<String, String> cache = loading(key -> "gone".equals(key) ? null : "new");
        assertThat(cache.getAll(List.of("gone", "kept"))).hasSize(2);

        assertThat(cache.refreshAll(List.of("gone", "kept")).get(5, TimeUnit.SECONDS))
                .containsExactly(Map.entry("kept", "new"));

        assertThat(cache.distributedPolicy().getFromStore("gone")).isNull();
        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("kept")).value()).isEqualTo("new");
        await(() -> other.getIfPresent("gone") == null); // once the other instance applied the message
        await(() -> cache.estimatedSize() == 1);
    }

    // guards the core's Loader, which has no bulk path (it does not override asyncLoadAll): should Caffeine ever
    // refresh in bulk, this fails, and the Loader needs a tested bulk path to follow it
    @Test
    void caffeine_refreshes_key_by_key_and_never_through_the_bulk_loader() throws Exception {
        AtomicInteger bulkLoads = new AtomicInteger();
        List<String> reloaded = Collections.synchronizedList(new ArrayList<>());
        AsyncLoadingCache<String, String> caffeine = Caffeine.newBuilder().executor(Runnable::run)
                .buildAsync(new AsyncCacheLoader<>() {
                    @Override
                    public CompletableFuture<String> asyncLoad(String key, Executor executor) {
                        return CompletableFuture.completedFuture("loaded");
                    }

                    @Override
                    public CompletableFuture<Map<String, String>> asyncLoadAll(Set<? extends String> keys,
                                                                              Executor executor) {
                        bulkLoads.incrementAndGet();
                        Map<String, String> loaded = new HashMap<>();
                        keys.forEach(key -> loaded.put(key, "bulk"));
                        return CompletableFuture.completedFuture(loaded);
                    }

                    @Override
                    public CompletableFuture<String> asyncReload(String key, String oldValue, Executor executor) {
                        reloaded.add(key);
                        return CompletableFuture.completedFuture("reloaded");
                    }
                });
        caffeine.synchronous().putAll(Map.of("a", "1", "b", "2"));

        assertThat(caffeine.synchronous().refreshAll(List.of("a", "b", "absent")).get(5, TimeUnit.SECONDS))
                .containsOnly(Map.entry("a", "reloaded"), Map.entry("b", "reloaded"), Map.entry("absent", "loaded"));
        assertThat(bulkLoads).hasValue(0);
        assertThat(reloaded).containsExactlyInAnyOrder("a", "b");
    }

    // lifecycle

    @Test
    void after_close_every_operation_throws_and_the_adapter_can_be_used_again() {
        Adapter<String, String> adapter = adapter();
        DistributedCache<String, String> cache = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10)).build();
        cache.put("k", "v");

        DistributedCaffeine<String, String> second = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10));
        assertThatThrownBy(second::build).isInstanceOf(IllegalStateException.class).hasMessageContaining("adapter");

        cache.close();
        cache.close();

        assertThatThrownBy(() -> cache.getIfPresent("k")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> cache.put("k", "v")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(cache::estimatedSize).isInstanceOf(IllegalStateException.class);
        assertThat(cache.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.CLOSED);
        DistributedCache<String, String> again = DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10)).build();
        closeables.add(again);
        assertThat(again.getIfPresent("k")).isEqualTo("v");
    }

    @Test
    void a_failing_store_fails_the_build_and_releases_the_adapter() {
        // the in-memory store's faults: skipped on a real database before anything is set up
        InMemoryDatabase faults = backend.faults();
        faults.setDown(true);
        Adapter<String, String> failing = settings -> {
            throw new StoreException("unreachable", false);
        };

        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(failing)
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10));
        assertThatThrownBy(builder::build).isInstanceOf(StoreException.class);
        faults.setDown(false);
        DistributedCache<String, String> cache = DistributedCaffeine.newBuilder(adapter())
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10)).build();
        closeables.add(cache);
    }

    @Test
    void a_store_not_answering_within_the_build_timeout_fails_the_build() {
        Adapter<String, String> slow = settings -> {
            pause(2_000);
            return backend.open(settings);
        };

        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(slow)
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10))
                .withBuildTimeout(Duration.ofMillis(200));
        assertThatThrownBy(builder::build).isInstanceOf(StoreException.class).hasMessageContaining("did not answer");
    }

    @Test
    void getFromStore_returns_value_and_age_without_filling_the_local_level() {
        plain().put("k", "v");
        DistributedCache<String, String> cache = plain();

        StoredValue<String> stored = cache.distributedPolicy().getFromStore("k");

        assertThat(requireNonNull(stored).value()).isEqualTo("v");
        assertThat(stored.age()).isLessThan(Duration.ofSeconds(5));
        assertThat(cache.estimatedSize()).isZero();
        assertThat(cache.distributedPolicy().getAllFromStore(List.of("k", "missing"))).containsOnlyKeys("k");
    }

    // a store that delegates everything, its change feed too, for wrapping single methods in tests
    static class DelegatingStore extends StorageOnlyStore implements ChangeFeed {

        private final Store delegate;

        DelegatingStore(Store delegate) {
            super(delegate);
            this.delegate = delegate;
        }

        @Override
        public io.github.oberhoff.distributedcaffeine.spi.Subscription subscribe(
                io.github.oberhoff.distributedcaffeine.spi.Listener listener) {
            return ((ChangeFeed) delegate).subscribe(listener);
        }
    }
}
