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
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.StoreProfile;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The asynchronous caches: hits answered at once, misses loaded on the store executor with the user's code on
 * Caffeine's executor, put(key, future), the asynchronous loader, the future-valued asMap(), and the shared core with
 * synchronous().
 */
@NullMarked
@Timeout(30)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
class InternalAsyncTest {

    private static final Duration LONG = Duration.ofHours(1);

    private final InMemoryDatabase database = new InMemoryDatabase();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final ExecutorService userExecutor = Executors.newFixedThreadPool(4, runnable -> {
        Thread thread = new Thread(runnable, "user-executor");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void tearDown() throws Exception {
        database.resumeDelivery();
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        userExecutor.shutdownNow();
    }

    private DistributedCaffeine<String, String> builder(Function<Store, Store> wrapping) {
        Adapter<String, String> adapter = settings -> wrapping.apply(database.openStore("cache"));
        return DistributedCaffeine.newBuilder(InternalCoreTest.offered(adapter))
                .withCaffeine(caffeine -> caffeine.executor(userExecutor))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaximumAge(LONG).withMaximumSize(1_000));
    }

    private DistributedAsyncCache<String, String> cache() {
        return track(builder(Function.identity()).buildAsync());
    }

    private DistributedAsyncLoadingCache<String, String> loading(AsyncCacheLoader<String, String> loader) {
        return track(builder(Function.identity()).buildAsync(loader));
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    // the value this instance holds for "k"
    private static @Nullable String held(DistributedAsyncCache<String, String> cache) {
        InternalCore<String, String> core = ((InternalDistributedAsyncCache<String, String>) cache).core;
        CompletableFuture<InternalValue<String>> future = core.l1().asMap().get(core.wrap("k"));
        InternalValue<String> value = future == null || !future.isDone() || future.isCompletedExceptionally() ? null
                : future.join();
        return value == null ? null : value.value();
    }

    private static @Nullable String stored(DistributedAsyncCache<String, String> cache, String key) {
        StoredValue<String> stored = cache.distributedPolicy().getFromStore(key);
        return stored == null ? null : stored.value();
    }

    private static InternalCore<String, String> core(DistributedAsyncCache<String, String> cache) {
        return ((InternalDistributedAsyncCache<String, String>) cache).core;
    }

    // the reservation registry: empty whenever nothing runs, or it would grow without bound

    @Test
    void a_future_put_that_never_completes_reserves_nothing() {
        DistributedAsyncCache<String, String> cache = cache();

        cache.put("k", new CompletableFuture<>());

        assertThat(core(cache).reservedKeys()).isZero();
    }

    @Test
    void a_store_executor_refusing_tasks_leaks_no_reservation() {
        AtomicBoolean refusing = new AtomicBoolean();
        Executor storeExecutor = task -> {
            if (refusing.get()) {
                throw new RejectedExecutionException("refusing");
            }
            userExecutor.execute(task);
        };
        DistributedAsyncCache<String, String> cache = track(builder(Function.identity())
                .withStoreExecutor(storeExecutor).buildAsync());
        CompletableFuture<String> value = new CompletableFuture<>();
        cache.put("k", value);

        refusing.set(true);
        value.complete("v");

        await(() -> core(cache).reservedKeys() == 0);
        assertThat(stored(cache, "k")).as("written on a thread of its own").isEqualTo("v");
        refusing.set(false);
    }

    private static <T extends @Nullable Object> T get(CompletableFuture<T> future) throws Exception {
        return future.get(10, TimeUnit.SECONDS);
    }

    private static void settle() {
        pause(300);
    }

    // reads

    @Test
    void getIfPresent_reads_through_the_store_and_answers_a_miss_with_a_future_of_null() throws Exception {
        DistributedAsyncCache<String, String> writer = cache();
        DistributedAsyncCache<String, String> reader = cache();
        writer.synchronous().put("k", "v");

        assertThat(get(reader.getIfPresent("k"))).isEqualTo("v");
        assertThat(held(reader)).isEqualTo("v");
        CompletableFuture<@Nullable String> miss = reader.getIfPresent("missing");
        assertThat(miss).as("never null: absence is known once the store answered").isNotNull();
        assertThat(get(miss)).isNull();
        assertThat(reader.synchronous().asMap()).doesNotContainKey("missing");
    }

    @Test
    void a_trusted_hit_is_answered_at_once_without_the_store() {
        AtomicInteger reads = new AtomicInteger();
        DistributedAsyncCache<String, String> cache = track(builder(store -> new InternalCoreTest.DelegatingStore(store) {
            @Override
            public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
                reads.incrementAndGet();
                return super.read(hashes, maxAge);
            }
        }).buildAsync());
        cache.synchronous().put("k", "v");

        CompletableFuture<@Nullable String> hit = cache.get("k", key -> "loaded");
        assertThat(hit).isCompletedWithValue("v");
        assertThat(cache.getIfPresent("k")).isCompletedWithValue("v");
        assertThat(reads).hasValue(0);
    }

    @Test
    void a_slow_load_does_not_block_the_caller_and_runs_once_on_caffeines_executor() throws Exception {
        DistributedAsyncCache<String, String> cache = cache();
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        List<String> threads = new ArrayList<>();
        Function<String, String> slow = key -> {
            calls.incrementAndGet();
            synchronized (threads) {
                threads.add(Thread.currentThread().getName());
            }
            try {
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "v";
        };

        CompletableFuture<@Nullable String> first = cache.get("k", slow);
        CompletableFuture<@Nullable String> second = cache.get("k", slow);
        assertThat(first).isNotDone();
        assertThat(cache.getIfPresent("k")).as("the load in flight, as in Caffeine").isNotDone();
        release.countDown();

        assertThat(get(first)).isEqualTo("v");
        assertThat(get(second)).isEqualTo("v");
        assertThat(calls).hasValue(1);
        assertThat(threads).containsExactly("user-executor");
        assertThat(stored(cache, "k")).isEqualTo("v");
    }

    @Test
    void an_asynchronous_mapping_function_gets_caffeines_executor() throws Exception {
        DistributedAsyncCache<String, String> cache = cache();
        List<Executor> executors = new ArrayList<>();

        String value = get(cache.get("k", (key, executor) -> {
            executors.add(executor);
            return CompletableFuture.supplyAsync(() -> "v", executor);
        }));

        assertThat(value).isEqualTo("v");
        assertThat(executors).containsExactly(userExecutor);
    }

    @Test
    // the key set shows what this instance holds, while containsKey asks the store
    @SuppressWarnings("RedundantCollectionOperation")
    void a_failed_load_fails_the_future_with_its_own_exception_and_caches_nothing() {
        DistributedAsyncCache<String, String> cache = cache();
        IllegalStateException failure = new IllegalStateException("source down");

        CompletableFuture<@Nullable String> future = cache.get("k", (key, executor) -> CompletableFuture.failedFuture(failure));

        assertThatThrownBy(() -> get(future)).isInstanceOf(ExecutionException.class).hasCause(failure);
        await(() -> !cache.synchronous().asMap().keySet().contains("k"));
        assertThat(stored(cache, "k")).isNull();
    }

    @Test
    void a_load_overtaken_by_a_write_elsewhere_is_not_kept() throws Exception {
        DistributedAsyncCache<String, String> writer = cache();
        DistributedAsyncCache<String, String> loader = cache();
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        CompletableFuture<@Nullable String> load = loader.get("k", key -> {
            loading.countDown();
            try {
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "source";
        });
        assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();
        writer.synchronous().put("k", "newer");
        settle();
        release.countDown();

        assertThat(get(load)).as("its write conflicts: the newer value wins").isEqualTo("newer");
        await(() -> "newer".equals(held(loader)));
    }

    @Test
    void an_untrusted_held_value_is_checked_against_the_store_asynchronously() throws Exception {
        assumeTrue(database.profile().changes() == StoreProfile.Changes.FEED, "interrupts the store's feed");
        DistributedAsyncCache<String, String> first = cache();
        DistributedAsyncCache<String, String> second = cache();
        first.synchronous().put("k", "v");
        assertThat(get(second.getIfPresent("k"))).isEqualTo("v");
        database.interruptSubscriptions();
        // the interruption reaches the cache through its delivery queue
        await(() -> second.distributedPolicy().getSynchronizationState() == SynchronizationState.DEGRADED);
        first.synchronous().invalidate("k");

        assertThat(get(second.getIfPresent("k"))).isNull();
        assertThat(get(second.get("k", key -> "loaded"))).isEqualTo("loaded");
    }

    @Test
    void getAll_answers_held_keys_at_once_and_loads_the_rest_in_one_call() throws Exception {
        DistributedAsyncCache<String, String> cache = cache();
        cache.synchronous().putAll(Map.of("a", "1", "b", "2"));
        List<Set<? extends String>> calls = new ArrayList<>();

        assertThat(cache.getAll(List.of("a", "b"), keys -> {
            calls.add(keys);
            return Map.of();
        })).isCompletedWithValue(Map.of("a", "1", "b", "2"));
        assertThat(calls).isEmpty();

        Map<String, String> all = get(cache.getAll(List.of("a", "c", "d"), keys -> {
            calls.add(Set.copyOf(keys));
            return Map.of("c", "3", "d", "4");
        }));
        assertThat(all).containsExactly(Map.entry("a", "1"), Map.entry("c", "3"), Map.entry("d", "4"));
        assertThat(calls).containsExactly(Set.of("c", "d"));
        assertThat(stored(cache, "d")).isEqualTo("4");
    }

    @Test
    void getAll_checks_untrusted_held_values_instead_of_answering_at_once() throws Exception {
        assumeTrue(database.profile().changes() == StoreProfile.Changes.FEED, "interrupts the store's feed");
        DistributedAsyncCache<String, String> first = cache();
        DistributedAsyncCache<String, String> second = cache();
        first.synchronous().putAll(Map.of("a", "1", "b", "2"));
        assertThat(get(second.getAll(List.of("a", "b"), keys -> Map.of()))).hasSize(2);
        database.interruptSubscriptions();
        await(() -> second.distributedPolicy().getSynchronizationState() == SynchronizationState.DEGRADED);
        first.synchronous().put("a", "changed");

        assertThat(get(second.getAll(List.of("a", "b"), keys -> Map.of())))
                .isEqualTo(Map.of("a", "changed", "b", "2"));
    }

    // non-blocking loads: no store-executor thread waits while the user's future is pending

    // a store executor that counts the tasks running on it
    private static final class CountingExecutor implements Executor {
        private final ExecutorService delegate = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "counted-store");
            thread.setDaemon(true);
            return thread;
        });
        private final AtomicInteger running = new AtomicInteger();

        @Override
        public void execute(Runnable command) {
            delegate.execute(() -> {
                running.incrementAndGet();
                try {
                    command.run();
                } finally {
                    running.decrementAndGet();
                }
            });
        }
    }

    @Test
    void pending_asynchronous_loads_hold_no_store_executor_thread() throws Exception {
        CountingExecutor storeExecutor = new CountingExecutor();
        List<CompletableFuture<String>> sources = new java.util.concurrent.CopyOnWriteArrayList<>();
        DistributedAsyncLoadingCache<String, String> cache = track(builder(Function.identity())
                .withStoreExecutor(storeExecutor)
                .buildAsync((key, executor) -> {
                    CompletableFuture<String> source = new CompletableFuture<>();
                    sources.add(source);
                    return source.thenApply(value -> value + "-" + key);
                }));
        List<CompletableFuture<@Nullable String>> loads = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            loads.add(cache.get("k" + i));
        }

        await(() -> sources.size() == 20);
        pause(50);
        assertThat(storeExecutor.running).as("every load waits for its source without a thread").hasValue(0);
        assertThat(loads).allMatch(load -> !load.isDone());

        sources.forEach(source -> source.complete("v"));
        for (int i = 0; i < 20; i++) {
            assertThat(get(loads.get(i))).isEqualTo("v-k" + i);
        }
        assertThat(stored(cache, "k7")).as("written after the source answered").isEqualTo("v-k7");
    }

    @Test
    void pending_asynchronous_bulk_loads_and_reloads_hold_no_store_executor_thread() throws Exception {
        CountingExecutor storeExecutor = new CountingExecutor();
        CompletableFuture<Map<String, String>> bulk = new CompletableFuture<>();
        CompletableFuture<String> reload = new CompletableFuture<>();
        DistributedAsyncLoadingCache<String, String> cache = track(builder(Function.identity())
                .withStoreExecutor(storeExecutor)
                .buildAsync(new AsyncCacheLoader<>() {
                    @Override
                    public CompletableFuture<String> asyncLoad(String key, Executor executor) {
                        return CompletableFuture.completedFuture("first");
                    }

                    @Override
                    public CompletableFuture<Map<String, String>> asyncLoadAll(Set<? extends String> keys,
                                                                               Executor executor) {
                        return bulk;
                    }

                    @Override
                    public CompletableFuture<String> asyncReload(String key, String oldValue, Executor executor) {
                        return reload;
                    }
                }));
        assertThat(get(cache.get("r"))).isEqualTo("first");

        CompletableFuture<Map<String, String>> all = cache.getAll(List.of("a", "b"));
        CompletableFuture<@Nullable String> refreshed = cache.synchronous().refresh("r");
        pause(100);
        assertThat(storeExecutor.running).hasValue(0);
        assertThat(all).isNotDone();
        assertThat(refreshed).isNotDone();

        bulk.complete(Map.of("a", "1", "b", "2"));
        reload.complete("second");
        assertThat(get(all)).isEqualTo(Map.of("a", "1", "b", "2"));
        assertThat(get(refreshed)).isEqualTo("second");
        assertThat(stored(cache, "b")).isEqualTo("2");
        assertThat(stored(cache, "r")).isEqualTo("second");
    }

    @Test
    void an_asynchronous_load_overtaken_by_an_invalidation_while_its_source_is_pending_loads_again() throws Exception {
        CompletableFuture<String> first = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        DistributedAsyncLoadingCache<String, String> cache = loading((key, executor) ->
                calls.incrementAndGet() == 1 ? first : CompletableFuture.completedFuture("fresh"));
        DistributedAsyncCache<String, String> other = cache();

        CompletableFuture<@Nullable String> load = cache.get("k");
        await(() -> calls.get() == 1);
        other.synchronous().invalidate("k");
        first.complete("outdated");

        assertThat(get(load)).as("its write conflicted with the tombstone, the source was asked again").isEqualTo("fresh");
        assertThat(stored(cache, "k")).isEqualTo("fresh");
    }

    // put(key, future)

    @Test
    void put_holds_the_future_at_once_and_writes_the_store_when_it_completes() throws Exception {
        DistributedAsyncCache<String, String> writer = cache();
        DistributedAsyncCache<String, String> reader = cache();
        CompletableFuture<String> value = new CompletableFuture<>();

        writer.put("k", value);
        CompletableFuture<@Nullable String> seen = writer.getIfPresent("k");
        assertThat(seen).as("readers wait on it").isNotDone();
        assertThat(stored(writer, "k")).isNull();

        value.complete("v");
        assertThat(get(seen)).isEqualTo("v");
        await(() -> "v".equals(stored(writer, "k")));
        assertThat(get(reader.getIfPresent("k"))).isEqualTo("v");
    }

    @Test
    // the key set shows what this instance holds, while containsKey asks the store
    @SuppressWarnings("RedundantCollectionOperation")
    void a_failed_or_empty_future_is_removed_and_the_previous_value_is_gone() throws Exception {
        DistributedAsyncCache<String, String> cache = cache();
        DistributedAsyncCache<String, String> other = cache();
        cache.synchronous().put("k", "old");
        await(() -> "old".equals(other.getIfPresent("k").join()));

        // as in Caffeine, where the put replaces the previous value at once: a failure leaves no value anywhere
        CompletableFuture<String> failing = new CompletableFuture<>();
        cache.put("k", failing);
        CompletableFuture<@Nullable String> waiting = cache.getIfPresent("k");
        failing.completeExceptionally(new IllegalStateException("failed"));
        assertThatThrownBy(() -> get(waiting)).as("waiters see the failure, as in Caffeine")
                .hasCauseInstanceOf(IllegalStateException.class);
        await(() -> !cache.synchronous().asMap().keySet().contains("k"));
        await(() -> stored(cache, "k") == null);
        await(() -> other.getIfPresent("k").join() == null);

        cache.synchronous().put("k", "again");
        cache.put("k", CompletableFuture.completedFuture(null));
        await(() -> stored(cache, "k") == null);
        assertThat(get(cache.getIfPresent("k"))).isNull();
    }

    @Test
    void two_puts_whose_futures_complete_in_reverse_order_follow_the_stores_order() throws Exception {
        DistributedAsyncCache<String, String> writer = cache();
        DistributedAsyncCache<String, String> reader = cache();
        CompletableFuture<String> slow = new CompletableFuture<>();

        writer.put("k", slow);
        writer.put("k", CompletableFuture.completedFuture("fast"));
        await(() -> "fast".equals(stored(writer, "k")));
        slow.complete("slow");

        await(() -> "slow".equals(stored(writer, "k")));
        await(() -> "slow".equals(writer.synchronous().getIfPresent("k")));
        assertThat(get(reader.getIfPresent("k"))).isEqualTo("slow");
    }

    // loading

    @Test
    void the_asynchronous_loader_loads_in_bulk_and_reloads_conditionally() throws Exception {
        AtomicInteger loads = new AtomicInteger();
        List<Set<? extends String>> bulks = new ArrayList<>();
        DistributedAsyncLoadingCache<String, String> cache = loading(new AsyncCacheLoader<>() {
            @Override
            public CompletableFuture<String> asyncLoad(String key, Executor executor) {
                loads.incrementAndGet();
                return CompletableFuture.supplyAsync(() -> "v-" + key, executor);
            }

            @Override
            public CompletableFuture<Map<String, String>> asyncLoadAll(Set<? extends String> keys, Executor executor) {
                bulks.add(Set.copyOf(keys));
                return CompletableFuture.completedFuture(Map.of("a", "bulk-a", "b", "bulk-b", "extra", "x"));
            }

            @Override
            public CompletableFuture<String> asyncReload(String key, String oldValue, Executor executor) {
                return CompletableFuture.completedFuture(oldValue + "+");
            }
        });

        assertThat(get(cache.get("k"))).isEqualTo("v-k");
        assertThat(get(cache.getAll(List.of("a", "b")))).isEqualTo(Map.of("a", "bulk-a", "b", "bulk-b"));
        assertThat(bulks).containsExactly(Set.of("a", "b"));
        assertThat(stored(cache, "extra")).as("extra keys where the store holds nothing").isEqualTo("x");

        assertThat(get(cache.synchronous().refresh("k"))).isEqualTo("v-k+");
        assertThat(stored(cache, "k")).isEqualTo("v-k+");
        assertThat(loads).hasValue(1);
    }

    @Test
    void a_reload_to_null_removes_the_key_everywhere_and_is_left_out_of_refreshAll() throws Exception {
        DistributedAsyncLoadingCache<String, String> cache = track(builder(Function.identity())
                .buildAsync(key -> null));
        cache.synchronous().put("k", "v");

        assertThat(get(cache.synchronous().refreshAll(List.of("k")))).isEmpty();
        // Caffeine completes the refresh future first and updates its map in a callback after it
        await(() -> held(cache) == null);
        assertThat(stored(cache, "k")).isNull();
        assertThat(cache.synchronous().asMap()).isEmpty();
    }

    @Test
    @SuppressWarnings("NullableProblems") // a loader resolving to null: absent, as Caffeine documents, not its types
    void getAll_without_a_bulk_loader_loads_key_by_key_and_leaves_out_what_the_source_lacks() throws Exception {
        List<String> loaded = Collections.synchronizedList(new ArrayList<>());
        DistributedAsyncLoadingCache<String, String> cache = loading((key, executor) -> {
            loaded.add(key);
            return CompletableFuture.supplyAsync(() -> "none".equals(key) ? null : "v-" + key, executor);
        });

        assertThat(get(cache.getAll(List.of("a", "b", "none")))).isEqualTo(Map.of("a", "v-a", "b", "v-b"));
        assertThat(loaded).containsExactlyInAnyOrder("a", "b", "none");
        assertThat(stored(cache, "a")).isEqualTo("v-a");
        assertThat(stored(cache, "b")).isEqualTo("v-b");
        assertThat(stored(cache, "none")).isNull();
    }

    @Test
    void a_synchronous_loader_runs_on_caffeines_executor_too() throws Exception {
        List<String> threads = new ArrayList<>();
        DistributedAsyncLoadingCache<String, String> cache = track(builder(Function.identity()).buildAsync(key -> {
            synchronized (threads) {
                threads.add(Thread.currentThread().getName());
            }
            return "v";
        }));

        assertThat(get(cache.get("k"))).isEqualTo("v");
        assertThat(threads).containsExactly("user-executor");
    }

    @Test
    void synchronous_shares_the_core() throws Exception {
        DistributedAsyncLoadingCache<String, String> cache = loading((key, executor) ->
                CompletableFuture.completedFuture("loaded"));

        cache.synchronous().put("k", "v");
        assertThat(cache.getIfPresent("k")).isCompletedWithValue("v");
        assertThat(cache.synchronous().get("other")).isEqualTo("loaded");
        assertThat(get(cache.get("other"))).isEqualTo("loaded");
        assertThat(cache.synchronous().distributedPolicy()).isSameAs(cache.distributedPolicy());
    }

    // asMap()

    @Test
    @SuppressWarnings("CollectionUndefinedEquality") // containsValue compares the values, as Caffeine's map does
    void asMap_answers_like_the_synchronous_view_with_futures() throws Exception {
        DistributedAsyncCache<String, String> first = cache();
        DistributedAsyncCache<String, String> second = cache();
        first.synchronous().put("k", "v");
        ConcurrentMap<String, CompletableFuture<String>> map = second.asMap();

        assertThat(map.get("missing")).as("absence is answered, as for a map").isNull();
        assertThat(get(map.get("k"))).isEqualTo("v");
        assertThat(map).containsKey("k");
        assertThat(map.putIfAbsent("k", CompletableFuture.completedFuture("other"))).isNotNull();
        assertThat(map.putIfAbsent("new", CompletableFuture.completedFuture("n"))).isNull();
        assertThat(stored(first, "new")).isEqualTo("n");
        assertThat(map.replace("k", CompletableFuture.completedFuture("v"), CompletableFuture.completedFuture("w")))
                .isTrue();
        assertThat(stored(first, "k")).isEqualTo("w");
        assertThat(get(map.remove("k"))).isEqualTo("w");
        assertThat(stored(first, "k")).isNull();
        assertThat(get(map.computeIfAbsent("c", key -> CompletableFuture.completedFuture("computed"))))
                .isEqualTo("computed");
        assertThat(map).containsValue(CompletableFuture.completedFuture("computed"));
    }

    @Test
    void asMap_merge_counts_without_losing_updates_across_instances() throws Exception {
        DistributedAsyncCache<String, String> first = cache();
        DistributedAsyncCache<String, String> second = cache();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                ConcurrentMap<String, CompletableFuture<String>> map = (i % 2 == 0 ? first : second).asMap();
                results.add(executor.submit(() -> {
                    for (int n = 0; n < 25; n++) {
                        map.merge("counter", CompletableFuture.completedFuture("1"), (a, b) ->
                                a.thenCombine(b, (x, y) -> String.valueOf(Integer.parseInt(x) + Integer.parseInt(y))));
                    }
                }));
            }
            for (Future<?> result : results) {
                result.get(20, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
        assertThat(stored(first, "counter")).isEqualTo("100");
    }

    @Test
    @SuppressWarnings("RedundantCollectionOperation") // the views are under test
    void the_asMap_views_include_loads_in_flight_and_removals_invalidate_globally() throws Exception {
        DistributedAsyncCache<String, String> first = cache();
        DistributedAsyncCache<String, String> second = cache();
        CompletableFuture<String> pending = new CompletableFuture<>();
        first.synchronous().put("done", "d");
        first.put("pending", pending);

        assertThat(first.asMap()).hasSize(2);
        assertThat(first.asMap().keySet()).containsExactlyInAnyOrder("done", "pending");
        assertThat(get(first.asMap().get("pending").thenApply(value -> value)
                .completeOnTimeout("timeout", 50, TimeUnit.MILLISECONDS))).isEqualTo("timeout");

        first.asMap().keySet().remove("done");
        assertThat(stored(second, "done")).isNull();
        pending.complete("p");
        await(() -> "p".equals(stored(second, "pending")));
    }

    @Test
    void the_asMap_writes_go_through_the_store() throws Exception {
        DistributedAsyncCache<String, String> first = cache();
        DistributedAsyncCache<String, String> second = cache();
        ConcurrentMap<String, CompletableFuture<String>> map = second.asMap();

        assertThat(map.put("a", CompletableFuture.completedFuture("1"))).isNull();
        await(() -> "1".equals(stored(first, "a")));
        assertThat(get(requireNonNull(map.put("a", CompletableFuture.completedFuture("2"))))).isEqualTo("1");
        map.putAll(Map.of("b", CompletableFuture.completedFuture("b1"), "c", CompletableFuture.completedFuture("c1")));
        await(() -> "2".equals(stored(first, "a")) && "b1".equals(stored(first, "b"))
                && "c1".equals(stored(first, "c")));

        assertThat(get(requireNonNull(map.replace("a", CompletableFuture.completedFuture("3"))))).isEqualTo("2");
        assertThat(stored(first, "a")).isEqualTo("3");
        assertThat(map.replace("missing", CompletableFuture.completedFuture("x"))).isNull();
        assertThat(stored(first, "missing")).isNull();

        assertThat(map.remove("a", CompletableFuture.completedFuture("other"))).isFalse();
        assertThat(map.remove("a", CompletableFuture.completedFuture("3"))).isTrue();
        assertThat(stored(first, "a")).isNull();

        assertThat(get(requireNonNull(map.computeIfPresent("b", (key, value) -> value.thenApply(v -> v + "+")))))
                .isEqualTo("b1+");
        assertThat(stored(first, "b")).isEqualTo("b1+");
        assertThat(map.computeIfPresent("missing", (key, value) -> CompletableFuture.completedFuture("x"))).isNull();
        assertThat(map.computeIfPresent("b", (key, value) -> null)).isNull();
        assertThat(stored(first, "b")).isNull();

        assertThat(get(requireNonNull(map.compute("d", (key, value) ->
                CompletableFuture.completedFuture(value == null ? "new" : "old"))))).isEqualTo("new");
        assertThat(stored(first, "d")).isEqualTo("new");
        assertThat(map.compute("d", (key, value) -> null)).isNull();
        assertThat(stored(first, "d")).isNull();

        map.clear();
        assertThat(stored(first, "c")).isNull();
        assertThat(map).isEmpty();
    }

    @Test
    @SuppressWarnings("RedundantCollectionOperation") // the views are under test
    void the_asMap_entries_write_through_and_removals_through_the_views_invalidate_globally() throws Exception {
        DistributedAsyncCache<String, String> first = cache();
        DistributedAsyncCache<String, String> second = cache();
        second.synchronous().putAll(Map.of("a", "1", "b", "2", "c", "3"));
        ConcurrentMap<String, CompletableFuture<String>> map = second.asMap();

        for (Map.Entry<String, CompletableFuture<String>> entry : map.entrySet()) {
            if ("a".equals(entry.getKey())) {
                entry.setValue(CompletableFuture.completedFuture("1+"));
            }
        }
        await(() -> "1+".equals(stored(first, "a")));

        assertThat(map.entrySet().remove(Map.entry("b", CompletableFuture.completedFuture("2")))).isTrue();
        assertThat(stored(first, "b")).isNull();

        Iterator<CompletableFuture<String>> values = map.values().iterator();
        while (values.hasNext()) {
            if ("3".equals(get(values.next()))) {
                values.remove();
                assertThatThrownBy(values::remove).isInstanceOf(IllegalStateException.class);
            }
        }
        assertThat(stored(first, "c")).isNull();
        assertThat(map.values()).hasSize(1);

        map.values().clear();
        assertThat(stored(first, "a")).isNull();
        assertThat(map).isEmpty();
    }

    @Test
    void every_operation_refuses_after_close() {
        DistributedAsyncLoadingCache<String, String> cache = loading((key, executor) ->
                CompletableFuture.completedFuture("v"));
        ConcurrentMap<String, CompletableFuture<String>> map = cache.asMap();
        cache.close();

        assertThatThrownBy(() -> cache.get("k")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> cache.getIfPresent("k")).isInstanceOf(IllegalStateException.class);
        CompletableFuture<String> value = CompletableFuture.completedFuture("v");
        List<String> keys = List.of("k");
        assertThatThrownBy(() -> cache.put("k", value)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> cache.getAll(keys)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(map::size).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(cache::asMap).isInstanceOf(IllegalStateException.class);
    }
}
