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
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code asMap()}: one-key methods through L2, the conditional methods atomic across instances, the views local with
 * global removals; and the write paths' ordering against messages announced while a write is in flight.
 */
@Timeout(30)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
class InternalMapTest {

    private static final Duration LONG = Duration.ofHours(1);

    private final InMemoryDatabase database = new InMemoryDatabase();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        database.resumeDelivery();
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private DistributedCaffeine<String, String> builder(Function<Store, Store> wrapping) {
        Adapter<String, String> adapter = settings -> wrapping.apply(database.openStore("cache"));
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000));
    }

    private DistributedCache<String, String> cache() {
        return track(builder(Function.identity()).build());
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    // the value this instance holds for "k"
    private static @Nullable String held(DistributedCache<String, String> cache) {
        InternalCore<String, String> core = ((InternalDistributedCache<String, String>) cache).core;
        CompletableFuture<InternalValue<String>> future = core.l1().asMap().get(core.wrap("k"));
        InternalValue<String> value = future == null || !future.isDone() ? null : future.join();
        return value == null ? null : value.value();
    }

    private static @Nullable String stored(DistributedCache<String, String> cache, String key) {
        StoredValue<String> stored = cache.distributedPolicy().getFromStore(key);
        return stored == null ? null : stored.value();
    }

    private static void settle() {
        pause(300);
    }

    // one key, through L2

    @Test
    void get_and_getOrDefault_read_through_the_store() {
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> reader = cache();
        writer.put("k", "v");

        assertThat(reader.asMap()).containsEntry("k", "v");
        assertThat(reader.asMap().getOrDefault("missing", "default")).isEqualTo("default");
        assertThat(held(reader)).as("a hit is held, as for getIfPresent").isEqualTo("v");
    }

    @Test
    void containsKey_checks_the_store_without_holding_the_value() {
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> reader = cache();
        writer.put("k", "v");

        assertThat(reader.asMap()).containsKey("k");
        assertThat(reader.asMap().containsKey("missing")).isFalse();
        assertThat(held(reader)).as("an existence check holds nothing").isNull();

        writer.invalidate("k");
        assertThat(reader.asMap().containsKey("k")).isFalse();
    }

    @Test
    void containsKey_answers_from_a_trusted_held_value_without_the_store() {
        AtomicInteger versionReads = new AtomicInteger();
        DistributedCache<String, String> cache = track(builder(store -> new InternalCoreTest.DelegatingStore(store) {
            @Override
            public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
                versionReads.incrementAndGet();
                return super.readVersions(hashes, maxAge);
            }
        }).build());
        cache.put("k", "v");

        assertThat(cache.asMap()).containsKey("k");
        assertThat(versionReads).hasValue(0);
    }

    @Test
    void containsKey_checks_an_untrusted_held_value_against_the_store() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        first.put("k", "v");
        assertThat(second.getIfPresent("k")).isEqualTo("v");
        // delivery stops: what second holds is no longer trusted, and the invalidation does not reach it
        database.interruptSubscriptions();
        // the interruption reaches the cache through its delivery queue
        await(() -> second.distributedPolicy().getSynchronizationState() == SynchronizationState.DEGRADED);
        first.invalidate("k");

        assertThat(second.asMap().containsKey("k")).isFalse();
        assertThat(held(second)).as("dropped by the same check").isNull();
    }

    @Test
    void a_conditional_removal_does_not_remove_a_value_written_after_the_decision() {
        AtomicBoolean interfere = new AtomicBoolean(true);
        DistributedCache<String, String> other = cache();
        DistributedCache<String, String> remover = track(builder(store -> new InternalCoreTest.DelegatingStore(store) {
            @Override
            public Set<String> write(Collection<Write> writes) {
                // before the conditional removal is applied
                if (writes.stream().anyMatch(write -> write.isTombstone() && !write.isUnconditional())
                        && interfere.getAndSet(false)) {
                    other.put("k", "newer");
                }
                return super.write(writes);
            }
        }).build());
        other.put("k", "v");

        assertThat(remover.asMap().remove("k", "v")).as("decided again on the newer value").isFalse();
        assertThat(stored(other, "k")).isEqualTo("newer");
    }

    @Test
    void put_and_remove_return_the_previous_value_and_act_globally() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();

        assertThat(first.asMap().put("k", "v1")).isNull();
        assertThat(second.asMap().put("k", "v2")).isEqualTo("v1");
        assertThat(first.asMap().remove("k")).isEqualTo("v2");

        assertThat(stored(second, "k")).isNull();
        await(() -> held(second) == null);
    }

    @Test
    void remove_without_a_value_still_defeats_a_load_in_flight() throws Exception {
        java.util.concurrent.atomic.AtomicReference<String> source = new java.util.concurrent.atomic.AtomicReference<>("old");
        CountDownLatch called = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        DistributedLoadingCache<String, String> loader = track(builder(Function.identity()).build(key -> {
            String value = source.get();
            if (calls.incrementAndGet() == 1) {
                called.countDown();
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            }
            return value;
        }));
        DistributedCache<String, String> remover = cache();
        CompletableFuture<String> load = CompletableFuture.supplyAsync(() -> loader.get("k"));
        assertThat(called.await(5, TimeUnit.SECONDS)).isTrue();

        // the record is deleted after the loader read it; the store holds no value, so remove finds nothing
        source.set(null);
        assertThat(remover.asMap().remove("k")).isNull();
        release.countDown();

        assertThat(load.get(5, TimeUnit.SECONDS)).as("its write conflicted, the source was read again").isNull();
        assertThat(stored(remover, "k")).isNull();
    }

    @Test
    void putIfAbsent_writes_only_when_the_store_holds_no_value() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();

        assertThat(first.asMap().putIfAbsent("k", "first")).isNull();
        assertThat(second.asMap().putIfAbsent("k", "second")).as("not held there, found in the store")
                .isEqualTo("first");
        assertThat(stored(first, "k")).isEqualTo("first");

        first.invalidate("k");
        assertThat(second.asMap().putIfAbsent("k", "again")).as("a tombstone holds no value").isNull();
        assertThat(stored(first, "k")).isEqualTo("again");
    }

    @Test
    void putIfAbsent_from_many_threads_on_two_instances_has_exactly_one_winner() throws Exception {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                ConcurrentMap<String, String> map = (i % 2 == 0 ? first : second).asMap();
                String value = "v" + i;
                results.add(executor.submit(() -> {
                    start.await();
                    String previous = map.putIfAbsent("k", value);
                    return previous == null ? value : null;
                }));
            }
            start.countDown();
            Set<String> winners = new HashSet<>();
            for (Future<String> result : results) {
                String winner = result.get(10, TimeUnit.SECONDS);
                if (winner != null) {
                    winners.add(winner);
                }
            }
            assertThat(winners).hasSize(1);
            assertThat(stored(first, "k")).isEqualTo(winners.iterator().next());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void merge_counts_without_losing_updates_across_instances() throws Exception {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                ConcurrentMap<String, String> map = (i % 2 == 0 ? first : second).asMap();
                results.add(executor.submit(() -> {
                    for (int n = 0; n < 50; n++) {
                        map.merge("counter", "1", (a, b) -> String.valueOf(Integer.parseInt(a) + Integer.parseInt(b)));
                    }
                }));
            }
            for (Future<?> result : results) {
                result.get(20, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
        assertThat(stored(first, "counter")).isEqualTo("400");
        await(() -> "400".equals(first.getIfPresent("counter")) && "400".equals(second.getIfPresent("counter")));
    }

    @Test
    void a_conditional_method_on_an_outdated_held_value_decides_again_on_the_newer_one() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        first.put("k", "v1");
        assertThat(second.getIfPresent("k")).isEqualTo("v1");
        // second keeps v1 as trusted, the message of v2 has not arrived
        database.pauseDelivery();
        first.put("k", "v2");

        assertThat(second.asMap().replace("k", "v1", "x")).as("the conflict shows v2").isFalse();
        assertThat(second.asMap().merge("k", "+", String::concat)).isEqualTo("v2+");
        assertThat(stored(first, "k")).isEqualTo("v2+");
        assertThat(held(second)).isEqualTo("v2+");
    }

    @Test
    void a_decision_to_change_nothing_on_an_outdated_held_value_is_made_again_on_the_newer_one() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        first.put("k", "v1");
        first.put("gone", "g");
        // first keeps v1 and g as trusted, the messages of what follows have not arrived
        database.pauseDelivery();
        second.put("k", "v2");
        second.invalidate("gone");

        assertThat(first.asMap().replace("k", "v2", "v3")).as("v1 held, v2 stored").isTrue();
        assertThat(first.asMap().putIfAbsent("gone", "again")).as("held, but invalidated elsewhere").isNull();
        assertThat(stored(second, "k")).isEqualTo("v3");
        assertThat(stored(second, "gone")).isEqualTo("again");
    }

    @Test
    void replace_writes_only_over_a_value() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();

        assertThat(first.asMap().replace("k", "v")).isNull();
        assertThat(stored(first, "k")).as("nothing to replace").isNull();

        first.put("k", "v1");
        assertThat(second.asMap().replace("k", "v2")).isEqualTo("v1");
        assertThat(stored(first, "k")).isEqualTo("v2");

        assertThat(first.asMap().replace("k", "v1", "v3")).isFalse();
        assertThat(first.asMap().replace("k", "v2", "v3")).isTrue();
        assertThat(stored(second, "k")).isEqualTo("v3");
    }

    @Test
    void remove_with_a_value_removes_only_that_value() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        first.put("k", "v");

        assertThat(second.asMap().remove("k", "other")).isFalse();
        assertThat(stored(first, "k")).isEqualTo("v");

        assertThat(second.asMap().remove("k", "v")).isTrue();
        assertThat(stored(first, "k")).isNull();
        await(() -> held(first) == null);
        assertThat(second.asMap().remove("k", "v")).isFalse();
    }

    @Test
    void compute_methods_read_through_the_store_and_remove_on_null() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        first.put("k", "v");
        AtomicInteger calls = new AtomicInteger();

        assertThat(second.asMap().compute("k", (key, current) -> current + "!")).isEqualTo("v!");
        assertThat(second.asMap().computeIfPresent("missing", (key, current) -> {
            calls.incrementAndGet();
            return "x";
        })).isNull();
        assertThat(calls).as("nothing present, not called").hasValue(0);
        assertThat(second.asMap().computeIfPresent("k", (key, current) -> null)).isNull();
        assertThat(stored(first, "k")).isNull();
        assertThat(second.asMap().compute("absent", (key, current) -> null)).isNull();
        assertThat(stored(first, "absent")).isNull();
    }

    @Test
    void computeIfAbsent_is_get_with_a_mapping_function() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        first.put("k", "stored");
        AtomicInteger calls = new AtomicInteger();

        assertThat(second.asMap().computeIfAbsent("k", key -> "computed" + calls.incrementAndGet()))
                .isEqualTo("stored");
        assertThat(calls).as("found in the store").hasValue(0);
        assertThat(second.asMap().computeIfAbsent("new", key -> "computed" + calls.incrementAndGet()))
                .isEqualTo("computed1");
        assertThat(stored(first, "new")).isEqualTo("computed1");
    }

    @Test
    void a_conditional_write_with_an_unknown_outcome_drops_the_key_and_throws() {
        DistributedCache<String, String> cache = cache();
        cache.put("k", "v");
        database.failNextWrites(1, true);

        Map<String, String> map = cache.asMap();
        assertThatThrownBy(() -> map.replace("k", "x")).isInstanceOf(StoreException.class);
        assertThat(held(cache)).isNull();
        assertThat(cache.getIfPresent("k")).as("it landed").isEqualTo("x");
    }

    // the whole map, local

    @Test
    void sizes_iteration_and_views_show_what_this_instance_holds() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        first.put("a", "1");
        first.put("b", "2");
        assertThat(second.getIfPresent("a")).isEqualTo("1");

        assertThat(first.asMap()).hasSize(2).containsEntry("a", "1").containsEntry("b", "2");
        assertThat(second.asMap()).hasSize(1);
        assertThat(second.asMap().keySet()).containsExactly("a");
        assertThat(second.asMap().values()).containsExactly("1");
        assertThat(second.asMap().containsValue("2")).as("not held here").isFalse();
        assertThat(second.asMap().keySet()).doesNotContain("b");
        assertThat(second.asMap()).as("containsKey asks the store").containsKey("b");
    }

    @Test
    @SuppressWarnings({"RedundantCollectionOperation", "Java8CollectionRemoveIf"}) // the views are under test
    void removals_through_the_views_invalidate_globally() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        first.putAll(Map.of("a", "1", "b", "2", "c", "3", "d", "4"));

        for (Iterator<String> keys = first.asMap().keySet().iterator(); keys.hasNext(); ) {
            if (keys.next().equals("a")) {
                keys.remove();
            }
        }
        assertThat(stored(second, "a")).isNull();

        assertThat(first.asMap().entrySet().remove(Map.entry("b", "wrong"))).isFalse();
        assertThat(stored(second, "b")).isEqualTo("2");
        assertThat(first.asMap().entrySet().remove(Map.entry("b", "2"))).isTrue();
        assertThat(stored(second, "b")).isNull();

        first.asMap().keySet().remove("c");
        assertThat(stored(second, "c")).isNull();
        first.asMap().values().removeIf("4"::equals);
        assertThat(stored(second, "d")).isNull();
    }

    @Test
    void setValue_writes_and_clear_invalidates_everything() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        first.put("k", "v");
        second.put("other", "o");

        Map.Entry<String, String> entry = first.asMap().entrySet().iterator().next();
        assertThat(entry.setValue("w")).isEqualTo("v");
        assertThat(stored(second, "k")).isEqualTo("w");

        first.asMap().clear();
        assertThat(stored(second, "k")).isNull();
        assertThat(stored(second, "other")).as("global, not only what first held").isNull();
        await(() -> second.asMap().isEmpty());
    }

    @Test
    void the_views_refuse_use_after_close() {
        DistributedCache<String, String> cache = cache();
        ConcurrentMap<String, String> map = cache.asMap();
        cache.close();

        assertThatThrownBy(map::size).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> map.get("k")).isInstanceOf(IllegalStateException.class);
        Set<String> keySet = map.keySet();
        assertThatThrownBy(keySet::iterator).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(cache::asMap).isInstanceOf(IllegalStateException.class);
    }

    // writes in flight against messages

    @Test
    void a_put_overtaken_by_a_newer_write_elsewhere_before_it_commits_does_not_keep_its_value() {
        AtomicBoolean interfere = new AtomicBoolean(true);
        DistributedCache<String, String> other = cache();
        DistributedCache<String, String> writer = track(builder(store -> new InternalCoreTest.DelegatingStore(store) {
            @Override
            public Set<String> write(Collection<Write> writes) {
                Set<String> conflicts = super.write(writes);
                if (writes.stream().anyMatch(write -> !write.isTombstone()) && interfere.getAndSet(false)) {
                    // a newer write elsewhere, announced to the writer before it commits its own to L1
                    other.put("k", "newer");
                    settle();
                }
                return conflicts;
            }
        }).build());

        writer.put("k", "older");

        assertThat(held(writer)).isNotEqualTo("older");
        assertThat(writer.getIfPresent("k")).isEqualTo("newer");
    }

    @Test
    void a_put_overtaken_by_an_invalidateAll_before_it_commits_is_checked_on_its_next_read() {
        AtomicBoolean interfere = new AtomicBoolean(true);
        DistributedCache<String, String> other = cache();
        DistributedCache<String, String> writer = track(builder(store -> new InternalCoreTest.DelegatingStore(store) {
            @Override
            public Set<String> write(Collection<Write> writes) {
                Set<String> conflicts = super.write(writes);
                if (writes.stream().anyMatch(write -> !write.isTombstone()) && interfere.getAndSet(false)) {
                    other.invalidateAll();
                    settle();
                }
                return conflicts;
            }
        }).build());

        writer.put("k", "v");

        assertThat(writer.getIfPresent("k")).isNull();
    }

    @Test
    void a_compute_overtaken_by_a_newer_write_elsewhere_before_it_commits_does_not_keep_its_value() throws Exception {
        CountDownLatch computing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DistributedCache<String, String> other = cache();
        DistributedCache<String, String> writer = cache();
        other.put("k", "0");

        CompletableFuture<String> computed = CompletableFuture.supplyAsync(() ->
                writer.asMap().compute("k", (key, current) -> {
                    computing.countDown();
                    try {
                        assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return current + "+";
                }));
        try {
            assertThat(computing.await(5, TimeUnit.SECONDS)).isTrue();
            other.put("k", "elsewhere");
            settle();
        } finally {
            release.countDown();
        }

        assertThat(computed.join()).as("decided again after the conflict").isEqualTo("elsewhere+");
        assertThat(writer.getIfPresent("k")).isEqualTo("elsewhere+");
    }
}
