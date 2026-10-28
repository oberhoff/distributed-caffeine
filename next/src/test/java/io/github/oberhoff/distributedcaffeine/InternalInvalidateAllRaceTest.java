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
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Conditional writes racing an {@code invalidateAll} (VISION.md, design study, "Conditional writes racing an
 * {@code invalidateAll}"): one instance runs an operation, which reads, and before its conditional write another
 * instance calls {@code invalidateAll} (the in-memory store's pause between read and write). Per key the outcome must
 * be one of Caffeine's two orders: "the operation, then {@code invalidateAll}" (the key absent) or "{@code
 * invalidateAll}, then the operation" (the operation as on an absent key). Never a value derived from the state before
 * the {@code invalidateAll}.
 * <p>
 * Since the {@code invalidateAll} by the store's time (2026-10-28) this race is accepted, as Caffeine, memcached, Redis,
 * Hazelcast and FusionCache accept it: the tests closing it are disabled until its repair; those of own values written
 * after an unannounced {@code invalidateAll} still hold.
 */
@Timeout(30)
@SuppressWarnings("resource") // every cache a test opens is closed after it (@AfterEach)
class InternalInvalidateAllRaceTest {

    private static final Duration LONG = Duration.ofHours(1);
    // the race these tests close is accepted for now, as the industry accepts it: a load or an update that read before
    // another instance's invalidateAll and writes after it survives it (VISION.md, "Target design: invalidateAll by the
    // store's time, hidden on read"). They are the acceptance tests of its repair (build plan, "Later")
    private static final String RACE_ACCEPTED = "racing loads and updates are accepted until their repair (VISION.md, "
            + "build plan, Later)";

    private final InMemoryDatabase database = new InMemoryDatabase();
    private final CacheBackend backend = CacheBackend.of(database);
    private final Map<String, String> source = new ConcurrentHashMap<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        database.setBeforeConditionalWrite(null);
        database.setBeforeInvalidateAllApplies(null);
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        backend.close();
    }

    private DistributedCaffeine<String, String> builder() {
        Adapter<String, String> adapter = backend::open;
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000));
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private DistributedLoadingCache<String, String> loading() {
        return track(builder().build(source::get));
    }

    private DistributedCache<String, String> plain() {
        return track(builder().build());
    }

    // the next conditional write pauses for the source to change (if given) and the other instance's invalidateAll
    private AtomicBoolean invalidateAllBeforeTheNextConditionalWrite(DistributedCache<String, String> other,
                                                                     @Nullable String newSource) {
        AtomicBoolean once = new AtomicBoolean();
        database.setBeforeConditionalWrite(() -> {
            if (once.compareAndSet(false, true)) {
                if (newSource != null) {
                    source.put("k", newSource);
                }
                other.invalidateAll();
            }
        });
        return once;
    }

    private static @Nullable String stored(DistributedCache<String, String> cache) {
        StoredValue<String> stored = cache.distributedPolicy().getFromStore("k");
        return stored == null ? null : stored.value();
    }

    // reads through the source: a value read before the invalidateAll must not stay

    @Test
    @Disabled(RACE_ACCEPTED)
    void a_load_racing_an_invalidate_all_leaves_no_value_from_before_it() {
        DistributedLoadingCache<String, String> loader = loading();
        DistributedCache<String, String> other = plain();
        source.put("k", "old");
        AtomicBoolean raced = invalidateAllBeforeTheNextConditionalWrite(other, "new");

        loader.get("k");

        assertThat(stored(other)).isIn(null, "new");
        assertThat(raced).as("the invalidateAll ran between the read and the write").isTrue();
    }

    @Test
    @Disabled(RACE_ACCEPTED)
    void a_refresh_racing_an_invalidate_all_leaves_no_value_from_before_it() throws Exception {
        DistributedLoadingCache<String, String> loader = loading();
        DistributedCache<String, String> other = plain();
        source.put("k", "held");
        loader.get("k");
        source.put("k", "old");
        AtomicBoolean raced = invalidateAllBeforeTheNextConditionalWrite(other, "new");

        loader.refresh("k").get(10, TimeUnit.SECONDS);

        assertThat(stored(other)).isIn(null, "new");
        assertThat(raced).as("the invalidateAll ran between the read and the write").isTrue();
    }

    @Test
    @Disabled(RACE_ACCEPTED)
    void a_compute_if_absent_racing_an_invalidate_all_leaves_no_value_from_before_it() {
        DistributedCache<String, String> computing = plain();
        DistributedCache<String, String> other = plain();
        source.put("k", "old");
        AtomicBoolean raced = invalidateAllBeforeTheNextConditionalWrite(other, "new");

        computing.asMap().computeIfAbsent("k", source::get);

        assertThat(stored(other)).isIn(null, "new");
        assertThat(raced).as("the invalidateAll ran between the read and the write").isTrue();
    }

    // derived from the value present: after the invalidateAll the key is absent

    @Test
    @Disabled(RACE_ACCEPTED)
    void a_compute_on_a_present_value_racing_an_invalidate_all_does_not_derive_from_it() {
        DistributedCache<String, String> computing = plain();
        DistributedCache<String, String> other = plain();
        other.put("k", "v");
        AtomicBoolean raced = invalidateAllBeforeTheNextConditionalWrite(other, null);

        computing.asMap().compute("k", (key, current) -> current + "+");

        assertThat(stored(other)).as("absent, or computed as on an absent key").isIn(null, "null+");
        assertThat(raced).as("the invalidateAll ran between the read and the write").isTrue();
    }

    @Test
    @Disabled(RACE_ACCEPTED)
    void a_compute_if_present_racing_an_invalidate_all_leaves_the_key_absent() {
        DistributedCache<String, String> computing = plain();
        DistributedCache<String, String> other = plain();
        other.put("k", "v");
        AtomicBoolean raced = invalidateAllBeforeTheNextConditionalWrite(other, null);

        computing.asMap().computeIfPresent("k", (key, current) -> current + "+");

        assertThat(stored(other)).isNull();
        assertThat(raced).as("the invalidateAll ran between the read and the write").isTrue();
    }

    @Test
    @Disabled(RACE_ACCEPTED)
    void a_merge_on_a_present_value_racing_an_invalidate_all_does_not_derive_from_it() {
        DistributedCache<String, String> computing = plain();
        DistributedCache<String, String> other = plain();
        other.put("k", "v");
        AtomicBoolean raced = invalidateAllBeforeTheNextConditionalWrite(other, null);

        computing.asMap().merge("k", "1", String::concat);

        assertThat(stored(other)).as("absent, or the given value as on an absent key").isIn(null, "1");
        assertThat(raced).as("the invalidateAll ran between the read and the write").isTrue();
    }

    @Test
    @Disabled(RACE_ACCEPTED)
    void a_replace_racing_an_invalidate_all_leaves_the_key_absent() {
        DistributedCache<String, String> replacing = plain();
        DistributedCache<String, String> other = plain();
        other.put("k", "v");
        AtomicBoolean raced = invalidateAllBeforeTheNextConditionalWrite(other, null);

        replacing.asMap().replace("k", "x");

        assertThat(stored(other)).isNull();
        assertThat(raced).as("the invalidateAll ran between the read and the write").isTrue();
    }

    @Test
    @Disabled(RACE_ACCEPTED)
    void a_replace_of_a_value_racing_an_invalidate_all_leaves_the_key_absent() {
        DistributedCache<String, String> replacing = plain();
        DistributedCache<String, String> other = plain();
        other.put("k", "v");
        AtomicBoolean raced = invalidateAllBeforeTheNextConditionalWrite(other, null);

        replacing.asMap().replace("k", "v", "x");

        assertThat(stored(other)).isNull();
        assertThat(raced).as("the invalidateAll ran between the read and the write").isTrue();
    }

    @Test
    @Disabled(RACE_ACCEPTED)
    void a_load_racing_an_invalidate_all_whose_write_lands_with_its_outcome_unknown_leaves_no_value_from_before_it() {
        DistributedLoadingCache<String, String> loader = loading();
        DistributedCache<String, String> other = plain();
        source.put("k", "old");
        AtomicBoolean once = new AtomicBoolean();
        database.setBeforeConditionalWrite(() -> {
            if (once.compareAndSet(false, true)) {
                source.put("k", "new");
                other.invalidateAll();
                database.failNextWrites(1, true); // the write lands, its caller learns nothing
            }
        });

        loader.get("k");

        assertThat(stored(other)).as("landed unseen: hidden by its basis like any other").isIn(null, "new");
    }

    // the window between the time of a write and its commit

    @Test
    @Disabled(RACE_ACCEPTED)
    void a_load_writing_while_an_invalidate_all_commits_is_hidden_by_its_basis() throws Exception {
        CountDownLatch called = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DistributedLoadingCache<String, String> loader = track(builder().build(key -> {
            String value = source.get(key);
            called.countDown();
            if (!release.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the load was not released");
            }
            return value;
        }));
        DistributedCache<String, String> other = plain();
        source.put("k", "old");
        CompletableFuture<String> load = CompletableFuture.supplyAsync(() -> loader.get("k"));
        assertThat(called.await(10, TimeUnit.SECONDS)).isTrue();
        source.put("k", "new");
        AtomicBoolean once = new AtomicBoolean();
        // the control write has its stamp, not yet visible: the load writes in between, after it by its stamp, but
        // decided under the cut-off before it
        database.setBeforeInvalidateAllApplies(() -> {
            if (once.compareAndSet(false, true)) {
                release.countDown();
                load.join();
            }
        });

        other.invalidateAll();

        assertThat(load.get(10, TimeUnit.SECONDS)).as("the load's own").isEqualTo("old");
        assertThat(stored(other)).as("hidden by its basis").isNull();
    }

    // not racing at all: decided on a value written after the invalidateAll, whose message has not arrived yet

    @Test
    void a_compute_on_an_own_value_written_after_an_unannounced_invalidate_all_is_kept() {
        DistributedCache<String, String> writer = plain();
        DistributedCache<String, String> other = plain();
        database.pauseDelivery();
        other.invalidateAll();
        writer.put("k", "v");

        writer.asMap().computeIfPresent("k", (key, current) -> current + "+");
        database.resumeDelivery();

        assertThat(stored(other)).isEqualTo("v+");
    }

    @Test
    void a_refresh_of_an_own_value_written_after_an_unannounced_invalidate_all_is_kept() throws Exception {
        DistributedLoadingCache<String, String> loader = loading();
        DistributedCache<String, String> other = plain();
        database.pauseDelivery();
        other.invalidateAll();
        loader.put("k", "v");
        source.put("k", "refreshed");

        loader.refresh("k").get(10, TimeUnit.SECONDS);
        database.resumeDelivery();

        assertThat(stored(other)).isEqualTo("refreshed");
    }

    // valid in both orders

    @Test
    void a_put_if_absent_racing_an_invalidate_all_writes_or_leaves_the_key_absent() {
        DistributedCache<String, String> putting = plain();
        DistributedCache<String, String> other = plain();
        AtomicBoolean raced = invalidateAllBeforeTheNextConditionalWrite(other, null);

        putting.asMap().putIfAbsent("k", "x");

        assertThat(stored(other)).isIn(null, "x");
        assertThat(raced).as("the invalidateAll ran between the read and the write").isTrue();
    }
}
