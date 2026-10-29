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

import com.github.benmanes.caffeine.cache.Expiry;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryStore;
import io.github.oberhoff.distributedcaffeine.testing.StoreProfile;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The core against the behaviours of real stores that the in-memory store's profiles describe (VISION.md, "In-memory
 * store profiles", dimensions 1 to 7), one behaviour per test, each at the value that challenges the core. The whole
 * suite runs under every profile as well ({@code inmemory.profile}); these tests pin the cases a randomized run hits
 * only by luck, and the events the property test's oracles cannot hold under (a failover, an eviction, a message lost
 * silently).
 */
@NullMarked
@Timeout(60)
@SuppressWarnings("resource") // every cache a test opens is closed after it (@AfterEach)
class InternalStoreProfileTest {

    private static final Duration LONG = Duration.ofHours(1);
    // the hash of the key "k", for evicting its row
    private static final String K_HASH = new InternalHasher<String>(null).wrap("k").hash();

    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private DistributedCache<String, String> cache(InMemoryDatabase database) {
        return cache(database, UnaryOperator.identity());
    }

    private DistributedCache<String, String> cache(InMemoryDatabase database,
                                                   UnaryOperator<DistributedCaffeine<String, String>> configuring) {
        return cache(database, LONG, configuring);
    }

    private DistributedCache<String, String> cache(InMemoryDatabase database, Duration maximumAge,
                                                   UnaryOperator<DistributedCaffeine<String, String>> configuring) {
        DistributedCache<String, String> cache = configuring.apply(DistributedCaffeine.<String, String>newBuilder(
                        settings -> database.openStore("cache"))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(maximumAge).withMaximumSize(1_000)
                        .withPollInterval(Duration.ofMillis(10)))).build();
        closeables.add(cache);
        return cache;
    }

    private static @Nullable String stored(DistributedCache<String, String> cache, String key) {
        StoredValue<String> stored = cache.distributedPolicy().getFromStore(key);
        return stored == null ? null : stored.value();
    }

    // dimension 1: the store's clock

    // a put after invalidateAll returns is written after the control row by a clock counting in microseconds or
    // milliseconds, repeats allowed: invalidateAll waits the store's unit (also less than a millisecond)
    @ParameterizedTest
    @EnumSource(value = StoreProfile.Clock.class, names = {"MICROS", "MILLIS"})
    void a_put_after_invalidateAll_returns_is_never_hidden_by_it(StoreProfile.Clock clock) {
        InMemoryDatabase database = new InMemoryDatabase(StoreProfile.POSTGRESQL.with(clock, Duration.ZERO));
        DistributedCache<String, String> cache = cache(database);
        for (int i = 0; i < 50; i++) {
            cache.invalidateAll();
            cache.put("k" + i, "v");
            assertThat(stored(cache, "k" + i)).as("put %d", i).isEqualTo("v");
        }
    }

    // a clock stepping back by less than it declares changes nothing; by more, a put after invalidateAll is stamped
    // before the control row and hidden: the edge the allowance bounds (Store#clockAllowance)
    @Test
    void a_clock_stepping_back_within_its_allowance_hides_nothing_and_beyond_it_hides_a_later_put() {
        InMemoryDatabase database = new InMemoryDatabase(StoreProfile.POSTGRESQL_STEPPING);
        DistributedCache<String, String> cache = cache(database);
        cache.invalidateAll();
        database.stepClockBack(Duration.ofMillis(4));
        cache.put("within", "v");
        assertThat(stored(cache, "within")).isEqualTo("v");

        cache.invalidateAll();
        database.stepClockBack(Duration.ofMillis(50));
        cache.put("beyond", "v");
        assertThat(stored(cache, "beyond")).as("hidden by the control row, stamped after it").isNull();
    }

    // a write concurrent with an invalidateAll, committed after the control row but within the clock's allowance of
    // it, is stamped no later than it (here: a clock standing still) and hidden by the store; its writer holds it only
    // until the allowance has passed, then checks it and drops it
    @Test
    void a_put_hidden_by_a_concurrent_invalidateAll_is_not_served_after_the_allowance() throws Exception {
        AtomicLong micros = new AtomicLong(ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now()));
        InMemoryDatabase database = new InMemoryDatabase(micros::get, 1,
                StoreProfile.MONGODB.with(StoreProfile.Clock.MILLIS, Duration.ofMillis(200)));
        DistributedCache<String, String> clearing = cache(database);
        DistributedCache<String, String> writer = cache(database,
                builder -> builder.withCaffeine(CaffeineConfigurer::recordStats));
        Thread invalidating = new Thread(clearing::invalidateAll);
        invalidating.start();
        await(() -> writer.distributedPolicy().stats().invalidatedAllMessageCount() == 1);
        writer.put("k", "v"); // within the allowance, at the control row's time: hidden
        assertThat(stored(writer, "k")).isNull();
        invalidating.join();

        micros.addAndGet(1_000_000);
        await(() -> writer.getIfPresent("k") == null);
        assertThat(writer.get("k", ignored -> "loaded")).isEqualTo("loaded");
    }

    // dimension 0 with dimension 1: a store polled by the core. The poll window is a duration back from the store's
    // now, open towards the future, so a clock stepping back costs nothing: a write after the step is young, one before
    // it has a negative age, both inside the window. (A step forward by more than the overlap would miss the writes
    // between the last poll and the step; not provoked here, it depends on when the poll runs)
    @Test
    void a_polled_store_whose_clock_steps_back_misses_no_write() {
        InMemoryDatabase database = new InMemoryDatabase(StoreProfile.JDBC);
        DistributedCache<String, String> writer = cache(database);
        DistributedCache<String, String> holder = cache(database);
        writer.put("k", "v1");
        await(database::isIdle);
        assertThat(holder.get("k", ignored -> "loaded")).isEqualTo("v1");

        database.stepClockBack(Duration.ofSeconds(5));
        writer.put("k", "v2");
        await(() -> !"v1".equals(holder.getIfPresent("k")));
        assertThat(holder.get("k", ignored -> "loaded")).isEqualTo("v2");
    }

    // dimension 2: a read of several keys that is not one snapshot

    // the rows of one read seen at different instants (MongoDB's $in): an invalidateAll and a put of a key landing
    // between the rows, the key after the control row by its write time. Its age, negative against the read's instant,
    // still compares as younger than the control row's (an age raised to zero would tie, hide the put, and let a load
    // replace it)
    @Test
    void a_put_landing_between_the_rows_of_a_read_after_an_invalidateAll_stays_visible() {
        InMemoryDatabase database = new InMemoryDatabase(StoreProfile.MONGODB);
        DistributedCache<String, String> writer = cache(database);
        DistributedCache<String, String> reader = cache(database);
        writer.put("k1", "old1");
        writer.put("k2", "old2");
        AtomicBoolean once = new AtomicBoolean();
        database.setBetweenRows(() -> {
            if (once.compareAndSet(false, true)) {
                writer.invalidateAll();
                writer.put("k2", "new");
            }
        });
        Map<String, String> read = reader.getAll(List.of("k1", "k2"), keys -> {
            Map<String, String> loaded = new LinkedHashMap<>();
            keys.forEach(key -> loaded.put(key, "loaded-" + key));
            return loaded;
        });
        database.setBetweenRows(null);
        assertThat(once).isTrue();
        assertThat(read).containsEntry("k1", "loaded-k1").containsEntry("k2", "new");
        assertThat(stored(writer, "k2")).isEqualTo("new");
    }

    // dimension 3: a batch applied in part

    static Stream<StoreProfile> perRowBatches() {
        return Stream.of(StoreProfile.JDBC.with(StoreProfile.Changes.FEED), StoreProfile.MONGODB);
    }

    // a putAll or an invalidateAll of keys failing after part of it applied: every key's outcome unknown, and every
    // instance ends where the store is
    @ParameterizedTest
    @MethodSource("perRowBatches")
    void a_batch_applied_in_part_leaves_every_instance_where_the_store_is(StoreProfile profile) {
        InMemoryDatabase database = new InMemoryDatabase(profile);
        DistributedCache<String, String> writer = cache(database);
        DistributedCache<String, String> holder = cache(database);
        List<String> keys = List.of("k1", "k2", "k3", "k4");
        keys.forEach(key -> writer.put(key, "old"));
        keys.forEach(key -> assertThat(holder.get(key, ignored -> "loaded")).isEqualTo("old"));

        database.failNextWritesPartly(1);
        Map<String, String> values = new LinkedHashMap<>();
        keys.forEach(key -> values.put(key, "new"));
        assertThatThrownBy(() -> writer.putAll(values)).isInstanceOf(StoreException.class);
        assertConverged(database, keys, writer, holder);
        assertThat(keys.stream().map(key -> stored(writer, key))).contains("new", "old");

        database.failNextWritesPartly(1);
        assertThatThrownBy(() -> writer.invalidateAll(keys)).isInstanceOf(StoreException.class);
        assertConverged(database, keys, writer, holder);
        assertThat(keys.stream().map(key -> stored(writer, key))).containsNull();
    }

    @SafeVarargs
    private static void assertConverged(InMemoryDatabase database, List<String> keys,
                                        DistributedCache<String, String>... caches) {
        await(database::isIdle);
        for (String key : keys) {
            String stored = stored(caches[0], key);
            for (DistributedCache<String, String> cache : caches) {
                String held = cache.getIfPresent(key);
                assertThat(held == null || held.equals(stored)).as("%s held %s, stored %s", key, held, stored)
                        .isTrue();
            }
        }
    }

    // dimension 4: a failover losing acknowledged writes

    // a put acknowledged and held on two instances, then lost in a failover: the gap makes both revalidate, and the
    // value the store no longer has goes (equality, not order, decides); a later write is taken by both
    @Test
    void a_write_lost_in_a_failover_is_dropped_by_every_holder() throws Exception {
        InMemoryDatabase database = new InMemoryDatabase(StoreProfile.MONGODB);
        DistributedCache<String, String> writer = cache(database);
        DistributedCache<String, String> holder = cache(database);
        writer.put("k", "kept");
        Thread.sleep(200);
        writer.put("k", "lost");
        assertThat(holder.get("k", ignored -> "loaded")).isEqualTo("lost");

        database.failover(Duration.ofMillis(100));
        assertThat(stored(writer, "k")).isEqualTo("kept");
        await(() -> !"lost".equals(writer.getIfPresent("k")) && !"lost".equals(holder.getIfPresent("k")));
        assertThat(holder.get("k", ignored -> "loaded")).isEqualTo("kept");

        writer.put("k", "after");
        await(() -> "after".equals(holder.getIfPresent("k")) || holder.getIfPresent("k") == null);
        assertThat(holder.get("k", ignored -> "loaded")).isEqualTo("after");
    }

    // dimension 5: a write conditioned on a version of a purged row

    // the store leaves a tombstone (MongoDB's upsert) or nothing: either way the load conditioned on the row it read
    // is refused, reads again, and installs what it loads then
    @ParameterizedTest
    @EnumSource(value = Tombstone.class)
    void a_load_conditioned_on_a_purged_row_is_refused_and_loads_again(Tombstone tombstone) {
        InMemoryDatabase database = new InMemoryDatabase(StoreProfile.JDBC.with(StoreProfile.Changes.FEED)
                .withTombstoneOnPurgedRow(tombstone == Tombstone.LEFT));
        DistributedCache<String, String> cache = cache(database);
        cache.invalidate("k"); // a tombstone the load reads and conditions its write on
        AtomicBoolean purged = new AtomicBoolean();
        String loaded = cache.get("k", key -> {
            if (purged.compareAndSet(false, true)) {
                database.evict(K_HASH); // purged while the source is read
            }
            return "loaded";
        });
        assertThat(loaded).isEqualTo("loaded");
        assertThat(stored(cache, "k")).isEqualTo("loaded");
    }

    enum Tombstone {
        LEFT, NONE
    }

    // dimension 6: rows deleted silently

    static Stream<StoreProfile> evicting() {
        return Stream.of(StoreProfile.REDIS_HOSTILE, StoreProfile.POSTGRESQL, StoreProfile.MONGODB,
                StoreProfile.JDBC.with(StoreProfile.Changes.FEED));
    }

    // a holder may keep a value the store forgot (no newer write exists); a write after the eviction starts the row
    // again, without the version it had, and every holder takes it, also one at once (the same millisecond as the
    // evicted write, whose proposal may lie below the held version: the holder then asks the store)
    @ParameterizedTest
    @MethodSource("evicting")
    void a_write_after_an_eviction_reaches_every_holder(StoreProfile profile) {
        InMemoryDatabase database = new InMemoryDatabase(profile);
        DistributedCache<String, String> writer = cache(database);
        DistributedCache<String, String> holder = cache(database);
        writer.put("k", "v1");
        // its own message delivered first: one arriving during the holder's load drops the loaded copy (its caller
        // still gets the value)
        await(database::isIdle);
        assertThat(holder.get("k", ignored -> "loaded")).isEqualTo("v1");

        database.evict(K_HASH);
        DistributedCache<String, String> another = cache(database);
        another.put("k", "v2");
        await(() -> "v2".equals(holder.getIfPresent("k")) || holder.getIfPresent("k") == null);
        assertThat(holder.get("k", ignored -> "loaded")).isEqualTo("v2");
        await(() -> "v2".equals(writer.getIfPresent("k")) || writer.getIfPresent("k") == null);
        assertThat(writer.get("k", ignored -> "loaded")).isEqualTo("v2");
    }

    // the same, provoked: after the eviction a write applies below the version the holder keeps, as a proposal from
    // the same millisecond or a clock behind can. The message, below the held version, would be taken for an older
    // write; the holder asks the store and takes what it holds now
    @Test
    void a_write_after_an_eviction_below_the_held_version_reaches_the_holder() {
        InMemoryDatabase database = new InMemoryDatabase(StoreProfile.JDBC.with(StoreProfile.Changes.FEED));
        DistributedCache<String, String> writer = cache(database);
        DistributedCache<String, String> holder = cache(database);
        writer.put("k", "v1");
        await(database::isIdle);
        assertThat(holder.get("k", ignored -> "loaded")).isEqualTo("v1");
        try (InMemoryStore store = database.openFeed("cache")) {
            long held = requireNonNull(store.read(List.of(K_HASH), LONG).get(K_HASH)).version();

            database.evict(K_HASH);
            store.write(List.of(Write.value(K_HASH, "k".getBytes(StandardCharsets.UTF_8),
                    "v2".getBytes(StandardCharsets.UTF_8), held - 1)));
        }
        await(() -> !"v1".equals(holder.getIfPresent("k")));
        assertThat(holder.get("k", ignored -> "loaded")).isEqualTo("v2");
    }

    // dimension 7: a feed losing messages silently

    // nothing tells the instances: a holder serves the old value until its local copy expires, at the store's
    // maximum age at the latest (without an expiry of the application's too): the cost of such a feed, bounded
    @Test
    void a_message_lost_silently_leaves_a_holder_stale_at_most_until_the_maximum_age() {
        InMemoryDatabase database = new InMemoryDatabase(StoreProfile.REDIS_HOSTILE);
        Duration maximumAge = Duration.ofMillis(300);
        DistributedCache<String, String> writer = cache(database, maximumAge, UnaryOperator.identity());
        DistributedCache<String, String> holder = cache(database, maximumAge, UnaryOperator.identity());
        writer.put("k", "v1");
        await(database::isIdle);
        assertThat(holder.get("k", ignored -> "loaded")).isEqualTo("v1");

        database.setDropProbability(1);
        writer.put("k", "v2");
        await(database::isIdle);
        database.setDropProbability(0);
        assertThat(holder.getIfPresent("k")).as("stale: the message was lost").isEqualTo("v1");
        await(() -> !"v1".equals(holder.getIfPresent("k")));
        assertThat(holder.get("k", ignored -> "loaded")).isNotEqualTo("v1");
    }

    // the application's expiry never keeps a copy beyond the store's maximum age: a longer expireAfterWrite is
    // lowered to it, and an Expiry lengthening a copy on every read stops at it
    @Test
    void no_expiry_of_the_application_keeps_a_copy_beyond_the_maximum_age() {
        InMemoryDatabase database = new InMemoryDatabase(StoreProfile.POSTGRESQL);
        Duration maximumAge = Duration.ofMillis(300);
        DistributedCache<String, String> longer = cache(database, maximumAge,
                builder -> builder.withCaffeine(caffeine -> caffeine.expireAfterWrite(LONG)));
        DistributedCache<String, String> lengthening = cache(database, maximumAge,
                builder -> builder.withCaffeine(caffeine -> caffeine.expireAfter(new Expiry<>() {
                    @Override
                    public long expireAfterCreate(String key, String value, long currentTime) {
                        return Long.MAX_VALUE;
                    }

                    @Override
                    public long expireAfterUpdate(String key, String value, long currentTime, long currentDuration) {
                        return Long.MAX_VALUE;
                    }

                    @Override
                    public long expireAfterRead(String key, String value, long currentTime, long currentDuration) {
                        return Long.MAX_VALUE;
                    }
                })));
        longer.put("k", "v");
        lengthening.put("k", "v");
        long start = System.nanoTime();
        await(() -> longer.getIfPresent("k") == null && lengthening.getIfPresent("k") == null); // read on and on
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
    }
}
