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

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.StoreProfile;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code distributedPolicy().stats()}: each counter at its source; and Caffeine's {@code stats()} counted from the
 * user's view ({@code getIfPresent} a hit or a miss, never a load).
 */
@NullMarked
@Timeout(30)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
class InternalStatsTest {

    private static final Duration LONG = Duration.ofHours(1);

    private final AtomicLong clockMicros = new AtomicLong(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
    private final InMemoryDatabase database = new InMemoryDatabase(clockMicros::get, 1);
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        database.setDown(false);
        database.resumeDelivery();
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private DistributedCaffeine<String, String> builder() {
        Adapter<String, String> adapter = settings -> database.openStore("cache");
        return DistributedCaffeine.newBuilder(adapter)
                .withCaffeine(CaffeineConfigurer::recordStats)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaximumAge(LONG).withMaximumSize(1_000));
    }

    private DistributedCache<String, String> cache() {
        return track(builder().build());
    }

    private DistributedLoadingCache<String, String> loading(Function<String, String> loader) {
        return track(builder().build(loader::apply));
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    // a store numbering its own versions: no proposal, and so no clock, reaches its rows
    private boolean ownVersions() {
        return database.profile().versioning() == StoreProfile.Versioning.ASSIGNING_OWN;
    }

    private static String hash(String key) {
        return new InternalHasher<String>(null).wrap(key).hash();
    }

    private static DistributedStats stats(DistributedCache<String, String> cache) {
        return cache.distributedPolicy().stats();
    }

    @Test
    void a_compute_writing_on_a_held_value_is_one_store_operation() {
        DistributedCache<String, String> cache = cache();
        cache.asMap().compute("k", (key, current) -> "1"); // read and write: held
        long reads = stats(cache).storeReadCount();
        long writes = stats(cache).storeWriteCount();

        cache.asMap().compute("k", (key, current) -> current + "2");
        assertThat(stats(cache).storeReadCount() - reads).as("decided on the held value").isZero();
        assertThat(stats(cache).storeWriteCount() - writes).isOne();

        cache.asMap().computeIfPresent("k", (key, current) -> current);
        assertThat(stats(cache).storeReadCount() - reads).as("writing nothing: checked by a read").isOne();
        assertThat(stats(cache).storeWriteCount() - writes).isOne();
        assertThat(cache.getIfPresent("k")).isEqualTo("12");
    }

    @Test
    void a_put_refused_by_a_version_from_a_clock_ahead_is_counted_and_then_applied_above_it() {
        // a row written by an instance whose clock runs an hour ahead: its version is above any this instance proposes
        long ahead = new InternalVersion().fresh() + (TimeUnit.HOURS.toMillis(1) << 20);
        track(database.openStore("cache")).write(List.of(Write.tombstone(hash("k"), ahead)));
        DistributedCache<String, String> cache = cache();

        // a store assigning versions refuses no advance: it applies the put above the row by itself
        long refusals = database.profile().assigns(1) ? 0 : 1;

        cache.put("k", "v");
        assertThat(stats(cache).versionRefusalCount()).as("refused once, then proposed above the row read")
                .isEqualTo(refusals);
        assertThat(cache().getIfPresent("k")).isEqualTo("v");

        cache.put("k", "w");
        assertThat(stats(cache).versionRefusalCount()).as("the clock has observed the version").isEqualTo(refusals);
        assertThat(stats(cache).plus(DistributedStats.empty()).minus(DistributedStats.empty()).versionRefusalCount())
                .as("carried by plus and minus").isEqualTo(refusals);
        if (!ownVersions()) {
            assertThat(stats(cache).clockAheadCount()).as("an hour ahead: seen and reported").isPositive();
        }
    }

    @Test
    void a_row_a_year_ahead_is_written_over_without_carrying_the_clock_along() {
        Assumptions.assumeFalse(ownVersions(), "a store numbering its own versions takes no proposal into a row");
        // a clock grossly wrong wrote it: followed, it would carry every version here a year ahead for good
        long farAhead = new InternalVersion().fresh() + (TimeUnit.DAYS.toMillis(365) << 20);
        track(database.openStore("cache")).write(List.of(Write.tombstone(hash("k"), farAhead)));
        DistributedCache<String, String> cache = cache();

        cache.put("k", "v");
        cache.put("other", "o");

        assertThat(cache().getIfPresent("k")).as("written above it").isEqualTo("v");
        assertThat(stats(cache).clockAheadCount()).isPositive();
        Map<String, Row> rows = track(database.openStore("cache")).read(List.of(hash("k"), hash("other")), LONG);
        assertThat(rows.get(hash("k")).version()).isGreaterThan(farAhead);
        assertThat(rows.get(hash("other")).version()).as("the clock not carried along").isLessThan(farAhead);
    }

    @Test
    void nothing_is_counted_unless_the_cache_records_stats() {
        Adapter<String, String> adapter = settings -> database.openStore("cache");
        DistributedCache<String, String> cache = track(DistributedCaffeine.newBuilder(adapter)
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaximumAge(LONG).withMaximumSize(1_000)).build());
        cache.put("k", "v");
        assertThat(cache.getIfPresent("missing")).isNull();

        assertThat(stats(cache)).isEqualTo(DistributedStats.empty());
    }

    @Test
    void caffeines_stats_count_getIfPresent_as_a_hit_or_a_miss_and_never_as_a_load() {
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> reader = cache();
        writer.put("stored", "v");

        assertThat(reader.getIfPresent("missing")).isNull();
        assertThat(reader.getIfPresent("stored")).isEqualTo("v");
        assertThat(reader.getIfPresent("stored")).isEqualTo("v");
        assertThat(reader.getAllPresent(List.of("stored", "missing", "other"))).isEqualTo(Map.of("stored", "v"));

        CacheStats caffeine = reader.stats();
        assertThat(caffeine.missCount()).isEqualTo(3);
        assertThat(caffeine.hitCount()).as("found in the store, then held").isEqualTo(3);
        assertThat(caffeine.loadCount()).isZero();
        assertThat(stats(reader).storeHitCount()).isEqualTo(1);
        assertThat(stats(reader).storeMissCount()).isEqualTo(3);
    }

    @Test
    void caffeines_stats_count_a_value_found_in_the_store_by_a_load_as_a_successful_load() {
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> reader = cache();
        writer.put("stored", "v");

        assertThat(reader.get("stored", key -> "source")).isEqualTo("v");
        assertThat(reader.get("new", key -> "source")).isEqualTo("source");

        CacheStats caffeine = reader.stats();
        assertThat(caffeine.missCount()).isEqualTo(2);
        assertThat(caffeine.loadSuccessCount()).isEqualTo(2);
        DistributedStats stats = stats(reader);
        assertThat(stats.storeHitCount()).isEqualTo(1);
        assertThat(stats.storeMissCount()).isEqualTo(1);
        assertThat(stats.sourceLoadCount()).as("only the key the store did not hold").isEqualTo(1);
        assertThat(stats.conditionalWriteCount()).isEqualTo(1);
    }

    @Test
    void a_refresh_counts_as_a_reload_from_the_source() throws Exception {
        DistributedLoadingCache<String, String> cache = loading(key -> "v-" + key);
        assertThat(cache.get("k")).isEqualTo("v-k");

        assertThat(cache.refresh("k").get(5, TimeUnit.SECONDS)).isEqualTo("v-k");

        assertThat(stats(cache).sourceReloadCount()).isEqualTo(1);
    }

    @Test
    void loads_in_bulk_count_per_key() {
        DistributedLoadingCache<String, String> writer = loading(key -> "v-" + key);
        DistributedLoadingCache<String, String> reader = loading(key -> "v-" + key);
        writer.put("a", "stored");

        assertThat(reader.getAll(List.of("a", "b", "c"))).isEqualTo(Map.of("a", "stored", "b", "v-b", "c", "v-c"));

        DistributedStats stats = stats(reader);
        assertThat(stats.storeHitCount()).isEqualTo(1);
        assertThat(stats.storeMissCount()).isEqualTo(2);
        assertThat(stats.sourceLoadCount()).isEqualTo(2);
    }

    @Test
    void writes_count_per_key_and_calls_are_timed() {
        DistributedCache<String, String> first = cache();
        DistributedCache<String, String> second = cache();
        first.put("k", "v1");
        first.putAll(Map.of("a", "1", "b", "2"));
        assertThat(second.getIfPresent("k")).isEqualTo("v1");
        // second decides on v1 and conflicts with v2, put between its read and its write
        AtomicBoolean once = new AtomicBoolean();
        database.setBeforeConditionalWrite(() -> {
            if (once.compareAndSet(false, true)) {
                first.put("k", "v2");
            }
        });

        second.asMap().merge("k", "+", String::concat);
        database.setBeforeConditionalWrite(null);

        DistributedStats firstStats = stats(first);
        assertThat(firstStats.putCount()).isEqualTo(4);
        assertThat(firstStats.storeWriteCount()).isEqualTo(3);
        assertThat(firstStats.totalStoreWriteTime()).isPositive();
        DistributedStats secondStats = stats(second);
        assertThat(secondStats.conditionalWriteCount()).isEqualTo(2);
        assertThat(secondStats.conflictCount()).isEqualTo(1);
        assertThat(secondStats.storeReadCount()).isPositive();
        assertThat(secondStats.totalStoreReadTime()).isPositive();
    }

    @Test
    void messages_count_by_kind_and_by_what_they_did_to_held_values() {
        assumeTrue(database.profile().changes() == StoreProfile.Changes.FEED, "counts the store's feed messages");
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = cache();
        writer.putAll(Map.of("a", "1", "b", "2"));
        assertThat(holder.getAllPresent(List.of("a", "b"))).isEqualTo(Map.of("a", "1", "b", "2"));

        writer.put("a", "new");
        writer.invalidate("b");
        writer.invalidateAll();

        // invalidateAll writes the control row once, announced once
        await(() -> stats(holder).invalidatedAllMessageCount() == 1);
        DistributedStats stats = stats(holder);
        assertThat(stats.writtenMessageCount()).isEqualTo(3);
        assertThat(stats.invalidatedMessageCount()).isEqualTo(1);
        assertThat(stats.replacedCount()).isEqualTo(1);
        assertThat(stats.invalidatedCount()).isEqualTo(1);
        await(() -> stats(writer).writtenMessageCount() == 3);
        assertThat(stats(writer).replacedCount()).as("its own echoes are not newer").isZero();
    }

    @Test
    void a_delayed_message_for_a_value_already_read_from_the_store_is_ignored() {
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = cache();
        database.pauseDelivery();
        writer.put("k", "v");
        assertThat(holder.getIfPresent("k")).isEqualTo("v");

        database.resumeDelivery();
        await(() -> stats(holder).writtenMessageCount() == 1);
        assertThat(stats(holder).ignoredMessageCount()).isEqualTo(1);
        assertThat(stats(holder).replacedCount()).isZero();
    }

    @Test
    void population_reads_and_expiry_are_counted() {
        assumeTrue(database.profile().changes() == StoreProfile.Changes.FEED, "duplicates the store's feed messages");
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = track(builder().withDistributionMode(DistributionMode.UPDATE)
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaintenanceInterval(Duration.ofMillis(50))).build());
        writer.put("k", "v1");
        assertThat(holder.getIfPresent("k")).isEqualTo("v1");

        // delivered twice: the duplicate finds the populated value and is ignored
        database.setDuplicateProbability(1.0);
        writer.put("k", "v2");
        await(() -> stats(holder).ignoredMessageCount() >= 1);
        database.setDuplicateProbability(0.0);
        // counted when the read starts: wait for its result before the clock moves on
        await(() -> "v2".equals(holder.policy().getIfPresentQuietly("k")));
        assertThat(stats(holder).populationReadCount()).isEqualTo(1);

        clockMicros.addAndGet(TimeUnit.HOURS.toMicros(2));
        await(() -> stats(holder).expiredCount() == 1);
        assertThat(stats(holder).expiredMessageCount()).isEqualTo(1);
    }

    @Test
    void recovery_counts_gaps_interruptions_and_revalidations() {
        assumeTrue(database.profile().changes() == StoreProfile.Changes.FEED, "counts the store's feed messages");
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = cache();
        writer.putAll(Map.of("kept", "1", "changed", "2"));
        assertThat(holder.getAllPresent(List.of("kept", "changed"))).isEqualTo(Map.of("kept", "1", "changed", "2"));

        database.interruptSubscriptions();
        await(() -> holder.distributedPolicy().getSynchronizationState() == SynchronizationState.DEGRADED);
        writer.put("changed", "3");
        assertThat(holder.getIfPresent("kept")).isEqualTo("1");
        assertThat(holder.getIfPresent("changed")).as("checked against the store").isEqualTo("3");
        database.resumeSubscriptions(false);
        await(() -> stats(holder).gapCount() == 1);
        // the background pass after the gap counts too: wait until it has finished
        InternalCore<String, String> core = ((InternalDistributedCache<String, String>) holder).core;
        await(core::isQuiescent);

        DistributedStats stats = stats(holder);
        assertThat(stats.interruptionCount()).isEqualTo(1);
        assertThat(stats.revalidatedCount()).isGreaterThanOrEqualTo(2);
        assertThat(stats.revalidationKeptCount()).isGreaterThanOrEqualTo(1);
        assertThat(stats.revalidationDroppedCount()).isEqualTo(1);
        assertThat(stats.revalidatedCount())
                .isEqualTo(stats.revalidationKeptCount() + stats.revalidationDroppedCount());
    }

    @Test
    void failures_unknown_outcomes_guard_openings_and_refusals_are_counted() {
        DistributedCache<String, String> cache = cache();
        database.failNextWrites(1, true);
        assertThatThrownBy(() -> cache.put("k", "v")).isInstanceOf(StoreException.class);
        assertThat(stats(cache).unknownOutcomeCount()).isEqualTo(1);
        assertThat(stats(cache).storeFailureCount()).isEqualTo(1);
        // a success in between, so that the guard counts the failures below from zero
        cache.put("k", "v");

        database.setDown(true);
        for (int i = 0; i < InternalStoreGuard.THRESHOLD; i++) {
            assertThatThrownBy(() -> cache.put("k", "v")).isInstanceOf(StoreException.class);
        }
        assertThatThrownBy(() -> cache.put("k", "v")).isInstanceOf(StoreException.class);

        DistributedStats stats = stats(cache);
        assertThat(stats.guardOpenCount()).isEqualTo(1);
        assertThat(stats.refusedCount()).isEqualTo(1);
        assertThat(stats.storeFailureCount()).isEqualTo(1 + InternalStoreGuard.THRESHOLD);
    }

    @Test
    void absent_hits_and_unreadable_rows_are_counted() {
        DistributedLoadingCache<String, String> cache = track(builder().withAbsentCaching(Duration.ofMinutes(1))
                .build(key -> null));
        assertThat(cache.get("missing")).isNull();
        assertThat(cache.get("missing")).isNull();
        assertThat(cache.getIfPresent("missing")).isNull();
        assertThat(stats(cache).absentHitCount()).as("by get and getIfPresent").isEqualTo(2);
        assertThat(stats(cache).sourceLoadCount()).isEqualTo(1);

        Adapter<String, Long> numbers = settings -> database.openStore("cache");
        DistributedCache<String, Long> reader = track(DistributedCaffeine.newBuilder(numbers)
                .withCaffeine(CaffeineConfigurer::recordStats)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofLong()))
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaximumAge(LONG).withMaximumSize(1_000)).build());
        cache.put("text", "not a number");
        assertThat(reader.getIfPresent("text")).isNull();
        assertThat(reader.distributedPolicy().stats().unreadableRowCount()).isEqualTo(1);
        assertThat(reader.distributedPolicy().stats().storeMissCount()).isEqualTo(1);
    }

    @Test
    void snapshots_add_and_subtract_and_name_their_counts() {
        assumeTrue(database.profile().changes() == StoreProfile.Changes.FEED, "counts the store's feed messages");
        DistributedCache<String, String> cache = cache();
        cache.put("a", "1");
        await(() -> stats(cache).writtenMessageCount() == 1 && database.isIdle());
        DistributedStats before = stats(cache);
        cache.put("b", "2");
        // after its own echo has been applied in full (the delivery idle only once the listener returned: every count
        // of it taken), so that nothing arrives later
        await(() -> stats(cache).writtenMessageCount() == 2 && database.isIdle());
        DistributedStats after = stats(cache);

        assertThat(after.minus(before).putCount()).isEqualTo(1);
        assertThat(before.minus(after).putCount()).as("never below zero").isZero();
        assertThat(before.plus(after).putCount()).isEqualTo(3);
        assertThat(after.plus(DistributedStats.empty())).isEqualTo(after);
        assertThat(after.toString()).contains("putCount=2", "storeHitCount=0");

        cache.close();
        assertThat(stats(cache)).as("answers after close").isEqualTo(after);
    }
}
