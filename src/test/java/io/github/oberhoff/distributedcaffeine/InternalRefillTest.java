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
import io.github.oberhoff.distributedcaffeine.testing.StoreProfile;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A refill while serving (VISION.md, "Refill while serving"): {@code invalidateAllBeforeMark} hides every value
 * written before the mark and keeps every one written since, and the caches stay hot: copies are revalidated, not
 * dropped. {@code invalidateAllOlderThan} is the same with a mark taken by the call.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(60)
@SuppressWarnings("resource") // every cache a test opens is closed after it (@AfterEach)
class InternalRefillTest {

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private InMemoryDatabase database = new InMemoryDatabase();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private DistributedCache<String, String> cache(UnaryOperator<DistributedCaffeine<String, String>> configuration) {
        InMemoryDatabase opened = database;
        Adapter<String, String> adapter = settings -> opened.openStore("cache");
        DistributedCache<String, String> cache = configuration.apply(
                        DistributedCaffeine.newBuilder(InternalCoreTest.offered(adapter))
                                .withSerializers(serializers -> serializers
                                        .withKeySerializer(StringSerializer.ofString())
                                        .withValueSerializer(StringSerializer.ofString()))
                                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                                        .withMaximumAge(Duration.ofHours(1))))
                .build();
        closeables.add(cache);
        return cache;
    }

    private DistributedCache<String, String> cache() {
        return cache(UnaryOperator.identity());
    }

    private static InternalCore<String, String> core(DistributedCache<String, String> cache) {
        return ((InternalDistributedCache<String, String>) cache).core;
    }

    private static void settle(DistributedCache<String, String> cache) {
        await(() -> core(cache).isQuiescent());
    }

    private static long revalidated(DistributedCache<String, String> cache) {
        return cache.distributedPolicy().stats().revalidatedCount();
    }

    @ParameterizedTest
    @ValueSource(strings = {"jdbc", "postgresql", "mongodb", "redis", "redis-hostile", "postgresql-stepping"})
    void a_refill_hides_what_it_did_not_write_and_keeps_what_it_did_on_every_instance(String profile) {
        database = new InMemoryDatabase(StoreProfile.of(profile));
        DistributedCache<String, String> refiller = cache(builder -> builder.withCaffeine(CaffeineConfigurer::recordStats));
        DistributedCache<String, String> holder = cache(builder -> builder.withCaffeine(CaffeineConfigurer::recordStats)
                .withDistributionMode(DistributionMode.UPDATE));
        refiller.putAll(Map.of("kept", "old", "gone", "old"));
        assertThat(holder.getAll(List.of("kept", "gone"), keys -> Map.of())).hasSize(2);
        pause(20); // the mark later than the old writes, also on a clock counting in milliseconds or stepping

        DistributedPolicy.StoreMark mark = refiller.distributedPolicy().markStore();
        refiller.put("kept", "new");
        refiller.distributedPolicy().invalidateAllBeforeMark(mark);

        assertThat(refiller.getIfPresent("gone")).as("hidden at once on the calling instance").isNull();
        assertThat(refiller.getIfPresent("kept")).isEqualTo("new");
        await(() -> holder.policy().getIfPresentQuietly("gone") == null);
        assertThat(holder.getIfPresent("gone")).as("hidden on every instance").isNull();
        await(() -> "new".equals(holder.getIfPresent("kept")));
        assertThat(refiller.distributedPolicy().getFromStore("gone")).isNull();
    }

    @Test
    void the_calling_instance_stays_hot_its_copies_revalidated_in_the_background_not_dropped() {
        DistributedCache<String, String> refiller = cache(builder -> builder.withCaffeine(CaffeineConfigurer::recordStats));
        refiller.putAll(Map.of("a", "old", "b", "old", "gone", "old"));
        pause(20);

        DistributedPolicy.StoreMark mark = refiller.distributedPolicy().markStore();
        refiller.putAll(Map.of("a", "new", "b", "new"));
        long before = revalidated(refiller);
        refiller.distributedPolicy().invalidateAllBeforeMark(mark);
        settle(refiller);

        assertThat(refiller.policy().getIfPresentQuietly("a")).as("still held").isEqualTo("new");
        assertThat(refiller.policy().getIfPresentQuietly("b")).isEqualTo("new");
        assertThat(refiller.policy().getIfPresentQuietly("gone")).as("dropped by the pass").isNull();
        long afterPass = revalidated(refiller);
        assertThat(afterPass - before).as("each copy checked once, by the pass").isEqualTo(3);
        assertThat(refiller.getIfPresent("a")).isEqualTo("new");
        assertThat(revalidated(refiller)).as("then trusted: no check on access").isEqualTo(afterPass);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALIDATE", "UPDATE"})
    void another_instance_keeps_its_rewritten_copies_hot_and_drops_the_hidden_ones(String mode) {
        DistributedCache<String, String> refiller = cache();
        DistributedCache<String, String> holder = cache(builder -> builder
                .withDistributionMode(DistributionMode.valueOf(mode)));
        refiller.putAll(Map.of("a", "old", "gone", "old"));
        holder.getAll(List.of("a", "gone"), keys -> Map.of());
        pause(20);

        DistributedPolicy.StoreMark mark = refiller.distributedPolicy().markStore();
        refiller.put("a", "new");
        refiller.distributedPolicy().invalidateAllBeforeMark(mark);

        await(() -> holder.policy().getIfPresentQuietly("gone") == null);
        settle(holder);
        assertThat(holder.getIfPresent("a")).isEqualTo("new");
        if (mode.equals("UPDATE")) {
            assertThat(holder.policy().getIfPresentQuietly("a")).as("held throughout").isEqualTo("new");
        }
    }

    @Test
    void a_write_after_the_mark_is_kept_also_when_the_store_clock_steps_back_within_its_allowance() {
        database = new InMemoryDatabase(StoreProfile.POSTGRESQL_STEPPING);
        DistributedCache<String, String> refiller = cache();
        refiller.put("gone", "old");
        pause(20);

        DistributedPolicy.StoreMark mark = refiller.distributedPolicy().markStore();
        // NTP steps the clock back within what the profile declares (5 ms): the fill is stamped before the mark
        database.stepClockBack(Duration.ofMillis(4));
        refiller.put("fill", "new");
        refiller.distributedPolicy().invalidateAllBeforeMark(mark);

        assertThat(refiller.distributedPolicy().getFromStore("fill")).as("not hidden").isNotNull();
        assertThat(refiller.getIfPresent("fill")).isEqualTo("new");
        assertThat(refiller.getIfPresent("gone")).isNull();
    }

    @Test
    void a_value_written_after_the_mark_by_another_instance_is_kept() {
        DistributedCache<String, String> refiller = cache();
        DistributedCache<String, String> other = cache();
        refiller.put("stale", "old");
        pause(20);

        DistributedPolicy.StoreMark mark = refiller.distributedPolicy().markStore();
        other.put("meanwhile", "written elsewhere");
        refiller.distributedPolicy().invalidateAllBeforeMark(mark);

        assertThat(refiller.getIfPresent("meanwhile")).isEqualTo("written elsewhere");
        assertThat(refiller.getIfPresent("stale")).isNull();
        await(() -> other.getIfPresent("stale") == null);
        assertThat(other.getIfPresent("meanwhile")).isEqualTo("written elsewhere");
    }

    @Test
    void an_earlier_mark_never_undoes_a_later_full_invalidateAll() {
        DistributedCache<String, String> cache = cache();
        DistributedPolicy.StoreMark early = cache.distributedPolicy().markStore();
        pause(20);
        cache.put("between", "v");
        pause(20);
        cache.invalidateAll();
        cache.put("after", "v");

        cache.distributedPolicy().invalidateAllBeforeMark(early);

        assertThat(cache.distributedPolicy().getFromStore("between")).as("hidden by the full one still").isNull();
        assertThat(cache.getIfPresent("between")).isNull();
        assertThat(cache.getIfPresent("after")).isEqualTo("v");
    }

    @Test
    void a_full_invalidateAll_elsewhere_still_drops_every_copy_from_before_it() {
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = cache();
        writer.putAll(Map.of("a", "1", "b", "2"));
        holder.getAll(List.of("a", "b"), keys -> Map.of());

        writer.invalidateAll();

        await(() -> holder.policy().getIfPresentQuietly("a") == null
                && holder.policy().getIfPresentQuietly("b") == null);
        assertThat(holder.getIfPresent("a")).isNull();
    }

    @Test
    void a_mark_of_another_instance_is_refused() {
        DistributedCache<String, String> one = cache();
        DistributedCache<String, String> other = cache();
        DistributedPolicy.StoreMark mark = one.distributedPolicy().markStore();
        DistributedPolicy<String, String> policy = other.distributedPolicy();

        assertThatThrownBy(() -> policy.invalidateAllBeforeMark(mark))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidateAllOlderThan_hides_what_was_written_longer_ago_than_the_duration() {
        DistributedCache<String, String> cache = cache();
        DistributedCache<String, String> holder = cache();
        cache.put("old", "v");
        pause(300);
        cache.put("young", "v");
        holder.getAll(List.of("old", "young"), keys -> Map.of());

        cache.distributedPolicy().invalidateAllOlderThan(Duration.ofMillis(150));

        assertThat(cache.getIfPresent("old")).isNull();
        assertThat(cache.getIfPresent("young")).isEqualTo("v");
        await(() -> holder.getIfPresent("old") == null);
        assertThat(holder.getIfPresent("young")).isEqualTo("v");
        DistributedPolicy<String, String> policy = cache.distributedPolicy();
        Duration negative = Duration.ofMillis(-1);
        assertThatThrownBy(() -> policy.invalidateAllOlderThan(negative))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void none_hides_in_the_store_and_revalidates_the_calling_instance_only() {
        DistributedCache<String, String> refiller = cache(builder -> builder
                .withDistributionMode(DistributionMode.NONE));
        DistributedCache<String, String> holder = cache(builder -> builder
                .withDistributionMode(DistributionMode.NONE));
        refiller.put("gone", "old");
        assertThat(holder.getIfPresent("gone")).isEqualTo("old");
        pause(20);

        DistributedPolicy.StoreMark mark = refiller.distributedPolicy().markStore();
        refiller.distributedPolicy().invalidateAllBeforeMark(mark);

        assertThat(refiller.getIfPresent("gone")).isNull();
        assertThat(holder.distributedPolicy().getFromStore("gone")).isNull();
        assertThat(holder.getIfPresent("gone")).as("kept until it expires locally, as for any change elsewhere")
                .isEqualTo("old");
    }

    @Test
    void with_the_maximum_age_for_the_store_only_the_hidden_copies_go_too() {
        DistributedCache<String, String> refiller = cache(builder -> builder
                .withStore(StoreConfigurer::withMaximumAgeForStoreOnly));
        DistributedCache<String, String> holder = cache(builder -> builder
                .withStore(StoreConfigurer::withMaximumAgeForStoreOnly));
        refiller.put("gone", "old");
        assertThat(holder.getIfPresent("gone")).isEqualTo("old");
        pause(20);

        DistributedPolicy.StoreMark mark = refiller.distributedPolicy().markStore();
        refiller.distributedPolicy().invalidateAllBeforeMark(mark);

        await(() -> holder.getIfPresent("gone") == null);
    }
}
