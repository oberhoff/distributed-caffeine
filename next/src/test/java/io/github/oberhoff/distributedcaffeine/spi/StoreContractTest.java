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
package io.github.oberhoff.distributedcaffeine.spi;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The storage contract ({@link Store}), run against every store implementation, with or without a change feed: the
 * reference store, a store offering only storage, and each adapter.
 */
@SuppressWarnings("resource") // every store a test opens is closed after it (@AfterEach)
public abstract class StoreContractTest extends AbstractContractTest {

    // contract point 1: versions come from the store, increase strictly, and are returned to the writer

    @Test
    void contract1_versions_increase_strictly_per_hash() {
        Store writer = open();
        long previous = 0;
        for (int i = 0; i < 200; i++) {
            long version = i % 3 == 2
                    ? writer.invalidate(List.of("h1")).get("h1")
                    : writer.put(List.of(entry("h1", "v" + i))).get("h1");
            assertThat(version).isGreaterThan(previous);
            previous = version;
        }
    }

    @Test
    void contract1_versions_are_shared_by_all_instances() {
        Store first = open();
        Store second = open();
        long v1 = first.put(List.of(entry("h1", "a"))).get("h1");
        long v2 = second.put(List.of(entry("h1", "b"))).get("h1");
        long v3 = first.put(List.of(entry("h1", "c"))).get("h1");

        assertThat(v2).isGreaterThan(v1);
        assertThat(v3).isGreaterThan(v2);
        assertThat(second.read(List.of("h1"), LONG).rows().get("h1").version()).isEqualTo(v3);
    }

    @Test
    void contract1_a_key_written_again_after_its_row_was_deleted_gets_a_higher_version() {
        Store writer = open();
        writer.put(List.of(entry("h1", "v1")));
        long tombstone = writer.invalidate(List.of("h1")).get("h1");
        pause(5);
        assertThat(writer.pruneTombstones(Duration.ZERO)).isEqualTo(1);

        long recreated = writer.put(List.of(entry("h1", "v2"))).get("h1");

        assertThat(recreated).isGreaterThan(tombstone);
    }

    // contract point 2: the age of a row and the time of a read come from the store's clock

    @Test
    void contract2_the_age_grows_with_the_store_clock() {
        Store writer = open();
        writer.put(List.of(entry("h1", "v1")));

        Duration first = writer.read(List.of("h1"), LONG).rows().get("h1").age();
        pause(250);
        Duration second = writer.read(List.of("h1"), LONG).rows().get("h1").age();

        assertThat(first).isBetween(Duration.ZERO, Duration.ofSeconds(5));
        assertThat(second.minus(first)).isBetween(Duration.ofMillis(200), Duration.ofSeconds(5));
    }

    @Test
    void contract2_a_read_returns_the_stores_time_on_the_clock_of_the_versions() {
        Store writer = open();
        long before = writer.read(List.of("none"), LONG).time();
        long version = writer.put(List.of(entry("h1", "v"))).get("h1");
        long after = writer.read(List.of("h1"), LONG).time();

        assertThat(before).as("also without any row").isPositive();
        assertThat(version).isGreaterThanOrEqualTo(before);
        assertThat(after).isGreaterThanOrEqualTo(before);
    }

    // contract point 5: maintenance is idempotent when run on several instances concurrently

    @Test
    void contract5_concurrent_expiry_expires_every_row_once() throws Exception {
        Store writer = open();
        Set<String> all = IntStream.range(0, 50).mapToObj(i -> "h" + i).collect(Collectors.toSet());
        for (String hash : all) {
            writer.put(List.of(entry(hash, "v")));
        }
        pause(100);

        int expired = race(4, store -> store.expire(Duration.ofMillis(50)));

        assertThat(expired).isEqualTo(50);
        assertThat(writer.read(all, LONG).rows().values()).hasSize(50).allMatch(row -> row.state() == State.EXPIRED);
    }

    @Test
    void contract5_concurrent_size_pruning_keeps_exactly_the_maximum() throws Exception {
        Store writer = open();
        for (int i = 0; i < 40; i++) {
            writer.put(List.of(entry("h" + i, "v")));
        }

        int pruned = race(4, store -> store.pruneToSize(10));

        assertThat(pruned).isEqualTo(30);
        Set<String> all = IntStream.range(0, 40).mapToObj(i -> "h" + i).collect(Collectors.toSet());
        assertThat(writer.read(all, LONG).rows()).hasSize(10);
    }

    // contract point 3: a read reflects every write confirmed before it began, on any instance

    @Test
    void contract3_a_read_on_another_instance_reflects_every_confirmed_write() {
        Store writer = open();
        Store reader = open();
        for (int round = 0; round < 20; round++) {
            long put = writer.put(List.of(entry("p", "v" + round))).get("p");
            assertThat(reader.read(List.of("p"), LONG).rows().get("p").version()).as("put").isGreaterThanOrEqualTo(put);
            assertThat(reader.readVersions(List.of("p"), LONG).get("p")).as("versions").isGreaterThanOrEqualTo(put);

            long conditional = writer.putIfUnchanged(List.of(ExpectedEntry.ifVersion(entry("p", "c" + round), put)),
                    LONG).get("p").version();
            assertThat(reader.read(List.of("p"), LONG).rows().get("p").version()).as("conditional")
                    .isGreaterThanOrEqualTo(conditional);

            long tombstone = writer.invalidate(List.of("p")).get("p");
            Row row = reader.read(List.of("p"), LONG).rows().get("p");
            assertThat(row.version()).as("invalidate").isGreaterThanOrEqualTo(tombstone);
            assertThat(row.state()).isEqualTo(State.TOMBSTONE);
        }
    }

    @Test
    void contract3_a_read_on_another_instance_reflects_a_confirmed_invalidate_all() {
        Store writer = open();
        Store reader = open();
        writer.put(List.of(entry("a", "1"), entry("b", "2")));

        long time = writer.invalidateAll();

        assertThat(reader.read(List.of("a", "b"), LONG).rows().values())
                .hasSize(2).allMatch(row -> row.state() == State.TOMBSTONE && row.version() >= time);
    }

    // contract point 4: conditional writes are atomic per key

    @Test
    void contract4_concurrent_conditional_writes_on_one_version_have_one_winner() throws Exception {
        Store writer = open();
        long version = writer.put(List.of(entry("h1", "base"))).get("h1");

        int winners = race(8, store -> store.putIfUnchanged(
                List.of(ExpectedEntry.ifVersion(entry("h1", "racer"), version)), LONG).get("h1").written() ? 1 : 0);

        assertThat(winners).isEqualTo(1);
    }

    @Test
    void contract4_concurrent_creations_of_a_missing_row_have_one_winner() throws Exception {
        int winners = race(8, store -> store.putIfUnchanged(
                List.of(ExpectedEntry.ifMissing(entry("h1", "creator"), readTime())), LONG).get("h1").written() ? 1 : 0);

        assertThat(winners).isEqualTo(1);
    }

    @Test
    void contract4_concurrent_conditional_invalidations_of_one_version_have_one_winner() throws Exception {
        Store writer = open();
        long version = writer.put(List.of(entry("h1", "base"))).get("h1");

        int winners = race(8, store -> store.invalidateIfUnchanged(Map.of("h1", version)).size());

        assertThat(winners).isEqualTo(1);
    }

    // contract point 6: keys and values byte for byte; only the store's own cache

    private static byte[] allBytes(int offset) {
        byte[] bytes = new byte[256];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) (index + offset);
        }
        return bytes;
    }

    @Test
    void contract6_keys_and_values_come_back_byte_for_byte() {
        Store writer = open();
        byte[] key = allBytes(0);
        byte[] value = allBytes(7);

        writer.put(List.of(new Entry("h1", key, value)));

        Row row = open().read(List.of("h1"), LONG).rows().get("h1");
        assertThat(row.key()).isEqualTo(key);
        assertThat(row.value()).isEqualTo(value);
        WriteResult conflict = writer.putIfUnchanged(List.of(ExpectedEntry.ifMissing(entry("h1", "other"), readTime())),
                LONG).get("h1");
        assertThat(requireNonNull(conflict.current()).value()).as("the row of a conflict too").isEqualTo(value);
    }

    @Test
    void contract6_a_store_touches_only_its_own_caches_rows() {
        Store ours = open();
        Store theirs = openOther();
        long version = ours.put(List.of(entry("h1", "ours"))).get("h1");
        theirs.put(List.of(entry("h1", "theirs"), entry("h2", "theirs")));
        pause(60);

        theirs.invalidateAll();
        theirs.expire(Duration.ofMillis(50));
        theirs.pruneToSize(0);
        theirs.pruneTombstones(Duration.ZERO);
        assertThat(theirs.invalidateIfUnchanged(Map.of("h1", version))).as("not our row").isEmpty();

        Map<String, Row> rows = ours.read(List.of("h1", "h2"), LONG).rows();
        assertThat(rows).containsOnlyKeys("h1");
        assertThat(value(rows.get("h1"))).isEqualTo("ours");
        assertThat(rows.get("h1").state()).as("not expired, not invalidated").isEqualTo(State.VALUE);
        assertThat(rows.get("h1").version()).isEqualTo(version);
    }

    // contract point 7: the semantics of every method

    @Test
    void contract7_read_returns_values_tombstones_and_expired_rows_and_omits_the_rest() {
        Store writer = open();
        writer.put(List.of(entry("value", "v"), entry("expired", "v")));
        writer.invalidate(List.of("tombstone"));
        pause(60);
        writer.expire(Duration.ofMillis(50));
        writer.put(List.of(entry("fresh", "f")));

        Map<String, Row> rows = writer.read(List.of("value", "expired", "tombstone", "fresh", "missing"), LONG).rows();

        // "value" was expired together with "expired"; "fresh" was written after
        assertThat(rows.keySet()).containsExactlyInAnyOrder("value", "expired", "tombstone", "fresh");
        assertThat(rows.get("fresh").state()).isEqualTo(State.VALUE);
        assertThat(value(rows.get("fresh"))).isEqualTo("f");
        assertThat(rows.get("expired").state()).isEqualTo(State.EXPIRED);
        assertThat(rows.get("expired").value()).isNull();
        assertThat(rows.get("tombstone").state()).isEqualTo(State.TOMBSTONE);
        assertThat(rows.get("tombstone").value()).isNull();
    }

    @Test
    void contract7_read_omits_value_rows_older_than_the_maximum_age() {
        Store writer = open();
        writer.put(List.of(entry("old", "o")));
        pause(100);
        writer.put(List.of(entry("young", "y")));

        Duration maxAge = Duration.ofMillis(60);
        assertThat(writer.read(List.of("old", "young"), maxAge).rows().keySet()).containsExactly("young");
        assertThat(writer.readVersions(List.of("old", "young"), maxAge).keySet()).containsExactly("young");
        assertThat(writer.read(List.of("old", "young"), LONG).rows()).hasSize(2);
    }

    @Test
    void contract7_readVersions_returns_value_rows_only() {
        Store writer = open();
        long version = writer.put(List.of(entry("value", "v"))).get("value");
        writer.invalidate(List.of("tombstone"));

        assertThat(writer.readVersions(List.of("value", "tombstone", "missing"), LONG))
                .containsExactly(Map.entry("value", version));
    }

    @Test
    void contract7_putIfUnchanged_if_missing_writes_only_where_no_row_exists() {
        Store writer = open();
        long tombstone = writer.invalidate(List.of("tombstone")).get("tombstone");
        long value = writer.put(List.of(entry("value", "v"))).get("value");

        Map<String, WriteResult> results = writer.putIfUnchanged(List.of(
                ExpectedEntry.ifMissing(entry("missing", "1"), readTime()),
                ExpectedEntry.ifMissing(entry("tombstone", "2"), readTime()),
                ExpectedEntry.ifMissing(entry("value", "3"), readTime())), LONG);

        assertThat(results.get("missing").written()).isTrue();
        assertThat(results.get("tombstone").written()).isFalse();
        assertThat(results.get("tombstone").currentVersion()).isEqualTo(tombstone);
        assertThat(requireNonNull(results.get("tombstone").current()).state()).isEqualTo(State.TOMBSTONE);
        assertThat(results.get("value").written()).isFalse();
        assertThat(results.get("value").currentVersion()).isEqualTo(value);
    }

    @Test
    void contract7_a_conflict_reports_the_version_of_a_row_too_old_to_count_as_present() {
        Store writer = open();
        long version = writer.put(List.of(entry("old", "o"))).get("old");
        pause(100);

        WriteResult result = writer.putIfUnchanged(List.of(ExpectedEntry.ifMissing(entry("old", "n"), readTime())),
                Duration.ofMillis(50)).get("old");

        assertThat(result.written()).isFalse();
        assertThat(result.current()).isNull();
        assertThat(result.currentVersion()).isEqualTo(version);
        assertThat(writer.putIfUnchanged(List.of(ExpectedEntry.ifVersion(entry("old", "n"), version)),
                Duration.ofMillis(50)).get("old").written()).isTrue();
    }

    @Test
    void contract7_putIfUnchanged_if_missing_conflicts_with_a_young_value_and_returns_it() {
        Store writer = open();
        long version = writer.put(List.of(entry("h1", "existing"))).get("h1");

        WriteResult result = writer.putIfUnchanged(List.of(ExpectedEntry.ifMissing(entry("h1", "new"), readTime())), LONG).get("h1");

        assertThat(result.written()).isFalse();
        assertThat(requireNonNull(result.current()).version()).isEqualTo(version);
        assertThat(result.currentVersion()).isEqualTo(version);
        assertThat(value(result.current())).isEqualTo("existing");
    }

    @Test
    void contract7_putIfUnchanged_with_a_version_writes_only_over_that_version() {
        Store writer = open();
        long first = writer.put(List.of(entry("h1", "a"))).get("h1");
        long second = writer.put(List.of(entry("h1", "b"))).get("h1");

        WriteResult stale = writer.putIfUnchanged(List.of(ExpectedEntry.ifVersion(entry("h1", "x"), first)), LONG).get("h1");
        WriteResult current = writer.putIfUnchanged(List.of(ExpectedEntry.ifVersion(entry("h1", "y"), second)), LONG).get("h1");

        assertThat(stale.written()).isFalse();
        assertThat(requireNonNull(stale.current()).version()).isEqualTo(second);
        assertThat(current.written()).isTrue();
        assertThat(current.version()).isGreaterThan(second);
        assertThat(value(writer.read(List.of("h1"), LONG).rows().get("h1"))).isEqualTo("y");
    }

    @Test
    void contract7_putIfUnchanged_with_a_version_of_a_tombstone_writes_over_it() {
        Store writer = open();
        writer.put(List.of(entry("h1", "a")));
        long tombstone = writer.invalidate(List.of("h1")).get("h1");

        WriteResult result = writer.putIfUnchanged(List.of(ExpectedEntry.ifVersion(entry("h1", "b"), tombstone)), LONG).get("h1");

        assertThat(result.written()).isTrue();
    }

    @Test
    void contract7_invalidate_creates_a_tombstone_for_a_missing_row() {
        Store writer = open();

        long version = writer.invalidate(List.of("missing")).get("missing");

        Row row = writer.read(List.of("missing"), LONG).rows().get("missing");
        assertThat(row.state()).isEqualTo(State.TOMBSTONE);
        assertThat(row.version()).isEqualTo(version);
    }

    @Test
    void contract7_invalidateIfUnchanged_invalidates_only_matching_value_rows() {
        Store writer = open();
        long a = writer.put(List.of(entry("a", "1"))).get("a");
        long b = writer.put(List.of(entry("b", "1"))).get("b");
        writer.put(List.of(entry("b", "2")));

        Map<String, Long> invalidated = writer.invalidateIfUnchanged(Map.of("a", a, "b", b, "missing", 1L));

        assertThat(invalidated.keySet()).containsExactly("a");
        Map<String, Row> rows = writer.read(List.of("a", "b", "missing"), LONG).rows();
        assertThat(rows.get("a").state()).isEqualTo(State.TOMBSTONE);
        assertThat(rows.get("a").version()).isEqualTo(invalidated.get("a"));
        assertThat(rows.get("b").state()).isEqualTo(State.VALUE);
        assertThat(rows).doesNotContainKey("missing");
    }

    @Test
    void contract7_invalidateIfUnchanged_also_matches_a_tombstone_by_its_version_and_a_missing_row() {
        Store writer = open();
        long tombstone = writer.invalidate(List.of("t")).get("t");
        writer.put(List.of(entry("present", "1")));

        Map<String, Long> invalidated = writer.invalidateIfUnchanged(Map.of("t", tombstone,
                "missing", ExpectedEntry.MISSING, "present", ExpectedEntry.MISSING));

        assertThat(invalidated.keySet()).containsExactlyInAnyOrder("t", "missing");
        assertThat(invalidated.get("t")).isGreaterThan(tombstone);
        assertThat(writer.read(List.of("t", "missing", "present"), LONG).rows())
                .hasEntrySatisfying("missing", row -> assertThat(row.state()).isEqualTo(State.TOMBSTONE))
                .hasEntrySatisfying("present", row -> assertThat(row.state()).isEqualTo(State.VALUE));
        assertThat(writer.invalidateIfUnchanged(Map.of("t", tombstone))).as("the version moved on").isEmpty();
    }

    @Test
    void contract7_invalidateAll_turns_every_value_into_a_tombstone() {
        Store writer = open();
        writer.put(List.of(entry("a", "1"), entry("b", "2")));

        writer.invalidateAll();

        assertThat(writer.readVersions(List.of("a", "b"), LONG)).isEmpty();
        assertThat(writer.read(List.of("a", "b"), LONG).rows().values()).allMatch(row -> row.state() == State.TOMBSTONE);
    }

    @Test
    void contract7_invalidateAll_gives_every_row_a_version_of_at_least_its_time() {
        Store writer = open();
        long tombstone = writer.invalidate(List.of("tombstone")).get("tombstone");
        writer.put(List.of(entry("expired", "e")));
        pause(60);
        writer.expire(Duration.ofMillis(50));
        long expired = writer.read(List.of("expired"), LONG).rows().get("expired").version();
        long value = writer.put(List.of(entry("value", "v"))).get("value");

        long time = writer.invalidateAll();

        Map<String, Row> rows = writer.read(List.of("tombstone", "expired", "value"), LONG).rows();
        assertThat(rows.get("tombstone").state()).isEqualTo(State.TOMBSTONE);
        assertThat(rows.get("expired").state()).isEqualTo(State.EXPIRED);
        assertThat(rows.get("value").state()).isEqualTo(State.TOMBSTONE);
        assertThat(rows.get("tombstone").version()).isGreaterThan(tombstone).isGreaterThanOrEqualTo(time);
        assertThat(rows.get("expired").version()).isGreaterThan(expired).isGreaterThanOrEqualTo(time);
        assertThat(rows.get("value").version()).isGreaterThan(value).isGreaterThanOrEqualTo(time);
    }

    @Test
    void contract7_a_row_created_on_missing_takes_the_given_version() {
        Store writer = open();
        long readTime = writer.read(List.of("h1"), LONG).time();
        pause(5);

        WriteResult result = writer.putIfUnchanged(List.of(ExpectedEntry.ifMissing(entry("h1", "v"), readTime)), LONG)
                .get("h1");

        assertThat(result.written()).isTrue();
        assertThat(result.version()).isEqualTo(readTime);
        assertThat(writer.read(List.of("h1"), LONG).rows().get("h1").version()).isEqualTo(readTime);
        assertThat(writer.put(List.of(entry("h1", "w"))).get("h1")).as("and increases from there").isGreaterThan(readTime);
    }

    @Test
    void contract7_expire_marks_rows_at_least_the_maximum_age_old_and_keeps_younger_ones() {
        Store writer = open();
        writer.put(List.of(entry("old", "o")));
        pause(400);
        writer.put(List.of(entry("young", "y")));

        assertThat(writer.expire(Duration.ofMillis(300))).isEqualTo(1);

        assertThat(writer.read(List.of("old"), LONG).rows().get("old").state()).isEqualTo(State.EXPIRED);
        assertThat(writer.read(List.of("young"), LONG).rows().get("young").state()).isEqualTo(State.VALUE);
    }

    @Test
    void contract7_pruneToSize_deletes_the_oldest_values() {
        Store writer = open();
        for (int i = 0; i < 5; i++) {
            writer.put(List.of(entry("h" + i, "v")));
            pause(2);
        }

        assertThat(writer.pruneToSize(2)).isEqualTo(3);

        assertThat(writer.read(List.of("h0", "h1", "h2", "h3", "h4"), LONG).rows().keySet()).containsExactlyInAnyOrder("h3", "h4");
    }

    @Test
    void contract7_pruneTombstones_keeps_values_and_young_tombstones() {
        Store writer = open();
        writer.put(List.of(entry("value", "v")));
        writer.invalidate(List.of("old"));
        pause(100);
        writer.invalidate(List.of("young"));

        assertThat(writer.pruneTombstones(Duration.ofMillis(60))).isEqualTo(1);

        assertThat(writer.read(List.of("value", "old", "young"), LONG).rows().keySet()).containsExactlyInAnyOrder("value", "young");
    }
}
