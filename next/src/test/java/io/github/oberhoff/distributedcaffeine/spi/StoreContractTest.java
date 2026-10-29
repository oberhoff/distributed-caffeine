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

import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The storage contract ({@link Store}), run against every store implementation, with or without a change feed: the
 * reference store, a store offering only storage, and each adapter.
 */
@NullMarked
@SuppressWarnings("resource") // every store a test opens is closed after it (@AfterEach)
public abstract class StoreContractTest extends AbstractContractTest {

    // contract point 1: the version is the one the write carried, stored as given; the store only compares it

    @Test
    void contract1_an_advancing_write_applies_only_above_the_stored_version() {
        assumeFalse(fixture.assigns(1), "an assigning store refuses no advance: see the test below");
        Store writer = open();
        Store reader = open();
        long[][] steps = {{900, 900}, {5, 900}, {1L << 50, 1L << 50}, {6, 1L << 50}, {(1L << 50) + 1, (1L << 50) + 1}};
        for (long[] step : steps) {
            Set<String> refused = writer.write(List.of(Write.value("h1", new byte[]{1}, new byte[]{1}, step[0]))).refused();
            assertThat(refused).as("version " + step[0]).isEqualTo(step[0] == step[1] ? Set.of() : Set.of("h1"));
            assertThat(reader.read(List.of("h1"), LONG).get("h1").version()).as("only grows").isEqualTo(step[1]);
            assertThat(reader.readVersions(List.of("h1"), LONG).versions().get("h1")).isEqualTo(step[1]);
        }
        assertThat(writer.write(List.of(Write.tombstone("h1", 3))).refused()).as("a tombstone below").containsExactly("h1");
        writer.write(List.of(Write.tombstone("h1", 1L << 51)));
        assertThat(reader.read(List.of("h1"), LONG).get("h1").version()).as("a tombstone above").isEqualTo(1L << 51);
        assertThat(writer.write(List.of(Write.tombstone("h1", 1L << 51))).refused()).as("an equal one").containsExactly("h1");
    }

    @Test
    void contract1_an_assigning_store_applies_every_advance_above_the_stored_version() {
        assumeTrue(fixture.assigns(1), "a store applying advances conditionally: see the test above");
        Store writer = open();
        Store reader = open();
        long stored = 0;
        for (long proposed : new long[]{nextVersion(), 5, nextVersion() + (1L << 40), 6}) {
            Write write = Write.value("h1", new byte[]{1}, new byte[]{1}, proposed);
            WriteResult result = writer.write(List.of(write));
            assertThat(result.refused()).as("proposed " + proposed).isEmpty();
            long version = result.version(write);
            assertThat(version).as("above the stored version").isGreaterThan(stored);
            assertThat(reader.read(List.of("h1"), LONG).get("h1").version()).as("as reported").isEqualTo(version);
            stored = version;
        }
        Write tombstone = Write.tombstone("h1", 3);
        WriteResult result = writer.write(List.of(tombstone));
        assertThat(result.refused()).as("a tombstone below too").isEmpty();
        assertThat(result.version(tombstone)).isGreaterThan(stored);
    }

    @Test
    void contract1_a_conditional_write_compares_versions_for_equality_only() {
        Store writer = open();
        long stored = written(writer, Write.value("h1", new byte[]{1}, new byte[]{1}, nextVersion()));

        if (stored > 1) {
            assertThat(applied(writer, ifVersion(Write.value("h1", new byte[]{1}, new byte[]{2}, nextVersion()),
                    stored - 1))).as("a lower version").isFalse();
        }
        assertThat(applied(writer, ifVersion(Write.value("h1", new byte[]{1}, new byte[]{2}, nextVersion()),
                stored + 1))).as("a higher version").isFalse();
        long applied = written(writer, ifVersion(Write.value("h1", new byte[]{1}, new byte[]{2}, nextVersion()),
                stored));
        assertThat(writer.read(List.of("h1"), LONG).get("h1").version()).as("the same version").isEqualTo(applied);
    }

    // contract point 2: the age of a row comes from the store's clock; the ages of one read compare like the writes

    @Test
    void contract2_the_age_grows_with_the_store_clock() {
        Store writer = open();
        writer.write(List.of(entry("h1", "v1")));

        Duration first = writer.read(List.of("h1"), LONG).get("h1").age();
        pause(250);
        Duration second = writer.read(List.of("h1"), LONG).get("h1").age();

        assertThat(first).isBetween(Duration.ZERO, Duration.ofSeconds(5));
        assertThat(second.minus(first)).isBetween(Duration.ofMillis(200), Duration.ofSeconds(5));
    }

    @Test
    void contract2_the_ages_of_one_read_order_the_writes_as_they_happened() {
        Store writer = open();
        Store reader = open();
        writer.write(List.of(entry("a", "1")));
        pause(5);
        writer.write(List.of(control()));
        pause(5);
        writer.write(List.of(entry("c", "3")));

        Map<String, Row> rows = reader.read(List.of("a", Store.CONTROL_HASH, "c"), LONG);
        assertThat(rows.get("a").age()).as("written before the control row").isGreaterThan(
                rows.get(Store.CONTROL_HASH).age());
        assertThat(rows.get("c").age()).as("written after it").isLessThan(rows.get(Store.CONTROL_HASH).age());
        Map<String, Duration> ages = reader.readVersions(List.of("a", Store.CONTROL_HASH, "c"), LONG).ages();
        assertThat(ages.get("a")).as("and from a read of versions").isGreaterThan(ages.get(Store.CONTROL_HASH));
        assertThat(ages.get("c")).isLessThan(ages.get(Store.CONTROL_HASH));
    }

    // contract point 3: a read reflects every write confirmed before it began, on any instance

    @Test
    void contract3_a_read_on_another_instance_reflects_every_confirmed_write() {
        Store writer = open();
        Store reader = open();
        for (int round = 0; round < 20; round++) {
            long put = written(writer, entry("p", "v" + round));
            assertThat(reader.read(List.of("p"), LONG).get("p").version()).as("unconditional").isEqualTo(put);
            assertThat(reader.readVersions(List.of("p"), LONG).versions().get("p")).as("versions").isEqualTo(put);

            long conditional = written(writer, ifVersion(entry("p", "c" + round), put));
            assertThat(reader.read(List.of("p"), LONG).get("p").version()).as("conditional")
                    .isEqualTo(conditional);

            long removed = written(writer, ifVersion(tombstone("p"), conditional));
            Row row = reader.read(List.of("p"), LONG).get("p");
            assertThat(row.version()).as("a tombstone").isEqualTo(removed);
            assertThat(row.state()).isEqualTo(State.TOMBSTONE);
        }
    }

    @Test
    void contract3_a_read_on_another_instance_reflects_a_confirmed_write_of_the_control_row() {
        Store writer = open();
        Store reader = open();
        writer.write(List.of(entry("a", "1"), entry("b", "2")));
        pause(5);

        long control = written(writer, control());

        Map<String, Row> rows = reader.read(List.of("a", "b", Store.CONTROL_HASH), LONG);
        assertThat(rows.get(Store.CONTROL_HASH).version()).isEqualTo(control);
        assertThat(rows.get(Store.CONTROL_HASH).state()).isEqualTo(State.VALUE);
        assertThat(rows.values()).as("rows before it are returned as they are, the core decides").hasSize(3)
                .allMatch(row -> row.state() == State.VALUE);
        assertThat(rows.get("a").age()).isGreaterThan(rows.get(Store.CONTROL_HASH).age());
    }

    // contract point 4: conditional writes are atomic per key

    @Test
    void contract4_concurrent_conditional_writes_on_one_version_have_one_winner() throws Exception {
        Store writer = open();
        long version = written(writer, entry("h1", "base"));

        int winners = race(8, store -> applied(store, ifVersion(entry("h1", "racer"), version)) ? 1 : 0);

        assertThat(winners).isEqualTo(1);
    }

    @Test
    void contract4_concurrent_creations_of_a_missing_row_have_one_winner() throws Exception {
        int winners = race(8, store -> applied(store, ifMissing(entry("h1", "creator"))) ? 1 : 0);

        assertThat(winners).isEqualTo(1);
    }

    @Test
    void contract4_concurrent_conditional_tombstones_on_one_version_have_one_winner() throws Exception {
        Store writer = open();
        long version = written(writer, entry("h1", "base"));

        int winners = race(8, store -> applied(store, ifVersion(tombstone("h1"), version)) ? 1 : 0);

        assertThat(winners).isEqualTo(1);
    }

    // contract point 5: maintenance is idempotent when run on several instances concurrently

    @Test
    void contract5_concurrent_expiry_expires_every_row_once() throws Exception {
        Store writer = open();
        Set<String> all = IntStream.range(0, 50).mapToObj(i -> "h" + i).collect(Collectors.toSet());
        for (String hash : all) {
            writer.write(List.of(entry(hash, "v")));
        }
        pause(100);

        int expired = race(4, store -> store.expire(Duration.ofMillis(50), nextVersion()));

        assertThat(expired).isEqualTo(50);
        assertThat(writer.read(all, LONG).values()).hasSize(50).allMatch(row -> row.state() == State.EXPIRED);
    }

    @Test
    void contract5_concurrent_size_pruning_keeps_exactly_the_maximum() throws Exception {
        Store writer = open();
        for (int i = 0; i < 40; i++) {
            writer.write(List.of(entry("h" + i, "v")));
        }

        int pruned = race(4, store -> store.pruneToSize(10, Duration.ZERO));

        assertThat(pruned).isEqualTo(30);
        Set<String> all = IntStream.range(0, 40).mapToObj(i -> "h" + i).collect(Collectors.toSet());
        assertThat(writer.read(all, LONG)).hasSize(10);
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

        long version = written(writer, Write.value("h1", key, value, nextVersion()));

        Row row = open().read(List.of("h1"), LONG).get("h1");
        assertThat(row.key()).isEqualTo(key);
        assertThat(row.value()).isEqualTo(value);
        writer.write(List.of(ifVersion(tombstone("h1"), version)));
        assertThat(open().read(List.of("h1"), LONG).get("h1").key()).as("a tombstone keeps the key")
                .isEqualTo(key);
    }

    @Test
    void contract6_a_store_touches_only_its_own_caches_rows() {
        Store ours = open();
        Store theirs = openOther();
        long version = written(ours, entry("h1", "ours"));
        theirs.write(List.of(entry("h1", "theirs"), entry("h2", "theirs")));
        pause(60);

        theirs.write(List.of(control()));
        theirs.expire(Duration.ofMillis(50), nextVersion());
        theirs.pruneToSize(0, Duration.ZERO);
        theirs.pruneTombstones(Duration.ZERO);
        assertThat(theirs.write(List.of(ifVersion(tombstone("h1"), version))).refused()).as("not our row")
                .containsExactly("h1");

        Map<String, Row> rows = ours.read(List.of("h1", "h2"), LONG);
        assertThat(rows).containsOnlyKeys("h1");
        assertThat(value(rows.get("h1"))).isEqualTo("ours");
        assertThat(rows.get("h1").state()).as("not expired, not invalidated").isEqualTo(State.VALUE);
        assertThat(rows.get("h1").version()).isEqualTo(version);
    }

    // contract point 7: the semantics of every method

    @Test
    void contract7_read_returns_values_tombstones_and_expired_rows_and_omits_missing_ones() {
        Store writer = open();
        writer.write(List.of(entry("value", "v"), entry("expired", "v")));
        writer.write(List.of(tombstone("tombstone")));
        pause(60);
        writer.expire(Duration.ofMillis(50), nextVersion());
        writer.write(List.of(entry("fresh", "f")));

        Map<String, Row> rows = writer.read(List.of("value", "expired", "tombstone", "fresh", "missing"), LONG);

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
    void contract7_read_returns_value_rows_older_than_the_maximum_age_as_expired_with_their_version() {
        Store writer = open();
        long old = written(writer, entry("old", "o"));
        pause(100);
        long young = written(writer, entry("young", "y"));

        Duration maxAge = Duration.ofMillis(60);
        Map<String, Row> rows = writer.read(List.of("old", "young"), maxAge);
        assertThat(rows.get("old").state()).isEqualTo(State.EXPIRED);
        assertThat(rows.get("old").value()).isNull();
        assertThat(rows.get("old").version()).as("so that a write can be conditioned on it").isEqualTo(old);
        assertThat(rows.get("young").state()).isEqualTo(State.VALUE);
        assertThat(rows.get("young").version()).isEqualTo(young);
        assertThat(writer.readVersions(List.of("old", "young"), maxAge).versions().keySet()).containsExactly("young");
        assertThat(writer.read(List.of("old", "young"), LONG).values()).hasSize(2)
                .allMatch(row -> row.state() == State.VALUE);
        assertThat(applied(writer, ifVersion(entry("old", "n"), old))).as("replaced at its version").isTrue();
    }

    @Test
    void contract7_readVersions_returns_value_rows_only() {
        Store writer = open();
        long version = written(writer, entry("value", "v"));
        writer.write(List.of(tombstone("tombstone")));

        assertThat(writer.readVersions(List.of("value", "tombstone", "missing"), LONG).versions())
                .containsExactly(Map.entry("value", version));
    }

    @Test
    void contract7_write_unconditionally_never_conflicts() {
        Store writer = open();
        writer.write(List.of(entry("value", "v"), tombstone("tombstone")));

        Write value = entry("value", "w");
        Write overTombstone = entry("tombstone", "w");
        Write tombstone = tombstone("missing");
        WriteResult result = writer.write(List.of(value, overTombstone, tombstone));
        assertThat(result.refused()).isEmpty();

        Map<String, Row> rows = writer.read(List.of("value", "tombstone", "missing"), LONG);
        assertThat(rows.get("value").version()).isEqualTo(result.version(value));
        assertThat(value(rows.get("value"))).isEqualTo("w");
        assertThat(rows.get("tombstone").version()).isEqualTo(result.version(overTombstone));
        assertThat(rows.get("tombstone").state()).isEqualTo(State.VALUE);
        assertThat(rows.get("missing").version()).isEqualTo(result.version(tombstone));
        assertThat(rows.get("missing").state()).as("created where no row existed").isEqualTo(State.TOMBSTONE);
    }

    @Test
    void contract7_write_if_missing_writes_only_where_no_row_exists() {
        Store writer = open();
        writer.write(List.of(entry("expired", "v")));
        pause(60);
        writer.expire(Duration.ofMillis(50), nextVersion());
        long tombstone = written(writer, tombstone("tombstone"));
        long value = written(writer, entry("value", "v"));

        Set<String> conflicts = writer.write(List.of(
                ifMissing(entry("missing", "1")),
                ifMissing(entry("tombstone", "2")),
                ifMissing(entry("value", "3")),
                ifMissing(entry("expired", "4")))).refused();

        assertThat(conflicts).containsExactlyInAnyOrder("tombstone", "value", "expired");
        Map<String, Row> rows = writer.read(List.of("missing", "tombstone", "value"), LONG);
        assertThat(value(rows.get("missing"))).isEqualTo("1");
        assertThat(rows.get("tombstone").version()).as("unchanged").isEqualTo(tombstone);
        assertThat(rows.get("value").version()).as("unchanged").isEqualTo(value);
    }

    @Test
    void contract7_write_with_a_version_writes_only_over_that_version_whatever_the_state() {
        Store writer = open();
        long first = written(writer, entry("h1", "a"));
        long second = written(writer, entry("h1", "b"));
        long tombstone = written(writer, tombstone("t"));

        assertThat(applied(writer, ifVersion(entry("h1", "x"), first))).as("an earlier version").isFalse();
        long current = written(writer, ifVersion(entry("h1", "y"), second));
        assertThat(applied(writer, ifVersion(entry("t", "z"), tombstone))).as("over a tombstone").isTrue();
        Write onMissing = ifVersion(entry("missing", "m"), first);
        assertThat(applied(writer, onMissing)).as("no row").isFalse();

        Row row = writer.read(List.of("h1"), LONG).get("h1");
        assertThat(value(row)).isEqualTo("y");
        assertThat(row.version()).isEqualTo(current);
        assertNothingOrItsTombstone(writer.read(List.of("missing"), LONG), onMissing);
    }

    @Test
    void contract7_a_conditional_tombstone_matches_values_tombstones_and_missing_rows() {
        Store writer = open();
        long a = written(writer, entry("a", "1"));
        long b = written(writer, entry("b", "1"));
        writer.write(List.of(entry("b", "2")));
        long t = written(writer, tombstone("t"));
        writer.write(List.of(entry("present", "1")));

        Write onA = ifVersion(tombstone("a"), a);
        Write onT = ifVersion(tombstone("t"), t);
        Write onGone = ifVersion(tombstone("gone"), a);
        WriteResult result = writer.write(List.of(onA, ifVersion(tombstone("b"), b), onT, onGone,
                ifMissing(tombstone("missing")), ifMissing(tombstone("present"))));

        assertThat(result.refused()).containsExactlyInAnyOrder("b", "gone", "present");
        Map<String, Row> rows = writer.read(List.of("a", "b", "t", "gone", "missing", "present"), LONG);
        assertThat(rows.get("a").state()).isEqualTo(State.TOMBSTONE);
        assertThat(rows.get("a").version()).isEqualTo(result.version(onA));
        assertThat(rows.get("t").version()).isEqualTo(result.version(onT));
        assertThat(rows.get("b").state()).isEqualTo(State.VALUE);
        assertThat(rows.get("missing").state()).isEqualTo(State.TOMBSTONE);
        assertThat(rows.get("present").state()).isEqualTo(State.VALUE);
        assertNothingOrItsTombstone(rows, onGone);
    }

    @Test
    void contract7_a_batch_mixes_conditions_and_kinds_with_a_result_per_key() {
        Store writer = open();
        long existing = written(writer, entry("existing", "e"));
        Write stale = ifVersion(entry("stale", "s"), existing);

        Set<String> conflicts = writer.write(List.of(
                entry("put", "p"),
                ifMissing(entry("created", "c")),
                ifMissing(entry("existing", "x")),
                stale,
                tombstone("removed"))).refused();

        assertThat(conflicts).containsExactlyInAnyOrder("existing", "stale");
        Map<String, Row> rows = writer.read(List.of("put", "created", "existing", "stale", "removed"), LONG);
        assertThat(rows).containsKeys("put", "created", "existing", "removed");
        assertThat(value(rows.get("existing"))).as("untouched").isEqualTo("e");
        assertNothingOrItsTombstone(rows, stale);
    }

    @Test
    void contract7_expire_and_pruneToSize_ignore_the_rows_the_control_row_hides() {
        Store writer = open();
        long a = written(writer, entry("a", "1"));
        long b = written(writer, entry("b", "2"));
        pause(60);
        long control = written(writer, control());
        pause(5);
        writer.write(List.of(entry("c", "3")));
        pause(60);

        assertThat(writer.expire(Duration.ofMillis(50), nextVersion())).as("only the row after the control row")
                .isEqualTo(1);
        assertThat(writer.pruneToSize(0, Duration.ZERO)).as("none left that counts").isZero();

        Map<String, Row> rows = writer.read(List.of("a", "b", "c", Store.CONTROL_HASH), LONG);
        assertThat(rows.get("a").state()).as("left as it was").isEqualTo(State.VALUE);
        assertThat(rows.get("a").version()).isEqualTo(a);
        assertThat(rows.get("b").version()).isEqualTo(b);
        assertThat(rows.get("c").state()).isEqualTo(State.EXPIRED);
        assertThat(rows.get(Store.CONTROL_HASH).version()).isEqualTo(control);
        assertThat(rows.get(Store.CONTROL_HASH).state()).isEqualTo(State.VALUE);
    }

    @Test
    void contract7_pruneTombstones_deletes_every_row_the_control_row_hides_without_a_grace_period() {
        Store writer = open();
        writer.write(List.of(entry("a", "1")));
        writer.write(List.of(tombstone("t")));
        pause(5);
        writer.write(List.of(control()));
        pause(5);
        writer.write(List.of(entry("after", "v")));
        writer.write(List.of(tombstone("young")));

        assertThat(writer.pruneTombstones(LONG)).as("the rows before it, without a grace period").isEqualTo(2);

        assertThat(writer.read(List.of("a", "t", "after", "young", Store.CONTROL_HASH), LONG))
                .containsOnlyKeys("after", "young", Store.CONTROL_HASH);
    }

    @Test
    void contract7_reserved_rows_are_never_expired_or_pruned() {
        Store writer = open();
        writer.write(List.of(Write.tombstone("#reserved", nextVersion())));
        pause(5);
        writer.write(List.of(control()));
        pause(5);
        writer.write(List.of(entry("v", "1")));
        pause(60);

        assertThat(writer.expire(Duration.ofMillis(50), nextVersion())).as("not the control row").isEqualTo(1);
        assertThat(writer.pruneToSize(0, Duration.ZERO)).isZero();
        assertThat(writer.pruneTombstones(Duration.ZERO))
                .as("the expired row, not the reserved tombstone, though older than the control row").isEqualTo(1);

        Map<String, Row> rows = writer.read(List.of("#reserved", Store.CONTROL_HASH, "v"), LONG);
        assertThat(rows).containsOnlyKeys("#reserved", Store.CONTROL_HASH);
        assertThat(rows.get("#reserved").state()).isEqualTo(State.TOMBSTONE);
        assertThat(rows.get(Store.CONTROL_HASH).state()).isEqualTo(State.VALUE);
    }

    @Test
    void contract7_expire_marks_rows_at_least_the_maximum_age_old_at_the_given_version() {
        Store writer = open();
        long written = written(writer, entry("old", "o"));
        pause(400);
        long young = written(writer, entry("young", "y"));
        long version = nextVersion();

        assertThat(writer.expire(Duration.ofMillis(300), version)).isEqualTo(1);

        Row old = writer.read(List.of("old"), LONG).get("old");
        assertThat(old.state()).isEqualTo(State.EXPIRED);
        if (fixture.ownVersions()) {
            assertThat(old.version()).as("a version of its own, above the value's").isGreaterThan(written);
        } else {
            assertThat(old.version()).isEqualTo(version);
        }
        Row kept = writer.read(List.of("young"), LONG).get("young");
        assertThat(kept.state()).isEqualTo(State.VALUE);
        assertThat(kept.version()).isEqualTo(young);
    }

    @Test
    void contract7_an_expired_row_is_kept_for_the_grace_period_from_its_expiry() {
        Store writer = open();
        writer.write(List.of(entry("h1", "v")));
        pause(400);
        assertThat(writer.expire(Duration.ofMillis(300), nextVersion())).isEqualTo(1);

        assertThat(writer.pruneTombstones(Duration.ofMillis(300))).as("its value was older, the expiry is not")
                .isZero();
        assertThat(writer.read(List.of("h1"), LONG).get("h1").state()).isEqualTo(State.EXPIRED);
        pause(400);
        assertThat(writer.pruneTombstones(Duration.ofMillis(300))).isEqualTo(1);
    }

    @Test
    void contract7_pruneToSize_deletes_the_oldest_values() {
        Store writer = open();
        for (int i = 0; i < 5; i++) {
            writer.write(List.of(entry("h" + i, "v")));
            pause(2);
        }

        assertThat(writer.pruneToSize(2, Duration.ZERO)).isEqualTo(3);

        assertThat(writer.read(List.of("h0", "h1", "h2", "h3", "h4"), LONG).keySet())
                .containsExactlyInAnyOrder("h3", "h4");
    }

    // the rank counts every value, the deleting spares the young ones beyond it: the maximum size exceeded until they
    // are old enough (Store#pruneToSize)
    @Test
    void contract7_pruneToSize_spares_values_younger_than_the_minimum_age() {
        Store writer = open();
        for (int i = 0; i < 3; i++) {
            writer.write(List.of(entry("old" + i, "v")));
        }
        pause(300);
        for (int i = 0; i < 3; i++) {
            writer.write(List.of(entry("young" + i, "v")));
        }

        assertThat(writer.pruneToSize(1, Duration.ofMillis(150))).as("the old ones beyond the rank").isEqualTo(3);
        assertThat(writer.read(List.of("old0", "old1", "old2", "young0", "young1", "young2"), LONG).keySet())
                .containsExactlyInAnyOrder("young0", "young1", "young2");

        pause(300);
        assertThat(writer.pruneToSize(1, Duration.ofMillis(150))).as("old enough now").isEqualTo(2);
    }

    @Test
    void contract7_pruneTombstones_keeps_values_and_young_tombstones() {
        Store writer = open();
        writer.write(List.of(entry("value", "v")));
        writer.write(List.of(tombstone("old")));
        pause(100);
        writer.write(List.of(tombstone("young")));

        assertThat(writer.pruneTombstones(Duration.ofMillis(60))).isEqualTo(1);

        assertThat(writer.read(List.of("value", "old", "young"), LONG).keySet())
                .containsExactlyInAnyOrder("value", "young");
    }
}
