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
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The contract of {@link ChangeScan}, one test (or more) per point, named after it ({@code contractN_...}); run by
 * every store that offers a scan, against the fixture's stores.
 */
@SuppressWarnings("resource") // every store a test opens is closed after it (@AfterEach)
public abstract class ChangeScanContractTest extends AbstractContractTest {

    private ChangeScan scanning(Store store) {
        assumeTrue(store instanceof ChangeScan, "the store offers no change scan");
        return (ChangeScan) store;
    }

    private static Map<String, Change> byHash(List<Change> changes) {
        return changes.stream().collect(Collectors.toMap(Change::hash, Function.identity()));
    }

    // contract point 1: every row written within the window, with its latest state and version

    @Test
    void contract1_every_row_written_within_the_window_is_reported_with_its_latest_state_and_version() {
        Store writer = open();
        ChangeScan scan = scanning(open());
        writer.put(List.of(entry("expired", "e")));
        pause(60);
        writer.expire(Duration.ofMillis(50));
        long expired = writer.read(List.of("expired"), LONG).rows().get("expired").version();
        writer.put(List.of(entry("value", "v1")));
        long value = writer.put(List.of(entry("value", "v2"))).get("value");
        long tombstone = writer.invalidate(List.of("tombstone")).get("tombstone");

        Map<String, Change> changes = byHash(scan.changedWithin(Duration.ofSeconds(10)));

        assertThat(changes.get("value")).isEqualTo(new Change(Change.Kind.WRITTEN, "value", value));
        assertThat(changes.get("tombstone")).isEqualTo(new Change(Change.Kind.INVALIDATED, "tombstone", tombstone));
        assertThat(changes.get("expired")).isEqualTo(new Change(Change.Kind.EXPIRED, "expired", expired));
    }

    @Test
    void contract1_a_row_written_before_the_window_may_be_left_out_and_one_after_it_is_not() {
        Store writer = open();
        ChangeScan scan = scanning(open());
        writer.put(List.of(entry("before", "v")));
        pause(300);
        writer.put(List.of(entry("within", "v")));

        Map<String, Change> changes = byHash(scan.changedWithin(Duration.ofMillis(150)));

        assertThat(changes).containsKey("within").doesNotContainKey("before");
    }

    @Test
    void contract1_the_control_row_of_invalidate_all_is_not_reported() {
        Store writer = open();
        ChangeScan scan = scanning(open());

        writer.invalidateAll();

        assertThat(scan.changedWithin(Duration.ofSeconds(10))).allMatch(change -> change.hash() != null)
                .noneMatch(change -> change.kind() == Change.Kind.INVALIDATED_ALL);
    }

    // contract point 2: deletes report nothing

    @Test
    void contract2_deleted_rows_report_nothing() {
        Store writer = open();
        ChangeScan scan = scanning(open());
        writer.put(List.of(entry("pruned", "v")));
        pause(2);
        writer.put(List.of(entry("kept", "v")));
        writer.invalidate(List.of("tombstone"));
        assertThat(writer.pruneToSize(1)).isEqualTo(1);
        assertThat(writer.pruneTombstones(Duration.ZERO)).isEqualTo(1);

        assertThat(byHash(scan.changedWithin(Duration.ofSeconds(10)))).containsOnlyKeys("kept");
    }

    // contract point 3: the own cache's rows only

    @Test
    void contract3_a_scan_reports_nothing_of_another_cache() {
        Store theirs = openOther();
        ChangeScan scan = scanning(open());
        theirs.put(List.of(entry("h1", "theirs")));
        theirs.invalidate(List.of("h2"));

        assertThat(scan.changedWithin(Duration.ofSeconds(10))).isEmpty();
    }
}
