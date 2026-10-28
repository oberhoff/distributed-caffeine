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
package io.github.oberhoff.distributedcaffeine.testing;

import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.RecordingListener;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The fault injection of the reference store, so that tests of the core can rely on it.
 */
class InMemoryDatabaseFaultsTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final Duration QUIET = Duration.ofMillis(300);
    private static final Duration LONG = Duration.ofHours(1);

    private static final AtomicLong VERSIONS = new AtomicLong();

    // an unconditional write of a value, at a version unique in this test class
    private static Write entry(String hash, String value) {
        return Write.value(hash, hash.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8),
                VERSIONS.incrementAndGet());
    }

    // writes the value and returns the version it was written at
    private static long put(InMemoryStore store, String hash, String value) {
        Write write = entry(hash, value);
        store.write(List.of(write));
        return write.version();
    }

    @Test
    void a_write_failing_before_commit_changes_and_announces_nothing() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            store.subscribe(observer);
            database.failNextWrites(1, false);

            assertThatThrownBy(() -> store.write(List.of(entry("h1", "v"))))
                    .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isFalse());

            assertThat(store.read(List.of("h1"), LONG)).isEmpty();
            assertThat(observer.settle(QUIET)).isEmpty();
            store.write(List.of(entry("h1", "v")));
            observer.await(m -> !m.isEmpty(), TIMEOUT);
        }
    }

    @Test
    void a_write_with_unknown_outcome_commits_announces_and_still_fails() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            store.subscribe(observer);
            database.failNextWrites(1, true);

            assertThatThrownBy(() -> store.write(List.of(entry("h1", "v"))))
                    .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isTrue());

            assertThat(store.read(List.of("h1"), LONG)).containsKey("h1");
            observer.await(m -> m.stream().anyMatch(x -> x.kind() == Change.Kind.WRITTEN), TIMEOUT);
        }
    }

    @Test
    void a_store_down_refuses_everything_delivers_nothing_and_resumes_with_messages_lost_when_back() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            ChangeSubscription subscription = store.subscribe(observer);
            database.pauseDelivery();
            store.write(List.of(entry("h1", "v")));

            database.setDown(true);
            observer.awaitLosses(1, TIMEOUT);
            assertThatThrownBy(subscription::resume).as("not reachable while down")
                    .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isRetryable()).isTrue());
            List<String> hashes = List.of("h1");
            List<Write> entries = List.of(entry("h2", "v"));
            assertThatThrownBy(() -> store.read(hashes, LONG)).isInstanceOf(StoreException.class);
            assertThatThrownBy(() -> store.write(entries)).isInstanceOf(StoreException.class);
            database.resumeDelivery();
            assertThat(observer.settle(QUIET)).as("undelivered messages are lost").isEmpty();

            database.setDown(false);
            subscription.resume();
            assertThat(store.read(List.of("h1"), LONG)).containsKey("h1");
        }
    }

    @Test
    void a_gap_loses_undelivered_messages() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            store.subscribe(observer);
            database.pauseDelivery();
            store.write(List.of(entry("h1", "v")));

            database.gap();
            database.resumeDelivery();

            observer.awaitGaps(1, TIMEOUT);
            assertThat(observer.settle(QUIET)).isEmpty();
        }
    }

    @Test
    void paused_delivery_queues_and_resumes_in_order() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            store.subscribe(observer);
            database.pauseDelivery();
            List<Long> versions = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                versions.add(put(store, "h1", "v" + i));
            }
            assertThat(observer.settle(QUIET)).isEmpty();

            database.resumeDelivery();

            List<Change> messages = observer.await(m -> m.size() == 10, TIMEOUT);
            assertThat(messages.stream().map(Change::version).toList()).isEqualTo(versions);
            assertThat(observer.batches()).hasSize(1);
        }
    }

    @Test
    void duplicates_are_delivered_next_to_their_originals() {
        InMemoryDatabase database = new InMemoryDatabase(InMemoryDatabaseFaultsTest::micros, 42);
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            store.subscribe(observer);
            database.setDuplicateProbability(1.0);

            long version = put(store, "h1", "v");

            assertThat(observer.await(m -> m.size() == 2, TIMEOUT))
                    .containsExactly(new Change(Change.Kind.WRITTEN, "h1", version),
                            new Change(Change.Kind.WRITTEN, "h1", version));
        }
    }

    @Test
    void reordering_across_keys_keeps_the_order_per_key_and_around_a_write_of_the_control_row() {
        InMemoryDatabase database = new InMemoryDatabase(InMemoryDatabaseFaultsTest::micros, 7);
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            store.subscribe(observer);
            database.setReorderAcrossKeys(true);
            database.pauseDelivery();
            List<Change> committed = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String hash = "h" + (i % 5);
                long version = put(store, hash, "v" + i);
                committed.add(new Change(Change.Kind.WRITTEN, hash, version));
            }
            // the core's invalidateAll: a write of the control row
            store.write(List.of(Write.value(Store.CONTROL_HASH, new byte[0], new byte[0], 999)));
            String hash = "h0";
            long after = put(store, hash, "after");

            database.resumeDelivery();

            List<Change> messages = observer.await(m -> m.size() == 102, TIMEOUT);
            assertThat(messages.get(100)).isEqualTo(new Change(Change.Kind.WRITTEN, Store.CONTROL_HASH, 999));
            assertThat(messages.get(101)).isEqualTo(new Change(Change.Kind.WRITTEN, hash, after));
            List<Change> beforeInvalidateAll = messages.subList(0, 100);
            assertThat(beforeInvalidateAll).as("reordered").isNotEqualTo(committed);
            Map<String, List<Long>> perKey = beforeInvalidateAll.stream().collect(Collectors.groupingBy(message -> requireNonNull(message.hash()),
                    Collectors.mapping(Change::version, Collectors.toList())));
            Map<String, List<Long>> committedPerKey = committed.stream().collect(Collectors.groupingBy(message -> requireNonNull(message.hash()),
                    Collectors.mapping(Change::version, Collectors.toList())));
            assertThat(perKey).isEqualTo(committedPerKey);
        }
    }

    @Test
    void delivery_delay_postpones_delivery() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            store.subscribe(observer);
            database.setDeliveryDelay(Duration.ofMillis(200));

            long start = System.nanoTime();
            int writes = 20;
            for (int i = 0; i < writes; i++) {
                store.write(List.of(entry("h" + i, "v")));
            }
            observer.await(m -> m.size() == writes, TIMEOUT);

            assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThan(Duration.ofMillis(10));
        }
    }

    @Test
    void the_store_clock_decides_ages_and_versions_are_stored_as_given() {
        AtomicLong clock = new AtomicLong(1_000_000_000L);
        InMemoryDatabase database = new InMemoryDatabase(clock::get, 1);
        try (InMemoryStore store = database.openStore("cache")) {

            store.write(List.of(Write.value("h1", new byte[]{1}, new byte[]{1}, 77)));
            clock.addAndGet(5_000_000L);
            Duration age = store.read(List.of("h1"), LONG).get("h1").age();
            clock.addAndGet(-60_000_000L); // the clock goes backwards
            store.write(List.of(Write.value("h1", new byte[]{1}, new byte[]{2}, 5)));

            assertThat(age).isEqualTo(Duration.ofSeconds(5));
            assertThat(store.read(List.of("h1"), LONG).get("h1").version()).as("as given, not ordered")
                    .isEqualTo(5);
        }
    }

    @Test
    void a_delayed_write_keeps_the_time_it_took_before_its_delay() {
        AtomicLong clock = new AtomicLong(1_000_000_000L);
        // every look at the clock moves it on: a write taking the time again after its delay would see a later one
        InMemoryDatabase database = new InMemoryDatabase(() -> clock.getAndAdd(1_000_000L), 1);
        try (InMemoryStore store = database.openStore("cache")) {
            database.setCommitDelay(Duration.ofMillis(5), 1);

            store.write(List.of(entry("h1", "v")));

            // the looks at the clock: the write time before the delay, the read
            Row row = store.read(List.of("h1"), LONG).get("h1");
            assertThat(row.age()).as("the write time is the time before the delay").isEqualTo(Duration.ofSeconds(1));
        }
    }

    @Test
    void the_step_before_a_conditional_write_runs_before_conditional_writes_only() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (InMemoryStore store = database.openStore("cache")) {
            AtomicLong steps = new AtomicLong();
            database.setBeforeConditionalWrite(steps::incrementAndGet);

            store.write(List.of(entry("h1", "v")));
            store.write(List.of(entry("h2", "v").ifMissing()));

            assertThat(steps).hasValue(1);
        }
    }

    private static long micros() {
        return System.nanoTime() / 1_000;
    }
}
