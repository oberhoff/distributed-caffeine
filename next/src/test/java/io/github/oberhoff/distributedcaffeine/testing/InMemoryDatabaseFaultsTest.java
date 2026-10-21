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

import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.Kind;
import io.github.oberhoff.distributedcaffeine.spi.Message;
import io.github.oberhoff.distributedcaffeine.spi.RecordingListener;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.Subscription;
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

    private static Entry entry(String hash, String value) {
        return new Entry(hash, hash.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void a_write_failing_before_commit_changes_and_announces_nothing() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            store.subscribe(observer);
            database.failNextWrites(1, false);

            assertThatThrownBy(() -> store.put(List.of(entry("h1", "v"))))
                    .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isFalse());

            assertThat(store.read(List.of("h1"), LONG).rows()).isEmpty();
            assertThat(observer.settle(QUIET)).isEmpty();
            store.put(List.of(entry("h1", "v")));
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

            assertThatThrownBy(() -> store.put(List.of(entry("h1", "v"))))
                    .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isTrue());

            assertThat(store.read(List.of("h1"), LONG).rows()).containsKey("h1");
            observer.await(m -> m.stream().anyMatch(x -> x.kind() == Kind.WRITTEN), TIMEOUT);
        }
    }

    @Test
    void a_store_down_refuses_everything_delivers_nothing_and_resumes_with_messages_lost_when_back() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            Subscription subscription = store.subscribe(observer);
            database.pauseDelivery();
            store.put(List.of(entry("h1", "v")));

            database.setDown(true);
            observer.awaitLosses(1, TIMEOUT);
            assertThatThrownBy(subscription::resume).as("not reachable while down")
                    .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isRetryable()).isTrue());
            List<String> hashes = List.of("h1");
            List<Entry> entries = List.of(entry("h2", "v"));
            assertThatThrownBy(() -> store.read(hashes, LONG)).isInstanceOf(StoreException.class);
            assertThatThrownBy(() -> store.put(entries)).isInstanceOf(StoreException.class);
            database.resumeDelivery();
            assertThat(observer.settle(QUIET)).as("undelivered messages are lost").isEmpty();

            database.setDown(false);
            subscription.resume();
            assertThat(store.read(List.of("h1"), LONG).rows()).containsKey("h1");
        }
    }

    @Test
    void a_gap_loses_undelivered_messages() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            store.subscribe(observer);
            database.pauseDelivery();
            store.put(List.of(entry("h1", "v")));

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
                versions.add(store.put(List.of(entry("h1", "v" + i))).get("h1"));
            }
            assertThat(observer.settle(QUIET)).isEmpty();

            database.resumeDelivery();

            List<Message> messages = observer.await(m -> m.size() == 10, TIMEOUT);
            assertThat(messages.stream().map(Message::version).toList()).isEqualTo(versions);
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

            long version = store.put(List.of(entry("h1", "v"))).get("h1");

            assertThat(observer.await(m -> m.size() == 2, TIMEOUT))
                    .containsExactly(new Message(Kind.WRITTEN, "h1", version),
                            new Message(Kind.WRITTEN, "h1", version));
        }
    }

    @Test
    void reordering_across_keys_keeps_the_order_per_key_and_around_invalidated_all() {
        InMemoryDatabase database = new InMemoryDatabase(InMemoryDatabaseFaultsTest::micros, 7);
        try (InMemoryStore store = database.openStore("cache")) {
            RecordingListener observer = new RecordingListener();
            store.subscribe(observer);
            database.setReorderAcrossKeys(true);
            database.pauseDelivery();
            List<Message> committed = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String hash = "h" + (i % 5);
                committed.add(new Message(Kind.WRITTEN, hash, store.put(List.of(entry(hash, "v" + i))).get(hash)));
            }
            store.invalidateAll();
            String hash = "h0";
            long after = store.put(List.of(entry(hash, "after"))).get(hash);

            database.resumeDelivery();

            List<Message> messages = observer.await(m -> m.size() == 102, TIMEOUT);
            assertThat(messages.get(100).kind()).isEqualTo(Kind.INVALIDATED_ALL);
            assertThat(messages.get(101)).isEqualTo(new Message(Kind.WRITTEN, hash, after));
            List<Message> beforeInvalidateAll = messages.subList(0, 100);
            assertThat(beforeInvalidateAll).as("reordered").isNotEqualTo(committed);
            Map<String, List<Long>> perKey = beforeInvalidateAll.stream().collect(Collectors.groupingBy(message -> requireNonNull(message.hash()),
                    Collectors.mapping(Message::version, Collectors.toList())));
            Map<String, List<Long>> committedPerKey = committed.stream().collect(Collectors.groupingBy(message -> requireNonNull(message.hash()),
                    Collectors.mapping(Message::version, Collectors.toList())));
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
                store.put(List.of(entry("h" + i, "v")));
            }
            observer.await(m -> m.size() == writes, TIMEOUT);

            assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThan(Duration.ofMillis(10));
        }
    }

    @Test
    void invalidate_all_can_also_announce_per_row() {
        InMemoryDatabase database = new InMemoryDatabase();
        try (InMemoryStore store = database.openStore("cache")) {
            store.put(List.of(entry("a", "1"), entry("b", "2")));
            RecordingListener observer = new RecordingListener();
            store.subscribe(observer);
            database.setInvalidateAllPerRow(true);

            store.invalidateAll();

            List<Message> messages = observer.await(
                    m -> m.stream().anyMatch(message -> message.kind() == Kind.INVALIDATED_ALL), TIMEOUT);
            assertThat(messages.stream().filter(m -> m.kind() == Kind.INVALIDATED).map(Message::hash))
                    .containsExactlyInAnyOrder("a", "b");
        }
    }

    @Test
    void the_store_clock_decides_versions_and_ages() {
        AtomicLong clock = new AtomicLong(1_000_000_000L);
        InMemoryDatabase database = new InMemoryDatabase(clock::get, 1);
        try (InMemoryStore store = database.openStore("cache")) {

            long first = store.put(List.of(entry("h1", "v"))).get("h1");
            clock.addAndGet(5_000_000L);
            Duration age = store.read(List.of("h1"), LONG).rows().get("h1").age();
            clock.addAndGet(-60_000_000L); // the clock goes backwards
            long second = store.put(List.of(entry("h1", "v2"))).get("h1");

            assertThat(first).isEqualTo(1_000_000_000L);
            assertThat(age).isEqualTo(Duration.ofSeconds(5));
            assertThat(second).isEqualTo(first + 1);
        }
    }

    private static long micros() {
        return System.nanoTime() / 1_000;
    }
}
