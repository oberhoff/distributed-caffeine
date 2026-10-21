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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The change feed contract ({@link ChangeFeed}), run against every store that announces its writes: the reference
 * store and each adapter able to distribute. The storage itself is checked by the {@link StoreContractTest}.
 */
@SuppressWarnings("resource") // every store a test opens is closed after it (@AfterEach)
public abstract class ChangeFeedContractTest extends AbstractContractTest {

    private final List<Subscription> subscriptions = new ArrayList<>();

    // before the stores: closing a store leaves its subscriptions to whoever subscribed
    @AfterEach
    void closeSubscriptions() {
        subscriptions.forEach(Subscription::close);
    }

    // subscribes to the announcements of the store's cache; a store under this contract must offer a change feed
    protected Subscription subscribe(Store store, Listener listener) {
        assertThat(store).as("a store with a change feed").isInstanceOf(ChangeFeed.class);
        Subscription subscription = ((ChangeFeed) store).subscribe(listener);
        subscriptions.add(subscription);
        return subscription;
    }

    // contract point 1: a message exists exactly when a write committed, and only for what each write announces

    @Test
    void contract1_a_committed_put_is_announced_with_its_version() {
        Store writer = open();
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);

        long version = writer.put(List.of(entry("h1", "v1"))).get("h1");

        List<Message> messages = observer.await(m -> !m.isEmpty(), TIMEOUT);
        assertThat(messages).isNotEmpty().allMatch(m -> m.equals(new Message(Kind.WRITTEN, "h1", version)));
    }

    @Test
    void contract1_an_overwritten_value_is_announced_as_written() {
        Store writer = open();
        writer.put(List.of(entry("h1", "v1")));
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);

        long version = writer.put(List.of(entry("h1", "v2"))).get("h1");

        observer.await(m -> !m.isEmpty(), TIMEOUT);
        assertThat(observer.settle(QUIET)).allMatch(m -> m.equals(new Message(Kind.WRITTEN, "h1", version)));
    }

    @Test
    void contract1_every_entry_of_a_batch_is_announced() {
        Store writer = open();
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);

        Map<String, Long> versions = writer.put(List.of(entry("a", "1"), entry("b", "2"), entry("c", "3")));

        observer.await(m -> m.stream().map(Message::hash).collect(Collectors.toSet()).size() == 3, TIMEOUT);
        assertThat(observer.settle(QUIET)).allMatch(m ->
                m.kind() == Kind.WRITTEN && versions.get(m.hash()) == m.version());
    }

    @Test
    void contract1_reads_and_conflicts_announce_nothing() {
        Store writer = open();
        writer.put(List.of(entry("h1", "v1")));
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);

        writer.read(List.of("h1"), LONG);
        writer.readVersions(List.of("h1"), LONG);
        writer.putIfUnchanged(List.of(ExpectedEntry.ifMissing(entry("h1", "v2"), readTime())), LONG);
        writer.invalidateIfUnchanged(Map.of("h1", 1L));

        assertThat(observer.settle(QUIET)).isEmpty();
    }

    @Test
    void contract1_pruning_deletes_silently() {
        Store writer = open();
        for (int i = 0; i < 10; i++) {
            writer.put(List.of(entry("v" + i, "v")));
            writer.invalidate(List.of("t" + i));
        }
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);
        pause(5);

        assertThat(writer.pruneToSize(3)).isEqualTo(7);
        assertThat(writer.pruneTombstones(Duration.ZERO)).isEqualTo(10);

        assertThat(observer.settle(QUIET)).isEmpty();
    }

    @Test
    void contract1_invalidate_announces_the_tombstone_also_of_a_missing_row() {
        Store writer = open();
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);

        long version = writer.invalidate(List.of("missing")).get("missing");

        observer.await(m -> m.contains(new Message(Kind.INVALIDATED, "missing", version)), TIMEOUT);
        assertThat(observer.settle(QUIET)).allMatch(m -> m.equals(new Message(Kind.INVALIDATED, "missing", version)));
    }

    @Test
    void contract1_invalidateIfUnchanged_announces_only_the_tombstones_written() {
        Store writer = open();
        long a = writer.put(List.of(entry("a", "1"))).get("a");
        long b = writer.put(List.of(entry("b", "1"))).get("b");
        writer.put(List.of(entry("b", "2")));
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);

        Map<String, Long> invalidated = writer.invalidateIfUnchanged(Map.of("a", a, "b", b, "missing", 1L));

        observer.await(m -> !m.isEmpty(), TIMEOUT);
        assertThat(observer.settle(QUIET)).allMatch(m -> m.equals(new Message(Kind.INVALIDATED, "a", invalidated.get("a"))));
    }

    @Test
    void contract1_invalidateAll_announces_invalidated_all_with_its_time() {
        Store writer = open();
        writer.put(List.of(entry("a", "1"), entry("b", "2")));
        writer.invalidate(List.of("tombstone"));
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);

        long time = writer.invalidateAll();

        observer.await(m -> m.stream().anyMatch(message -> message.kind() == Kind.INVALIDATED_ALL), TIMEOUT);
        List<Message> messages = observer.settle(QUIET);
        assertThat(messages).allMatch(m -> m.kind() == Kind.INVALIDATED_ALL || m.kind() == Kind.INVALIDATED);
        assertThat(messages).filteredOn(message -> message.kind() == Kind.INVALIDATED_ALL)
                .containsExactly(Message.invalidatedAll(time));
    }

    @Test
    void contract1_expire_announces_only_the_rows_it_expired() {
        Store writer = open();
        // opened before the writes: opening a store takes long enough on some stores to age the young row
        Store watching = open();
        writer.put(List.of(entry("old", "o")));
        pause(400);
        writer.put(List.of(entry("young", "y")));
        RecordingListener observer = new RecordingListener();
        subscribe(watching, observer);

        assertThat(writer.expire(Duration.ofMillis(300))).isEqualTo(1);

        observer.await(m -> m.stream().anyMatch(x -> x.kind() == Kind.EXPIRED && "old".equals(x.hash())), TIMEOUT);
        assertThat(observer.settle(QUIET)).allMatch(m -> m.kind() == Kind.EXPIRED && "old".equals(m.hash()));
    }

    @Test
    void contract1_concurrent_expiry_announces_every_row_once() throws Exception {
        Store writer = open();
        for (int i = 0; i < 50; i++) {
            writer.put(List.of(entry("h" + i, "v")));
        }
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);
        pause(100);

        race(4, store -> store.expire(Duration.ofMillis(50)));

        List<Message> messages = observer.await(m -> m.stream().map(Message::hash).distinct().count() == 50, TIMEOUT);
        Map<String, Set<Long>> versionsPerHash = messages.stream().collect(Collectors.groupingBy(
                message -> requireNonNull(message.hash()), Collectors.mapping(Message::version, Collectors.toSet())));
        assertThat(messages).isNotEmpty().allMatch(m -> m.kind() == Kind.EXPIRED);
        assertThat(versionsPerHash.values()).isNotEmpty().allMatch(versions -> versions.size() == 1);
    }

    // contract point 2: a message carries the version the write got, as returned to the writer

    @Test
    void contract2_every_kind_of_write_announces_the_version_returned_to_the_writer() {
        Store writer = open();
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);

        long put = writer.put(List.of(entry("p", "v"))).get("p");
        long conditional = writer.putIfUnchanged(List.of(ExpectedEntry.ifMissing(entry("c", "v"), readTime())), LONG)
                .get("c").version();
        long invalidated = writer.invalidate(List.of("i")).get("i");
        long removed = writer.invalidateIfUnchanged(Map.of("p", put)).get("p");

        Set<Message> expected = Set.of(new Message(Kind.WRITTEN, "p", put), new Message(Kind.WRITTEN, "c", conditional),
                new Message(Kind.INVALIDATED, "i", invalidated), new Message(Kind.INVALIDATED, "p", removed));
        observer.await(m -> m.containsAll(expected), TIMEOUT);
        assertThat(observer.settle(QUIET)).allMatch(expected::contains);
    }

    // contract point 3: while subscribed every message arrives at least once, per key in commit order

    @Test
    void contract3_every_write_arrives_per_key_in_commit_order() {
        Store writer = open();
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);

        Map<String, List<Long>> committed = new HashMap<>();
        for (int i = 0; i < 300; i++) {
            String hash = "h" + (i % 20);
            long version = writer.put(List.of(entry(hash, "v" + i))).get(hash);
            committed.computeIfAbsent(hash, h -> new ArrayList<>()).add(version);
        }

        // by key and version: versions from a millisecond clock (MongoDB) repeat across keys
        List<Message> messages = observer.await(m -> committed.entrySet().stream().allMatch(entry -> m.stream()
                .filter(message -> entry.getKey().equals(message.hash())).map(Message::version)
                .collect(Collectors.toSet()).containsAll(entry.getValue())), TIMEOUT);
        committed.forEach((hash, versions) -> {
            List<Long> received = messages.stream().filter(m -> hash.equals(m.hash())).map(Message::version)
                    .distinct().toList();
            assertThat(received).as("per key in commit order: " + hash).isEqualTo(versions);
        });
    }

    @Test
    void contract3_lost_continuity_is_reported_as_a_gap() {
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);
        assumeTrue(fixture.induceGap(), "the fixture cannot induce a gap");

        assertThat(observer.awaitGaps(1, TIMEOUT)).isPositive();
    }

    // contract point 4: a subscription receives every write committed after subscribing, nothing after closing

    @Test
    void contract4_a_subscription_receives_every_write_committed_after_subscribing() {
        Store writer = open();
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);
        long version = writer.put(List.of(entry("h1", "v1"))).get("h1");

        assertThat(observer.await(m -> m.stream().anyMatch(x -> x.version() == version), TIMEOUT))
                .anyMatch(x -> x.version() == version);
    }

    @Test
    void contract4_a_closed_subscription_receives_nothing() {
        Store writer = open();
        RecordingListener observer = new RecordingListener();
        Subscription subscription = subscribe(open(), observer);
        subscription.close();

        writer.put(List.of(entry("h1", "v1")));

        assertThat(observer.settle(QUIET)).isEmpty();
    }

    // contract point 5: a lost delivery is reported and restored by resume(), from then on (what was written meanwhile
    // may be missed: the core counts every resume as a gap)

    // resumes until it succeeds (a feed may need a moment to reach its store again)
    private static void resume(Subscription subscription) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (true) {
            try {
                subscription.resume();
                return;
            } catch (StoreException e) {
                if (System.nanoTime() > deadline) {
                    throw e;
                }
                pause(100);
            }
        }
    }

    @Test
    void contract5_a_lost_delivery_is_reported_and_resume_restores_it() {
        Store writer = open();
        RecordingListener observer = new RecordingListener();
        Subscription subscription = subscribe(open(), observer);
        assumeTrue(fixture.induceLoss(), "the fixture cannot induce a loss");
        observer.awaitLosses(1, TIMEOUT);
        writer.put(List.of(entry("h1", "while lost")));

        resume(subscription);

        long after = writer.put(List.of(entry("h1", "after"))).get("h1");
        observer.await(m -> m.stream().anyMatch(x -> x.version() == after), TIMEOUT);
    }

    @Test
    void contract5_resuming_a_delivery_that_seems_alive_keeps_it_delivering() {
        Store writer = open();
        RecordingListener observer = new RecordingListener();
        Subscription subscription = subscribe(open(), observer);
        writer.put(List.of(entry("h1", "before")));

        resume(subscription);

        long after = writer.put(List.of(entry("h1", "after"))).get("h1");
        observer.await(m -> m.stream().anyMatch(x -> x.version() == after), TIMEOUT);
    }

    // contract point 6: once a message was delivered, a read through the subscribed store reflects its write

    @Test
    void contract6_a_read_after_a_message_reflects_the_announced_write() {
        Store writer = open();
        Store subscribed = open();
        RecordingListener observer = new RecordingListener();
        subscribe(subscribed, observer);

        for (int round = 0; round < 20; round++) {
            String hash = "h" + round;
            long version = writer.put(List.of(entry(hash, "v"))).get(hash);
            observer.await(m -> m.stream().anyMatch(x -> hash.equals(x.hash()) && x.version() == version), TIMEOUT);

            assertThat(subscribed.read(List.of(hash), LONG).rows().get(hash).version()).isGreaterThanOrEqualTo(version);
        }
    }

    // contract point 7: the writes of its own cache only

    @Test
    void contract7_a_subscription_receives_nothing_of_another_cache() {
        Store ours = open();
        Store theirs = openOther();
        RecordingListener observer = new RecordingListener();
        subscribe(open(), observer);

        theirs.put(List.of(entry("theirs", "v")));
        theirs.invalidate(List.of("theirs"));
        theirs.invalidateAll();
        long marker = ours.put(List.of(entry("ours", "v"))).get("ours");

        // per key in commit order only, so give the other cache's messages time to show up, should they
        observer.await(m -> m.stream().anyMatch(x -> x.version() == marker), TIMEOUT);
        assertThat(observer.settle(QUIET)).allMatch(m -> "ours".equals(m.hash()));
    }
}
