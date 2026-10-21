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
import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.ReadResult;
import io.github.oberhoff.distributedcaffeine.spi.RecordingListener;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryStore;
import io.github.oberhoff.distributedcaffeine.testing.ScanOnlyStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The polling feed of the core (VISION.md, "The polling feed"): a store without a change feed of its own but with a
 * change scan is polled, its writes reach other instances within a poll interval, an {@code invalidateAll} through
 * the cut-off; writes that may have committed too late for every poll are touched again; polls failing longer than
 * the grace period report a loss.
 */
@Timeout(30)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
class InternalPollingTest {

    private static final Duration LONG = Duration.ofHours(1);
    private static final Duration POLL = Duration.ofMillis(20);

    private final InMemoryDatabase database = new InMemoryDatabase();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        executor.shutdownNow();
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private DistributedCache<String, String> polled() {
        Adapter<String, String> adapter = settings -> new ScanOnlyStore(database.openStore("cache"));
        return track(DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000).withPollInterval(POLL))
                .build());
    }

    // a polling store as the core builds it, on a given store, with a short overlap and grace period
    private InternalPollingStore polling(Store storage, Duration grace) {
        InMemoryStore scanned = track(database.openStore("cache"));
        return track(new InternalPollingStore(storage, scanned, POLL, Duration.ofMillis(20), grace,
                Duration.ofMillis(20), executor));
    }

    private static Entry entry(String hash, String value) {
        return new Entry(hash, hash.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    // the version of the one key the touch tests write
    private long version() {
        try (InMemoryStore store = database.openStore("cache")) {
            return store.read(List.of("h1"), LONG).rows().get("h1").version();
        }
    }

    // distribution through polls

    @Test
    void a_write_elsewhere_reaches_a_holder_through_a_poll() {
        DistributedCache<String, String> writer = polled();
        DistributedCache<String, String> holder = polled();
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        writer.put("k", "new");

        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("new");
        assertThat(holder.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);
    }

    @Test
    void an_invalidate_all_reaches_a_holder_through_the_cut_off() {
        DistributedCache<String, String> writer = polled();
        DistributedCache<String, String> holder = polled();
        writer.put("k", "v");
        assertThat(holder.getIfPresent("k")).isEqualTo("v");

        writer.invalidateAll();

        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isNull();
    }

    @Test
    void an_expiry_reaches_a_holder_although_the_value_is_older_than_a_polls_window() {
        java.util.function.Supplier<DistributedCache<String, String>> expiring = () -> {
            Adapter<String, String> adapter = settings -> new ScanOnlyStore(database.openStore("cache"));
            return track(DistributedCaffeine.newBuilder(adapter)
                    .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                            .withValueSerializer(StringSerializer.ofString()))
                    // older than the window of interval and overlap when it expires: found only by the expiry's own
                    // write time
                    .withStore(store -> store.withMaximumAge(Duration.ofSeconds(3)).withMaximumSize(1_000)
                            .withPollInterval(POLL).withMaintenanceInterval(Duration.ofMillis(100)))
                    .withCaffeine(CaffeineConfigurer::recordStats)
                    .build());
        };
        DistributedCache<String, String> writer = expiring.get();
        DistributedCache<String, String> holder = expiring.get();
        writer.put("k", "v");
        assertThat(holder.getIfPresent("k")).isEqualTo("v");

        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.distributedPolicy().stats().expiredMessageCount()).isPositive();
    }

    @Test
    void a_poll_interval_above_half_the_heartbeat_interval_is_refused() {
        Adapter<String, String> adapter = settings -> new ScanOnlyStore(database.openStore("cache"));
        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000)
                        .withPollInterval(Duration.ofSeconds(6)).withHeartbeatInterval(Duration.ofSeconds(10)));

        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("half the heartbeat interval");
    }

    // writes that may have committed too late for every poll

    @Test
    void a_write_slower_than_the_overlap_is_touched_again() {
        Store slow = new io.github.oberhoff.distributedcaffeine.testing.StorageOnlyStore(track(database.openStore(
                "cache"))) {
            @Override
            public Map<String, Long> put(Collection<Entry> entries) {
                Map<String, Long> versions = super.put(entries);
                pause(60);
                return versions;
            }
        };
        InternalPollingStore store = polling(slow, LONG);

        long written = store.put(List.of(entry("h1", "v"))).get("h1");

        await(() -> version() > written);
    }

    @Test
    void a_write_with_its_outcome_unknown_is_touched_again_after_twice_the_timeout() {
        InternalPollingStore store = polling(track(database.openStore("cache")), LONG);
        store.put(List.of(entry("h1", "before")));
        long before = version();
        database.failNextWrites(1, true);

        assertThatThrownBy(() -> store.put(List.of(entry("h1", "v")))).isInstanceOf(StoreException.class);
        long landed = version();

        assertThat(landed).as("it landed").isGreaterThan(before);
        await(() -> version() > landed);
    }

    @Test
    void a_row_hidden_by_the_cut_off_is_not_touched_back() {
        InMemoryStore other = track(database.openStore("cache"));
        Store slow = new io.github.oberhoff.distributedcaffeine.testing.StorageOnlyStore(track(database.openStore(
                "cache"))) {
            @Override
            public Map<String, Long> put(Collection<Entry> entries) {
                Map<String, Long> versions = super.put(entries);
                other.invalidateAll(); // the write is before the cut-off, its call returns after it
                pause(60);
                return versions;
            }
        };
        InternalPollingStore store = polling(slow, LONG);

        long written = store.put(List.of(entry("h1", "v"))).get("h1");
        pause(200);

        ReadResult read = other.read(List.of("h1"), LONG);
        assertThat(read.rows().get("h1").version()).as("not rewritten").isEqualTo(written);
        assertThat(read.rows().get("h1").stamp()).isLessThan(read.cutoff());
    }

    // polls failing

    @Test
    void polls_failing_shorter_than_the_grace_period_lose_nothing_longer_report_a_loss() {
        InternalPollingStore store = polling(track(database.openStore("cache")), Duration.ofMillis(300));
        RecordingListener observer = new RecordingListener();
        ChangeSubscription subscription = track(store.subscribe(observer));

        database.setDown(true);
        pause(100);
        database.setDown(false);
        store.put(List.of(entry("h1", "after a short failure")));
        observer.await(messages -> messages.stream().anyMatch(message -> "h1".equals(message.hash())),
                Duration.ofSeconds(5));
        assertThat(observer.awaitLosses(0, Duration.ZERO)).isZero();

        database.setDown(true);
        observer.awaitLosses(1, Duration.ofSeconds(5));
        database.setDown(false);
        subscription.resume();
        store.put(List.of(entry("h2", "after the resume")));

        List<Change> messages = observer.await(received -> received.stream()
                .anyMatch(message -> "h2".equals(message.hash())), Duration.ofSeconds(5));
        assertThat(messages).anyMatch(message -> message.kind() == Change.Kind.WRITTEN && "h2".equals(message.hash()));
    }
}
