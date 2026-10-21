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
package io.github.oberhoff.distributedcaffeine.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.StoredValue;
import io.github.oberhoff.distributedcaffeine.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The MongoDB adapter on a replica set of three data-bearing members while members fail: two caches keep writing
 * through a step-down of the primary, a secondary restarting and the primary dying, and every write either succeeds
 * or fails as the store failed; afterwards both caches agree with the store, and no confirmed write is lost. Without
 * a majority, writes fail within the client's timeout instead of waiting, and resume once the members are back.
 */
@Timeout(300)
abstract class MongoFailoverTest {

    private static final Duration LONG = Duration.ofHours(1);
    private static final Duration CONVERGENCE = Duration.ofSeconds(60);
    private static final AtomicInteger COLLECTIONS = new AtomicInteger();
    private final MongoReplicaSet replicaSet = MongoTestDatabase.get().replicaSet();

    private final String collection = "failover_" + COLLECTIONS.incrementAndGet();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (int index = closeables.size() - 1; index >= 0; index--) {
            closeables.get(index).close();
        }
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    // a client with a timeout (client-side operation timeout): the driver retries an interrupted operation for as
    // long as a failover takes rather than once; without one (null), a write a failover interrupts may fail
    private MongoClient client(@Nullable Duration timeout) {
        return track(MongoClients.create(timeout == null ? replicaSet.settings().build()
                : replicaSet.settings().timeout(timeout.toMillis(), TimeUnit.MILLISECONDS).build()));
    }

    private DistributedCache<String, String> cache(MongoClient client) {
        return cache(client, 10_000, LONG);
    }

    private DistributedCache<String, String> cache(MongoClient client, long maximumSize, Duration maintenance) {
        return track(DistributedCaffeine.<String, String>newBuilder(
                        MongoAdapter.newBuilder(client, "failover", collection).build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(maximumSize)
                        .withMaintenanceInterval(maintenance))
                .build());
    }

    private long values(MongoClient client) {
        try {
            return client.getDatabase("failover").getCollection(collection)
                    .countDocuments(new org.bson.Document("state", "VALUE"));
        } catch (com.mongodb.MongoException e) {
            return -1;
        }
    }

    // failovers and restarts

    @Test
    void writing_rides_out_a_step_down_of_the_primary() throws Exception {
        assertWritingRidesOut(client(Duration.ofSeconds(30)), false, () -> {
            replicaSet.stepDown();
            replicaSet.handBack();
        });
    }

    @Test
    void a_write_a_step_down_makes_fail_leaves_both_caches_consistent() throws Exception {
        // on the client's defaults the driver retries an interrupted write only once, which may still land inside
        // the stepping down: whatever became of such a write, kept or lost, both caches have to agree with the store
        assertWritingRidesOut(client(null), true, () -> {
            replicaSet.stepDown();
            replicaSet.handBack();
        });
    }

    @Test
    void writing_rides_out_a_restart_of_a_secondary() throws Exception {
        // what a rolling update does to every secondary in turn; the two members left are a majority
        assertWritingRidesOut(client(Duration.ofSeconds(30)), false, () -> {
            replicaSet.stop(2);
            pause(1_000);
            replicaSet.start(2);
            replicaSet.awaitHealthy();
        });
    }

    @Test
    void writing_rides_out_the_primary_dying_abruptly() throws Exception {
        // killed rather than stepped down, so that it hands nothing over: the others notice only once its heartbeats
        // stay away for the election timeout, which is the longest a write waits
        assertWritingRidesOut(client(Duration.ofSeconds(30)), false, () -> {
            replicaSet.killPreferred();
            replicaSet.awaitFailover();
            replicaSet.start(0);
            replicaSet.handBack();
        });
    }

    @Test
    void without_a_majority_writes_fail_within_the_timeout_and_resume_once_it_is_back() {
        MongoClient client = client(Duration.ofSeconds(3));
        DistributedCache<String, String> writer = cache(client);
        DistributedCache<String, String> holder = cache(client);
        writer.put("k", "before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");

        replicaSet.stop(1);
        replicaSet.stop(2);
        try {
            for (int attempt = 0; attempt < 3; attempt++) {
                long started = System.nanoTime();
                String value = "during " + attempt;
                assertThatThrownBy(() -> writer.put("k", value)).isInstanceOf(StoreException.class);
                assertThat(Duration.ofNanos(System.nanoTime() - started)).as("failing rather than waiting")
                        .isLessThan(Duration.ofSeconds(15));
            }
            assertThat(writer.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);
        } finally {
            replicaSet.start(1);
            replicaSet.start(2);
            replicaSet.handBack();
        }

        assertThat(awaitUpTo(() -> {
            try {
                writer.put("k", "after");
                return true;
            } catch (StoreException e) {
                return false;
            }
        }, CONVERGENCE, Duration.ofMillis(500))).as("writing again").isTrue();
        await(() -> "after".equals(holder.getIfPresent("k")));
        assertThat(writer.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);
    }

    @Test
    void a_restart_of_the_whole_replica_set_is_ridden_out_once_it_is_back() {
        MongoClient client = client(Duration.ofSeconds(3));
        DistributedCache<String, String> writer = cache(client);
        DistributedCache<String, String> holder = cache(client);
        // maintenance every 200 ms, pruning to 10 values: it fails while the replica set is down
        cache(client, 10, Duration.ofMillis(200));
        writer.put("k", "before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");

        replicaSet.stop(2);
        replicaSet.stop(1);
        replicaSet.stop(0);
        try {
            for (int attempt = 0; attempt < 3; attempt++) {
                long started = System.nanoTime();
                String value = "during " + attempt;
                assertThatThrownBy(() -> writer.put("k", value)).isInstanceOf(StoreException.class);
                assertThat(Duration.ofNanos(System.nanoTime() - started)).as("failing rather than waiting")
                        .isLessThan(Duration.ofSeconds(15));
            }
            assertThat(writer.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);
            await(() -> holder.distributedPolicy().getSynchronizationState() == SynchronizationState.DEGRADED);
            assertThat(holder.getIfPresent("k")).as("a hit is served while the store is down").isEqualTo("before");
        } finally {
            replicaSet.start(0);
            replicaSet.start(1);
            replicaSet.start(2);
            replicaSet.handBack();
        }

        assertThat(awaitUpTo(() -> {
            try {
                writer.put("k", "after");
                return true;
            } catch (StoreException e) {
                return false;
            }
        }, CONVERGENCE, Duration.ofMillis(500))).as("writing again").isTrue();
        assertThat(awaitUpTo(() -> holder.distributedPolicy().getSynchronizationState()
                == SynchronizationState.SYNCHRONIZED, CONVERGENCE, Duration.ofMillis(250))).as("watching again")
                .isTrue();
        await(() -> "after".equals(holder.getIfPresent("k")));
        writer.put("k", "after the resume");
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("after the resume");
        assertThat(writer.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);

        // and maintenance goes on: what is written now is pruned to the maximum again
        for (int index = 0; index < 30; index++) {
            writer.put("p" + index, "v");
        }
        assertThat(awaitUpTo(() -> values(client) == 10, CONVERGENCE, Duration.ofMillis(100))).as("pruned again")
                .isTrue();
    }

    @Test
    void a_write_a_rollback_undoes_is_held_by_no_cache() {
        MongoClient client = client(Duration.ofSeconds(3));
        DistributedCache<String, String> writer = cache(client);
        DistributedCache<String, String> holder = cache(client);
        writer.put("k", "before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");

        // the secondaries stop replicating: the primary applies a write that no majority acknowledges
        replicaSet.stopReplication(1, true);
        replicaSet.stopReplication(2, true);
        try {
            assertThatThrownBy(() -> writer.put("k", "doomed")).isInstanceOfSatisfying(StoreException.class,
                    e -> assertThat(e.isOutcomeUnknown()).isTrue());
            assertThat(holder.getIfPresent("k")).as("majority reads never see it").isNotEqualTo("doomed");
            // the others elect a primary of their own, without that write; the old one rolls it back on its return
            replicaSet.pausePreferred();
            replicaSet.stopReplication(1, false);
            replicaSet.stopReplication(2, false);
            replicaSet.awaitFailover();
            assertThat(awaitUpTo(() -> {
                try {
                    writer.put("k", "after");
                    return true;
                } catch (StoreException e) {
                    return false;
                }
            }, CONVERGENCE, Duration.ofMillis(500))).as("writing on the new primary").isTrue();
        } finally {
            replicaSet.unpausePreferred();
        }
        replicaSet.handBack();

        assertThat(replicaSet.preferredLogs()).as("the old primary rolled back").contains("Rollback complete");
        // a cache started now has seen nothing yet; its first read must not be older than the confirmed write either
        DistributedCache<String, String> newcomer = cache(client);
        assertThat(newcomer.getIfPresent("k")).isEqualTo("after");
        assertThat(requireNonNull(writer.distributedPolicy().getFromStore("k")).value()).isEqualTo("after");
        await(() -> "after".equals(holder.getIfPresent("k")) && "after".equals(writer.getIfPresent("k")));
        for (DistributedCache<String, String> cache : List.of(writer, holder)) {
            assertThat(cache.policy().getIfPresentQuietly("k")).isNotEqualTo("doomed");
        }
        writer.put("k", "after the rollback");
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("after the rollback");
    }

    // the shared change stream

    @Test
    void a_listener_failing_on_a_change_event_leaves_the_others_on_the_stream_undisturbed() {
        MongoClient client = client(Duration.ofSeconds(30));
        AtomicInteger failed = new AtomicInteger();
        Store raw = track(MongoAdapter.newBuilder(client, "failover", collection).build()
                .createStore(StoreSettings.of(Format.TEXT, Format.TEXT)));
        track(((ChangeFeed) raw).subscribe(new ChangeListener() {
            @Override
            public void onChanges(List<Change> messages) {
                failed.incrementAndGet();
                throw new IllegalStateException("a listener failing on purpose");
            }

            @Override
            public void onGap() {
                // nothing held
            }

            @Override
            public void onLost(Throwable cause) {
                // nothing held, and resumed by the caches on the same session
            }
        }));
        DistributedCache<String, String> writer = cache(client);
        DistributedCache<String, String> holder = cache(client);

        for (String value : List.of("first", "second")) {
            writer.put("k", value);
            assertThat(holder.getIfPresent("k")).isEqualTo(value);
            writer.put("k", value + ", replaced");
            await(() -> holder.policy().getIfPresentQuietly("k") == null);
        }
        assertThat(failed).as("the failing listener keeps receiving").hasValueGreaterThanOrEqualTo(2);
        assertThat(holder.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);
    }

    // two caches writing 100 keys in turn while the disruption runs; a key whose write failed is not written again,
    // so that the failed write stays the last word on it
    private void assertWritingRidesOut(MongoClient client, boolean failuresAllowed, Runnable disruption)
            throws Exception {
        DistributedCache<String, String> first = cache(client);
        DistributedCache<String, String> second = cache(client);
        List<String> keys = IntStream.range(0, 100).mapToObj(index -> "k" + index).toList();
        AtomicBoolean writing = new AtomicBoolean(true);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        AtomicLong longestWriteNanos = new AtomicLong();
        // written by the writer alone and read once it is done
        Map<String, String> lastWritten = new HashMap<>();
        Set<String> inDoubt = new HashSet<>();
        CompletableFuture<Void> writer = CompletableFuture.runAsync(() -> {
            for (int index = 0; writing.get(); index++) {
                String key = keys.get(index % keys.size());
                if (inDoubt.contains(key)) {
                    continue;
                }
                long started = System.nanoTime();
                try {
                    (index % 2 == 0 ? first : second).put(key, String.valueOf(index));
                    lastWritten.put(key, String.valueOf(index));
                } catch (RuntimeException e) {
                    failures.add(e);
                    inDoubt.add(key);
                }
                longestWriteNanos.accumulateAndGet(System.nanoTime() - started, Math::max);
            }
        });
        try {
            pause(500); // writes under way before the disruption, so that some are caught by it
            disruption.run();
            pause(500); // and writes against the store restored
        } finally {
            writing.set(false);
            writer.get(2, TimeUnit.MINUTES);
        }

        if (!failuresAllowed) {
            assertThat(failures).isEmpty();
            assertThat(Duration.ofNanos(longestWriteNanos.get())).as("paused, never left waiting")
                    .isLessThan(Duration.ofSeconds(30));
        }
        assertThat(failures).as("failing only as the store failed").allSatisfy(failure ->
                assertThat(Stream.iterate(failure, Objects::nonNull, Throwable::getCause))
                        .anyMatch(StoreException.class::isInstance));
        assertThat(lastWritten.size() + inDoubt.size()).as("every key written").isGreaterThanOrEqualTo(keys.size());
        assertThat(awaitUpTo(() -> converged(keys, lastWritten, inDoubt, first, second), CONVERGENCE,
                Duration.ofMillis(200))).as("both caches agree with the store, which kept every confirmed write")
                .isTrue();
    }

    // every confirmed write is in the store, and neither cache holds anything else than the store
    private static boolean converged(List<String> keys, Map<String, String> lastWritten, Set<String> inDoubt,
                                     DistributedCache<String, String> first, DistributedCache<String, String> second) {
        for (String key : keys) {
            StoredValue<String> stored;
            try {
                stored = first.distributedPolicy().getFromStore(key);
            } catch (StoreException e) {
                return false;
            }
            String value = stored == null ? null : stored.value();
            if (!inDoubt.contains(key) && !Objects.equals(value, lastWritten.get(key))) {
                return false;
            }
            for (DistributedCache<String, String> cache : List.of(first, second)) {
                String held = cache.policy().getIfPresentQuietly(key);
                if (held != null && !held.equals(value)) {
                    return false;
                }
            }
        }
        return true;
    }
}
