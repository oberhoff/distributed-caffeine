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
package io.github.oberhoff.distributedcaffeine.redis;

import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.DistributionMode;
import io.github.oberhoff.distributedcaffeine.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XTrimArgs;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reader caches share (one per client, one per keyspace in a cluster), on the server or cluster of the enclosing
 * image: the stream trimmed past a cache's position is a gap after which what changed is found; a listener failing,
 * or a cache slow to apply what arrives, holds up no other cache on the reader. Every cache of a test shares one
 * driver, so one reader; the writer has a driver, so a reader, of its own.
 */
@NullMarked
@Timeout(120)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
abstract class RedisFeedTests {

    private static final Duration LONG = Duration.ofHours(1);

    private final String name = RedisTestServer.freshName();
    private final RedisKeys keys = new RedisKeys(name, RedisAdapter.DEFAULT_DISCRIMINATOR);
    private final RedisDriver shared = driver();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    // the tests' own commands on the server or cluster of the enclosing image
    protected abstract RedisClusterCommands<String, String> admin();

    // a new driver of the client of the enclosing image: one per call, so a reader of its own
    protected abstract RedisDriver driver();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private DistributedCaffeine<String, String> strings(RedisDriver driver, StringSerializer<String> values) {
        RedisAdapter<String, String> adapter = RedisAdapter.newBuilder(driver, name).build();
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(values))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10_000));
    }

    private DistributedCache<String, String> cache(RedisDriver driver) {
        return track(strings(driver, StringSerializer.ofString()).build());
    }

    // writes only, through a reader of its own
    private DistributedCache<String, String> writer() {
        return track(strings(driver(), StringSerializer.ofString()).withDistributionMode(DistributionMode.NONE)
                .build());
    }

    // a raw subscription on the shared reader, its listener as given
    private void subscribe(ChangeListener listener) {
        Store raw = track(RedisAdapter.newBuilder(shared, name).build()
                .createStore(StoreSettings.of(Format.TEXT, Format.TEXT)));
        track(((ChangeFeed) raw).subscribe(listener));
    }

    // the stream trimmed (MINID) past a cache's position, as by age while the cache fell behind

    @Test
    void the_stream_trimmed_past_a_caches_position_is_a_gap_after_which_what_changed_is_found() throws Exception {
        CountDownLatch stalled = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean stall = new AtomicBoolean();
        AtomicBoolean gap = new AtomicBoolean();
        // holds up the shared reader, so every cache on it falls behind, as a slow reader would
        subscribe(new ChangeListener() {
            @Override
            public void onChanges(List<Change> changes) {
                if (stall.getAndSet(false)) {
                    stalled.countDown();
                    try {
                        if (!release.await(60, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("the reader was never released");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            @Override
            public void onGap() {
                gap.set(true);
            }

            @Override
            public void onLost(Throwable cause) {
                // resumed by the cache on the same reader
            }
        });
        DistributedCache<String, String> holder = cache(shared);
        DistributedCache<String, String> writer = writer();
        writer.put("k1", "old");
        assertThat(holder.getIfPresent("k1")).isEqualTo("old");

        stall.set(true);
        writer.put("k0", "stalling the reader");
        assertThat(stalled.await(10, TimeUnit.SECONDS)).as("the reader held up").isTrue();
        writer.put("k1", "new");
        writer.put("k2", "after");
        // everything before the last entry trimmed, k1's change with it, unread by the holder
        List<StreamMessage<String, String>> last = admin().xrevrange(keys.stream(), Range.create("-", "+"),
                io.lettuce.core.Limit.from(1));
        admin().xtrim(keys.stream(), XTrimArgs.Builder.minId(last.get(0).getId()));
        assertThat(holder.policy().getIfPresentQuietly("k1")).as("not yet read").isEqualTo("old");
        release.countDown();

        await(() -> !"old".equals(holder.policy().getIfPresentQuietly("k1")));
        assertThat(gap).as("the trimmed entries' numbers skipped").isTrue();
        assertThat(holder.getIfPresent("k1")).isEqualTo("new");
        assertThat(holder.getIfPresent("k2")).isEqualTo("after");
        assertThat(awaitUpTo(() -> holder.distributedPolicy().getSynchronizationState()
                == SynchronizationState.SYNCHRONIZED, Duration.ofSeconds(30), Duration.ofMillis(50))).isTrue();
    }

    // a server sharding behind one endpoint (Azure Managed Redis, Enterprise policy) refuses the shared read once two
    // keyspaces use it (CROSSSLOT): the reader splits by keyspace, and caches of both keyspaces go on hearing each
    // other, nothing lost, nothing reported
    @Test
    void a_server_refusing_reads_across_slots_gets_a_reader_per_keyspace_and_nothing_is_missed() {
        RedisSlotEnforcingDriver enforcing = new RedisSlotEnforcingDriver(driver());
        String other = RedisTestServer.freshName();
        // a cache of each keyspace on the enforcing driver, and a writer of each with a driver of its own
        DistributedCache<String, String> first = track(strings(enforcing, name).build());
        DistributedCache<String, String> second = track(strings(enforcing, other).build());
        DistributedCache<String, String> writer = writer();
        DistributedCache<String, String> otherWriter = track(strings(driver(), other)
                .withDistributionMode(DistributionMode.NONE).build());

        for (String value : List.of("first", "second")) {
            writer.put("k", value);
            otherWriter.put("k", value);
            assertThat(first.getIfPresent("k")).isEqualTo(value);
            assertThat(second.getIfPresent("k")).isEqualTo(value);
            writer.put("k", value + ", replaced");
            otherWriter.put("k", value + ", replaced");
            await(() -> first.policy().getIfPresentQuietly("k") == null
                    && second.policy().getIfPresentQuietly("k") == null);
        }
        assertThat(first.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);
        assertThat(second.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);
        // in a cluster the readers are per keyspace anyway, and no shared read is ever made (Valkey from 8 reports
        // server_mode, not redis_mode)
        String server = admin().info("server");
        if (!server.contains("redis_mode:cluster") && !server.contains("server_mode:cluster")) {
            assertThat(enforcing.refused).as("the shared read refused once").hasPositiveValue();
        }
    }

    private DistributedCaffeine<String, String> strings(RedisDriver driver, String keyspace) {
        RedisAdapter<String, String> adapter = RedisAdapter.newBuilder(driver, keyspace).build();
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10_000));
    }

    // the reader caches share

    @Test
    void a_listener_failing_on_its_changes_leaves_the_others_on_the_reader_undisturbed() {
        AtomicInteger failed = new AtomicInteger();
        subscribe(new ChangeListener() {
            @Override
            public void onChanges(List<Change> changes) {
                failed.incrementAndGet();
                throw new IllegalStateException("a listener failing on purpose");
            }

            @Override
            public void onGap() {
                // nothing held
            }

            @Override
            public void onLost(Throwable cause) {
                // nothing held, and resumed by the caches on the same reader
            }
        });
        DistributedCache<String, String> writer = writer();
        DistributedCache<String, String> holder = cache(shared);

        for (String value : List.of("first", "second")) {
            writer.put("k", value);
            assertThat(holder.getIfPresent("k")).isEqualTo(value);
            writer.put("k", value + ", replaced");
            await(() -> holder.policy().getIfPresentQuietly("k") == null);
        }
        assertThat(failed).as("the failing listener keeps receiving").hasValueGreaterThanOrEqualTo(2);
        assertThat(holder.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);
    }

    @Test
    void a_cache_slow_to_apply_what_arrives_holds_up_no_other_cache_on_the_reader() {
        AtomicBoolean slowly = new AtomicBoolean();
        // updates its copy by reading the new value, which takes it 2 s to deserialize once slowed
        DistributedCache<String, String> slow = track(strings(shared, new StringSerializer<>() {
            @Override
            public String serialize(String object) {
                return object;
            }

            @Override
            @SuppressWarnings("java:S2925") // a slow deserializer is what the test needs
            public String deserialize(String value) throws InterruptedException {
                if (slowly.get()) {
                    Thread.sleep(2_000);
                }
                return value;
            }
        }).withDistributionMode(DistributionMode.UPDATE).build());
        DistributedCache<String, String> fast = cache(shared);
        DistributedCache<String, String> writer = writer();
        writer.put("k", "old");
        assertThat(slow.getIfPresent("k")).isEqualTo("old");
        assertThat(fast.getIfPresent("k")).isEqualTo("old");

        slowly.set(true);
        writer.put("k", "new");

        assertThat(awaitUpTo(() -> fast.policy().getIfPresentQuietly("k") == null, Duration.ofMillis(1_500),
                Duration.ofMillis(10))).as("the other cache applies the change at once").isTrue();
        assertThat(slow.policy().getIfPresentQuietly("k")).as("still reading the new value").isEqualTo("old");
        assertThat(awaitUpTo(() -> "new".equals(slow.policy().getIfPresentQuietly("k")), Duration.ofSeconds(20),
                Duration.ofMillis(50))).as("and the slow one gets there").isTrue();
    }
}
