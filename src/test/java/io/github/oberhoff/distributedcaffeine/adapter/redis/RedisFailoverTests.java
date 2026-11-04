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
package io.github.oberhoff.distributedcaffeine.adapter.redis;

import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Redis adapter on a primary with a replica, under Sentinel or in a cluster, while the primary changes (in a
 * cluster also while the slot moves to another primary): two caches (each its own
 * driver, as two applications) keep writing through a failover, and a write the failover loses is dropped by both,
 * never shadowing a later one. A write is lost by running the store's write script with replication switched off
 * ({@code redis.set_repl}): the primary applies and announces it, the replica never gets the row, the entry or the
 * counter, as after a primary failing before it replicated. (Pausing the replica's container does not lose it: the
 * kernel still takes the replication stream, which the replica applies once resumed.)
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(300)
abstract class RedisFailoverTests {

    private static final Duration LONG = Duration.ofHours(1);
    private static final Duration CONVERGENCE = Duration.ofSeconds(60);
    // the one key the tests write, its own hash
    private static final String KEY = "k";

    private final RedisTopology topology = topology();
    private final String name = RedisTestServer.freshName();
    // the row of the key, whose primary fails over
    private final String row = new RedisKeys(name, RedisAdapter.DEFAULT_DISCRIMINATOR).row(KEY);
    private final List<AutoCloseable> closeables = new ArrayList<>();

    // the topology of the enclosing image
    protected abstract RedisTopology topology();

    // the binding under test: a new driver of the topology's client of its library, one per cache, as two
    // applications would have
    protected abstract RedisDriver driver();

    @BeforeEach
    void requireFailover() {
        Assumptions.assumeTrue(topology.failsOver(), "the topology fails over only by its control plane");
    }

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

    private RedisAdapter<String, String> adapter() {
        return RedisAdapter.newBuilder(driver(), name).build();
    }

    // a cache noticing a silent feed within seconds, and giving up on a paused primary within two; a key is its own
    // hash, so its row can be read directly
    private DistributedCache<String, String> cache() {
        return track(DistributedCaffeine.newBuilder(adapter())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withHashProvider((key, hasher) -> key)
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10_000)
                        .withOperationTimeout(Duration.ofSeconds(2)).withHeartbeatInterval(Duration.ofSeconds(2)))
                .build());
    }

    // puts the key, until the store takes it: a failover refuses writes for a moment
    private static void put(DistributedCache<String, String> cache, String value) {
        AtomicReference<@Nullable StoreException> last = new AtomicReference<>();
        boolean written = awaitUpTo(() -> {
            try {
                cache.put(KEY, value);
                return true;
            } catch (StoreException e) {
                last.set(e);
                return false;
            }
        }, CONVERGENCE, Duration.ofMillis(200));
        assertThat(written).as(() -> {
            StoreException failure = last.get();
            return "put " + value + " within " + CONVERGENCE + "; last failure: "
                    + (failure == null ? "none" : failure + ", caused by " + failure.getCause());
        }).isTrue();
    }

    private static @Nullable String quietly(DistributedCache<String, String> cache) {
        try {
            return cache.getIfPresent(KEY);
        } catch (StoreException e) {
            return "failed";
        }
    }

    private static void converges(DistributedCache<String, String> cache, String value) {
        assertThat(awaitUpTo(() -> value.equals(quietly(cache)), CONVERGENCE, Duration.ofMillis(200)))
                .as("converges on " + value + ", last " + quietly(cache)).isTrue();
    }

    // the store's write script, its writes not replicated
    private static final RedisScript UNREPLICATED = RedisScript.of("unreplicated", RedisScript.Reply.ARRAY,
            "redis.set_repl(redis.REPL_NONE)\n" + RedisScripts.WRITE.source());
    // the same on a primary whose clock runs an hour ahead of its replica's: its versions' floor raised by an hour
    // (the write time and the stream's trimming, which also read the clock, left alone, so the entry is still read)
    private static final String FLOOR = "local floor = math.floor(now / 1000) - tonumber(ARGV[1])";
    private static final RedisScript UNREPLICATED_AHEAD = RedisScript.of("unreplicated-ahead",
            RedisScript.Reply.ARRAY, UNREPLICATED.source().replace(FLOOR, FLOOR + " + 3600000"));

    @Test
    void writing_rides_out_a_failover() {
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = cache();
        put(writer, "before");
        assertThat(holder.getIfPresent(KEY)).isEqualTo("before");
        int primary = topology.primary(row);

        topology.failover(row);

        assertThat(topology.primary(row)).as("the replica is primary now").isNotEqualTo(primary);
        put(writer, "after");
        converges(holder, "after");
        converges(writer, "after");
    }

    // killed rather than failed over by hand, so that it hands nothing over: the writer's commands fail until the
    // replica took over (Sentinel's or the cluster's view), the readers lose their connection; afterwards both caches
    // read, write and hear each other. The killed node is started again for the next tests
    @Test
    void writing_rides_out_the_primary_dying_abruptly() {
        Assumptions.assumeTrue(topology.revives(), "a killed node does not return as a replica here");
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = cache();
        put(writer, "before");
        assertThat(holder.getIfPresent(KEY)).isEqualTo("before");
        int primary = topology.primary(row);

        try {
            topology.kill(row);
            assertThat(topology.primary(row)).as("the replica is primary now").isNotEqualTo(primary);
            put(writer, "after");
            converges(holder, "after");
            converges(writer, "after");
        } finally {
            topology.revive();
        }
        put(writer, "revived");
        converges(holder, "revived");
    }

    // resharding in a cluster: while a writer keeps writing, the slot of the cache's keyspace moves to another primary
    // with every key in it (the stream included, its entries and IDs kept); afterwards both caches still read, write
    // and hear each other
    @Test
    void writing_rides_out_the_slot_moving() throws InterruptedException {
        Assumptions.assumeTrue(topology instanceof RedisCluster, "a slot moves only in a cluster");
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = cache();
        put(writer, "before");
        converges(holder, "before");
        int primary = topology.primary(row);
        AtomicBoolean moving = new AtomicBoolean(true);
        AtomicInteger written = new AtomicInteger();
        Thread writing = new Thread(() -> {
            while (moving.get()) {
                try {
                    writer.put(KEY, "during-" + written.incrementAndGet());
                } catch (StoreException e) {
                    // refused while the slot moves (TRYAGAIN, a redirect not yet followed): nothing applied
                }
            }
        }, "writing while the slot moves");
        writing.start();
        try {
            ((RedisCluster) topology).move(row);
        } finally {
            moving.set(false);
            writing.join();
        }

        assertThat(topology.primary(row)).as("another primary owns the slot now").isNotEqualTo(primary);
        put(writer, "after");
        converges(holder, "after");
        converges(writer, "after");
    }

    @Test
    void a_write_the_failover_lost_is_dropped_by_every_cache_and_never_shadows_a_later_one() {
        Assumptions.assumeTrue(topology.losesWritesOnDemand(), "no redis.set_repl to keep a write from the replica");
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = cache();
        put(writer, "kept");
        assertThat(holder.getIfPresent(KEY)).isEqualTo("kept");
        converges(holder, "kept");
        RedisStore store = (RedisStore) track(adapter().createStore(StoreSettings.of(Format.TEXT, Format.TEXT)));
        // the kept value on the replica too, or the failover loses it as well
        topology.awaitInSync(row);

        // confirmed by the primary, never replicated
        byte[] lostValue = "lost".getBytes(StandardCharsets.UTF_8);
        Write unreplicated = Write.value(KEY, lostValue, lostValue, 1);
        long lost = store.write(List.of(unreplicated), "unreplicated", UNREPLICATED).version(unreplicated);
        converges(writer, "lost");
        converges(holder, "lost");
        String before = topology.describe(row);
        topology.failover(row);

        try {
            converges(writer, "kept");
            converges(holder, "kept");
        } catch (AssertionError e) {
            throw new AssertionError(e.getMessage() + "; before the failover: " + before + "after: "
                    + topology.describe(row), e);
        }
        byte[] bytes = "later".getBytes(StandardCharsets.UTF_8);
        Write later = Write.value(KEY, bytes, bytes, 1);
        assertThat(store.write(List.of(later)).version(later)).as("above the lost write's version").isGreaterThan(lost);
    }

    // the clock that matters is the store's: a new primary whose clock is behind the old one's issues versions below
    // those of a write the failover lost. Every cache must still drop the lost value (it held it at a version the
    // store no longer has) and take the later writes, whose versions are lower than the one it dropped
    @Test
    void a_write_lost_with_a_clock_ahead_never_shadows_the_later_ones_below_it() {
        Assumptions.assumeTrue(topology.losesWritesOnDemand(), "no redis.set_repl to keep a write from the replica");
        assertThat(UNREPLICATED_AHEAD.source()).as("the write script's floor found and raised").contains(FLOOR + " + ");
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = cache();
        put(writer, "kept");
        converges(holder, "kept");
        RedisStore store = (RedisStore) track(adapter().createStore(StoreSettings.of(Format.TEXT, Format.TEXT)));
        topology.awaitInSync(row);

        byte[] lostValue = "lost".getBytes(StandardCharsets.UTF_8);
        Write unreplicated = Write.value(KEY, lostValue, lostValue, 1);
        long lost = store.write(List.of(unreplicated), "unreplicated-ahead", UNREPLICATED_AHEAD)
                .version(unreplicated);
        converges(writer, "lost");
        converges(holder, "lost");
        topology.failover(row);

        converges(writer, "kept");
        converges(holder, "kept");
        put(writer, "later");
        converges(holder, "later");
        converges(writer, "later");
        byte[] bytes = "latest".getBytes(StandardCharsets.UTF_8);
        Write latest = Write.value(KEY, bytes, bytes, 1);
        assertThat(store.write(List.of(latest)).version(latest)).as("below the lost write's version")
                .isLessThan(lost);
        converges(holder, "latest");
        converges(writer, "latest");
    }
}
