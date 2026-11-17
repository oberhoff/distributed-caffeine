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
package io.github.oberhoff.distributedcaffeine.adapter.jdbc;

import io.github.oberhoff.distributedcaffeine.CaffeineConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.DistributedPolicy;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The JDBC adapters on a MySQL or MariaDB primary (the image of the subclass, {@link JdbcIntegrationTest}) with an
 * asynchronous replica while the primary fails over (VISION.md, "MariaDB and MySQL", step 4): writing rides it out, and
 * a write the failover loses (confirmed by the primary, never replicated) is held by no cache afterwards, although the
 * caches learn of changes by polling only.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(300)
abstract class JdbcFailoverTest {

    private static final Duration CONVERGENCE = Duration.ofSeconds(30);
    // not JdbcTestDatabase's names, which would start its database
    private static final AtomicInteger TABLES = new AtomicInteger();

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private @Nullable JdbcReplication replication;

    @AfterEach
    void closeCaches() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
        closeables.clear();
        if (replication != null) {
            replication.close();
            replication = null;
        }
    }

    // the image of both servers
    protected abstract String image();

    // the URL scheme of the applications' driver (JdbcTestDatabase)
    protected abstract String driver();

    // a topology of its own per test, as a failover leaves it with one server
    private JdbcReplication replication() throws IOException {
        replication = new JdbcReplication(image(), driver());
        return replication;
    }

    private DistributedCache<String, String> cache(JdbcReplication topology, String table) {
        DistributedCache<String, String> cache = DistributedCaffeine.<String, String>newBuilder(
                        topology.adapter(table))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withHashProvider((key, hasher) -> key)
                .withCaffeine(CaffeineConfigurer::recordStats)
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000)
                        .withPollInterval(Duration.ofMillis(100)).withOperationTimeout(Duration.ofSeconds(3)))
                .build();
        closeables.add(cache);
        return cache;
    }

    // puts the key, until the store takes it: a failover refuses writes for a moment
    private static void put(DistributedCache<String, String> cache, String value) {
        put(cache, "k", value);
    }

    private static void put(DistributedCache<String, String> cache, String key, String value) {
        assertThat(awaitUpTo(() -> {
            try {
                cache.put(key, value);
                return true;
            } catch (StoreException e) {
                return false;
            }
        }, CONVERGENCE, Duration.ofMillis(200))).as("put " + value + " within " + CONVERGENCE).isTrue();
    }

    @SuppressWarnings("java:S2925") // time passing is the point, there is nothing to poll
    private static void pause(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static @Nullable String quietly(DistributedCache<String, String> cache) {
        return quietly(cache, "k");
    }

    private static @Nullable String quietly(DistributedCache<String, String> cache, String key) {
        try {
            return cache.getIfPresent(key);
        } catch (StoreException e) {
            return "failed";
        }
    }

    private static boolean converges(DistributedCache<String, String> cache, String value) {
        return awaitUpTo(() -> value.equals(quietly(cache)), CONVERGENCE, Duration.ofMillis(200));
    }

    @Test
    void writing_rides_out_a_failover() throws IOException {
        JdbcReplication topology = replication();
        String table = "failover_" + TABLES.incrementAndGet();
        DistributedCache<String, String> writer = cache(topology, table);
        DistributedCache<String, String> holder = cache(topology, table);
        put(writer, "before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");
        topology.awaitInSync();

        topology.failover();

        put(writer, "after");
        assertThat(converges(holder, "after")).as("the holder converges on the write after the failover").isTrue();
        assertThat(converges(writer, "after")).isTrue();
    }

    @Test
    void a_write_the_failover_lost_is_held_by_no_cache() throws IOException {
        JdbcReplication topology = replication();
        String table = "failover_" + TABLES.incrementAndGet();
        DistributedCache<String, String> writer = cache(topology, table);
        DistributedCache<String, String> holder = cache(topology, table);
        put(writer, "kept");
        assertThat(converges(holder, "kept")).isTrue();
        topology.holdBackReplication();
        // the value the failover falls back to older than the polls' overlap (2 s, the scan's window reaching twice
        // that), as it usually is: a poll after the failover cannot report it again (found: written moments before
        // the lost write, it was reported again, and the holder dropped the lost write for that reason alone)
        pause(Duration.ofSeconds(6));

        // confirmed by the primary, never replicated; both caches learn of it (the holder by polling)
        put(writer, "lost");
        assertThat(converges(holder, "lost")).as("the holder polled the lost write").isTrue();
        String before = "primary " + topology.row(true, table, "k") + ", replica " + topology.row(false, table, "k");
        topology.failover();

        // the store now has "kept" again: no cache may go on serving "lost"
        assertThat(converges(holder, "kept")).as(() -> "the holder drops the lost write; before the failover: "
                + before + "; after: " + topology.row(false, table, "k") + "; the holder serves " + quietly(holder))
                .isTrue();
        assertThat(converges(writer, "kept")).as(() -> "the writer drops the lost write; it serves "
                + quietly(writer)).isTrue();
    }

    // a key the lost write created: the new primary has no row of it, so no scan can report it; what the caches hold
    // is checked against the store after the failover (another server answering), and the store has none
    @Test
    void a_key_the_failover_lost_with_its_first_write_is_held_by_no_cache() throws IOException {
        JdbcReplication topology = replication();
        String table = "failover_" + TABLES.incrementAndGet();
        DistributedCache<String, String> writer = cache(topology, table);
        DistributedCache<String, String> holder = cache(topology, table);
        put(writer, "other", "replicated");
        topology.holdBackReplication();

        put(writer, "fresh", "lost");
        assertThat(awaitUpTo(() -> "lost".equals(quietly(holder, "fresh")), CONVERGENCE, Duration.ofMillis(200)))
                .as("the holder read the lost key").isTrue();
        topology.failover();

        for (int index = 0; index < 2; index++) {
            DistributedCache<String, String> cache = index == 0 ? holder : writer;
            String which = index == 0 ? "holder" : "writer";
            assertThat(awaitUpTo(() -> cache.policy().getIfPresentQuietly("fresh") == null
                            && quietly(cache, "fresh") == null, CONVERGENCE, Duration.ofMillis(200)))
                    .as(() -> "the " + which + " dropped the key; it serves " + quietly(cache, "fresh") + ", holds "
                            + cache.policy().getIfPresentQuietly("fresh") + ", the replica has "
                            + topology.row(false, table, "fresh") + "; " + cache.distributedPolicy().stats())
                    .isTrue();
        }
        assertThat(quietly(holder, "other")).isEqualTo("replicated");
    }

    // a refill across a failover (VISION.md, "Refill while serving"): the mark taken and the fill written before it,
    // invalidateAllBeforeMark retried until the new primary takes it, as an application retries a failed call (it is
    // idempotent); then every cache drops what the refill did not write and keeps what it did
    @Test
    void a_refill_rides_out_a_failover() throws IOException {
        JdbcReplication topology = replication();
        String table = "failover_" + TABLES.incrementAndGet();
        DistributedCache<String, String> writer = cache(topology, table);
        DistributedCache<String, String> holder = cache(topology, table);
        put(writer, "stale", "old");
        put(writer, "fill", "old");
        assertThat(converges(holder, "stale", "old")).isTrue();
        pause(Duration.ofMillis(50));
        DistributedPolicy.StoreMark mark = writer.distributedPolicy().markStore();
        put(writer, "fill", "new");
        topology.awaitInSync();

        topology.failover();
        invalidateAllBeforeMark(writer, mark);

        for (DistributedCache<String, String> cache : List.of(writer, holder)) {
            assertThat(converges(cache, "stale", null)).as("the key the refill did not write dropped").isTrue();
            assertThat(converges(cache, "fill", "new")).as("the refill's write kept").isTrue();
        }
    }

    // the backdated control row confirmed by the primary and lost with it: the store hides nothing again, and no cache
    // may keep hiding what the store has (the epoch the message moved, the gap after the failover); a refill after
    // the failover takes effect on every cache, also though the new primary may give its control row a version a
    // cache saw already (a cache applies each version of the control row once)
    @Test
    void a_backdated_invalidation_the_failover_lost_hides_nothing_and_a_later_one_takes_effect() throws IOException {
        JdbcReplication topology = replication();
        String table = "failover_" + TABLES.incrementAndGet();
        DistributedCache<String, String> writer = cache(topology, table);
        DistributedCache<String, String> holder = cache(topology, table);
        put(writer, "stale", "old");
        assertThat(converges(holder, "stale", "old")).isTrue();
        topology.awaitInSync();
        topology.holdBackReplication();
        pause(Duration.ofSeconds(6)); // the row older than the polls' overlap, as in the test of a lost write
        DistributedPolicy.StoreMark lost = writer.distributedPolicy().markStore();

        writer.distributedPolicy().invalidateAllBeforeMark(lost);
        assertThat(converges(holder, "stale", null)).as("the holder polled the lost invalidation").isTrue();
        topology.failover();

        for (DistributedCache<String, String> cache : List.of(writer, holder)) {
            assertThat(converges(cache, "stale", "old")).as(() -> "the store has the value again; the cache serves "
                    + quietly(cache, "stale")).isTrue();
        }
        pause(Duration.ofMillis(50));
        DistributedPolicy.StoreMark later = writer.distributedPolicy().markStore();
        invalidateAllBeforeMark(writer, later);
        for (DistributedCache<String, String> cache : List.of(writer, holder)) {
            assertThat(converges(cache, "stale", null)).as(() -> "the later invalidation reached the cache; it serves "
                    + quietly(cache, "stale")).isTrue();
        }
    }

    private static void invalidateAllBeforeMark(DistributedCache<String, String> cache,
                                                DistributedPolicy.StoreMark mark) {
        assertThat(awaitUpTo(() -> {
            try {
                cache.distributedPolicy().invalidateAllBeforeMark(mark);
                return true;
            } catch (StoreException e) {
                return false;
            }
        }, CONVERGENCE, Duration.ofMillis(200))).as("invalidated within " + CONVERGENCE).isTrue();
    }

    private static boolean converges(DistributedCache<String, String> cache, String key, @Nullable String value) {
        return awaitUpTo(() -> Objects.equals(value, quietly(cache, key)), CONVERGENCE, Duration.ofMillis(200));
    }
}
