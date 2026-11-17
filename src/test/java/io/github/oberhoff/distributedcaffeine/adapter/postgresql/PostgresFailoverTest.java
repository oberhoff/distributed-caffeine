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
package io.github.oberhoff.distributedcaffeine.adapter.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.DistributedPolicy;
import io.github.oberhoff.distributedcaffeine.DistributionMode;
import io.github.oberhoff.distributedcaffeine.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The PostgreSQL adapter through a failover: a primary streaming to a standby, both reached through multi-host
 * connection details that look for the primary ({@code targetServerType=primary}), listened through as well, the
 * primary stopped and the standby promoted. Writing resumes on the new primary, the listening session reconnects to
 * it, and what was written before is still there. On a primary with a standby, built anew after each test, which breaks
 * it.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(300)
@SuppressWarnings("SqlNoDataSourceInspection") // the SQL runs against the test containers
abstract class PostgresFailoverTest {

    private static final Duration LONG = Duration.ofHours(1);
    private static final Duration BACK = Duration.ofMinutes(1);
    private final PostgresTestDatabase database = PostgresTestDatabase.get();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (int index = closeables.size() - 1; index >= 0; index--) {
            closeables.get(index).close();
        }
        // a test ends with the primary gone: the topology is built anew for what runs next on it
        database.ensureIntact();
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }


    // both servers, the primary wanted: whichever is primary now is used
    private HikariDataSource pool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:" + database.port() + ",localhost:" + database.standbyPort()
                + "/" + PostgresTestDatabase.DATABASE + "?targetServerType=primary");
        config.setUsername(PostgresTestDatabase.USER);
        config.setPassword(PostgresTestDatabase.PASSWORD);
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(2_000);
        config.setInitializationFailTimeout(-1);
        return track(new HikariDataSource(config));
    }

    private DistributedCache<String, String> cache(HikariDataSource pool, DistributionMode mode) {
        return track(DistributedCaffeine.<String, String>newBuilder(PostgresAdapter.newBuilder(pool, "failover",
                                "cache").build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000)
                        .withOperationTimeout(Duration.ofSeconds(5)))
                .withDistributionMode(mode)
                .build());
    }

    // a statement on the standby directly; the first column of its first row, if any
    private @Nullable String onStandby(String sql) throws SQLException {
        return database.onStandby(sql);
    }

    @Test
    void writing_and_listening_move_to_the_promoted_standby_and_nothing_confirmed_is_lost() throws Exception {
        HikariDataSource pool = pool();
        DistributedCache<String, String> writer = cache(pool, DistributionMode.NONE);
        DistributedCache<String, String> holder = cache(pool, DistributionMode.INVALIDATE);
        writer.put("k", "before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");
        assertThat(awaitUpTo(() -> {
            try {
                return "before".equals(onStandby("SELECT value FROM failover.cache WHERE key = 'k'"));
            } catch (SQLException e) {
                return false;
            }
        }, BACK, Duration.ofMillis(100))).as("replicated").isTrue();

        // stopped cleanly, the primary hands the standby every change it has; then the standby is promoted
        database.stopPrimary();
        database.promoteStandby();

        assertThat(awaitUpTo(() -> {
            try {
                writer.put("k", "after");
                return true;
            } catch (StoreException e) {
                return false;
            }
        }, BACK, Duration.ofMillis(250))).as("writing on the promoted standby").isTrue();
        assertThat(awaitUpTo(() -> holder.distributedPolicy().synchronizationState()
                == SynchronizationState.SYNCHRONIZED, BACK, Duration.ofMillis(250))).as("listening on it").isTrue();
        await(() -> "after".equals(holder.getIfPresent("k")));
        writer.put("k", "after the failover");
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("after the failover");
        assertThat(onStandby("SELECT pg_is_in_recovery()")).as("the standby is the primary now").isEqualTo("f");
    }

    @Test
    void a_confirmed_write_the_failover_loses_is_held_by_no_cache() throws Exception {
        HikariDataSource pool = pool();
        DistributedCache<String, String> writer = cache(pool, DistributionMode.INVALIDATE);
        DistributedCache<String, String> holder = cache(pool, DistributionMode.INVALIDATE);
        writer.put("k", "before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");
        assertThat(awaitUpTo(() -> {
            try {
                return "before".equals(onStandby("SELECT value FROM failover.cache WHERE key = 'k'"));
            } catch (SQLException e) {
                return false;
            }
        }, BACK, Duration.ofMillis(100))).as("replicated").isTrue();

        // replication is asynchronous: with the standby cut off from the primary, a write is confirmed by the primary
        // alone (the standby pointed at a host that does not exist, which PostgreSQL 13 and later reload). A
        // synchronous standby would hold the write back until it has it: a confirmed write cannot be lost there
        assumeTrue(database.topology == PostgresTopology.PRIMARY_WITH_ASYNCHRONOUS_STANDBY,
                "a confirmed write is lost only with an asynchronous standby");
        assumeTrue(database.major() >= 13, "primary_conninfo is reloaded from PostgreSQL 13 on");
        database.markBroken();
        onStandby("ALTER SYSTEM SET primary_conninfo = 'host=nowhere connect_timeout=1'");
        assertThat(onStandby("SELECT pg_reload_conf()")).isEqualTo("t");
        assertThat(awaitUpTo(() -> {
            try {
                return "0".equals(onStandby("SELECT count(*) FROM pg_stat_wal_receiver WHERE status = 'streaming'"));
            } catch (SQLException e) {
                return false;
            }
        }, BACK, Duration.ofMillis(100))).as("the standby stopped receiving").isTrue();
        writer.put("k", "lost");
        await(() -> "lost".equals(holder.getIfPresent("k")));
        // the primary dies, and the standby is promoted without the write
        database.killPrimary();
        database.promoteStandby();
        assertThat(onStandby("SELECT value FROM failover.cache WHERE key = 'k'")).as("the write is lost")
                .isEqualTo("before");

        // both listening sessions reconnect to the new primary, a gap: the held values are checked against it
        for (DistributedCache<String, String> cache : List.of(writer, holder)) {
            assertThat(awaitUpTo(() -> "before".equals(cache.getIfPresent("k")), BACK, Duration.ofMillis(250)))
                    .as("the lost value dropped").isTrue();
        }
        writer.put("k", "after the failover");
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("after the failover");
    }

    // a refill across a failover (VISION.md, "Refill while serving"): the mark taken and the fill replicated before
    // it, invalidateAllBeforeMark retried until the promoted standby takes it, as an application retries a failed call
    // (it is idempotent); then both caches drop what the refill did not write and keep what it did
    @Test
    void a_refill_rides_out_a_failover() {
        HikariDataSource pool = pool();
        DistributedCache<String, String> writer = cache(pool, DistributionMode.INVALIDATE);
        DistributedCache<String, String> holder = cache(pool, DistributionMode.INVALIDATE);
        writer.putAll(Map.of("stale", "old", "fill", "old"));
        assertThat(holder.getIfPresent("stale")).isEqualTo("old");
        pause(50);
        DistributedPolicy.Mark mark = writer.distributedPolicy().store().mark();
        writer.put("fill", "new");
        assertThat(awaitUpTo(() -> {
            try {
                return "new".equals(onStandby("SELECT value FROM failover.cache WHERE key = 'fill'"));
            } catch (SQLException e) {
                return false;
            }
        }, BACK, Duration.ofMillis(100))).as("replicated").isTrue();

        database.stopPrimary();
        database.promoteStandby();
        assertThat(awaitUpTo(() -> {
            try {
                writer.distributedPolicy().store().invalidateAllBeforeMark(mark);
                return true;
            } catch (StoreException e) {
                return false;
            }
        }, BACK, Duration.ofMillis(250))).as("invalidated on the promoted standby").isTrue();

        for (DistributedCache<String, String> cache : List.of(writer, holder)) {
            assertThat(awaitUpTo(() -> cache.getIfPresent("stale") == null, BACK, Duration.ofMillis(250)))
                    .as("the key the refill did not write dropped").isTrue();
            assertThat(awaitUpTo(() -> "new".equals(cache.getIfPresent("fill")), BACK, Duration.ofMillis(250)))
                    .as("the refill's write kept").isTrue();
        }
    }

    // the backdated control row confirmed by the primary alone and lost with it: the store hides nothing again, and no
    // cache may keep hiding what the store has; an invalidation after the failover takes effect on every cache, also
    // where the promoted standby's sequence gives its control row a version a cache saw already (the lost write's
    // nextval may be lost with it)
    @Test
    void a_backdated_invalidation_the_failover_lost_hides_nothing_and_a_later_one_takes_effect() throws Exception {
        HikariDataSource pool = pool();
        DistributedCache<String, String> writer = cache(pool, DistributionMode.INVALIDATE);
        DistributedCache<String, String> holder = cache(pool, DistributionMode.INVALIDATE);
        writer.put("stale", "old");
        assertThat(holder.getIfPresent("stale")).isEqualTo("old");
        assertThat(awaitUpTo(() -> {
            try {
                return "old".equals(onStandby("SELECT value FROM failover.cache WHERE key = 'stale'"));
            } catch (SQLException e) {
                return false;
            }
        }, BACK, Duration.ofMillis(100))).as("replicated").isTrue();
        assumeTrue(database.topology == PostgresTopology.PRIMARY_WITH_ASYNCHRONOUS_STANDBY,
                "a confirmed write is lost only with an asynchronous standby");
        assumeTrue(database.major() >= 13, "primary_conninfo is reloaded from PostgreSQL 13 on");
        database.markBroken();
        onStandby("ALTER SYSTEM SET primary_conninfo = 'host=nowhere connect_timeout=1'");
        assertThat(onStandby("SELECT pg_reload_conf()")).isEqualTo("t");
        assertThat(awaitUpTo(() -> {
            try {
                return "0".equals(onStandby("SELECT count(*) FROM pg_stat_wal_receiver WHERE status = 'streaming'"));
            } catch (SQLException e) {
                return false;
            }
        }, BACK, Duration.ofMillis(100))).as("the standby stopped receiving").isTrue();
        pause(50);

        writer.distributedPolicy().store().invalidateAllBeforeMark(writer.distributedPolicy().store().mark());
        await(() -> holder.getIfPresent("stale") == null);
        database.killPrimary();
        database.promoteStandby();

        for (DistributedCache<String, String> cache : List.of(writer, holder)) {
            assertThat(awaitUpTo(() -> "old".equals(cache.getIfPresent("stale")), BACK, Duration.ofMillis(250)))
                    .as("the store has the value again").isTrue();
        }
        pause(50);
        assertThat(awaitUpTo(() -> {
            try {
                writer.distributedPolicy().store().invalidateAllBeforeMark(writer.distributedPolicy().store().mark());
                return true;
            } catch (StoreException e) {
                return false;
            }
        }, BACK, Duration.ofMillis(250))).as("invalidated on the promoted standby").isTrue();
        for (DistributedCache<String, String> cache : List.of(writer, holder)) {
            assertThat(awaitUpTo(() -> cache.getIfPresent("stale") == null, BACK, Duration.ofMillis(250)))
                    .as(() -> "the later invalidation reached the cache; it serves " + cache.getIfPresent("stale"))
                    .isTrue();
        }
    }
}
