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

import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.DistributionMode;
import io.github.oberhoff.distributedcaffeine.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The MariaDB and MySQL adapters while the server stops and starts again on its data, as on a restart for an update
 * (VISION.md, "MariaDB and MySQL", step 4): writes fail within the timeouts rather than waiting, hits are still
 * served, everything written before is still there (InnoDB is durable), and once the server is back writing resumes
 * and the polls find what changed. The server keeps its host port across the restart.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(300)
abstract class JdbcRestartTest {

    private static final Duration LONG = Duration.ofHours(1);
    private static final Duration BACK = Duration.ofMinutes(2);
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

    private DistributedCache<String, String> cache(HikariDataSource pool, String table, DistributionMode mode) {
        return track(DistributedCaffeine.<String, String>newBuilder(JdbcTestDatabase.adapter(pool, table,
                        JdbcTestDatabase.DEFAULT_DISCRIMINATOR))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000)
                        .withPollInterval(Duration.ofMillis(200)).withOperationTimeout(Duration.ofSeconds(5)))
                .withDistributionMode(mode)
                .build());
    }

    private static boolean answers(HikariDataSource pool) {
        try (Connection connection = pool.getConnection()) {
            return connection.isValid(2);
        } catch (SQLException e) {
            return false;
        }
    }

    @Test
    void a_restart_of_the_server_is_ridden_out_once_it_is_back() {
        HikariDataSource pool = track(JdbcTestDatabase.pool(4, null));
        String table = JdbcTestDatabase.freshTable();
        DistributedCache<String, String> writer = cache(pool, table, DistributionMode.NONE);
        DistributedCache<String, String> holder = cache(pool, table, DistributionMode.INVALIDATE);
        writer.put("k", "before");
        writer.put("kept", "written before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");

        JdbcTestDatabase.stopServer();
        try {
            for (int attempt = 0; attempt < 3; attempt++) {
                long started = System.nanoTime();
                String value = "during " + attempt;
                assertThatThrownBy(() -> writer.put("k", value)).isInstanceOf(StoreException.class);
                assertThat(Duration.ofNanos(System.nanoTime() - started)).as("failing rather than waiting")
                        .isLessThan(Duration.ofSeconds(15));
            }
            // the writer by its own failures (its store guard), the holder, which neither reads nor writes through
            // the store meanwhile, by its failing polls
            assertThat(writer.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);
            await(() -> holder.distributedPolicy().getSynchronizationState() == SynchronizationState.DEGRADED);
            assertThat(holder.getIfPresent("k")).as("a hit is served while the server is down").isEqualTo("before");
        } finally {
            JdbcTestDatabase.startServer();
        }
        assertThat(awaitUpTo(() -> answers(pool), BACK, Duration.ofMillis(250))).as("the server back").isTrue();

        assertThat(awaitUpTo(() -> {
            try {
                writer.put("k", "after");
                return true;
            } catch (StoreException e) {
                return false;
            }
        }, BACK, Duration.ofMillis(250))).as("writing again").isTrue();
        assertThat(requireNonNull(writer.distributedPolicy().getFromStore("kept")).value())
                .as("written before the restart, kept").isEqualTo("written before");
        assertThat(awaitUpTo(() -> holder.distributedPolicy().getSynchronizationState()
                == SynchronizationState.SYNCHRONIZED, BACK, Duration.ofMillis(250))).as("polling again").isTrue();
        await(() -> "after".equals(holder.getIfPresent("k")));
        writer.put("k", "after the restart");
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("after the restart");
    }
}
