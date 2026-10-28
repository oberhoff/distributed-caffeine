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
package io.github.oberhoff.distributedcaffeine.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.DistributionMode;
import io.github.oberhoff.distributedcaffeine.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The PostgreSQL adapter while the server stops and starts again on its data, as on a restart for an update: writes
 * fail within the timeouts rather than waiting, hits are still served, and once the server is back writing resumes,
 * the listening session reconnects and what changed meanwhile is found. On the single server, whose fixed host port
 * it keeps across the restart.
 */
@Timeout(300)
// the SQL runs against the test container
@SuppressWarnings("SqlNoDataSourceInspection")
abstract class PostgresRestartTest {

    private static final Duration LONG = Duration.ofHours(1);
    private static final Duration BACK = Duration.ofMinutes(1);
    private final PostgresTestDatabase database = PostgresTestDatabase.get();
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


    private HikariDataSource pool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername(database.username());
        config.setPassword(database.password());
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(2_000);
        config.setInitializationFailTimeout(-1);
        return track(new HikariDataSource(config));
    }

    private DistributedCache<String, String> cache(HikariDataSource pool, DistributionMode mode) {
        return cache(pool, mode, 1_000, LONG);
    }

    private DistributedCache<String, String> cache(HikariDataSource pool, DistributionMode mode, long maximumSize,
                                                   Duration maintenance) {
        return track(DistributedCaffeine.<String, String>newBuilder(PostgresAdapter.newBuilder(pool, "restart",
                                "cache").build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(maximumSize)
                        .withMaintenanceInterval(maintenance).withTimeout(Duration.ofSeconds(5)))
                .withDistributionMode(mode)
                .build());
    }

    private static long values(HikariDataSource pool) {
        try (Connection connection = pool.getConnection();
             java.sql.Statement statement = connection.createStatement();
             java.sql.ResultSet resultSet = statement.executeQuery(
                     "SELECT count(*) FROM restart.cache WHERE state = 'VALUE'")) {
            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            return -1;
        }
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
        HikariDataSource pool = pool();
        DistributedCache<String, String> writer = cache(pool, DistributionMode.NONE);
        DistributedCache<String, String> holder = cache(pool, DistributionMode.INVALIDATE);
        // maintenance every 200 ms, pruning the table to 10 values: it fails while the server is down
        cache(pool, DistributionMode.NONE, 10, Duration.ofMillis(200));
        writer.put("k", "before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");

        database.stopPrimary();
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
            assertThat(holder.getIfPresent("k")).as("a hit is served while the server is down").isEqualTo("before");
        } finally {
            database.startPrimary();
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
        assertThat(awaitUpTo(() -> holder.distributedPolicy().getSynchronizationState()
                == SynchronizationState.SYNCHRONIZED, BACK, Duration.ofMillis(250))).as("listening again").isTrue();
        await(() -> "after".equals(holder.getIfPresent("k")));
        writer.put("k", "after the reconnect");
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("after the reconnect");
        assertThat(writer.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);

        // and maintenance goes on: what is written now is pruned to the maximum again
        for (int index = 0; index < 30; index++) {
            writer.put("p" + index, "v");
        }
        assertThat(awaitUpTo(() -> values(pool) == 10, BACK, Duration.ofMillis(100))).as("pruned again").isTrue();
    }
}
