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
import io.github.oberhoff.distributedcaffeine.AdapterOutageTest.Interruption;
import io.github.oberhoff.distributedcaffeine.AdapterOutageTest.Outage;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.testing.PausedContainers;
import org.jspecify.annotations.NullMarked;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;

/**
 * MariaDB's and MySQL's side of {@code AdapterOutageTest}: the server paused, stopped and started again on its data
 * (as {@code JdbcRestartTest} has it happen under running caches), and read-only, as a primary demoted during a
 * failover is (as {@code JdbcFailoverTest} has it). Each adapter reaches the server through a pool of its own,
 * connecting on first use and waiting at most a second for a connection.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class JdbcOutage implements Outage {

    private static final Duration BACK = Duration.ofMinutes(1);

    private final String table = JdbcTestDatabase.freshTable();
    private final List<HikariDataSource> pools = new ArrayList<>();

    @Override
    public Adapter<String, String> adapter() {
        HikariDataSource pool = JdbcTestDatabase.pool(JdbcTestDatabase.host(), JdbcTestDatabase.port(), null);
        pools.add(pool);
        return JdbcTestDatabase.adapter(pool, table, JdbcTestDatabase.DEFAULT_DISCRIMINATOR);
    }

    @Override
    public PausedContainers pause() {
        return new PausedContainers(List.of(JdbcTestDatabase.container()));
    }

    @Override
    public boolean stops() {
        return true;
    }

    @Override
    public Interruption stop() {
        JdbcTestDatabase.stopServer();
        return () -> {
            JdbcTestDatabase.startServer();
            if (!awaitUpTo(() -> {
                try (Connection ignored = JdbcTestDatabase.root()) {
                    return true;
                } catch (SQLException e) {
                    return false;
                }
            }, BACK, Duration.ofMillis(250))) {
                throw new AssertionError("the server not back within " + BACK.toSeconds() + " s");
            }
        };
    }

    @Override
    public boolean losesPrimary() {
        return true;
    }

    @Override
    public Interruption withoutPrimary() {
        asRoot("SET GLOBAL read_only = ON");
        return () -> asRoot("SET GLOBAL read_only = OFF");
    }

    private static void asRoot(String sql) {
        try {
            JdbcTestDatabase.asRoot(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        pools.forEach(HikariDataSource::close);
    }
}
