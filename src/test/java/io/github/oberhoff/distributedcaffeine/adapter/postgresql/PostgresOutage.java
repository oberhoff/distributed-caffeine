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

import io.github.oberhoff.distributedcaffeine.AdapterOutageTest.Interruption;
import io.github.oberhoff.distributedcaffeine.AdapterOutageTest.Outage;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.testing.PausedContainers;
import org.jspecify.annotations.NullMarked;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.SQLException;
import java.time.Duration;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;

/**
 * PostgreSQL's side of {@code AdapterOutageTest}: every container of the topology paused (the standby and the pooler
 * with the primary), and the primary stopped and started again on its data, as {@code PostgresRestartTest} has it
 * happen under running caches; behind the pooler, the pooler stays up while the server behind it is out. With a
 * standby, the primary also stopped for good while the standby is not yet promoted (as {@code PostgresFailoverTest}
 * has it happen under running caches): the adapters list both servers ({@code targetServerType=any}, as a client set
 * up for a failover may), so they reach the standby, refused as retryable, until it is promoted; the topology is then
 * built anew for the suites after. Each adapter reaches the servers through data sources of its own, waiting at most a
 * second to connect and to log in.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class PostgresOutage implements Outage {

    private static final String SCHEMA = "outage";
    private static final Duration BACK = Duration.ofMinutes(1);

    private final PostgresTestDatabase database = PostgresTestDatabase.get();
    private final String table = PostgresTestDatabase.freshTable();
    // whether withoutPrimary promoted the standby, which leaves the topology to be built anew
    private boolean promoted;

    @Override
    public Adapter<String, String> adapter() {
        if (database.topology == PostgresTopology.SINGLE_SERVER_BEHIND_TRANSACTION_POOLER) {
            // the data source through the pooler in transaction mode, the listener directly (a session of its own)
            return PostgresAdapter.newBuilder(dataSource(database.poolerHost(), database.poolerPort(),
                            "&prepareThreshold=0"), SCHEMA, table)
                    .withListenerDataSource(dataSource(PostgresTestDatabase.HOST, database.port(), "")).build();
        }
        if (database.topology.hasStandby()) {
            // both servers, the primary first: the standby only while the primary is out
            return PostgresAdapter.newBuilder(dataSource(PostgresTestDatabase.HOST + ":" + database.port() + ","
                    + PostgresTestDatabase.HOST, database.standbyPort(), "&targetServerType=any"), SCHEMA, table)
                    .build();
        }
        return PostgresAdapter.newBuilder(dataSource(PostgresTestDatabase.HOST, database.port(), ""), SCHEMA, table)
                .build();
    }

    // a connection of its own per use, waiting at most a second to connect and to log in (PostgresConformance)
    private static PGSimpleDataSource dataSource(String host, int port, String options) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl("jdbc:postgresql://" + host + ":" + port + "/" + PostgresTestDatabase.DATABASE
                + "?connectTimeout=1&loginTimeout=1" + options);
        dataSource.setUser(PostgresTestDatabase.USER);
        dataSource.setPassword(PostgresTestDatabase.PASSWORD);
        return dataSource;
    }

    @Override
    public PausedContainers pause() {
        return new PausedContainers(database.allContainers());
    }

    @Override
    public boolean stops() {
        return true;
    }

    @Override
    public Interruption stop() {
        database.stopPrimary();
        return () -> {
            database.startPrimary();
            if (!awaitUpTo(() -> {
                try {
                    database.execute("SELECT 1");
                    return true;
                } catch (IllegalStateException e) {
                    return false;
                }
            }, BACK, Duration.ofMillis(250))) {
                throw new AssertionError("the primary not back within " + BACK.toSeconds() + " s");
            }
        };
    }

    @Override
    public boolean losesPrimary() {
        return database.topology.hasStandby();
    }

    @Override
    public Interruption withoutPrimary() {
        // the rows written before the outage on the standby first: an asynchronous standby may lag, and promoted, it
        // lacks what it had not received (found on PostgreSQL 13.0, the value stored before the outage gone after it)
        // a table not created yet has nothing to wait for, as where a test writes nothing before the outage
        String rows = "SELECT coalesce(md5(string_agg(t::text, ',' ORDER BY t::text)), '') FROM " + SCHEMA + "."
                + table + " t";
        if (!awaitUpTo(() -> {
            String primary;
            try {
                primary = database.onPrimary(rows);
            } catch (SQLException e) {
                return "42P01".equals(e.getSQLState()); // undefined_table
            }
            try {
                return primary != null && primary.equals(database.onStandby(rows));
            } catch (SQLException e) {
                return false;
            }
        }, Duration.ofSeconds(30), Duration.ofMillis(100))) {
            throw new IllegalStateException("the standby did not catch up with the primary within 30 s");
        }
        database.stopPrimary();
        promoted = true;
        return database::promoteStandby;
    }

    @Override
    public void close() {
        // the data sources hold no connections between uses; a promoted standby leaves the topology to be built anew
        if (promoted) {
            database.ensureIntact();
        }
    }
}
