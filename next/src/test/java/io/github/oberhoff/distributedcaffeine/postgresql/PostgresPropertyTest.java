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

import io.github.oberhoff.distributedcaffeine.InternalPropertyTest;
import io.github.oberhoff.distributedcaffeine.PropertyBackend;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.testing.SilencingProxy;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The property test (freshness and convergence, see {@link InternalPropertyTest}) on PostgreSQL: real latency, real
 * notifications through the shared R2DBC session, and the faults PostgreSQL has: a terminated listening session (a
 * gap, as every reconnect is one), a statement rejected by the server (retried), and a connection broken during a
 * statement (the outcome unknown).
 */
@Timeout(600)
abstract class PostgresPropertyTest {

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4})
    void the_store_stays_fresh_and_every_instance_converges_on_it(long seed) throws Exception {
        assertThat(new InternalPropertyTest().run(seed, new PostgresBackend(), 25)).as("phases checked")
                .isEqualTo(25);
    }

    private static final class PostgresBackend implements PropertyBackend {

        private final PostgresTestDatabase database = PostgresTestDatabase.get();
        private final String table = PostgresTestDatabase.freshTable();
        // the listening session, through a proxy that can slow every round trip down; named as the shared one, so
        // that terminating listeners reaches it
        private final SilencingProxy proxy = proxy(database);
        private final ConnectionFactory listener = new PostgresqlConnectionFactory(
                PostgresqlConnectionConfiguration.builder()
                        .host("127.0.0.1")
                        .port(proxy.port())
                        .username(database.username())
                        .password(database.password())
                        .database(database.databaseName())
                        .applicationName(PostgresTestDatabase.LISTENER_APPLICATION)
                        .build());

        private static SilencingProxy proxy(PostgresTestDatabase database) {
            try {
                return new SilencingProxy(database.host(), database.port());
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
        private final AtomicInteger failing = new AtomicInteger();
        private volatile String sqlState = "40P01";

        @Override
        public Store open(StoreSettings settings) {
            return PostgresAdapter.newBuilder(faulty(), "property", table).withListener(listener).build()
                    .createStore(settings);
        }

        @Override
        public void fault(Random random, Consumer<String> trace) {
            double choice = random.nextDouble();
            pause(random.nextInt(5));
            if (choice < 0.15) {
                trace.accept("FAULT listening session terminated");
                database.terminateListeners();
            } else if (choice < 0.30) {
                trace.accept("FAULT next statement rejected (deadlock)");
                sqlState = "40P01";
                failing.set(1);
            } else if (choice < 0.45) {
                trace.accept("FAULT connection broken during the next statement");
                sqlState = "08006";
                failing.set(1);
            } else if (choice < 0.6) {
                long delay = 20L + random.nextInt(200);
                trace.accept("FAULT every notification round trip " + delay + " ms slower");
                proxy.delayRequests(Duration.ofMillis(delay));
            }
        }

        @Override
        public void heal() {
            failing.set(0);
            proxy.delayRequests(Duration.ZERO);
        }

        @Override
        public boolean isIdle() {
            return PostgresListener.barrier(listener, database.dataSource);
        }

        @Override
        public void close() {
            try {
                proxy.close();
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }

        // the database's data source; the next statement on rows fails as injected
        private DataSource faulty() {
            DataSource real = database.dataSource;
            return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                    (proxy, method, args) -> {
                        Object result = invoke(real, method, args);
                        return method.getName().equals("getConnection") ? connection((Connection) result) : result;
                    });
        }

        private Connection connection(Connection real) {
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                    (proxy, method, args) -> {
                        Object result = invoke(real, method, args);
                        if (method.getName().equals("prepareStatement") && ((String) args[0]).contains("discriminator")) {
                            PreparedStatement statement = (PreparedStatement) result;
                            return Proxy.newProxyInstance(getClass().getClassLoader(),
                                    new Class<?>[]{PreparedStatement.class}, (p, m, a) -> {
                                        if (m.getName().startsWith("execute")
                                                && failing.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                                            if (sqlState.equals("08006")) {
                                                // broken after the statement ran: it committed, the caller cannot know
                                                invoke(statement, m, a);
                                            }
                                            throw new SQLException("injected", sqlState);
                                        }
                                        return invoke(statement, m, a);
                                    });
                        }
                        return result;
                    });
        }

        private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }

    }
}
