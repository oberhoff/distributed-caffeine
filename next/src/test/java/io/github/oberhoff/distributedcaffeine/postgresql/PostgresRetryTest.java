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

import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.Format;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How the PostgreSQL store classifies failures for the core's retries (VISION.md, "Write retries"), with injected SQL
 * failures: a rejected statement or a connection that cannot be obtained is retryable and certainly not applied; a
 * broken connection is retryable with a write's outcome unknown; other errors are not retryable; a batch failing
 * after a part of it was written has its outcome unknown. And the retries through a cache: the store tries once, the
 * core again.
 */
abstract class PostgresRetryTest {

    private static final Duration LONG = Duration.ofHours(1);
    private static final Entry ENTRY = new Entry("h", "h".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8));

    private final PostgresTestDatabase database = PostgresTestDatabase.get();
    private final AtomicInteger passingStatements = new AtomicInteger();
    private final AtomicInteger failingStatements = new AtomicInteger();
    private final AtomicInteger failingConnections = new AtomicInteger();
    private final AtomicInteger executions = new AtomicInteger();
    private volatile String sqlState = "40P01";

    // the database's data source, with the next statements writing or reading rows (not the standby check) failing
    private DataSource faulty() {
        DataSource real = database.dataSource;
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getConnection")) {
                        if (failingConnections.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                            throw new SQLException("injected: cannot connect", "08001");
                        }
                        return connection((Connection) invoke(real, method, args));
                    }
                    return invoke(real, method, args);
                });
    }

    private Connection connection(Connection real) {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    Object result = invoke(real, method, args);
                    if (method.getName().equals("prepareStatement") && ((String) args[0]).contains("discriminator")) {
                        return statement((PreparedStatement) result);
                    }
                    return result;
                });
    }

    private PreparedStatement statement(PreparedStatement real) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getName().startsWith("execute")) {
                executions.incrementAndGet();
                if (passingStatements.getAndUpdate(n -> Math.max(0, n - 1)) == 0
                        && failingStatements.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                    throw new SQLException("injected", sqlState);
                }
            }
            return invoke(real, method, args);
        };
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, handler);
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    // counting starts after the schema check
    private Store store() {
        Store store = PostgresAdapter.newBuilder(faulty(), "retry", PostgresTestDatabase.freshTable()).build()
                .createStore(StoreSettings.of(Format.TEXT, Format.TEXT));
        executions.set(0);
        return store;
    }

    private DistributedCache<String, String> cache() {
        DistributedCache<String, String> cache = DistributedCaffeine.<String, String>newBuilder(
                        PostgresAdapter.newBuilder(faulty(), "retry", PostgresTestDatabase.freshTable())
                                .withListener(database.listener).build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10_000))
                .build();
        executions.set(0);
        return cache;
    }

    private static void assertFailure(Runnable call, boolean outcomeUnknown, boolean retryable) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(StoreException.class, e -> {
            assertThat(e.isOutcomeUnknown()).as("outcome unknown").isEqualTo(outcomeUnknown);
            assertThat(e.isRetryable()).as("retryable").isEqualTo(retryable);
        });
    }

    // the store: one attempt, classified

    @Test
    void a_rejected_statement_is_retryable_and_certainly_not_applied() {
        try (Store store = store()) {
            failingStatements.set(1);
            sqlState = "40P01";

            assertFailure(() -> store.put(List.of(ENTRY)), false, true);
            assertThat(executions).as("tried once").hasValue(1);
        }
    }

    @Test
    void a_connection_that_cannot_be_obtained_is_retryable_and_certainly_not_applied() {
        try (Store store = store()) {
            failingConnections.set(1);

            assertFailure(() -> store.put(List.of(ENTRY)), false, true);
        }
    }

    @Test
    void a_broken_connection_is_retryable_with_a_writes_outcome_unknown() {
        try (Store store = store()) {
            sqlState = "08006";
            failingStatements.set(1);
            assertFailure(() -> store.put(List.of(ENTRY)), true, true);
            failingStatements.set(1);
            assertFailure(() -> store.read(List.of("h"), LONG), false, true);
        }
    }

    @Test
    void other_errors_are_not_retryable() {
        try (Store store = store()) {
            failingStatements.set(1);
            sqlState = "42601";

            assertFailure(() -> store.put(List.of(ENTRY)), false, false);
        }
    }

    @Test
    void a_batch_failing_after_a_part_of_it_was_written_has_its_outcome_unknown() {
        try (Store store = store()) {
            List<Entry> entries = new ArrayList<>();
            for (int index = 0; index < 1_500; index++) {
                entries.add(new Entry("h" + index, ("k" + index).getBytes(StandardCharsets.UTF_8),
                        "v".getBytes(StandardCharsets.UTF_8)));
            }
            passingStatements.set(1); // the first chunk of 1,000 is written, the second rejected
            failingStatements.set(1);
            sqlState = "40P01";

            assertThatThrownBy(() -> store.put(entries)).isInstanceOfSatisfying(StoreException.class,
                    e -> assertThat(e.isOutcomeUnknown()).isTrue()).hasMessageContaining("part of the write");
        }
    }

    // through a cache: the core retries

    @Test
    void a_rejected_put_is_written_by_the_cores_next_attempt() {
        try (DistributedCache<String, String> cache = cache()) {
            failingStatements.set(2);
            sqlState = "40001";

            cache.put("k", "v");

            assertThat(executions).as("two rejected, the third lands").hasValue(3);
            assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).isEqualTo("v");
        }
    }

    @Test
    void a_put_on_a_broken_connection_is_not_repeated_but_a_conditional_write_is() {
        try (DistributedCache<String, String> cache = cache()) {
            sqlState = "08006";
            failingStatements.set(1);
            assertThatThrownBy(() -> cache.put("k", "v")).isInstanceOfSatisfying(StoreException.class,
                    e -> assertThat(e.isOutcomeUnknown()).isTrue());
            assertThat(executions).hasValue(1);

            executions.set(0);
            failingStatements.set(1);
            assertThat(cache.asMap().putIfAbsent("other", "v")).isNull();
            assertThat(executions.get()).as("broken, then repeated").isGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void exhausted_retries_fail_after_three_attempts() {
        try (DistributedCache<String, String> cache = cache()) {
            failingStatements.set(5);
            sqlState = "40001";

            assertThatThrownBy(() -> cache.put("k", "v")).isInstanceOfSatisfying(StoreException.class,
                    e -> assertThat(e.isOutcomeUnknown()).isFalse()).hasMessageContaining("after 3 attempts");
            assertThat(executions).hasValue(3);
        }
    }
}
