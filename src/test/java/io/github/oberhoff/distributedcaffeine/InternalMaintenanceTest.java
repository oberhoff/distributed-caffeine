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
package io.github.oberhoff.distributedcaffeine;

import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.LogCapture;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Maintenance in paced batches (VISION.md, "Maintenance in paced batches"): a backlog larger than one batch is worked
 * off by repeated calls of at most {@link InternalMaintenance#EXPIRY_BATCH} rows each (expiry) or
 * {@link InternalMaintenance#PRUNE_BATCH} (pruning), in one run.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(60)
@SuppressWarnings("resource") // every cache a test opens is closed after it (@AfterEach), and with it its store
class InternalMaintenanceTest {

    private static final Duration LONG = Duration.ofHours(1);

    // a maintenance call as the store saw it: its operation, its limit and what it changed
    private record Call(String operation, int limit, int changed) {
    }

    private final AtomicLong clockMicros = new AtomicLong(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
    private final InMemoryDatabase database = new InMemoryDatabase(clockMicros::get, 1);
    private final List<Call> calls = new CopyOnWriteArrayList<>();
    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    // whether expire throws what no store should (a bug's exception)
    private volatile boolean failExpiry;

    // the store, with its maintenance calls recorded
    private Store recording() {
        Store store = database.openStore("cache");
        return (Store) Proxy.newProxyInstance(Store.class.getClassLoader(), new Class<?>[]{Store.class},
                (proxy, method, arguments) -> {
                    if (failExpiry && method.getName().equals("expire")) {
                        throw new IllegalStateException("expiry broke");
                    }
                    Object result;
                    try {
                        result = method.invoke(store, arguments);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (method.getName().equals("expire") || method.getName().startsWith("prune")) {
                        calls.add(new Call(method.getName(), (int) arguments[arguments.length - 1], (int) result));
                    }
                    return result;
                });
    }

    private List<Call> calls(String operation) {
        return calls.stream().filter(call -> call.operation().equals(operation)).toList();
    }

    // a cache of the given rows, maintained every 100 ms, its maximum size far above them
    private DistributedCache<String, String> cache(int rows) {
        DistributedCache<String, String> cache = DistributedCaffeine.<String, String>newBuilder(settings -> recording())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withDistributionMode(DistributionMode.NONE)
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10L * rows)
                        .withMaintenanceInterval(Duration.ofMillis(100)))
                .build();
        closeables.add(cache);
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < rows; i++) {
            values.put("k" + i, "v");
        }
        cache.putAll(values);
        return cache;
    }

    @Test
    void an_unexpected_failure_of_maintenance_is_logged_at_once_with_its_stack_trace_and_once_only() {
        try (LogCapture log = new LogCapture()) {
            failExpiry = true;
            cache(10);
            await(() -> log.count("Maintenance of the store (expiry, pruning) failed") == 1);
            pause(500); // several more runs

            assertThat(log.count("Maintenance of the store (expiry, pruning) failed")).isEqualTo(1);
            assertThat(log.text()).contains("java.lang.IllegalStateException: expiry broke").contains("\tat ");

            failExpiry = false;
            await(() -> log.count("Maintenance of the store succeeded again") == 1);
        }
    }

    @Test
    void a_backlog_larger_than_a_batch_is_expired_in_batches_in_one_run() {
        int rows = 2 * InternalMaintenance.EXPIRY_BATCH + 5_000;
        cache(rows);

        clockMicros.addAndGet(TimeUnit.HOURS.toMicros(2));
        await(() -> calls("expire").stream().mapToInt(Call::changed).sum() >= rows);

        List<Call> expiring = calls("expire").stream().filter(call -> call.changed() > 0).toList();
        assertThat(expiring).as("one batch at a time").allMatch(call ->
                call.limit() == InternalMaintenance.EXPIRY_BATCH && call.changed() <= InternalMaintenance.EXPIRY_BATCH);
        assertThat(expiring).extracting(Call::changed).as("full batches until the rest")
                .containsExactly(InternalMaintenance.EXPIRY_BATCH, InternalMaintenance.EXPIRY_BATCH, 5_000);
    }

    @Test
    void the_rows_an_invalidateAll_hides_are_pruned_in_batches_in_one_run() {
        int rows = 2 * InternalMaintenance.PRUNE_BATCH + 50_000;
        DistributedCache<String, String> cache = cache(rows);

        cache.invalidateAll();
        await(() -> calls("pruneTombstones").stream().mapToInt(Call::changed).sum() >= rows);

        List<Call> pruning = calls("pruneTombstones").stream().filter(call -> call.changed() > 0).toList();
        assertThat(pruning).as("one batch at a time").allMatch(call ->
                call.limit() == InternalMaintenance.PRUNE_BATCH && call.changed() <= InternalMaintenance.PRUNE_BATCH);
        assertThat(pruning).extracting(Call::changed).as("full batches until the rest")
                .containsExactly(InternalMaintenance.PRUNE_BATCH, InternalMaintenance.PRUNE_BATCH, 50_000);
    }
}
