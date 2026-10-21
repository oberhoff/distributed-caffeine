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
package io.github.oberhoff.distributedcaffeine.spi;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * What the SPI contract tests share: a fixture opening several stores on one fresh cache (as several instances
 * would), every test on a fresh cache, and the stores a test opened closed after it.
 */
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
public abstract class AbstractContractTest {

    /**
     * Opens stores on one fresh cache.
     */
    protected interface Fixture extends AutoCloseable {

        /**
         * Opens a store on the fixture's cache, as one more instance would.
         *
         * @return the store
         */
        Store openStore();

        /**
         * Makes every subscription lose undelivered messages and report a gap, if the fixture can.
         *
         * @return {@code false} if not supported
         */
        default boolean induceGap() {
            return false;
        }

        /**
         * Makes every subscription lose its delivery (reported with {@link Listener#onLost}) until resumed, if the
         * fixture can; {@link Subscription#resume()} must be able to restore it right away.
         *
         * @return {@code false} if not supported
         */
        default boolean induceLoss() {
            return false;
        }

        /**
         * Opens a store of another cache that shares the table or collection with the fixture's cache, if the
         * store has such a thing (another discriminator), else another cache in the same database.
         *
         * @return the store, or {@code null} if not supported
         */
        default @Nullable Store openStoreOfAnotherCache() {
            return null;
        }

        @Override
        default void close() {
        }
    }

    protected static final Duration TIMEOUT = Duration.ofSeconds(10);
    protected static final Duration QUIET = Duration.ofMillis(300);
    protected static final Duration LONG = Duration.ofHours(1);

    protected Fixture fixture;
    private final List<Store> stores = new ArrayList<>();

    /**
     * Creates a fixture on a fresh cache.
     *
     * @return the fixture
     */
    protected abstract Fixture createFixture();

    @BeforeEach
    void setUp() {
        fixture = createFixture();
    }

    @AfterEach
    void tearDown() {
        clock = null;
        stores.forEach(Store::close);
        fixture.close();
    }

    // the store's time now, for the version of a row created on "missing"
    protected long readTime() {
        if (clock == null) {
            clock = open();
        }
        return clock.read(List.of(), LONG).time();
    }

    private Store clock;

    protected Store open() {
        Store store = fixture.openStore();
        stores.add(store);
        return store;
    }

    // a store of another cache next to the fixture's (skipped where the fixture has none)
    protected Store openOther() {
        Store store = fixture.openStoreOfAnotherCache();
        assumeTrue(store != null, "the fixture cannot open another cache");
        stores.add(store);
        return store;
    }

    protected static Entry entry(String hash, String value) {
        return new Entry(hash, ("key-" + hash).getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    protected static String value(Row row) {
        return new String(requireNonNull(row.value()), StandardCharsets.UTF_8);
    }

    // runs the operation on that many stores at once and sums the results
    protected int race(int racers, java.util.function.ToIntFunction<Store> operation) throws Exception {
        List<Store> racing = new ArrayList<>();
        for (int i = 0; i < racers; i++) {
            racing.add(open());
        }
        ExecutorService executor = Executors.newFixedThreadPool(racers);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Integer>> futures = new ArrayList<>();
            for (Store store : racing) {
                Callable<Integer> task = () -> {
                    start.await();
                    return operation.applyAsInt(store);
                };
                futures.add(executor.submit(task));
            }
            start.countDown();
            int sum = 0;
            for (Future<Integer> future : futures) {
                sum += future.get();
            }
            return sum;
        } finally {
            executor.shutdownNow();
        }
    }
}
