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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * What the SPI contract tests share: a fixture opening several stores on one fresh cache (as several instances
 * would), every test on a fresh cache, and the stores a test opened closed after it.
 */
@NullMarked
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
         * Makes every subscription lose its delivery (reported with {@link ChangeListener#onLost}) until resumed, if
         * the fixture can; {@link ChangeSubscription#resume()} must be able to restore it right away.
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

        /**
         * Whether the store applies the writes of a call of that size by assigning their versions (VISION.md, "Target
         * design: versions applied as well as each store can"): an advance never refused, at a version the store
         * chooses above the stored one. Otherwise conditionally, at the versions proposed.
         *
         * @param batchSize the number of writes in the call
         * @return whether it assigns
         */
        default boolean assigns(int batchSize) {
            return false;
        }

        /**
         * Whether the store numbers versions in a domain of its own: every write assigned, the proposals and the
         * version given to {@code expire} ignored.
         *
         * @return whether its versions are its own
         */
        default boolean ownVersions() {
            return false;
        }

        @Override
        default void close() {
        }
    }

    protected static final Duration TIMEOUT = Duration.ofSeconds(10);
    protected static final Duration QUIET = Duration.ofMillis(300);
    protected static final Duration LONG = Duration.ofHours(1);

    @SuppressWarnings("NotNullFieldNotInitialized") // set before each test (@BeforeEach), the way JUnit sets up
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
        stores.forEach(Store::close);
        fixture.close();
    }

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

    // versions as the core proposes them (InternalVersion): the time in milliseconds since 2020 above 20 bits, each
    // above the last. Half a millisecond's worth of counter added, so that a version proposed now lies above what a
    // store assigning in the core's domain chose for a write in this millisecond (the store's time, low bits zero)
    private static final long CORE_EPOCH_MILLIS = 1_577_836_800_000L;
    private static final AtomicLong VERSIONS = new AtomicLong();

    protected static long nextVersion() {
        long now = ((System.currentTimeMillis() - CORE_EPOCH_MILLIS) << 20) | (1L << 19);
        return VERSIONS.updateAndGet(last -> Math.max(now, last + 1));
    }

    // an unconditional write of a value at a new version
    protected static Write entry(String hash, String value) {
        return Write.value(hash, ("key-" + hash).getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8), nextVersion());
    }

    // an unconditional tombstone at a new version
    protected static Write tombstone(String hash) {
        return Write.tombstone(hash, nextVersion());
    }

    // the core's invalidateAll: an unconditional write of the control row, a value row (Store#CONTROL_HASH)
    protected static Write control() {
        return Write.value(Store.CONTROL_HASH, new byte[0], new byte[0], nextVersion());
    }

    protected static Write ifVersion(Write write, long version) {
        return write.ifVersion(version);
    }

    protected static Write ifMissing(Write write) {
        return write.ifMissing();
    }

    // writes and returns the version it was applied at (the store's, where it assigns one); the write must not
    // conflict
    protected static long written(Store store, Write write) {
        WriteResult result = store.write(List.of(write));
        if (result.isRefused(write)) {
            throw new AssertionError("the write conflicted: " + write);
        }
        return result.version(write);
    }

    // whether a single write was applied
    protected static boolean applied(Store store, Write write) {
        return store.write(List.of(write)).refused().isEmpty();
    }

    protected static String value(Row row) {
        return new String(requireNonNull(row.value()), StandardCharsets.UTF_8);
    }

    // runs the operation on that many stores at once and sums the results
    // what a write on a version that found no row may leave (Store#write): nothing, or a tombstone at its version
    protected static void assertNothingOrItsTombstone(Map<String, Row> rows, Write write) {
        Row row = rows.get(write.hash());
        if (row != null) {
            assertThat(row.state()).as(write.hash() + ": a tombstone, if anything").isEqualTo(State.TOMBSTONE);
            assertThat(row.version()).as(write.hash() + ": at the write's version").isEqualTo(write.version());
        }
    }

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
