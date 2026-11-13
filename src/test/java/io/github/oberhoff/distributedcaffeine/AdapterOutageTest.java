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
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.testing.Awaiting;
import io.github.oberhoff.distributedcaffeine.testing.PausedContainers;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.github.oberhoff.distributedcaffeine.AdapterConformanceTest.put;
import static io.github.oberhoff.distributedcaffeine.AdapterConformanceTest.recovers;
import static io.github.oberhoff.distributedcaffeine.AdapterConformanceTest.startsDegraded;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A whole store out while a cache is built (VISION.md, "Starting without the store"), held to what an outage while
 * running is held to (the restart and failover suites of each store): paused (every container frozen: connections
 * accepted, nothing answered), briefly and for several of the connect's attempts; stopped as a restart does it; and
 * without a primary (a replica set without its majority, a primary dead before Sentinel or the cluster fails over).
 * The cache starts {@code DEGRADED}, serves misses from the source, and once the store is back takes what is stored
 * over what it loaded meanwhile and distributes both ways with an instance built before the outage; with
 * {@code withFailFast()} the build fails as retryable instead. Also for topologies reached through several addresses,
 * where {@link AdapterConformanceTest} cannot put a proxy in front.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(600)
@SuppressWarnings("resource") // every cache and outage a test opens is closed after it (@AfterEach)
public abstract class AdapterOutageTest {

    // short, so that a paused topology stays paused about a second (see PausedContainers)
    private static final Duration SHORT = Duration.ofSeconds(1);
    // the connect's backoff grows to 30 s, plus a check of the store bounded by the operation timeout
    private static final Duration CONNECT = Duration.ofSeconds(60);
    // a store started again, or a primary elected, before the connect's next attempt finds it
    private static final Duration BACK = Duration.ofMinutes(2);
    // paused across several of the connect's attempts (the backoff starts at 250 ms and doubles)
    private static final Duration LONG_OUTAGE = Duration.ofSeconds(6);
    // a build that cannot reach the store: a few checks, each bounded by the operation timeout (a client may try a
    // topology's addresses one after another; Redisson with lazy initialization connects inside its first command,
    // with its own retries, about 16 s)
    private static final Duration BUILD = Duration.ofSeconds(30);

    private final List<AutoCloseable> closeables = new ArrayList<>();

    /**
     * Returns the store's side of the outage, on a fresh place.
     *
     * @return the hooks
     */
    protected abstract Outage outage();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private Outage supportedOutage() {
        Outage outage = track(outage());
        assumeTrue(outage.supported(), "the topology is refused at the build");
        return outage;
    }

    private DistributedCache<String, String> build(Adapter<String, String> adapter, boolean failFast) {
        DistributedCache<String, String> cache = DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> failFast ? store.withOperationTimeout(SHORT).withFailFast()
                        : store.withOperationTimeout(SHORT))
                .build();
        closeables.add(cache);
        return cache;
    }

    // the instance built before the outage, with a value stored
    private DistributedCache<String, String> other(Outage outage) {
        DistributedCache<String, String> other = build(outage.adapter(), false);
        put(other, "held", "stored", CONNECT);
        return other;
    }

    // built while the store is out: within a bound, not hanging on a check without a timeout of its own (MongoDB's
    // deployment check once held the build for good)
    private DistributedCache<String, String> buildWithout(Adapter<String, String> adapter) {
        long before = System.nanoTime();
        DistributedCache<String, String> started = build(adapter, false);
        assertThat(Duration.ofNanos(System.nanoTime() - before)).as("the build waits a bounded time")
                .isLessThan(BUILD);
        return started;
    }

    private static void failsAsRetryable(ThrowingBuild build) {
        long before = System.nanoTime();
        assertThatThrownBy(build::build)
                .isInstanceOfSatisfying(StoreException.class, e -> {
                    assertThat(e.isRetryable()).as("retryable: " + e.getMessage()).isTrue();
                    assertThat(e.isOutcomeUnknown()).as("nothing written").isFalse();
                });
        assertThat(Duration.ofNanos(System.nanoTime() - before)).as("the build waits a bounded time")
                .isLessThan(BUILD);
    }

    @Test
    void a_paused_store_starts_the_cache_without_it_which_connects_once_it_answers() {
        Outage outage = supportedOutage();
        DistributedCache<String, String> other = other(outage);
        DistributedCache<String, String> started;
        try (PausedContainers ignored = outage.pause()) {
            started = buildWithout(outage.adapter());
            startsDegraded(started);
        }

        recovers(other, started, CONNECT);
    }

    // paused across several of the connect's attempts, each bounded by the operation timeout: none hangs (MongoDB's
    // deployment check without a timeout of its own held the build for good), and misses load from the source at once
    @Test
    void a_store_paused_for_several_attempts_starts_the_cache_without_it_which_connects_once_it_answers() {
        Outage outage = supportedOutage();
        DistributedCache<String, String> other = other(outage);
        DistributedCache<String, String> started;
        try (PausedContainers ignored = outage.pause()) {
            long end = System.nanoTime() + LONG_OUTAGE.toNanos();
            started = buildWithout(outage.adapter());
            startsDegraded(started);
            for (int i = 0; System.nanoTime() < end; i++) {
                long before = System.nanoTime();
                assertThat(started.get("miss " + i, key -> "source")).isEqualTo("source");
                assertThat(Duration.ofNanos(System.nanoTime() - before)).as("a miss waits for no store")
                        .isLessThan(Duration.ofMillis(500));
                assertThat(started.distributedPolicy().getSynchronizationState())
                        .isEqualTo(SynchronizationState.DEGRADED);
                Awaiting.pause(250);
            }
        }

        recovers(other, started, CONNECT);
    }

    @Test
    void a_paused_store_fails_the_build_as_retryable_with_fail_fast() {
        Outage outage = supportedOutage();
        try (PausedContainers ignored = outage.pause()) {
            Adapter<String, String> adapter = outage.adapter();
            failsAsRetryable(() -> build(adapter, true));
        }
    }

    // stopped as a restart for an update does it, on its data and at its addresses, as the restart suites do it to
    // caches running
    @Test
    void a_stopped_store_starts_the_cache_without_it_which_connects_once_it_is_back() {
        Outage outage = supportedOutage();
        assumeTrue(outage.stops(), "the topology is not restarted at its addresses here");
        DistributedCache<String, String> other = other(outage);
        DistributedCache<String, String> started;
        try (Interruption ignored = outage.stop()) {
            started = buildWithout(outage.adapter());
            startsDegraded(started);
        }

        recovers(other, started, BACK);
    }

    @Test
    void a_stopped_store_fails_the_build_as_retryable_with_fail_fast() {
        Outage outage = supportedOutage();
        assumeTrue(outage.stops(), "the topology is not restarted at its addresses here");
        try (Interruption ignored = outage.stop()) {
            Adapter<String, String> adapter = outage.adapter();
            failsAsRetryable(() -> build(adapter, true));
        }
    }

    // reached, but without a primary to write to, as the failover suites leave a running cache for a while
    @Test
    void a_store_without_a_primary_starts_the_cache_without_it_which_connects_once_one_is_back() {
        Outage outage = supportedOutage();
        assumeTrue(outage.losesPrimary(), "the topology has no primary to lose");
        DistributedCache<String, String> other = other(outage);
        DistributedCache<String, String> started;
        try (Interruption ignored = outage.withoutPrimary()) {
            started = buildWithout(outage.adapter());
            startsDegraded(started);
        }

        recovers(other, started, BACK);
    }

    @Test
    void a_store_without_a_primary_fails_the_build_as_retryable_with_fail_fast() {
        Outage outage = supportedOutage();
        assumeTrue(outage.losesPrimary(), "the topology has no primary to lose");
        try (Interruption ignored = outage.withoutPrimary()) {
            Adapter<String, String> adapter = outage.adapter();
            failsAsRetryable(() -> build(adapter, true));
        }
    }

    @FunctionalInterface
    private interface ThrowingBuild {

        void build();
    }

    /**
     * An outage in progress, ended by closing it: the store back, waited for until it serves.
     *
     * @author Andreas Oberhoff
     */
    @FunctionalInterface
    public interface Interruption extends AutoCloseable {

        @Override
        void close();
    }

    /**
     * What a topology contributes: adapters on one place, each with a client of its own, and its outages.
     *
     * @author Andreas Oberhoff
     */
    public interface Outage extends AutoCloseable {

        /**
         * Returns whether the topology is one a cache can use at all.
         *
         * @return {@code false} for a topology the adapter refuses (a MongoDB standalone server)
         */
        default boolean supported() {
            return true;
        }

        /**
         * Returns an adapter on the outage's place, with a client of its own that waits about a second to connect
         * and to find its servers, created while the store may be out, and closed by {@link #close()}.
         *
         * @return the adapter
         */
        Adapter<String, String> adapter();

        /**
         * Pauses every container of the topology until the result is closed.
         *
         * @return the pause
         */
        PausedContainers pause();

        /**
         * Returns whether {@link #stop()} is supported.
         *
         * @return whether the topology can be stopped and started at its addresses
         */
        default boolean stops() {
            return false;
        }

        /**
         * Stops the whole store as a restart does it (gracefully, on its data, at its addresses); closing starts it
         * again and waits until it serves.
         *
         * @return the outage
         */
        default Interruption stop() {
            throw new UnsupportedOperationException("not stopped here");
        }

        /**
         * Returns whether {@link #withoutPrimary()} is supported.
         *
         * @return whether the topology has a primary to lose
         */
        default boolean losesPrimary() {
            return false;
        }

        /**
         * Leaves the store reachable but without a primary to write to; closing brings one back (elected, failed
         * over or started again) and waits until it serves.
         *
         * @return the outage
         */
        default Interruption withoutPrimary() {
            throw new UnsupportedOperationException("no primary to lose here");
        }

        /**
         * Closes the clients of {@link #adapter()}.
         */
        @Override
        void close();
    }
}
