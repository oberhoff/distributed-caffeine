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
import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.ScanOnlyStore;
import io.github.oberhoff.distributedcaffeine.testing.StoreProfile;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The core on simulated time (VISION.md, "Deterministic simulation", B): {@link SimulatedEnvironment} and the cache's
 * executor direct, so that the paths waiting for time run at the production defaults (polls every second, the
 * heartbeat every 10 s, the polls' grace of about five minutes) in milliseconds, and what depends on randomness repeats
 * with the seed.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(60)
class InternalSimulationTest {

    private static final long SEED = 42;
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (int index = closeables.size() - 1; index >= 0; index--) {
            closeables.get(index).close();
        }
    }

    // a cache on the database's table, at the defaults but for its bounds, on simulated time
    private DistributedCache<String, String> cache(InMemoryDatabase database, SimulatedEnvironment environment) {
        return cache(settings -> database.openStore("cache"), environment);
    }

    private DistributedCache<String, String> cache(Adapter<String, String> adapter,
                                                   SimulatedEnvironment environment) {
        DistributedCache<String, String> cache = DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000)
                        .withExecutor(Runnable::run))
                // Caffeine on the simulation too: its executor direct, its clock (L1's expiry) the environment's
                .withCaffeine(caffeine -> caffeine.recordStats().executor(Runnable::run)
                        .ticker(environment::nanoTime))
                .withEnvironment(environment)
                .build();
        closeables.add(cache);
        return cache;
    }

    // polled by the core, as MariaDB and MySQL are; the store's clock (microseconds) from the environment's wall clock,
    // so that ages move with the simulated time
    private static InMemoryDatabase polledDatabase(SimulatedEnvironment environment) {
        return database(environment, StoreProfile.JDBC, SEED);
    }

    // its time and its deliveries on the simulation too
    private static InMemoryDatabase database(SimulatedEnvironment environment, StoreProfile profile, long seed) {
        InMemoryDatabase database = new InMemoryDatabase(() -> environment.currentTimeMillis() * 1_000, seed, profile);
        database.simulate(environment.forDatabase());
        return database;
    }

    // a store polled by the core down for minutes: DEGRADED at once (its polls fail), the delivery lost past the
    // grace, a hit served throughout; back, the cache resumes, and a write elsewhere reaches it again. Real time:
    // the six minutes of the outage would take six minutes
    @Test
    @SuppressWarnings("resource") // the caches closed after the test
    void a_polled_cache_rides_out_an_outage_of_minutes_at_the_production_defaults() {
        SimulatedEnvironment environment = new SimulatedEnvironment(SEED);
        InMemoryDatabase database = polledDatabase(environment);
        DistributedCache<String, String> writer = cache(database, environment);
        DistributedCache<String, String> holder = cache(database, environment);
        writer.put("k", "before");
        environment.advance(Duration.ofSeconds(2));
        assertThat(holder.getIfPresent("k")).isEqualTo("before");
        assertThat(holder.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);

        database.setDown(true);
        environment.advance(Duration.ofSeconds(2));
        assertThat(holder.distributedPolicy().getSynchronizationState()).as("its polls failing")
                .isEqualTo(SynchronizationState.DEGRADED);
        assertThat(holder.distributedPolicy().stats().interruptionCount()).as("within the grace").isZero();
        environment.advance(Duration.ofMinutes(6));
        assertThat(holder.distributedPolicy().stats().interruptionCount()).as("its delivery lost past the grace")
                .isPositive();
        assertThat(holder.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);
        assertThat(holder.policy().getIfPresentQuietly("k")).as("still held").isEqualTo("before");

        database.setDown(false);
        environment.advance(Duration.ofMinutes(1));
        assertThat(holder.distributedPolicy().getSynchronizationState()).as("resumed")
                .isEqualTo(SynchronizationState.SYNCHRONIZED);
        assertThat(holder.distributedPolicy().stats().gapCount()).as("the resume a gap").isPositive();
        writer.put("k", "after");
        environment.advance(Duration.ofSeconds(2));
        assertThat(holder.getIfPresent("k")).isEqualTo("after");
    }

    // three calls failing in a row open the store guard: calls are refused, also once the store answers again, until
    // its suspension of a second has passed; then a probe goes through and closes it. Real time: the second waited
    @Test
    @SuppressWarnings("resource") // the cache closed after the test
    void the_store_guard_refuses_calls_for_its_suspension_then_lets_a_probe_through() {
        SimulatedEnvironment environment = new SimulatedEnvironment(SEED);
        InMemoryDatabase database = polledDatabase(environment);
        DistributedCache<String, String> cache = cache(database, environment);
        cache.put("k", "v");

        DistributedPolicy<String, String> policy = cache.distributedPolicy();
        database.setDown(true);
        for (int call = 0; call < InternalStoreGuard.THRESHOLD; call++) {
            assertThatThrownBy(() -> policy.getFromStore("k")).isInstanceOf(StoreException.class);
        }
        database.setDown(false);
        assertThat(policy.stats().guardOpenCount()).isEqualTo(1);
        assertThatThrownBy(() -> policy.getFromStore("k")).as("refused while suspended")
                .isInstanceOf(StoreException.class).hasMessageContaining("refused");
        assertThat(cache.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);

        environment.advance(Duration.ofNanos(InternalStoreGuard.SUSPENSION_NANOS));
        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).as("the probe")
                .isEqualTo("v");
        assertThat(cache.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.SYNCHRONIZED);
    }

    // a feed that stops delivering without a word (a scan answering nothing while writes are made): the heartbeat
    // misses its own beat, the held values are no longer trusted (each access checks its version against the store,
    // so a write elsewhere meanwhile is not served stale), and once the feed delivers again the cache resumes with a
    // gap. Real time: the heartbeat's 10 s, twice
    @Test
    @SuppressWarnings("resource") // the caches closed after the test
    void the_heartbeat_finds_a_silent_feed_and_nothing_stale_is_served_meanwhile() {
        SimulatedEnvironment environment = new SimulatedEnvironment(SEED);
        InMemoryDatabase database = polledDatabase(environment);
        AtomicBoolean silent = new AtomicBoolean();
        // the holder's feed goes silent, not the writer's
        DistributedCache<String, String> holder = cache(settings -> new ScanOnlyStore(database.openFeed("cache")) {
            @Override
            public List<Change> changedWithin(Duration within) {
                List<Change> changes = super.changedWithin(within);
                return silent.get() ? List.of() : changes;
            }
        }, environment);
        DistributedCache<String, String> writer = cache(database, environment);
        writer.put("k", "before");
        environment.advance(Duration.ofSeconds(2));
        assertThat(holder.getIfPresent("k")).isEqualTo("before");

        silent.set(true);
        writer.put("k", "while silent");
        environment.advance(Duration.ofSeconds(30));
        assertThat(holder.distributedPolicy().stats().interruptionCount()).as("silenced").isPositive();
        assertThat(holder.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);
        assertThat(holder.getIfPresent("k")).as("checked against the store, not served stale")
                .isEqualTo("while silent");

        silent.set(false);
        environment.advance(Duration.ofSeconds(30));
        assertThat(holder.distributedPolicy().getSynchronizationState()).as("delivering again")
                .isEqualTo(SynchronizationState.SYNCHRONIZED);
        assertThat(holder.distributedPolicy().stats().gapCount()).isPositive();
    }

    // a store with a feed of its own, delivering late and twice at random: under one seed, every run sees the same
    // messages at the same simulated instants, so what the holder holds at each step repeats exactly (the goal: a
    // failure replayed from its seed)
    @Test
    void the_same_seed_replays_the_same_run_on_a_feed_with_faults() {
        List<String> first = trace(SEED);
        List<String> second = trace(SEED);

        assertThat(second).isEqualTo(first);
        assertThat(first).as("the holder saw writes arrive, late and in between").contains("v99")
                .contains("absent");
        assertThat(trace(SEED + 1)).as("another seed, another run").isNotEqualTo(first);
    }

    // what the holder holds of the key just written, after a step of random simulated time, for 100 writes
    private List<String> trace(long seed) {
        SimulatedEnvironment environment = new SimulatedEnvironment(seed);
        InMemoryDatabase database = database(environment, StoreProfile.REDIS, seed);
        database.setDeliveryDelay(Duration.ofMillis(50));
        database.setDuplicateProbability(0.2);
        Random steps = new Random(seed);
        List<String> trace = new ArrayList<>();
        try (DistributedCache<String, String> writer = cache(database, environment);
             DistributedCache<String, String> holder = cache(database, environment)) {
            for (int write = 0; write < 100; write++) {
                String key = "k" + write % 5;
                holder.getIfPresent(key);
                writer.put(key, "v" + write);
                environment.advance(Duration.ofMillis(steps.nextInt(60)));
                trace.add(String.valueOf(holder.policy().getIfPresentQuietly(key) == null ? "absent"
                        : holder.policy().getIfPresentQuietly(key)));
            }
            environment.advance(Duration.ofSeconds(1));
            trace.add(String.valueOf(holder.getIfPresent("k4")));
            trace.add(holder.distributedPolicy().stats().toString());
        }
        return trace;
    }

    // invalidateAll on a store whose clock counts in milliseconds: the call waits out the allowance (simulated), so a
    // put after it is stamped after the control row; and an instance that applied the message distrusts what it
    // installed within the allowance afterwards (a write there could carry the control row's time and be hidden): once
    // the allowance has passed, its next read checks the held value against the store. Real time: a millisecond, too
    // short to place a write within
    @Test
    @SuppressWarnings("resource") // the caches closed after the test
    void invalidate_all_waits_out_the_clock_allowance_and_distrusts_what_was_installed_within_it() {
        SimulatedEnvironment environment = new SimulatedEnvironment(SEED);
        InMemoryDatabase database = database(environment, StoreProfile.MONGODB, SEED);
        Duration allowance = StoreProfile.MONGODB.clockAllowance();
        DistributedCache<String, String> writer = cache(database, environment);
        DistributedCache<String, String> holder = cache(database, environment);

        long before = environment.nanoTime();
        writer.invalidateAll();
        assertThat(Duration.ofNanos(environment.nanoTime() - before)).as("waited out")
                .isGreaterThanOrEqualTo(allowance);
        environment.advance(Duration.ZERO); // the message applied by the holder, no time passing
        holder.put("k", "within the allowance");
        assertThat(holder.getIfPresent("k")).isEqualTo("within the allowance");
        long revalidated = holder.distributedPolicy().stats().revalidatedCount();

        environment.advance(allowance);
        assertThat(holder.getIfPresent("k")).isEqualTo("within the allowance");
        assertThat(holder.distributedPolicy().stats().revalidatedCount()).as("checked once the allowance passed")
                .isGreaterThan(revalidated);
    }

    // the maximum age (an hour) passing: the holder's copy expires in L1 (Caffeine on the simulated clock), maintenance
    // expires the row in the store and announces it, and a read finds nothing. Real time: an hour
    @Test
    @SuppressWarnings("resource") // the caches closed after the test
    void a_value_expires_everywhere_at_the_maximum_age() {
        SimulatedEnvironment environment = new SimulatedEnvironment(SEED);
        InMemoryDatabase database = database(environment, StoreProfile.REDIS, SEED);
        DistributedCache<String, String> writer = cache(database, environment);
        DistributedCache<String, String> holder = cache(database, environment);
        writer.put("k", "v");
        environment.advance(Duration.ofSeconds(1));
        assertThat(holder.getIfPresent("k")).isEqualTo("v");

        environment.advance(Duration.ofMinutes(50));
        assertThat(holder.policy().getIfPresentQuietly("k")).as("held before the maximum age").isEqualTo("v");
        environment.advance(Duration.ofMinutes(20));
        assertThat(holder.policy().getIfPresentQuietly("k")).as("expired in L1").isNull();
        assertThat(holder.distributedPolicy().getFromStore("k")).as("expired in the store").isNull();
        assertThat(holder.getIfPresent("k")).isNull();
        // announced by maintenance (the copy itself expired in L1 first, so none was dropped by the message)
        assertThat(holder.distributedPolicy().stats().expiredMessageCount()).as("expired by maintenance").isPositive();
    }

    // the clock's random node and the wall clock from the environment: the same seed proposes the same versions
    @Test
    void the_same_seed_proposes_the_same_versions() {
        InternalVersion first = new InternalVersion(new SimulatedEnvironment(SEED), ignored -> {
        });
        InternalVersion second = new InternalVersion(new SimulatedEnvironment(SEED), ignored -> {
        });
        InternalVersion other = new InternalVersion(new SimulatedEnvironment(SEED + 1), ignored -> {
        });

        long version = first.fresh();
        assertThat(second.fresh()).isEqualTo(version);
        assertThat(other.fresh()).as("the node differs with the seed").isNotEqualTo(version);
    }
}
