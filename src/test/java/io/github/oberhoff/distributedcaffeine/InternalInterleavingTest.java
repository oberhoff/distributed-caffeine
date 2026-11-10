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
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.StoreProfile;
import org.jetbrains.lincheck.Lincheck;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The core's races explored by Lincheck's model checking of whole scenarios (VISION.md, "Deterministic simulation",
 * D): two instances on the simulated in-memory store, the threads of a scenario and one delivering messages
 * meanwhile, every interleaving Lincheck chooses replayable. One invariant for all: once the delivery has caught up,
 * a read of the holder serves nothing or what the store holds (no value staler than the delivery). Each scenario
 * reaches one mechanism (a load's reservation, an invalidation, invalidateAll's epoch, a gap's revalidation), and a
 * mutation of that mechanism fails it. About a minute each; run with {@code -Dmodelchecking=true}.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@EnabledIfSystemProperty(named = "modelchecking", matches = "true")
@Timeout(1_800)
@SuppressWarnings("resource") // a scenario's caches, reached through its fixture, are closed by race
class InternalInterleavingTest {

    private static final int INVOCATIONS = 500;

    // two instances on one store, on simulated time, everything direct: the scenario's threads are the only ones
    // the holder loading (its loader answers "loaded"), for refresh
    private record Fixture(SimulatedEnvironment environment, InMemoryDatabase database,
                           DistributedCache<String, String> writer, DistributedLoadingCache<String, String> holder) {
    }

    // the scenario's threads raced, with a delivery meanwhile; then the delivery caught up, and the invariant checked
    private static void race(Consumer<Fixture> before, Function<Fixture, List<Runnable>> threads) {
        race(StoreProfile.REDIS, before, threads, fixture -> {
        });
    }

    private static void race(StoreProfile profile, Consumer<Fixture> before,
                             Function<Fixture, List<Runnable>> threads) {
        race(profile, before, threads, fixture -> {
        });
    }

    // after: a check of the scenario's own, once the invariant holds
    private static void race(StoreProfile profile, Consumer<Fixture> before,
                             Function<Fixture, List<Runnable>> threads, Consumer<Fixture> after) {
        race(INVOCATIONS, profile, before, threads, after);
    }

    private static void race(int invocations, StoreProfile profile, Consumer<Fixture> before,
                             Function<Fixture, List<Runnable>> threads, Consumer<Fixture> after) {
        Lincheck.runConcurrentTest(invocations, () -> {
            SimulatedEnvironment environment = new SimulatedEnvironment(42);
            InMemoryDatabase database = new InMemoryDatabase(() -> environment.currentTimeMillis() * 1_000, 42,
                    profile);
            database.simulate(environment.forDatabase());
            try (DistributedCache<String, String> writer = builder(database, environment).build();
                 DistributedLoadingCache<String, String> holder = builder(database, environment)
                         .build(key -> "loaded")) {
                Fixture fixture = new Fixture(environment, database, writer, holder);
                before.accept(fixture);
                List<Thread> started = new ArrayList<>();
                List<Runnable> raced = new ArrayList<>(threads.apply(fixture));
                // delivering meanwhile: a message may reach the holder in the middle of an operation
                raced.add(() -> environment.advance(Duration.ofMillis(10)));
                for (Runnable runnable : raced) {
                    Thread thread = new Thread(runnable);
                    started.add(thread);
                    thread.start();
                }
                for (Thread thread : started) {
                    join(thread);
                }
                environment.advance(Duration.ofSeconds(1));
                // what a read serves, not what L1 holds: a value held but no longer trusted (a load overlapping an
                // invalidateAll, under the old epoch) is checked against the store before it is served
                String inStore = value(holder.distributedPolicy().getFromStore("k"));
                String served = holder.getIfPresent("k");
                check(served == null || served.equals(inStore), "served " + served + ", the store has " + inStore);
                after.accept(fixture);
            }
        });
    }

    // a write's message during a load: the load's result, older than the write, must not stay (its reservation)
    @Test
    void a_load_racing_a_write_elsewhere() {
        race(fixture -> fixture.writer().put("k", "old"), fixture -> List.of(
                () -> fixture.writer().put("k", "new"),
                () -> fixture.holder().get("k", key -> "loaded")));
    }

    // an invalidation during a load: what the load read before it must not stay
    @Test
    void a_load_racing_an_invalidation_elsewhere() {
        race(fixture -> fixture.writer().put("k", "old"), fixture -> List.of(
                () -> fixture.writer().invalidate("k"),
                () -> fixture.holder().get("k", key -> "loaded")));
    }

    // invalidateAll during a load: a value read before its cut-off must not stay (the epoch its message moves). On a
    // clock without allowance: with one, the epoch moves once more an allowance later (distrustWithinAllowance), which
    // would cover a message not moving it (found: that mutation survived on the Redis profile's microseconds)
    @Test
    void a_load_racing_an_invalidate_all_elsewhere() {
        race(StoreProfile.REDIS.with(StoreProfile.Clock.MONOTONIC, Duration.ZERO),
                fixture -> fixture.writer().put("k", "old"), fixture -> List.of(
                () -> fixture.writer().invalidateAll(),
                () -> fixture.holder().get("k", key -> "loaded")));
    }

    // invalidateAll during a read through the store (getIfPresent): it reads first and installs after, with no future
    // in L1 that the clear would remove, so only the epoch its message moves marks a value read before the cut-off as
    // untrusted (a load registers its future before it reads: the clear covers it)
    @Test
    void a_read_through_racing_an_invalidate_all_elsewhere() {
        race(StoreProfile.REDIS.with(StoreProfile.Clock.MONOTONIC, Duration.ZERO),
                fixture -> fixture.writer().put("k", "old"), fixture -> List.of(
                        () -> fixture.writer().invalidateAll(),
                        () -> fixture.holder().getIfPresent("k")));
    }

    // a write elsewhere whose message is lost in a gap during a load: the gap's revalidation must catch it
    @Test
    void a_load_racing_a_write_lost_in_a_gap() {
        race(fixture -> fixture.writer().put("k", "old"), fixture -> List.of(
                () -> {
                    fixture.writer().put("k", "new");
                    fixture.database().gap();
                },
                () -> fixture.holder().get("k", key -> "loaded")));
    }

    // a refresh racing a write elsewhere: the refresh's result, older than the write, must not stay (a refresh is a
    // load, written conditionally)
    @Test
    void a_refresh_racing_a_write_elsewhere() {
        race(fixture -> {
            fixture.writer().put("k", "old");
            fixture.holder().get("k");
        }, fixture -> List.of(
                () -> fixture.writer().put("k", "new"),
                () -> fixture.holder().refresh("k")));
    }

    // two instances incrementing one key: neither increment lost in the store (each compute writes conditionally on
    // the version it read; refused, it reads and computes again). The invariant alone would not see a lost update: the
    // store's value is what the caches converge on either way
    @Test
    void two_computes_racing_lose_no_update() {
        race(StoreProfile.REDIS, fixture -> {
            fixture.writer().put("k", "0");
            fixture.holder().get("k");
        }, fixture -> List.of(
                () -> fixture.writer().asMap().compute("k", (key, value) -> increment(value)),
                () -> fixture.holder().asMap().compute("k", (key, value) -> increment(value))),
                fixture -> {
                    String inStore = value(fixture.holder().distributedPolicy().getFromStore("k"));
                    check("2".equals(inStore), "two increments, the store has " + inStore);
                });
    }

    private static String increment(@Nullable String value) {
        return String.valueOf(value == null ? 1 : Integer.parseInt(value) + 1);
    }

    // a failover losing the latest write while the holder loads: what the holder took from before the failover must
    // not stay over what the store fell back to (the failover reports a gap, and the gap revalidates)
    @Test
    void a_load_racing_a_failover_that_loses_a_write() {
        race(fixture -> fixture.writer().put("k", "old"), fixture -> List.of(
                () -> {
                    fixture.writer().put("k", "lost");
                    fixture.database().failover(Duration.ofSeconds(1));
                },
                () -> fixture.holder().get("k", key -> "loaded")));
    }

    // a load finding nothing (an absence remembered) racing a write elsewhere: once delivered, the key is found, not
    // answered absent from the memory of a load that read before the write. The invariant alone would pass a stale
    // absence (it serves nothing)
    // with 3,000 interleavings: its window (the write's message applied between the load's read and its recording
    // the absence) was missed in 500, found within 36 s in 3,000 (an absence kept despite the raised reservation)
    @Test
    void a_load_finding_nothing_racing_a_write_elsewhere() {
        race(3_000, StoreProfile.REDIS, fixture -> {
        }, fixture -> List.of(
                () -> fixture.writer().put("k", "new"),
                () -> fixture.holder().get("k", key -> null)),
                fixture -> {
                    String inStore = value(fixture.holder().distributedPolicy().getFromStore("k"));
                    String found = fixture.holder().get("k", key -> null);
                    check(Objects.equals(found, inStore), "found " + found + ", the store has " + inStore);
                });
    }

    // two instances claiming one key (putIfAbsent): exactly one wins, the store holds its value, and the other is
    // told that value
    @Test
    void two_put_if_absents_racing_claim_once() {
        AtomicReference<@Nullable String> first = new AtomicReference<>("unset");
        AtomicReference<@Nullable String> second = new AtomicReference<>("unset");
        race(StoreProfile.REDIS, fixture -> {
        }, fixture -> List.of(
                () -> first.set(fixture.writer().asMap().putIfAbsent("k", "a")),
                () -> second.set(fixture.holder().asMap().putIfAbsent("k", "b"))),
                fixture -> {
                    String inStore = value(fixture.holder().distributedPolicy().getFromStore("k"));
                    String a = first.get();
                    String b = second.get();
                    boolean aWon = a == null && "a".equals(inStore) && "a".equals(b);
                    boolean bWon = b == null && "b".equals(inStore) && "b".equals(a);
                    check(aWon || bWon, "told " + a + " and " + b + ", the store has " + inStore);
                });
    }

    // both instances writing while the holder also reads its own write back
    @Test
    void two_instances_writing_the_same_key() {
        race(fixture -> fixture.writer().put("k", "old"), fixture -> List.of(
                () -> fixture.writer().put("k", "first"),
                () -> {
                    fixture.holder().put("k", "second");
                    fixture.holder().getIfPresent("k");
                }));
    }

    private static DistributedCaffeine<String, String> builder(InMemoryDatabase database,
                                                               SimulatedEnvironment environment) {
        Adapter<String, String> adapter = settings -> database.openStore("cache");
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                // the key as its hash: Lincheck 3.7 does not support hash4j's VarHandle on byte arrays
                .withHashProvider((key, hasher) -> key)
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000)
                        .withExecutor(Runnable::run))
                .withCaffeine(caffeine -> caffeine.executor(Runnable::run))
                // a load finding nothing remembered (the scenario of absences); no other scenario's load finds nothing
                .withNegativeCaching(Duration.ofMinutes(1))
                .withEnvironment(environment);
    }

    // no AssertJ inside a Lincheck block: its instrumentation calls equals on the objects it meets, which AssertJ's
    // assertions refuse
    private static void check(boolean condition, String failure) {
        if (!condition) {
            throw new AssertionError(failure);
        }
    }

    private static void join(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static @Nullable String value(@Nullable StoredValue<String> stored) {
        return stored == null ? null : stored.value();
    }
}
