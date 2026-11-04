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

import com.github.benmanes.caffeine.cache.CacheLoader;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeScan;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.ScanOnlyStore;
import io.github.oberhoff.distributedcaffeine.testing.StorageOnlyStore;
import io.github.oberhoff.distributedcaffeine.testing.StoreProfile;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The central property test (see the lessons checklist in VISION.md): several instances, writers and loaders running
 * concurrently, latency in every store call, delivery faults, checked phase by phase against three oracles.
 * <p>
 * The <b>source</b> is modelled: a map of the current value per key. Writers change the source and then the cache,
 * one writer per key at a time (a lock per key, as an application serializes its writes to one record); loaders read
 * the source. Values are unique tokens, so a stale one is recognizable.
 * <ul>
 *     <li><b>Freshness</b> (of L2): after each phase, the store holds for every key either no value or the source's
 *     current one. A stale load that slipped past a write (ordering rules 4 and 6) leaves an old token. Allowed: a
 *     token the source held when an {@code invalidateAll} began, which a load that read it before and wrote after
 *     leaves behind (the race accepted since 2026-10-28).</li>
 *     <li><b>Presence</b> (of L2): a key whose last confirmed write (put, {@code putAll}, compute, replace) was not
 *     invalidated since holds exactly that value in the store. Freshness allows no value, so a write lost or hidden
 *     (by a filter hiding too much) passes it; writes and {@code invalidateAll} never overlap in this model, so the
 *     last confirmed write is well defined. Not asked for in the variant with size pruning.</li>
 *     <li><b>Convergence</b> (of L1): after each phase, once nothing is left to deliver, every instance answers
 *     {@code getIfPresent} with exactly what the store holds. A held value that a message failed to drop, or an
 *     untrusted one that was not checked, answers differently.</li>
 * </ul>
 * Checking after every phase, not only at the end, matters: a later write of the same key heals a divergence
 * (lesson of the current generation). Threads interleave freely, so a run is not exactly reproducible; the seed fixes
 * the operations and faults chosen.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(120)
public class InternalPropertyTest {

    private static final Duration LONG = Duration.ofHours(1);
    private static final int KEYS = 8;
    private static final int PHASES = 40;
    private static final int OPERATIONS = 6;
    private static final long STORE_LATENCY_NANOS = TimeUnit.MICROSECONDS.toNanos(200);
    private static final long SOURCE_LATENCY_NANOS = TimeUnit.MICROSECONDS.toNanos(300);

    private final Map<String, String> source = new ConcurrentHashMap<>();
    private final ReentrantLock[] locks = new ReentrantLock[KEYS];
    private final AtomicLong tokens = new AtomicLong();
    // what happened, in order, for the report of a violation
    private final java.util.Queue<String> trace = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final Map<String, String> keysByHash = new ConcurrentHashMap<>();
    // the presence oracle: per key, the value of the last confirmed write since its last invalidation
    private final Map<String, String> confirmed = new ConcurrentHashMap<>();
    // per key, the source's values at each invalidateAll: a load that read one before it and writes after it leaves it
    // behind, the race accepted (VISION.md, "Target design: invalidateAll by the store's time, hidden on read"), so the
    // freshness oracle allows them; tokens are unique, and a later write of the key replaces such a value in the store
    private final Map<String, Set<String>> beforeInvalidateAll = new ConcurrentHashMap<>();
    // the pruning variant: a maximum size below the number of keys and maintenance every few milliseconds, so that
    // size pruning deletes rows silently while the instances work (presence then cannot be asked for); only rows
    // older than its grace (4 s here), so that no poll misses a write and no load's write on "missing" applies over a
    // newer row pruned meanwhile
    private boolean pruning;
    // the deterministic mode (VISION.md, "Deterministic simulation"): one thread, simulated time, choices from the seed;
    // null in the threaded mode
    private @Nullable SimulatedEnvironment simulation;
    private Random choices = new Random(0);

    private void trace(String event) {
        SimulatedEnvironment simulated = simulation;
        trace.add((simulated == null ? System.nanoTime() : simulated.nanoTime()) + " "
                + Thread.currentThread().getName() + " " + event);
    }

    // a wait between operations: real time in the threaded mode, simulated time (deliveries, polls, maintenance running
    // meanwhile) in the deterministic one
    private void idle(Duration duration) {
        SimulatedEnvironment simulated = simulation;
        if (simulated == null) {
            pause(duration.toMillis());
        } else {
            simulated.advance(duration);
        }
    }

    // a latency within an operation: real in the threaded mode, none in the deterministic one (time moving there would
    // run deliveries inside the operation, on its own thread, which real threads never do)
    private void latency(long maxNanos) {
        if (simulation == null) {
            LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(maxNanos));
        }
    }

    {
        for (int i = 0; i < KEYS; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8})
    void the_store_stays_fresh_and_every_instance_converges_on_it(long seed) throws Exception {
        assertThat(run(seed, new InMemoryBackend(seed), PHASES)).as("phases checked").isEqualTo(PHASES);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4})
    void with_stores_without_a_change_feed_polled_by_the_core(long seed) throws Exception {
        assertThat(run(seed, new InMemoryBackend(seed, true), PHASES)).as("phases checked").isEqualTo(PHASES);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4})
    void with_stores_not_checking_the_basis(long seed) throws Exception {
        InMemoryBackend backend = new InMemoryBackend(seed);
        assertThat(run(seed, backend, PHASES)).as("phases checked").isEqualTo(PHASES);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4})
    void with_size_pruning_while_the_instances_work_too(long seed) throws Exception {
        pruning = true;
        assertThat(run(seed, new InMemoryBackend(seed), PHASES)).as("phases checked").isEqualTo(PHASES);
    }

    // the deterministic mode: the same operations, faults and oracles on one thread and simulated time, every choice
    // from the seed, so that a failure replays from its seed alone; fast enough for many seeds. What happens within an
    // operation, between its threads, is InternalInterleavingTest's
    // every store profile in each run, cheap as it is: the threaded mode takes one per run (-Dinmemory.profile). Polled
    // by the core under jdbc's; a gap or an interruption of a feed under the others' (found: on jdbc's alone, the
    // default, no injected gap had a feed to interrupt, and a gap not revalidating went unnoticed)
    @ParameterizedTest(name = "{0}, seed {1}")
    @MethodSource("profilesAndSeeds")
    void deterministically_the_store_stays_fresh_and_every_instance_converges_on_it(String profile, long seed)
            throws Exception {
        assertThat(runDeterministically(seed, StoreProfile.of(profile))).as("phases checked").isEqualTo(PHASES);
    }

    @ParameterizedTest(name = "{0}, seed {1}")
    @MethodSource("profilesAndSeeds")
    void deterministically_with_size_pruning_while_the_instances_work_too(String profile, long seed) throws Exception {
        pruning = true;
        assertThat(runDeterministically(seed, StoreProfile.of(profile))).as("phases checked").isEqualTo(PHASES);
    }

    // a run replayed: the same seed, the same history, event by event (a profile with a feed of its own, its faults
    // and its deliveries)
    @Test
    void a_deterministic_run_replays_from_its_seed() throws Exception {
        InternalPropertyTest first = new InternalPropertyTest();
        InternalPropertyTest second = new InternalPropertyTest();
        first.runDeterministically(7, StoreProfile.REDIS);
        second.runDeterministically(7, StoreProfile.REDIS);

        assertThat(List.copyOf(second.trace)).isEqualTo(List.copyOf(first.trace));
    }

    // 50 seeds per profile by default, -Dproperty.seeds for more
    static Stream<Arguments> profilesAndSeeds() {
        return StoreProfile.names().stream().flatMap(profile -> LongStream.rangeClosed(1,
                Long.getLong("property.seeds", 50)).mapToObj(seed -> Arguments.of(profile, seed)));
    }

    // the store on the simulation, the caches too, then the phases driven by one thread
    int runDeterministically(long seed, StoreProfile profile) throws Exception {
        SimulatedEnvironment simulated = new SimulatedEnvironment(seed);
        simulation = simulated;
        choices = new Random(seed);
        return run(seed, new InMemoryBackend(seed, profile, simulated), PHASES);
    }

    /**
     * Runs the test on a backend (public, so that the adapters' tests run it on their stores).
     *
     * @param seed    the seed of the operations and faults
     * @param backend the store
     * @param phases  the number of phases
     * @return the number of phases checked
     * @throws Exception if the test fails
     */
    public int run(long seed, PropertyBackend backend, int phases) throws Exception {
        int checked = 0;
        List<AutoCloseable> closeables = new ArrayList<>();
        ExecutorService threads = Executors.newCachedThreadPool();
        try {
            // a plain instance with a small L1 and bulk loads returning an extra key, one with population and negative
            // caching, and an asynchronous one
            for (int key = 0; key < KEYS; key++) {
                keysByHash.put(new InternalHasher<String>(null).wrap(key(key)).hash(), key(key));
            }
            // a small L1 in the threaded mode only: Caffeine's admission breaks ties with ThreadLocalRandom, seeded
            // per JVM and thread and not settable, so its evictions would differ between two deterministic runs of one
            // seed (found: the replay failed about one run in four); the threaded mode keeps evicting
            DistributedLoadingCache<String, String> plain = builder(backend, "plain")
                    .withCaffeine(caffeine -> caffeine.maximumSize(simulation == null ? 4 : KEYS * 2))
                    .build(new BulkSourceLoader());
            DistributedLoadingCache<String, String> populated = builder(backend, "populated").withDistributionMode(DistributionMode.UPDATE)
                    .withNegativeCaching(Duration.ofMinutes(1))
                    .build(new SourceLoader());
            DistributedAsyncLoadingCache<String, String> async = builder(backend, "async")
                    .buildAsync(new SourceLoader());
            closeables.add(plain);
            closeables.add(populated);
            closeables.add(async);
            List<DistributedLoadingCache<String, String>> instances = List.of(plain, populated, async.synchronous());

            Random random = new Random(seed);
            for (int phase = 0; phase < phases; phase++) {
                SimulatedEnvironment simulated = simulation;
                if (simulated != null) {
                    runPhase(random, backend, instances, async, simulated);
                    backend.heal();
                    awaitQuiescence(backend, instances, simulated);
                    check(seed, phase, instances);
                    checked++;
                    continue;
                }
                List<Future<?>> running = new ArrayList<>();
                for (DistributedLoadingCache<String, String> cache : instances) {
                    long writerSeed = random.nextLong();
                    long loaderSeed = random.nextLong();
                    running.add(threads.submit(() -> write(cache, new Random(writerSeed))));
                    running.add(threads.submit(() -> load(cache, async, new Random(loaderSeed))));
                }
                long faultSeed = random.nextLong();
                running.add(threads.submit(() -> backend.fault(new Random(faultSeed), this::trace)));
                for (Future<?> future : running) {
                    future.get(30, TimeUnit.SECONDS);
                }
                backend.heal();
                awaitQuiescence(backend, instances);
                check(seed, phase, instances);
                checked++;
            }
        } finally {
            threads.shutdownNow();
            backend.heal();
            for (AutoCloseable closeable : closeables) {
                closeable.close();
            }
            backend.close();
        }
        return checked;
    }

    private DistributedCaffeine<String, String> builder(PropertyBackend backend, String label) {
        Adapter<String, String> adapter = settings -> {
            Store opened = backend.open(settings);
            LatencyStore slowed = new LatencyStore(opened, label);
            // without a change feed: offered as storage and scan only, so that the core polls it
            return opened instanceof ChangeScan scan && !(opened instanceof ChangeFeed)
                    ? new PolledStore(slowed, scan, label) : slowed;
        };
        DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(adapter);
        SimulatedEnvironment simulated = simulation;
        if (simulated != null) {
            // everything on the simulation: the core's executor and Caffeine's direct, Caffeine's clock the simulated
            builder.withEnvironment(simulated).withStore(store -> store.withExecutor(Runnable::run))
                    .withCaffeine(caffeine -> caffeine.executor(Runnable::run).ticker(simulated::nanoTime));
        }
        return builder
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> (pruning ? store.withMaximumAge(LONG).withMaximumSize(KEYS / 2)
                        .withMaintenanceInterval(Duration.ofMillis(5)) : store.withMaximumAge(LONG)
                        .withMaximumSize(1_000)).withPollInterval(Duration.ofMillis(10)));
    }

    // writers: the source first, then the cache, under the key's lock; a failed cache call is retried, as an
    // application would, so that the cache ends where the source is

    private void write(DistributedLoadingCache<String, String> cache, Random random) {
        for (int i = 0; i < OPERATIONS; i++) {
            writeOnce(cache, random);
        }
    }

    // one operation of a writer
    private void writeOnce(DistributedLoadingCache<String, String> cache, Random random) {
        int key = random.nextInt(KEYS);
        double choice = random.nextDouble();
        if (choice < 0.08) {
            // under every key's lock: the conditional writes below update the source after the cache, and an
            // invalidateAll in between would let a load put the old source value back
            // the source changes broadly, then everything is invalidated: a load that read the source before
            // must not leave its value behind
            lockAll(() -> {
                source.forEach((k, value) -> beforeInvalidateAll
                        .computeIfAbsent(k, ignored -> ConcurrentHashMap.newKeySet()).add(value));
                for (int changed = 0; changed < KEYS; changed++) {
                    double change = random.nextDouble();
                    if (change < 0.5) {
                        setSource(key(changed), token(changed));
                    } else if (change < 0.7) {
                        setSource(key(changed), null);
                    }
                }
                retry(cache::invalidateAll);
                confirmed.clear();
            });
            return;
        }
        if (choice < 0.16) {
            int other = (key + 1 + random.nextInt(KEYS - 1)) % KEYS;
            lock(Math.min(key, other), Math.max(key, other), () -> {
                Map<String, String> values = Map.of(key(key), token(key), key(other), token(other));
                values.forEach(InternalPropertyTest.this::setSource);
                retry(() -> cache.putAll(values));
                confirmed.putAll(values);
            });
            return;
        }
        lock(key, key, () -> {
            String k = key(key);
            if (choice < 0.45) {
                String token = token(key);
                setSource(k, token);
                retry(() -> cache.put(k, token));
                confirmed.put(k, token);
            } else if (choice < 0.70) {
                // the source changes or loses the record, the cache is told by an invalidation
                if (random.nextBoolean()) {
                    setSource(k, token(key));
                } else {
                    setSource(k, null);
                }
                retry(() -> cache.invalidate(k));
                confirmed.remove(k);
            } else if (choice < 0.90) {
                // the atomic methods decide in the store; the source changes within the decision, before the
                // store is written: changed after it, a load reading the source in between would put the old
                // value back over the new one (a race of this model, not of the cache). A decision run again
                // after a conflict sets it again. Only compute, which writes in every run: an operation
                // conditional on presence (replace, computeIfPresent) may find the key present in one run and
                // pruned or hidden in the next, leaving the source as the earlier run set it; those are covered
                // by the race and parity tests
                String token = token(key);
                retry(() -> cache.asMap().compute(k, (ignored, current) -> {
                    setSource(k, token);
                    return token;
                }));
                confirmed.put(k, token);
            } else {
                setSource(k, null);
                retry(() -> cache.asMap().remove(k));
                confirmed.remove(k);
            }
        });
    }

    // loaders: every way of reading; results are not checked here (the phase check decides)

    private void load(DistributedLoadingCache<String, String> cache, DistributedAsyncLoadingCache<String, String> async,
                      Random random) {
        for (int i = 0; i < OPERATIONS; i++) {
            loadOnce(cache, async, random);
        }
    }

    // one operation of a loader; its results are not checked here (the phase check decides)
    @SuppressWarnings({"CheckReturnValue", "ReturnValueIgnored", "ResultOfMethodCallIgnored"})
    private void loadOnce(DistributedLoadingCache<String, String> cache,
                          DistributedAsyncLoadingCache<String, String> async, Random random) {
        String k = key(random.nextInt(KEYS));
        double choice = random.nextDouble();
        try {
            if (choice < 0.30) {
                cache.get(k);
            } else if (choice < 0.45) {
                cache.getAll(List.of(k, key(random.nextInt(KEYS)), key(random.nextInt(KEYS))));
            } else if (choice < 0.60) {
                cache.getIfPresent(k);
            } else if (choice < 0.70) {
                cache.refresh(k).get(10, TimeUnit.SECONDS);
            } else if (choice < 0.78) {
                cache.asMap().containsKey(k);
            } else if (choice < 0.86) {
                cache.get(k, ignored -> readSource(k));
            } else if (choice < 0.93) {
                async.get(k).get(10, TimeUnit.SECONDS);
            } else {
                async.getIfPresent(k).get(10, TimeUnit.SECONDS);
            }
        } catch (StoreException e) {
            // an injected fault; the phase check decides
        } catch (Exception e) {
            if (!(e.getCause() instanceof StoreException)) {
                throw new AssertionError("load failed", e);
            }
        }
    }

    // one phase on one thread: each instance's writer and loader take a step at a time, in an order drawn from the seed,
    // the simulated time moving a little between steps (deliveries, polls, maintenance, resumes run there), and the
    // fault at a step drawn too
    private void runPhase(Random random, PropertyBackend backend, List<DistributedLoadingCache<String, String>> instances,
                          DistributedAsyncLoadingCache<String, String> async, SimulatedEnvironment simulated) {
        List<Runnable> steps = new ArrayList<>();
        List<Integer> remaining = new ArrayList<>();
        for (DistributedLoadingCache<String, String> cache : instances) {
            Random writer = new Random(random.nextLong());
            Random loader = new Random(random.nextLong());
            steps.add(() -> writeOnce(cache, writer));
            remaining.add(OPERATIONS);
            steps.add(() -> loadOnce(cache, async, loader));
            remaining.add(OPERATIONS);
        }
        Random fault = new Random(random.nextLong());
        Random order = new Random(random.nextLong());
        int total = steps.size() * OPERATIONS;
        int faultAt = order.nextInt(total);
        for (int step = 0; step < total; step++) {
            int actor;
            do {
                actor = order.nextInt(steps.size());
            } while (remaining.get(actor) == 0);
            remaining.set(actor, remaining.get(actor) - 1);
            steps.get(actor).run();
            // the fault after the step, before the time moves: what the step wrote is still undelivered, so a gap or an
            // interruption loses it (found: injected before a step, after the last advance had delivered everything,
            // a gap lost nothing, and a gap not revalidating went unnoticed)
            if (step == faultAt) {
                backend.fault(fault, this::trace);
            }
            simulated.advance(Duration.ofMillis(order.nextInt(5)));
        }
    }

    // on simulated time: the time moved until everything is delivered and processed, three checks in a row. Steps of
    // 100 ms: a resume after an interruption backs off for simulated seconds, which steps of 10 ms checked a hundred
    // times each (found: half the runs on a feed took two seconds instead of a fifth)
    private static void awaitQuiescence(PropertyBackend backend, List<DistributedLoadingCache<String, String>> instances,
                                        SimulatedEnvironment simulated) {
        int quiet = 0;
        for (int step = 0; quiet < 3; step++) {
            if (step == 3_000) {
                throw new AssertionError("delivery did not come to rest in 5 simulated minutes");
            }
            simulated.advance(Duration.ofMillis(100));
            boolean idle = instances.stream().allMatch(cache -> cache.distributedPolicy().getSynchronizationState()
                    == SynchronizationState.SYNCHRONIZED) && backend.isIdle() && instances.stream().allMatch(cache ->
                    ((InternalDistributedLoadingCache<String, String>) cache).core.isQuiescent());
            quiet = idle ? quiet + 1 : 0;
        }
    }

    // the store has delivered everything, every instance has processed it, and every subscription is up again
    private static void awaitQuiescence(PropertyBackend backend, List<DistributedLoadingCache<String, String>> instances) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        int quiet = 0;
        while (quiet < 3) {
            boolean idle = instances.stream().allMatch(cache -> cache.distributedPolicy().getSynchronizationState()
                    == SynchronizationState.SYNCHRONIZED) && backend.isIdle() && instances.stream().allMatch(cache ->
                    ((InternalDistributedLoadingCache<String, String>) cache).core.isQuiescent());
            quiet = idle ? quiet + 1 : 0;
            if (System.nanoTime() > deadline) {
                throw new AssertionError("delivery did not come to rest");
            }
            pause(2);
        }
    }

    private void check(long seed, int phase, List<DistributedLoadingCache<String, String>> instances) {
        List<String> violations = new ArrayList<>();
        // with nothing running, the reservation registries are empty (a release runs on the store executor right
        // after the last future completed, so waited for briefly): a leak would grow without bound
        for (DistributedLoadingCache<String, String> instance : instances) {
            InternalCore<?, ?> core = ((InternalDistributedCache<?, ?>) instance).core;
            if (!(simulation == null ? awaitUpTo(() -> core.reservedKeys() == 0, Duration.ofSeconds(5),
                    Duration.ofMillis(10)) : core.reservedKeys() == 0)) {
                violations.add("leak: " + core.reservedKeys() + " keys still reserved with nothing running");
            }
        }
        for (int key = 0; key < KEYS; key++) {
            String k = key(key);
            StoredValue<String> stored = instances.get(0).distributedPolicy().getFromStore(k);
            String inStore = stored == null ? null : stored.value();
            String inSource = source.get(k);
            if (inStore != null && !inStore.equals(inSource)
                    && !beforeInvalidateAll.getOrDefault(k, Set.of()).contains(inStore)) {
                violations.add("freshness: the store holds " + inStore + " for " + k + ", the source " + inSource);
            }
            String written = confirmed.get(k);
            if (!pruning && written != null && !written.equals(inStore)) {
                violations.add("presence: the store holds " + inStore + " for " + k + ", the last confirmed write "
                        + written);
            }
            for (int instance = 0; instance < instances.size(); instance++) {
                String answer = instances.get(instance).getIfPresent(k);
                // pruned silently, a held copy stays: it is still the last value written, the source's, or one a load
                // racing an invalidateAll left behind (allowed by freshness as well)
                boolean prunedButHeld = pruning && inStore == null && answer != null && (answer.equals(inSource)
                        || beforeInvalidateAll.getOrDefault(k, Set.of()).contains(answer));
                if (!Objects.equals(answer, inStore) && !prunedButHeld) {
                    InternalCore<String, String> core = ((InternalDistributedLoadingCache<String, String>) instances.get(instance)).core;
                    violations.add("convergence: instance " + instance + " answers " + answer + " for " + k
                            + ", the store holds " + inStore + " (held: " + core.l1().asMap().get(core.wrap(k))
                            + ", absence remembered: " + core.remembersAbsence(k) + ")");
                }
            }
        }
        if (!violations.isEmpty()) {
            Matcher found = Pattern.compile(" for (k[0-9]+)").matcher(violations.get(0));
            String key = found.find() ? found.group(1) : "";
            List<String> history = trace.stream().filter(event -> event.contains(key + " ")
                    || event.contains(key + "=") || event.contains(key + "#") || event.contains("ALL")
                    || event.contains("FAULT")).toList();
            fail("seed " + seed + ", phase " + phase + ":\n  " + String.join("\n  ", violations)
                    + "\nhistory of " + key + ":\n  "
                    + String.join("\n  ", history.subList(Math.max(0, history.size() - 60), history.size())));
        }
    }

    // helpers

    private void setSource(String key, @Nullable String value) {
        trace("source " + key + " = " + value);
        if (value == null) {
            source.remove(key);
        } else {
            source.put(key, value);
        }
    }

    private static String key(int key) {
        return "k" + key;
    }

    private String token(int key) {
        return key(key) + "#" + tokens.incrementAndGet();
    }

    private void lock(int first, int second, Runnable action) {
        locks[first].lock();
        try {
            if (second != first) {
                locks[second].lock();
            }
            try {
                action.run();
            } finally {
                if (second != first) {
                    locks[second].unlock();
                }
            }
        } finally {
            locks[first].unlock();
        }
    }

    private void lockAll(Runnable action) {
        for (ReentrantLock lock : locks) {
            lock.lock();
        }
        try {
            action.run();
        } finally {
            for (ReentrantLock lock : locks) {
                lock.unlock();
            }
        }
    }

    private void retry(Runnable call) {
        for (int attempt = 0; ; attempt++) {
            try {
                call.run();
                return;
            } catch (StoreException e) {
                if (attempt == 20) {
                    throw e;
                }
                // the guard may be open after repeated faults: wait for its probe
                idle(Duration.ofMillis(attempt < 3 ? 1 : 200));
            }
        }
    }

    private @Nullable String readSource(String key) {
        latency(SOURCE_LATENCY_NANOS);
        String value = source.get(key);
        trace("loader reads " + key + " = " + value);
        latency(SOURCE_LATENCY_NANOS);
        return value;
    }

    // a bulk loader over the source (the other instances load key by key): returns one extra key unasked
    // the source key by key; null where it holds nothing
    private final class SourceLoader implements CacheLoader<String, String> {

        @Override
        public @Nullable String load(String key) {
            return readSource(key);
        }
    }

    private final class BulkSourceLoader implements CacheLoader<String, String> {

        @Override
        public @Nullable String load(String key) {
            return readSource(key);
        }

        @Override
        public Map<String, String> loadAll(Set<? extends String> keys) {
            Map<String, String> result = new LinkedHashMap<>();
            for (String key : keys) {
                String value = readSource(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            String extra = key((simulation == null ? ThreadLocalRandom.current() : choices).nextInt(KEYS));
            String value = readSource(extra);
            if (value != null) {
                result.putIfAbsent(extra, value);
            }
            return result;
        }
    }

    // the reference store with all its faults, under the profile of the system property with its clock (the system's,
    // in its unit), and the faults the profile allows that the oracles hold under: the clock stepping back within its
    // allowance, other writes landing between the rows of a read, batches applied in part. The events that lose what
    // the oracles ask for, a failover, an eviction or a message lost silently, are tested on their own
    // (InternalStoreProfileTest)
    // polling: stores without a change feed, which the core polls (VISION.md, "The polling feed"); idle once every
    // store began two scans after the last heal, the second one after everything written before it
    private static final class InMemoryBackend implements PropertyBackend {

        private final InMemoryDatabase database;
        private final boolean polling;
        // the deterministic mode's simulation, null in the threaded mode
        private final @Nullable SimulatedEnvironment simulation;
        private final List<ScanOnlyStore> scanning = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final Map<ScanOnlyStore, Long> scansAtHeal = new ConcurrentHashMap<>();

        InMemoryBackend(long seed) {
            this(seed, false);
        }

        InMemoryBackend(long seed, boolean polling) {
            this.database = new InMemoryDatabase(() -> ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now()), seed,
                    StoreProfile.ofProperty());
            this.polling = polling;
            this.simulation = null;
        }

        // on the simulation, under the given profile: the store's clock and its deliveries; polled where the profile has
        // no feed. The clock follows the simulated time in microseconds
        // and moves at least a microsecond per reading, as a real one moves between two writes: the simulated time
        // stands still within a step, and writes stamped equal to an invalidateAll's control row would be hidden by
        // it (found: most seeds failed so, a confirmed write hidden)
        InMemoryBackend(long seed, StoreProfile profile, SimulatedEnvironment simulation) {
            java.util.concurrent.atomic.AtomicLong last = new java.util.concurrent.atomic.AtomicLong();
            this.database = new InMemoryDatabase(() -> last.updateAndGet(previous -> Math.max(previous + 1,
                    simulation.currentTimeMillis() * 1_000 + simulation.nanoTime() / 1_000 % 1_000)), seed, profile);
            this.database.simulate(simulation.forDatabase());
            this.polling = false;
            this.simulation = simulation;
        }

        // a fault held a moment: real time in the threaded mode, simulated in the deterministic one
        private void holdBriefly() {
            if (simulation == null) {
                pause(5);
            } else {
                simulation.advance(Duration.ofMillis(5));
            }
        }

        @Override
        public Store open(StoreSettings settings) {
            if (!polling) {
                return database.openStore("cache");
            }
            ScanOnlyStore store = new ScanOnlyStore(database.openFeed("cache"));
            scanning.add(store);
            return store;
        }

        @Override
        public void fault(Random random, java.util.function.Consumer<String> trace) {
            double choice = random.nextDouble();
            database.setReorderAcrossKeys(random.nextBoolean());
            database.setDuplicateProbability(random.nextDouble() < 0.3 ? 0.3 : 0);
            database.setDeliveryDelay(random.nextDouble() < 0.3 ? Duration.ofMillis(2) : Duration.ZERO);
            database.setCommitDelay(random.nextDouble() < 0.3 ? Duration.ofMillis(1) : Duration.ZERO, 0.3);
            StoreProfile profile = database.profile();
            if (!profile.snapshotReads() && simulation == null) {
                // a read row by row: other writes get a chance to land between its rows
                database.setBetweenRows(() -> LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(50_000)));
            }
            // a moment for the instances to go on, in the threaded mode; none in the deterministic one, where the
            // time moving would deliver what the step before wrote, and a gap would then lose nothing (found: a gap
            // not revalidating survived 1,000 seeds)
            int moment = random.nextInt(5);
            if (simulation == null) {
                pause(moment);
            }
            if (!profile.clockStepBack().isZero() && random.nextDouble() < 0.3) {
                // within its allowance, while the instances work: also under writes concurrent with an invalidateAll
                long micros = 1L + random.nextInt((int) (profile.clockStepBack().toNanos() / 1_000));
                trace.accept("FAULT clock steps back " + micros + " us");
                database.stepClockBack(Duration.ofNanos(micros * 1_000));
            }
            if (profile.batch() == StoreProfile.Batch.PER_ROW && random.nextDouble() < 0.2) {
                trace.accept("FAULT next batch applies in part");
                database.failNextWritesPartly(1);
            }
            if (choice < 0.15) {
                trace.accept("FAULT pause delivery");
                database.pauseDelivery();
                holdBriefly();
                database.resumeDelivery();
                trace.accept("FAULT resume delivery");
            } else if (choice < 0.30) {
                trace.accept("FAULT gap");
                database.gap();
            } else if (choice < 0.45) {
                boolean deliverMissed = random.nextBoolean();
                trace.accept("FAULT interrupt");
                database.interruptSubscriptions();
                holdBriefly();
                database.resumeSubscriptions(deliverMissed);
                trace.accept("FAULT resume, missed delivered " + deliverMissed);
            } else if (choice < 0.55) {
                boolean unknown = random.nextBoolean();
                trace.accept("FAULT next write fails, outcome unknown " + unknown);
                database.failNextWrites(1, unknown);
            } else if (choice < 0.65) {
                trace.accept("FAULT next read fails");
                database.failNextReads(1);
            }
        }

        @Override
        public void heal() {
            scanning.forEach(store -> scansAtHeal.put(store, store.scans()));
            database.failNextWrites(0, false);
            database.failNextReads(0);
            database.failNextWritesPartly(0);
            database.resumeDelivery();
            database.resumeSubscriptions(true);
            database.setDuplicateProbability(0);
            database.setDeliveryDelay(Duration.ZERO);
            database.setCommitDelay(Duration.ZERO, 0);
        }

        @Override
        public boolean isIdle() {
            return database.isIdle() && scanning.stream().allMatch(store ->
                    store.scans() >= scansAtHeal.getOrDefault(store, 0L) + 2);
        }
    }

    // latency before and after every call, so that the windows between a store call and what follows it are wide
    // enough to be hit (in memory they are nanoseconds otherwise)
    // storage with latency, and the scan of a store without a change feed, traced
    private final class PolledStore extends StorageOnlyStore implements ChangeScan {

        private final ChangeScan scan;
        private final String label;

        PolledStore(Store storage, ChangeScan scan, String label) {
            super(storage);
            this.scan = scan;
            this.label = label;
        }

        @Override
        public List<Change> changedWithin(Duration within) {
            List<Change> changes = scan.changedWithin(within);
            changes.forEach(change -> trace(label + " POLLED " + change.kind() + " "
                    + (Store.CONTROL_HASH.equals(change.hash()) ? "ALL"
                    : keysByHash.getOrDefault(change.hash(), "?")) + " @"
                    + change.version()));
            return changes;
        }
    }

    private final class LatencyStore extends InternalCoreTest.DelegatingStore {

        private final String label;

        LatencyStore(Store delegate, String label) {
            super(delegate);
            this.label = label;
        }

        private <T> T slow(String call, Supplier<T> action) {
            latency(STORE_LATENCY_NANOS);
            T result;
            try {
                result = action.get();
            } catch (RuntimeException e) {
                trace(label + " " + call + " FAILED " + e.getMessage());
                throw e;
            }
            trace(label + " " + call + " -> " + describe(result));
            latency(STORE_LATENCY_NANOS);
            return result;
        }

        private String describe(@Nullable Object result) {
            if (result instanceof Map<?, ?> map) {
                List<String> parts = new ArrayList<>();
                map.forEach((hash, value) -> parts.add(keysByHash.getOrDefault(String.valueOf(hash), "?") + "="
                        + (value instanceof Row row ? row.state() + "@" + row.version()
                        + (row.value() == null ? "" : ":" + new String(row.value(), java.nio.charset.StandardCharsets.UTF_8))
                        : String.valueOf(value))));
                return parts.toString();
            }
            return String.valueOf(result);
        }

        private String keys(Collection<String> hashes) {
            return hashes.stream().map(hash -> keysByHash.getOrDefault(hash, "?") + " ").toList().toString();
        }

        @Override
        public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
            return slow("read " + keys(hashes), () -> super.read(hashes, maxAge));
        }

        @Override
        public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
            return slow("readVersions " + keys(hashes), () -> super.readVersions(hashes, maxAge));
        }

        @Override
        public WriteResult write(Collection<Write> writes) {
            return slow("write " + writes.stream().map(write -> keysByHash.getOrDefault(write.hash(), "?") + "="
                    + (write.value() == null ? "TOMBSTONE"
                    : new String(write.value(), java.nio.charset.StandardCharsets.UTF_8)) + "@" + write.version()
                    + (write.isAdvance() ? "" : " if " + (write.expectsMissing() ? "missing" : write.expected())))
                    .toList(), () -> super.write(writes));
        }

        // what the instance receives, for the trace
        @Override
        public ChangeSubscription subscribe(
                ChangeListener listener) {
            return super.subscribe(new ChangeListener() {
                @Override
                public void onChanges(List<Change> messages) {
                    for (Change message : messages) {
                        trace(label + " RECEIVED " + message.kind() + " " + (Store.CONTROL_HASH.equals(message.hash()) ? "ALL "
                                : keysByHash.getOrDefault(message.hash(), "?") + " ") + "@" + message.version());
                    }
                    listener.onChanges(messages);
                }

                @Override
                public void onGap() {
                    trace(label + " RECEIVED GAP (ALL)");
                    listener.onGap();
                }

                @Override
                public void onLost(Throwable cause) {
                    trace(label + " RECEIVED LOST (ALL)");
                    listener.onLost(cause);
                }
            });
        }
    }
}
