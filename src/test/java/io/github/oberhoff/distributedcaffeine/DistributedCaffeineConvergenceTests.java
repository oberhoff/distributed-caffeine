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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.CachedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractRepository;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractSynchronizer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntryMetadata;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_RETAINED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_SIZE_RETAINED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_TIME_RETAINED;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What every cache instance has to end up with, put under random interleavings of local operations and deliveries.
 * <p>
 * The underlying store is stood in for by the {@link InMemoryBroker} below, so the order cache entries are published
 * in and the moment each cache instance is handed one are decided here rather than by a change stream. That is what
 * makes this a safety net for the ordering rules in {@link InternalCacheManager}: a mistake in them shows up as a
 * failing seed in milliseconds, with the operations that caused it printed, instead of as a diverging stress test.
 * <p>
 * <b>Note:</b> the cache instances are built without a maximum size or expiry and with population distributed, which
 * is what makes the log an exact oracle: every operation is published, nothing is dropped by Caffeine on its own, so
 * replaying the log gives the one content all of them have to agree on. Invalidating all is deliberately left out -
 * it is applied to whatever a cache instance holds when the command reaches it rather than to the log, so the log
 * cannot say what it should have removed.
 */
@DisplayName("Distributed Caffeine Convergence Test Suite")
final class DistributedCaffeineConvergenceTests {

    private static final int INSTANCES = 3;
    private static final int KEYS = 8;
    private static final int STEPS = 300;

    @DisplayName("Test that cache instances converge on the published order under random interleavings")
    @ParameterizedTest(name = "with seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L})
    void test_convergence_under_random_interleavings(long seed) {
        Random random = new Random(seed);
        InMemoryBroker<Key, Value> broker = new InMemoryBroker<>();
        List<AbstractAdapter<Key, Value>> adapters = new ArrayList<>();
        List<DistributedCache<Key, Value>> caches = new ArrayList<>();
        for (int instance = 0; instance < INSTANCES; instance++) {
            AbstractAdapter<Key, Value> adapter = broker.newAdapter("in-memory-" + instance);
            adapters.add(adapter);
            caches.add(DistributedCaffeine.newBuilder(adapter)
                    .withDistributionMode(POPULATION_AND_INVALIDATION)
                    .build());
        }

        List<String> history = new ArrayList<>();
        try {
            for (int step = 0; step < STEPS; step++) {
                int instance = random.nextInt(INSTANCES);
                if (random.nextInt(100) < 55) {
                    history.add(operate(caches.get(instance), instance, random));
                } else {
                    int count = 1 + random.nextInt(3);
                    int delivered = broker.deliver(adapters.get(instance), count);
                    history.add(format("deliver %d to %d", delivered, instance));
                }
            }

            // everything published reaches everyone, which is where they have to agree
            broker.deliverAll();
            assertThat(broker.hasUndelivered()).isFalse();

            Map<Key, Value> expected = replay(broker.getLog());
            for (int instance = 0; instance < INSTANCES; instance++) {
                assertThat(caches.get(instance).asMap())
                        .describedAs("cache instance %d after%n  %s", instance, String.join("%n  ", history))
                        .containsExactlyInAnyOrderEntriesOf(expected);
            }
        } finally {
            caches.forEach(cache -> cache.distributedPolicy().stopSynchronization());
        }
    }

    @DisplayName("Test that reading the whole store back over a full cache loses nothing the store backs")
    @Test
    void test_restore_under_size_pressure_keeps_what_the_store_backs() {
        // A reactivated cache instance reads the whole store back, and every cache entry it applies is a write -
        // so a cache that is already full evicts while being restored. Whatever the data store still backs has to
        // survive that, and the point of the same-thread executor below is that Caffeine performs those evictions
        // inline, which makes the interleaving the same on every run instead of once in a few minutes.
        // Restoration is what makes a reactivation read the store at all, hence the persistence configuration
        int maximumSize = 2_000;
        InMemoryBroker<Key, Value> broker = new InMemoryBroker<>();
        AbstractAdapter<Key, Value> adapter = broker.newAdapter("in-memory-restore");
        DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
                // evictions have to be part of it, because cache residency alongside an eviction policy says
                // residency is a property of all the cache instances rather than of each one
                .withDistributionMode(POPULATION_AND_INVALIDATION_AND_EVICTION)
                .withCaffeine(Caffeine.newBuilder()
                        .executor(Runnable::run)
                        .maximumSize(maximumSize))
                .withPersistence(configurer -> configurer
                        .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                .build();
        try {
            // fill it to its maximum, so the store backs exactly what is held
            IntStream.rangeClosed(1, maximumSize).forEach(id ->
                    cache.put(Key.of(id), Value.of(id, "init")));
            broker.deliverAll();
            assertThat(cache.asMap()).hasSize(maximumSize);

            // stopped, then filled with other keys so that reading the store back has to write over a cache that
            // is already at its maximum - which is what makes the restore evict while it runs
            cache.distributedPolicy().stopSynchronization();
            IntStream.rangeClosed(maximumSize + 1, 2 * maximumSize).forEach(id ->
                    cache.put(Key.of(id), Value.of(id, "while-stopped")));

            // A clean up running alongside the restore, which is what the maintenance worker does: it calls
            // cache.cleanUp() without taking the synchronization lock, so its evictions land in the middle of the
            // restore. That is the only way an entry can disappear between being read and being marked
            AtomicBoolean cleaning = new AtomicBoolean(true);
            Thread cleaner = new Thread(() -> {
                while (cleaning.get()) {
                    cache.cleanUp();
                }
            });
            cleaner.start();
            try {
                cache.distributedPolicy().startSynchronization();
            } finally {
                cleaning.set(false);
                try {
                    cleaner.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // whatever the store still backs has to be held: an eviction while restoring transitions its own
            // record, so the store shrinks along with the cache - what must not happen is a key the store still
            // calls cached being gone here, because nothing reads the store again to notice
            assertThat(cache.asMap().keySet())
                    .describedAs("keys the store backs but the cache instance no longer holds")
                    .containsAll(broker.keysBackedByStore());
        } finally {
            cache.distributedPolicy().stopSynchronization();
        }
    }

    @DisplayName("Test that a stopped cache instance drops what was evicted elsewhere while it was not listening")
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L})
    void test_stopped_instance_drops_what_was_evicted_elsewhere(long seed) {
        // An eviction elsewhere is published while this cache instance is not taking part, so the cache entry
        // written for it never reaches it - and reading the data store back on reactivation is the only thing that
        // can tell it the key is gone, because what it still holds for it is of no activation any more. Which is
        // the shape of a divergence seen in the stress test: one cache instance serving what another one dropped
        int maximumSize = 4;
        InMemoryBroker<Key, Value> broker = new InMemoryBroker<>();
        AbstractAdapter<Key, Value> evictingAdapter = broker.newAdapter("in-memory-evicting-" + seed);
        AbstractAdapter<Key, Value> stoppedAdapter = broker.newAdapter("in-memory-stopped-" + seed);
        DistributedCache<Key, Value> evicting = restorableCache(evictingAdapter, maximumSize);
        DistributedCache<Key, Value> stopped = restorableCache(stoppedAdapter, maximumSize);
        try {
            IntStream.rangeClosed(1, maximumSize).forEach(id ->
                    evicting.put(Key.of(id), Value.of(id, "init")));
            broker.deliverAll();
            assertThat(stopped.asMap()).hasSize(maximumSize);

            // not listening from here on, so nothing published reaches it
            stopped.distributedPolicy().stopSynchronization();

            // one key over the maximum, which evicts another one and publishes that eviction
            evicting.put(Key.of(maximumSize + 1), Value.of(maximumSize + 1, "over-the-maximum"));
            evicting.cleanUp();
            broker.deliverAll();

            Set<Key> backedByStore = broker.keysBackedByStore();
            assertThat(backedByStore)
                    .describedAs("the eviction has to have taken the key out of the cached group")
                    .hasSizeLessThan(maximumSize + 1);

            // listening again: reading the data store back is what has to bring it in line, both by adding what
            // it missed and by dropping what the store no longer backs
            stopped.distributedPolicy().startSynchronization();
            broker.deliverAll();

            assertThat(stopped.asMap().keySet())
                    .describedAs("what the reactivated cache instance holds against what the store backs")
                    .containsExactlyInAnyOrderElementsOf(backedByStore);
        } finally {
            stopped.distributedPolicy().stopSynchronization();
            evicting.distributedPolicy().stopSynchronization();
        }
    }

    @DisplayName("Test that overlapping maintenance runs never prune evicted entries below their maximum size")
    @ParameterizedTest(name = "with seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L,
            21L, 22L, 23L, 24L, 25L, 26L, 27L, 28L, 29L, 30L, 31L, 32L})
    void test_pruning_by_size_under_overlapping_maintenance(long seed) throws Exception {
        // Every cache instance prunes the same records on its own schedule, so runs overlap, and between any two of
        // the store calls one of them makes, another one may make any of its own - which is where pruning by size
        // went wrong before, each run removing what a count it took earlier said was over. Here the runs are real
        // threads, but each store call waits for its turn and the turns are drawn from the seed, so an interleaving
        // that fails is the same one on every run of that seed. New evictions keep arriving meanwhile, and
        // timestamps are drawn from a few milliseconds only, so that many of them coincide - the case where which
        // records are the oldest is not decided by the order the store returns them in
        Random random = new Random(seed);
        int maximumSize = 1 + random.nextInt(4);
        int runs = 2 + random.nextInt(3);
        InMemoryBroker<Key, Value> broker = new InMemoryBroker<>();
        AbstractAdapter<Key, Value> adapter = broker.newAdapter("in-memory-pruning");
        DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
                .withCaffeine(Caffeine.newBuilder().maximumSize(1))
                .withPersistence(configurer -> configurer
                        .withEvictedEntries(evictedEntries -> evictedEntries.withMaximumSize(maximumSize)))
                .build();
        // stopped, so that the cache instance's own scheduled maintenance stays out of it - what is driven below
        // is the pruning step itself, which does not ask whether it is activated
        cache.distributedPolicy().stopSynchronization();
        Repository<Key, Value> repository = adapter.getRepository().orElseThrow();
        InternalMaintenanceWorker<Key, Value> maintenanceWorker =
                ((InternalDistributedCache<Key, Value>) cache).instanceRegistry.getMaintenanceWorker();
        Method pruning = InternalMaintenanceWorker.class.getDeclaredMethod("processEvictedEntryPersistenceBySize");
        pruning.setAccessible(true);
        java.lang.reflect.Field repositoryField = InternalMaintenanceWorker.class.getDeclaredField("repository");
        repositoryField.setAccessible(true);

        Instant base = Instant.now();
        int[] nextId = {0};
        java.util.function.IntFunction<CacheEntry<Key, Value>> evicted = millis -> {
            int id = nextId[0]++;
            return CacheEntry.of("h" + id, null, Key.of(id), Value.of(id),
                    random.nextBoolean() ? EVICTED_SIZE_RETAINED : EVICTED_TIME_RETAINED, base.plusMillis(millis));
        };
        int initial = maximumSize + 1 + random.nextInt(6);
        List<CacheEntry<Key, Value>> seeded = new ArrayList<>();
        for (int index = 0; index < initial; index++) {
            seeded.add(evicted.apply(random.nextInt(4)));
        }
        repository.publishCacheEntries(seeded);

        List<String> history = new ArrayList<>();
        Interleaving interleaving = new Interleaving();
        repositoryField.set(maintenanceWorker, interleaving.stepping(repository));
        List<Thread> threads = new ArrayList<>();
        List<Throwable> failures = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int run = 0; run < runs; run++) {
            Thread thread = new Thread(() -> {
                try {
                    pruning.invoke(maintenanceWorker);
                } catch (Throwable throwable) {
                    failures.add(throwable);
                } finally {
                    interleaving.finish();
                }
            }, Interleaving.RUN_PREFIX + run);
            threads.add(thread);
        }
        threads.forEach(Thread::start);
        try {
            int newest = 4;
            for (List<Thread> parked = interleaving.awaitQuiescence(runs); !parked.isEmpty();
                 parked = interleaving.awaitQuiescence(runs)) {
                if (random.nextInt(4) == 0) {
                    // newer than or as new as anything there, as evictions are
                    newest += random.nextInt(2);
                    repository.publishCacheEntries(List.of(evicted.apply(newest)));
                    history.add("evicted one more");
                }
                Thread next = parked.get(random.nextInt(parked.size()));
                history.add(next.getName() + " " + interleaving.pendingCallOf(next));
                interleaving.release(next);
                interleaving.awaitQuiescence(runs);
                assertThat(repository.countCacheEntries(EVICTED_RETAINED_GROUP, null))
                        .describedAs("retained evicted entries (maximum %d) after%n  %s", maximumSize,
                                String.join(System.lineSeparator() + "  ", history))
                        .isGreaterThanOrEqualTo(maximumSize);
            }
            for (Thread thread : threads) {
                thread.join();
            }
            assertThat(failures).isEmpty();

            // and on its own, a run prunes to exactly the maximum
            repositoryField.set(maintenanceWorker, repository);
            pruning.invoke(maintenanceWorker);
            assertThat(repository.countCacheEntries(EVICTED_RETAINED_GROUP, null))
                    .describedAs("retained evicted entries after a run on its own, after%n  %s",
                            String.join(System.lineSeparator() + "  ", history))
                    .isEqualTo(maximumSize);
        } finally {
            threads.forEach(Thread::interrupt);
        }
    }

    // Lets one thread at a time through to the store, and only the one the test picks: every call to the stepping
    // repository parks its thread until released, so between any two store calls of one run the test decides who
    // goes next
    private static final class Interleaving {

        private static final String RUN_PREFIX = "run-";

        private final Object lock = new Object();
        private final Map<Thread, String> parked = new LinkedHashMap<>();
        private int finished;

        @SuppressWarnings("unchecked")
        private <K, V> Repository<K, V> stepping(Repository<K, V> repository) {
            return (Repository<K, V>) java.lang.reflect.Proxy.newProxyInstance(Repository.class.getClassLoader(),
                    new Class<?>[]{Repository.class}, (proxy, method, arguments) -> {
                        // only the runs under test take turns: anything else reaching the store this way - the
                        // cache instance's own maintenance winding down, say - is not part of the interleaving
                        if (method.getDeclaringClass() != Object.class
                                && Thread.currentThread().getName().startsWith(RUN_PREFIX)) {
                            step(method.getName());
                        }
                        try {
                            return method.invoke(repository, arguments);
                        } catch (java.lang.reflect.InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }

        private void step(String call) throws InterruptedException {
            synchronized (lock) {
                Thread thread = Thread.currentThread();
                parked.put(thread, call);
                lock.notifyAll();
                while (parked.containsKey(thread)) {
                    lock.wait();
                }
            }
        }

        private void finish() {
            synchronized (lock) {
                finished++;
                lock.notifyAll();
            }
        }

        // the parked threads once every thread is either parked or done, in a stable order so that the seed alone
        // decides which one is picked
        private List<Thread> awaitQuiescence(int threads) throws InterruptedException {
            synchronized (lock) {
                while (parked.size() + finished < threads) {
                    lock.wait();
                }
                return parked.keySet().stream()
                        .sorted(Comparator.comparing(Thread::getName))
                        .toList();
            }
        }

        private String pendingCallOf(Thread thread) {
            synchronized (lock) {
                return parked.get(thread);
            }
        }

        private void release(Thread thread) {
            synchronized (lock) {
                parked.remove(thread);
                lock.notifyAll();
            }
        }
    }

    @DisplayName("Test that no key leaves a cache instance without a removal being reported for it")
    @Test
    // java:S2925 - the removal notifications are handed to an executor, and nothing reports when the last of them
    // has arrived, so there is no condition left to poll and only time separates "not yet delivered" from "never
    // delivered"
    @SuppressWarnings("java:S2925")
    void test_no_key_leaves_a_cache_instance_unreported() throws Exception {
        // Plain Caffeine accounts for every key under this workload (see the unit tests), so if a key can leave a
        // Distributed Caffeine instance with nothing reported for it, the difference is in what this library does
        // with Caffeine rather than in Caffeine. That is the shape of the residual still open on the stress test:
        // one key gone from a cache instance with no removal of any cause recorded for it
        int maximumSize = 200;
        int keys = 800;
        int rounds = 20;
        int operationsPerWorker = 1_000;
        int workers = 3;

        for (int round = 0; round < rounds; round++) {
            ExecutorService workerExecutor = Executors.newFixedThreadPool(workers + 1);
            Set<Key> touched = ConcurrentHashMap.newKeySet();
            Set<Key> removed = ConcurrentHashMap.newKeySet();
            InMemoryBroker<Key, Value> broker = new InMemoryBroker<>();
            AbstractAdapter<Key, Value> adapter = broker.newAdapter("in-memory-accounting-" + round);
            DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
                    .withDistributionMode(POPULATION_AND_INVALIDATION_AND_EVICTION)
                    .withCaffeine(Caffeine.newBuilder()
                            .maximumSize(maximumSize)
                            .removalListener((Key key, Value value, RemovalCause cause) -> {
                                if (cause != RemovalCause.REPLACED) {
                                    removed.add(key);
                                }
                            }))
                    .build();
            try {
                int currentRound = round;
                AtomicBoolean delivering = new AtomicBoolean(true);
                Future<?> deliverer = workerExecutor.submit(() -> {
                    while (delivering.get()) {
                        broker.deliverAll();
                    }
                });
                List<? extends Future<?>> running = IntStream.range(0, workers)
                        .mapToObj(worker -> workerExecutor.submit(() -> {
                            Random random = new Random(currentRound * 131L + worker);
                            for (int operation = 0; operation < operationsPerWorker; operation++) {
                                Key key = Key.of(random.nextInt(keys));
                                switch (random.nextInt(3)) {
                                    case 0 -> {
                                        touched.add(key);
                                        cache.put(key, Value.of(key.getId(), "put"));
                                    }
                                    case 1 -> cache.invalidate(key);
                                    default -> cache.getIfPresent(key);
                                }
                            }
                        }))
                        .toList();
                for (Future<?> future : running) {
                    future.get();
                }
                delivering.set(false);
                deliverer.get();
                broker.deliverAll();
                cache.cleanUp();
                broker.deliverAll();
                Thread.sleep(100);

                Set<Key> held = cache.asMap().keySet();
                Set<Key> unaccountedFor = touched.stream()
                        .filter(key -> !held.contains(key) && !removed.contains(key))
                        .collect(toCollection(LinkedHashSet::new));
                assertThat(unaccountedFor)
                        .describedAs("round %d: keys neither held nor reported as removed, out of %d touched",
                                round, touched.size())
                        .isEmpty();
            } finally {
                cache.distributedPolicy().stopSynchronization();
                workerExecutor.shutdownNow();
            }
        }
    }

    // a cache instance that reads the data store back when it is activated, which is what persistence of cached
    // entries with cache residency asks for - and evictions have to be distributed alongside it
    private DistributedCache<Key, Value> restorableCache(AbstractAdapter<Key, Value> adapter, int maximumSize) {
        return DistributedCaffeine.newBuilder(adapter)
                .withDistributionMode(POPULATION_AND_INVALIDATION_AND_EVICTION)
                .withCaffeine(Caffeine.newBuilder()
                        .executor(Runnable::run)
                        .maximumSize(maximumSize))
                .withPersistence(configurer -> configurer
                        .withCachedEntries(CachedEntryPersistenceConfigurer::withCacheResidency))
                .build();
    }

    // the content the published cache entries add up to: the last one written for a key decides, which is what the
    // underlying store ends up holding and therefore what every cache instance has to end up holding as well
    private Map<Key, Value> replay(List<CacheEntry<Key, Value>> log) {
        Map<Key, Value> replayed = new LinkedHashMap<>();
        log.forEach(cacheEntry -> {
            Key key = cacheEntry.getKey();
            if (cacheEntry.isCached()) {
                replayed.put(key, cacheEntry.getValue());
            } else {
                replayed.remove(key);
            }
        });
        return replayed;
    }

    private String operate(DistributedCache<Key, Value> cache, int instance, Random random) {
        Key key = Key.of(random.nextInt(KEYS));
        return switch (random.nextInt(4)) {
            case 0 -> {
                Value value = Value.of(key.getId(), "put-" + instance + "-" + random.nextInt(1_000));
                cache.put(key, value);
                yield format("%d put %s=%s", instance, key, value);
            }
            case 1 -> {
                Key otherKey = Key.of(random.nextInt(KEYS));
                Map<Key, Value> map = new HashMap<>();
                map.put(key, Value.of(key.getId(), "putAll-" + instance));
                map.put(otherKey, Value.of(otherKey.getId(), "putAll-" + instance));
                cache.putAll(map);
                yield format("%d putAll %s", instance, map.keySet());
            }
            case 2 -> {
                cache.invalidate(key);
                yield format("%d invalidate %s", instance, key);
            }
            default -> {
                cache.asMap().remove(key);
                yield format("%d remove %s", instance, key);
            }
        };
    }

    /**
     * A publisher and synchronizer pair standing in for an underlying store, for tests that need to decide when a
     * published cache entry reaches which cache instance.
     * <p>
     * Everything published lands in one ordered log, which is the order every cache instance observes as required of a
     * publisher - and nothing is handed on until a test asks for it. That is the whole point: the ordering a real store
     * decides by itself, and which no test can steer, is here a sequence of explicit steps, so an interleaving that
     * would take a stress run to stumble upon can be written down (see {@link #deliver}).
     */
    // null-marked like the adapter interfaces it implements, so the overrides below line up with them instead of
    // leaving their nullness unstated
    @NullMarked
    private static final class InMemoryBroker<K, V> {

        private final List<CacheEntry<K, V>> log;
        // what the store keeps, one record per key, which is exactly what a reactivation reads back
        private final Map<String, CacheEntry<K, V>> retained;
        private final List<InMemorySynchronizer<K, V>> synchronizers;

        private InMemoryBroker() {
            // both are written by whichever thread publishes and read by whichever thread delivers, and a test
            // may well drive those concurrently. The log is only ever appended to, so reading an index below its
            // size stays stable
            this.log = java.util.Collections.synchronizedList(new ArrayList<>());
            this.retained = new ConcurrentHashMap<>();
            this.synchronizers = new ArrayList<>();
        }

        /**
         * Returns an adapter on this broker, for one cache instance.
         */
        private AbstractAdapter<K, V> newAdapter(String identifier) {
            InMemorySynchronizer<K, V> synchronizer = new InMemorySynchronizer<>(this);
            synchronizers.add(synchronizer);
            return new InMemoryAdapter<>(new InMemoryRepository<>(this), synchronizer, identifier);
        }

        /**
         * Hands the next {@code count} cache entries of the log that this adapter has not seen yet to it, one at a
         * time and in the order they were published. Returns how many were actually handed over, which is less than
         * asked for once the adapter has caught up.
         */
        private int deliver(AbstractAdapter<K, V> adapter, int count) {
            return synchronizerOf(adapter).deliverNext(count);
        }

        /**
         * Hands every cache entry not seen yet to every adapter, which is what a test does before comparing what the
         * cache instances hold: whatever they end up with then is what they converge on.
         */
        private void deliverAll() {
            synchronizers.forEach(synchronizer -> synchronizer.deliverNext(Integer.MAX_VALUE));
        }

        /**
         * Returns whether any adapter still has cache entries waiting for it.
         */
        private boolean hasUndelivered() {
            return synchronizers.stream().anyMatch(synchronizer -> synchronizer.position < log.size());
        }

        /**
         * Returns the log as published, which is the order the cache instances have to agree on.
         */
        // the keys the store still backs as cached, which is what a cache instance has to hold after reading it
        // back - evictions transition their record away, so this shrinks as the restore evicts
        private Set<K> keysBackedByStore() {
            return retained.values().stream()
                    .filter(CacheEntry::isCached)
                    .map(CacheEntry::getKey)
                    .filter(java.util.Objects::nonNull)
                    .collect(java.util.stream.Collectors.toSet());
        }

        private List<CacheEntry<K, V>> getLog() {
            return List.copyOf(log);
        }

        private InMemorySynchronizer<K, V> synchronizerOf(AbstractAdapter<K, V> adapter) {
            return synchronizers.stream()
                    .filter(synchronizer -> synchronizer == ((InMemoryAdapter<K, V>) adapter).inMemorySynchronizer)
                    .findFirst()
                    .orElseThrow();
        }

        private void publish(Collection<CacheEntry<K, V>> cacheEntries) {
            log.addAll(cacheEntries);
        }

        private static final class InMemoryAdapter<K, V> extends AbstractAdapter<K, V> {

            private final InMemorySynchronizer<K, V> inMemorySynchronizer;

            private InMemoryAdapter(InMemoryRepository<K, V> publisher, InMemorySynchronizer<K, V> synchronizer,
                                    String identifier) {
                super(publisher, synchronizer, identifier);
                this.inMemorySynchronizer = synchronizer;
            }
        }

        // A repository rather than a plain publisher, so that what is published is also retained and can be read
        // back - which is what lets a test drive a reactivation, the one path that reads the whole store again
        private static final class InMemoryRepository<K, V> extends AbstractRepository<K, V> {

            private final InMemoryBroker<K, V> broker;

            private InMemoryRepository(InMemoryBroker<K, V> broker) {
                this.broker = broker;
            }

            @Override
            public void publishCacheEntries(Collection<CacheEntry<K, V>> cacheEntries) {
                // retained under the same uniqueness a real store enforces, the discriminator and the hash - one
                // record per key, so publishing the same key again replaces what was there
                cacheEntries.forEach(cacheEntry -> broker.retained.put(cacheEntry.getHash(), cacheEntry));
                broker.publish(cacheEntries);
            }

            @Override
            public Stream<CacheEntry<K, V>> streamCacheEntries(@Nullable Set<String> hashes,
                                                               @Nullable Set<Status> statuses,
                                                               Order order) {
                return matching(hashes, statuses, null, order);
            }

            @Override
            public Stream<CacheEntryMetadata> streamCacheEntryMetadata(@Nullable Set<String> hashes,
                                                                       @Nullable Set<Status> statuses,
                                                                       Order order) {
                return matching(hashes, statuses, null, order)
                        .map(cacheEntry -> CacheEntryMetadata.of(cacheEntry.getHash(), cacheEntry.getOperation(),
                                cacheEntry.getStatus(), cacheEntry.getTimestamp()));
            }

            @Override
            public void updateStatusOfCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                                   @Nullable Instant olderThan, Status newStatus) {
                matching(hashes, statuses, olderThan, Order.UNORDERED)
                        .toList()
                        .forEach(cacheEntry -> broker.retained.put(cacheEntry.getHash(), CacheEntry.of(
                                cacheEntry.getHash(), null, cacheEntry.getKey(), cacheEntry.getValue(),
                                newStatus, Instant.now())));
            }

            @Override
            public void deleteCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                           @Nullable Instant olderThan) {
                matching(hashes, statuses, olderThan, Order.UNORDERED)
                        .toList()
                        .forEach(cacheEntry -> broker.retained.remove(cacheEntry.getHash()));
            }

            @Override
            public long countCacheEntries(@Nullable Set<Status> statuses, @Nullable Instant notOlderThan) {
                return matching(null, statuses, null, Order.UNORDERED)
                        .filter(cacheEntry -> isNull(notOlderThan)
                                || !cacheEntry.getTimestamp().isBefore(notOlderThan))
                        .count();
            }

            private Stream<CacheEntry<K, V>> matching(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                                      @Nullable Instant olderThan, Order order) {
                Stream<CacheEntry<K, V>> matching = List.copyOf(broker.retained.values()).stream()
                        .filter(cacheEntry -> isNull(hashes) || hashes.contains(cacheEntry.getHash()))
                        .filter(cacheEntry -> isNull(statuses) || statuses.contains(cacheEntry.getStatus()))
                        .filter(cacheEntry -> isNull(olderThan) || cacheEntry.getTimestamp().isBefore(olderThan));
                Comparator<CacheEntry<K, V>> oldestFirst = Comparator.comparing(CacheEntry::getTimestamp);
                return switch (order) {
                    case ASCENDING -> matching.sorted(oldestFirst);
                    case DESCENDING -> matching.sorted(oldestFirst.reversed());
                    case UNORDERED -> matching;
                };
            }
        }

        private static final class InMemorySynchronizer<K, V> extends AbstractSynchronizer<K, V> {

            private final InMemoryBroker<K, V> broker;
            private int position;
            private boolean activated;

            private InMemorySynchronizer(InMemoryBroker<K, V> broker) {
                this.broker = broker;
            }

            @Override
            public void activate() {
                // caught up with whatever was published while this cache instance was not taking part, the way a
                // watcher started now sees only what follows
                this.position = broker.log.size();
                this.activated = true;
            }

            @Override
            public void deactivate() {
                this.activated = false;
            }

            @Override
            public boolean isActivated() {
                return activated;
            }

            // handing the cache entries over from here rather than from the broker, because the receiver an adapter is
            // wired up with is the synchronizer's own
            private int deliverNext(int count) {
                int delivered = 0;
                while (delivered < count && position < broker.log.size()) {
                    CacheEntry<K, V> cacheEntry = broker.log.get(position);
                    position++;
                    delivered++;
                    if (activated) {
                        receiver.receiveCacheEntries(List.of(cacheEntry));
                    }
                }
                return delivered;
            }
        }
    }
}
