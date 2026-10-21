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
import io.github.oberhoff.distributedcaffeine.spi.Entry;
import io.github.oberhoff.distributedcaffeine.spi.Expected;
import io.github.oberhoff.distributedcaffeine.spi.ExpectedEntry;
import io.github.oberhoff.distributedcaffeine.spi.ReadResult;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.ScanOnlyStore;
import io.github.oberhoff.distributedcaffeine.testing.StorageOnlyStore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
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
 *     current one. A stale load that slipped past a write (ordering rules 4 and 6) leaves an old token.</li>
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
 */
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
    // the pruning variant: a maximum size below the number of keys and maintenance every few milliseconds, so that
    // size pruning deletes rows silently while the instances work (presence then cannot be asked for)
    private boolean pruning;

    private void trace(String event) {
        trace.add(System.nanoTime() + " " + Thread.currentThread().getName() + " " + event);
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
        backend.database.setChecksBasis(false); // as MongoDB: writes decided under an older cut-off are only hidden
        assertThat(run(seed, backend, PHASES)).as("phases checked").isEqualTo(PHASES);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4})
    void with_size_pruning_while_the_instances_work_too(long seed) throws Exception {
        pruning = true;
        assertThat(run(seed, new InMemoryBackend(seed), PHASES)).as("phases checked").isEqualTo(PHASES);
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
            // a plain instance with a small L1 and bulk loads returning an extra key, one with population and absent
            // caching, and an asynchronous one
            for (int key = 0; key < KEYS; key++) {
                keysByHash.put(new InternalHasher<String>(null).wrap(key(key)).hash(), key(key));
            }
            DistributedLoadingCache<String, String> plain = builder(backend, "plain")
                    .withCaffeine(caffeine -> caffeine.maximumSize(4))
                    .build(new BulkSourceLoader());
            DistributedLoadingCache<String, String> populated = builder(backend, "populated").withDistributionMode(DistributionMode.UPDATE)
                    .withAbsentCaching(Duration.ofMinutes(1))
                    .build(new SourceLoader());
            DistributedAsyncLoadingCache<String, String> async = builder(backend, "async")
                    .buildAsync(new SourceLoader());
            closeables.add(plain);
            closeables.add(populated);
            closeables.add(async);
            List<DistributedLoadingCache<String, String>> instances = List.of(plain, populated, async.synchronous());

            Random random = new Random(seed);
            for (int phase = 0; phase < phases; phase++) {
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
        return DistributedCaffeine.newBuilder(adapter)
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
            int key = random.nextInt(KEYS);
            double choice = random.nextDouble();
            if (choice < 0.08) {
                // under every key's lock: the conditional writes below update the source after the cache, and an
                // invalidateAll in between would let a load put the old source value back
                // the source changes broadly, then everything is invalidated: a load that read the source before
                // must not leave its value behind
                lockAll(() -> {
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
                continue;
            }
            if (choice < 0.16) {
                int other = (key + 1 + random.nextInt(KEYS - 1)) % KEYS;
                lock(Math.min(key, other), Math.max(key, other), () -> {
                    Map<String, String> values = Map.of(key(key), token(key), key(other), token(other));
                    values.forEach(InternalPropertyTest.this::setSource);
                    retry(() -> cache.putAll(values));
                    confirmed.putAll(values);
                });
                continue;
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
    }

    // loaders: every way of reading; results are not checked here (the phase check decides)

    @SuppressWarnings({"CheckReturnValue", "ReturnValueIgnored", "ResultOfMethodCallIgnored"}) // see above
    private void load(DistributedLoadingCache<String, String> cache, DistributedAsyncLoadingCache<String, String> async,
                      Random random) {
        for (int i = 0; i < OPERATIONS; i++) {
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
        for (int key = 0; key < KEYS; key++) {
            String k = key(key);
            StoredValue<String> stored = instances.get(0).distributedPolicy().getFromStore(k);
            String inStore = stored == null ? null : stored.value();
            String inSource = source.get(k);
            if (inStore != null && !inStore.equals(inSource)) {
                violations.add("freshness: the store holds " + inStore + " for " + k + ", the source " + inSource);
            }
            String written = confirmed.get(k);
            if (!pruning && written != null && !written.equals(inStore)) {
                violations.add("presence: the store holds " + inStore + " for " + k + ", the last confirmed write "
                        + written);
            }
            for (int instance = 0; instance < instances.size(); instance++) {
                String answer = instances.get(instance).getIfPresent(k);
                // pruned silently, a held copy stays: it is still the last value written, the source's
                boolean prunedButHeld = pruning && inStore == null && answer != null && answer.equals(inSource);
                if (!Objects.equals(answer, inStore) && !prunedButHeld) {
                    InternalCore<String, String> core = ((InternalDistributedLoadingCache<String, String>) instances.get(instance)).core;
                    violations.add("convergence: instance " + instance + " answers " + answer + " for " + k
                            + ", the store holds " + inStore + " (held: " + core.l1().asMap().get(core.wrap(k))
                            + ", absent: " + core.isAbsent(k) + ")");
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

    private static void retry(Runnable call) {
        for (int attempt = 0; ; attempt++) {
            try {
                call.run();
                return;
            } catch (StoreException e) {
                if (attempt == 20) {
                    throw e;
                }
                // the guard may be open after repeated faults: wait for its probe
                pause(attempt < 3 ? 1 : 200);
            }
        }
    }

    private @Nullable String readSource(String key) {
        LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(SOURCE_LATENCY_NANOS));
        String value = source.get(key);
        trace("loader reads " + key + " = " + value);
        LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(SOURCE_LATENCY_NANOS));
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
            String extra = key(ThreadLocalRandom.current().nextInt(KEYS));
            String value = readSource(extra);
            if (value != null) {
                result.putIfAbsent(extra, value);
            }
            return result;
        }
    }

    // the reference store with all its faults
    // polling: stores without a change feed, which the core polls (VISION.md, "The polling feed"); idle once every
    // store began two scans after the last heal, the second one after everything written before it
    private static final class InMemoryBackend implements PropertyBackend {

        private final InMemoryDatabase database;
        private final boolean polling;
        private final List<ScanOnlyStore> scanning = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final Map<ScanOnlyStore, Long> scansAtHeal = new ConcurrentHashMap<>();

        InMemoryBackend(long seed) {
            this(seed, false);
        }

        InMemoryBackend(long seed, boolean polling) {
            this.database = new InMemoryDatabase(() -> TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()),
                    seed);
            this.polling = polling;
        }

        @Override
        public Store open(StoreSettings settings) {
            if (!polling) {
                return database.openStore("cache");
            }
            ScanOnlyStore store = new ScanOnlyStore(database.openStore("cache"));
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
            pause(random.nextInt(5));
            if (choice < 0.15) {
                trace.accept("FAULT pause delivery");
                database.pauseDelivery();
                pause(5);
                database.resumeDelivery();
                trace.accept("FAULT resume delivery");
            } else if (choice < 0.30) {
                trace.accept("FAULT gap");
                database.gap();
            } else if (choice < 0.45) {
                boolean deliverMissed = random.nextBoolean();
                trace.accept("FAULT interrupt");
                database.interruptSubscriptions();
                pause(5);
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
                    + (change.hash() == null ? "ALL" : keysByHash.getOrDefault(change.hash(), "?")) + " @"
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
            LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(STORE_LATENCY_NANOS));
            T result;
            try {
                result = action.get();
            } catch (RuntimeException e) {
                trace(label + " " + call + " FAILED " + e.getMessage());
                throw e;
            }
            trace(label + " " + call + " -> " + describe(result));
            LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(STORE_LATENCY_NANOS));
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
        public ReadResult read(Collection<String> hashes, Duration maxAge) {
            return slow("read " + keys(hashes), () -> super.read(hashes, maxAge));
        }

        @Override
        public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
            return slow("readVersions " + keys(hashes), () -> super.readVersions(hashes, maxAge));
        }

        @Override
        public Map<String, Long> put(Collection<Entry> entries) {
            return slow("put " + entries.stream().map(entry -> keysByHash.get(entry.hash()) + "="
                    + new String(entry.value(), java.nio.charset.StandardCharsets.UTF_8)).toList(), () -> super.put(entries));
        }

        @Override
        public Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge) {
            return slow("putIfUnchanged " + entries.stream().map(expected -> keysByHash.get(expected.entry().hash()) + "="
                    + new String(expected.entry().value(), java.nio.charset.StandardCharsets.UTF_8) + " if "
                    + expected.expectedVersion()).toList(), () -> super.putIfUnchanged(entries, maxAge));
        }

        @Override
        public Map<String, Long> invalidate(Collection<String> hashes) {
            return slow("invalidate " + keys(hashes), () -> super.invalidate(hashes));
        }

        @Override
        public Map<String, Long> invalidateIfUnchanged(Collection<Expected> expectations) {
        Map<String, Long> expected = Expected.byHash(expectations);
            return slow("invalidateIfUnchanged " + keys(expected.keySet()) + expected.values(),
                    () -> super.invalidateIfUnchanged(expectations));
        }

        @Override
        public long invalidateAll() {
            return slow("invalidateALL", super::invalidateAll);
        }

        // what the instance receives, for the trace
        @Override
        public ChangeSubscription subscribe(
                ChangeListener listener) {
            return super.subscribe(new ChangeListener() {
                @Override
                public void onChanges(List<Change> messages) {
                    for (Change message : messages) {
                        trace(label + " RECEIVED " + message.kind() + " " + (message.hash() == null ? "ALL"
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
