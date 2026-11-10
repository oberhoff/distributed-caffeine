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
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Policy.VarExpiration;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.StoreProfile;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The same situation through every path gives the same answer: reads, writes and removals, single key and bulk,
 * synchronous and asynchronous, the cache's methods and {@code asMap()}, a loader and a mapping function. The paths
 * share the core's rules but not all of its code, and a rule kept by one path and missed by another is invisible to
 * tests that look at one path at a time (found: a bulk load that gave up left the key out of its result, where a single
 * one returns the value; bulk loads skipping the refresh of an old value; bulk and asynchronous reads not checking a
 * copy while the store guard was open; a removal through an atomic {@code asMap()} method keeping the copy the store
 * refused to remove). Each case is one path in one situation, on a cache of its own, in each distribution mode and on
 * the store profile of the run.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(30)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
@ParameterizedClass(name = "{0}")
@MethodSource("configurations")
class InternalPathParityTest {

    private static final Duration LONG = Duration.ofHours(1);
    private static final String KEY = "k";
    // a value the serializer refuses to read, as one written by an instance with another serializer
    private static final String UNREADABLE = "unreadable";
    // the default asMap().getOrDefault answers with where nothing is found
    private static final String ABSENT = "absent";

    // the profile of the suite's run (the nightly matrix runs each), in every distribution mode: the mode decides
    // what happens to the other instances' copies, the profile how changes reach them
    private final InMemoryDatabase database = new InMemoryDatabase(StoreProfile.ofProperty());
    private final DistributionMode mode;
    private final boolean maximumAgeForStoreOnly;
    private final boolean weighedWithVariableExpiry;
    private final CacheBackend backend = CacheBackend.of(database);
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final ExecutorService background = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "test-background");
        thread.setDaemon(true);
        return thread;
    });

    // the source behind every path: what it answers is set per situation, and its calls are counted
    private final AtomicInteger loads = new AtomicInteger();
    private volatile Function<String, @Nullable String> source = key -> "loaded";

    private @Nullable DistributedLoadingCache<String, String> sync;
    private @Nullable DistributedAsyncLoadingCache<String, String> async;
    // another instance, which writes what the instance under test reads
    private @Nullable DistributedCache<String, String> elsewhere;
    private @Nullable Duration negativeCaching;
    // a key the bulk source returns unasked, if any
    private volatile @Nullable String extraKey;
    // what the removal listener of the instance under test reported, as "key=value CAUSE"
    private final Queue<String> removals = new ConcurrentLinkedQueue<>();

    InternalPathParityTest(Configuration configuration) {
        this.mode = configuration.mode();
        this.maximumAgeForStoreOnly = configuration.maximumAgeForStoreOnly();
        this.weighedWithVariableExpiry = configuration.weighedWithVariableExpiry();
    }

    // every distribution mode; the maximum age for the store only (which needs distribution), where the atomic
    // methods read the store instead of deciding on a held copy; and the local level bounded by weight and expiring
    // variably, which Caffeine runs through other code and which offers policy().expireVariably()
    static List<Configuration> configurations() {
        return List.of(new Configuration(DistributionMode.INVALIDATE, false, false),
                new Configuration(DistributionMode.UPDATE, false, false),
                new Configuration(DistributionMode.NONE, false, false),
                new Configuration(DistributionMode.INVALIDATE, true, false),
                new Configuration(DistributionMode.UPDATE, true, false),
                new Configuration(DistributionMode.INVALIDATE, false, true));
    }

    record Configuration(DistributionMode mode, boolean maximumAgeForStoreOnly, boolean weighedWithVariableExpiry) {

        @Override
        public String toString() {
            return mode + (maximumAgeForStoreOnly ? ", maximum age for the store only" : "")
                    + (weighedWithVariableExpiry ? ", weighed, variable expiry" : "");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        backend.close();
        background.shutdownNow();
    }

    // the paths that load on a miss

    static List<Path> loadPaths() {
        return List.of(
                new Path("get(key, function)", false, (test, key) -> test.sync().get(key, test::load)),
                new Path("LoadingCache.get(key)", false, (test, key) -> test.sync().get(key)),
                new Path("getAll(keys, function)", false, (test, key) ->
                        test.sync().getAll(List.of(key), test::loadAll).get(key)),
                new Path("LoadingCache.getAll(keys)", false, (test, key) ->
                        test.sync().getAll(List.of(key)).get(key)),
                new Path("asMap().computeIfAbsent", false, (test, key) ->
                        test.sync().asMap().computeIfAbsent(key, test::load)),
                new Path("async get(key, function)", true, (test, key) -> test.async().get(key, test::load).join()),
                new Path("async get(key, bifunction)", true, (test, key) -> test.async().get(key, (k, executor) ->
                        CompletableFuture.supplyAsync(() -> test.load(k), executor)).join()),
                new Path("AsyncLoadingCache.get(key)", true, (test, key) -> test.async().get(key).join()),
                new Path("async getAll(keys, function)", true, (test, key) ->
                        test.async().getAll(List.of(key), test::loadAll).join().get(key)),
                new Path("async getAll(keys, bifunction)", true, (test, key) ->
                        test.async().getAll(List.of(key), (keys, executor) ->
                                CompletableFuture.supplyAsync(() -> test.loadAll(keys), executor)).join().get(key)),
                new Path("AsyncLoadingCache.getAll(keys)", true, (test, key) ->
                        test.async().getAll(List.of(key)).join().get(key)),
                new Path("async asMap().computeIfAbsent", true, (test, key) -> test.async().asMap()
                        .computeIfAbsent(key, k -> CompletableFuture.supplyAsync(() -> test.load(k))).join()));
    }

    // the paths that never load: a miss is read through the store only

    static List<Path> lookupPaths() {
        return List.of(
                new Path("getIfPresent", false, (test, key) -> test.sync().getIfPresent(key)),
                new Path("getAllPresent", false, (test, key) -> test.sync().getAllPresent(List.of(key)).get(key)),
                new Path("asMap().get", false, (test, key) -> test.sync().asMap().get(key)),
                new Path("asMap().getOrDefault", false, (test, key) -> {
                    String value = test.sync().asMap().getOrDefault(key, ABSENT);
                    return ABSENT.equals(value) ? null : value;
                }),
                new Path("async getIfPresent", true, (test, key) -> test.async().getIfPresent(key).join()),
                new Path("async asMap().get", true, (test, key) -> {
                    CompletableFuture<String> future = test.async().asMap().get(key);
                    return future == null ? null : future.join();
                }));
    }

    // the synchronous lookups: the asynchronous ones return the future of a load in flight, by design
    static List<Path> synchronousLookupPaths() {
        return lookupPaths().stream().filter(path -> !path.async()).toList();
    }

    // one way of reading a key, on the synchronous or the asynchronous cache
    record Path(String name, boolean async, Read read) {

        @Override
        public String toString() {
            return name;
        }
    }

    @FunctionalInterface
    interface Read {
        @Nullable String read(InternalPathParityTest test, String key);
    }

    // loading paths

    @ParameterizedTest
    @MethodSource("loadPaths")
    void a_value_in_the_store_is_returned_without_a_load(Path path) {
        open(path.async(), null);
        putElsewhere("stored");

        assertThat(path.read().read(this, KEY)).isEqualTo("stored");

        assertThat(loads).hasValue(0);
        await(() -> "stored".equals(held(path.async())));
        // the store's activity, counted alike on every path
        DistributedStats stats = policy(path.async()).stats();
        assertThat(stats.storeHitCount()).isOne();
        assertThat(stats.storeMissCount()).isZero();
        assertThat(stats.sourceLoadCount()).isZero();
        assertThat(stats.conditionalWriteCount()).isZero();
    }

    @ParameterizedTest
    @MethodSource("loadPaths")
    void a_miss_is_loaded_written_and_held(Path path) {
        open(path.async(), null);

        assertThat(path.read().read(this, KEY)).isEqualTo("loaded");

        DistributedStats stats = policy(path.async()).stats();
        assertThat(stats.storeHitCount()).isZero();
        assertThat(stats.storeMissCount()).isOne();
        assertThat(stats.sourceLoadCount()).isOne();
        assertThat(stats.conditionalWriteCount()).isOne();
        assertThat(loads).hasValue(1);
        assertThat(stored(path.async())).isEqualTo("loaded");
        await(() -> "loaded".equals(held(path.async())));
    }

    @ParameterizedTest
    @MethodSource("loadPaths")
    void a_source_without_a_value_leaves_the_key_absent(Path path) {
        open(path.async(), null);
        source = key -> null;

        assertThat(path.read().read(this, KEY)).isNull();

        assertThat(loads).hasValue(1);
        assertThat(stored(path.async())).isNull();
        assertThat(held(path.async())).isNull();
    }

    @ParameterizedTest
    @MethodSource("loadPaths")
    void a_failing_source_fails_the_call_and_leaves_the_key_absent(Path path) {
        open(path.async(), null);
        source = key -> {
            throw new SourceFailure();
        };

        Read read = path.read();
        assertThatThrownBy(() -> read.read(this, KEY)).satisfies(failure ->
                assertThat(causes(failure)).anyMatch(SourceFailure.class::isInstance));

        assertThat(stored(path.async())).isNull();
        assertThat(held(path.async())).isNull();
    }

    @ParameterizedTest
    @MethodSource("loadPaths")
    void while_the_store_does_not_answer_the_source_answers_and_nothing_is_written(Path path) {
        open(path.async(), null);
        database.setDown(true);

        assertThat(path.read().read(this, KEY)).isEqualTo("loaded");

        assertThat(loads).hasValue(1);
        database.setDown(false);
        assertThat(stored(path.async())).isNull();
    }

    @ParameterizedTest
    @MethodSource("loadPaths")
    void a_removal_during_every_attempt_returns_the_last_value_uncached(Path path) {
        open(path.async(), null);
        Store other = store();
        InternalVersion otherClock = new InternalVersion();
        source = key -> {
            // another instance, with a clock of its own: above what it finds, as its write is refused otherwise
            Row row = other.read(List.of(hash(key)), LONG).get(hash(key));
            other.write(List.of(Write.tombstone(hash(key), row == null ? otherClock.fresh()
                    : otherClock.next(row.version()))));
            return "loaded";
        };

        assertThat(path.read().read(this, KEY)).isEqualTo("loaded");

        await(() -> held(path.async()) == null);
        assertThat(policy(path.async()).stats().uncachedLoadCount()).isOne();
        assertThat(stored(path.async())).isNull();
    }

    @ParameterizedTest
    @MethodSource("loadPaths")
    void a_write_elsewhere_during_the_load_wins(Path path) {
        open(path.async(), null);
        DistributedCache<String, String> other = other();
        source = key -> {
            other.put(key, "written");
            return "loaded";
        };

        assertThat(path.read().read(this, KEY)).isEqualTo("written");

        assertThat(stored(path.async())).isEqualTo("written");
        await(() -> "written".equals(held(path.async())));
    }

    @ParameterizedTest
    @MethodSource("loadPaths")
    void an_unreadable_value_is_loaded_again_and_replaced(Path path) {
        open(path.async(), null);
        store().write(List.of(Write.value(hash(KEY), KEY.getBytes(StandardCharsets.UTF_8),
                UNREADABLE.getBytes(StandardCharsets.UTF_8), new InternalVersion().fresh())));
        assertThat(stored(path.async())).as("read directly too, it counts as none").isNull();

        assertThat(path.read().read(this, KEY)).isEqualTo("loaded");

        assertThat(loads).hasValue(1);
        assertThat(stored(path.async())).isEqualTo("loaded");
    }

    @ParameterizedTest
    @MethodSource("loadPaths")
    void a_value_read_from_the_store_beyond_the_refresh_duration_is_refreshed_at_once(Path path) {
        open(path.async(), Duration.ofMillis(50));
        putElsewhere("stored");
        pause(100);

        assertThat(path.read().read(this, KEY)).isEqualTo("stored");

        // refresh rule 2: in the background, without a further access
        await(() -> "loaded".equals(stored(path.async())));
        assertThat(loads.get()).isPositive();
    }

    @ParameterizedTest
    @MethodSource("loadPaths")
    void an_unconfirmed_copy_is_checked_while_the_guard_is_open(Path path) {
        open(path.async(), null);
        putElsewhere("old");
        assertThat(path.read().read(this, KEY)).isEqualTo("old");
        changeUnseenAndOpenGuard(path.async());

        assertThat(path.read().read(this, KEY)).as("checked as the guard's probe").isEqualTo("new");
    }

    // lookup paths

    @ParameterizedTest
    @MethodSource("lookupPaths")
    void a_lookup_returns_a_value_in_the_store_and_holds_it(Path path) {
        open(path.async(), null);
        putElsewhere("stored");

        assertThat(path.read().read(this, KEY)).isEqualTo("stored");
        assertThat(policy(path.async()).stats().storeHitCount()).isOne();
        assertThat(containsKey(path.async())).isTrue();

        assertThat(loads).hasValue(0);
        await(() -> "stored".equals(held(path.async())));
    }

    @ParameterizedTest
    @MethodSource("lookupPaths")
    void a_lookup_of_a_key_nowhere_returns_nothing_and_never_loads(Path path) {
        open(path.async(), null);

        assertThat(path.read().read(this, KEY)).isNull();
        assertThat(policy(path.async()).stats().storeMissCount()).isOne();
        assertThat(containsKey(path.async())).isFalse();

        assertThat(loads).hasValue(0);
        assertThat(held(path.async())).isNull();
    }

    @ParameterizedTest
    @MethodSource("lookupPaths")
    void a_lookup_while_the_store_does_not_answer_is_a_miss(Path path) {
        open(path.async(), null);
        putElsewhere("stored");
        database.setDown(true);

        assertThat(path.read().read(this, KEY)).isNull();
        assertThat(containsKey(path.async())).isFalse();

        assertThat(loads).hasValue(0);
    }

    @ParameterizedTest
    @MethodSource("synchronousLookupPaths")
    void a_lookup_during_a_load_here_reads_the_store(Path path) throws Exception {
        open(false, null);
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        source = key -> {
            loading.countDown();
            awaitQuietly(release);
            return "loaded";
        };
        CompletableFuture<@Nullable String> load = CompletableFuture.supplyAsync(() -> sync().get(KEY), background);
        assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();
        putElsewhere("stored");

        assertThat(path.read().read(this, KEY)).isEqualTo("stored");

        release.countDown();
        load.get(5, TimeUnit.SECONDS);
    }

    @ParameterizedTest
    @MethodSource("lookupPaths")
    void a_lookup_checks_an_unconfirmed_copy_while_the_guard_is_open(Path path) {
        open(path.async(), null);
        putElsewhere("old");
        assertThat(path.read().read(this, KEY)).isEqualTo("old");
        changeUnseenAndOpenGuard(path.async());

        assertThat(path.read().read(this, KEY)).as("checked as the guard's probe").isEqualTo("new");
        assertThat(loads).hasValue(0);
    }

    // the read situations every path meets alike

    @ParameterizedTest
    @MethodSource("loadPaths")
    void a_remembered_absence_is_answered_without_the_source(Path path) {
        negativeCaching = LONG;
        open(path.async(), null);
        source = key -> null;

        assertThat(path.read().read(this, KEY)).isNull();
        // a load's future completed with null leaves the map only after its callers see it
        InternalCore<String, String> core = core(path.async());
        await(() -> core.l1().asMap().get(core.wrap(KEY)) == null);
        assertThat(path.read().read(this, KEY)).isNull();

        assertThat(loads).as("remembered after the first load").hasValue(1);
        assertThat(policy(path.async()).stats().negativeHitCount()).isPositive();
    }

    @ParameterizedTest
    @MethodSource("allReadPaths")
    void while_changes_stop_arriving_a_copy_is_checked_against_the_store(Path path) {
        open(path.async(), null);
        putElsewhere("old");
        assertThat(path.read().read(this, KEY)).isEqualTo("old");
        stopChanges(path.async());
        other().put(KEY, "new"); // unseen: changes stopped arriving

        assertThat(path.read().read(this, KEY)).isEqualTo("new");
    }

    @ParameterizedTest
    @MethodSource("allReadPaths")
    void a_closed_cache_refuses_every_read(Path path) {
        open(path.async(), null);
        cache(path.async()).close();

        Read read = path.read();
        assertThatThrownBy(() -> read.read(this, KEY)).satisfies(failure ->
                assertThat(causes(failure)).anyMatch(IllegalStateException.class::isInstance));
        assertThat(loads).hasValue(0);
    }

    @ParameterizedTest
    @MethodSource("allReadPaths")
    void a_copy_lost_in_a_failover_is_not_taken_for_a_later_value_at_its_version(Path path) {
        holdOldLostToNewAtItsVersion(path.async(), false);
        await(() -> delivered() && core(path.async()).isQuiescent());

        assertThat(path.read().read(this, KEY)).isEqualTo("new");
    }

    // the atomic methods deciding on the copy held, not to write: the decision is confirmed by a read, which finds the
    // held copy's version with another value, so each decides again on what the store holds
    static List<Decision> decisionsWritingNothing() {
        return List.of(
                new Decision("asMap().putIfAbsent", false, "new", test ->
                        test.sync().asMap().putIfAbsent(KEY, "absent")),
                new Decision("asMap().replace(old)", false, "newer", test ->
                        test.sync().asMap().replace(KEY, "new", "newer") ? "newer" : "refused"),
                new Decision("asMap().computeIfPresent unchanged", false, "new", test ->
                        test.sync().asMap().computeIfPresent(KEY, (k, v) -> v)),
                new Decision("asMap().compute unchanged", false, "new", test ->
                        test.sync().asMap().compute(KEY, (k, v) -> v)),
                new Decision("asMap().merge unchanged", false, "new", test ->
                        test.sync().asMap().merge(KEY, "merged", (held, given) -> held)),
                new Decision("async asMap().putIfAbsent", true, "new", test -> {
                    CompletableFuture<String> previous = test.async().asMap().putIfAbsent(KEY,
                            CompletableFuture.completedFuture("absent"));
                    return previous == null ? null : previous.join();
                }),
                new Decision("async asMap().computeIfPresent unchanged", true, "new", test -> {
                    CompletableFuture<String> current = test.async().asMap().computeIfPresent(KEY, (k, v) -> v);
                    return current == null ? null : current.join();
                }));
    }

    // a decision and what it must answer, which the key then holds in the store too
    record Decision(String name, boolean async, String expected,
                    Function<InternalPathParityTest, @Nullable String> decide) {

        @Override
        public String toString() {
            return name;
        }
    }

    @ParameterizedTest
    @MethodSource("decisionsWritingNothing")
    void a_decision_on_a_copy_lost_in_a_failover_is_made_again_on_the_later_value_at_its_version(Decision decision) {
        holdOldLostToNewAtItsVersion(decision.async(), true);

        String answered = decision.decide().apply(this);
        database.resumeDelivery();

        assertThat(answered).isEqualTo(decision.expected());
        assertThat(stored(decision.async())).isEqualTo(decision.expected());
    }

    // the paths that write a value: "new" over the "old" held here and elsewhere

    static List<Mutation> writePaths() {
        return List.of(
                new Mutation("put", false, test -> test.sync().put(KEY, "new")),
                new Mutation("putAll", false, test -> test.sync().putAll(Map.of(KEY, "new"))),
                new Mutation("asMap().put", false, test -> test.sync().asMap().put(KEY, "new")),
                new Mutation("asMap().putAll", false, test ->
                        test.sync().asMap().putAll(Map.of(KEY, "new", "another", "value"))),
                new Mutation("asMap().replace", false, test -> test.sync().asMap().replace(KEY, "new")),
                new Mutation("asMap().replace(old)", false, test -> test.sync().asMap().replace(KEY, "old", "new")),
                new Mutation("asMap().compute", false, test -> test.sync().asMap().compute(KEY, (k, v) -> "new")),
                new Mutation("asMap().computeIfPresent", false, test ->
                        test.sync().asMap().computeIfPresent(KEY, (k, v) -> "new")),
                new Mutation("asMap().merge", false, test -> test.sync().asMap().merge(KEY, "new", (a, b) -> b)),
                new Mutation("entry.setValue", false, test ->
                        test.sync().asMap().entrySet().iterator().next().setValue("new")),
                new Mutation("async put", true, test ->
                        test.async().put(KEY, CompletableFuture.completedFuture("new"))),
                new Mutation("async asMap().put", true, test ->
                        test.async().asMap().put(KEY, CompletableFuture.completedFuture("new"))),
                new Mutation("async asMap().putAll", true, test ->
                        test.async().asMap().putAll(Map.of(KEY, CompletableFuture.completedFuture("new"),
                                "another", CompletableFuture.completedFuture("value")))),
                new Mutation("async asMap().replace", true, test ->
                        test.async().asMap().replace(KEY, CompletableFuture.completedFuture("new"))),
                new Mutation("async asMap().compute", true, test -> complete(test.async().asMap().compute(KEY,
                        (k, v) -> CompletableFuture.completedFuture("new")))),
                new Mutation("async asMap().merge", true, test -> complete(test.async().asMap().merge(KEY,
                        CompletableFuture.completedFuture("new"), (a, b) -> b))),
                new Mutation("expireVariably().put", false, test ->
                        assertThat(variable(test.sync()).put(KEY, "new", 1, TimeUnit.HOURS)).isEqualTo("old"), true),
                new Mutation("expireVariably().compute", false, test -> assertThat(variable(test.sync())
                        .compute(KEY, (k, v) -> "new", Duration.ofHours(1))).isEqualTo("new"), true),
                new Mutation("asynchronous expireVariably().put", true, test -> assertThat(
                        variable(test.async().synchronous()).put(KEY, "new", 1, TimeUnit.HOURS)).isEqualTo("old"),
                        true));
    }

    private static VarExpiration<String, String> variable(DistributedCache<String, String> cache) {
        return cache.policy().expireVariably().orElseThrow();
    }

    // the paths that remove the "old" held here and elsewhere

    static List<Mutation> removalPaths() {
        return List.of(
                new Mutation("invalidate", false, test -> test.sync().invalidate(KEY)),
                new Mutation("invalidateAll(keys)", false, test -> test.sync().invalidateAll(List.of(KEY))),
                new Mutation("asMap().remove", false, test -> test.sync().asMap().remove(KEY)),
                new Mutation("asMap().remove(old)", false, test -> test.sync().asMap().remove(KEY, "old")),
                new Mutation("asMap().compute to null", false, test ->
                        test.sync().asMap().compute(KEY, (k, v) -> null)),
                new Mutation("asMap().computeIfPresent to null", false, test ->
                        test.sync().asMap().computeIfPresent(KEY, (k, v) -> null)),
                new Mutation("asMap().merge to null", false, test ->
                        test.sync().asMap().merge(KEY, "new", (a, b) -> null)),
                new Mutation("keySet().remove", false, test -> {
                    Set<String> keys = test.sync().asMap().keySet();
                    keys.remove(KEY);
                }),
                new Mutation("entrySet().remove", false, test ->
                        test.sync().asMap().entrySet().remove(Map.entry(KEY, "old"))),
                new Mutation("iterator().remove", false, test -> {
                    var iterator = test.sync().asMap().keySet().iterator();
                    iterator.next();
                    iterator.remove();
                }),
                new Mutation("async asMap().remove", true, test -> test.async().asMap().remove(KEY)),
                new Mutation("async asMap().remove(old)", true, test ->
                        test.async().asMap().remove(KEY, CompletableFuture.completedFuture("old"))),
                new Mutation("async asMap().compute to null", true, test ->
                        complete(test.async().asMap().compute(KEY, (k, v) -> null))),
                new Mutation("expireVariably().compute to null", false, test -> assertThat(variable(test.sync())
                        .compute(KEY, (k, v) -> null, Duration.ofHours(1))).isNull(), true),
                new Mutation("async keySet().remove", true, test -> {
                    Set<String> keys = test.async().asMap().keySet();
                    keys.remove(KEY);
                }),
                // removing everything
                new Mutation("invalidateAll()", false, test -> test.sync().invalidateAll()),
                new Mutation("asMap().clear()", false, test -> test.sync().asMap().clear()),
                new Mutation("keySet().clear()", false, test -> {
                    Set<String> keys = test.sync().asMap().keySet();
                    keys.clear();
                }),
                new Mutation("values().clear()", false, test -> {
                    Collection<String> values = test.sync().asMap().values();
                    values.clear();
                }),
                new Mutation("entrySet().clear()", false, test -> {
                    Set<Map.Entry<String, String>> entries = test.sync().asMap().entrySet();
                    entries.clear();
                }),
                new Mutation("async asMap().clear()", true, test -> test.async().asMap().clear()),
                new Mutation("async keySet().clear()", true, test -> {
                    Set<String> keys = test.async().asMap().keySet();
                    keys.clear();
                }));
    }

    // one way of changing a key, on the synchronous or the asynchronous cache
    record Mutation(String name, boolean async, Change change, boolean variableExpiry) {

        Mutation(String name, boolean async, Change change) {
            this(name, async, change, false);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @FunctionalInterface
    interface Change {
        void apply(InternalPathParityTest test);
    }

    @ParameterizedTest
    @MethodSource("writePaths")
    void a_write_reaches_the_store_and_this_copy_and_the_others_as_the_mode_says(Mutation path) {
        DistributedCache<String, String> other = holdOldHereAndElsewhere(path);

        path.change().apply(this);

        await(() -> "new".equals(stored(path.async())));
        await(() -> "new".equals(held(path.async())));
        assertTheOtherCopy(other, "new");
        assertRemovalsReported(KEY + "=old REPLACED");
    }

    @ParameterizedTest
    @MethodSource("removalPaths")
    void a_removal_reaches_the_store_and_the_other_copies_as_the_mode_says(Mutation path) {
        DistributedCache<String, String> other = holdOldHereAndElsewhere(path);

        path.change().apply(this);

        assertThat(stored(path.async())).isNull();
        await(() -> held(path.async()) == null);
        assertTheOtherCopy(other, null);
        assertRemovalsReported(KEY + "=old EXPLICIT");
    }

    // the removal listener of the instance under test, for the key: the same report whichever path changed it
    private void assertRemovalsReported(String expected) {
        await(() -> removals.stream().anyMatch(removal -> removal.startsWith(KEY + "=")));
        await(this::delivered);
        assertThat(removals).filteredOn(removal -> removal.startsWith(KEY + "=")).containsExactly(expected);
    }

    // what the mode does to a copy of "old" another instance holds, once the change is written: dropped, and read
    // anew from the store (INVALIDATE); replaced by what was written, or dropped with a removal (UPDATE); kept until
    // it expires (NONE)
    private void assertTheOtherCopy(DistributedCache<String, String> other, @Nullable String written) {
        switch (mode) {
            case INVALIDATE -> {
                await(() -> !"old".equals(other.policy().getIfPresentQuietly(KEY)));
                assertThat(other.getIfPresent(KEY)).isEqualTo(written);
            }
            case UPDATE -> await(() -> Objects.equals(other.policy().getIfPresentQuietly(KEY), written));
            case NONE -> {
                await(this::delivered);
                assertThat(other.policy().getIfPresentQuietly(KEY)).isEqualTo("old");
                assertThat(other.getIfPresent(KEY)).as("served until it expires").isEqualTo("old");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("writePaths")
    void a_write_the_store_refuses_fails_and_nothing_unwritten_is_served(Mutation path) {
        holdOldHereAndElsewhere(path);
        database.setDown(true);

        try {
            path.change().apply(this);
            // only the asynchronous put cannot tell its caller (README): its write fails later, in the background
            assertThat(path.name()).as("returned although the store does not answer").contains("async")
                    .doesNotContain("compute").doesNotContain("merge").doesNotContain("replace");
        } catch (RuntimeException e) {
            assertThat(causes(e)).anyMatch(StoreException.class::isInstance);
        }

        // the asynchronous puts write in the background: done once the future held here completed
        await(() -> settledLocally(path.async()) && !"new".equals(held(path.async())));
        database.setDown(false);
        assertThat(stored(path.async())).isEqualTo("old");
    }

    @ParameterizedTest
    @MethodSource("removalPaths")
    void a_removal_the_store_refuses_fails_and_drops_this_copy_first(Mutation path) {
        holdOldHereAndElsewhere(path);
        database.setDown(true);

        Change change = path.change();
        assertThatThrownBy(() -> change.apply(this)).satisfies(failure ->
                assertThat(causes(failure)).anyMatch(StoreException.class::isInstance));

        assertThat(held(path.async())).as("dropped locally (README, Store not answering)").isNull();
        database.setDown(false);
        assertThat(stored(path.async())).isEqualTo("old");
    }

    @ParameterizedTest
    @MethodSource("allMutations")
    void a_change_with_an_unknown_outcome_leaves_no_copy_the_store_may_not_hold(Mutation path) {
        holdOldHereAndElsewhere(path);
        database.failNextWrites(1, true);

        try {
            path.change().apply(this);
        } catch (RuntimeException e) {
            assertThat(causes(e)).anyMatch(failure ->
                    failure instanceof StoreException storeException && storeException.isOutcomeUnknown());
        }

        // whatever was held is what the store holds, or nothing
        await(() -> {
            String held = held(path.async());
            return held == null || held.equals(stored(path.async()));
        });
    }

    @ParameterizedTest
    @MethodSource("allMutations")
    void a_closed_cache_refuses_every_change(Mutation path) {
        holdOldHereAndElsewhere(path);
        cache(path.async()).close();

        Change change = path.change();
        assertThatThrownBy(() -> change.apply(this)).satisfies(failure ->
                assertThat(causes(failure)).anyMatch(IllegalStateException.class::isInstance));
        assertThat(stored(elsewhere())).isEqualTo("old");
    }

    static List<Path> allReadPaths() {
        List<Path> paths = new ArrayList<>(loadPaths());
        paths.addAll(lookupPaths());
        return paths;
    }

    static List<Mutation> allMutations() {
        List<Mutation> mutations = new ArrayList<>(writePaths());
        mutations.addAll(removalPaths());
        return mutations;
    }

    // as below, for a mutation; one through policy().expireVariably() needs the configuration that offers it
    private DistributedCache<String, String> holdOldHereAndElsewhere(Mutation path) {
        Assumptions.assumeTrue(!path.variableExpiry() || weighedWithVariableExpiry,
                "policy().expireVariably() needs a variable expiry");
        return holdOldHereAndElsewhere(path.async());
    }

    // "old" written by another instance, held there and here
    private DistributedCache<String, String> holdOldHereAndElsewhere(boolean asynchronous) {
        open(asynchronous, null);
        putElsewhere("old");
        String read = asynchronous ? async().getIfPresent(KEY).join() : sync().getIfPresent(KEY);
        assertThat(read).isEqualTo("old");
        assertThat(held(asynchronous)).isEqualTo("old");
        return elsewhere();
    }

    // "old" held here, then lost in a failover and "new" written at its very version (a store assigning versions onto a
    // clock that stands, found 2026-11-10): the version alone no longer tells the two apart. The gap and the later
    // write's message are delivered, or held back where delivery is paused (resumed by the caller), so that this
    // instance still trusts its copy. Only a feed of the store's own carries them; a store polled has versions proposed
    // by the instances, which repeat only as two proposals do (see InternalCore#decide)
    private void holdOldLostToNewAtItsVersion(boolean asynchronous, boolean paused) {
        Assumptions.assumeTrue(mode != DistributionMode.NONE,
                "without distribution, a copy changed elsewhere is served until it expires");
        Assumptions.assumeTrue(database.profile().changes() == StoreProfile.Changes.FEED,
                "the store's own feed reports the failover's gap");
        holdOldHereAndElsewhere(asynchronous);
        if (paused) {
            database.pauseDelivery();
        }
        database.reuseVersion(hash(KEY), "new".getBytes(StandardCharsets.UTF_8));
        assertThat(stored(asynchronous)).isEqualTo("new");
    }

    // waits for what an asynchronous map method returns, if anything
    private static void complete(@Nullable CompletableFuture<?> future) {
        if (future != null) {
            future.join();
        }
    }

    // bulk loads of several keys: each key answered as a single read would answer it

    static List<BulkPath> bulkPaths() {
        return List.of(
                new BulkPath("getAll(keys, function)", false, (test, keys) -> test.sync().getAll(keys, test::loadAll)),
                new BulkPath("LoadingCache.getAll(keys)", false, (test, keys) -> test.sync().getAll(keys)),
                new BulkPath("async getAll(keys, function)", true, (test, keys) ->
                        test.async().getAll(keys, test::loadAll).join()),
                new BulkPath("async getAll(keys, bifunction)", true, (test, keys) -> test.async().getAll(keys,
                        (missing, executor) -> CompletableFuture.supplyAsync(() -> test.loadAll(missing), executor))
                        .join()),
                new BulkPath("AsyncLoadingCache.getAll(keys)", true, (test, keys) -> test.async().getAll(keys).join()));
    }

    record BulkPath(String name, boolean async, BulkRead read) {

        @Override
        public String toString() {
            return name;
        }
    }

    @FunctionalInterface
    interface BulkRead {
        Map<String, String> read(InternalPathParityTest test, List<String> keys);
    }

    @ParameterizedTest
    @MethodSource("bulkPaths")
    void a_bulk_load_answers_each_key_as_a_single_read_would(BulkPath path) {
        open(path.async(), null);
        elsewhere().put("held", "held");
        elsewhere().put("stored", "stored");
        putElsewhere("other");
        assertThat(cache(path.async()).getIfPresent("held")).isEqualTo("held");
        // the source has no value for "absent", and returns "extra" unasked
        source = key -> key.equals("absent") ? null : "loaded-" + key;
        extraKey = "extra";

        Map<String, String> result = path.read().read(this, List.of("held", "stored", "absent", "loaded"));

        assertThat(result).containsExactly(Map.entry("held", "held"), Map.entry("stored", "stored"),
                Map.entry("loaded", "loaded-loaded"));
        assertThat(loads).as("only the keys missing in the store").hasValue(2);
        assertThat(stored(path.async(), "loaded")).isEqualTo("loaded-loaded");
        assertThat(stored(path.async(), "absent")).isNull();
        assertThat(stored(path.async(), "extra")).as("written where no row exists").isEqualTo("loaded-extra");
        await(() -> "stored".equals(held(path.async(), "stored"))
                && "loaded-loaded".equals(held(path.async(), "loaded")));
    }

    @ParameterizedTest
    @MethodSource("bulkPaths")
    void a_bulk_load_with_one_key_removed_on_every_attempt_writes_the_others(BulkPath path) {
        open(path.async(), null);
        Store other = store();
        InternalVersion otherClock = new InternalVersion();
        source = key -> {
            if (key.equals("removed")) {
                Row row = other.read(List.of(hash(key)), LONG).get(hash(key));
                other.write(List.of(Write.tombstone(hash(key), row == null ? otherClock.fresh()
                        : otherClock.next(row.version()))));
            }
            return "loaded-" + key;
        };

        Map<String, String> result = path.read().read(this, List.of("removed", "kept"));

        assertThat(result).containsOnly(Map.entry("removed", "loaded-removed"), Map.entry("kept", "loaded-kept"));
        assertThat(stored(path.async(), "kept")).isEqualTo("loaded-kept");
        assertThat(stored(path.async(), "removed")).isNull();
        await(() -> held(path.async(), "removed") == null && "loaded-kept".equals(held(path.async(), "kept")));
        assertThat(policy(path.async()).stats().uncachedLoadCount()).isOne();
    }

    // refreshes: through the cache, or by refreshAfterWrite on a read; "old" held here and elsewhere

    static List<RefreshPath> refreshPaths() {
        return List.of(
                new RefreshPath("refresh(key)", false, false, test -> test.sync().refresh(KEY).join()),
                new RefreshPath("refreshAll(keys)", false, false, test -> test.sync().refreshAll(List.of(KEY)).join()),
                new RefreshPath("refreshAfterWrite", false, true, test -> test.sync().getIfPresent(KEY)),
                new RefreshPath("async synchronous().refresh(key)", true, false, test ->
                        test.async().synchronous().refresh(KEY).join()),
                new RefreshPath("async synchronous().refreshAll(keys)", true, false, test ->
                        test.async().synchronous().refreshAll(List.of(KEY)).join()),
                new RefreshPath("async refreshAfterWrite", true, true, test -> test.async().getIfPresent(KEY).join()));
    }

    record RefreshPath(String name, boolean async, boolean afterWrite, Change trigger) {

        @Override
        public String toString() {
            return name;
        }
    }

    // "old" held here and elsewhere; with refreshAfterWrite, old enough that the next read refreshes it
    private DistributedCache<String, String> holdOldForRefresh(RefreshPath path) {
        open(path.async(), path.afterWrite() ? Duration.ofMillis(100) : null);
        putElsewhere("old");
        String read = path.async() ? async().getIfPresent(KEY).join() : sync().getIfPresent(KEY);
        assertThat(read).isEqualTo("old");
        if (path.afterWrite()) {
            pause(200);
        }
        return elsewhere();
    }

    private void refresh(RefreshPath path) {
        try {
            path.trigger().apply(this);
        } catch (RuntimeException e) {
            // a failing source fails the refresh's future; what it leaves held is asserted
        }
    }

    @ParameterizedTest
    @MethodSource("refreshPaths")
    void a_refresh_writes_the_new_value_like_a_write(RefreshPath path) {
        DistributedCache<String, String> other = holdOldForRefresh(path);
        source = key -> "new";

        refresh(path);

        await(() -> "new".equals(stored(path.async())));
        await(() -> "new".equals(held(path.async())));
        assertTheOtherCopy(other, "new");
    }

    @ParameterizedTest
    @MethodSource("refreshPaths")
    void a_refresh_finding_no_value_removes_the_key_like_a_removal(RefreshPath path) {
        DistributedCache<String, String> other = holdOldForRefresh(path);
        source = key -> null;

        refresh(path);

        await(() -> stored(path.async()) == null);
        await(() -> held(path.async()) == null);
        assertTheOtherCopy(other, null);
    }

    @ParameterizedTest
    @MethodSource("refreshPaths")
    void a_failing_refresh_keeps_the_value(RefreshPath path) {
        holdOldForRefresh(path);
        source = key -> {
            throw new SourceFailure();
        };

        refresh(path);

        await(() -> loads.get() > 0);
        await(() -> settledLocally(path.async()) && core(path.async()).isQuiescent());
        assertThat(stored(path.async())).isEqualTo("old");
        assertThat(held(path.async())).isEqualTo("old");
    }

    @ParameterizedTest
    @MethodSource("refreshPaths")
    void a_refresh_overtaken_by_a_write_elsewhere_takes_that_write(RefreshPath path) {
        holdOldForRefresh(path);
        DistributedCache<String, String> writer = other();
        source = key -> {
            writer.put(key, "written");
            return "refreshed";
        };

        refresh(path);

        // a refresh after write runs in the background, the source after its count
        await(() -> "written".equals(stored(path.async())));
        await(() -> {
            String held = held(path.async());
            // dropped by the write's message, or replaced by what the store holds now; never the refreshed value
            return held == null || held.equals("written");
        });
        assertThat(cache(path.async()).getIfPresent(KEY)).isEqualTo("written");
    }

    // asynchronous writes whose futures complete later

    @ParameterizedTest
    @MethodSource("pendingFutureWrites")
    void a_pending_future_is_written_once_it_completes(String name) {
        DistributedCache<String, String> other = holdOldHereAndElsewhere(true);
        CompletableFuture<String> pending = new CompletableFuture<>();
        writePending(name, pending);
        CompletableFuture<@Nullable String> read = async().getIfPresent(KEY);
        assertThat(read).as("readers wait for it").isNotDone();

        pending.complete("new");

        assertThat(read.join()).isEqualTo("new");
        await(() -> "new".equals(stored(true)));
        assertTheOtherCopy(other, "new");
    }

    @ParameterizedTest
    @MethodSource("pendingFutureWrites")
    void a_pending_future_that_fails_removes_the_key_everywhere(String name) {
        DistributedCache<String, String> other = holdOldHereAndElsewhere(true);
        CompletableFuture<String> pending = new CompletableFuture<>();
        writePending(name, pending);

        pending.completeExceptionally(new SourceFailure());

        // as in Caffeine, where the put replaced the previous value at once
        await(() -> stored(true) == null);
        await(() -> settledLocally(true) && held(true) == null);
        assertTheOtherCopy(other, null);
    }

    static List<String> pendingFutureWrites() {
        return List.of("async put", "async asMap().put", "async asMap().putAll");
    }

    private void writePending(String name, CompletableFuture<String> pending) {
        switch (name) {
            case "async put" -> async().put(KEY, pending);
            case "async asMap().put" -> async().asMap().put(KEY, pending);
            case "async asMap().putAll" ->
                    async().asMap().putAll(Map.of(KEY, pending, "another", CompletableFuture.completedFuture("value")));
            default -> throw new IllegalArgumentException(name);
        }
    }

    // negative caching: a remembered absence ends with a write elsewhere, as a held copy would be dropped; without
    // distribution, nothing tells, so it stays until it expires

    @ParameterizedTest
    @MethodSource("loadPaths")
    void a_remembered_absence_ends_with_a_write_elsewhere(Path path) {
        negativeCaching = LONG;
        open(path.async(), null);
        source = key -> null;
        assertThat(path.read().read(this, KEY)).isNull();

        putElsewhere("new");

        assertThat(path.read().read(this, KEY)).isEqualTo(mode == DistributionMode.NONE ? null : "new");
        assertThat(loads).as("the source asked once").hasValue(1);
    }

    @ParameterizedTest
    @MethodSource("loadPaths")
    void a_remembered_absence_is_checked_against_the_store_while_changes_stop_arriving(Path path) {
        negativeCaching = LONG;
        open(path.async(), null);
        source = key -> null;
        assertThat(path.read().read(this, KEY)).isNull();
        stopChanges(path.async());
        other().put(KEY, "new"); // unseen: changes stopped arriving

        assertThat(path.read().read(this, KEY)).isEqualTo("new");
        assertThat(loads).hasValue(1);
    }

    @ParameterizedTest
    @MethodSource("lookupPaths")
    void a_lookup_answers_a_remembered_absence_until_a_write_elsewhere(Path path) {
        negativeCaching = LONG;
        open(path.async(), null);
        source = key -> null;
        // remembered by a load of the same instance
        assertThat(path.async() ? async().get(KEY).join() : sync().get(KEY)).isNull();
        long negativeHits = policy(path.async()).stats().negativeHitCount();

        assertThat(path.read().read(this, KEY)).isNull();
        assertThat(policy(path.async()).stats().negativeHitCount()).as("answered without the store")
                .isGreaterThan(negativeHits);

        putElsewhere("new");
        assertThat(path.read().read(this, KEY)).isEqualTo(mode == DistributionMode.NONE ? null : "new");
    }

    // a refresh while the store does not answer: its value is served here, never written; once the store answers,
    // what it holds is served again

    @ParameterizedTest
    @MethodSource("refreshPaths")
    void a_refresh_while_the_store_does_not_answer_serves_its_value_here_only(RefreshPath path) {
        holdOldForRefresh(path);
        source = key -> "new";
        database.setDown(true);

        refresh(path);

        await(() -> "new".equals(held(path.async())));
        // a later refresh, after the store is back, would write the source's value: "old" again, so that what is
        // served comes from the store
        source = key -> "old";
        database.setDown(false);
        assertThat(stored(path.async())).isEqualTo("old");
        await(() -> "old".equals(cache(path.async()).getIfPresent(KEY)));
    }

    // the asynchronous map's methods given futures: they wait for them, as the decision is made in the store

    static List<String> asyncMapMethodsGivenFutures() {
        return List.of("compute", "computeIfPresent", "merge", "replace", "replace(old)");
    }

    private void applyAsyncMapMethod(String name, CompletableFuture<String> future) {
        ConcurrentMap<String, CompletableFuture<String>> map = async().asMap();
        switch (name) {
            case "compute" -> complete(map.compute(KEY, (k, v) -> future));
            case "computeIfPresent" -> complete(map.computeIfPresent(KEY, (k, v) -> future));
            case "merge" -> complete(map.merge(KEY, future, (a, b) -> b));
            case "replace" -> complete(map.replace(KEY, future));
            case "replace(old)" -> map.replace(KEY, CompletableFuture.completedFuture("old"), future);
            default -> throw new IllegalArgumentException(name);
        }
    }

    @ParameterizedTest
    @MethodSource("asyncMapMethodsGivenFutures")
    void an_asynchronous_map_method_waits_for_the_future_it_is_given(String name) {
        DistributedCache<String, String> other = holdOldHereAndElsewhere(true);
        CompletableFuture<String> later = new CompletableFuture<>();
        background.execute(() -> {
            pause(100);
            later.complete("new");
        });

        applyAsyncMapMethod(name, later);

        assertThat(stored(true)).as("written once it completed").isEqualTo("new");
        await(() -> "new".equals(held(true)));
        assertTheOtherCopy(other, "new");
    }

    @ParameterizedTest
    @MethodSource("asyncMapMethodsGivenFutures")
    void an_asynchronous_map_method_given_a_failing_future_changes_nothing(String name) {
        holdOldHereAndElsewhere(true);

        CompletableFuture<String> failing = CompletableFuture.failedFuture(new SourceFailure());
        assertThatThrownBy(() -> applyAsyncMapMethod(name, failing))
                .satisfies(failure -> assertThat(causes(failure)).anyMatch(SourceFailure.class::isInstance));

        assertThat(stored(true)).isEqualTo("old");
        assertThat(held(true)).isEqualTo("old");
    }

    @Test
    void an_asynchronous_compute_if_absent_given_a_failing_future_caches_nothing() {
        open(true, null);

        CompletableFuture<String> computed = async().asMap()
                .computeIfAbsent(KEY, k -> CompletableFuture.failedFuture(new SourceFailure()));

        assertThatThrownBy(computed::join).satisfies(failure ->
                assertThat(causes(failure)).anyMatch(SourceFailure.class::isInstance));
        assertThat(stored(true)).isNull();
        await(() -> settledLocally(true) && held(true) == null);
    }

    // closing while a load is in flight: the caller gets the loaded value or the closed cache's refusal, and is never
    // left waiting

    @ParameterizedTest
    @MethodSource("loadPaths")
    void closing_during_a_load_answers_or_refuses_but_never_hangs(Path path) throws Exception {
        open(path.async(), null);
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        source = key -> {
            loading.countDown();
            awaitQuietly(release);
            return "loaded";
        };
        CompletableFuture<@Nullable String> read = CompletableFuture.supplyAsync(() -> path.read().read(this, KEY),
                background);
        assertThat(loading.await(5, TimeUnit.SECONDS)).isTrue();

        cache(path.async()).close();
        release.countDown();

        try {
            assertThat(read.get(5, TimeUnit.SECONDS)).isEqualTo("loaded");
        } catch (ExecutionException e) {
            assertThat(causes(e)).anyMatch(IllegalStateException.class::isInstance);
        }
    }

    // the situation of the guard tests: the copy held is no longer trusted (the subscription is interrupted) and
    // outdated by a write elsewhere it was not told about; the store guard is open after reads that failed, and its
    // suspension is over, so the next call goes through as its probe
    private void changeUnseenAndOpenGuard(boolean async) {
        stopChanges(async);
        other().put(KEY, "new"); // unseen: changes stopped arriving
        database.failNextReads(Integer.MAX_VALUE);
        DistributedPolicy<String, String> policy = policy(async);
        for (int call = 0; call < InternalStoreGuard.THRESHOLD; call++) {
            assertThatThrownBy(() -> policy.getFromStore("other")).isInstanceOf(StoreException.class);
        }
        database.failNextReads(0);
        pause(TimeUnit.NANOSECONDS.toMillis(InternalStoreGuard.SUSPENSION_NANOS) + 200);
    }

    // changes made elsewhere stop arriving while the store answers: the subscription interrupted, or the polls
    // failing; the instance no longer trusts what it holds, which DEGRADED shows. Without distribution, nothing tells
    // a copy is outdated, so nothing could stop: such a case does not apply
    private void stopChanges(boolean async) {
        Assumptions.assumeTrue(mode != DistributionMode.NONE,
                "without distribution, a copy changed elsewhere is served until it expires");
        if (database.profile().changes() == StoreProfile.Changes.FEED) {
            database.interruptSubscriptions();
        } else {
            database.setPollsFailing(true);
        }
        await(() -> policy(async).getSynchronizationState() == SynchronizationState.DEGRADED);
    }

    // the caches

    private void open(boolean asynchronous, @Nullable Duration refreshAfterWrite) {
        CacheLoader<String, String> loader = new CacheLoader<>() {
            @Override
            public @Nullable String load(String key) {
                return InternalPathParityTest.this.load(key);
            }

            @Override
            public Map<String, String> loadAll(Set<? extends String> keys) {
                return InternalPathParityTest.this.loadAll(keys);
            }
        };
        DistributedCaffeine<String, String> builder = builder().withCaffeine(caffeine -> {
            caffeine.recordStats()
                    .removalListener((key, value, cause) -> removals.add(key + "=" + value + " " + cause));
            if (weighedWithVariableExpiry) {
                caffeine.maximumWeight(1_000).weigher((key, value) -> value.length())
                        .expireAfter(Expiry.writing((key, value) -> LONG));
            }
            return refreshAfterWrite == null ? caffeine : caffeine.refreshAfterWrite(refreshAfterWrite);
        });
        if (negativeCaching != null) {
            builder.withNegativeCaching(negativeCaching);
        }
        if (asynchronous) {
            async = builder.buildAsync(loader);
            closeables.add(async);
        } else {
            sync = builder.build(loader);
            closeables.add(sync);
        }
    }

    private DistributedCaffeine<String, String> builder() {
        return DistributedCaffeine.<String, String>newBuilder(backend::open)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(new StringSerializer<>() {
                            @Override
                            public String serialize(String object) {
                                return object;
                            }

                            @Override
                            public String deserialize(String value) {
                                if (value.equals(UNREADABLE)) {
                                    throw new IllegalArgumentException("unreadable");
                                }
                                return value;
                            }
                        }))
                .withDistributionMode(mode)
                // maintenance would close the guard on its own, before the paths' probe
                .withStore(store -> {
                    store.withPollInterval(Duration.ofMillis(10)).withMaintenanceInterval(LONG).withMaximumAge(LONG)
                            .withMaximumSize(1_000);
                    return maximumAgeForStoreOnly ? store.withMaximumAgeForStoreOnly() : store;
                });
    }

    private DistributedCache<String, String> other() {
        DistributedCache<String, String> other = builder().build();
        closeables.add(other);
        return other;
    }

    // a write by another instance, whose message the instance under test has processed: one arriving during a read,
    // before the read reserved the key, drops what it loads (safe, and on every path), which would make the case
    // depend on timing
    private void putElsewhere(String value) {
        elsewhere().put(KEY, value);
        InternalCore<String, String> core = core(sync == null);
        await(() -> delivered() && core.isQuiescent());
    }

    private DistributedCache<String, String> elsewhere() {
        if (elsewhere == null) {
            elsewhere = other();
        }
        return elsewhere;
    }

    private DistributedCache<String, String> cache(boolean asynchronous) {
        return asynchronous ? async().synchronous() : sync();
    }

    // whether the store has handed every change to the instances that listen or poll; without distribution, none
    // does, and on a polling profile the database would wait for scans that never come
    private boolean delivered() {
        return mode == DistributionMode.NONE || database.isIdle();
    }

    private Store store() {
        Store store = backend.open();
        closeables.add(store);
        return store;
    }

    private DistributedLoadingCache<String, String> sync() {
        return requireNonNull(sync);
    }

    private DistributedAsyncLoadingCache<String, String> async() {
        return requireNonNull(async);
    }

    private DistributedPolicy<String, String> policy(boolean asynchronous) {
        return asynchronous ? async().distributedPolicy() : sync().distributedPolicy();
    }

    // whether nothing is in flight here for the key (a load, or an asynchronous put's write)
    private boolean settledLocally(boolean asynchronous) {
        InternalCore<String, String> core = core(asynchronous);
        CompletableFuture<InternalValue<String>> future = core.l1().asMap().get(core.wrap(KEY));
        return future == null || future.isDone();
    }

    private InternalCore<String, String> core(boolean asynchronous) {
        return asynchronous ? ((InternalDistributedAsyncLoadingCache<String, String>) async()).core
                : ((InternalDistributedLoadingCache<String, String>) sync()).core;
    }

    private @Nullable String held(boolean asynchronous) {
        return held(asynchronous, KEY);
    }

    private @Nullable String held(boolean asynchronous, String key) {
        return cache(asynchronous).policy().getIfPresentQuietly(key);
    }

    private @Nullable String stored(boolean asynchronous, String key) {
        StoredValue<String> stored = cache(asynchronous).distributedPolicy().getFromStore(key);
        return stored == null ? null : stored.value();
    }

    private @Nullable String stored(boolean asynchronous) {
        return stored(cache(asynchronous));
    }

    // through the given instance: one closed cannot read the store
    private static @Nullable String stored(DistributedCache<String, String> cache) {
        StoredValue<String> stored = cache.distributedPolicy().getFromStore(KEY);
        return stored == null ? null : stored.value();
    }

    private boolean containsKey(boolean asynchronous) {
        return asynchronous ? async().asMap().containsKey(KEY) : sync().asMap().containsKey(KEY);
    }

    // the source

    private @Nullable String load(String key) {
        loads.incrementAndGet();
        return source.apply(key);
    }

    private Map<String, String> loadAll(Set<? extends String> keys) {
        Map<String, String> loaded = new HashMap<>();
        for (String key : keys) {
            String value = load(key);
            if (value != null) {
                loaded.put(key, value);
            }
        }
        String extra = extraKey;
        if (extra != null && !keys.contains(extra)) {
            loaded.put(extra, "loaded-" + extra);
        }
        return loaded;
    }

    private static String hash(String key) {
        return new InternalHasher<String>(null).wrap(key).hash();
    }

    private static List<Throwable> causes(Throwable failure) {
        List<Throwable> causes = new ArrayList<>();
        for (Throwable cause = failure; cause != null && !causes.contains(cause); cause = cause.getCause()) {
            causes.add(cause);
        }
        return causes;
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("never released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class SourceFailure extends RuntimeException {
    }
}
