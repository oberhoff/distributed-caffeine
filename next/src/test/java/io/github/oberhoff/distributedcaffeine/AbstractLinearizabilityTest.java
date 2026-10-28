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
import org.jetbrains.lincheck.datastructures.IntGen;
import org.jetbrains.lincheck.datastructures.ModelCheckingOptions;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.Param;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Linearizability of one instance (Lincheck): the operations of the cache, run from several threads, must give results
 * that some sequential order of them on a plain map gives too; the subclasses choose the keys and operations. Model
 * checking explores the interleavings systematically (and reports a failing one step by step), the stress test runs
 * them on real threads. The instance runs without background threads (no distribution, the store's and Caffeine's
 * tasks on the caller), which model checking needs; distribution and the store's faults are the property test's.
 */
@Param(name = "key", gen = IntGen.class, conf = "1:3")
@Param(name = "value", gen = IntGen.class, conf = "1:3")
@SuppressWarnings("unused") // the operations and the model's methods are called by Lincheck, through reflection
public abstract class AbstractLinearizabilityTest {

    private static final Duration LONG = Duration.ofHours(1);

    // Lincheck creates an instance per invocation and never closes it: the previous one is closed by the next
    private static @Nullable DistributedLoadingCache<String, String> previous;

    protected final DistributedLoadingCache<String, String> cache;

    @SuppressWarnings("StaticAssignmentInConstructor") // Lincheck owns the instances: only so is the last one closed
    protected AbstractLinearizabilityTest() {
        closePrevious();
        InMemoryDatabase database = new InMemoryDatabase();
        Adapter<String, String> adapter = settings -> database.openStore("cache");
        cache = DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                // the key as its hash: hash4j reads bytes through VarHandles, which model checking does not support
                .withHashProvider((key, hasher) -> key)
                .withDistributionMode(DistributionMode.NONE)
                .withStoreExecutor(Runnable::run)
                .withCaffeine(caffeine -> caffeine.executor(Runnable::run))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000).withMaintenanceInterval(LONG))
                .build(AbstractLinearizabilityTest::load);
        previous = cache;
    }

    // on a thread of its own: an invocation the model checker aborted may have left a lock of the previous instance
    // held (Caffeine's own), and closing it would then wait for good while Lincheck collects a failure's trace
    @AfterAll
    static void closePrevious() {
        DistributedLoadingCache<String, String> open = previous;
        previous = null;
        if (open != null) {
            Thread closer = new Thread(open::close, "lincheck-close-previous");
            closer.setDaemon(true);
            closer.start();
        }
    }

    private static String load(String key) {
        return "loaded" + key;
    }

    // the key an operation acts on: a subclass may map several to one
    protected String key(int key) {
        return "k" + key;
    }

    // the sequential specification of the subclass
    protected Class<? extends Model> model() {
        return Model.class;
    }

    private static String value(int value) {
        return "v" + value;
    }

    @Operation
    public @Nullable String get(@Param(name = "key") int key) {
        return cache.get(key(key));
    }

    @Operation
    public @Nullable String getIfPresent(@Param(name = "key") int key) {
        return cache.getIfPresent(key(key));
    }

    @Operation
    public void put(@Param(name = "key") int key, @Param(name = "value") int value) {
        cache.put(key(key), value(value));
    }

    @Operation
    public @Nullable String putReturning(@Param(name = "key") int key, @Param(name = "value") int value) {
        return cache.asMap().put(key(key), value(value));
    }

    @Operation
    public @Nullable String putIfAbsent(@Param(name = "key") int key, @Param(name = "value") int value) {
        return cache.asMap().putIfAbsent(key(key), value(value));
    }

    @Operation
    public @Nullable String remove(@Param(name = "key") int key) {
        return cache.asMap().remove(key(key));
    }

    @Operation
    public boolean removeValue(@Param(name = "key") int key, @Param(name = "value") int value) {
        return cache.asMap().remove(key(key), value(value));
    }

    @Operation
    public @Nullable String replace(@Param(name = "key") int key, @Param(name = "value") int value) {
        return cache.asMap().replace(key(key), value(value));
    }

    @Operation
    public boolean replaceValue(@Param(name = "key") int key, @Param(name = "value") int value) {
        return cache.asMap().replace(key(key), value(value), value(value + 1));
    }

    @Operation
    public @Nullable String compute(@Param(name = "key") int key, @Param(name = "value") int value) {
        return cache.asMap().compute(key(key), (k, current) -> current == null ? value(value) : current + value);
    }

    @Operation
    public @Nullable String computeIfPresent(@Param(name = "key") int key, @Param(name = "value") int value) {
        return cache.asMap().computeIfPresent(key(key), (k, current) -> current + value);
    }

    @Operation
    public @Nullable String merge(@Param(name = "key") int key, @Param(name = "value") int value) {
        return cache.asMap().merge(key(key), value(value), String::concat);
    }

    @Operation
    public void invalidate(@Param(name = "key") int key) {
        cache.invalidate(key(key));
    }

    // minutes, as every invocation builds an instance through instrumented code: run on request (-Dmodelchecking=true)
    @Test
    @EnabledIfSystemProperty(named = "modelchecking", matches = "true")
    void model_checking() {
        new ModelCheckingOptions().iterations(10).invocationsPerIteration(500).threads(3).actorsPerThread(3)
                .sequentialSpecification(model()).check(getClass());
    }

    @Test
    void stress() {
        new StressOptions().iterations(50).invocationsPerIteration(1_000).threads(3).actorsPerThread(3)
                .sequentialSpecification(model()).check(getClass());
    }

    /**
     * The sequential specification: what a plain map with a loader does (with every operation of every subclass).
     */
    public static class Model {

        private final Map<String, String> map = new HashMap<>();

        protected String key(int key) {
            return "k" + key;
        }

        public @Nullable String get(int key) {
            return map.computeIfAbsent(key(key), AbstractLinearizabilityTest::load);
        }

        public @Nullable String getIfPresent(int key) {
            return map.get(key(key));
        }

        public void put(int key, int value) {
            map.put(key(key), value(value));
        }

        public @Nullable String putReturning(int key, int value) {
            return map.put(key(key), value(value));
        }

        public @Nullable String putIfAbsent(int key, int value) {
            return map.putIfAbsent(key(key), value(value));
        }

        public @Nullable String remove(int key) {
            return map.remove(key(key));
        }

        public boolean removeValue(int key, int value) {
            return map.remove(key(key), value(value));
        }

        public @Nullable String replace(int key, int value) {
            return map.replace(key(key), value(value));
        }

        public boolean replaceValue(int key, int value) {
            return map.replace(key(key), value(value), value(value + 1));
        }

        public @Nullable String compute(int key, int value) {
            return map.compute(key(key), (k, current) -> current == null ? value(value) : current + value);
        }

        public @Nullable String computeIfPresent(int key, int value) {
            return map.computeIfPresent(key(key), (k, current) -> current + value);
        }

        public @Nullable String merge(int key, int value) {
            return map.merge(key(key), value(value), String::concat);
        }

        public void invalidate(int key) {
            map.remove(key(key));
        }

        public void invalidateAll() {
            map.clear();
        }
    }
}
