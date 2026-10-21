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
import java.util.List;
import java.util.Map;

/**
 * Linearizability across two instances sharing one store (Lincheck): the operations that are atomic across instances
 * (the {@code asMap()} compute methods, {@code putIfAbsent}, {@code put}, {@code remove}, {@code replace}, through
 * the store), a read of the store, {@code put} and {@code invalidateAll}, each on either instance, must give results
 * that some sequential order of them on a plain map gives too. Reads served from L1 are not among them: across
 * instances they are only eventually consistent, by design. Without distribution, the instances learn of each other
 * only through the store, so their L1 is as stale as it can be (the hot path's hardest case), and nothing runs in the
 * background. {@code invalidateAll} on several keys: its cut-off is one write, atomic for the store's readers.
 */
@Param(name = "instance", gen = IntGen.class, conf = "0:1")
@Param(name = "key", gen = IntGen.class, conf = "1:3")
@Param(name = "value", gen = IntGen.class, conf = "1:3")
// unused: the operations and the model's methods are called by Lincheck, through reflection; resource: the caches
// are closed by the next instance, or after the test (closePrevious)
@SuppressWarnings({"unused", "resource"})
public abstract class AbstractTwoInstanceLinearizabilityTest {

    private static final Duration LONG = Duration.ofHours(1);

    // Lincheck creates an instance per invocation and never closes it: the previous ones are closed by the next
    private static List<DistributedCache<String, String>> previous = List.of();

    private final List<DistributedCache<String, String>> caches;

    @SuppressWarnings("StaticAssignmentInConstructor") // Lincheck owns the instances: only so are the last closed
    protected AbstractTwoInstanceLinearizabilityTest() {
        closePrevious();
        InMemoryDatabase database = new InMemoryDatabase();
        database.setChecksBasis(checksBasis());
        caches = List.of(open(database), open(database));
        previous = caches;
    }

    // whether the store refuses a write decided under an older cut-off (Store#checksBasis), enabling the hot path
    protected abstract boolean checksBasis();

    private static DistributedCache<String, String> open(InMemoryDatabase database) {
        Adapter<String, String> adapter = settings -> database.openStore("cache");
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                // the key as its hash: hash4j reads bytes through VarHandles, which model checking does not support
                .withHashProvider((key, hasher) -> key)
                .withDistributionMode(DistributionMode.NONE)
                .withStoreExecutor(Runnable::run)
                .withCaffeine(caffeine -> caffeine.executor(Runnable::run))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000).withMaintenanceInterval(LONG))
                .build();
    }

    @AfterAll
    static void closePrevious() {
        List<DistributedCache<String, String>> open = previous;
        previous = List.of();
        open.forEach(DistributedCache::close);
    }

    private DistributedCache<String, String> on(int instance) {
        return caches.get(instance);
    }

    private static String key(int key) {
        return "k" + key;
    }

    private static String value(int value) {
        return "v" + value;
    }

    @Operation
    public @Nullable String stored(@Param(name = "instance") int instance, @Param(name = "key") int key) {
        StoredValue<String> stored = on(instance).distributedPolicy().getFromStore(key(key));
        return stored == null ? null : stored.value();
    }

    @Operation
    public void put(@Param(name = "instance") int instance, @Param(name = "key") int key,
                    @Param(name = "value") int value) {
        on(instance).put(key(key), value(value));
    }

    @Operation
    public @Nullable String putReturning(@Param(name = "instance") int instance, @Param(name = "key") int key,
                                         @Param(name = "value") int value) {
        return on(instance).asMap().put(key(key), value(value));
    }

    @Operation
    public @Nullable String putIfAbsent(@Param(name = "instance") int instance, @Param(name = "key") int key,
                                        @Param(name = "value") int value) {
        return on(instance).asMap().putIfAbsent(key(key), value(value));
    }

    @Operation
    public @Nullable String remove(@Param(name = "instance") int instance, @Param(name = "key") int key) {
        return on(instance).asMap().remove(key(key));
    }

    @Operation
    public boolean removeValue(@Param(name = "instance") int instance, @Param(name = "key") int key,
                               @Param(name = "value") int value) {
        return on(instance).asMap().remove(key(key), value(value));
    }

    @Operation
    public @Nullable String replace(@Param(name = "instance") int instance, @Param(name = "key") int key,
                                    @Param(name = "value") int value) {
        return on(instance).asMap().replace(key(key), value(value));
    }

    @Operation
    public boolean replaceValue(@Param(name = "instance") int instance, @Param(name = "key") int key,
                                @Param(name = "value") int value) {
        return on(instance).asMap().replace(key(key), value(value), value(value + 1));
    }

    @Operation
    public @Nullable String compute(@Param(name = "instance") int instance, @Param(name = "key") int key,
                                    @Param(name = "value") int value) {
        return on(instance).asMap().compute(key(key), (k, current) -> current == null ? value(value) : current + value);
    }

    @Operation
    public @Nullable String computeIfPresent(@Param(name = "instance") int instance, @Param(name = "key") int key,
                                             @Param(name = "value") int value) {
        return on(instance).asMap().computeIfPresent(key(key), (k, current) -> current + value);
    }

    @Operation
    public @Nullable String merge(@Param(name = "instance") int instance, @Param(name = "key") int key,
                                  @Param(name = "value") int value) {
        return on(instance).asMap().merge(key(key), value(value), String::concat);
    }

    @Operation
    public void invalidateAll(@Param(name = "instance") int instance) {
        on(instance).invalidateAll();
    }

    // minutes, as every invocation builds two instances through instrumented code: run on request
    // (-Dmodelchecking=true)
    @Test
    @EnabledIfSystemProperty(named = "modelchecking", matches = "true")
    void model_checking() {
        new ModelCheckingOptions().iterations(10).invocationsPerIteration(500).threads(3).actorsPerThread(3)
                .sequentialSpecification(Model.class).check(getClass());
    }

    @Test
    void stress() {
        new StressOptions().iterations(50).invocationsPerIteration(1_000).threads(3).actorsPerThread(3)
                .sequentialSpecification(Model.class).check(getClass());
    }

    /**
     * The sequential specification: what one plain map does, whichever instance an operation runs on.
     */
    public static final class Model {

        private final Map<String, String> map = new HashMap<>();

        public @Nullable String stored(int instance, int key) {
            return map.get(key(key));
        }

        public void put(int instance, int key, int value) {
            map.put(key(key), value(value));
        }

        public @Nullable String putReturning(int instance, int key, int value) {
            return map.put(key(key), value(value));
        }

        public @Nullable String putIfAbsent(int instance, int key, int value) {
            return map.putIfAbsent(key(key), value(value));
        }

        public @Nullable String remove(int instance, int key) {
            return map.remove(key(key));
        }

        public boolean removeValue(int instance, int key, int value) {
            return map.remove(key(key), value(value));
        }

        public @Nullable String replace(int instance, int key, int value) {
            return map.replace(key(key), value(value));
        }

        public boolean replaceValue(int instance, int key, int value) {
            return map.replace(key(key), value(value), value(value + 1));
        }

        public String compute(int instance, int key, int value) {
            return map.compute(key(key), (k, current) -> current == null ? value(value) : current + value);
        }

        public @Nullable String computeIfPresent(int instance, int key, int value) {
            return map.computeIfPresent(key(key), (k, current) -> current + value);
        }

        public String merge(int instance, int key, int value) {
            return map.merge(key(key), value(value), String::concat);
        }

        public void invalidateAll(int instance) {
            map.clear();
        }
    }
}
