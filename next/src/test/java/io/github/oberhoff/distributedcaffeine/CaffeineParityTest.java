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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Feature parity with Caffeine (VISION.md), differentially: the same random sequence of calls goes to a plain Caffeine
 * cache and to distributed caches built with the same settings, and after every call the results are compared (value
 * or exception type), and for a single instance also the map, the size, {@code stats()} and the removal events. Each
 * side has its own loader, whose values count its calls per key, so a load one side makes and the other does not shows
 * up as a different value.
 * <p>
 * Variants: a synchronous loading cache (with and without a bulk loader), an asynchronous one (its own methods and
 * its {@code synchronous()} view), and two instances answering for one Caffeine cache, calls going to either at
 * random, with the delivery settled in between (in both distribution modes); there only the global methods are
 * compared, since views, sizes and stats are local (documented).
 * <p>
 * Not compared: size eviction (Caffeine's admission is randomized, so its victims are not reproducible) and expiry
 * (L1 expires on Caffeine's ticker, L2 on the store's clock); the documented deviations are normalized where they
 * apply: an {@code AsyncCache.getIfPresent} future completing with {@code null} counts as Caffeine's {@code null}.
 */
@Timeout(300)
public class CaffeineParityTest {

    private static final int STEPS = 150;
    private static final int SEEDS = Integer.getInteger("parity.seeds", 12); // more: -Dparity.seeds=100
    // "nul" loads null, "err" fails to load
    private static final List<String> KEYS = List.of("a", "b", "c", "nul", "err");
    private static final List<String> VALUES = List.of("v0", "v1", "v2");

    // ---------------------------------------------------------------------------------------------------------------
    // the variants

    // the store under test: the in-memory store here, a real database in the subclasses; a fresh one per seed
    protected CacheBackend backend() {
        return CacheBackend.of(new InMemoryDatabase());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_loading_cache_behaves_as_caffeine(boolean bulkLoader) {
        for (int seed = 0; seed < SEEDS; seed++) {
            try (Pair pair = new Pair(backend(), bulkLoader, 1, DistributionMode.INVALIDATE, false)) {
                pair.run(seed, syncOperations(true), true);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void an_async_loading_cache_behaves_as_caffeine(boolean bulkLoader) {
        for (int seed = 0; seed < SEEDS; seed++) {
            try (Pair pair = new Pair(backend(), bulkLoader, 1, DistributionMode.INVALIDATE, true)) {
                List<Operation> operations = new ArrayList<>(asyncOperations(true));
                operations.addAll(syncOperations(true)); // through synchronous()
                pair.run(seed, operations, true);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALIDATE", "UPDATE"})
    void two_instances_behave_as_one_caffeine_cache(String mode) {
        for (int seed = 0; seed < SEEDS; seed++) {
            try (Pair pair = new Pair(backend(), true, 2, DistributionMode.valueOf(mode), false)) {
                pair.run(seed, syncOperations(false), false);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALIDATE", "UPDATE"})
    void two_async_instances_behave_as_one_caffeine_cache(String mode) {
        for (int seed = 0; seed < SEEDS; seed++) {
            try (Pair pair = new Pair(backend(), true, 2, DistributionMode.valueOf(mode), true)) {
                List<Operation> operations = new ArrayList<>(asyncOperations(false));
                operations.addAll(syncOperations(false));
                pair.run(seed, operations, false);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // the operations: each draws its arguments, then runs on either side

    private interface Call {
        @Nullable Object apply(Side side);
    }

    // local: views, sizes, quiet reads; compared for a single instance only
    private record Operation(String name, boolean local, Function<Random, Named> draw) {
    }

    private record Named(String description, Call call) {
    }

    private static Operation global(String name, Function<Random, Named> draw) {
        return new Operation(name, false, draw);
    }

    private static Operation local(String name, Function<Random, Named> draw) {
        return new Operation(name, true, draw);
    }

    private static String key(Random random) {
        return KEYS.get(random.nextInt(KEYS.size()));
    }

    private static String value(Random random) {
        return VALUES.get(random.nextInt(VALUES.size()));
    }

    private static List<String> keys(Random random) {
        List<String> keys = new ArrayList<>();
        int count = 1 + random.nextInt(3);
        for (int i = 0; i < count; i++) {
            String key = key(random);
            if (!keys.contains(key)) {
                keys.add(key);
            }
        }
        return keys;
    }

    // a mapping function: a value, null, or a failure
    private static Function<String, @Nullable String> mapping(int kind) {
        return switch (kind) {
            case 0 -> key -> "f-" + key;
            case 1 -> key -> null;
            default -> key -> {
                throw new IllegalArgumentException("mapping failed");
            };
        };
    }

    // the null arguments are the point (both sides reject them), and so are the views (keySet().remove)
    @SuppressWarnings({"NullArgumentForNonNullParameter", "DataFlowIssue", "RedundantCollectionOperation"})
    private static List<Operation> syncOperations(boolean withLocal) {
        List<Operation> operations = new ArrayList<>(List.of(
                // Cache / LoadingCache
                global("getIfPresent", random -> {
                    String key = key(random);
                    return new Named("getIfPresent(" + key + ")", side -> side.sync().getIfPresent(key));
                }),
                global("get", random -> {
                    String key = key(random);
                    return new Named("get(" + key + ")", side -> side.sync().get(key));
                }),
                global("getWithFunction", random -> {
                    String key = key(random);
                    int kind = random.nextInt(3);
                    return new Named("get(" + key + ", f" + kind + ")", side -> side.sync().get(key, mapping(kind)));
                }),
                global("getAll", random -> {
                    List<String> keys = keys(random);
                    return new Named("getAll(" + keys + ")", side -> side.sync().getAll(keys));
                }),
                global("getAllWithFunction", random -> {
                    List<String> keys = keys(random);
                    boolean fail = random.nextInt(4) == 0;
                    return new Named("getAll(" + keys + ", fail " + fail + ")", side -> side.sync().getAll(keys,
                            requested -> {
                                if (fail) {
                                    throw new IllegalArgumentException("bulk mapping failed");
                                }
                                Map<String, String> found = new LinkedHashMap<>();
                                requested.forEach(key -> {
                                    if (!key.equals("nul")) {
                                        found.put(key, "bulk-" + key);
                                    }
                                });
                                return found;
                            }));
                }),
                global("getAllPresent", random -> {
                    List<String> keys = keys(random);
                    return new Named("getAllPresent(" + keys + ")", side -> side.sync().getAllPresent(keys));
                }),
                global("put", random -> {
                    String key = key(random);
                    String value = value(random);
                    return new Named("put(" + key + ", " + value + ")", side -> {
                        side.sync().put(key, value);
                        return null;
                    });
                }),
                global("putAll", random -> {
                    Map<String, String> map = new LinkedHashMap<>();
                    keys(random).forEach(key -> map.put(key, value(random)));
                    return new Named("putAll(" + map + ")", side -> {
                        side.sync().putAll(map);
                        return null;
                    });
                }),
                global("invalidate", random -> {
                    String key = key(random);
                    return new Named("invalidate(" + key + ")", side -> {
                        side.sync().invalidate(key);
                        return null;
                    });
                }),
                global("invalidateAllKeys", random -> {
                    List<String> keys = keys(random);
                    return new Named("invalidateAll(" + keys + ")", side -> {
                        side.sync().invalidateAll(keys);
                        return null;
                    });
                }),
                global("invalidateAll", random -> new Named("invalidateAll()", side -> {
                    side.sync().invalidateAll();
                    return null;
                })),
                global("refresh", random -> {
                    String key = key(random);
                    return new Named("refresh(" + key + ")", side -> side.sync().refresh(key));
                }),
                global("refreshAll", random -> {
                    List<String> keys = keys(random);
                    return new Named("refreshAll(" + keys + ")", side -> side.sync().refreshAll(keys));
                }),
                global("nullArguments", random -> {
                    int which = random.nextInt(6);
                    return new Named("null argument " + which, side -> switch (which) {
                        case 0 -> side.sync().getIfPresent(null);
                        case 1 -> side.sync().get(null);
                        case 2 -> {
                            side.sync().put("a", null);
                            yield null;
                        }
                        case 3 -> side.sync().asMap().put(null, "v0");
                        case 4 -> side.sync().getAll(java.util.Arrays.asList("a", null));
                        default -> side.sync().asMap().merge("a", null, (a, b) -> a);
                    });
                }),
                // asMap(): one key
                global("map.get", random -> {
                    String key = key(random);
                    return new Named("asMap().get(" + key + ")", side -> side.sync().asMap().get(key));
                }),
                global("map.getOrDefault", random -> {
                    String key = key(random);
                    return new Named("asMap().getOrDefault(" + key + ")",
                            side -> side.sync().asMap().getOrDefault(key, "default"));
                }),
                global("map.containsKey", random -> {
                    String key = key(random);
                    return new Named("asMap().containsKey(" + key + ")", side -> side.sync().asMap().containsKey(key));
                }),
                global("map.put", random -> {
                    String key = key(random);
                    String value = value(random);
                    return new Named("asMap().put(" + key + ", " + value + ")",
                            side -> side.sync().asMap().put(key, value));
                }),
                global("map.putIfAbsent", random -> {
                    String key = key(random);
                    String value = value(random);
                    return new Named("asMap().putIfAbsent(" + key + ", " + value + ")",
                            side -> side.sync().asMap().putIfAbsent(key, value));
                }),
                global("map.remove", random -> {
                    String key = key(random);
                    return new Named("asMap().remove(" + key + ")", side -> side.sync().asMap().remove(key));
                }),
                global("map.removeValue", random -> {
                    String key = key(random);
                    String value = value(random);
                    return new Named("asMap().remove(" + key + ", " + value + ")",
                            side -> side.sync().asMap().remove(key, value));
                }),
                global("map.replace", random -> {
                    String key = key(random);
                    String value = value(random);
                    return new Named("asMap().replace(" + key + ", " + value + ")",
                            side -> side.sync().asMap().replace(key, value));
                }),
                global("map.replaceValue", random -> {
                    String key = key(random);
                    String expected = value(random);
                    String value = value(random);
                    return new Named("asMap().replace(" + key + ", " + expected + ", " + value + ")",
                            side -> side.sync().asMap().replace(key, expected, value));
                }),
                global("map.computeIfAbsent", random -> {
                    String key = key(random);
                    int kind = random.nextInt(3);
                    return new Named("asMap().computeIfAbsent(" + key + ", f" + kind + ")",
                            side -> side.sync().asMap().computeIfAbsent(key, mapping(kind)));
                }),
                global("map.computeIfPresent", random -> {
                    String key = key(random);
                    boolean remove = random.nextBoolean();
                    return new Named("asMap().computeIfPresent(" + key + ", remove " + remove + ")",
                            side -> side.sync().asMap().computeIfPresent(key, (k, v) -> remove ? null : v + "+"));
                }),
                global("map.compute", random -> {
                    String key = key(random);
                    int kind = random.nextInt(4);
                    return new Named("asMap().compute(" + key + ", r" + kind + ")",
                            side -> side.sync().asMap().compute(key, (k, v) -> switch (kind) {
                                case 0 -> v == null ? "new" : v + "+";
                                case 1 -> null;
                                case 2 -> v; // unchanged
                                default -> throw new IllegalArgumentException("remapping failed");
                            }));
                }),
                global("map.merge", random -> {
                    String key = key(random);
                    String value = value(random);
                    boolean remove = random.nextInt(3) == 0;
                    return new Named("asMap().merge(" + key + ", " + value + ", remove " + remove + ")",
                            side -> side.sync().asMap().merge(key, value, (a, b) -> remove ? null : a + b));
                }),
                global("map.putAll", random -> {
                    Map<String, String> map = new LinkedHashMap<>();
                    keys(random).forEach(key -> map.put(key, value(random)));
                    return new Named("asMap().putAll(" + map + ")", side -> {
                        side.sync().asMap().putAll(map);
                        return null;
                    });
                }),
                global("map.clear", random -> new Named("asMap().clear()", side -> {
                    side.sync().asMap().clear();
                    return null;
                })),
                global("map.containsKeyNull", random -> new Named("asMap().containsKey(null)",
                        side -> side.sync().asMap().containsKey(null)))));
        if (withLocal) {
            operations.addAll(List.of(
                    local("estimatedSize", random -> new Named("estimatedSize()", side -> {
                        side.sync().cleanUp();
                        return side.sync().estimatedSize();
                    })),
                    local("map.size", random -> new Named("asMap().size()", side -> side.sync().asMap().size())),
                    local("map.isEmpty", random -> new Named("asMap().isEmpty()", side -> side.sync().asMap().isEmpty())),
                    local("map.containsValue", random -> {
                        String value = value(random);
                        return new Named("asMap().containsValue(" + value + ")",
                                side -> side.sync().asMap().containsValue(value));
                    }),
                    local("map.views", random -> new Named("asMap() views", side -> List.of(
                            new TreeMap<>(side.sync().asMap()), Set.copyOf(side.sync().asMap().keySet()),
                            sorted(side.sync().asMap().values()), entries(side.sync().asMap().entrySet())))),
                    local("map.keySet.remove", random -> {
                        String key = key(random);
                        return new Named("asMap().keySet().remove(" + key + ")",
                                side -> side.sync().asMap().keySet().remove(key));
                    }),
                    local("map.values.removeIf", random -> {
                        String value = value(random);
                        return new Named("asMap().values().removeIf(" + value + ")",
                                side -> side.sync().asMap().values().removeIf(value::equals));
                    }),
                    local("map.entrySet.remove", random -> {
                        String key = key(random);
                        String value = value(random);
                        return new Named("asMap().entrySet().remove(" + key + "=" + value + ")",
                                side -> side.sync().asMap().entrySet().remove(Map.entry(key, value)));
                    }),
                    local("map.keySet.retainAll", random -> {
                        List<String> keys = keys(random);
                        return new Named("asMap().keySet().retainAll(" + keys + ")",
                                side -> side.sync().asMap().keySet().retainAll(keys));
                    }),
                    local("map.iterator.remove", random -> {
                        String key = key(random);
                        return new Named("asMap().entrySet().iterator() remove " + key, side -> {
                            Iterator<Map.Entry<String, String>> iterator = side.sync().asMap().entrySet().iterator();
                            while (iterator.hasNext()) {
                                if (iterator.next().getKey().equals(key)) {
                                    iterator.remove();
                                    return true;
                                }
                            }
                            return false;
                        });
                    }),
                    local("map.entry.setValue", random -> {
                        String key = key(random);
                        String value = value(random);
                        return new Named("asMap() entry " + key + " setValue(" + value + ")", side -> {
                            for (Map.Entry<String, String> entry : side.sync().asMap().entrySet()) {
                                if (entry.getKey().equals(key)) {
                                    return entry.setValue(value);
                                }
                            }
                            return "not held";
                        });
                    }),
                    local("policy.getIfPresentQuietly", random -> {
                        String key = key(random);
                        return new Named("policy().getIfPresentQuietly(" + key + ")",
                                side -> side.sync().policy().getIfPresentQuietly(key));
                    }),
                    local("policy.getEntryIfPresentQuietly", random -> {
                        String key = key(random);
                        return new Named("policy().getEntryIfPresentQuietly(" + key + ")", side -> {
                            var entry = side.sync().policy().getEntryIfPresentQuietly(key);
                            return entry == null ? null : entry.getKey() + "=" + entry.getValue();
                        });
                    }),
                    local("policy.flags", random -> new Named("policy() flags", side -> List.of(
                            side.sync().policy().isRecordingStats(), side.sync().policy().eviction().isPresent(),
                            side.sync().policy().expireAfterWrite().isPresent(),
                            side.sync().policy().refreshAfterWrite().isPresent())))));
        }
        return operations;
    }

    @SuppressWarnings("FutureReturnValueIgnored") // the futures are returned to outcome(), which waits for them
    private static List<Operation> asyncOperations(boolean withLocal) {
        List<Operation> operations = new ArrayList<>(List.of(
                global("async.getIfPresent", random -> {
                    String key = key(random);
                    return new Named("async getIfPresent(" + key + ")", side -> side.async().getIfPresent(key));
                }),
                global("async.get", random -> {
                    String key = key(random);
                    return new Named("async get(" + key + ")", side -> side.async().get(key));
                }),
                global("async.getWithFunction", random -> {
                    String key = key(random);
                    int kind = random.nextInt(3);
                    return new Named("async get(" + key + ", f" + kind + ")",
                            side -> side.async().get(key, mapping(kind)));
                }),
                global("async.getWithAsyncFunction", random -> {
                    String key = key(random);
                    boolean fail = random.nextBoolean();
                    return new Named("async get(" + key + ", async fail " + fail + ")",
                            side -> side.async().get(key, (k, executor) -> fail
                                    ? CompletableFuture.failedFuture(new IllegalArgumentException("async failed"))
                                    : CompletableFuture.completedFuture("af-" + k)));
                }),
                global("async.getAll", random -> {
                    List<String> keys = keys(random);
                    return new Named("async getAll(" + keys + ")", side -> side.async().getAll(keys));
                }),
                global("async.put", random -> {
                    String key = key(random);
                    String value = value(random);
                    boolean fail = random.nextInt(4) == 0;
                    return new Named("async put(" + key + ", " + (fail ? "failed" : value) + ")", side -> {
                        side.async().put(key, fail ? CompletableFuture.failedFuture(
                                new IllegalArgumentException("failed value")) : CompletableFuture.completedFuture(value));
                        return null;
                    });
                }),
                global("async.map.get", random -> {
                    String key = key(random);
                    return new Named("async asMap().get(" + key + ")", side -> side.async().asMap().get(key));
                }),
                global("async.map.remove", random -> {
                    String key = key(random);
                    return new Named("async asMap().remove(" + key + ")", side -> side.async().asMap().remove(key));
                }),
                global("async.map.putIfAbsent", random -> {
                    String key = key(random);
                    String value = value(random);
                    return new Named("async asMap().putIfAbsent(" + key + ", " + value + ")",
                            side -> side.async().asMap().putIfAbsent(key, CompletableFuture.completedFuture(value)));
                }),
                global("async.map.computeIfAbsent", random -> {
                    String key = key(random);
                    String value = value(random);
                    return new Named("async asMap().computeIfAbsent(" + key + ", " + value + ")",
                            side -> side.async().asMap().computeIfAbsent(key,
                                    k -> CompletableFuture.completedFuture(value)));
                })));
        if (withLocal) {
            operations.add(local("async.map.views", random -> new Named("async asMap() views", side -> {
                Map<String, Object> joined = new TreeMap<>();
                side.async().asMap().forEach((key, future) -> joined.put(key, normalize(future)));
                return List.of(joined, side.async().asMap().size());
            })));
        }
        return operations;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // the two sides

    // the loader of one side: values count its calls per key
    private static final class Loader implements CacheLoader<String, String> {

        private final boolean bulk;
        private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();

        Loader(boolean bulk) {
            this.bulk = bulk;
        }

        @Override
        public @Nullable String load(String key) {
            int call = calls.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
            return switch (key) {
                case "nul" -> null;
                case "err" -> throw new IllegalStateException("load failed");
                default -> "L-" + key + "#" + call;
            };
        }

        @Override
        public Map<? extends String, ? extends String> loadAll(Set<? extends String> keys) throws Exception {
            if (!bulk) {
                return CacheLoader.super.loadAll(keys); // unsupported: Caffeine loads one by one
            }
            if (keys.contains("err")) {
                throw new IllegalStateException("bulk load failed");
            }
            Map<String, String> loaded = new LinkedHashMap<>();
            for (String key : keys) {
                int call = calls.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
                if (!key.equals("nul")) {
                    loaded.put(key, "B-" + key + "#" + call);
                }
            }
            return loaded;
        }
    }

    private interface Side {
        LoadingCache<String, String> sync();

        AsyncLoadingCache<String, String> async();
    }

    // removal events as the removal listeners report them, from any thread
    private static final class Removals {

        private final List<String> events = new ArrayList<>();

        synchronized void add(String event) {
            events.add(event);
        }

        synchronized int size() {
            return events.size();
        }

        // the events from the index on, sorted
        synchronized List<String> sortedFrom(int from) {
            return events.subList(from, events.size()).stream().sorted().toList();
        }
    }

    private record CaffeineSide(LoadingCache<String, String> sync, @Nullable AsyncLoadingCache<String, String> asyncCache,
                                Removals removals) implements Side {

        @Override
        public AsyncLoadingCache<String, String> async() {
            return java.util.Objects.requireNonNull(asyncCache);
        }
    }

    private record DistributedSide(LoadingCache<String, String> sync,
                                   @Nullable AsyncLoadingCache<String, String> asyncCache,
                                   InternalCore<String, String> core, AutoCloseable cache) implements Side {

        @Override
        public AsyncLoadingCache<String, String> async() {
            return java.util.Objects.requireNonNull(asyncCache);
        }
    }

    private static final class Pair implements AutoCloseable {

        private final CacheBackend backend;
        private final CaffeineSide caffeine;
        private final List<DistributedSide> instances = new ArrayList<>();
        private final Removals distributedRemovals = new Removals();

        Pair(CacheBackend backend, boolean bulkLoader, int instanceCount, DistributionMode mode, boolean async) {
            this.backend = backend;
            Removals caffeineRemovals = new Removals();
            Caffeine<Object, Object> builder = Caffeine.newBuilder().recordStats().executor(Runnable::run);
            RemovalListener<String, String> caffeineListener = (key, value, cause) ->
                    caffeineRemovals.add(cause + " " + key + "=" + value);
            Loader caffeineLoader = new Loader(bulkLoader);
            if (async) {
                AsyncLoadingCache<String, String> cache = builder.removalListener(caffeineListener)
                        .buildAsync(caffeineLoader);
                caffeine = new CaffeineSide(cache.synchronous(), cache, caffeineRemovals);
            } else {
                caffeine = new CaffeineSide(builder.removalListener(caffeineListener).build(caffeineLoader), null,
                        caffeineRemovals);
            }
            Loader distributedLoader = new Loader(bulkLoader); // shared by the instances, as one source
            for (int i = 0; i < instanceCount; i++) {
                Adapter<String, String> adapter = backend::open;
                DistributedCaffeine<String, String> distributed = DistributedCaffeine.newBuilder(adapter)
                        .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                                .withValueSerializer(StringSerializer.ofString()))
                        .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(10_000))
                        .withDistributionMode(mode)
                        .withCaffeine(configurer -> configurer.recordStats().executor(Runnable::run)
                                .removalListener((key, value, cause) ->
                                        distributedRemovals.add(cause + " " + key + "=" + value)));
                if (async) {
                    DistributedAsyncLoadingCache<String, String> cache = distributed.buildAsync(distributedLoader);
                    instances.add(new DistributedSide(cache.synchronous(), cache,
                            ((InternalDistributedAsyncLoadingCache<String, String>) cache).core, cache));
                } else {
                    DistributedLoadingCache<String, String> cache = distributed.build(distributedLoader);
                    instances.add(new DistributedSide(cache, null,
                            ((InternalDistributedLoadingCache<String, String>) cache).core, cache));
                }
            }
        }

        void run(int seed, List<Operation> operations, boolean compareLocal) {
            Random random = new Random(seed);
            List<String> history = new ArrayList<>();
            for (int step = 0; step < STEPS; step++) {
                Operation operation = operations.get(random.nextInt(operations.size()));
                Named named = operation.draw.apply(random);
                DistributedSide instance = instances.get(random.nextInt(instances.size()));
                history.add((instances.size() > 1 ? "instance " + instances.indexOf(instance) + ": " : "")
                        + named.description);
                String where = "seed " + seed + ", step " + step + ": " + named.description
                        + "\nhistory:\n  " + String.join("\n  ", history);

                int caffeineRemovalCount = caffeine.removals.size();
                int distributedRemovalCount = distributedRemovals.size();
                Object expected = outcome(named.call, caffeine);
                Object actual = outcome(named.call, instance);
                settle();
                assertThat(actual).as("result, " + where).isEqualTo(expected);
                if (!compareLocal) {
                    continue;
                }
                assertThat(new TreeMap<>(instance.sync().asMap())).as("map, " + where)
                        .isEqualTo(new TreeMap<>(caffeine.sync().asMap()));
                // Caffeine records a refresh's load in a callback on its future, which may run just after the future
                // completes for the caller (in Caffeine too, with an asynchronous executor): equal within a second
                List<Long> expectedStats = statsOf(caffeine.sync().stats());
                awaitQuietly(() -> statsOf(instance.sync().stats()).equals(expectedStats));
                assertThat(statsOf(instance.sync().stats())).as("stats, " + where).isEqualTo(expectedStats);
                assertThat(distributedRemovals.sortedFrom(distributedRemovalCount))
                        .as("removal events, " + where + "\nall distributed events: " + distributedRemovals.sortedFrom(0)
                                + "\nall caffeine events: " + caffeine.removals.sortedFrom(0))
                        .isEqualTo(caffeine.removals.sortedFrom(caffeineRemovalCount));
            }
        }

        // waits until every write has landed (an async put writes once its future completes), the store has delivered
        // everything and every instance has applied it
        private void settle() {
            await(() -> instances.stream().allMatch(instance -> instance.core.l1().asMap().values().stream()
                    .allMatch(CompletableFuture::isDone)) && backend.isIdle()
                    && instances.stream().allMatch(instance -> instance.core.isQuiescent()));
        }

        @Override
        public void close() {
            for (DistributedSide instance : instances) {
                try {
                    instance.cache.close();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            backend.close();
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // comparing

    private static Object outcome(Call call, Side side) {
        try {
            return normalize(call.apply(side));
        } catch (RuntimeException e) {
            return "threw " + unwrap(e).getClass().getSimpleName();
        }
    }

    private static Object normalize(@Nullable Object result) {
        if (result == null) {
            return "null";
        }
        if (result instanceof CompletableFuture<?> future) {
            try {
                return normalize(future.get(10, TimeUnit.SECONDS)); // a future of null is Caffeine's null
            } catch (java.util.concurrent.ExecutionException e) {
                return "failed " + unwrap(e.getCause()).getClass().getSimpleName();
            } catch (Exception e) {
                throw new AssertionError("future not completed", e);
            }
        }
        if (result instanceof Map<?, ?> map) {
            Map<String, Object> normalized = new TreeMap<>();
            map.forEach((key, value) -> normalized.put(String.valueOf(key), normalize(value)));
            return normalized;
        }
        if (result instanceof Collection<?> collection) {
            return collection.stream().map(CaffeineParityTest::normalize).map(String::valueOf).sorted().toList();
        }
        return result;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable cause = failure;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static List<Long> statsOf(CacheStats stats) {
        return List.of(stats.hitCount(), stats.missCount(), stats.loadSuccessCount(), stats.loadFailureCount(),
                stats.evictionCount());
    }

    private static List<String> sorted(Collection<String> values) {
        return values.stream().sorted().toList();
    }

    private static List<String> entries(Set<Map.Entry<String, String>> entries) {
        return entries.stream().map(entry -> entry.getKey() + "=" + entry.getValue()).sorted().toList();
    }

    // up to a second
    private static void awaitQuietly(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

}
