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

import io.github.oberhoff.distributedcaffeine.adapter.AbstractAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractPublisher;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractSynchronizer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION;
import static java.lang.String.format;
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
class DistributedCaffeineConvergenceTests {

    private static final int INSTANCES = 3;
    private static final int KEYS = 8;
    private static final int STEPS = 300;

    @DisplayName("Test that cache instances converge on the published order under random interleavings")
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L})
    void test_convergence_under_random_interleavings(long seed) {
        Random random = new Random(seed);
        InMemoryBroker<Key, Value> broker = new InMemoryBroker<>();
        List<AbstractAdapter<Key, Value>> adapters = new ArrayList<>();
        List<DistributedCache<Key, Value>> caches = new ArrayList<>();
        for (int instance = 0; instance < INSTANCES; instance++) {
            AbstractAdapter<Key, Value> adapter = broker.newAdapter("in-memory-" + instance);
            adapters.add(adapter);
            caches.add(DistributedCaffeine.<Key, Value>newBuilder(adapter)
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
    private static final class InMemoryBroker<K, V> {

        private final List<CacheEntry<K, V>> log;
        private final List<InMemorySynchronizer<K, V>> synchronizers;

        private InMemoryBroker() {
            this.log = new ArrayList<>();
            this.synchronizers = new ArrayList<>();
        }

        /**
         * Returns an adapter on this broker, for one cache instance.
         */
        private AbstractAdapter<K, V> newAdapter(String identifier) {
            InMemorySynchronizer<K, V> synchronizer = new InMemorySynchronizer<>(this);
            synchronizers.add(synchronizer);
            return new InMemoryAdapter<>(new InMemoryPublisher<>(this), synchronizer, identifier);
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

            private InMemoryAdapter(InMemoryPublisher<K, V> publisher, InMemorySynchronizer<K, V> synchronizer,
                                    String identifier) {
                super(publisher, synchronizer, identifier);
                this.inMemorySynchronizer = synchronizer;
            }
        }

        private static final class InMemoryPublisher<K, V> extends AbstractPublisher<K, V> {

            private final InMemoryBroker<K, V> broker;

            private InMemoryPublisher(InMemoryBroker<K, V> broker) {
                this.broker = broker;
            }

            @Override
            public void publishCacheEntries(Collection<CacheEntry<K, V>> cacheEntries) {
                broker.publish(cacheEntries);
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
