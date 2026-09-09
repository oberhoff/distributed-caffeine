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
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.CachedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractPublisher;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractSynchronizer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractRepository;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntryMetadata;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import io.github.oberhoff.distributedcaffeine.common.Key;
import io.github.oberhoff.distributedcaffeine.common.Value;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import java.time.Instant;

import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION;
import static io.github.oberhoff.distributedcaffeine.DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION;
import static java.util.Comparator.comparing;
import static java.util.Objects.isNull;
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
        DistributedCache<Key, Value> cache = DistributedCaffeine.<Key, Value>newBuilder(adapter)
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
        // what the store keeps, one record per key, which is exactly what a reactivation reads back
        private final Map<String, CacheEntry<K, V>> retained;
        private final List<InMemorySynchronizer<K, V>> synchronizers;

        private InMemoryBroker() {
            this.log = new ArrayList<>();
            this.retained = new LinkedHashMap<>();
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
                                                               boolean orderByTimestampAsc) {
                return matching(hashes, statuses, null, orderByTimestampAsc);
            }

            @Override
            public Stream<CacheEntryMetadata> streamCacheEntryMetadata(@Nullable Set<String> hashes,
                                                                       @Nullable Set<Status> statuses,
                                                                       boolean orderByTimestampAsc) {
                return matching(hashes, statuses, null, orderByTimestampAsc)
                        .map(cacheEntry -> CacheEntryMetadata.of(cacheEntry.getHash(), cacheEntry.getOperation(),
                                cacheEntry.getStatus(), cacheEntry.getTimestamp()));
            }

            @Override
            public void updateStatusOfCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                                   @Nullable Instant olderThan, Status newStatus) {
                matching(hashes, statuses, olderThan, false)
                        .toList()
                        .forEach(cacheEntry -> broker.retained.put(cacheEntry.getHash(), CacheEntry.of(
                                cacheEntry.getHash(), null, cacheEntry.getKey(), cacheEntry.getValue(),
                                newStatus, Instant.now())));
            }

            @Override
            public void deleteCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                           @Nullable Instant olderThan) {
                matching(hashes, statuses, olderThan, false)
                        .toList()
                        .forEach(cacheEntry -> broker.retained.remove(cacheEntry.getHash()));
            }

            @Override
            public long countCacheEntries(@Nullable Set<Status> statuses) {
                return matching(null, statuses, null, false).count();
            }

            private Stream<CacheEntry<K, V>> matching(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                                      @Nullable Instant olderThan, boolean orderByTimestampAsc) {
                Stream<CacheEntry<K, V>> matching = List.copyOf(broker.retained.values()).stream()
                        .filter(cacheEntry -> isNull(hashes) || hashes.contains(cacheEntry.getHash()))
                        .filter(cacheEntry -> isNull(statuses) || statuses.contains(cacheEntry.getStatus()))
                        .filter(cacheEntry -> isNull(olderThan) || cacheEntry.getTimestamp().isBefore(olderThan));
                return orderByTimestampAsc
                        ? matching.sorted(comparing(CacheEntry::getTimestamp))
                        : matching;
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
