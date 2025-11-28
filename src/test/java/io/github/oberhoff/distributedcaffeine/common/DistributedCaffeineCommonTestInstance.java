/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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
package io.github.oberhoff.distributedcaffeine.common;

import com.github.benmanes.caffeine.cache.Cache;
import com.mongodb.client.MongoCollection;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.MethodName.class)
public abstract class DistributedCaffeineCommonTestInstance {

    protected AtomicInteger testCounter;
    protected TestInfo testInfo;
    protected ExecutorService executorService;
    protected Collection<DistributedCache<?, ?>> distributedCacheInstances;

    @BeforeAll
    void beforeAll() {
        this.testCounter = new AtomicInteger(0);
        this.executorService = Executors.newCachedThreadPool();
    }

    @AfterAll
    void afterAll() {
        this.executorService.shutdown();
        Runtime.getRuntime().gc();
    }

    @BeforeEach
    void beforeEach(TestInfo testInfo) {
        this.testCounter.incrementAndGet();
        this.testInfo = testInfo;
        this.distributedCacheInstances = new HashSet<>();
    }

    @AfterEach
    void afterEach() {
        // stop synchronization (release database connections)
        this.distributedCacheInstances.forEach(distributedCache ->
                distributedCache.distributedPolicy().stopSynchronization());
        // invalidate all cache entries (only after synchronization is already stopped for all caches)
        this.distributedCacheInstances.forEach(Cache::invalidateAll);
        this.distributedCacheInstances.clear();
    }

    protected <K, V> DistributedCache<K, V> createCache(MongoCollection<Document> mongoCollection,
                                                        CacheBuilder<K, V> cacheBuilder,
                                                        CacheConstructor<K, V> cacheConstructor) {
        DistributedCache<K, V> distributedCache = cacheConstructor
                .construct(cacheBuilder.apply(DistributedCaffeine.newBuilder(mongoCollection)));
        distributedCacheInstances.add(distributedCache);
        return distributedCache;
    }

    protected void await(Duration duration) {
        await("duration")
                .pollInterval(duration)
                .timeout(duration.plusSeconds(1)) // timeout must be greater than the poll delay
                .until(() -> true);
    }

    protected ConditionFactory await(String alias) {
        return Awaitility.await(alias)
                .pollExecutorService(executorService);
    }

    @SuppressWarnings("SameReturnValue")
    protected <T> T _null() {
        return null;
    }

    protected <T> Set<T> _set(@Nullable T element) {
        return singleton(element);
    }

    protected <K, V> Map<K, V> _map(@Nullable K key, @Nullable V value) {
        return singletonMap(key, value);
    }


    @FunctionalInterface
    protected interface CacheBuilder<K, V> {

        DistributedCaffeine.Builder<K, V> apply(DistributedCaffeine.Builder<K, V> builder);

        static <K, V> CacheBuilder<K, V> identity() {
            return cacheBuilder -> cacheBuilder;
        }
    }

    @FunctionalInterface
    protected interface CacheConstructor<K, V> {

        DistributedCache<K, V> construct(DistributedCaffeine.Builder<K, V> builder);
    }

    @FunctionalInterface
    protected interface CacheFactory<K, V> {

        DistributedCache<K, V> create(CacheBuilder<K, V> cacheBuilder, CacheConstructor<K, V> cacheConstructor);
    }
}
