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
package io.github.oberhoff.distributedcaffeine.common;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.spy;

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

    protected <K, V> DistributedCache<K, V> createCache(Adapter<K, V> adapter,
                                                        CacheBuilder<K, V> cacheBuilder,
                                                        CacheConstructor<K, V> cacheConstructor) {
        DistributedCache<K, V> distributedCache = cacheConstructor
                .construct(cacheBuilder.apply(DistributedCaffeine.newBuilder(adapter)));
        distributedCacheInstances.add(distributedCache);
        return distributedCache;
    }

    @SuppressWarnings("java:S2925")
    protected void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"SameReturnValue", "TypeParameterUnusedInFormals"})
    protected <T> T _null() {
        return null;
    }

    protected <T> Set<T> _set(@Nullable T element) {
        return singleton(element);
    }

    protected <K, V> Map<K, V> _map(@Nullable K key, @Nullable V value) {
        return singletonMap(key, value);
    }

    protected <T, R> R injectSpy(Object instanceObject, Class<T> instanceClass, String fieldName, Class<? super R> fieldClass) {
        R spy = spy(readFieldValue(instanceObject, instanceClass, fieldName, fieldClass));
        writeFieldValue(instanceObject, instanceClass, fieldName, spy);
        return spy;
    }

    @SuppressWarnings("unchecked")
    protected <T, R> R readFieldValue(Object instanceObject, Class<T> instanceClass, String fieldName, Class<? super R> fieldClass) {
        return (R) ReflectionUtils.tryToReadFieldValue(instanceClass, fieldName, instanceClass.cast(instanceObject))
                .toOptional()
                .filter(fieldClass::isInstance)
                .orElseThrow(NoSuchFieldError::new);
    }

    protected <T> void writeFieldValue(Object instanceObject, Class<T> instanceClass, String fieldName, Object fieldValue) {
        Predicate<Field> fieldPredicate = field -> field.getName().equals(fieldName);
        Field field = ReflectionUtils.streamFields(instanceClass, fieldPredicate, ReflectionUtils.HierarchyTraversalMode.TOP_DOWN)
                .findFirst()
                .orElseThrow(NoSuchFieldError::new);
        ReflectionUtils.makeAccessible(field);
        try {
            field.set(instanceObject, fieldValue);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"unchecked", "UnusedReturnValue", "SameParameterValue", "TypeParameterUnusedInFormals"})
    protected <T, R> R invokeMethod(Object instanceObject, Class<T> instanceClass, String methodName, List<Class<?>> parameterClasses, List<Object> parameterObjects) {
        return (R) ReflectionUtils.invokeMethod(
                ReflectionUtils.findMethod(instanceClass, methodName, parameterClasses.toArray(Class[]::new))
                        .orElseThrow(NoSuchMethodError::new),
                instanceObject, parameterObjects.toArray(Object[]::new));
    }

    @FunctionalInterface
    protected interface CacheBuilder<K, V> {

        DistributedCaffeine<K, V> apply(DistributedCaffeine<K, V> builder);

        static <K, V> CacheBuilder<K, V> identity() {
            return cacheBuilder -> cacheBuilder;
        }
    }

    @FunctionalInterface
    protected interface CacheConstructor<K, V> {

        DistributedCache<K, V> construct(DistributedCaffeine<K, V> builder);
    }

    @FunctionalInterface
    protected interface CacheFactory<K, V> {

        DistributedCache<K, V> create(CacheBuilder<K, V> cacheBuilder, CacheConstructor<K, V> cacheConstructor);
    }
}
