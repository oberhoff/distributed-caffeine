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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.ExtendedPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.SerializersConfigurer;
import io.github.oberhoff.distributedcaffeine.adapter.Adapter;

import java.lang.System.Logger;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedSet;

@SuppressWarnings("UnusedReturnValue")
class InternalInstanceRegistry<K, V> {

    @SuppressWarnings("java:S3416")
    private final Logger logger = System.getLogger(DistributedCaffeine.class.getName());

    private final AtomicBoolean isInitialized;
    private final Set<InternalLazyInitializer<K, V>> lazyInitializers;

    private final InternalSynchronizationLock synchronizationLock;
    private final InternalCacheManager<K, V> cacheManager;
    private final InternalMaintenanceWorker<K, V> maintenanceWorker;

    private Adapter<K, V> adapter;
    private InternalHasher<K> hasher;
    private DistributionMode distributionMode;
    private SerializersConfigurer<K, V> serializersConfigurer;
    private ExtendedPersistenceConfigurer extendedPersistenceConfigurer;
    private InternalCacheLoader<K, V> cacheLoader;
    private InternalRemovalListener<K, V> removalListener;
    private InternalEvictionListener<K, V> evictionListener;
    private Executor executor;
    private StatsCounter statsCounter;
    private Cache<InternalKey<K>, InternalValue<V>> cache;
    private Supplier<InternalInstanceRegistry<K, V>> instanceRegistrySupplier;

    InternalInstanceRegistry() {
        this.isInitialized = new AtomicBoolean(false);
        this.lazyInitializers = synchronizedSet(newSetFromMap(new WeakHashMap<>()));
        this.synchronizationLock = new InternalSynchronizationLock();
        this.cacheManager = initializeLazy(new InternalCacheManager<>());
        this.maintenanceWorker = initializeLazy(new InternalMaintenanceWorker<>());
    }

    <T extends InternalLazyInitializer<K, V>> T initializeLazy(T instance) {
        lazyInitializers.add(instance);
        return instance;
    }

    <T extends InternalLazyInitializer<K, V>> T initializeNowAndLazy(T instance) {
        instance.initialize(this);
        initializeLazy(instance);
        return instance;
    }

    void activate() {
        if (!isActivated()) {
            synchronizationLock.runLocked(() -> {
                if (isInitialized.get()) {
                    swapInstances();
                } else {
                    isInitialized.set(true);
                }

                lazyInitializers.stream()
                        .filter(Objects::nonNull)
                        .forEach(lazyInitializer -> lazyInitializer.initialize(this));

                this.adapter.setKeySerializer(serializersConfigurer.getKeySerializer());
                this.adapter.setValueSerializer(serializersConfigurer.getValueSerializer());
                this.adapter.setRetriever(cacheManager);

                cacheManager.activate();
                maintenanceWorker.activate();
                adapter.activate();
                // synchronization after retrieving by adapter so that no changes are missed
                cacheManager.synchronizeCacheEntries();
            });
        }
    }

    void deactivate() {
        if (isActivated()) {
            synchronizationLock.runLocked(() -> {
                adapter.deactivate();
                maintenanceWorker.deactivate();
                cacheManager.deactivate();
            });
        }
    }

    boolean isActivated() {
        return adapter.isActivated() && maintenanceWorker.isActivated() && cacheManager.isActivated();
    }

    private void swapInstances() {
        Optional.ofNullable(cacheLoader)
                .map(InternalCacheLoader::neutralize)
                .ifPresent(lazyInitializers::remove);
        Optional.ofNullable(removalListener)
                .map(InternalRemovalListener::neutralize)
                .ifPresent(lazyInitializers::remove);
        Optional.ofNullable(evictionListener)
                .map(InternalEvictionListener::neutralize)
                .ifPresent(lazyInitializers::remove);

        InternalInstanceRegistry<K, V> instanceRegistry = instanceRegistrySupplier.get();

        cacheLoader = initializeLazy(instanceRegistry.getCacheLoader());
        removalListener = initializeLazy(instanceRegistry.getRemovalListener());
        evictionListener = initializeLazy(instanceRegistry.getEvictionListener());

        executor = instanceRegistry.getExecutor();
        statsCounter = instanceRegistry.getStatsCounter();
        cache = instanceRegistry.getCache();
        instanceRegistrySupplier = instanceRegistry.getInstanceRegistrySupplier();
    }

    public Logger getLogger() {
        return logger;
    }

    public InternalSynchronizationLock getSynchronizationLock() {
        return synchronizationLock;
    }

    public InternalCacheManager<K, V> getCacheManager() {
        return cacheManager;
    }

    public InternalMaintenanceWorker<K, V> getMaintenanceWorker() {
        return maintenanceWorker;
    }

    public Adapter<K, V> getAdapter() {
        return adapter;
    }

    public void setAdapter(Adapter<K, V> adapter) {
        this.adapter = adapter;
    }

    public InternalHasher<K> getHasher() {
        return hasher;
    }

    public void setHasher(InternalHasher<K> hasher) {
        this.hasher = hasher;
    }

    public DistributionMode getDistributionMode() {
        return distributionMode;
    }

    public void setDistributionMode(DistributionMode distributionMode) {
        this.distributionMode = distributionMode;
    }

    public SerializersConfigurer<K, V> getSerializersConfigurer() {
        return serializersConfigurer;
    }

    public void setSerializersConfigurer(SerializersConfigurer<K, V> serializersConfigurer) {
        this.serializersConfigurer = serializersConfigurer;
    }

    public ExtendedPersistenceConfigurer getExtendedPersistenceConfigurer() {
        return extendedPersistenceConfigurer;
    }

    public void setExtendedPersistenceConfigurer(ExtendedPersistenceConfigurer extendedPersistenceConfigurer) {
        this.extendedPersistenceConfigurer = extendedPersistenceConfigurer;
    }

    public InternalCacheLoader<K, V> getCacheLoader() {
        return cacheLoader;
    }

    public void setCacheLoader(InternalCacheLoader<K, V> cacheLoader) {
        this.cacheLoader = cacheLoader;
    }

    public InternalRemovalListener<K, V> getRemovalListener() {
        return removalListener;
    }

    public void setRemovalListener(InternalRemovalListener<K, V> removalListener) {
        this.removalListener = removalListener;
    }

    public InternalEvictionListener<K, V> getEvictionListener() {
        return evictionListener;
    }

    public void setEvictionListener(InternalEvictionListener<K, V> evictionListener) {
        this.evictionListener = evictionListener;
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public StatsCounter getStatsCounter() {
        return statsCounter;
    }

    public void setStatsCounter(StatsCounter statsCounter) {
        this.statsCounter = statsCounter;
    }

    public Cache<InternalKey<K>, InternalValue<V>> getCache() {
        return cache;
    }

    public void setCache(Cache<InternalKey<K>, InternalValue<V>> cache) {
        this.cache = cache;
    }

    public Supplier<InternalInstanceRegistry<K, V>> getInstanceRegistrySupplier() {
        return instanceRegistrySupplier;
    }

    public void setInstanceRegistrySupplier(Supplier<InternalInstanceRegistry<K, V>> instanceRegistrySupplier) {
        this.instanceRegistrySupplier = instanceRegistrySupplier;
    }
}
