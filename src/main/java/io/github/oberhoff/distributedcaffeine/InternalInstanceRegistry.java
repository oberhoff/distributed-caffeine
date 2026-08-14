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
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.util.Optional;
import java.util.concurrent.Executor;

@SuppressWarnings("UnusedReturnValue")
class InternalInstanceRegistry<K, V> {

    @SuppressWarnings("java:S3416")
    private final Logger logger = System.getLogger(DistributedCaffeine.class.getName());

    private final InternalSynchronizationLock synchronizationLock;
    private final InternalCacheManager<K, V> cacheManager;
    private final InternalMaintenanceWorker<K, V> maintenanceWorker;

    @SuppressWarnings("NotNullFieldNotInitialized")
    private Adapter<K, V> adapter;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalHasher<K> hasher;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private DistributionMode distributionMode;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private SerializersConfigurer<K, V> serializersConfigurer;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private ExtendedPersistenceConfigurer extendedPersistenceConfigurer;
    private @Nullable InternalCacheLoader<K, V> cacheLoader;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalRemovalListener<K, V> removalListener;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalEvictionListener<K, V> evictionListener;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Executor executor;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private StatsCounter statsCounter;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Cache<InternalKey<K>, InternalValue<V>> cache;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalInstanceRegistry() {
        this.synchronizationLock = new InternalSynchronizationLock();
        this.cacheManager = new InternalCacheManager<>();
        this.maintenanceWorker = new InternalMaintenanceWorker<>();
    }

    // Called once while building, deliberately not from activate(), for two reasons.
    // It would be pointless: every field of this registry is assigned while building and never reassigned, so a later
    // activation cannot offer an initializer anything it did not already see here. That only became true once the
    // cache stopped being rebuilt on every restart - back then the whole point was to hand the components the new
    // cache instance.
    // And it would be unsafe: initializing assigns the fields of components the application is already using by then,
    // and the read path (getIfPresent, getAllPresent) deliberately does not take the synchronization lock, so those
    // readers would have no guarantee of ever seeing the reassignment - holding the lock here would not protect them.
    // Wiring while the instance has not been handed out yet avoids that question entirely
    void initializeComponents() {
        initialize(cacheManager);
        initialize(maintenanceWorker);
        // a cache built without a cache loader has none
        Optional.ofNullable(cacheLoader).ifPresent(this::initialize);
        initialize(removalListener);
        initialize(evictionListener);

        adapter.setKeySerializer(serializersConfigurer.getKeySerializer());
        adapter.setValueSerializer(serializersConfigurer.getValueSerializer());
        adapter.setRetriever(cacheManager);
    }

    // for the parts that are created on demand after building (the cache facade and the views it hands out)
    <T extends InternalInitializable<K, V>> T initialize(T instance) {
        instance.initialize(this);
        return instance;
    }

    // checking activation outside the lock would not be atomic with acting on it, and this body does not tolerate
    // being entered twice: activating an already activated component joins a worker future that only completes once
    // that component stops, so a second caller would block forever - holding the lock, which freezes every cache
    // operation. Activation is also not instantaneous (it waits for the watcher to report itself started), so the
    // window in which a second caller could slip past an unlocked check is wide
    void activate() {
        synchronizationLock.runLocked(() -> {
            if (isActivated()) {
                return;
            }

            try {
                cacheManager.activate();
                maintenanceWorker.activate();
                adapter.activate();
                // synchronization after retrieving by adapter so that no changes are missed
                cacheManager.synchronizeCacheEntries();
            } catch (RuntimeException e) {
                // activating is not atomic by itself, and a half activated instance cannot be recovered from the
                // outside: isActivated() below requires all three components, so it reports false and deactivate()
                // skips its body, leaving whatever did come up running with no way to stop it. The maintenance
                // worker is the harmful one - its retry loop only ends once it sees itself deactivated, so the
                // worker future never completes, and the next activate() joins it forever while holding this lock.
                // Deactivating is safe for components that never got activated
                adapter.deactivate();
                maintenanceWorker.deactivate();
                cacheManager.deactivate();
                throw e;
            }
        });
    }

    // deliberately without an activation check: isActivated() below requires all three components, so anything less
    // than fully activated would skip the body and leave the components that are up running with no way to stop
    // them - which is reachable both by an activation that failed halfway and by stopping the adapter directly
    // through its own public API. Deactivating a component that is not activated does nothing
    void deactivate() {
        synchronizationLock.runLocked(() -> {
            adapter.deactivate();
            maintenanceWorker.deactivate();
            cacheManager.deactivate();
        });
    }

    boolean isActivated() {
        return adapter.isActivated() && maintenanceWorker.isActivated() && cacheManager.isActivated();
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

    public @Nullable InternalCacheLoader<K, V> getCacheLoader() {
        return cacheLoader;
    }

    public void setCacheLoader(@Nullable InternalCacheLoader<K, V> cacheLoader) {
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
}
