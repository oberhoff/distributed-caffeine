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

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.ExtendedPersistenceConfigurer;
import org.bson.types.ObjectId;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static java.util.Collections.synchronizedSet;
import static java.util.stream.Collectors.toSet;

class InternalMaintenanceWorker<K, V> implements InternalLazyInitializer<K, V> {

    private static final Duration MAINTENANCE_INTERVAL = Duration.ofSeconds(1);
    private static final Integer MAX_QUEUE_DRAIN_SIZE = 1_000;
    private static final Duration OUTDATED_DURATION = Duration.ofMinutes(1);

    private final AtomicBoolean isActivated;
    private final Set<ObjectId> toBeMarkedAsStale;
    private final Set<Integer> maybeToBeMarkedAsStaleForExtendedPersistence;
    private final AtomicBoolean checkToBeMarkedAsStaleForExtendedPersistenceBySize;

    private Logger logger;
    private String mongoCollectionName;
    private InternalMongoRepository<K, V> mongoRepository;
    private InternalCacheManager<K, V> cacheManager;
    private ExtendedPersistenceConfigurer extendedPersistenceConfigurer;
    private CompletableFuture<Void> maintenanceCompletableFuture;

    InternalMaintenanceWorker() {
        this.isActivated = new AtomicBoolean(false);
        // LinkedHashSet for FIFO without duplicate elements
        this.toBeMarkedAsStale = synchronizedSet(new LinkedHashSet<>());
        this.maybeToBeMarkedAsStaleForExtendedPersistence = synchronizedSet(new LinkedHashSet<>());
        this.checkToBeMarkedAsStaleForExtendedPersistenceBySize = new AtomicBoolean(false);
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.logger = distributedCaffeine.getLogger();
        this.mongoCollectionName = distributedCaffeine.getMongoCollection().getNamespace().getCollectionName();
        this.mongoRepository = distributedCaffeine.getMongoRepository();
        this.cacheManager = distributedCaffeine.getCacheManager();
        this.extendedPersistenceConfigurer = distributedCaffeine.getExtendedPersistenceConfigurer();
    }

    void activate() {
        // wait for completion if required
        Optional.ofNullable(maintenanceCompletableFuture)
                .ifPresent(CompletableFuture::join);

        isActivated.set(true);

        scheduleMaintenanceWork();
    }

    void deactivate() {
        isActivated.set(false);
        toBeMarkedAsStale.clear();
        maybeToBeMarkedAsStaleForExtendedPersistence.clear();
        checkToBeMarkedAsStaleForExtendedPersistenceBySize.set(false);
    }

    boolean isActivated() {
        return isActivated.get();
    }

    void queueReplacement(InternalCacheDocument<K, V> cacheDocument) {
        if (isActivated()) {
            Optional.ofNullable(cacheDocument)
                    .filter(it -> it.isCached() && !it.isStale())
                    .map(InternalCacheDocument::getId)
                    .ifPresent(toBeMarkedAsStale::add);
        }
    }

    void queueActivity(Set<InternalCacheDocument<K, V>> cacheDocuments) {
        if (isActivated()) {
            if (extendedPersistenceConfigurer.isConfigured()) {
                maybeToBeMarkedAsStaleForExtendedPersistence.addAll(cacheDocuments.stream()
                        .map(InternalCacheDocument::getHash)
                        .collect(toSet()));
            }
            if (extendedPersistenceConfigurer.getMaximumSize().isPresent()) {
                checkToBeMarkedAsStaleForExtendedPersistenceBySize.set(true);
            }
        }
    }

    private void scheduleMaintenanceWork() {
        RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                .handleResultIf(result -> isActivated())
                .withMaxAttempts(-1)
                .withDelay(MAINTENANCE_INTERVAL)
                .withDelayFnOn(context -> MAINTENANCE_INTERVAL.multipliedBy(Math.min(context.getAttemptCount(), 10)),
                        Throwable.class)
                .onRetryScheduled(event -> Optional.ofNullable(event.getLastException())
                        .ifPresent(throwable -> logger.log(Level.WARNING,
                                format("Maintenance failed for collection '%s'. Retrying...",
                                        mongoCollectionName), throwable)))
                .build();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        InternalTaskPolicy<Void> taskPolicy = new InternalTaskPolicy<Void>()
                .withPostExecutionTask(result -> executorService.shutdown());
        maintenanceCompletableFuture = Failsafe.with(taskPolicy, retryPolicy)
                .with(executorService)
                .runAsync(this::processMaintenance);
    }

    private void processMaintenance() {
        if (isActivated()) {
            processToBeMarkedAsStale();
            processMaybeToBeMarkedAsStaleForExtendedPersistence();
            processToBeMarkedAsStaleForExtendedPersistenceBySize();
            processCleanUp();
        }
    }

    private void processToBeMarkedAsStale() {
        // traversing must be synchronized additionally (see documentation of synchronizedSet())
        synchronized (toBeMarkedAsStale) {
            Set<ObjectId> batch = toBeMarkedAsStale.stream()
                    .limit(MAX_QUEUE_DRAIN_SIZE)
                    .collect(toSet());
            mongoRepository.markAsStale(batch);
            toBeMarkedAsStale.removeAll(batch);
        }
    }

    private void processMaybeToBeMarkedAsStaleForExtendedPersistence() {
        // avoid unnecessary work
        if (extendedPersistenceConfigurer.isConfigured()
                && toBeMarkedAsStale.isEmpty()) {
            // traversing must be synchronized additionally (see documentation of synchronizedSet())
            synchronized (maybeToBeMarkedAsStaleForExtendedPersistence) {
                Set<Integer> batch = maybeToBeMarkedAsStaleForExtendedPersistence.stream()
                        .limit(MAX_QUEUE_DRAIN_SIZE)
                        .collect(toSet());
                toBeMarkedAsStale.addAll(mongoRepository.identifyStaleExtended(batch));
                maybeToBeMarkedAsStaleForExtendedPersistence.removeAll(batch);
            }
        }
    }

    private void processToBeMarkedAsStaleForExtendedPersistenceBySize() {
        // avoid unnecessary work
        if (extendedPersistenceConfigurer.getMaximumSize().isPresent()
                && toBeMarkedAsStale.isEmpty()
                && maybeToBeMarkedAsStaleForExtendedPersistence.isEmpty()
                && checkToBeMarkedAsStaleForExtendedPersistenceBySize.get()) {
            toBeMarkedAsStale.addAll(mongoRepository.identifyStaleExtendedBySize());
            checkToBeMarkedAsStaleForExtendedPersistenceBySize.set(false);
        }
    }

    private void processCleanUp() {
        cacheManager.manageCleanUp(OUTDATED_DURATION);
    }
}
