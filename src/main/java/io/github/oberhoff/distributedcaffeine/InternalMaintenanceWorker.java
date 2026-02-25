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
import io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status;
import org.bson.types.ObjectId;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.HASH;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.KEY;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.STALE;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.STATUS;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field.TOUCHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Field._ID;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.CACHED_GROUP;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.Status.EVICTED_EXTENDED_GROUP;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.splitIntoPartitions;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Collections.synchronizedSet;
import static java.util.stream.Collectors.toSet;

class InternalMaintenanceWorker<K, V> implements InternalLazyInitializer<K, V> {

    private static final Duration MAINTENANCE_INTERVAL = Duration.ofSeconds(1);
    private static final int MAX_IN_CLAUSE = 1_000;
    private static final Duration OUTDATED_DURATION = Duration.ofMinutes(1);

    private final AtomicBoolean isActivated;
    private final Set<ObjectId> toBeMarkedAsStale;
    private final Set<Integer> maybeToBeMarkedAsStaleForExtendedPersistence;
    private final AtomicBoolean checkToBeMarkedAsStaleForExtendedPersistenceBySize;
    private final AtomicBoolean checkForInconsistencies;

    private Logger logger;
    private String mongoCollectionName;
    private InternalMongoRepository<K, V> mongoRepository;
    private InternalCacheManager<K, V> cacheManager;
    private ExtendedPersistenceConfigurer extendedPersistenceConfigurer;
    private Executor executor;
    private CompletableFuture<Void> maintenanceCompletableFuture;

    InternalMaintenanceWorker() {
        this.isActivated = new AtomicBoolean(false);
        // LinkedHashSet for FIFO without duplicate elements
        this.toBeMarkedAsStale = synchronizedSet(new LinkedHashSet<>());
        this.maybeToBeMarkedAsStaleForExtendedPersistence = synchronizedSet(new LinkedHashSet<>());
        this.checkToBeMarkedAsStaleForExtendedPersistenceBySize = new AtomicBoolean(false);
        this.checkForInconsistencies = new AtomicBoolean(false);
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.logger = distributedCaffeine.getLogger();
        this.mongoCollectionName = distributedCaffeine.getMongoCollection().getNamespace().getCollectionName();
        this.mongoRepository = distributedCaffeine.getMongoRepository();
        this.cacheManager = distributedCaffeine.getCacheManager();
        this.extendedPersistenceConfigurer = distributedCaffeine.getExtendedPersistenceConfigurer();
        this.executor = distributedCaffeine.getExecutor();
    }

    void activate() {
        // wait for completion if required
        Optional.ofNullable(maintenanceCompletableFuture)
                .ifPresent(CompletableFuture::join);

        isActivated.set(true);
        checkForInconsistencies.set(true);

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
                    .filter(it -> !it.isStale())
                    .map(InternalCacheDocument::getId)
                    .ifPresent(toBeMarkedAsStale::add);
        }
    }

    void queueOutboundActivity(Set<InternalCacheDocument<K, V>> cacheDocuments) {
        if (isActivated()) {
            if (extendedPersistenceConfigurer.isConfigured()) {
                maybeToBeMarkedAsStaleForExtendedPersistence.addAll(cacheDocuments.stream()
                        .filter(cacheDocument -> cacheDocument.isCached() || cacheDocument.isEvictedExtended())
                        .map(InternalCacheDocument::getHash)
                        .collect(toSet()));
            }
            if (extendedPersistenceConfigurer.getMaximumSize().isPresent()) {
                checkToBeMarkedAsStaleForExtendedPersistenceBySize.set(cacheDocuments.stream()
                        .anyMatch(cacheDocument -> cacheDocument.isCached() || cacheDocument.isEvictedExtended()));
            }
        }
    }

    private void scheduleMaintenanceWork() {
        RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                .handleResultIf(result -> isActivated())
                .withMaxAttempts(-1)
                .withDelay(MAINTENANCE_INTERVAL)
                .withDelayFnOn(context -> MAINTENANCE_INTERVAL.multipliedBy(min(context.getAttemptCount(), 10)),
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
            processInconsistenciesAsync();
        }
    }

    private void processToBeMarkedAsStale() {
        // traversing must be synchronized additionally (see documentation of synchronizedSet())
        synchronized (toBeMarkedAsStale) {
            Set<ObjectId> batch = toBeMarkedAsStale.stream()
                    .limit(MAX_IN_CLAUSE)
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
                        .limit(MAX_IN_CLAUSE)
                        .collect(toSet());
                // collect hashes by status for batch
                Set<Integer> hashes = new HashSet<>();
                mongoRepository.consumeHashesWithCountGreaterOrEqual(
                        batch, Set.of(EVICTED_EXTENDED_GROUP),
                        false, 1,
                        stream -> hashes.addAll(stream.toList()));
                // retain hashes with count > 1
                Set<Integer> countedHashes = new HashSet<>();
                mongoRepository.consumeHashesWithCountGreaterOrEqual(
                        hashes, null,
                        null, 2,
                        stream -> stream.forEach(countedHashes::add));
                hashes.retainAll(countedHashes);
                // process cache entries per partition
                mongoRepository.consumeCacheDocumentsGroupedByKeyNewestFirstForHashes(
                        hashes, null,
                        null, Set.of(_ID, HASH, KEY, STALE, TOUCHED),
                        stream -> stream.forEach(cacheDocuments ->
                                cacheDocuments.stream()
                                        .skip(1)
                                        .forEach(this::queueReplacement)));
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
            extendedPersistenceConfigurer.getMaximumSize().ifPresent(maximumSize -> {
                // avoid unnecessary work
                long count = mongoRepository.count(Set.of(EVICTED_EXTENDED_GROUP), false);
                if (count > maximumSize) {
                    // collect hashes by status
                    Set<Integer> hashes = new HashSet<>();
                    mongoRepository.consumeHashesWithCountGreaterOrEqual(
                            null, Set.of(EVICTED_EXTENDED_GROUP),
                            false, 1,
                            stream -> hashes.addAll(stream.toList()));
                    // split hashes into partitions
                    List<Set<Integer>> partitionedHashes = splitIntoPartitions(hashes, MAX_IN_CLAUSE);
                    // process cache entries per partition
                    Set<InternalCacheDocument<K, V>> sortedCacheDocuments = new TreeSet<>();
                    Set<Status> statuses = Stream.of(CACHED_GROUP, EVICTED_EXTENDED_GROUP)
                            .flatMap(Stream::of)
                            .collect(toSet());
                    partitionedHashes.forEach(partition ->
                            mongoRepository.consumeCacheDocumentsGroupedByKeyNewestFirstForHashes(
                                    partition, statuses,
                                    null, Set.of(_ID, HASH, KEY, STATUS, STALE, TOUCHED),
                                    stream -> stream.forEach(cacheDocuments ->
                                            cacheDocuments.stream()
                                                    .findFirst()
                                                    .filter(InternalCacheDocument::isEvictedExtended)
                                                    .map(InternalCacheDocument::weakened)
                                                    .ifPresent(sortedCacheDocuments::add))));
                    if (sortedCacheDocuments.size() > maximumSize) {
                        int limit = sortedCacheDocuments.size() - maximumSize;
                        sortedCacheDocuments.stream()
                                .limit(limit)
                                .forEach(this::queueReplacement);
                    }
                }
            });
            checkToBeMarkedAsStaleForExtendedPersistenceBySize.set(false);
        }
    }

    private void processCleanUp() {
        cacheManager.manageCleanUp(OUTDATED_DURATION);
    }

    private void processInconsistenciesAsync() {
        if (checkForInconsistencies.get()) {
            CompletableFuture.runAsync(() -> {
                // collect hashes with count > 1
                List<Integer> hashes = new ArrayList<>();
                mongoRepository.consumeHashesWithCountGreaterOrEqual(
                        null, null,
                        null, 2,
                        stream -> hashes.addAll(stream.toList()));
                // split hashes into partitions
                List<Set<Integer>> partitionedHashes = splitIntoPartitions(hashes, MAX_IN_CLAUSE);
                // process cache entries per partition
                partitionedHashes.forEach(partition ->
                        mongoRepository.consumeCacheDocumentsGroupedByKeyNewestFirstForHashes(partition, null,
                                null, Set.of(_ID, HASH, KEY, STALE, TOUCHED),
                                stream -> stream.forEach(groupedCacheDocuments ->
                                        groupedCacheDocuments.stream()
                                                .skip(1)
                                                .forEach(this::queueReplacement))));
            }, executor);
            // process only once per activation
            checkForInconsistencies.set(false);
            // TODO logging on exception?
        }
    }
}
