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
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.CachedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.EvictedEntryPersistenceConfigurer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntryMetadata;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.InternalUtils.getFailable;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.requireRepository;
import static io.github.oberhoff.distributedcaffeine.InternalUtils.runFailable;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.CACHED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.DISTRIBUTION_ONLY_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_RETAINED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.STALE;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

@SuppressWarnings("java:S1450")
class InternalMaintenanceWorker<K, V> implements InternalInitializable<K, V> {

    @SuppressWarnings({"java:S116", "FieldMayBeFinal", "CanBeFinal"}) // not static final for testing
    private Duration MAINTENANCE_INTERVAL = Duration.ofMinutes(1);
    private static final Duration DISTRIBUTION_DURATION = Duration.ofMinutes(1);
    private static final Set<Status> NOT_RETAINED_GROUP = Stream
            .concat(DISTRIBUTION_ONLY_GROUP.stream(), CACHED_GROUP.stream())
            .collect(toUnmodifiableSet());

    private final AtomicBoolean isActivated;
    private CompletableFuture<Void> maintenanceCompletableFuture;

    @SuppressWarnings("NotNullFieldNotInitialized")
    private Logger logger;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private String identifier;
    private @Nullable Repository<K, V> repository;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalCacheManager<K, V> cacheManager;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private CachedEntryPersistenceConfigurer cachedEntryPersistenceConfigurer;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private EvictedEntryPersistenceConfigurer evictedEntryPersistenceConfigurer;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private DistributionMode distributionMode;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalMaintenanceWorker() {
        this.isActivated = new AtomicBoolean(false);
        maintenanceCompletableFuture = CompletableFuture.completedFuture(null);
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.logger = instanceRegistry.getLogger();
        this.identifier = instanceRegistry.getAdapter().getIdentifier();
        this.repository = instanceRegistry.getAdapter().getRepository().orElse(null);
        this.cacheManager = instanceRegistry.getCacheManager();
        this.cachedEntryPersistenceConfigurer = instanceRegistry.getCachedEntryPersistenceConfigurer();
        this.evictedEntryPersistenceConfigurer = instanceRegistry.getEvictedEntryPersistenceConfigurer();
        this.distributionMode = instanceRegistry.getDistributionMode();
    }

    void activate() {
        // wait for completion if required
        if (!maintenanceCompletableFuture.isDone()) {
            maintenanceCompletableFuture.join();
        }

        isActivated.set(true);

        scheduleMaintenanceWork();
    }

    void deactivate() {
        isActivated.set(false);

        if (!maintenanceCompletableFuture.isDone()) {
            maintenanceCompletableFuture.cancel(true);
        }
    }

    boolean isActivated() {
        return isActivated.get();
    }

    @SuppressWarnings("FutureReturnValueIgnored") // the shutdown callback below is attached for its side effect
    private void scheduleMaintenanceWork() {
        RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                .handleResultIf(result -> isActivated())
                .withMaxAttempts(-1)
                .withDelay(MAINTENANCE_INTERVAL)
                .withDelayFnOn(context -> MAINTENANCE_INTERVAL.multipliedBy(min(context.getAttemptCount(), 10)),
                        Throwable.class)
                .onRetryScheduled(event -> Optional.ofNullable(event.getLastException())
                        .ifPresent(throwable -> logger.log(Level.WARNING,
                                format("Maintenance failed for cache at '%s'. Retrying...",
                                        identifier), throwable)))
                .build();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        // keep the future returned by Failsafe itself, because only its cancel() aborts the retry loop
        // (dev.failsafe.spi.FailsafeFuture overrides cancel() but not newIncompleteFuture(), so the stage derived
        // from whenComplete() is a plain CompletableFuture whose cancel() merely completes that stage while the
        // loop keeps running - and reports isDone() == true, which would let activate() skip its join() below)
        CompletableFuture<Void> failsafeCompletableFuture = Failsafe.with(retryPolicy)
                .with(executorService)
                .runAsync(() -> processMaintenance(DISTRIBUTION_DURATION));
        failsafeCompletableFuture.whenComplete((result, throwable) -> executorService.shutdown());
        maintenanceCompletableFuture = failsafeCompletableFuture;
    }

    @SuppressWarnings("SameParameterValue")
    private void processMaintenance(Duration distributionDuration) {
        if (isActivated()) {
            // TODO check for real activities
            processCleanUp();
            processCachedEntryPersistenceByTime(distributionDuration);
            processCachedEntryPersistenceBySize(distributionDuration);
            processEvictedEntryPersistenceByTime(distributionDuration);
            processEvictedEntryPersistenceBySize();
            // intentionally at last position
            processNotRetained(distributionDuration);
        }
    }

    private void processCleanUp() {
        cacheManager.cleanup();
    }

    private void processCachedEntryPersistenceBySize(Duration distributionDuration) {
        cachedEntryPersistenceConfigurer.getMaximumSize().ifPresent(maximumSize -> {
            Repository<K, V> retaining = requireRepository(repository, identifier);
            Long count = getFailable(() ->
                    retaining.countCacheEntries(CACHED_GROUP));
            if (count > maximumSize) {
                long limit = count - maximumSize;
                Set<String> hashes = new HashSet<>();
                try (Stream<CacheEntryMetadata> cacheEntryMetadataStream = getFailable(() ->
                        retaining.streamCacheEntryMetadata(
                                null,
                                CACHED_GROUP,
                                true))) {
                    cacheEntryMetadataStream
                            .limit(limit)
                            .map(CacheEntryMetadata::getHash)
                            .forEach(hashes::add);
                }
                if (!hashes.isEmpty()) {
                    Instant deadline = Instant.now().minus(distributionDuration);
                    runFailable(() -> retaining.deleteCacheEntries(hashes, CACHED_GROUP, deadline));
                }
            }
        });
    }

    private void processCachedEntryPersistenceByTime(Duration distributionDuration) {
        cachedEntryPersistenceConfigurer.getMaximumTime().ifPresent(maximumTime -> {
            Repository<K, V> retaining = requireRepository(repository, identifier);
            Instant now = Instant.now().minus(distributionDuration);
            Instant min = Instant.ofEpochMilli(Long.MIN_VALUE);
            Instant deadline = maximumTime.compareTo(Duration.between(min, now)) > 0
                    ? min
                    : now.minus(maximumTime);
            runFailable(() -> retaining.deleteCacheEntries(null, CACHED_GROUP, deadline));
        });
    }

    private void processEvictedEntryPersistenceBySize() {
        evictedEntryPersistenceConfigurer.getMaximumSize().ifPresent(maximumSize -> {
            Repository<K, V> retaining = requireRepository(repository, identifier);
            Long count = getFailable(() ->
                    retaining.countCacheEntries(EVICTED_RETAINED_GROUP));
            if (count > maximumSize) {
                long limit = count - maximumSize;
                Set<String> hashes = new HashSet<>(maximumSize);
                try (Stream<CacheEntryMetadata> cacheEntryMetadataStream = getFailable(() ->
                        retaining.streamCacheEntryMetadata(
                                null,
                                EVICTED_RETAINED_GROUP,
                                true))) {
                    cacheEntryMetadataStream
                            .limit(limit)
                            .map(CacheEntryMetadata::getHash)
                            .forEach(hashes::add);
                }
                if (!hashes.isEmpty()) {
                    // transition the status (instead of hard delete)
                    runFailable(() -> retaining.updateStatusOfCacheEntries(hashes,
                            EVICTED_RETAINED_GROUP, null, pruningStatus()));
                }
            }
        });
    }

    private void processEvictedEntryPersistenceByTime(Duration distributionDuration) {
        evictedEntryPersistenceConfigurer.getMaximumTime().ifPresent(maximumTime -> {
            Repository<K, V> retaining = requireRepository(repository, identifier);
            Instant now = Instant.now().minus(distributionDuration);
            Instant min = Instant.ofEpochMilli(Long.MIN_VALUE);
            Instant deadline = maximumTime.compareTo(Duration.between(min, now)) > 0
                    ? min
                    : now.minus(maximumTime);
            // transition the status (instead of hard delete)
            runFailable(() -> retaining.updateStatusOfCacheEntries(null,
                    EVICTED_RETAINED_GROUP, deadline, pruningStatus()));
        });
    }

    // Without persistence configured for them, cached entries are kept for distribution only as well, so
    // they go the same way a removal does. Deliberately a delete: a transition could only be to STALE, which no
    // distribution mode considers, so it would just add change stream events every cache instance discards
    private void processNotRetained(Duration distributionDuration) {
        // what a write leaves behind until it is swept only exists because the underlying store is what distributes
        // it as well. Where distributing does not retain, the delivery is the whole of the record and there is
        // nothing left over to collect
        @Nullable Repository<K, V> retaining = repository;
        if (nonNull(retaining)) {
            Repository<K, V> retainingRepository = retaining;
            Instant deadline = Instant.now().minus(distributionDuration);
            Set<Status> statuses = cachedEntryPersistenceConfigurer.isConfigured()
                    ? DISTRIBUTION_ONLY_GROUP
                    : NOT_RETAINED_GROUP;
            runFailable(() -> retainingRepository.deleteCacheEntries(null,
                    statuses, deadline));
        }
    }

    // The data store is the authority on what a cache entry's value is and on whether it was invalidated. Which
    // cache instance keeps which key in memory is a different matter, and who owns that decision is what the
    // distribution mode says - which is why pruning the tier of evicted cache entries cannot mean the same thing in
    // every mode.
    // Where evictions are distributed, residency is a property of the whole set of cache instances: none of them
    // holds what the data store no longer backs, so the removal is theirs to follow and an invalidation states it.
    // Where evictions are not distributed, residency is deliberately local - a cache instance is meant to go on
    // serving a key another one evicted - so the same transition must state nothing about anyone's content. It only
    // records that the data store is done keeping the value, which is what STALE says and what no distribution mode
    // considers
    private Status pruningStatus() {
        return distributionMode.isEvictionConsidered()
                ? INVALIDATED
                : STALE;
    }
}
