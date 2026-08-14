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
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.Repository;

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
import static io.github.oberhoff.distributedcaffeine.InternalUtils.runFailable;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.EVICTED_EXTENDED_GROUP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.INVALIDATED;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status.SHORT_LIVING_GROUP;
import static java.lang.Math.min;
import static java.lang.String.format;

@SuppressWarnings("java:S1450")
class InternalMaintenanceWorker<K, V> implements InternalInitializable<K, V> {

    private static final Duration SHORT_LIVING_DURATION = Duration.ofMinutes(1);
    @SuppressWarnings({"java:S116", "FieldMayBeFinal", "CanBeFinal"}) // not static final for testing
    private Duration MAINTENANCE_INTERVAL = Duration.ofMinutes(1);

    private final AtomicBoolean isActivated;
    private CompletableFuture<Void> maintenanceCompletableFuture;

    @SuppressWarnings("NotNullFieldNotInitialized")
    private Logger logger;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private String identifier;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Repository<K, V> repository;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private InternalCacheManager<K, V> cacheManager;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private ExtendedPersistenceConfigurer extendedPersistenceConfigurer;

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
        this.repository = instanceRegistry.getAdapter().getRepository();
        this.cacheManager = instanceRegistry.getCacheManager();
        this.extendedPersistenceConfigurer = instanceRegistry.getExtendedPersistenceConfigurer();
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
                .runAsync(() -> processMaintenance(SHORT_LIVING_DURATION));
        failsafeCompletableFuture.whenComplete((result, throwable) -> executorService.shutdown());
        maintenanceCompletableFuture = failsafeCompletableFuture;
    }

    @SuppressWarnings("SameParameterValue")
    private void processMaintenance(Duration shortLivingDuration) {
        if (isActivated()) {
            // TODO check for real activities
            processCleanUp();
            processExtendedPersistenceByTime();
            processExtendedPersistenceBySize();
            // intentionally at last position
            processShortLived(shortLivingDuration);
        }
    }

    private void processCleanUp() {
        cacheManager.cleanup();
    }

    private void processExtendedPersistenceByTime() {
        extendedPersistenceConfigurer.getMaximumTime().ifPresent(maximumTime -> {
            Instant now = Instant.now();
            Instant min = Instant.ofEpochMilli(Long.MIN_VALUE);
            Instant deadline = maximumTime.compareTo(Duration.between(min, now)) > 0
                    ? min
                    : now.minus(maximumTime);
            // invalidate (instead of hard delete)
            runFailable(() -> repository.updateStatusOfCacheEntries(null,
                    EVICTED_EXTENDED_GROUP, deadline, INVALIDATED));
        });
    }

    private void processExtendedPersistenceBySize() {
        extendedPersistenceConfigurer.getMaximumSize().ifPresent(maximumSize -> {
            Long count = getFailable(() ->
                    repository.countCacheEntries(EVICTED_EXTENDED_GROUP));
            if (count > maximumSize) {
                long limit = count - maximumSize;
                Set<String> hashes = new HashSet<>(maximumSize);
                try (Stream<CacheEntry<K, V>> cacheEntryStream = getFailable(() -> repository.streamCacheEntries(
                        null,
                        EVICTED_EXTENDED_GROUP,
                        null, // TODO use projection
                        true))) {
                    cacheEntryStream
                            .limit(limit)
                            .map(CacheEntry::getHash)
                            .forEach(hashes::add);
                }
                if (!hashes.isEmpty()) {
                    // invalidate (instead of hard delete)
                    runFailable(() -> repository.updateStatusOfCacheEntries(hashes,
                            EVICTED_EXTENDED_GROUP, null, INVALIDATED));
                }
            }
        });
    }

    private void processShortLived(Duration shortLivingDuration) {
        Instant deadline = Instant.now().minus(shortLivingDuration);
        runFailable(() -> repository.deleteCacheEntries(null,
                SHORT_LIVING_GROUP, deadline));
    }
}
