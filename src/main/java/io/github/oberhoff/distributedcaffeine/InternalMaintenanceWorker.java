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
package io.github.oberhoff.distributedcaffeine;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.LazyInitializer;
import org.bson.types.ObjectId;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static java.util.Objects.nonNull;

class InternalMaintenanceWorker<K, V> implements LazyInitializer<K, V> {

    private final AtomicBoolean isActivated;
    private final PriorityBlockingQueue<ObjectId> toBeOrphaned;

    private Logger logger;
    private String mongoCollectionName;
    private InternalMongoRepository<K, V> mongoRepository;
    private CompletableFuture<Void> maintenanceCompletableFuture;

    InternalMaintenanceWorker() {
        this.isActivated = new AtomicBoolean(false);
        this.toBeOrphaned = new PriorityBlockingQueue<>();
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.logger = distributedCaffeine.getLogger();
        this.mongoCollectionName = distributedCaffeine.getMongoCollection().getNamespace().getCollectionName();
        this.mongoRepository = distributedCaffeine.getMongoRepository();
    }

    void activate() {
        // wait for completion if required
        Optional.ofNullable(maintenanceCompletableFuture)
                .ifPresent(CompletableFuture::join);

        scheduleMaintenanceWork();

        isActivated.set(true);
    }

    void deactivate() {
        isActivated.set(false);
    }

    boolean isActivated() {
        return isActivated.get();
    }

    void queueToBeOrphaned(ObjectId objectId) {
        if (nonNull(objectId) && !toBeOrphaned.contains(objectId)) {
            toBeOrphaned.add(objectId);
        }
    }

    private void scheduleMaintenanceWork() {
        RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                .handleResultIf(result -> isActivated())
                .withMaxAttempts(-1)
                .withDelay(Duration.ofSeconds(1))
                .withDelayFnOn(context -> Duration.ofSeconds(Math.min(context.getAttemptCount(), 10)), Throwable.class)
                .onRetryScheduled(event -> Optional.ofNullable(event.getLastException())
                        .ifPresent(throwable -> logger.log(Level.WARNING,
                                format("Maintenance failed for collection '%s'. Retrying...",
                                        mongoCollectionName), throwable)))
                .build();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        InternalTaskPolicy<Void> taskPolicy = new InternalTaskPolicy<Void>()
                .withPostExecutionTask(executorService::shutdown);
        maintenanceCompletableFuture = Failsafe.with(taskPolicy, retryPolicy)
                .with(executorService)
                .runAsync(this::processMaintenance);
    }

    private void processMaintenance() {
        if (isActivated()) {
            HashSet<ObjectId> drained = new HashSet<>();
            toBeOrphaned.drainTo(drained, 10_000);
            mongoRepository.updateOrphaned(drained);
        }
    }
}
