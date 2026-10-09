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
package io.github.oberhoff.distributedcaffeine.adapter.mongodb;

import com.mongodb.MongoClientException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractSynchronizer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoAdapter.WatcherSharingMode;
import org.bson.Document;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoRepository.toCacheEntryOrNull;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.nonNull;

/**
 * Receives for one cache instance through a change stream cursor that it may share with others, as the sharing mode
 * decides.
 * <p>
 * Every activation subscribes anew, with a subscription of its own that does the work for this cache instance:
 * reading the cache entries out of what the cursor hands over and applying them. Activating and deactivating run
 * under the lock of the cache instance, and so does applying - so neither of them ever waits for the work of a
 * subscription, and a subscription that outlives its deactivation simply finds itself closed.
 */
final class MongoSynchronizer<K, V> extends AbstractSynchronizer<K, V> {

    private static final Logger LOGGER = System.getLogger(MongoSynchronizer.class.getName());

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);
    // cache entries handed over in one go - bounded so that a backlog cannot hold the synchronization lock of the
    // cache for arbitrarily long
    private static final int MAXIMUM_BATCH_SIZE = 100;

    private final MongoClient mongoClient;
    private final String databaseName;
    private final String collectionName;
    private final WatcherSharingMode sharingMode;
    private final Object scope;
    // How many documents may wait to be applied before this cache instance is considered to have fallen behind -
    // at which point they are dropped and the cache instance is reconciled instead, so that falling behind costs a
    // reconcile rather than memory without bound. A field rather than a constant only so that a test can lower it
    // before activation instead of producing ten thousand writes
    @SuppressWarnings({"FieldMayBeFinal", "CanBeFinal", "FieldCanBeLocal"})
    private int pendingLimit = 10_000;
    // How long a single operation of watching may take - polls included - before its connection is considered dead
    // and replaced: far above what a poll takes, which is the time the server waits for something to arrive. Taken
    // over by the watcher only from the cache instance that creates it. A field for the same reason as the one above
    @SuppressWarnings({"FieldMayBeFinal", "CanBeFinal", "FieldCanBeLocal"})
    private Duration watcherTimeout = Duration.ofSeconds(10);
    // How long activating waits for watching to take this cache instance in - which, while a shared cursor is being
    // replaced, includes waiting for it to come back. The client's own timeout if it has one, MongoDB's default of 30
    // seconds otherwise. Not final for the same reason as the ones above
    @SuppressWarnings({"FieldMayBeFinal", "CanBeFinal"})
    private Duration activationTimeout;

    private final AtomicReference<@Nullable Subscription> subscription = new AtomicReference<>();

    MongoSynchronizer(MongoClient mongoClient, String databaseName, String collectionName,
                      WatcherSharingMode sharingMode) {
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        MongoCollection<Document> mongoCollection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
        this.activationTimeout = Optional.ofNullable(mongoCollection.getTimeout(TimeUnit.MILLISECONDS))
                .filter(millis -> millis > 0)
                .map(Duration::ofMillis)
                .orElseGet(() -> Duration.ofSeconds(30));
        this.sharingMode = sharingMode;
        this.scope = switch (sharingMode) {
            // a scope nobody else has, which shares the cursor with nobody
            case INSTANCE -> new Object();
            case COLLECTION -> collectionName;
            case DATABASE -> WatcherSharingMode.DATABASE;
        };
    }

    @Override
    public void activate() {
        if (isActivated()) {
            return;
        }
        MongoWatcherRegistry.Key key = new MongoWatcherRegistry.Key(mongoClient, databaseName, scope);
        MongoWatcher watcher = MongoWatcherRegistry.acquire(key, () -> new MongoWatcher(mongoClient, databaseName,
                sharingMode == WatcherSharingMode.DATABASE ? null : collectionName,
                sharingMode != WatcherSharingMode.INSTANCE, watcherTimeout));
        Subscription activated = new Subscription(key, watcher);
        // whatever this replaces is closed, which for the one deactivated before does nothing - and for one that a
        // concurrent activation put there in the meantime keeps it from staying subscribed with nobody to close it
        Subscription replaced = subscription.getAndSet(activated);
        if (replaced != null) {
            replaced.close();
        }
        try {
            CompletableFuture<@Nullable Void> subscribed = watcher.subscribe(activated);
            watcher.start();
            // wait until watching covers this cache instance, or fail after the timeout - but fail fast if watching
            // is not possible at all
            subscribed.get(activationTimeout.toMillis(), TimeUnit.MILLISECONDS);
            activated.confirm();
        } catch (Exception e) {
            activated.close();
            // An interruption is addressed to the thread rather than to this call, and waiting for the watcher to
            // come up clears the flag on its way out. Set again before the failure is reported, so that whoever
            // asked this thread to stop is still heard by whatever it does next
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new MongoClientException(format("Watching change streams failed for cache at '%s'", identifier),
                    causeOf(e));
        }
    }

    // what activating failed for: the failure of watching itself, or the activation timeout running out first
    private @Nullable Throwable causeOf(Exception e) {
        if (e instanceof ExecutionException) {
            return e.getCause();
        }
        if (e instanceof TimeoutException) {
            return new MongoTimeoutException(format("Timeout after %s seconds", activationTimeout.toSeconds()));
        }
        return e;
    }

    @Override
    public void deactivate() {
        Subscription deactivated = subscription.get();
        if (deactivated != null) {
            deactivated.close();
        }
    }

    @Override
    public boolean isActivated() {
        Subscription activated = subscription.get();
        return activated != null && activated.isActive();
    }

    /**
     * What one activation of this cache instance receives through, and the worker that reads the cache entries out
     * of what it receives and applies them.
     * <p>
     * What waits to be applied is kept in the order the store produced it, because a change stream carries each
     * cache entry as it stood: unlike a notification naming a record to read back, applying an older one after a
     * newer one would apply it as the newer state. A failure to apply concerns this cache instance alone: what was
     * taken for the failed attempt goes back to the front, and is applied again after a growing delay - after
     * reconciling, because the attempt may have applied anything up to all of it. That is what a cursor resuming
     * where it failed would do, without the cursor having to fail for everybody else on it.
     */
    private final class Subscription implements MongoWatcher.Subscriber {

        private final MongoWatcherRegistry.Key key;
        private final MongoWatcher watcher;
        private final ExecutorService executorService = Executors.newSingleThreadExecutor();
        private final CountDownLatch closed = new CountDownLatch(1);
        // guarded by this
        private List<Document> pending = new ArrayList<>();
        private boolean restartRequested;
        private boolean scheduled;
        private volatile boolean confirmed;

        private Subscription(MongoWatcherRegistry.Key key, MongoWatcher watcher) {
            this.key = key;
            this.watcher = watcher;
        }

        @Override
        public String getCollectionName() {
            return collectionName;
        }

        @Override
        public String getDiscriminator() {
            return discriminator;
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public synchronized void receiveDocuments(List<Document> documents) {
            // handed over in the moment between closing and the watcher taking the leaving into account
            if (isClosed()) {
                return;
            }
            pending.addAll(documents);
            if (pending.size() > pendingLimit) {
                pending = new ArrayList<>();
                restartRequested = true;
            }
            schedule();
        }

        // What waits to be applied stays: it was delivered before the cursor failed, and is applied after the
        // reconcile, as the replay of a resumed cursor would be
        @Override
        public synchronized void receiveRestart() {
            restartRequested = true;
            schedule();
        }

        private void confirm() {
            confirmed = true;
        }

        private boolean isActive() {
            return confirmed && !isClosed();
        }

        private boolean isClosed() {
            return closed.getCount() == 0;
        }

        // Never waits for the worker: closing may happen under the lock of the cache instance, which the worker
        // takes to apply what it read. The worker notices on its own and ends
        private void close() {
            synchronized (this) {
                if (isClosed()) {
                    return;
                }
                closed.countDown();
            }
            watcher.unsubscribe(this);
            MongoWatcherRegistry.release(key, watcher);
            executorService.shutdown();
        }

        // guarded by this
        private void schedule() {
            if (scheduled || isClosed()) {
                return;
            }
            scheduled = true;
            try {
                executorService.execute(this::work);
            } catch (RejectedExecutionException e) {
                // closed in the meantime, which leaves nothing to do
                scheduled = false;
            }
        }

        private void work() {
            int failures = 0;
            while (true) {
                boolean restart;
                List<Document> documents;
                synchronized (this) {
                    if (isClosed() || (!restartRequested && pending.isEmpty())) {
                        scheduled = false;
                        return;
                    }
                    restart = restartRequested;
                    restartRequested = false;
                    documents = pending;
                    pending = new ArrayList<>();
                }
                try {
                    if (restart) {
                        receiver.receiveSynchronizationRestart();
                    }
                    receive(documents);
                    failures = 0;
                } catch (RuntimeException e) {
                    failures++;
                    LOGGER.log(Level.WARNING, format("Receiving change stream events failed for cache at '%s'. "
                            + "Retrying...", identifier), e);
                    synchronized (this) {
                        documents.addAll(pending);
                        pending = documents;
                        // beyond the limit, falling behind costs the reconcile alone, as when receiving
                        if (pending.size() > pendingLimit) {
                            pending = new ArrayList<>();
                        }
                        restartRequested = true;
                    }
                    if (awaitClosed(RETRY_INTERVAL.multipliedBy(min(failures, 10)))) {
                        synchronized (this) {
                            scheduled = false;
                        }
                        return;
                    }
                }
            }
        }

        // waits out the delay before the next attempt, and reports whether closing cut it short
        private boolean awaitClosed(Duration delay) {
            try {
                return closed.await(delay.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return true;
            }
        }

        // Handed over in batches rather than one by one, because the receiving side takes a lock per handover and
        // one acquisition per cache entry would leave it moving at the rate of whoever holds it
        private void receive(List<Document> documents) {
            List<CacheEntry<K, V>> cacheEntries = new ArrayList<>(min(documents.size(), MAXIMUM_BATCH_SIZE));
            for (Document document : documents) {
                if (isClosed()) {
                    return;
                }
                // skipped (logged and left out) rather than thrown on, as the contract of a synchronizer asks for:
                // a document that cannot be read now cannot be read on a retry either
                CacheEntry<K, V> cacheEntry = toCacheEntryOrNull(keySerializer, valueSerializer, document, LOGGER,
                        identifier);
                if (nonNull(cacheEntry)) {
                    cacheEntries.add(cacheEntry);
                }
                if (cacheEntries.size() == MAXIMUM_BATCH_SIZE) {
                    receiver.receiveCacheEntries(cacheEntries);
                    cacheEntries = new ArrayList<>(MAXIMUM_BATCH_SIZE);
                }
            }
            if (!cacheEntries.isEmpty() && !isClosed()) {
                receiver.receiveCacheEntries(cacheEntries);
            }
        }
    }
}
