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
package io.github.oberhoff.distributedcaffeine.adapter.postgresql;

import io.github.oberhoff.distributedcaffeine.adapter.AbstractSynchronizer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresAdapter.ListenerSharingMode;
import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.adapter.Repository.Order.UNORDERED;
import static java.lang.Math.min;
import static java.lang.String.format;

/**
 * Receives for one cache instance over a listening session that it may share with others, as the sharing level
 * decides.
 * <p>
 * Every activation subscribes anew, with a subscription of its own that does the work for this cache instance:
 * reading back what the session hands over and applying it. Activating and deactivating run under the lock of the
 * cache instance, and so does applying - so neither of them ever waits for the work of a subscription, and a
 * subscription that outlives its deactivation simply finds itself closed.
 */
final class PostgresSynchronizer<K, V> extends AbstractSynchronizer<K, V> {

    private static final Logger LOGGER = System.getLogger(PostgresSynchronizer.class.getName());

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);
    // hashes read back in one statement, so that a burst does not turn into an arbitrarily long IN-list
    private static final int MAXIMUM_BATCH_SIZE = 500;

    // what writes go through, and so what the probe is sent through - the path whose notifications have to arrive
    private final DataSource dataSource;
    // what the listening connection is taken from, which may be a different endpoint than the one writes use
    private final DataSource listenerDataSource;
    private final PostgresRepository<K, V> repository;
    private final Object scope;
    // How often the listening connection is asked whether it still reaches the server, and how long it gets to
    // answer. Polling only waits for something to arrive and never sends anything, so a connection that died
    // without being reset - a failover moving the server's address, a NAT entry expiring on an idle path - looks
    // exactly like one on which nothing is published, and would keep being polled forever. Fields rather than
    // constants only so that a test can shorten them before activation instead of waiting them out - and taken
    // over by the session only from the cache instance that opens it
    @SuppressWarnings({"FieldMayBeFinal", "CanBeFinal"})
    private Duration heartbeatInterval = Duration.ofSeconds(10);
    @SuppressWarnings({"FieldMayBeFinal", "CanBeFinal"})
    private Duration heartbeatTimeout = Duration.ofSeconds(5);
    // How long a probe sent through the data source writes use gets to arrive at the listening connection before
    // listening is considered not to work. A field for the same reason as the two above
    @SuppressWarnings({"FieldMayBeFinal", "CanBeFinal"})
    private Duration probeTimeout = Duration.ofSeconds(5);
    // How long a single poll may stay inside the driver before the connection is aborted from outside - see the
    // watchdog of the listener. Far above what a poll takes, so that it only ever fires on a thread that is stuck.
    // A field for the same reason as the ones above
    @SuppressWarnings({"FieldMayBeFinal", "CanBeFinal"})
    private Duration watchdogTimeout = Duration.ofSeconds(30);
    // How many hashes may wait to be read back before this cache instance is considered to have fallen behind -
    // at which point they are dropped and the cache instance is reconciled instead, so that falling behind costs
    // a reconcile rather than memory without bound. A field for the same reason as the ones above
    @SuppressWarnings({"FieldMayBeFinal", "CanBeFinal", "FieldCanBeLocal"})
    private int pendingLimit = 10_000;
    // How long activating waits for the listening session to take this cache instance in - which, while a shared
    // session is being replaced, includes waiting for it to come back. A field for the same reason as the ones above
    @SuppressWarnings({"FieldMayBeFinal", "CanBeFinal", "FieldCanBeLocal"})
    private Duration activationTimeout = Duration.ofSeconds(30);

    private final AtomicReference<@Nullable Subscription> subscription = new AtomicReference<>();

    PostgresSynchronizer(DataSource dataSource, DataSource listenerDataSource, ListenerSharingMode sharing,
                         String schemaName, String tableName, PostgresRepository<K, V> repository) {
        this.dataSource = dataSource;
        this.listenerDataSource = listenerDataSource;
        this.repository = repository;
        this.scope = switch (sharing) {
            // a scope nobody else has, which shares the session with nobody
            case INSTANCE -> new Object();
            case TABLE -> List.of(schemaName, tableName);
            case DATABASE -> ListenerSharingMode.DATABASE;
        };
    }

    @Override
    public void activate() {
        if (isActivated()) {
            return;
        }
        PostgresListenerRegistry.Key key = new PostgresListenerRegistry.Key(listenerDataSource, dataSource, scope);
        PostgresListener listener = PostgresListenerRegistry.acquire(key,
                () -> new PostgresListener(listenerDataSource, dataSource, new PostgresListener.Settings(
                        heartbeatInterval, heartbeatTimeout, probeTimeout, watchdogTimeout)));
        Subscription activated = new Subscription(key, listener);
        // whatever this replaces is closed, which for the one deactivated before does nothing - and for one that a
        // concurrent activation put there in the meantime keeps it from staying subscribed with nobody to close it
        Subscription replaced = subscription.getAndSet(activated);
        if (replaced != null) {
            replaced.close();
        }
        try {
            listener.subscribe(activated).get(activationTimeout.toMillis(), TimeUnit.MILLISECONDS);
            activated.confirm();
        } catch (Exception e) {
            activated.close();
            // An interruption is addressed to the thread rather than to this call, and waiting for the listener to
            // come up clears the flag on its way out. Set again before the failure is reported, so that whoever
            // asked this thread to stop is still heard by whatever it does next
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw new IllegalStateException(
                    format("Listening for notifications failed for cache at '%s'", identifier), cause);
        }
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
     * What one activation of this cache instance receives through, and the worker that reads back and applies what
     * it receives.
     * <p>
     * What waits to be read back is a set of hashes rather than a list of notifications, because what is read is
     * the record as it is by then: however often a record changes before its turn comes, it is read once. A
     * failure to read back or to apply concerns this cache instance alone - it is retried after a growing delay,
     * by reconciling, because what was taken for the failed attempt may have been anything up to all of it.
     */
    private final class Subscription implements PostgresListener.Subscriber {

        private final PostgresListenerRegistry.Key key;
        private final PostgresListener listener;
        private final String channel = PostgresChannel.channelOf(identifier);
        private final ExecutorService executorService = Executors.newSingleThreadExecutor();
        private final CountDownLatch closed = new CountDownLatch(1);
        // guarded by this
        private Set<String> pending = new LinkedHashSet<>();
        private boolean restartRequested;
        private boolean scheduled;
        private volatile boolean confirmed;

        private Subscription(PostgresListenerRegistry.Key key, PostgresListener listener) {
            this.key = key;
            this.listener = listener;
        }

        @Override
        public String getChannel() {
            return channel;
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public synchronized void receiveHashes(Set<String> hashes) {
            // handed over in the moment between closing and the listener taking the leaving into account
            if (isClosed()) {
                return;
            }
            pending.addAll(hashes);
            if (pending.size() > pendingLimit) {
                pending = new LinkedHashSet<>();
                restartRequested = true;
            }
            schedule();
        }

        @Override
        public synchronized void receiveRestart() {
            // a restart reads everything back, so whatever waits to be read back already is part of it
            pending = new LinkedHashSet<>();
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
            listener.unsubscribe(this);
            PostgresListenerRegistry.release(key, listener);
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
                Set<String> hashes;
                synchronized (this) {
                    if (isClosed() || (!restartRequested && pending.isEmpty())) {
                        scheduled = false;
                        return;
                    }
                    restart = restartRequested;
                    restartRequested = false;
                    hashes = pending;
                    pending = new LinkedHashSet<>();
                }
                try {
                    if (restart) {
                        receiver.receiveSynchronizationRestart();
                    }
                    receive(hashes);
                    failures = 0;
                } catch (Exception e) {
                    failures++;
                    LOGGER.log(Level.WARNING, format("Receiving notifications failed for cache at '%s'. Retrying...",
                            identifier), e);
                    synchronized (this) {
                        pending = new LinkedHashSet<>();
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

        // Everything taken together is read and handed over together, because the receiving side takes a lock per
        // handover and one acquisition per record would leave it moving at the rate of whoever holds it
        private void receive(Set<String> hashes) throws SQLException {
            List<String> taken = new ArrayList<>(hashes);
            for (int from = 0; from < taken.size() && !isClosed(); from += MAXIMUM_BATCH_SIZE) {
                Set<String> batch = new LinkedHashSet<>(
                        taken.subList(from, min(from + MAXIMUM_BATCH_SIZE, taken.size())));
                List<CacheEntry<K, V>> cacheEntries;
                // a record swept before it could be read comes back as nothing rather than as an event that
                // vanished, so what is delivered is what the store still holds
                try (Stream<CacheEntry<K, V>> stream = streamOf(batch)) {
                    cacheEntries = stream.toList();
                }
                if (!cacheEntries.isEmpty()) {
                    receiver.receiveCacheEntries(cacheEntries);
                }
            }
        }

        private Stream<CacheEntry<K, V>> streamOf(Set<String> hashes) throws SQLException {
            try {
                return repository.streamCacheEntries(hashes, null, UNORDERED);
            } catch (SQLException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(
                        format("Reading cache entries failed for cache at '%s'", identifier), e);
            }
        }
    }
}
