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

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractSynchronizer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import javax.sql.DataSource;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.github.oberhoff.distributedcaffeine.adapter.Repository.Order.UNORDERED;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.nonNull;

// The statements here name a channel, and a channel cannot be a parameter of one. What is concatenated into
// them is derived from the identifier rather than taken from anywhere a caller reaches - a fixed prefix and a
// digest - so there is nothing in it for a caller to have put there
@SuppressWarnings({"java:S2077", "SqlNoDataSourceInspection", "SqlSourceToSinkFlow"})
final class PostgresSynchronizer<K, V> extends AbstractSynchronizer<K, V> {

    private static final Logger LOGGER = System.getLogger(PostgresSynchronizer.class.getName());

    private static final Duration WATCHER_INTERVAL = Duration.ofSeconds(1);
    private static final Duration ACTIVATION_TIMEOUT = Duration.ofSeconds(30);
    // how long a poll waits for something to arrive before looking at whether it is still supposed to be listening
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    // hashes read back in one statement, so that a burst does not turn into an arbitrarily long IN-list
    private static final int MAXIMUM_BATCH_SIZE = 500;

    private final DataSource dataSource;
    private final PostgresRepository<K, V> repository;
    private final AtomicReference<WatchState> watchState;

    private @Nullable CompletableFuture<Void> watcherCompletableFuture;
    private @Nullable CompletableFuture<@Nullable Void> listening;

    PostgresSynchronizer(DataSource dataSource, PostgresRepository<K, V> repository) {
        this.dataSource = dataSource;
        this.repository = repository;
        this.watchState = new AtomicReference<>(WatchState.STOPPED);
        // TODO connection sharing
    }

    @Override
    public void activate() {
        // wait for completion if required
        Optional.ofNullable(watcherCompletableFuture)
                .filter(future -> !future.isDone())
                .ifPresent(CompletableFuture::join);

        // Completed once listening has begun, and completed exceptionally when starting it turns out to be
        // impossible - which is what activating waits on, rather than polling its own state until it changes
        CompletableFuture<@Nullable Void> started = new CompletableFuture<>();
        listening = started;

        watchState.set(WatchState.STARTING);

        scheduleNotificationWatcher(started);

        try {
            started.get(ACTIVATION_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            watchState.set(WatchState.STOPPED);
            // An interruption is addressed to the thread rather than to this call, and waiting for the listener to
            // come up clears the flag on its way out. Set again before the failure is reported, so that whoever
            // asked this thread to stop is still heard by whatever it does next
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            Throwable cause = e instanceof java.util.concurrent.ExecutionException ? e.getCause() : e;
            throw new IllegalStateException(
                    format("Listening for notifications failed for cache at '%s'", identifier), cause);
        }
    }

    @Override
    public void deactivate() {
        // an attempt that failed earlier may already be scheduled and is not aborted by this, so it still runs
        // afterwards - it observes STOPPED and returns without listening
        watchState.set(WatchState.STOPPED);
    }

    @Override
    public boolean isActivated() {
        return watchState.get() == WatchState.STARTED;
    }

    private boolean isStopped() {
        return watchState.get() == WatchState.STOPPED;
    }

    private void scheduleNotificationWatcher(CompletableFuture<@Nullable Void> started) {
        RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                // abort unless listening had already begun: a failure while starting up is final and must fail
                // fast, whereas a failure after that is treated as transient and retried
                .abortOn(throwable -> watchState.get() != WatchState.STARTED)
                .withMaxAttempts(-1)
                .withDelay(WATCHER_INTERVAL)
                .withDelayFnOn(context -> WATCHER_INTERVAL.multipliedBy(min(context.getAttemptCount(), 10)),
                        Throwable.class)
                .onRetryScheduled(event -> Optional.ofNullable(event.getLastException())
                        .ifPresent(throwable -> LOGGER.log(Level.WARNING,
                                format("Listening for notifications failed for cache at '%s'. Retrying...",
                                        identifier), throwable)))
                .build();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        watcherCompletableFuture = Failsafe.with(retryPolicy)
                .with(executorService)
                .runAsync(this::processNotifications)
                .whenComplete((result, throwable) -> {
                    if (nonNull(throwable)) {
                        started.completeExceptionally(throwable);
                    }
                    executorService.shutdown();
                });
    }

    private void processNotifications() throws SQLException {
        // this attempt may have been scheduled before deactivation, in which case listening must not be (re)started
        if (isStopped()) {
            return;
        }
        String channel = PostgresChannel.channelOf(identifier);
        // a connection of its own, held for as long as it listens: a notification reaches the sessions that are
        // listening when it is issued and nobody else, so this one cannot be borrowed and returned between polls
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                // Dropping whatever this session was subscribed to before taking it over: a connection comes from
                // a pool, and a listener that was not given the chance to unsubscribe - one whose thread was
                // interrupted, or whose connection broke - hands its subscriptions on with it. Inherited, they
                // deliver notifications of another scope, whose hashes name records this one does not hold
                statement.execute("UNLISTEN *");
                statement.execute("LISTEN " + channel);
            }
            // and dropping what it had already been handed: unsubscribing stops what comes next, while whatever
            // reached this session before it is queued and would be delivered on the first poll regardless
            drainNotifications(connection);
            try {
                if (isStopped()) {
                    return;
                }
                // Listening having begun once before means this connection replaces one that failed, and whatever
                // was published while nothing was listening is gone: a notification is delivered to the sessions
                // listening at the time and is not kept for anyone else, so there is nothing to catch up on from
                // here. Reported once listening is live again, so that what arrives while the cache instance
                // recovers is delivered rather than missed in turn
                boolean relistened = watchState.getAndSet(WatchState.STARTED) == WatchState.STARTED;
                Optional.ofNullable(listening).ifPresent(future -> future.complete(null));
                if (relistened) {
                    receiver.receiveSynchronizationRestart();
                }
                PGConnection pgConnection = connection.unwrap(PGConnection.class);
                while (!isStopped()) {
                    // blocks until something arrives or the timeout is over, without a query of its own, which is
                    // what makes a held connection enough to be woken by
                    PGNotification[] notifications = pgConnection.getNotifications((int) POLL_TIMEOUT.toMillis());
                    if (nonNull(notifications) && notifications.length > 0 && !isStopped()) {
                        receiveCacheEntriesOf(notifications);
                    }
                }
            } finally {
                // Closing a pooled connection hands it back rather than closing it, and LISTEN is session state
                // that outlives the hand-back: left subscribed, the connection delivers notifications to whoever
                // borrows it next, and the server keeps queueing for a session nobody reads. Best effort, because
                // a watcher that failed may hold a connection that can no longer carry a statement at all
                try {
                    // A connection that is already gone took its session with it, and the subscription with it -
                    // and the pool discards it rather than handing it on, so there is nothing left to unsubscribe
                    // from. Asked rather than assumed, because a broken one does not report itself as closed
                    if (connection.isValid(1)) {
                        try (Statement statement = connection.createStatement()) {
                            statement.execute("UNLISTEN " + channel);
                        }
                    }
                } catch (SQLException e) {
                    LOGGER.log(Level.DEBUG, format("Unsubscribing from notifications failed for cache at '%s'",
                            identifier), e);
                }
            }
        }
    }

    private static void drainNotifications(Connection connection) throws SQLException {
        PGConnection pgConnection = connection.unwrap(PGConnection.class);
        PGNotification[] stale;
        do {
            stale = pgConnection.getNotifications(1);
        } while (nonNull(stale) && stale.length > 0);
    }

    // What arrives names records rather than carrying them, so the payload is where a read starts and not what is
    // applied. Everything polled together is read and handed over together, because the receiving side takes a
    // lock per handover and one acquisition per record would leave it moving at the rate of whoever holds it
    private void receiveCacheEntriesOf(PGNotification[] notifications) throws SQLException {
        Set<String> hashes = new LinkedHashSet<>();
        for (PGNotification notification : notifications) {
            String payload = notification.getParameter();
            if (nonNull(payload) && !payload.isEmpty()) {
                hashes.addAll(PostgresChannel.hashesOf(payload));
            }
        }
        List<String> pending = new ArrayList<>(hashes);
        for (int from = 0; from < pending.size(); from += MAXIMUM_BATCH_SIZE) {
            Set<String> batch = new LinkedHashSet<>(
                    pending.subList(from, min(from + MAXIMUM_BATCH_SIZE, pending.size())));
            List<CacheEntry<K, V>> cacheEntries;
            // a record swept before it could be read comes back as nothing rather than as an event that vanished,
            // so what is delivered is what the store still holds
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
            throw new IllegalStateException(format("Reading cache entries failed for cache at '%s'", identifier), e);
        }
    }

    private enum WatchState {

        /**
         * Not listening and not supposed to: either never activated or deactivated since. A scheduled retry
         * attempt observing this state returns without listening.
         */
        STOPPED,

        /**
         * Activation is under way, but listening has not begun yet. A failure in this state is final (fail fast).
         */
        STARTING,

        /**
         * Listening has begun. This state is kept while a transient failure is being retried, so that activation
         * is not reported as lost during a short interruption, and so that such a failure is retried instead of
         * aborted.
         */
        STARTED
    }
}
