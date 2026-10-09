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
import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import javax.sql.DataSource;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.nonNull;

/**
 * One listening session, shared by the cache instances that subscribe to it.
 * <p>
 * The session is held by a thread of its own for as long as anybody subscribes, and that thread does nothing but
 * keep the session alive and hand what arrives to the subscribers it is meant for. What a subscriber does with it -
 * reading the records back, applying them to its cache instance - happens on the subscriber's side, so that one
 * cache instance being slow or failing never holds up the others or the session itself.
 * <p>
 * Subscribing and unsubscribing are requests that the listening thread carries out between polls: the driver holds
 * the connection for the whole of a poll, so a statement issued from anywhere else would wait for it anyway.
 */
// The statements here name a channel, and a channel cannot be a parameter of one. What is concatenated into
// them is derived from the identifier rather than taken from anywhere a caller reaches - a fixed prefix and a
// digest - so there is nothing in it for a caller to have put there
@SuppressWarnings({"java:S2077", "SqlNoDataSourceInspection"})
final class PostgresListener {

    private static final Logger LOGGER = System.getLogger(PostgresListener.class.getName());

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);
    // How long a poll waits for something to arrive before looking at the requests and at whether it is still
    // supposed to be listening. Short, because a cache instance subscribing waits for the poll under way to be
    // over before its subscription can be carried out - and waiting costs nothing but a timer on a local socket
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    // the statements a session subscribes and unsubscribes with, completed by the channel they concern
    private static final String LISTEN = "LISTEN ";
    private static final String UNLISTEN = "UNLISTEN ";
    private static final String UNLISTEN_ALL = "UNLISTEN *";

    /**
     * Who a session hands what arrives to.
     */
    interface Subscriber {

        String getChannel();

        String getIdentifier();

        // called on the listening thread, so it hands the hashes over rather than acting on them
        void receiveHashes(Set<String> hashes);

        // called on the listening thread as well: the session was lost and is back, and whatever was announced in
        // the meantime is gone
        void receiveRestart();
    }

    /**
     * How the session watches over itself - taken from the cache instance that opens it.
     */
    record Settings(Duration heartbeatInterval, Duration heartbeatTimeout, Duration probeTimeout,
                    Duration watchdogTimeout) {
    }

    private enum State {

        /**
         * Listening has not begun yet. A failure in this state is final, and fails everybody waiting to subscribe.
         */
        STARTING,

        /**
         * Listening has begun. Kept while a lost session is being replaced, so that a failure is retried.
         */
        STARTED,

        /**
         * Starting failed. Whoever subscribes is refused, and the registry replaces the listener.
         */
        FAILED,

        /**
         * Nobody subscribes any more, and the session is given up.
         */
        CLOSED
    }

    private record Request(Subscriber subscriber, @Nullable CompletableFuture<@Nullable Void> subscribed) {
    }

    // the channels that the requests taken into account have left to be listened for, and to be listened for no more
    private record Changes(List<String> listen, List<String> unlisten) {
    }

    private final DataSource listenerDataSource;
    // what writes go through, and so what the probe is sent through - the path whose notifications have to arrive
    private final DataSource dataSource;
    private final Settings settings;
    private final AtomicReference<State> state = new AtomicReference<>(State.STARTING);
    private final Queue<Request> requests = new ConcurrentLinkedQueue<>();
    // named in what is logged, which happens outside the listening thread
    private final Set<String> identifiers = ConcurrentHashMap.newKeySet();
    // the channel of the session currently listening, through which a subscriber cuts the poll under way short -
    // only while a session is listening, since otherwise there is no poll to cut short
    private volatile @Nullable String wakeChannel;
    // The rest is touched by the listening thread alone - and by whatever completes it once that thread is done
    private final Map<String, Set<Subscriber>> subscribersByChannel = new HashMap<>();
    // who has been told that it is subscribed, and so has to be told when the session it relies on was lost
    private final Set<Subscriber> confirmed = new HashSet<>();
    private final Map<Subscriber, CompletableFuture<@Nullable Void>> unconfirmed = new ConcurrentHashMap<>();
    // counted by the registry, under its lock
    int references;

    PostgresListener(DataSource listenerDataSource, DataSource dataSource, Settings settings) {
        this.listenerDataSource = listenerDataSource;
        this.dataSource = dataSource;
        this.settings = settings;
    }

    // nothing waits for the session to end, and how it ended is dealt with where it ends rather than by whoever
    // would otherwise hold on to its future
    @SuppressWarnings("FutureReturnValueIgnored")
    void start() {
        RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                // abort unless listening had already begun: a failure while starting up is final and must fail
                // fast, whereas a failure after that is treated as transient and retried
                .abortOn(throwable -> state.get() != State.STARTED)
                .withMaxAttempts(-1)
                .withDelay(RETRY_INTERVAL)
                .withDelayFnOn(context -> RETRY_INTERVAL.multipliedBy(min(context.getAttemptCount(), 10)),
                        Throwable.class)
                .onRetryScheduled(event -> Optional.ofNullable(event.getLastException())
                        .ifPresent(throwable -> LOGGER.log(Level.WARNING,
                                format("Listening for notifications failed for %s. Retrying...", describe()),
                                throwable)))
                .build();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Failsafe.with(retryPolicy)
                .with(executorService)
                .runAsync(this::listen)
                .whenComplete((result, throwable) -> {
                    // completed here rather than where the failure happens, so that whoever is told of it finds
                    // the connection it failed on already returned
                    if (nonNull(throwable)) {
                        fail(throwable);
                    }
                    executorService.shutdown();
                });
    }

    /**
     * Subscribes, and returns what is completed once the subscriber is listened for on a session that is known to
     * receive - or completed exceptionally if listening cannot be begun at all.
     */
    CompletableFuture<@Nullable Void> subscribe(Subscriber subscriber) {
        CompletableFuture<@Nullable Void> subscribed = new CompletableFuture<>();
        synchronized (this) {
            // checked under the same lock that failing takes, so that a request cannot slip in after failing has
            // refused everybody waiting and then wait forever itself
            if (state.get() == State.FAILED || state.get() == State.CLOSED) {
                subscribed.completeExceptionally(new IllegalStateException("Listening has been given up"));
                return subscribed;
            }
            identifiers.add(subscriber.getIdentifier());
            requests.add(new Request(subscriber, subscribed));
        }
        wake();
        return subscribed;
    }

    // without waking, because nothing waits for an unsubscription to be carried out
    void unsubscribe(Subscriber subscriber) {
        requests.add(new Request(subscriber, null));
    }

    // Cuts the poll under way short, so that a request is carried out right away rather than once the poll is over:
    // a cache instance subscribing waits for that, and a poll the driver holds the connection for cannot be
    // interrupted from here. Announced through the data source writes go through, which is the way the probe has
    // proven to arrive. A wake that fails costs no more than the rest of the poll, so it is not retried
    private void wake() {
        String channel = wakeChannel;
        if (channel == null) {
            return;
        }
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT pg_notify(?, '')")) {
            statement.setString(1, channel);
            statement.execute();
        } catch (SQLException e) {
            LOGGER.log(Level.DEBUG, format("Waking the listening session failed for %s", describe()), e);
        }
    }

    // by the registry, once nobody subscribes any more
    void close() {
        state.compareAndSet(State.STARTING, State.CLOSED);
        state.compareAndSet(State.STARTED, State.CLOSED);
    }

    boolean isFailed() {
        return state.get() == State.FAILED;
    }

    private boolean isClosed() {
        return state.get() == State.CLOSED;
    }

    private synchronized void fail(Throwable throwable) {
        if (!state.compareAndSet(State.STARTING, State.FAILED)) {
            return;
        }
        Request request;
        while ((request = requests.poll()) != null) {
            Optional.ofNullable(request.subscribed()).ifPresent(future -> future.completeExceptionally(throwable));
        }
        unconfirmed.values().forEach(future -> future.completeExceptionally(throwable));
        unconfirmed.clear();
    }

    private String describe() {
        return identifiers.stream()
                .map(identifier -> format("'%s'", identifier))
                .collect(Collectors.joining(", ", "cache instances at ", ""));
    }

    private void listen() throws SQLException {
        // this attempt may have been scheduled before closing, in which case listening must not be (re)started
        if (isClosed()) {
            return;
        }
        // a connection of its own, held for as long as it listens: a notification reaches the sessions that are
        // listening when it is issued and nobody else, so this one cannot be borrowed and returned between polls
        try (Connection connection = listenerDataSource.getConnection()) {
            Watchdog watchdog = new Watchdog(connection);
            try {
                listen(connection, watchdog);
            } catch (SQLException e) {
                throw watchdog.explain(e);
            } finally {
                watchdog.stop();
            }
        }
    }

    private void listen(Connection connection, Watchdog watchdog) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            // Dropping whatever this session was subscribed to before taking it over: a connection comes from a
            // pool, and a listener that was not given the chance to unsubscribe - one whose thread was interrupted,
            // or whose connection broke - hands its subscriptions on with it. Inherited, they deliver
            // notifications of channels nobody here subscribes to
            statement.execute(UNLISTEN_ALL);
        }
        // the requests made while there was no session are taken into account before listening, so that they are
        // listened for along with everybody else rather than one by one afterwards
        applyRequests();
        execute(connection, LISTEN, subscribersByChannel.keySet());
        // and dropping what it had already been handed: unsubscribing stops what comes next, while whatever reached
        // this session before it is queued and would be delivered on the first poll regardless
        drainNotifications(watchdog);
        try {
            // what arrives while the probe is under way is kept, and handed over once listening has begun
            String sessionChannel = PostgresChannel.sessionChannel();
            List<PGNotification> arrivedWhileProbing = probe(connection, sessionChannel, watchdog);
            if (isClosed()) {
                return;
            }
            // Listening having begun once before means this session replaces one that failed, and whatever was
            // published while nothing was listening is gone: a notification is delivered to the sessions listening
            // at the time and is not kept for anyone else, so there is nothing to catch up on from here. Reported
            // to whoever relied on the lost session once listening is live again, so that what arrives while they
            // recover is delivered rather than missed in turn
            boolean relistened = state.getAndSet(State.STARTED) == State.STARTED;
            // from here on, whoever subscribes can wake this session - and only after this point, so that a wake
            // can never be taken for the probe
            wakeChannel = sessionChannel;
            if (relistened) {
                confirmed.forEach(Subscriber::receiveRestart);
            }
            confirmSubscriptions();
            dispatch(arrivedWhileProbing);
            long lastHeartbeat = System.nanoTime();
            while (!isClosed()) {
                Changes changes = applyRequests();
                execute(connection, LISTEN, changes.listen());
                execute(connection, UNLISTEN, changes.unlisten());
                confirmSubscriptions();
                // blocks until something arrives or the timeout is over, without a query of its own, which is what
                // makes a held connection enough to be woken by
                PGNotification[] notifications = watchdog.poll((int) POLL_TIMEOUT.toMillis());
                if (nonNull(notifications) && notifications.length > 0) {
                    dispatch(List.of(notifications));
                }
                // Failing here is what turns a dead connection into the failure it is, so that it is replaced and
                // what was missed meanwhile is reconciled. A notification arriving while the heartbeat is under
                // way is kept by the driver and handed over on the next poll, so nothing is lost to it
                if (System.nanoTime() - lastHeartbeat >= settings.heartbeatInterval().toNanos() && !isClosed()) {
                    if (!connection.isValid((int) settings.heartbeatTimeout().toSeconds())) {
                        throw new SQLException(format("Listening connection did not respond within %d seconds",
                                settings.heartbeatTimeout().toSeconds()), "08006");
                    }
                    lastHeartbeat = System.nanoTime();
                }
            }
        } finally {
            // a session that is over cannot be woken, and whoever subscribes now is taken into account when the
            // next one begins
            wakeChannel = null;
            // Closing a pooled connection hands it back rather than closing it, and LISTEN is session state that
            // outlives the hand-back: left subscribed, the connection delivers notifications to whoever borrows it
            // next, and the server keeps queueing for a session nobody reads. Best effort, because a session that
            // failed may hold a connection that can no longer carry a statement at all
            try {
                // A connection that is already gone took its session with it, and the subscriptions with it - and
                // the pool discards it rather than handing it on, so there is nothing left to unsubscribe from.
                // Asked rather than assumed, because a broken one does not report itself as closed
                if (connection.isValid(1)) {
                    try (Statement statement = connection.createStatement()) {
                        statement.execute(UNLISTEN_ALL);
                    }
                }
            } catch (SQLException e) {
                LOGGER.log(Level.DEBUG, format("Unsubscribing from notifications failed for %s", describe()), e);
            }
        }
    }

    // Takes the requests made since the last time into account, and returns which channels are new and which are
    // left without anybody subscribing. Whatever still arrives on a channel before it is unlistened finds nobody
    // to be handed to, and is dropped
    @SuppressWarnings("java:S3776")
    private Changes applyRequests() {
        List<String> listen = new ArrayList<>();
        List<String> unlisten = new ArrayList<>();
        Request request;
        while ((request = requests.poll()) != null) {
            Subscriber subscriber = request.subscriber();
            String channel = subscriber.getChannel();
            CompletableFuture<@Nullable Void> subscribed = request.subscribed();
            if (nonNull(subscribed)) {
                if (subscribersByChannel.computeIfAbsent(channel, ignored -> new LinkedHashSet<>()).isEmpty()
                        && !unlisten.remove(channel)) {
                    listen.add(channel);
                }
                subscribersByChannel.get(channel).add(subscriber);
                unconfirmed.put(subscriber, subscribed);
            } else {
                Set<Subscriber> subscribers = subscribersByChannel.get(channel);
                if (nonNull(subscribers) && subscribers.remove(subscriber) && subscribers.isEmpty()) {
                    subscribersByChannel.remove(channel);
                    // a channel subscribed to and left again within the same requests was never listened for
                    if (!listen.remove(channel)) {
                        unlisten.add(channel);
                    }
                }
                confirmed.remove(subscriber);
                Optional.ofNullable(unconfirmed.remove(subscriber)).ifPresent(future -> future.cancel(false));
                identifiers.remove(subscriber.getIdentifier());
            }
        }
        return new Changes(listen, unlisten);
    }

    // in one round trip however many channels there are, which is what makes several cache instances starting
    // together cost the session one statement rather than one each
    private static void execute(Connection connection, String command, Collection<String> channels)
            throws SQLException {
        if (channels.isEmpty()) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            for (String channel : channels) {
                statement.addBatch(command + channel);
            }
            statement.executeBatch();
        }
    }

    // only on a session that is known to receive, which is what a subscriber waits to be told
    private void confirmSubscriptions() {
        if (state.get() != State.STARTED || unconfirmed.isEmpty()) {
            return;
        }
        unconfirmed.forEach((subscriber, subscribed) -> {
            confirmed.add(subscriber);
            subscribed.complete(null);
        });
        unconfirmed.clear();
    }

    // What arrives names records rather than carrying them, so it is handed over as hashes, grouped per channel so
    // that a subscriber gets everything one poll brought for it in one go
    private void dispatch(List<PGNotification> notifications) {
        Map<String, Set<String>> hashesByChannel = new LinkedHashMap<>();
        for (PGNotification notification : notifications) {
            String payload = notification.getParameter();
            if (subscribersByChannel.containsKey(notification.getName()) && nonNull(payload) && !payload.isEmpty()) {
                hashesByChannel.computeIfAbsent(notification.getName(), ignored -> new LinkedHashSet<>())
                        .addAll(PostgresChannel.hashesOf(payload));
            }
        }
        hashesByChannel.forEach((channel, hashes) -> subscribersByChannel.getOrDefault(channel, Set.of())
                .forEach(subscriber -> subscriber.receiveHashes(hashes)));
    }

    // Proves that what writes announce reaches this connection, by announcing something on a channel of its own
    // through the data source writes go through. A subscription that is accepted is not one that receives: behind a
    // pooler in transaction mode, LISTEN succeeds on a server session that is handed to somebody else as soon as
    // the statement is over, and a connection to another server or database listens to a channel nobody writes
    // to - both without an error, and both leaving a cache instance that never hears of a change. Sent from a
    // connection other than the listening one, because a session notifying itself is delivered what it sent even
    // where nothing else would reach it. The channel is the session's own, so no other listener sees it - and it
    // stays listened to afterwards, as the channel the session is woken through. Once per session rather than per
    // subscriber, because it is the session that is in question
    private List<PGNotification> probe(Connection connection, String probeChannel, Watchdog watchdog)
            throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(LISTEN + probeChannel);
        }
        try (Connection probing = dataSource.getConnection();
             PreparedStatement statement = probing.prepareStatement("SELECT pg_notify(?, '')")) {
            statement.setString(1, probeChannel);
            statement.execute();
        }
        List<PGNotification> arrived = new ArrayList<>();
        long deadline = System.nanoTime() + settings.probeTimeout().toNanos();
        boolean probed = false;
        long remaining;
        while (!probed && (remaining = deadline - System.nanoTime()) > 0 && !isClosed()) {
            PGNotification[] notifications = watchdog.poll(
                    (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
            // the whole batch is looked at even once the probe is in it, because what follows the probe in the
            // same batch has been taken from the connection and would otherwise be lost
            for (PGNotification notification : nonNull(notifications) ? notifications : new PGNotification[0]) {
                if (notification.getName().equals(probeChannel)) {
                    probed = true;
                } else {
                    arrived.add(notification);
                }
            }
        }
        // the channel stays listened to once probed, because it is what the session is woken through from now on
        if (!probed && !isClosed()) {
            throw new SQLException(format("A notification sent through the data source did not reach the "
                    + "listening connection within %d seconds. Listening needs a session of its own on the server "
                    + "the data source writes to, which a pooler in transaction mode or a connection to another "
                    + "server or database does not provide - see PostgresAdapter.Builder#withListenerDataSource "
                    + "for listening through a direct or session-mode connection",
                    settings.probeTimeout().toSeconds()), "55000");
        }
        return arrived;
    }

    private static void drainNotifications(Watchdog watchdog) throws SQLException {
        PGNotification[] stale;
        do {
            stale = watchdog.poll(1);
        } while (nonNull(stale) && stale.length > 0);
    }

    // Watches the polls of one listening connection from outside the thread that makes them, and aborts the
    // connection when one of them has been inside the driver for longer than any poll takes. Once the first byte of
    // a message has arrived, the driver reads the rest without any timeout at all, so a connection that dies in the
    // middle of a message leaves the listening thread blocked for good - and the heartbeat, which runs on that very
    // thread, never comes round. Aborting closes the socket without the lock the blocked thread holds, so the
    // blocked read fails, the session fails with it, and it is replaced and what was missed reconciled - as after
    // any other failure. Checked on the shared timer behind CompletableFuture.delayedExecutor rather than on a
    // thread of its own, because a check is one comparison, and so is the abort that a check rarely ends in
    private final class Watchdog {

        private static final long NOT_POLLING = Long.MIN_VALUE;

        private final Connection connection;
        private final PGConnection pgConnection;
        private final AtomicLong pollingSince = new AtomicLong(NOT_POLLING);
        private final AtomicBoolean fired = new AtomicBoolean(false);
        private volatile boolean stopped;

        private Watchdog(Connection connection) throws SQLException {
            this.connection = connection;
            this.pgConnection = connection.unwrap(PGConnection.class);
            schedule();
        }

        private PGNotification @Nullable [] poll(int timeoutMillis) throws SQLException {
            pollingSince.set(System.nanoTime());
            try {
                return pgConnection.getNotifications(timeoutMillis);
            } finally {
                pollingSince.set(NOT_POLLING);
            }
        }

        private void schedule() {
            // a few checks per timeout, so that a stuck poll is aborted not much later than it is due
            long period = Math.max(1, settings.watchdogTimeout().toNanos() / 4);
            CompletableFuture.delayedExecutor(period, TimeUnit.NANOSECONDS).execute(this::check);
        }

        private void check() {
            if (stopped) {
                return;
            }
            long since = pollingSince.get();
            if (since != NOT_POLLING && System.nanoTime() - since > settings.watchdogTimeout().toNanos()) {
                fired.set(true);
                try {
                    connection.abort(Runnable::run);
                } catch (SQLException | RuntimeException e) {
                    LOGGER.log(Level.DEBUG, format("Aborting the listening connection failed for %s", describe()),
                            e);
                }
                return;
            }
            schedule();
        }

        private void stop() {
            stopped = true;
        }

        // what the blocked read fails with once the socket is closed says only that the connection broke, so the
        // reason it was broken is put in front of it
        private SQLException explain(SQLException e) {
            if (!fired.get()) {
                return e;
            }
            return new SQLException(format("Listening connection was stuck inside the driver for more than %d "
                    + "seconds and was aborted", settings.watchdogTimeout().toSeconds()), "08006", e);
        }
    }
}
