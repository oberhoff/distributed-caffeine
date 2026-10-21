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
package io.github.oberhoff.distributedcaffeine.postgresql;

import io.github.oberhoff.distributedcaffeine.spi.Kind;
import io.github.oberhoff.distributedcaffeine.spi.Listener;
import io.github.oberhoff.distributedcaffeine.spi.Message;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.Subscription;
import io.r2dbc.postgresql.api.Notification;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Wrapped;
import org.jspecify.annotations.Nullable;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Listening on PostgreSQL through R2DBC (VISION.md, "PostgreSQL listening: R2DBC" and "Connection sharing"): one
 * session per connection factory (by identity), shared by every cache instance using it, each cache on a channel of
 * its own (table and discriminator), dispatched by channel name. The payload is {@code kind|hash|version}.
 * <p>
 * The session proves it receives notifications on every subscription: a notification sent through the JDBC data
 * source must arrive within 5 seconds (which detects a transaction-mode pooler). A lost session is reported as
 * {@code onLost()} and re-established only when a cache asks ({@code resume()}, VISION.md, "Supervision"): once per
 * loss, whoever asks first; and also when a cache suspects the session went silent (its heartbeat), which then counts
 * as a gap for the other caches on it.
 * <p>
 * The session's threads only enqueue into the caches' own queues (the core's delivery), so they never call into a
 * cache's locks.
 */
@SuppressWarnings("SqlNoDataSourceInspection") // the SQL runs against the user's database, unknown to the IDE
final class PostgresListener {

    private static final Logger LOGGER = System.getLogger("io.github.oberhoff.distributedcaffeine.postgresql");
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    static final long PROBE_TIMEOUT_SECONDS = 5;
    private static final int PROBE_ATTEMPTS = 3;
    private static final IdentityHashMap<ConnectionFactory, Session> SESSIONS = new IdentityHashMap<>();

    private PostgresListener() {
    }

    // jdbc:postgresql://host:port/database?options -> r2dbc:postgresql://host:port/database?options
    static ConnectionFactory connectionFactory(String jdbcUrl, String user, String password) {
        if (!jdbcUrl.startsWith("jdbc:postgresql:")) {
            throw new IllegalArgumentException("not a PostgreSQL JDBC URL: " + jdbcUrl);
        }
        ConnectionFactoryOptions options = ConnectionFactoryOptions.parse("r2dbc:" + jdbcUrl.substring("jdbc:".length()))
                .mutate()
                .option(ConnectionFactoryOptions.USER, user)
                .option(ConnectionFactoryOptions.PASSWORD, password)
                .option(Option.valueOf("applicationName"), "distributed-caffeine-listener")
                .build();
        return ConnectionFactories.get(options);
    }

    /**
     * Subscribes a cache instance to its channel: returns once the session listens on it and has proven that
     * notifications arrive, so every write committed afterwards is delivered (contract point 4).
     */
    @SuppressWarnings("java:S1181") // whatever fails, the registration is released before it is rethrown
    static Subscription subscribe(ConnectionFactory factory, DataSource dataSource, String channel, Listener listener) {
        Session session;
        Registration registration;
        synchronized (SESSIONS) {
            session = SESSIONS.computeIfAbsent(factory, Session::new);
            registration = session.register(channel, listener, dataSource);
        }
        try {
            session.listen(registration);
            session.requireDelivery(registration);
        } catch (RuntimeException | Error e) {
            registration.close();
            throw e;
        }
        return registration;
    }

    /**
     * Whether the session of the factory has dispatched everything notified before this call (tests): a notification
     * sent now must arrive through it; notifications arrive in commit order, so everything before it has arrived too.
     */
    static boolean barrier(ConnectionFactory factory, DataSource dataSource) {
        Session session;
        synchronized (SESSIONS) {
            session = SESSIONS.get(factory);
        }
        if (session == null) {
            return true;
        }
        synchronized (session) {
            if (session.connection == null) {
                return false;
            }
        }
        return session.probe(dataSource);
    }

    // kind|hash|version; INVALIDATED_ALL carries neither
    static @Nullable Message parse(String payload) {
        String[] parts = payload.split("\\|", -1);
        if (parts.length != 3) {
            return null;
        }
        try {
            Kind kind = Kind.valueOf(parts[0]);
            return kind == Kind.INVALIDATED_ALL ? Message.invalidatedAll(Long.parseLong(parts[2]))
                    : new Message(kind, parts[1], Long.parseLong(parts[2]));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static final class Registration implements Subscription {

        private final Session session;
        private final String channel;
        private final Listener listener;
        private final DataSource dataSource;
        // guarded by the session: the session's connection it was last delivered on, and whether it was told that
        // delivery stopped
        private long generation;
        private boolean lost;
        private volatile boolean closed;

        Registration(Session session, String channel, Listener listener, DataSource dataSource) {
            this.session = session;
            this.channel = channel;
            this.listener = listener;
            this.dataSource = dataSource;
        }

        @Override
        public void resume() {
            if (!closed) {
                session.resume(this);
            }
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                session.unregister(this);
            }
        }
    }

    // one listening session; its state is guarded by the session's monitor, dispatch reads concurrent structures
    private static final class Session {

        private final ConnectionFactory factory;
        private final Map<String, List<Registration>> channels = new ConcurrentHashMap<>();
        private final Map<String, CompletableFuture<Void>> probes = new ConcurrentHashMap<>();
        private final String probeChannel = "dc_probe_" + UUID.randomUUID().toString().replace("-", "");
        private @Nullable Connection connection;
        private @Nullable Disposable notifications;
        // counts the connections established, so that a loss is re-established once, whoever asks first
        private long generation;
        private boolean closed;

        Session(ConnectionFactory factory) {
            this.factory = factory;
        }

        Registration register(String channel, Listener listener, DataSource dataSource) {
            Registration registration = new Registration(this, channel, listener, dataSource);
            channels.computeIfAbsent(channel, ignored -> new CopyOnWriteArrayList<>()).add(registration);
            return registration;
        }

        // listens on the registration's channel, connecting if there is no connection (re-established for the caches
        // that lost it, who learn of it when they resume)
        synchronized void listen(Registration registration) {
            if (connection != null) {
                try {
                    listen(connection, registration.channel);
                } catch (StoreException e) {
                    // broken before its loss was noticed: connected anew below
                    discard(connection);
                }
            }
            if (connection == null) {
                connect();
            }
            registration.generation = generation;
        }

        // re-establishes the session for a registration (VISION.md, "Supervision"): if another one did since this one
        // lost it, nothing is left to do; otherwise connected anew, also if the connection seems alive (suspected
        // silent), which the others delivering on it are told as a gap. Only a registration not told of a loss can
        // suspect its connection: for one told, a connection there now is a newer one. PostgreSQL keeps no history:
        // a gap either way
        void resume(Registration registration) {
            List<Registration> delivering = new ArrayList<>();
            synchronized (this) {
                if (closed) {
                    return;
                }
                if (connection == null || (!registration.lost && generation == registration.generation)) {
                    if (connection != null) {
                        LOGGER.log(Level.WARNING, "The PostgreSQL listener is suspected silent; reconnecting");
                        discard(connection);
                        forEachRegistration(other -> {
                            if (other != registration && !other.lost) {
                                delivering.add(other);
                            }
                        });
                    }
                    connect();
                    LOGGER.log(Level.INFO, "The PostgreSQL listener reconnected");
                    delivering.forEach(other -> other.generation = generation);
                }
                registration.generation = generation;
                registration.lost = false;
            }
            for (Registration other : delivering) {
                notify(other.listener::onGap);
            }
        }

        // proves that a notification sent through the registration's data source reaches the session; a probe lost
        // because the session reconnected meanwhile proves nothing and is repeated
        void requireDelivery(Registration registration) {
            for (int attempt = 1; ; attempt++) {
                long probed;
                synchronized (this) {
                    probed = generation;
                }
                if (probe(registration.dataSource)) {
                    return;
                }
                boolean reconnected;
                synchronized (this) {
                    reconnected = connection == null || generation != probed;
                }
                if (!reconnected || attempt == PROBE_ATTEMPTS) {
                    throw new StoreException("The listener connection received no notification within "
                            + PROBE_TIMEOUT_SECONDS + " s (transaction-mode pooler?); point the R2DBC ConnectionFactory "
                            + "directly at the database or at a session-mode pooler", false);
                }
            }
        }

        // connects and listens on every channel; fails with a StoreException
        private void connect() {
            Connection opened;
            try {
                opened = Mono.from(factory.create()).block(CONNECT_TIMEOUT);
            } catch (RuntimeException e) {
                throw new StoreException("Connecting the PostgreSQL listener failed: " + e.getMessage(), e, false);
            }
            if (opened == null) {
                throw new StoreException("Connecting the PostgreSQL listener failed", false);
            }
            try {
                PostgresqlConnection postgres = unwrap(opened);
                listen(opened, probeChannel);
                for (String channel : channels.keySet()) {
                    listen(opened, channel);
                }
                notifications = postgres.getNotifications().subscribe(this::dispatch,
                        failure -> lost(opened, failure), () -> lost(opened, null));
                connection = opened;
                generation++;
            } catch (RuntimeException e) {
                closeQuietly(opened);
                throw e instanceof StoreException storeException ? storeException
                        : new StoreException("Listening on PostgreSQL failed: " + e.getMessage(), e, false);
            }
        }

        @SuppressWarnings("ReferenceEquality") // a wrapper unwrapping to itself ends the chain
        private static PostgresqlConnection unwrap(Connection connection) {
            Object current = connection;
            while (!(current instanceof PostgresqlConnection) && current instanceof Wrapped<?> wrapped) {
                Object inner = wrapped.unwrap();
                if (inner == current) {
                    break;
                }
                current = inner;
            }
            if (current instanceof PostgresqlConnection postgres) {
                return postgres;
            }
            throw new StoreException("The listener's ConnectionFactory must create r2dbc-postgresql connections, not "
                    + connection.getClass().getName(), false);
        }

        private static void listen(Connection connection, String channel) {
            execute(connection, "LISTEN " + PostgresTable.quote(channel));
        }

        private static void execute(Connection connection, String sql) {
            try {
                Flux.from(connection.createStatement(sql).execute()).flatMap(Result::getRowsUpdated).then()
                        .block(CONNECT_TIMEOUT);
            } catch (RuntimeException e) {
                throw new StoreException("Listening on PostgreSQL failed (" + sql + "): " + e.getMessage(), e, false);
            }
        }

        private void dispatch(Notification notification) {
            String payload = notification.getParameter();
            if (probeChannel.equals(notification.getName())) {
                CompletableFuture<Void> probe = payload == null ? null : probes.remove(payload);
                if (probe != null) {
                    probe.complete(null);
                }
                return;
            }
            List<Registration> registrations = channels.get(notification.getName());
            Message message = payload == null ? null : parse(payload);
            if (registrations == null || message == null) {
                return;
            }
            for (Registration registration : registrations) {
                try {
                    registration.listener.onMessages(List.of(message));
                } catch (RuntimeException e) {
                    LOGGER.log(Level.WARNING, "A listener failed on a notification", e);
                }
            }
        }

        // a notification sent through JDBC must arrive on this session; false if it does not within the timeout.
        // A failure to send (the store down) proves nothing about the session and counts as arrived
        private boolean probe(DataSource dataSource) {
            String token = UUID.randomUUID().toString();
            CompletableFuture<Void> arrived = new CompletableFuture<>();
            probes.put(token, arrived);
            try (java.sql.Connection jdbc = dataSource.getConnection()) {
                int previousTimeout = PostgresStore.bound(jdbc, Duration.ofSeconds(PROBE_TIMEOUT_SECONDS));
                try (PreparedStatement statement = jdbc.prepareStatement("SELECT pg_notify(?, ?)")) {
                    statement.setString(1, probeChannel);
                    statement.setString(2, token);
                    statement.executeQuery().close();
                } finally {
                    PostgresStore.unbound(jdbc, previousTimeout);
                }
            } catch (SQLException | RuntimeException e) {
                probes.remove(token);
                return true;
            }
            try {
                arrived.get(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                return true;
            } catch (TimeoutException e) {
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return true;
            } catch (java.util.concurrent.ExecutionException e) {
                return true;
            } finally {
                probes.remove(token);
            }
        }

        // the session broke: every cache is told, and resumes it when it can
        @SuppressWarnings("ReferenceEquality") // only the connection that broke is the current one
        private void lost(Connection broken, @Nullable Throwable failure) {
            List<Registration> told = new ArrayList<>();
            synchronized (this) {
                if (connection != broken || closed) {
                    return;
                }
                discard(broken);
                forEachRegistration(registration -> {
                    registration.lost = true;
                    told.add(registration);
                });
            }
            LOGGER.log(Level.WARNING, "The PostgreSQL listener lost its connection{0}",
                    failure == null ? "" : " (" + failure.getMessage() + ")");
            Throwable cause = failure != null ? failure : new IllegalStateException("the listening session ended");
            told.forEach(registration -> notify(() -> registration.listener.onLost(cause)));
        }

        // lets go of a broken connection; its notifications end, and its loss reported later is ignored
        private synchronized void discard(Connection broken) {
            connection = null;
            if (notifications != null) {
                notifications.dispose();
            }
            closeQuietly(broken);
        }

        private static void notify(Runnable call) {
            try {
                call.run();
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "A listener failed", e);
            }
        }

        private void forEachRegistration(java.util.function.Consumer<Registration> action) {
            for (List<Registration> registrations : channels.values()) {
                registrations.forEach(action);
            }
        }

        void unregister(Registration registration) {
            boolean empty;
            synchronized (SESSIONS) {
                synchronized (this) {
                    List<Registration> registrations = channels.get(registration.channel);
                    if (registrations != null) {
                        registrations.remove(registration);
                        if (registrations.isEmpty()) {
                            channels.remove(registration.channel);
                            if (connection != null) {
                                try {
                                    execute(connection, "UNLISTEN " + PostgresTable.quote(registration.channel));
                                } catch (StoreException e) {
                                    // the session breaks or closes anyway
                                }
                            }
                        }
                    }
                    empty = channels.isEmpty();
                    if (empty) {
                        close();
                        SESSIONS.remove(factory, this);
                    }
                }
            }
        }

        private void close() {
            closed = true;
            if (notifications != null) {
                notifications.dispose();
            }
            if (connection != null) {
                closeQuietly(connection);
                connection = null;
            }
        }

        // without waiting: a broken connection may take its timeout to close, and is closed under the session's lock
        private static void closeQuietly(Connection connection) {
            try {
                Mono.from(connection.close()).timeout(CONNECT_TIMEOUT).onErrorResume(e -> Mono.empty()).subscribe();
            } catch (RuntimeException ignored) {
                // closing a broken connection
            }
        }
    }
}
