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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.PGStream;
import org.postgresql.core.QueryExecutorBase;
import org.postgresql.util.DriverInfo;

import javax.sql.DataSource;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Listening on PostgreSQL through pgjdbc (VISION.md, "PostgreSQL listening without R2DBC" and "Connection sharing"):
 * one session per listener data source (by identity), shared by every cache instance using it, each cache on a
 * channel of its own (table and discriminator), dispatched by channel name. The payload is {@code kind|hash|version}.
 * <p>
 * The session is one connection of the data source whose stream the listener takes over. pgjdbc connects (with
 * whatever authentication and TLS the data source is configured for) and runs the first {@code LISTEN}s; then the
 * listener reads pgjdbc's stream itself, on a thread of its own, parsing notifications and skipping every other
 * message. pgjdbc's own way to them ({@code getNotifications}) hands out only about 750 notifications/s per session;
 * reading the stream directly is as fast as R2DBC was (measured, VISION.md). Later {@code LISTEN}s and
 * {@code UNLISTEN}s are sent on the same stream as simple queries, answered through the listener's thread. The stream
 * is reached through reflection on one protected field, checked at the first subscription.
 * <p>
 * The session proves it receives notifications on every subscription: a notification sent through the store's data
 * source must arrive within 5 seconds (which detects a transaction-mode pooler). A lost session is reported as
 * {@code onLost()} and re-established only when a cache asks ({@code resume()}, VISION.md, "Supervision"): once per
 * loss, whoever asks first; and also when a cache suspects the session went silent (its heartbeat), which then counts
 * as a gap for the other caches on it.
 * <p>
 * The session's threads only enqueue into the caches' own queues (the core's delivery), so they never call into a
 * cache's locks.
 */
// the SQL runs against the user's database, unknown to the IDE; what is formatted into it are quoted channel names
@SuppressWarnings({"SqlNoDataSourceInspection", "java:S2077", "SqlSourceToSinkFlow"})
final class PostgresListener {

    private static final Logger LOGGER = System.getLogger("io.github.oberhoff.distributedcaffeine.postgresql");
    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(10);
    static final long PROBE_TIMEOUT_SECONDS = 5;
    private static final int PROBE_ATTEMPTS = 3;
    private static final IdentityHashMap<DataSource, Session> SESSIONS = new IdentityHashMap<>();

    // the listening sessions open: none once every cache using them is closed (tests check it, a leak would show)
    static int openSessions() {
        synchronized (SESSIONS) {
            return SESSIONS.size();
        }
    }
    // pgjdbc's stream behind a connection: protected, with no getter; null if this pgjdbc does not have it
    private static final @Nullable Field PG_STREAM = pgStreamField();

    private PostgresListener() {
    }

    /**
     * Subscribes a cache instance to its channel: returns once the session listens on it and has proven that
     * notifications arrive, so every write committed afterwards is delivered (contract point 4).
     */
    @SuppressWarnings("java:S1181") // whatever fails, the registration is released before it is rethrown
    static ChangeSubscription subscribe(DataSource listenerDataSource, DataSource dataSource, String channel,
                                        ChangeListener listener) {
        if (PG_STREAM == null) {
            throw new StoreException("Listening on PostgreSQL reads pgjdbc's stream, reached through the field "
                    + "QueryExecutorBase.pgStream, which pgjdbc " + DriverInfo.DRIVER_VERSION + " does not have or "
                    + "does not let be reached; use a pgjdbc version this library supports", false);
        }
        Session session;
        Registration registration;
        synchronized (SESSIONS) {
            session = SESSIONS.computeIfAbsent(listenerDataSource, Session::new);
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
     * Whether the session of the listener data source has dispatched everything notified before this call (tests): a
     * notification sent now must arrive through it; notifications arrive in commit order, so everything before it has
     * arrived too.
     */
    static boolean barrier(DataSource listenerDataSource, DataSource dataSource) {
        Session session;
        synchronized (SESSIONS) {
            session = SESSIONS.get(listenerDataSource);
        }
        if (session == null) {
            return true;
        }
        synchronized (session) {
            if (session.held == null) {
                return false;
            }
        }
        return session.probe(dataSource);
    }

    // kind|hash|version; anything else (garbage on the channel, a payload of an earlier layout) is ignored
    static @Nullable Change parse(String payload) {
        String[] parts = payload.split("\\|", -1);
        if (parts.length != 3 || parts[1].isEmpty()) {
            return null;
        }
        try {
            return new Change(Change.Kind.valueOf(parts[0]), parts[1], Long.parseLong(parts[2]));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    // the message and SQL state of an ErrorResponse's body: fields of a type byte and a string each, ended by a zero
    // byte (pgjdbc sets client_encoding to UTF8)
    static String error(byte[] body) {
        String message = "";
        String state = "";
        int at = 0;
        while (at < body.length && body[at] != 0) {
            byte field = body[at++];
            int end = at;
            while (end < body.length && body[end] != 0) {
                end++;
            }
            String value = new String(body, at, end - at, StandardCharsets.UTF_8);
            if (field == 'M') {
                message = value;
            } else if (field == 'C') {
                state = value;
            }
            at = end + 1;
        }
        return message + " (SQL state " + state + ")";
    }

    // InaccessibleObjectException (a module not opening it) and SecurityException are RuntimeExceptions
    @SuppressWarnings("java:S3011") // the one internal field taken over, by design (VISION.md)
    private static @Nullable Field pgStreamField() {
        try {
            Field field = QueryExecutorBase.class.getDeclaredField("pgStream");
            if (field.getType() != PGStream.class) {
                return null;
            }
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException | RuntimeException e) {
            return null;
        }
    }

    private static String listen(String channel) {
        return "LISTEN " + PostgresTable.quote(channel);
    }

    // aborted, not closed: the stream was taken over, so the connection must never be used again, and a pool must not
    // hand it out again. Closed after that, which hands it back to a pool as broken (HikariCP evicts it, failing to
    // reset its state). On a thread of its own: a broken connection must hold nothing up, and is released under the
    // session's lock
    @SuppressWarnings("java:S1166") // a connection being let go of fails as it pleases
    private static void release(Connection connection) {
        Thread closer = new Thread(() -> {
            try {
                connection.abort(Runnable::run);
            } catch (SQLException | RuntimeException ignored) {
                // closed already
            }
            try {
                connection.close();
            } catch (SQLException | RuntimeException ignored) {
                // a pool finding it broken
            }
        }, "distributed-caffeine-postgresql-release");
        closer.setDaemon(true);
        closer.start();
    }

    private static final class Registration implements ChangeSubscription {

        private final Session session;
        private final String channel;
        private final ChangeListener listener;
        private final DataSource dataSource;
        // guarded by the session (not expressible as @GuardedBy): the session's connection it was last delivered on,
        // and whether it was told that delivery stopped
        private long generation;
        private boolean lost;
        private volatile boolean closed;

        Registration(Session session, String channel, ChangeListener listener, DataSource dataSource) {
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

    // a connection whose stream the listener took over: read only by its session's thread, written (LISTEN, UNLISTEN)
    // only under this object's monitor
    private static final class Held {

        private final Connection connection;
        private final PGStream stream;
        // the commands sent and not yet answered, in the order sent: each ReadyForQuery answers the oldest
        private final Queue<CompletableFuture<@Nullable Void>> commands = new ConcurrentLinkedQueue<>();
        @GuardedBy("this")
        private boolean ended;

        Held(Connection connection, PGStream stream) {
            this.connection = connection;
            this.stream = stream;
        }

        // a simple query ('Q'), waiting for its ReadyForQuery; an error ends the session (see read)
        void execute(String sql) {
            CompletableFuture<@Nullable Void> answered = new CompletableFuture<>();
            synchronized (this) {
                if (ended) {
                    throw new StoreException("The PostgreSQL listener's session ended", false);
                }
                commands.add(answered);
                try {
                    byte[] text = stream.getEncoding().encode(sql);
                    stream.sendChar('Q');
                    stream.sendInteger4(4 + text.length + 1);
                    stream.send(text);
                    stream.sendChar(0);
                    stream.flush();
                } catch (IOException e) {
                    throw new StoreException("Listening on PostgreSQL failed (" + sql + "): " + e.getMessage(), e,
                            false);
                }
            }
            try {
                answered.get(COMMAND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw new StoreException("Listening on PostgreSQL failed (" + sql + "): " + cause.getMessage(), cause,
                        false);
            } catch (TimeoutException e) {
                throw new StoreException("Listening on PostgreSQL failed (" + sql + "): no answer within "
                        + COMMAND_TIMEOUT.toSeconds() + " s", e, false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StoreException("Listening on PostgreSQL was interrupted (" + sql + ")", e, false);
            }
        }

        void answered() {
            CompletableFuture<@Nullable Void> oldest = commands.poll();
            if (oldest != null) {
                oldest.complete(null);
            }
        }

        // no command is sent after this; those waiting fail
        void end(Throwable cause) {
            synchronized (this) {
                ended = true;
            }
            for (CompletableFuture<@Nullable Void> waiting; (waiting = commands.poll()) != null; ) {
                waiting.completeExceptionally(cause);
            }
        }
    }

    // one listening session; its state is guarded by the session's monitor, dispatch reads concurrent structures
    private static final class Session {

        private final DataSource listenerDataSource;
        private final Map<String, List<Registration>> channels = new ConcurrentHashMap<>();
        private final Map<String, CompletableFuture<@Nullable Void>> probes = new ConcurrentHashMap<>();
        private final String probeChannel = "dc_probe_" + UUID.randomUUID().toString().replace("-", "");
        @GuardedBy("this")
        private @Nullable Held held;
        // counts the connections established, so that a loss is re-established once, whoever asks first
        @GuardedBy("this")
        private long generation;
        @GuardedBy("this")
        private boolean closed;

        Session(DataSource listenerDataSource) {
            this.listenerDataSource = listenerDataSource;
        }

        Registration register(String channel, ChangeListener listener, DataSource dataSource) {
            Registration registration = new Registration(this, channel, listener, dataSource);
            channels.computeIfAbsent(channel, ignored -> new CopyOnWriteArrayList<>()).add(registration);
            return registration;
        }

        // listens on the registration's channel, connecting if there is no connection (re-established for the caches
        // that lost it, who learn of it when they resume)
        synchronized void listen(Registration registration) {
            if (held != null) {
                try {
                    held.execute(PostgresListener.listen(registration.channel));
                } catch (StoreException e) {
                    // broken before its loss was noticed: connected anew below
                    discard(held);
                }
            }
            if (held == null) {
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
                if (held == null || (!registration.lost && generation == registration.generation)) {
                    if (held != null) {
                        LOGGER.log(Level.WARNING, "The PostgreSQL listener is suspected silent; reconnecting");
                        discard(held);
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
                    reconnected = held == null || generation != probed;
                }
                if (!reconnected || attempt == PROBE_ATTEMPTS) {
                    throw new StoreException("The listener connection received no notification within "
                            + PROBE_TIMEOUT_SECONDS + " s (transaction-mode pooler?); listen through a data source "
                            + "that reaches the database directly or through a session-mode pooler "
                            + "(PostgresAdapter.Builder.withListenerDataSource)", false);
                }
            }
        }

        // takes a connection of the data source over and listens on every channel; fails with a StoreException
        @GuardedBy("this")
        @SuppressWarnings("java:S2095") // the connection is held until the session lets go of it (release)
        private void connect() {
            Connection connection;
            try {
                connection = listenerDataSource.getConnection();
            } catch (SQLException | RuntimeException e) {
                throw new StoreException("Connecting the PostgreSQL listener failed: " + e.getMessage(), e, false);
            }
            try {
                if (!connection.isWrapperFor(BaseConnection.class)) {
                    throw new StoreException("The listener's data source must hand out pgjdbc connections "
                            + "(org.postgresql), not " + connection.getClass().getName(), false);
                }
                // LISTEN takes effect at its commit
                connection.setAutoCommit(true);
                PostgresStore.bound(connection, COMMAND_TIMEOUT);
                try (Statement statement = connection.createStatement()) {
                    statement.execute(PostgresListener.listen(probeChannel));
                    for (String channel : channels.keySet()) {
                        statement.execute(PostgresListener.listen(channel));
                    }
                }
                // what pgjdbc received meanwhile, read without waiting; then nothing is left in its hands
                PGNotification[] early = connection.unwrap(PGConnection.class).getNotifications();
                // the session idles between notifications: no read timeout (a silent one is the core's heartbeat's
                // to find)
                connection.setNetworkTimeout(Runnable::run, 0);
                PGStream stream = (PGStream) streamField().get(
                        connection.unwrap(BaseConnection.class).getQueryExecutor());
                Held taken = new Held(connection, stream);
                held = taken;
                generation++;
                if (early != null) {
                    for (PGNotification notification : early) {
                        dispatch(notification.getName(), notification.getParameter());
                    }
                }
                Thread reader = new Thread(() -> read(taken), "distributed-caffeine-postgresql-listener");
                reader.setDaemon(true);
                reader.start();
            } catch (SQLException | ReflectiveOperationException | RuntimeException e) {
                held = null;
                release(connection);
                throw e instanceof StoreException storeException ? storeException
                        : new StoreException("Listening on PostgreSQL failed: " + e.getMessage(), e, false);
            }
        }

        private static Field streamField() {
            Field field = PG_STREAM;
            if (field == null) {
                throw new IllegalStateException("checked when subscribing");
            }
            return field;
        }

        // the session's thread: the backend's messages, by their type byte and length (which counts itself).
        // NotificationResponse ('A') is dispatched, ReadyForQuery ('Z') answers a command, an ErrorResponse ('E')
        // ends the session (a LISTEN refused, the backend terminated); everything else is skipped
        @SuppressWarnings("java:S1181") // whatever ends the session, it is reported as lost
        private void read(Held taken) {
            PGStream stream = taken.stream;
            Throwable failure;
            try {
                for (; ; ) {
                    int type = stream.receiveChar();
                    int length = stream.receiveInteger4();
                    if (length < 4) {
                        throw new IOException("a backend message of type " + type + " with length " + length);
                    }
                    switch (type) {
                        case 'A' -> {
                            stream.receiveInteger4(); // the notifying backend's process id
                            String channel = stream.receiveCanonicalString();
                            dispatch(channel, stream.receiveString());
                        }
                        case 'Z' -> {
                            stream.skip(length - 4);
                            taken.answered();
                        }
                        case 'E' -> throw new StoreException("The PostgreSQL listener's session failed: "
                                + error(stream.receive(length - 4)), false);
                        default -> stream.skip(length - 4);
                    }
                }
            } catch (IOException | RuntimeException | Error e) {
                failure = e;
            }
            taken.end(failure);
            lost(taken, failure);
        }

        private void dispatch(String channel, @Nullable String payload) {
            if (probeChannel.equals(channel)) {
                CompletableFuture<@Nullable Void> probe = payload == null ? null : probes.remove(payload);
                if (probe != null) {
                    probe.complete(null);
                }
                return;
            }
            List<Registration> registrations = channels.get(channel);
            Change message = payload == null ? null : parse(payload);
            if (registrations == null || message == null) {
                return;
            }
            for (Registration registration : registrations) {
                try {
                    registration.listener.onChanges(List.of(message));
                } catch (RuntimeException e) {
                    LOGGER.log(Level.WARNING, "A listener failed on a notification", e);
                }
            }
        }

        // a notification sent through JDBC must arrive on this session; false if it does not within the timeout.
        // A failure to send (the store down) proves nothing about the session and counts as arrived
        private boolean probe(DataSource dataSource) {
            String token = UUID.randomUUID().toString();
            CompletableFuture<@Nullable Void> arrived = new CompletableFuture<>();
            probes.put(token, arrived);
            try (Connection jdbc = dataSource.getConnection()) {
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
            } catch (ExecutionException e) {
                return true;
            } finally {
                probes.remove(token);
            }
        }

        // the session broke: every cache is told, and resumes it when it can
        @SuppressWarnings("ReferenceEquality") // only the connection that broke is the current one
        private void lost(Held broken, Throwable failure) {
            List<Registration> told = new ArrayList<>();
            synchronized (this) {
                if (held != broken || closed) {
                    return;
                }
                discard(broken);
                forEachRegistration(registration -> {
                    registration.lost = true;
                    told.add(registration);
                });
            }
            LOGGER.log(Level.WARNING, "The PostgreSQL listener lost its connection ({0})", failure.getMessage());
            told.forEach(registration -> notify(() -> registration.listener.onLost(failure)));
        }

        // lets go of a broken connection; its thread ends, and its loss reported later is ignored
        @GuardedBy("this")
        private void discard(Held broken) {
            held = null;
            release(broken.connection);
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
            synchronized (SESSIONS) {
                synchronized (this) {
                    List<Registration> registrations = channels.get(registration.channel);
                    if (registrations != null) {
                        registrations.remove(registration);
                        if (registrations.isEmpty()) {
                            channels.remove(registration.channel);
                            if (held != null && !channels.isEmpty()) {
                                try {
                                    held.execute("UNLISTEN " + PostgresTable.quote(registration.channel));
                                } catch (StoreException e) {
                                    // the session breaks anyway
                                }
                            }
                        }
                    }
                    if (channels.isEmpty()) {
                        close();
                        SESSIONS.remove(listenerDataSource, this);
                    }
                }
            }
        }

        @GuardedBy("this")
        private void close() {
            closed = true;
            if (held != null) {
                release(held.connection);
                held = null;
            }
        }
    }
}
