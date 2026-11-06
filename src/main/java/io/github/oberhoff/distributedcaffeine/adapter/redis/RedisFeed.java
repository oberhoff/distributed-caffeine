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
package io.github.oberhoff.distributedcaffeine.adapter.redis;

import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;

/**
 * The change feed of the Redis store (VISION.md, "Redis adapters"): every write and expiry appends an entry to its
 * cache's stream in the script that makes it ({@link RedisScripts}), so the stream is complete and in write order.
 * Readers, shared by the cache instances using a driver (by identity), read the streams with blocking {@code XREAD}s,
 * each on a connection of its own, and hand each subscription the entries of its stream: on a single server or behind
 * Sentinel one reader per driver, reading every stream at once; in a cluster one per keyspace, as one {@code XREAD}
 * may only read streams of one slot, which a keyspace's hash tag is.
 * <p>
 * A reader holds no thread while it waits: it starts a read and handles its answer on a shared executor, where the
 * client library reads asynchronously (Lettuce, Redisson); with a blocking one (Jedis) a thread waits per read in
 * flight. One read per reader is in flight at a time, so a subscription's changes arrive in order, one batch at a
 * time.
 * <p>
 * A subscription starts at the stream's position when it subscribes (its last entry's ID and number, taken in one
 * script), so it receives every later write, however soon its reader includes its stream (within a block time, one
 * second). Each entry carries a number one above the entry before; a number skipped means entries were missed
 * (trimmed before they were read, or lost with a failover), reported as a gap.
 * <p>
 * A read that fails reports {@code onLost} to every subscription its reader delivers to, and the reader waits until a
 * cache asks ({@code resume()}, VISION.md, "Supervision"): the first to ask for a loss opens a new connection, and each
 * resumes from the stream's position then. A cache suspecting the reader went silent (its heartbeat) makes it open a
 * new connection too, while the others keep reading from where they were, missing nothing and told of nothing. In a
 * cluster, a slot moving to another node fails the read the same way, and the new connection reads from the slot's
 * new owner, as the client library routes it.
 * <p>
 * A server that answers as a single one but refuses a command across hash slots ({@code CROSSSLOT}: a managed service
 * sharding behind one endpoint, as Azure Managed Redis under its Enterprise cluster policy) refuses the shared read
 * once caches of two keyspaces use it: the reader then splits into one per keyspace, as in a cluster, each
 * subscription going on from its position (nothing missed, nothing reported), and the driver's later subscriptions
 * get a reader per keyspace at once.
 * <p>
 * Answers are handled on the feed's own threads, never the client library's, and only handed to the caches'
 * listeners, which enqueue them (the core's delivery), so the feed never calls into a cache's locks.
 *
 * @author Andreas Oberhoff
 */
final class RedisFeed {

    private static final Logger LOGGER = System.getLogger(RedisAdapter.class.getPackageName());
    // every command of the reader answers within this, beyond its block time. Fixed, unlike a cache's operation
    // timeout: no caller waits for the reader, which is shared by caches with different timeouts, and its failures are
    // recovered (or noticed by the heartbeat) by the library, so the bound follows from its own waits (as MongoWatcher)
    static final Duration TIMEOUT = Duration.ofSeconds(10);
    // how long a read waits for entries: also how soon a stream subscribed meanwhile is read (it misses nothing
    // meanwhile, starting at its own position)
    private static final Duration BLOCK = Duration.ofSeconds(1);
    private static final int COUNT = 1_000;
    // the route of every stream of a driver on a single server or behind Sentinel: one reader for all
    static final String ALL = "";
    // where answers are handled, on threads of the feed's own, never a client library's I/O threads
    private static final Executor HANDLING = daemons("distributed-caffeine-redis-feed");
    // where a client library that reads only blocking (Jedis) waits, a thread per read in flight
    static final Executor BLOCKING_READS = daemons("distributed-caffeine-redis-read");
    private static final IdentityHashMap<RedisDriver, Map<String, Reader>> READERS = new IdentityHashMap<>();
    // the drivers whose server refused a read across hash slots (CROSSSLOT): a reader per keyspace from then on
    private static final Set<RedisDriver> SPLIT = Collections.newSetFromMap(new IdentityHashMap<>());

    private RedisFeed() {
    }

    // where a stream stands: its last entry's ID ("0-0" for none) and number (0 for none)
    record Position(String id, long number) {

        Position {
            requireNonNull(id, "id cannot be null");
        }
    }

    private static Executor daemons(String name) {
        return Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
    }

    // the readers running: none once every cache using them is closed (tests check it, a leak would show)
    static int openReaders() {
        synchronized (READERS) {
            return READERS.values().stream().mapToInt(Map::size).sum();
        }
    }

    private static List<Reader> readers(RedisDriver driver) {
        synchronized (READERS) {
            Map<String, Reader> byRoute = READERS.get(driver);
            return byRoute == null ? List.of() : List.copyOf(byRoute.values());
        }
    }

    // tests: every reader of the driver loses its connection, as a network or server failure does that Lettuce does
    // not heal by itself (a connection reset it heals, replaying the waiting read from the same position, which loses
    // nothing): the waiting read fails and every subscription delivering is told of the loss
    static void simulateLoss(RedisDriver driver) {
        readers(driver).forEach(Reader::simulateLoss);
    }

    // tests: every subscription of the stream is told of a gap at its reader's next round (within a block time), as
    // a skipped number tells it, without an entry to read
    static void simulateGap(RedisDriver driver, String stream) {
        readers(driver).forEach(reader -> reader.simulateGap(stream));
    }

    // tests: whether every subscription of the store's stream has handed its listener everything written to the stream
    // before this call; false while one is lost
    static boolean barrier(RedisDriver driver, RedisStore store, String stream) {
        List<Reader> readers = readers(driver);
        if (readers.isEmpty()) {
            return true;
        }
        Position last = store.position();
        return readers.stream().allMatch(reader -> reader.barrier(stream, last));
    }

    // subscribes to a stream through the driver's reader of its route: the keyspace in a cluster, or where the server
    // refused a read across slots; ALL otherwise
    static ChangeSubscription subscribe(RedisDriver driver, RedisStore store, String stream, String keyspace,
                                        boolean clustered, ChangeListener listener) {
        // before registering, so that every later entry is above it
        Registration registration = new Registration(store, stream, keyspace, listener, store.position());
        Reader reader;
        synchronized (READERS) {
            reader = readerOf(driver, clustered || SPLIT.contains(driver) ? keyspace : ALL);
            reader.add(registration);
        }
        reader.read();
        return registration;
    }

    // the driver's reader of the route, opened where there is none; under READERS
    private static Reader readerOf(RedisDriver driver, String route) {
        return READERS.computeIfAbsent(driver, ignored -> new LinkedHashMap<>())
                .computeIfAbsent(route, ignored -> new Reader(driver, route));
    }

    // whether a stream ID is after another ("<milliseconds>-<sequence>")
    static boolean after(String id, String other) {
        long[] a = parts(id);
        long[] b = parts(other);
        return a[0] > b[0] || (a[0] == b[0] && a[1] > b[1]);
    }

    private static long[] parts(String id) {
        int dash = id.indexOf('-');
        return dash < 0 ? new long[]{Long.parseLong(id), 0}
                : new long[]{Long.parseLong(id.substring(0, dash)), Long.parseLong(id.substring(dash + 1))};
    }

    private static String older(String id, String other) {
        return after(id, other) ? other : id;
    }

    private static final class Registration implements ChangeSubscription {

        private final RedisStore store;
        private final String stream;
        private final String keyspace;
        private final ChangeListener listener;
        // read and advanced by the reader's thread, set anew by a resume while the subscription is lost
        @SuppressWarnings("java:S3077") // a Position is an immutable record: the volatile reference is all it needs
        private volatile Position position;
        private volatile boolean closed;
        // a gap to report at the reader's next round (simulateGap)
        private volatile boolean gapPending;
        // guarded by the reader
        private boolean lost;
        private long generation;
        private @Nullable Reader reader;

        @SuppressWarnings("java:S2637") // every field is assigned here, the volatile position included
        Registration(RedisStore store, String stream, String keyspace, ChangeListener listener, Position position) {
            this.store = store;
            this.stream = stream;
            this.keyspace = keyspace;
            this.listener = listener;
            this.position = position;
        }

        // on the reader's thread: the entries after its position, a number skipped reported as a gap first; the
        // position moves only once the listener has them, so a barrier never passes before
        void deliver(List<RedisStreamReader.Entry> entries) {
            List<Change> changes = new ArrayList<>();
            boolean gap = false;
            Position at = position;
            for (RedisStreamReader.Entry entry : entries) {
                if (!after(entry.id(), at.id())) {
                    // read for another subscription of the stream, further behind
                    continue;
                }
                long number = Long.parseLong(text(entry, "n"));
                if (number != at.number() + 1) {
                    gap = true;
                }
                changes.add(change(entry));
                at = new Position(entry.id(), number);
            }
            if (!closed) {
                if (gap) {
                    listener.onGap();
                }
                if (!changes.isEmpty()) {
                    listener.onChanges(changes);
                }
            }
            position = at;
        }

        @Override
        public void resume() {
            Reader current = reader;
            if (!closed && current != null) {
                current.resume(this);
            }
        }

        @Override
        public void close() {
            Reader current = reader;
            if (!closed && current != null) {
                closed = true;
                current.remove(this);
            }
        }

        private static Change change(RedisStreamReader.Entry entry) {
            Change.Kind kind = switch (text(entry, "c")) {
                case "W" -> Change.Kind.WRITTEN;
                case "I" -> Change.Kind.INVALIDATED;
                case "E" -> Change.Kind.EXPIRED;
                default -> throw new IllegalStateException("An entry of an unknown kind: " + text(entry, "c"));
            };
            return new Change(kind, text(entry, "h"), RedisStore.version(Long.parseLong(text(entry, "vh")),
                    Long.parseLong(text(entry, "vl"))));
        }

        private static String text(RedisStreamReader.Entry entry, String field) {
            String value = entry.fields().get(field);
            if (value == null) {
                throw new IllegalStateException("An entry without its field " + field + ": " + entry.id());
            }
            return value;
        }
    }

    private static final class Reader {

        private final RedisDriver driver;
        private final String route;
        private final RedisSession session;
        private final List<Registration> registrations = new ArrayList<>();
        // null while lost, until a cache resumes
        private @Nullable RedisStreamReader connection;
        // counts the connections opened: a read failing on one replaced meanwhile is no loss
        private long generation;
        private boolean ended;
        // a read started and not yet handled: at most one, so that every subscription's changes arrive in order
        private boolean reading;

        Reader(RedisDriver driver, String route) {
            this.driver = driver;
            this.route = route;
            this.session = driver.open(TIMEOUT);
            this.connection = session.openReader();
        }

        synchronized void add(Registration registration) {
            registration.reader = this;
            registration.generation = generation;
            registrations.add(registration);
        }

        // the last one out ends the reader
        void remove(Registration registration) {
            RedisStreamReader closing = null;
            boolean last = false;
            synchronized (READERS) {
                synchronized (this) {
                    registrations.remove(registration);
                    if (registrations.isEmpty()) {
                        last = true;
                        ended = true;
                        Map<String, Reader> byRoute = READERS.get(driver);
                        if (byRoute != null) {
                            byRoute.remove(route);
                            if (byRoute.isEmpty()) {
                                READERS.remove(driver);
                            }
                        }
                        closing = connection;
                        connection = null;
                    }
                }
            }
            if (last) {
                if (closing != null) {
                    closing.close();
                }
                session.close();
            }
        }

        // the first resume after a loss opens a new connection, as does one from a subscription that is not lost
        // (its heartbeat suspects the reader silent); a lost subscription then starts again from the stream's position
        void resume(Registration registration) {
            boolean reposition;
            RedisStreamReader replaced = null;
            synchronized (this) {
                if (ended) {
                    return;
                }
                if (connection == null || (!registration.lost && registration.generation == generation)) {
                    replaced = connection;
                    connection = session.openReader();
                    generation++;
                }
                registration.generation = generation;
                reposition = registration.lost;
            }
            if (replaced != null) {
                // fails the read waiting on it, which is then taken for no loss (another generation)
                replaced.close();
            }
            if (reposition) {
                // outside the lock: a round trip, through the cache's own session; throws if the store cannot answer
                RedisFeed.Position now = registration.store.position();
                synchronized (this) {
                    registration.position = now;
                    registration.lost = false;
                }
            }
            read();
        }

        // closes the connection in its generation, so the read waiting on it fails as a loss (see simulateLoss)
        void simulateLoss() {
            RedisStreamReader closing;
            synchronized (this) {
                closing = connection;
            }
            if (closing != null) {
                closing.close();
            }
        }

        synchronized void simulateGap(String stream) {
            registrations.stream().filter(registration -> registration.stream.equals(stream))
                    .forEach(registration -> registration.gapPending = true);
        }

        synchronized boolean barrier(String stream, Position last) {
            for (Registration registration : registrations) {
                if (registration.stream.equals(stream) && !registration.closed && (registration.lost
                        || registration.gapPending || after(last.id(), registration.position.id()))) {
                    return false;
                }
            }
            return true;
        }

        private List<Registration> delivering() {
            return registrations.stream().filter(registration -> !registration.lost && !registration.closed).toList();
        }

        // starts a read, unless one is in flight, or there is no connection or nobody to deliver to; its answer is
        // handled on the feed's threads, which start the next (the handler takes every outcome, failures included, so
        // the future it returns is not needed)
        @SuppressWarnings("FutureReturnValueIgnored")
        void read() {
            RedisStreamReader used;
            long current;
            List<Registration> delivering;
            Map<String, String> afterIds = new LinkedHashMap<>();
            synchronized (this) {
                if (ended || reading || connection == null) {
                    return;
                }
                delivering = delivering();
                if (delivering.isEmpty()) {
                    return;
                }
                reading = true;
                used = connection;
                current = generation;
                // a stream read by several subscriptions from the oldest position, each taking what is after its own
                delivering.forEach(registration -> afterIds.merge(registration.stream, registration.position.id(),
                        RedisFeed::older));
            }
            CompletableFuture<Map<String, List<RedisStreamReader.Entry>>> answer;
            try {
                answer = used.readAsync(afterIds, BLOCK, COUNT);
            } catch (RuntimeException e) {
                answer = CompletableFuture.failedFuture(e);
            }
            answer.whenCompleteAsync((entries, failure) -> answered(used, current, delivering, entries, failure),
                    HANDLING);
        }

        private void answered(RedisStreamReader used, long current, List<Registration> delivering,
                              @Nullable Map<String, List<RedisStreamReader.Entry>> entries,
                              @Nullable Throwable failure) {
            synchronized (this) {
                reading = false;
            }
            if (failure != null || entries == null) {
                Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                        ? failure.getCause() : failure;
                if (route.equals(ALL) && cause != null && crossSlot(cause)) {
                    split(cause);
                    return;
                }
                lost(used, current, cause != null ? cause : new IllegalStateException("A read answered nothing"));
            } else {
                deliverAll(delivering, entries);
            }
            read();
        }

        // the entries of a read to each subscription it was for, a gap pending reported first
        private static void deliverAll(List<Registration> delivering,
                                       Map<String, List<RedisStreamReader.Entry>> entries) {
            for (Registration registration : delivering) {
                if (registration.gapPending) {
                    registration.gapPending = false;
                    registration.listener.onGap();
                }
                List<RedisStreamReader.Entry> of = entries.get(registration.stream);
                if (of != null) {
                    try {
                        registration.deliver(of);
                    } catch (RuntimeException e) {
                        // an entry this reader cannot parse, or a listener failing: the others go on
                        LOGGER.log(Logger.Level.WARNING, "Delivering the changes of " + registration.stream
                                + " failed; reported as a gap", e);
                        registration.listener.onGap();
                    }
                }
            }
        }

        // the server refused the shared read across hash slots: this reader ends, and each subscription moves to the
        // driver's reader of its keyspace with its position and state, missing nothing; later ones go there at once
        private void split(Throwable cause) {
            List<Registration> moving;
            RedisStreamReader closing;
            Set<Reader> targets = Collections.newSetFromMap(new IdentityHashMap<>());
            synchronized (READERS) {
                synchronized (this) {
                    if (ended) {
                        return;
                    }
                    ended = true;
                    moving = List.copyOf(registrations);
                    registrations.clear();
                    closing = connection;
                    connection = null;
                }
                SPLIT.add(driver);
                Map<String, Reader> byRoute = READERS.get(driver);
                if (byRoute != null) {
                    byRoute.remove(route);
                }
                for (Registration registration : moving) {
                    Reader target = readerOf(driver, registration.keyspace);
                    target.add(registration);
                    targets.add(target);
                }
            }
            LOGGER.log(Logger.Level.INFO, "The server refused reading the changes of several keyspaces at once ("
                    + cause.getMessage() + "); reading them by keyspace from now on");
            if (closing != null) {
                closing.close();
            }
            session.close();
            targets.forEach(Reader::read);
        }

        // a read failed: a loss for every subscription delivering, unless the connection was replaced in the meantime
        // (a resume) or the reader ended
        private void lost(RedisStreamReader used, long current, Throwable cause) {
            List<Registration> lost;
            synchronized (this) {
                if (ended || current != generation) {
                    return;
                }
                connection = null;
                lost = delivering();
                lost.forEach(registration -> registration.lost = true);
            }
            used.close();
            LOGGER.log(Logger.Level.WARNING, "Reading the changes on Redis failed; delivery lost until resumed: "
                    + cause.getMessage());
            lost.forEach(registration -> registration.listener.onLost(cause));
        }

        // whether a read failed for spanning hash slots, as a server refuses it where it shards behind one endpoint
        private static boolean crossSlot(Throwable failure) {
            return failure instanceof RedisCommandException e && e.kind() == RedisCommandException.Kind.REFUSED
                    && String.valueOf(e.getMessage()).contains("CROSSSLOT");
        }
    }
}
