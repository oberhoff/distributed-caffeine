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
package io.github.oberhoff.distributedcaffeine.mongodb;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoException;
import com.mongodb.MongoServerException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.github.oberhoff.distributedcaffeine.spi.Kind;
import io.github.oberhoff.distributedcaffeine.spi.Listener;
import io.github.oberhoff.distributedcaffeine.spi.Message;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.Subscription;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Watching MongoDB (VISION.md, "Distribution: messages carry no values" and "Connection sharing"): one change stream
 * per client (by identity) and database, shared by every cache instance using them, filtered on the server to the
 * subscribed collections and to inserts, updates and replaces (deletes carry no meaning), with the values projected
 * out. A cache joining with a collection not covered yet reopens the stream from its resume token, so nothing is lost;
 * when the last cache of a collection leaves, the watching thread reopens it the same way without that collection.
 * <p>
 * The kind of an event comes from the state in the document ({@code fullDocument} of an insert or replace) or in the
 * changed fields of an update: {@code TOMBSTONE} is {@code INVALIDATED}, {@code EXPIRED} is {@code EXPIRED},
 * {@code VALUE} is {@code WRITTEN}. An update leaving the state as it was names none: it is {@code WRITTEN} if it
 * wrote a value (a flag the stream computes before projecting the value out), otherwise {@code INVALIDATED} (a
 * tombstone or expired row given a new version, as by {@code invalidateAll}); the control document's time is
 * {@code INVALIDATED_ALL}. A failed stream is reported as {@code onLost()} and reopened only when a cache asks
 * ({@code resume()}, VISION.md, "Supervision"): once per loss, whoever asks first, from the resume token, so that the
 * caches still on it miss nothing; also when a cache suspects the stream went silent (its heartbeat). The core counts
 * every resume as a gap for the cache resuming. A history the stream can no longer resume from (errors 280 and 286,
 * e.g. after a rollback or a too short oplog) is a gap: reopened at once without the token ({@code onGap()}), or, if
 * that fails, lost.
 * <p>
 * The watching thread only enqueues into the caches' own queues, so it never calls into a cache's locks.
 */
final class MongoWatcher {

    private static final Logger LOGGER = System.getLogger("io.github.oberhoff.distributedcaffeine.mongodb");
    private static final long MAX_AWAIT_MILLIS = 100;
    // the flag of an update that wrote a value, in its changed fields (a name no document field has)
    private static final String VALUE_WRITTEN = "valueWritten";
    // every operation of the watcher answers within this (the client's own timeout aside): a getMore that waits
    // longer, on a network that silently stopped carrying or a server that does not answer, fails and the stream is
    // resumed or reopened, instead of blocking the watching thread for good while the caches believe they are
    // synchronized (the getMores themselves wait at most MAX_AWAIT_MILLIS for events)
    static final long OPERATION_TIMEOUT_SECONDS = 10;
    private static final long JOIN_TIMEOUT_SECONDS = 10;
    private static final Set<Integer> HISTORY_LOST = Set.of(280, 286, 136);
    private static final IdentityHashMap<MongoClient, Map<String, Watcher>> WATCHERS = new IdentityHashMap<>();

    private MongoWatcher() {
    }

    /**
     * Subscribes a cache instance: returns once the change stream covers its collection, so every write committed
     * afterwards is delivered (contract point 4).
     */
    @SuppressWarnings("java:S1181") // whatever fails, the registration is released before it is rethrown
    static Subscription subscribe(MongoClient client, MongoLocation location, Listener listener,
                                  MongoClusterTime clusterTime) {
        // a read, so outside the lock every cache of the JVM takes to subscribe and to leave
        BsonTimestamp since = majorityTime(client, MongoSchema.database(client, location), location.collectionName);
        Watcher watcher;
        Registration registration;
        synchronized (WATCHERS) {
            watcher = WATCHERS.computeIfAbsent(client, ignored -> new java.util.HashMap<>())
                    .computeIfAbsent(location.databaseName, name -> new Watcher(client, location));
            registration = watcher.register(location, listener, since, clusterTime);
        }
        try {
            watcher.cover(location.collectionName);
        } catch (RuntimeException | Error e) {
            registration.close();
            throw e;
        }
        return registration;
    }

    /**
     * Whether the watcher of the location's database has delivered everything written before this call (tests): a
     * change of the schema marker made now must arrive through it; events arrive in oplog order.
     */
    static boolean barrier(MongoClient client, MongoLocation location) {
        Watcher watcher;
        synchronized (WATCHERS) {
            Map<String, Watcher> byDatabase = WATCHERS.get(client);
            watcher = byDatabase == null ? null : byDatabase.get(location.databaseName);
        }
        return watcher == null || watcher.barrier(location);
    }

    /**
     * Makes the watcher of the location's database lose its history (tests): it reopens without its resume token and
     * reports a gap, as after errors 280 and 286, which a test cannot provoke on demand.
     */
    static void simulateHistoryLost(MongoClient client, MongoLocation location) {
        synchronized (WATCHERS) {
            Map<String, Watcher> byDatabase = WATCHERS.get(client);
            Watcher watcher = byDatabase == null ? null : byDatabase.get(location.databaseName);
            if (watcher != null) {
                watcher.historyLost = true;
            }
        }
    }

    /**
     * Makes the watcher of the location's database lose its stream (tests): it reports the loss, as after a network
     * failure, and reopens from its resume token when a cache resumes it.
     */
    static void simulateInterruption(MongoClient client, MongoLocation location) {
        synchronized (WATCHERS) {
            Map<String, Watcher> byDatabase = WATCHERS.get(client);
            Watcher watcher = byDatabase == null ? null : byDatabase.get(location.databaseName);
            if (watcher != null) {
                watcher.interrupted = true;
            }
        }
    }

    /**
     * Holds the watching threads of the client back from reading their cursors, or lets them go on (tests): events
     * pile up unread while joins and leaves still reopen the streams, which then have to resume from the token behind
     * them.
     */
    static void simulateLag(MongoClient client, boolean lagging) {
        synchronized (WATCHERS) {
            WATCHERS.getOrDefault(client, Map.of()).values().forEach(watcher -> watcher.lagging = lagging);
        }
    }

    /**
     * The collections the change streams of the client filter on now (tests).
     */
    static Set<String> coveredCollections(MongoClient client) {
        List<Watcher> watchers;
        synchronized (WATCHERS) {
            watchers = List.copyOf(WATCHERS.getOrDefault(client, Map.of()).values());
        }
        Set<String> covered = new HashSet<>();
        watchers.forEach(watcher -> covered.addAll(watcher.covered()));
        return covered;
    }

    /**
     * The time of the majority snapshot now: a majority read in a causally consistent session reports it as its
     * operation time. Every write outside the snapshot has a later time, so a stream started at it delivers it,
     * and every write inside is visible to the majority reads that follow. Not the latest applied time (a fresh
     * stream's default start, and {@code hello}'s operation time): a write applied but not yet majority-committed
     * would then be neither in the stream nor in the reads (measured on MongoDB 4.4 by the property test).
     */
    private static @Nullable BsonTimestamp majorityTime(MongoClient client, MongoDatabase database,
                                                        String collection) {
        try (ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true)
                .build())) {
            database.getCollection(collection, BsonDocument.class).find(session).limit(1).first();
            return session.getOperationTime();
        } catch (MongoException e) {
            return null;
        }
    }

    // the fields an event carries: the whole document (insert, replace) or the updated ones
    private static @Nullable BsonDocument changedFields(ChangeStreamDocument<BsonDocument> event) {
        if (event.getFullDocument() != null) {
            return event.getFullDocument();
        }
        return event.getUpdateDescription() != null ? event.getUpdateDescription().getUpdatedFields() : null;
    }

    // the message of an event, or null if it carries none for a cache
    static @Nullable Message message(ChangeStreamDocument<BsonDocument> event) {
        BsonDocument key = event.getDocumentKey();
        BsonValue id = key == null ? null : key.get("_id");
        if (id == null || !id.isDocument()) {
            return null;
        }
        BsonDocument fields = changedFields(event);
        if (fields == null) {
            return null;
        }
        if (id.asDocument().containsKey("c")) {
            BsonValue time = fields.get("t");
            return time != null && time.isNumber() && time.asNumber().longValue() > 0
                    ? Message.invalidatedAll(time.asNumber().longValue()) : null;
        }
        BsonValue hash = id.asDocument().get("h");
        BsonValue version = fields.get("version");
        if (hash == null || !hash.isString() || version == null || !version.isNumber()) {
            return null;
        }
        BsonValue state = fields.get("state");
        // an update leaving the state as it was names no state: with a new value it is a value row; without, the row
        // may be anything (a tombstone given a new version by invalidateAll, say), so it is announced as invalidated,
        // which is never wrong (at worst, a value written again unchanged is read again on demand)
        if (state == null || !state.isString()) {
            BsonValue written = fields.get(VALUE_WRITTEN);
            Kind kind = written != null && written.isBoolean() && written.asBoolean().getValue()
                    ? Kind.WRITTEN : Kind.INVALIDATED;
            return new Message(kind, hash.asString().getValue(), version.asNumber().longValue());
        }
        Kind kind = switch (state.asString().getValue()) {
            case "TOMBSTONE" -> Kind.INVALIDATED;
            case "EXPIRED" -> Kind.EXPIRED;
            case "VALUE" -> Kind.WRITTEN;
            default -> null; // a document of no cache
        };
        return kind == null ? null : new Message(kind, hash.asString().getValue(), version.asNumber().longValue());
    }

    private static final class Registration implements Subscription {

        private final Watcher watcher;
        private final String scope;
        private final Listener listener;
        // the cluster time when it subscribed: a shared stream may still carry events of earlier writes
        private final @Nullable BsonTimestamp since;
        // the store's, advanced by every event before its message is handed on
        private final MongoClusterTime clusterTime;
        // guarded by the watcher: the stream it was last delivered on, and whether it was told that delivery stopped
        private long generation;
        private boolean lost;
        private volatile boolean closed;

        Registration(Watcher watcher, String scope, Listener listener, @Nullable BsonTimestamp since,
                     MongoClusterTime clusterTime) {
            this.watcher = watcher;
            this.scope = scope;
            this.listener = listener;
            this.since = since;
            this.clusterTime = clusterTime;
        }

        @Override
        public void resume() {
            if (!closed) {
                watcher.resume(this);
            }
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                watcher.unregister(this);
            }
        }
    }

    // one change stream; the cursor belongs to the watching thread, joins hand it the collections to cover
    private static final class Watcher {

        private final MongoClient client;
        private final MongoDatabase database;
        private final String databaseName;
        private final Map<String, List<Registration>> scopes = new ConcurrentHashMap<>();
        private final Map<String, CompletableFuture<Void>> barriers = new ConcurrentHashMap<>();
        private final Set<String> wanted = new LinkedHashSet<>();
        private Set<String> covered = Set.of();
        private @Nullable CompletableFuture<Void> widening;
        private @Nullable MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;
        private @Nullable BsonDocument resumeToken;
        private @Nullable Thread thread;
        private volatile boolean closed;
        private volatile boolean historyLost;
        private volatile boolean interrupted;
        private volatile boolean lagging;
        // counts the streams opened, so that a loss is re-established once, whoever asks first
        private long generation;

        Watcher(MongoClient client, MongoLocation location) {
            this.client = client;
            this.databaseName = location.databaseName;
            this.database = MongoSchema.database(client, location).withTimeout(OPERATION_TIMEOUT_SECONDS,
                    TimeUnit.SECONDS);
        }


        private static String scope(String collection, String discriminator) {
            return collection + "\0" + discriminator;
        }

        Registration register(MongoLocation location, Listener listener, @Nullable BsonTimestamp since,
                              MongoClusterTime clusterTime) {
            Registration registration = new Registration(this,
                    scope(location.collectionName, location.discriminator), listener, since, clusterTime);
            scopes.computeIfAbsent(registration.scope, ignored -> new CopyOnWriteArrayList<>()).add(registration);
            registration.generation = generation;
            return registration;
        }

        // returns once the stream covers the collection: opened here for the first one, reopened by the watching
        // thread from its resume token for a further one
        void cover(String collection) {
            CompletableFuture<Void> covering;
            synchronized (this) {
                wanted.add(collection);
                if (thread == null) {
                    try {
                        open(new LinkedHashSet<>(wanted), null);
                    } catch (MongoException e) {
                        throw new StoreException("Opening a change stream on " + databaseName + " failed: "
                                + e.getMessage(), e, false);
                    }
                    thread = new Thread(this::run, "distributed-caffeine-mongodb-" + databaseName);
                    thread.setDaemon(true);
                    thread.start();
                    return;
                }
                if (cursor == null) {
                    // lost, and not resumed yet: the joining cache would wait for a resume it does not know of
                    reopen();
                }
                if (covered.contains(collection)) {
                    return;
                }
                if (widening == null) {
                    widening = new CompletableFuture<>();
                }
                covering = widening;
            }
            try {
                covering.get(JOIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StoreException("Interrupted while the change stream on " + databaseName
                        + " took collection " + collection, e, false);
            } catch (ExecutionException | TimeoutException e) {
                throw new StoreException("The change stream on " + databaseName + " did not take collection "
                        + collection + " in time", e, false);
            }
        }

        private void open(Set<String> collections, @Nullable BsonDocument token) {
            org.bson.BsonArray names = new org.bson.BsonArray();
            collections.forEach(name -> names.add(new org.bson.BsonString(name)));
            // the writes of the subscribed collections, and what removes their documents all at once: dropping or
            // renaming one, dropping the database (which also ends the stream with an invalidate event)
            BsonDocument match = new BsonDocument("$or", new org.bson.BsonArray(List.of(
                    BsonDocument.parse("{operationType: {$in: ['insert', 'update', 'replace', 'drop', 'rename']}}")
                            .append("ns.coll", new BsonDocument("$in", names)),
                    BsonDocument.parse("{operationType: {$in: ['dropDatabase', 'invalidate']}}"))));
            // whether an update wrote a value, kept as a flag before the value is projected out
            BsonDocument valueWritten = BsonDocument.parse("{$addFields: {updateDescription: {$cond: {"
                    + "if: {$eq: ['$operationType', 'update']}, else: '$$REMOVE', then: {$mergeObjects: ["
                    + "'$updateDescription', {updatedFields: {$mergeObjects: ['$updateDescription.updatedFields', {"
                    + VALUE_WRITTEN + ": {$ne: [{$type: '$updateDescription.updatedFields.value'}, 'missing']}}]}}]}}}}}");
            List<BsonDocument> stages = List.of(new BsonDocument("$match", match), valueWritten,
                    BsonDocument.parse("{$project: {'fullDocument.value': 0, 'fullDocument.key': 0, "
                            + "'updateDescription.updatedFields.value': 0, 'updateDescription.updatedFields.key': 0}}"));
            ChangeStreamIterable<BsonDocument> stream = database.watch(stages, BsonDocument.class)
                    .maxAwaitTime(MAX_AWAIT_MILLIS, TimeUnit.MILLISECONDS);
            if (token != null) {
                stream = stream.resumeAfter(token);
            } else {
                BsonTimestamp start = majorityTime(client, database, collections.iterator().next());
                if (start != null) {
                    stream = stream.startAtOperationTime(start);
                }
            }
            MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> opened = stream.cursor();
            if (closed) {
                closeQuietly(opened); // the last cache left meanwhile
                return;
            }
            closeCursor();
            cursor = opened;
            generation++;
            covered = Set.copyOf(collections);
            BsonDocument position = opened.getResumeToken();
            if (position != null) {
                resumeToken = position;
            }
        }

        @SuppressWarnings({"java:S3776", "java:S135", "java:S1181"}) // each way out of a read is a case of its own
        private void run() {
            try {
                watch();
            } catch (RuntimeException | Error e) {
                // never expected: the caches are told that delivery stopped rather than left trusting a dead stream
                LOGGER.log(Level.ERROR, "The change stream on " + databaseName + " stopped", e);
                lose(null, e);
                throw e;
            }
        }

        @SuppressWarnings({"java:S3776", "java:S135"}) // the watching loop: each way out of a read is a case of its own
        private void watch() {
            while (!closed) {
                try {
                    reopenIfAsked();
                    if (historyLost) {
                        historyLost = false;
                        reopenFresh(new IllegalStateException("simulated: history lost"));
                        continue;
                    }
                    if (interrupted) {
                        interrupted = false;
                        lose(currentCursor(), new IllegalStateException("simulated: connection lost"));
                        continue;
                    }
                    @SuppressWarnings("resource") // the stream's cursor, closed by whoever replaces or loses it
                    MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> current = currentCursor();
                    if (lagging || current == null) {
                        pause(); // lagging, or lost until a cache resumes the stream
                        continue;
                    }
                    ChangeStreamDocument<BsonDocument> event = current.tryNext();
                    BsonDocument position = current.getResumeToken();
                    if (position != null) {
                        resumeToken = position;
                    }
                    OperationType type = event == null ? null : event.getOperationType();
                    if (event != null && type == null) {
                        dispatchOrGap(event);
                    } else if (event != null) {
                        switch (type) {
                            // every document of the collection is gone, which no message announces: a gap
                            case DROP, RENAME -> collectionGone(event);
                            case DROP_DATABASE -> {
                                // the invalidate event that ends the stream follows
                            }
                            // the stream ended and cannot be resumed; what was dropped is gone: a gap
                            case INVALIDATE -> reopenFresh(new IllegalStateException("the database was dropped"));
                            default -> dispatchOrGap(event);
                        }
                    }
                } catch (RuntimeException e) {
                    if (closed) {
                        return;
                    }
                    failed(e);
                }
            }
        }

        private synchronized @Nullable MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> currentCursor() {
            return cursor;
        }

        private void collectionGone(ChangeStreamDocument<BsonDocument> event) {
            String collection = event.getNamespace() == null ? null : event.getNamespace().getCollectionName();
            LOGGER.log(Level.WARNING, "Collection {0} in {1} was dropped or renamed; gap", collection, databaseName);
            for (Map.Entry<String, List<Registration>> scope : scopes.entrySet()) {
                if (collection == null || scope.getKey().startsWith(collection + "\0")) {
                    for (Registration registration : scope.getValue()) {
                        try {
                            registration.listener.onGap();
                        } catch (RuntimeException e) {
                            LOGGER.log(Level.WARNING, "A listener failed", e);
                        }
                    }
                }
            }
        }

        // an event that cannot be dispatched is lost to the caches, which is a gap
        private void dispatchOrGap(ChangeStreamDocument<BsonDocument> event) {
            try {
                dispatch(event);
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "A change event on " + databaseName + " could not be dispatched; gap", e);
                forEachRegistration(registration -> registration.listener.onGap());
            }
        }

        private synchronized Set<String> covered() {
            return covered;
        }

        // widening for a join, narrowing after the last cache of a collection left: from the resume token
        private synchronized void reopenIfAsked() {
            if (!covered.equals(wanted)) {
                open(new LinkedHashSet<>(wanted), resumeToken);
            }
            joined(null);
        }

        // ends the wait of the caches joining: covered now, or (with a failure) not
        private synchronized void joined(@Nullable Exception failure) {
            if (widening != null) {
                if (failure == null) {
                    widening.complete(null);
                } else {
                    widening.completeExceptionally(failure);
                }
                widening = null;
            }
        }

        @SuppressWarnings("java:S3776") // one event, its kinds and the barriers among them
        private void dispatch(ChangeStreamDocument<BsonDocument> event) {
            if (event.getNamespace() == null) {
                return;
            }
            BsonDocument key = event.getDocumentKey();
            if (key != null && key.get("_id") != null && key.get("_id").isString()) {
                // the schema marker: barriers (tests)
                BsonDocument fields = event.getUpdateDescription() == null ? null
                        : event.getUpdateDescription().getUpdatedFields();
                BsonValue barrier = fields == null ? null : fields.get("barrier");
                if (barrier != null && barrier.isString()) {
                    CompletableFuture<Void> arrived = barriers.remove(barrier.asString().getValue());
                    if (arrived != null) {
                        arrived.complete(null);
                    }
                }
                return;
            }
            BsonValue id = key == null ? null : key.get("_id");
            BsonValue discriminator = id == null || !id.isDocument() ? null : id.asDocument().get("d");
            if (discriminator == null || !discriminator.isString()) {
                return; // a document of no cache
            }
            List<Registration> registrations = scopes.get(scope(event.getNamespace().getCollectionName(),
                    discriminator.asString().getValue()));
            Message message = registrations == null ? null : message(event);
            if (message == null) {
                return;
            }
            BsonTimestamp time = event.getClusterTime();
            for (Registration registration : registrations) {
                if (time != null && registration.since != null && time.compareTo(registration.since) <= 0) {
                    continue; // committed before it subscribed
                }
                // first: the reads the message leads to must see at least the write it announces
                registration.clusterTime.advance(time);
                try {
                    registration.listener.onMessages(List.of(message));
                } catch (RuntimeException e) {
                    LOGGER.log(Level.WARNING, "A listener failed on a change event", e);
                }
            }
        }

        // a read failed: a history the stream cannot resume from is a gap (reopened without the token at once);
        // anything else loses the stream until a cache resumes it. A failure of a stream replaced meanwhile (reopened
        // by a resume) is not this one's
        private void failed(RuntimeException failure) {
            MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> failedCursor = currentCursor();
            if (isHistoryLost(failure)) {
                reopenFresh(failure);
            } else {
                lose(failedCursor, failure);
            }
        }

        // the stream lost: every cache is told, and resumes it when it can
        @SuppressWarnings("ReferenceEquality") // only the cursor that failed is the current one
        private void lose(@Nullable MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> failedCursor,
                          Throwable failure) {
            List<Registration> told = new ArrayList<>();
            synchronized (this) {
                if (closed || (failedCursor != null && cursor != failedCursor)) {
                    return;
                }
                closeCursor();
                forEachRegistration(registration -> {
                    registration.lost = true;
                    told.add(registration);
                });
            }
            LOGGER.log(Level.WARNING, "The change stream on {0} failed ({1})", databaseName, failure.getMessage());
            told.forEach(registration -> notify(() -> registration.listener.onLost(failure)));
        }

        // the history lost: reopened at once without the token, a gap; if that fails, lost (a resume is a gap anyway)
        private void reopenFresh(Exception failure) {
            LOGGER.log(Level.WARNING, "The change stream on {0} lost its history ({1}); reopening, gap",
                    databaseName, failure.getMessage());
            List<Registration> told = new ArrayList<>();
            try {
                synchronized (this) {
                    resumeToken = null;
                    open(new LinkedHashSet<>(wanted), null);
                    joined(null);
                    forEachRegistration(registration -> {
                        if (!registration.lost) {
                            registration.generation = generation;
                            told.add(registration);
                        }
                    });
                }
            } catch (RuntimeException e) {
                joined(e);
                lose(null, e);
                return;
            }
            told.forEach(registration -> notify(registration.listener::onGap));
        }

        // reopens the stream for a registration (VISION.md, "Supervision"): if another one did since this one lost
        // it, nothing is left to do; otherwise from the token (so that the others on it miss nothing), also if the
        // stream seems alive (suspected silent). Only a registration not told of a loss can suspect the stream: for
        // one told, a stream there now is a newer one. The core counts every resume as a gap
        void resume(Registration registration) {
            synchronized (this) {
                if (closed) {
                    return;
                }
                if (cursor == null || (!registration.lost && generation == registration.generation)) {
                    if (cursor != null) {
                        LOGGER.log(Level.WARNING, "The change stream on {0} is suspected silent; reopening",
                                databaseName);
                    }
                    reopen();
                }
                registration.generation = generation;
                registration.lost = false;
            }
        }

        // from the token, or without it (a gap) if the history is lost; the caller holds the lock
        private void reopen() {
            try {
                try {
                    open(new LinkedHashSet<>(wanted), resumeToken);
                } catch (RuntimeException e) {
                    if (!isHistoryLost(e)) {
                        throw e;
                    }
                    resumeToken = null;
                    open(new LinkedHashSet<>(wanted), null);
                }
                joined(null);
                LOGGER.log(Level.INFO, "The change stream on {0} reopened", databaseName);
            } catch (MongoException e) {
                throw new StoreException("Reopening the change stream on " + databaseName + " failed: "
                        + e.getMessage(), e, false, true);
            }
        }

        private static void notify(Runnable call) {
            try {
                call.run();
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "A listener failed", e);
            }
        }

        private static boolean isHistoryLost(Exception failure) {
            // a getMore reports it as a MongoQueryException, opening or resuming as a MongoCommandException: both are
            // server exceptions (CappedPositionLost on an open cursor was taken for an interruption, and the stream
            // resumed past the lost events without a gap, found by the oplog rollover test)
            return failure instanceof MongoServerException server && HISTORY_LOST.contains(server.getCode());
        }

        boolean barrier(MongoLocation location) {
            String token = UUID.randomUUID().toString();
            CompletableFuture<Void> arrived = new CompletableFuture<>();
            barriers.put(token, arrived);
            try {
                database.getCollection(location.collectionName).updateOne(
                        Filters.eq("_id", MongoLocation.MARKER_ID), Updates.set("barrier", token));
                arrived.get(5, TimeUnit.SECONDS);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException | TimeoutException | RuntimeException e) {
                return false;
            } finally {
                barriers.remove(token);
            }
        }

        private void forEachRegistration(Consumer<Registration> action) {
            for (List<Registration> registrations : scopes.values()) {
                for (Registration registration : registrations) {
                    try {
                        action.accept(registration);
                    } catch (RuntimeException e) {
                        LOGGER.log(Level.WARNING, "A listener failed", e);
                    }
                }
            }
        }

        void unregister(Registration registration) {
            MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> last = null;
            synchronized (WATCHERS) {
                synchronized (this) {
                    List<Registration> registrations = scopes.get(registration.scope);
                    if (registrations != null) {
                        registrations.remove(registration);
                        if (registrations.isEmpty()) {
                            scopes.remove(registration.scope);
                        }
                    }
                    String collection = registration.scope.substring(0, registration.scope.indexOf('\0'));
                    if (scopes.keySet().stream().noneMatch(scope -> scope.startsWith(collection + "\0"))
                            && wanted.size() > 1) {
                        wanted.remove(collection); // the watching thread narrows the stream
                    }
                    if (scopes.isEmpty()) {
                        closed = true;
                        last = cursor;
                        cursor = null;
                        Map<String, Watcher> byDatabase = WATCHERS.get(client);
                        if (byDatabase != null) {
                            byDatabase.remove(databaseName, this);
                            if (byDatabase.isEmpty()) {
                                WATCHERS.remove(client);
                            }
                        }
                    }
                }
            }
            closeQuietly(last); // a round trip, so outside the locks; the watching thread ends with it
        }

        private void closeCursor() {
            closeQuietly(cursor);
            cursor = null;
        }

        private static void closeQuietly(@Nullable MongoChangeStreamCursor<?> cursor) {
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (RuntimeException ignored) {
                    // closing a broken cursor
                }
            }
        }

        // a moment's wait of the watching thread, while there is nothing to read
        private static void pause() {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
