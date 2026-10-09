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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static com.mongodb.client.model.changestream.OperationType.UPDATE;
import static io.github.oberhoff.distributedcaffeine.adapter.DiscriminatorAware.DISCRIMINATOR_FIELD;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * One change stream cursor, shared by the cache instances that subscribe to it.
 * <p>
 * The cursor is held by a thread of its own for as long as anybody subscribes, and that thread does nothing but keep
 * the cursor alive and hand what arrives to the subscribers it is meant for. What a subscriber does with it -
 * reading the cache entries out of the documents, applying them to its cache instance - happens on the
 * subscriber's side, so that one cache instance being slow or failing never holds up the others or the cursor.
 * <p>
 * Which documents the cursor delivers is decided by its pipeline, which is fixed once the cursor is opened. A
 * subscriber whose collection the pipeline does not cover yet is therefore served by reopening the cursor with a
 * wider pipeline, resuming where the narrower one left off - which delivers everything written in between, so nobody
 * misses anything for it. A subscriber whose collection is covered already is served right away, without the cursor
 * having to do anything. A subscriber leaving never narrows it: what nobody subscribes to any more finds nobody to be
 * handed to, and is dropped.
 * <p>
 * A shared cursor does not select by discriminator, only by collection: the discriminator is matched on the server
 * only after the full document has been looked up, so selecting by it saves the server nothing, while it would make
 * every subscriber of a new discriminator reopen the cursor. What no subscriber is interested in is dropped here
 * instead. A cursor of one subscriber alone, which nobody can join, does select by discriminator, because it never
 * has to be reopened for it.
 */
final class MongoWatcher {

    private static final Logger LOGGER = System.getLogger(MongoWatcher.class.getName());

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);
    // How long the server waits for something to arrive before answering a poll empty, for a cursor that others may
    // join: a subscriber joining waits for the poll under way to be over, and a poll cannot be cut short from here.
    // A cursor nobody can join keeps the server's default, which polls less often
    private static final Duration SHARED_POLL_TIMEOUT = Duration.ofMillis(100);
    // documents already buffered by the cursor are taken along together, bounded so that a backlog is handed over
    // in portions rather than all at once
    private static final int MAXIMUM_BATCH_SIZE = 100;
    // "ChangeStreamHistoryLost": the resume position has fallen out of the oplog and never becomes valid again
    private static final int CHANGE_STREAM_HISTORY_LOST = 286;
    private static final String DOCUMENT_KEY = "documentKey";
    private static final String CLUSTER_TIME = "clusterTime";
    private static final String OPERATION_TYPE = "operationType";
    private static final String FULL_DOCUMENT = "fullDocument";
    private static final String NAMESPACE = "ns";
    private static final String NAMESPACE_COLLECTION = "ns.coll";

    /**
     * Who a cursor hands what arrives to.
     */
    interface Subscriber {

        String getCollectionName();

        String getDiscriminator();

        String getIdentifier();

        // called on the watching thread, so it hands the documents over rather than acting on them
        void receiveDocuments(List<Document> documents);

        // called on the watching thread as well: the cursor failed and is back, and whatever happened in the
        // meantime may not be delivered
        void receiveRestart();
    }

    private enum State {

        /**
         * Watching has not begun yet. A failure in this state is final, and fails everybody waiting to subscribe.
         */
        STARTING,

        /**
         * Watching has begun. Kept while a failed cursor is being replaced, so that a failure is retried.
         */
        STARTED,

        /**
         * Starting failed. Whoever subscribes is refused, and the registry replaces the watcher.
         */
        FAILED,

        /**
         * Nobody subscribes any more, and the cursor is given up.
         */
        CLOSED
    }

    private record Request(Subscriber subscriber, @Nullable CompletableFuture<@Nullable Void> subscribed) {
    }

    // what a subscriber is told apart by - the collection is part of it because a cursor may watch a whole database
    private record Scope(String collectionName, String discriminator) {
    }

    private final MongoDatabase mongoDatabase;
    // the collection watched, or nothing for a cursor that watches the whole database
    private final @Nullable String collectionName;
    private final boolean shared;
    private final @Nullable Duration pollTimeout;
    private final AtomicReference<State> state = new AtomicReference<>(State.STARTING);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Queue<Request> requests = new ConcurrentLinkedQueue<>();
    // named in what is logged, which happens outside the watching thread
    private final Set<String> identifiers = ConcurrentHashMap.newKeySet();
    // Marks how far the change stream has been consumed, so that watching can be resumed there after a failure - and
    // after reopening with a wider pipeline. A resume token is used rather than an operation time because the
    // server reports one for every batch polled, including empty ones (post-batch resume token), so a position is
    // available while nothing happens at all. An operation time can only be taken from an event that actually
    // arrived, which leaves no resume position until the first one does - and a cursor failing before that resumes
    // at "now", silently losing everything written in the meantime
    private final AtomicReference<@Nullable BsonDocument> resumeToken = new AtomicReference<>();
    // Concurrent, because a subscriber whose collection the open cursor covers already registers itself, from its
    // own thread, while the watching thread hands documents over
    private final Map<Scope, Set<Subscriber>> subscribersByScope = new ConcurrentHashMap<>();
    // who has been told that it is subscribed, and so has to be told when the cursor it relies on failed
    private final Set<Subscriber> confirmed = ConcurrentHashMap.newKeySet();
    private final Map<Subscriber, CompletableFuture<@Nullable Void>> unconfirmed = new ConcurrentHashMap<>();
    // What the pipeline of the open cursor covers, and whether a cursor is open at all - guarded by this, so that a
    // subscriber registering itself and the watching thread replacing the cursor agree on what is covered
    private final Set<String> watchedCollectionNames = new HashSet<>();
    private final Set<String> watchedDiscriminators = new HashSet<>();
    private boolean watching;
    // counted by the registry, under its lock
    int references;

    // Every operation of watching is limited to the given timeout, polls included. A poll returns within the time the
    // server waits for something to arrive, so one that does not return within the timeout is on a connection that
    // died without being reset - a failover moving the server's address, a NAT entry expiring on an idle path - and
    // would otherwise only fail once TCP keepalive gives up on the connection, after minutes, the driver setting no
    // read limit of its own. Failing it is what has the cursor replaced, resuming where it was and reconciling, as
    // after any other failure. Limited here only, so that the client's own operations keep whatever limit its owner
    // gave them
    MongoWatcher(MongoClient mongoClient, String databaseName, @Nullable String collectionName, boolean shared,
                 Duration operationTimeout) {
        this.mongoDatabase = mongoClient.getDatabase(databaseName)
                .withTimeout(operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
        this.collectionName = collectionName;
        this.shared = shared;
        this.pollTimeout = shared ? SHARED_POLL_TIMEOUT : null;
    }

    // Started by whoever subscribed first, after subscribing, so that the first cursor already covers its first
    // subscriber rather than being opened empty and reopened at once. Starting again does nothing.
    // Nothing waits for the cursor to be given up, and how it ended is dealt with where it ends rather than by
    // whoever would otherwise hold on to its future
    @SuppressWarnings("FutureReturnValueIgnored")
    void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                // abort unless watching had already begun: a failure while starting up (for example a read concern
                // that does not support change streams) is final and must fail fast, whereas a failure after that is
                // treated as transient and retried
                .abortOn(throwable -> state.get() != State.STARTED)
                .withMaxAttempts(-1)
                .withDelay(RETRY_INTERVAL)
                .withDelayFnOn(context -> RETRY_INTERVAL.multipliedBy(min(context.getAttemptCount(), 10)),
                        Throwable.class)
                .onRetryScheduled(event -> Optional.ofNullable(event.getLastException())
                        .ifPresent(throwable -> LOGGER.log(Level.WARNING,
                                format("Watching change streams failed for %s. Retrying...", describe()),
                                throwable)))
                .build();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Failsafe.with(retryPolicy)
                .with(executorService)
                .runAsync(this::watch)
                .whenComplete((result, throwable) -> {
                    if (nonNull(throwable)) {
                        fail(throwable);
                    }
                    executorService.shutdown();
                });
    }

    /**
     * Subscribes, and returns what is completed once the cursor covers the subscriber - or completed exceptionally
     * if watching cannot be begun at all.
     */
    CompletableFuture<@Nullable Void> subscribe(Subscriber subscriber) {
        CompletableFuture<@Nullable Void> subscribed = new CompletableFuture<>();
        synchronized (this) {
            // checked under the same lock that failing takes, so that a request cannot slip in after failing has
            // refused everybody waiting and then wait forever itself
            if (state.get() == State.FAILED || state.get() == State.CLOSED) {
                subscribed.completeExceptionally(new IllegalStateException("Watching has been given up"));
                return subscribed;
            }
            identifiers.add(subscriber.getIdentifier());
            Scope scope = new Scope(subscriber.getCollectionName(), subscriber.getDiscriminator());
            // Covered by the open cursor already, so served right away: whatever is handed over from now on reaches
            // it, and whatever was handed over before is what the cache instance reads from the store once
            // activated. Otherwise the watching thread takes it into account between polls, reopening if need be
            if (watching && covers(scope)) {
                subscribersByScope.computeIfAbsent(scope, ignored -> ConcurrentHashMap.newKeySet()).add(subscriber);
                confirmed.add(subscriber);
                subscribed.complete(null);
            } else {
                requests.add(new Request(subscriber, subscribed));
            }
        }
        return subscribed;
    }

    // through the watching thread even for a subscriber that registered itself, so that leaving can never overtake
    // a subscription of the same subscriber that is still waiting to be carried out
    void unsubscribe(Subscriber subscriber) {
        requests.add(new Request(subscriber, null));
    }

    // guarded by this
    private boolean covers(Scope scope) {
        return watchedCollectionNames.contains(scope.collectionName())
                && (shared || watchedDiscriminators.contains(scope.discriminator()));
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

    private void watch() {
        // this attempt may have been scheduled before closing, in which case watching must not be (re)started
        if (isClosed()) {
            return;
        }
        // Watching having begun before this attempt means the cursor it replaces failed, so cache entries may have
        // been missed: an event whose document was swept in the meantime resolves to no full document and is
        // dropped by the server-side match on the discriminator, so it never arrives and no replay can make it good.
        // Whether that actually happened cannot be told from here - the stream that skipped it looks exactly like
        // one with nothing to deliver - which is why the possibility is reported to everybody who relied on it
        boolean recovering = state.get() == State.STARTED;
        // a cursor reopened with a wider pipeline is not a failure, and resumes exactly where its predecessor was
        while (!isClosed()) {
            applyRequests();
            if (!watchUntilWidened(recovering)) {
                return;
            }
            recovering = false;
        }
    }

    // Watches with a pipeline covering everybody subscribed so far, until somebody subscribes whom it does not
    // cover - returning true, to be reopened wider - or until closed, returning false
    @SuppressWarnings("java:S3776")
    private boolean watchUntilWidened(boolean recovering) {
        List<Bson> pipeline;
        synchronized (this) {
            watching = false;
            watchedCollectionNames.clear();
            watchedDiscriminators.clear();
            subscribersByScope.keySet().forEach(scope -> {
                watchedCollectionNames.add(scope.collectionName());
                watchedDiscriminators.add(scope.discriminator());
            });
            pipeline = buildAggregationPipeline();
        }
        ChangeStreamIterable<Document> changeStreamIterable = (isNull(collectionName)
                ? mongoDatabase.watch(pipeline)
                : mongoDatabase.getCollection(collectionName).watch(pipeline))
                .fullDocument(FullDocument.UPDATE_LOOKUP);
        if (nonNull(pollTimeout)) {
            changeStreamIterable = changeStreamIterable.maxAwaitTime(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        changeStreamIterable = Optional.ofNullable(resumeToken.get())
                .map(changeStreamIterable::resumeAfter)
                .orElse(changeStreamIterable);
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStreamIterable.cursor()) {
            if (isClosed()) {
                return false;
            }
            // reported once the new cursor is live, so that changes arriving while the cache instances recover are
            // delivered rather than missed in turn, which is the order activation uses for the same reason
            state.set(State.STARTED);
            if (recovering) {
                confirmed.forEach(Subscriber::receiveRestart);
            }
            synchronized (this) {
                watching = true;
            }
            confirmSubscriptions();
            while (!isClosed()) {
                if (applyRequests()) {
                    return true;
                }
                confirmSubscriptions();
                ChangeStreamDocument<Document> changeStreamDocument = cursor.tryNext();
                if (isNull(changeStreamDocument) && isNull(cursor.getServerCursor())) {
                    // Ended by the server rather than idle, which an empty poll alone cannot tell apart: dropping or
                    // renaming the collection or database watched ends a change stream for good, and every poll after
                    // that comes back empty. Resuming after its end is refused, so watching starts anew from now -
                    // failing here has the cursor replaced and the cache instances reconciled, which covers whatever
                    // happened in between, such as the collection being recreated
                    resumeToken.set(null);
                    throw new IllegalStateException(format("Change stream was ended by the server for %s",
                            describe()));
                } else if (isNull(changeStreamDocument)) {
                    // nothing pending, so the position reported for the batch just polled can be adopted as is: it
                    // marks how far the server has looked without there being an event that still needs to be
                    // handed over. Doing this while idle is what closes the gap, because the first event may be
                    // hours away or never come
                    BsonDocument postBatchResumeToken = cursor.getResumeToken();
                    // the cursor reports none before its first poll, and a position once held must not be given up
                    // again, because that would mean resuming at "now" - the very gap it is kept for
                    if (nonNull(postBatchResumeToken)) {
                        resumeToken.set(postBatchResumeToken);
                    }
                } else if (!isClosed()) {
                    // whatever else the cursor already holds is taken along, so that a burst of changes is handed
                    // over as one batch. available() counts what can be taken without going to the server, so
                    // nothing here waits for an event that has not arrived yet
                    List<ChangeStreamDocument<Document>> changeStreamDocuments = new ArrayList<>();
                    changeStreamDocuments.add(changeStreamDocument);
                    while (changeStreamDocuments.size() < MAXIMUM_BATCH_SIZE && cursor.available() > 0
                            && !isClosed()) {
                        ChangeStreamDocument<Document> bufferedChangeStreamDocument = cursor.tryNext();
                        if (isNull(bufferedChangeStreamDocument)) {
                            break;
                        }
                        changeStreamDocuments.add(bufferedChangeStreamDocument);
                    }
                    dispatch(changeStreamDocuments);
                    // advanced once handed over: what a subscriber fails to apply is its own to recover from, by
                    // reconciling, rather than a reason to deliver everything again to everybody
                    resumeToken.set(changeStreamDocuments.get(changeStreamDocuments.size() - 1).getResumeToken());
                }
            }
            return false;
        } catch (MongoCommandException e) {
            // A resume position that has fallen out of the oplog never becomes valid again, so retrying with it
            // would repeat this failure for as long as the cache instances live. Giving the position up lets the
            // retry open a fresh cursor, which starts at "now": watching recovers, the changes made in between do
            // not, which is why this is logged rather than passed over silently
            if (e.getErrorCode() == CHANGE_STREAM_HISTORY_LOST) {
                resumeToken.set(null);
                LOGGER.log(Level.WARNING, format("Resume position lost for %s. Watching continues without it, so "
                        + "changes made in the meantime are not received", describe()));
            }
            throw e;
        } finally {
            // whoever subscribes from now on waits for the next cursor, which takes them into account
            synchronized (this) {
                watching = false;
            }
        }
    }

    // Takes the requests made since the last time into account, and returns whether somebody subscribed whom the
    // pipeline of the open cursor does not cover
    private synchronized boolean applyRequests() {
        boolean widened = false;
        Request request;
        while ((request = requests.poll()) != null) {
            Subscriber subscriber = request.subscriber();
            Scope scope = new Scope(subscriber.getCollectionName(), subscriber.getDiscriminator());
            CompletableFuture<@Nullable Void> subscribed = request.subscribed();
            if (nonNull(subscribed)) {
                subscribersByScope.computeIfAbsent(scope, ignored -> ConcurrentHashMap.newKeySet()).add(subscriber);
                unconfirmed.put(subscriber, subscribed);
                widened |= !covers(scope);
            } else {
                Set<Subscriber> subscribers = subscribersByScope.get(scope);
                if (nonNull(subscribers) && subscribers.remove(subscriber) && subscribers.isEmpty()) {
                    subscribersByScope.remove(scope);
                }
                confirmed.remove(subscriber);
                Optional.ofNullable(unconfirmed.remove(subscriber)).ifPresent(future -> future.cancel(false));
                identifiers.remove(subscriber.getIdentifier());
            }
        }
        return widened;
    }

    // only once the open cursor covers them, which is what a subscriber waits to be told
    private void confirmSubscriptions() {
        if (unconfirmed.isEmpty()) {
            return;
        }
        unconfirmed.forEach((subscriber, subscribed) -> {
            confirmed.add(subscriber);
            subscribed.complete(null);
        });
        unconfirmed.clear();
    }

    // handed over grouped per subscriber, each in the order the store produced them
    private void dispatch(List<ChangeStreamDocument<Document>> changeStreamDocuments) {
        Map<Scope, List<Document>> documentsByScope = new LinkedHashMap<>();
        for (ChangeStreamDocument<Document> changeStreamDocument : changeStreamDocuments) {
            Document fullDocument = changeStreamDocument.getFullDocument();
            if (isNull(fullDocument) || !(INSERT.equals(changeStreamDocument.getOperationType())
                    || UPDATE.equals(changeStreamDocument.getOperationType()))) {
                continue;
            }
            String documentCollectionName = Optional.ofNullable(changeStreamDocument.getNamespace())
                    .map(MongoNamespace::getCollectionName)
                    .orElse(collectionName);
            String discriminator = fullDocument.getString(DISCRIMINATOR_FIELD);
            if (nonNull(documentCollectionName) && nonNull(discriminator)) {
                Scope scope = new Scope(documentCollectionName, discriminator);
                if (subscribersByScope.containsKey(scope)) {
                    documentsByScope.computeIfAbsent(scope, ignored -> new ArrayList<>()).add(fullDocument);
                }
            }
        }
        documentsByScope.forEach((scope, documents) -> subscribersByScope.getOrDefault(scope, Set.of())
                .forEach(subscriber -> subscriber.receiveDocuments(documents)));
    }

    private List<Bson> buildAggregationPipeline() {
        List<String> projectionFields = new ArrayList<>();
        projectionFields.add(DOCUMENT_KEY);
        projectionFields.add(CLUSTER_TIME);
        projectionFields.add(OPERATION_TYPE);
        // what a document is handed over by: its collection, and its discriminator - which is not a field of a cache
        // entry, and so has to be kept by name
        projectionFields.add(NAMESPACE);
        projectionFields.add(fullDocument(DISCRIMINATOR_FIELD));
        projectionFields.addAll(Stream.of(Field.values())
                .map(Object::toString)
                .map(MongoWatcher::fullDocument)
                .toList());
        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.in(OPERATION_TYPE, INSERT.getValue(), UPDATE.getValue()));
        // A cursor on the whole database has to name the collections on the server: that match is evaluated before
        // the full document is looked up, so without it the server would look up every document updated anywhere in
        // the database, only for it to be dropped
        if (isNull(collectionName)) {
            filters.add(Filters.in(NAMESPACE_COLLECTION, watchedCollectionNames));
        }
        // events of other caches sharing these collections are not ours to hand over - selected on the server only
        // for a cursor nobody can join, as described above
        if (!shared) {
            filters.add(Filters.in(fullDocument(DISCRIMINATOR_FIELD), watchedDiscriminators));
        }
        return List.of(
                Aggregates.match(Filters.and(filters)),
                Aggregates.project(Projections.fields(Projections.include(projectionFields))));
    }

    private static String fullDocument(String field) {
        return format("%s.%s", FULL_DOCUMENT, field);
    }
}
