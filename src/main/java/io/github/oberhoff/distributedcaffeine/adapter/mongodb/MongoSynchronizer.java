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
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractSynchronizer;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static com.mongodb.client.model.changestream.OperationType.UPDATE;
import static io.github.oberhoff.distributedcaffeine.adapter.Repository.DISCRIMINATOR_FIELD;
import static io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoRepository.toCacheEntryOrNull;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

final class MongoSynchronizer<K, V> extends AbstractSynchronizer<K, V> {

    private static final Logger LOGGER = System.getLogger(MongoSynchronizer.class.getName());

    private static final Duration WATCHER_INTERVAL = Duration.ofSeconds(1);
    private static final String DOCUMENT_KEY = "documentKey";
    private static final String CLUSTER_TIME = "clusterTime";
    private static final String OPERATION_TYPE = "operationType";
    private static final String FULL_DOCUMENT = "fullDocument";

    private final MongoCollection<Document> mongoCollection;
    // unlike its sibling components, which get by with a single activation flag, this one needs three states: its
    // restarts are scheduled by a retry policy it does not control, so "stopped" has to be distinguishable from
    // "not started yet" - an attempt queued behind a retry delay must not start watching once deactivated
    private final AtomicReference<WatchState> watchState;
    private final AtomicReference<@Nullable Throwable> failFastThrowable;
    // marks how far the change stream has been consumed, so that watching can be resumed there after a failure.
    // A resume token is used rather than an operation time because the server reports one for every batch polled,
    // including empty ones (post-batch resume token), so a position is available while nothing happens at all. An
    // operation time can only be taken from an event that actually arrived, which leaves no resume position until
    // the first one does - and a cursor failing before that resumes at "now", silently losing everything written
    // in the meantime. Being exact, it also avoids re-applying events on every resume, unlike an operation time,
    // which is second-granular and inclusive
    private final AtomicReference<@Nullable BsonDocument> resumeToken;

    private @Nullable CompletableFuture<Void> watcherCompletableFuture;

    MongoSynchronizer(MongoClient mongoClient, String databaseName, String collectionName) {
        this.mongoCollection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
        this.watchState = new AtomicReference<>(WatchState.STOPPED);
        this.failFastThrowable = new AtomicReference<>(null);
        this.resumeToken = new AtomicReference<>(null);
        // TODO connection sharing
    }

    @Override
    public void activate() {
        // wait for completion if required
        Optional.ofNullable(watcherCompletableFuture)
                .filter(future -> !future.isDone())
                .ifPresent(CompletableFuture::join);

        // discard any throwable recorded by a previous activation attempt (after joining that attempt, so it cannot
        // record another one afterwards). deactivate() is the only other place clearing it, but it is reached via
        // InternalInstanceRegistry.deactivate(), which is skipped while isActivated() is false - exactly the state
        // left behind by a failed activation. A stale throwable would otherwise make abortIf() below abort every
        // later activation on the first poll, so a single failed start would permanently break synchronization.
        failFastThrowable.set(null);

        watchState.set(WatchState.STARTING);

        scheduleChangeStreamWatcher();

        // wait until watching is activated or throw exception after timeout, but fail fast if watching is not possible
        Duration timeoutDuration = Optional.ofNullable(mongoCollection.getTimeout(TimeUnit.SECONDS))
                .filter(seconds -> seconds > 0)
                .map(Duration::ofSeconds)
                .orElseGet(() -> Duration.ofSeconds(30)); // default MongoDB timeout
        Fallback<Boolean> fallback = Fallback.<Boolean>builderOfException(event ->
                        new MongoClientException(format("Watching change streams failed for cache at '%s'",
                                identifier), Optional.ofNullable(failFastThrowable.get())
                                .orElseGet(() -> new MongoTimeoutException(format("Timeout after %s seconds",
                                        timeoutDuration.toSeconds())))))
                .handleResult(false)
                .build();
        RetryPolicy<Boolean> retryPolicy = RetryPolicy.<Boolean>builder()
                .handleResult(false)
                .abortIf(result -> nonNull(failFastThrowable.get()))
                .withMaxAttempts(-1)
                .withMaxDuration(timeoutDuration)
                .build();
        Failsafe.with(fallback, retryPolicy)
                .get(this::isActivated);
    }

    @Override
    public void deactivate() {
        // an attempt that failed earlier may already be scheduled (up to ten intervals ahead) and is not aborted by
        // this, so it still runs afterwards - it observes STOPPED and returns without watching. Otherwise it would
        // start watching and report itself activated again, leaving the adapter activated while cache manager and
        // maintenance worker are deactivated, in a loop that never ends - so the next activate() would join a
        // future that can never complete
        watchState.set(WatchState.STOPPED);
        failFastThrowable.set(null);
        resumeToken.set(null);
    }

    @Override
    public boolean isActivated() {
        return watchState.get() == WatchState.STARTED;
    }

    private boolean isStopped() {
        return watchState.get() == WatchState.STOPPED;
    }

    private void scheduleChangeStreamWatcher() {
        RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                .abortOn(throwable -> {
                    failFastThrowable.set(throwable);
                    // abort unless watching had already started: a failure while starting up (for example a read
                    // concern that does not support change streams) is final and must fail fast, whereas a failure
                    // after that is treated as transient and retried
                    return watchState.get() != WatchState.STARTED;
                })
                .withMaxAttempts(-1)
                .withDelay(WATCHER_INTERVAL)
                .withDelayFnOn(context -> WATCHER_INTERVAL.multipliedBy(min(context.getAttemptCount(), 10)),
                        Throwable.class)
                .onRetryScheduled(event -> Optional.ofNullable(event.getLastException())
                        .ifPresent(throwable -> LOGGER.log(Level.WARNING,
                                format("Watching change streams failed for cache at '%s'. Retrying...",
                                        identifier), throwable)))
                .build();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        watcherCompletableFuture = Failsafe.with(retryPolicy)
                .with(executorService)
                .runAsync(this::processChangeStreams)
                .whenComplete((result, throwable) -> executorService.shutdown());
    }

    private void processChangeStreams() {
        // this attempt may have been scheduled before deactivation, in which case watching must not be (re)started
        // the retry policy only evaluates its abort condition at failure time, not when a delayed attempt resumes
        if (isStopped()) {
            return;
        }
        // get change stream iterable, resuming where a previous attempt left off if it got that far. The pipeline is
        // built here rather than once statically because it depends on the discriminator, which is not known before
        // the adapter has wired this synchronizer up - which is cheap enough, as this runs once per opened cursor
        ChangeStreamIterable<Document> changeStreamIterable = mongoCollection.watch(buildAggregationPipeline())
                .fullDocument(FullDocument.UPDATE_LOOKUP);
        changeStreamIterable = Optional.ofNullable(resumeToken.get())
                .map(changeStreamIterable::resumeAfter)
                .orElse(changeStreamIterable);
        // get the cursor to iterate over inbound change stream documents
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStreamIterable.cursor()) {
            // do not report activation if deactivation happened while the cursor was being opened
            if (isStopped()) {
                return;
            }
            watchState.set(WatchState.STARTED);
            while (!isStopped()) {
                ChangeStreamDocument<Document> changeStreamDocument = cursor.tryNext();
                // additional check necessary because tryNext() seems to be paced (blocked for a while)
                if (isNull(changeStreamDocument)) {
                    // nothing pending, so the position reported for the batch just polled can be adopted as is: it
                    // marks how far the server has looked without there being an event that still needs to be
                    // applied. Doing this while idle is what closes the gap, because the first event may be hours
                    // away or never come
                    BsonDocument postBatchResumeToken = cursor.getResumeToken();
                    // the cursor reports none before its first poll, and a position once held must not be given up
                    // again, because that would mean resuming at "now" - the very gap it is kept for
                    if (nonNull(postBatchResumeToken)) {
                        resumeToken.set(postBatchResumeToken);
                    }
                } else if (!isStopped()) {
                    processChangeStreamDocument(changeStreamDocument);
                    // advance only now that the event has been applied. Resuming happens strictly *after* the
                    // recorded position, so advancing beforehand would drop an event whose processing failed.
                    // An event always carries its own position, so no null check is needed here
                    resumeToken.set(changeStreamDocument.getResumeToken());
                }
            }
        }
    }

    private void processChangeStreamDocument(ChangeStreamDocument<Document> changeStreamDocument) {
        OperationType operationType = changeStreamDocument.getOperationType();
        if (nonNull(changeStreamDocument.getFullDocument()) && nonNull(operationType)
                && (operationType.equals(INSERT) || operationType.equals(UPDATE))) {
            // skipped (logged and left out) rather than thrown on, as the contract of a synchronizer asks for: the
            // resume token is advanced only once an event has been applied, so failing here would make the watcher
            // retry that very event indefinitely
            CacheEntry<K, V> cacheEntry = toCacheEntryOrNull(keySerializer, valueSerializer,
                    changeStreamDocument.getFullDocument(), LOGGER, identifier);
            Optional.ofNullable(cacheEntry)
                    .map(Set::of)
                    .ifPresent(cacheEntries -> receiver.receiveCacheEntries(cacheEntries));
        }
    }

    private List<Bson> buildAggregationPipeline() {
        List<String> projectionFields = new ArrayList<>();
        projectionFields.add(DOCUMENT_KEY);
        projectionFields.add(CLUSTER_TIME);
        projectionFields.add(OPERATION_TYPE);
        projectionFields.addAll(Stream.of(Field.values())
                .map(Object::toString)
                .map(MongoSynchronizer::fullDocument)
                .toList());
        return List.of(
                Aggregates.match(
                        Filters.and(
                                Filters.in(OPERATION_TYPE, INSERT.getValue(), UPDATE.getValue()),
                                // events of other caches sharing this collection are not ours to apply
                                Filters.eq(fullDocument(DISCRIMINATOR_FIELD), discriminator))),
                Aggregates.project(
                        Projections.fields(
                                Projections.include(projectionFields))));
    }

    private static String fullDocument(String field) {
        return format("%s.%s", FULL_DOCUMENT, field);
    }

    private enum WatchState {

        /**
         * Not watching and not supposed to: either never activated or deactivated since. A scheduled retry attempt
         * observing this state returns without watching.
         */
        STOPPED,

        /**
         * Activation is under way, but watching has not begun yet. A failure in this state is final (fail fast).
         */
        STARTING,

        /**
         * Watching has begun. This state is kept while a transient failure is being retried, so that activation is
         * not reported as lost during a short interruption, and so that such a failure is retried instead of aborted.
         */
        STARTED
    }
}
