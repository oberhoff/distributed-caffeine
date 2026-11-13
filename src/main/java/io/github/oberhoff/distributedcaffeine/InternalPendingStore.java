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
package io.github.oberhoff.distributedcaffeine;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The store of a cache built while its store could not be reached (VISION.md, "Starting without the store"): it stands
 * in for the store until the core connects it, and passes every call through afterwards. Until then every call fails at
 * once, as a store that does not answer, with nothing sent (so never with an unknown outcome) and not retryable (a
 * retry would only wait); the core's store guard, open from the start, refuses most calls before they get here. The
 * core takes its subscription for lost from the start and resumes it, and the first resume after the connect
 * subscribes the store's own feed, or the poller of its scan. The kind of change source is decided by the store
 * created (DistributedCaffeine#distributing), so the stand-in is always a feed: the core sees only feeds, a scan being
 * polled into one.
 *
 * @author Andreas Oberhoff
 */
// the store connected is closed by close(), not by each call passed on to it
@SuppressWarnings("resource")
final class InternalPendingStore implements Store, ChangeFeed {

    // the store the adapter created, or the stand-in for a store not reached, with the failure that the connect in
    // the background starts from
    record Created(Store store, @Nullable StoreException notReached) {

        // how to connect the stand-in, or null where the store was created
        InternalCore.@Nullable Connector connector(Supplier<InternalCore.Connected> connect, boolean unbounded) {
            return notReached == null ? null : new InternalCore.Connector(connect, notReached, unbounded);
        }
    }

    // what replaces the store's default until it is connected; only invalidateAll waits for it, and that writes, which
    // is refused until then
    private static final Duration CLOCK_ALLOWANCE = Duration.ofMillis(10);

    // the store connected (made ready for distribution, a scan polled), set once
    @SuppressWarnings("java:S3077") // a store is safe for concurrent use: the volatile reference is all it needs
    private volatile @Nullable Store store;
    @GuardedBy("this")
    private boolean closed;

    // the store the adapter creates, or the stand-in where it is not reached (retryable) and the cache may start
    // without it (VISION.md, "Starting without the store"); any other failure, and any under fail-fast, is thrown
    static Created create(Adapter<?, ?> adapter, StoreSettings settings, boolean failFast,
                          InternalEnvironment environment) {
        try {
            return new Created(InternalReachability.create(adapter, settings, environment), null);
        } catch (StoreException e) {
            if (failFast || !e.isRetryable()) {
                throw e;
            }
            return new Created(new InternalPendingStore(), e);
        }
    }

    // whether the store is connected
    boolean isConnected() {
        return store != null;
    }

    // the store created by the connect, which the stand-in passes every call to from now on; closed instead if the
    // cache was closed in the meantime, its connect running on
    void connect(Store connected) {
        synchronized (this) {
            if (!closed) {
                store = connected;
                return;
            }
        }
        connected.close();
    }

    private Store store() {
        Store connected = store;
        if (connected == null) {
            throw new StoreException("The store has not been reached since the cache started", false);
        }
        return connected;
    }

    @Override
    public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
        return store().read(hashes, maxAge);
    }

    @Override
    public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        return store().readVersions(hashes, maxAge);
    }

    @Override
    public Duration clockAllowance() {
        Store connected = store;
        return connected == null ? CLOCK_ALLOWANCE : connected.clockAllowance();
    }

    @Override
    public WriteResult write(Collection<Write> writes) {
        return store().write(writes);
    }

    @Override
    public int expire(Duration maxAge, long version, int limit) {
        return store().expire(maxAge, version, limit);
    }

    @Override
    public int pruneToSize(long maxSize, Duration minAge, int limit) {
        return store().pruneToSize(maxSize, minAge, limit);
    }

    @Override
    public int pruneTombstones(Duration retention, int limit) {
        return store().pruneTombstones(retention, limit);
    }

    @Override
    public void close() {
        Store connected;
        synchronized (this) {
            closed = true;
            connected = store;
        }
        if (connected != null) {
            connected.close();
        }
    }

    // the feed: lost from the start (InternalCore), subscribed on the first resume after the connect

    @Override
    public ChangeSubscription subscribe(ChangeListener listener) {
        return new Subscription(listener);
    }

    final class Subscription implements ChangeSubscription, InternalPollingStore.PollStatus {

        private final ChangeListener listener;
        // the subscription of the store connected, once subscribed
        @SuppressWarnings("java:S3077") // safe for concurrent use: the volatile reference is all it needs
        private volatile @Nullable ChangeSubscription subscribed;
        @GuardedBy("this")
        private boolean unsubscribed;

        Subscription(ChangeListener listener) {
            this.listener = listener;
        }

        // the first resume after the connect subscribes, every later one resumes; before the connect it fails, so the
        // core tries again
        @Override
        public void resume() {
            ChangeSubscription current;
            synchronized (this) {
                if (unsubscribed) {
                    return;
                }
                current = subscribed;
                if (current == null) {
                    // only a feed is connected where there is distribution (DistributedCaffeine#distributing)
                    subscribed = ((ChangeFeed) store()).subscribe(listener);
                    return;
                }
            }
            current.resume();
        }

        @Override
        public void close() {
            ChangeSubscription current;
            synchronized (this) {
                unsubscribed = true;
                current = subscribed;
            }
            if (current != null) {
                current.close();
            }
        }

        // as the poller of a store connected and polled says: changes may be missed while its latest poll failed
        @Override
        public boolean isFailing() {
            return subscribed instanceof InternalPollingStore.PollStatus status && status.isFailing();
        }
    }
}
