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

import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

// A cache's subscription: what the store delivers is queued and processed serially on the store executor (one drain
// at a time, in delivery order), so the store's listening thread never calls into the cache and no thread of its own
// is needed per cache. A failure while processing counts as a gap, since messages may then have been lost. A cache
// that falls too far behind (more messages pending than MAXIMUM_PENDING: its executor busy, its store slow) drops
// what is pending and revalidates instead, as after a gap, rather than letting the queue grow without bound.
final class InternalDelivery implements ChangeListener {

    private static final int MAXIMUM_BATCH = 1_000;
    static final int MAXIMUM_PENDING = 100_000;

    private enum Marker {
        GAP,
        LOST
    }

    private final Executor executor;
    private final Consumer<List<Change>> onMessages;
    private final Runnable onGap;
    private static final System.Logger LOGGER = System.getLogger("io.github.oberhoff.distributedcaffeine");

    private final Runnable onLost;
    private final Queue<Object> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean draining = new AtomicBoolean();
    // the messages queued and not yet taken, and whether that number went beyond the maximum
    private final AtomicInteger pending = new AtomicInteger();
    private final AtomicBoolean overflowed = new AtomicBoolean();
    private volatile boolean closed;

    InternalDelivery(Executor executor, Consumer<List<Change>> onMessages, Runnable onGap, Runnable onLost) {
        this.executor = executor;
        this.onMessages = onMessages;
        this.onGap = onGap;
        this.onLost = onLost;
    }

    @Override
    public void onChanges(List<Change> messages) {
        if (pending.addAndGet(messages.size()) > MAXIMUM_PENDING) {
            pending.addAndGet(-messages.size());
            overflowed.set(true);
        } else {
            queue.add(List.copyOf(messages));
        }
        schedule();
    }

    @Override
    public void onGap() {
        queue.add(Marker.GAP);
        schedule();
    }

    @Override
    public void onLost(Throwable cause) {
        LOGGER.log(System.Logger.Level.WARNING, "The change feed lost delivery; resuming", cause);
        queue.add(Marker.LOST);
        schedule();
    }

    // nothing queued and no drain running (tests)
    boolean isIdle() {
        return queue.isEmpty() && !overflowed.get() && !draining.get();
    }

    void close() {
        closed = true;
        queue.clear();
        pending.set(0);
    }

    private void schedule() {
        if (!closed && draining.compareAndSet(false, true)) {
            try {
                executor.execute(this::drain);
            } catch (RejectedExecutionException e) {
                draining.set(false); // closing: nothing is drained any more
            }
        }
    }

    // fallen too far behind: what is pending is dropped (the markers kept, in order) and replaced by a gap
    private void dropPending() {
        queue.removeIf(item -> {
            if (item instanceof List<?> messages) {
                pending.addAndGet(-messages.size());
                return true;
            }
            return false;
        });
        onGap.run();
    }

    // consecutive batches as one (a store may deliver one message at a time, as PostgreSQL does), so that the reads
    // of a batch (population) stay batched; never across a marker, so the order is kept
    @SuppressWarnings("unchecked")
    private List<Change> merged(List<Change> first) {
        if (!(queue.peek() instanceof List<?>)) {
            return first;
        }
        List<Change> merged = new ArrayList<>(first);
        while (merged.size() < MAXIMUM_BATCH && queue.peek() instanceof List<?>) {
            List<Change> next = (List<Change>) queue.poll();
            pending.addAndGet(-next.size());
            merged.addAll(next);
        }
        return merged;
    }

    @SuppressWarnings("unchecked")
    private void drain() {
        try {
            while (!closed) {
                if (overflowed.getAndSet(false)) {
                    try {
                        dropPending();
                    } catch (RuntimeException e) {
                        onGap.run();
                    }
                    continue;
                }
                Object item = queue.poll();
                if (item == null) {
                    break;
                }
                try {
                    if (item instanceof Marker marker) {
                        switch (marker) {
                            case GAP -> onGap.run();
                            case LOST -> onLost.run();
                        }
                    } else {
                        List<Change> messages = (List<Change>) item;
                        pending.addAndGet(-messages.size());
                        onMessages.accept(merged(messages));
                    }
                } catch (RuntimeException e) {
                    onGap.run();
                }
            }
        } finally {
            draining.set(false);
        }
        if (!closed && (!queue.isEmpty() || overflowed.get())) {
            schedule();
        }
    }
}
