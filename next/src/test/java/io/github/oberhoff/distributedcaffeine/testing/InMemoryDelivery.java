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
package io.github.oberhoff.distributedcaffeine.testing;

import io.github.oberhoff.distributedcaffeine.spi.Kind;
import io.github.oberhoff.distributedcaffeine.spi.Listener;
import io.github.oberhoff.distributedcaffeine.spi.Message;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.Subscription;
import org.jspecify.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

// One subscription: a queue filled on commit, in commit order, and a daemon thread delivering what has accumulated
// as one batch, with the faults of the database applied. A gap marker in the queue becomes an onGap() call at its
// position.
final class InMemoryDelivery implements Subscription {

    private enum Marker {
        GAP
    }

    private final InMemoryDatabase database;
    private final Listener listener;
    private final Consumer<InMemoryDelivery> onClose;
    private final Deque<Object> queue = new ArrayDeque<>();
    private final Thread thread;
    private boolean closed;
    private boolean interrupted;
    // whether an interrupted subscription can be resumed now, and whether what queued meanwhile is delivered then;
    // null while not reachable
    private @Nullable Boolean resumable;
    private boolean delivering;

    InMemoryDelivery(InMemoryDatabase database, Listener listener, Consumer<InMemoryDelivery> onClose) {
        this.database = database;
        this.listener = listener;
        this.onClose = onClose;
        this.thread = new Thread(this::run, "in-memory-delivery");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    synchronized void enqueue(List<Message> messages) {
        if (closed || database.faults.down) {
            return;
        }
        for (Message message : messages) {
            queue.addLast(message);
            if (database.faults.duplicateProbability > 0 && database.nextDouble() < database.faults.duplicateProbability) {
                queue.addLast(message);
            }
        }
        notifyAll();
    }

    synchronized void gap() {
        if (closed) {
            return;
        }
        queue.clear();
        queue.addLast(Marker.GAP);
        notifyAll();
    }

    synchronized void dropUndelivered() {
        queue.clear();
    }

    // delivery stops at once (reported synchronously); queued and further messages wait for resume()
    void interrupt() {
        synchronized (this) {
            if (closed || interrupted) {
                return;
            }
            interrupted = true;
            resumable = null;
        }
        listener.onLost(new IllegalStateException("injected: delivery interrupted"));
    }

    // the store reachable again (the fault healed): what queued meanwhile is delivered after resume(), or lost (both
    // are allowed: the core counts every resume as a gap)
    synchronized void allowResume(boolean deliverMissed) {
        if (!closed && interrupted) {
            resumable = deliverMissed;
        }
    }

    @Override
    public synchronized void resume() {
        if (closed || !interrupted) {
            return; // an in-memory delivery has no connection to re-establish
        }
        if (resumable == null) {
            throw new StoreException("injected: the store is not reachable", null, false, true);
        }
        boolean deliverMissed = resumable;
        interrupted = false;
        resumable = null;
        if (!deliverMissed) {
            queue.clear();
        }
        notifyAll();
    }

    synchronized void wake() {
        notifyAll();
    }

    // nothing queued and no batch on its way to the listener
    synchronized boolean isIdle() {
        return closed || (queue.isEmpty() && !delivering);
    }

    @Override
    @SuppressWarnings("ReferenceEquality") // whether close is called from the delivery thread itself
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            queue.clear();
            notifyAll();
        }
        onClose.accept(this);
        if (Thread.currentThread() != thread) {
            try {
                thread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void run() {
        while (true) {
            List<Object> batch = take();
            if (batch == null) {
                return;
            }
            long delay = database.nextLong(database.faults.maxDelayNanos);
            if (delay > 0) {
                LockSupport.parkNanos(delay);
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
            }
            try {
                deliver(batch);
            } finally {
                synchronized (this) {
                    delivering = false;
                }
            }
        }
    }

    private synchronized @Nullable List<Object> take() {
        while (!closed && (queue.isEmpty() || database.faults.paused || interrupted)) {
            try {
                wait(100);
            } catch (InterruptedException e) {
                return null;
            }
        }
        if (closed) {
            return null;
        }
        List<Object> batch = new ArrayList<>(queue);
        queue.clear();
        delivering = true;
        return batch;
    }

    private void deliver(List<Object> batch) {
        List<Message> segment = new ArrayList<>();
        for (Object item : batch) {
            if (item instanceof Marker marker) {
                flush(segment);
                if (isOpen() && marker == Marker.GAP) {
                    listener.onGap();
                }
            } else {
                Message message = (Message) item;
                if (message.kind() == Kind.INVALIDATED_ALL) {
                    // nothing moves across an INVALIDATED_ALL
                    segment.add(message);
                    flush(segment);
                } else {
                    segment.add(message);
                }
            }
        }
        flush(segment);
    }

    private void flush(List<Message> segment) {
        if (segment.isEmpty()) {
            return;
        }
        List<Message> messages = database.faults.reorderAcrossKeys ? reorderAcrossKeys(segment) : List.copyOf(segment);
        segment.clear();
        if (isOpen()) {
            listener.onMessages(messages);
        }
    }

    // a random interleaving of the per-key sequences: per key the order stays, across keys it does not
    private List<Message> reorderAcrossKeys(List<Message> segment) {
        Map<String, Deque<Message>> byKey = new LinkedHashMap<>();
        Message trailingInvalidatedAll = null;
        for (Message message : segment) {
            if (message.kind() == Kind.INVALIDATED_ALL) {
                trailingInvalidatedAll = message;
            } else {
                byKey.computeIfAbsent(message.hash(), hash -> new ArrayDeque<>()).addLast(message);
            }
        }
        List<Deque<Message>> sequences = new ArrayList<>(byKey.values());
        List<Message> result = new ArrayList<>(segment.size());
        while (!sequences.isEmpty()) {
            int index = database.nextInt(sequences.size());
            Deque<Message> sequence = sequences.get(index);
            result.add(sequence.removeFirst());
            if (sequence.isEmpty()) {
                sequences.remove(index);
            }
        }
        if (trailingInvalidatedAll != null) {
            result.add(trailingInvalidatedAll);
        }
        return result;
    }

    private synchronized boolean isOpen() {
        return !closed;
    }
}
