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

import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A simulated environment of the core (VISION.md, "Deterministic simulation", B): its time moves only when a test
 * advances it, or when the core sleeps (a back-off, a pause: the clock moves on by the sleep, and what fell due meanwhile
 * runs at the next advance, not on the sleeping thread, which may be inside an operation, where a delivery could not
 * run with real threads); scheduled tasks run in the order they are due, on the thread that advances the time; randomness comes from
 * the seed, the wall clock from a fixed date. With the cache's executor direct ({@code Runnable::run}), what a task
 * starts (a poll, a heartbeat, maintenance, a resume) runs on that thread too, so one test thread drives virtual
 * minutes in milliseconds.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class SimulatedEnvironment implements InternalEnvironment {

    // 2026-11-03T00:00:00Z: a fixed wall clock, so that versions repeat with the seed
    private static final long EPOCH_MILLIS = 1_793_664_000_000L;

    private final Random random;
    // guarded by this: the tasks due, the time (nanoseconds since the start), the order of tasks due at once
    private final PriorityQueue<Task> due = new PriorityQueue<>();
    private long now;
    private long sequence;

    SimulatedEnvironment(long seed) {
        this.random = new Random(seed);
    }

    @Override
    public synchronized long nanoTime() {
        return now;
    }

    @Override
    public synchronized long currentTimeMillis() {
        return EPOCH_MILLIS + now / 1_000_000;
    }

    @Override
    public void sleep(Duration duration) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        synchronized (this) {
            now += Math.max(0, duration.toNanos());
        }
    }

    @Override
    public synchronized ScheduledFuture<?> schedule(Runnable task, Duration delay) {
        Task scheduled = new Task(task, now + Math.max(1, delay.toNanos()), sequence++);
        due.add(scheduled);
        return scheduled;
    }

    @Override
    public synchronized long random(long bound) {
        return random.nextLong(bound);
    }

    /**
     * Returns this simulation for an in-memory database ({@link InMemoryDatabase#simulate}): its time, and its
     * deliveries as tasks of this simulation.
     *
     * @return the simulation
     */
    @SuppressWarnings("FutureReturnValueIgnored") // a delivery is never cancelled
    InMemoryDatabase.Simulation forDatabase() {
        return new InMemoryDatabase.Simulation() {
            @Override
            public long nanoTime() {
                return SimulatedEnvironment.this.nanoTime();
            }

            @Override
            public void runAfter(Runnable task, Duration delay) {
                schedule(task, delay);
            }
        };
    }

    /**
     * Moves the time on by the duration, running every task due meanwhile at its time, in order (a task scheduled by
     * another one within the duration runs too).
     *
     * @param duration how far
     */
    void advance(Duration duration) {
        long target;
        synchronized (this) {
            target = now + duration.toNanos();
        }
        while (true) {
            Task next;
            synchronized (this) {
                next = due.peek();
                // up to the target, or further where a task's sleep moved the clock past it: what fell due runs
                if (next == null || next.at > Math.max(target, now)) {
                    now = Math.max(now, target);
                    return;
                }
                due.poll();
                now = Math.max(now, next.at);
            }
            next.run();
        }
    }

    // a task due at a time; cancelled, it does not run
    private final class Task implements ScheduledFuture<Void> {

        private final Runnable task;
        private final long at;
        private final long order;
        private volatile boolean cancelled;
        private volatile boolean done;

        Task(Runnable task, long at, long order) {
            this.task = task;
            this.at = at;
            this.order = order;
        }

        void run() {
            if (!cancelled) {
                try {
                    task.run();
                } finally {
                    done = true;
                }
            }
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(at - nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            Task that = (Task) other;
            return at != that.at ? Long.compare(at, that.at) : Long.compare(order, that.order);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelled = true;
            synchronized (SimulatedEnvironment.this) {
                due.remove(this);
            }
            return !done;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return done || cancelled;
        }

        @Override
        public @Nullable Void get() {
            throw new UnsupportedOperationException("a simulated task is not waited for");
        }

        @Override
        public @Nullable Void get(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException("a simulated task is not waited for");
        }
    }
}
