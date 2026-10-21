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
package io.github.oberhoff.distributedcaffeine.spi;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;

/**
 * A listener that records what it receives, for tests.
 */
public final class RecordingListener implements ChangeListener {

    private final List<Change> messages = new CopyOnWriteArrayList<>();
    private final List<List<Change>> batches = new CopyOnWriteArrayList<>();
    private final AtomicInteger gaps = new AtomicInteger();
    private final AtomicInteger losses = new AtomicInteger();

    @Override
    public void onChanges(List<Change> batch) {
        if (batch.isEmpty()) {
            throw new AssertionError("empty batch delivered");
        }
        batches.add(List.copyOf(batch));
        messages.addAll(batch);
    }

    @Override
    public void onGap() {
        gaps.incrementAndGet();
    }

    @Override
    public void onLost(Throwable cause) {
        losses.incrementAndGet();
    }

    public List<Change> messages() {
        return new ArrayList<>(messages);
    }

    public List<List<Change>> batches() {
        return new ArrayList<>(batches);
    }

    /**
     * Waits until the recorded messages satisfy the condition.
     *
     * @param condition the condition on all messages recorded so far
     * @param timeout   how long to wait
     * @return the messages recorded when the condition held
     * @throws AssertionError if it did not hold within the timeout
     */
    public List<Change> await(Predicate<List<Change>> condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            List<Change> snapshot = messages();
            if (condition.test(snapshot)) {
                return snapshot;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout + ", recorded: " + snapshot);
            }
            pause(5);
        }
    }

    /**
     * Waits until at least the given number of gaps were reported.
     *
     * @param count   the number of gaps
     * @param timeout how long to wait
     * @return the number of gaps reported
     */
    public int awaitGaps(int count, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (gaps.get() < count) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(count + " gaps not reported within " + timeout + ", got " + gaps.get());
            }
            pause(5);
        }
        return gaps.get();
    }

    /**
     * Waits until at least the given number of losses were reported.
     *
     * @param count   the number of losses
     * @param timeout how long to wait
     * @return the number of losses reported
     */
    public int awaitLosses(int count, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (losses.get() < count) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(count + " losses not reported within " + timeout + ", got " + losses.get());
            }
            pause(5);
        }
        return losses.get();
    }

    /**
     * Waits a little and returns what arrived meanwhile, to check that nothing (more) arrives.
     *
     * @param quiet how long to wait
     * @return all messages recorded so far
     */
    public List<Change> settle(Duration quiet) {
        pause(quiet.toMillis());
        return messages();
    }

}
