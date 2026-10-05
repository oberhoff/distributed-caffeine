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

import dev.failsafe.CircuitBreaker;
import dev.failsafe.CircuitBreakerOpenException;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import io.github.oberhoff.distributedcaffeine.InternalUtils.FailableRunnable;
import io.github.oberhoff.distributedcaffeine.InternalUtils.FailableSupplier;

import java.time.Duration;

import static java.lang.String.format;
import static java.util.Objects.nonNull;

/**
 * What one cache instance knows about whether its underlying store is answering, shared by everything that talks
 * to it. A store that is gone does not refuse a call, it fails to answer one - and waiting for that answer is what
 * costs, because the driver waits its own timeout (30 seconds by default on both adapters) and the write path
 * holds the synchronization lock while it does. Without this, that bill is paid once per operation for as long as
 * the outage lasts.
 * <p>
 * Shared rather than one per class, because there is one store and one question about it: a write that failed is
 * evidence for the next read, and a read that answered is evidence for the next write. Separate guards would each
 * have to find out for themselves, at a timeout apiece.
 * <p>
 * Two ways to use it, and which one fits follows from who is waiting. What the library does of its own accord is
 * {@code guarded}: refused outright while the store is known to be down, because the caller asked for a cache
 * operation and the cache cannot carry it out. What a caller asked of the store itself is {@code observed}: tried
 * whatever this knows, because the answer is what they came for - but its outcome is still recorded here, so that
 * asking the store directly teaches the same lesson as everything else.
 */
// java:S112 - what the store raised is what a caller is handed back, and the failable interfaces this is built
// on are declared to throw Throwable for exactly that reason (see InternalUtils). Narrowing it here would not
// narrow what a store can raise, only what this class is able to pass on: the wrapping is decided where the
// call is caught, which is the one place that knows what the caller already expects
@SuppressWarnings("java:S112")
class InternalStoreGuard {

    // How many calls in a row have to fail before the store is left alone. Three, because the point is to stop
    // paying the driver's timeout once per operation: at 30 seconds apiece that is already a minute and a half,
    // and fewer would let one unlucky call stand for a store that is gone
    private static final int FAILURE_THRESHOLD = 3;
    // How long it is left alone for. Short, because this is exactly the window in which an operation is refused
    // although it would have worked - and nothing is spent on finding out, since the next operation is the probe
    private static final Duration SUSPENSION = Duration.ofSeconds(1);

    // Object rather than a narrower result type, because the same breaker carries both a call that returns
    // something and one that does not: Failsafe ties the executor's result type to the policy's, so anything
    // narrower would admit only that type. Nothing here decides on a result anyway - what counts as a failure is
    // a throwable and nothing else
    private final CircuitBreaker<Object> circuitBreaker;

    InternalStoreGuard() {
        this.circuitBreaker = CircuitBreaker.builder()
                .handle(Throwable.class)
                .withFailureThreshold(FAILURE_THRESHOLD)
                .withDelay(SUSPENSION)
                .withSuccessThreshold(1)
                .build();
    }

    void runGuarded(String identifier, FailableRunnable runnable) throws Throwable {
        try {
            Failsafe.with(circuitBreaker).run(runnable::run);
        } catch (CircuitBreakerOpenException e) {
            throw suspended(identifier, e);
        } catch (FailsafeException e) {
            throw unwrapped(e);
        }
    }

    <T> T getGuarded(String identifier, FailableSupplier<T> supplier) throws Throwable {
        try {
            return Failsafe.with(circuitBreaker).get(supplier::get);
        } catch (CircuitBreakerOpenException e) {
            throw suspended(identifier, e);
        } catch (FailsafeException e) {
            throw unwrapped(e);
        }
    }

    private static IllegalStateException suspended(String identifier, CircuitBreakerOpenException e) {
        return new IllegalStateException(format("The underlying store was not contacted for cache at '%s', "
                + "because the last %d attempts to contact it failed", identifier, FAILURE_THRESHOLD), e);
    }

    // what a caller is owed is what the store raised, not the news that Failsafe carried the call out - which it
    // wraps only for a checked exception, the very case the caller wraps in turn
    private static Throwable unwrapped(FailsafeException e) {
        return nonNull(e.getCause()) ? e.getCause() : e;
    }

    // Carried out whatever this knows, and what happens to it is remembered. A caller reaching for the store
    // itself is answered rather than turned away - and an answer is better evidence than anything this could
    // gather on its own, so a successful one ends the suspension outright
    <T> T observed(FailableSupplier<T> supplier) throws Throwable {
        try {
            T result = supplier.get();
            reportReachable();
            return result;
        } catch (Throwable t) {
            circuitBreaker.recordFailure();
            throw t;
        }
    }

    // Whether the store is being left alone, which is the one thing about it this knows for certain: it follows
    // from calls that were actually made, rather than from anything asked on the side
    boolean isStoreUnreachable() {
        return circuitBreaker.isOpen();
    }

    // The store answered somebody, so there is no reason left to go on refusing. Used by whatever contacts it
    // outside the paths above - the maintenance worker above all, which does so every minute on a fixed interval
    // and is therefore the cheapest evidence there is
    void reportReachable() {
        circuitBreaker.close();
    }
}
