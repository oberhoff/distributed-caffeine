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

import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Caches sharing a client wait once for a store that does not answer ({@link InternalReachability},
 * {@link Adapter#client()}): a creation that waited and failed as not reached makes the next ones through the client
 * fail at once, for one operation timeout. On the simulated clock: a creation waits by moving it on.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@SuppressWarnings("resource") // the stores created are the in-memory database's, closed with nothing to release
class InternalReachabilityTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final StoreSettings SETTINGS = new StoreSettings(Format.TEXT, Format.TEXT, TIMEOUT);

    private final SimulatedEnvironment environment = new SimulatedEnvironment(1);
    private final InMemoryDatabase database = new InMemoryDatabase();
    private final Object client = new Object();
    private final AtomicInteger asked = new AtomicInteger();

    // how a creation through the adapter goes: it waits so long, then reaches the store or fails as given
    private enum Outcome {
        REACHED, NOT_REACHED, REFUSED
    }

    private Adapter<String, String> adapter(@Nullable Object shared, Duration wait, Outcome outcome) {
        return new Adapter<>() {
            @Override
            public Store createStore(StoreSettings settings) {
                asked.incrementAndGet();
                environment.advance(wait);
                return switch (outcome) {
                    case REACHED -> database.openStore("cache");
                    case NOT_REACHED -> throw new StoreException("not answered", null, false, true);
                    case REFUSED -> throw new StoreException("refused", false);
                };
            }

            @Override
            public @Nullable Object client() {
                return shared;
            }
        };
    }

    private Store create(Adapter<String, String> adapter) {
        return InternalReachability.create(adapter, SETTINGS, environment);
    }

    private void failsAsNotReached(Adapter<String, String> adapter, String message) {
        assertThatThrownBy(() -> create(adapter)).isInstanceOfSatisfying(StoreException.class, e -> {
            assertThat(e.isRetryable()).isTrue();
            assertThat(e.isOutcomeUnknown()).isFalse();
        }).hasMessageContaining(message);
    }

    private Adapter<String, String> silent() {
        return adapter(client, TIMEOUT, Outcome.NOT_REACHED);
    }

    private Adapter<String, String> reaching() {
        return adapter(client, Duration.ZERO, Outcome.REACHED);
    }

    @Test
    void a_creation_that_waited_for_no_answer_makes_the_next_ones_through_the_client_fail_at_once() {
        failsAsNotReached(silent(), "not answered");
        environment.advance(Duration.ofSeconds(9));

        failsAsNotReached(reaching(), "did not answer another cache on the same client");
        failsAsNotReached(silent(), "did not answer another cache on the same client");
        assertThat(asked.get()).as("the store asked once").isEqualTo(1);
    }

    @Test
    void the_store_is_asked_again_after_one_operation_timeout() {
        failsAsNotReached(silent(), "not answered");
        environment.advance(TIMEOUT);

        assertThat(create(reaching())).isNotNull();
        assertThat(create(reaching())).isNotNull();
        assertThat(asked.get()).isEqualTo(3);
    }

    // a refused connection or an unknown host costs nothing to ask again, and a restarting store may be back
    @Test
    void a_creation_failing_fast_is_not_remembered() {
        failsAsNotReached(adapter(client, Duration.ofMillis(999), Outcome.NOT_REACHED), "not answered");

        assertThat(create(reaching())).isNotNull();
        assertThat(asked.get()).isEqualTo(2);
    }

    // the store answered, so it is reached: the next creation asks it
    @Test
    void a_failure_the_store_answered_is_not_remembered() {
        assertThatThrownBy(() -> create(adapter(client, TIMEOUT, Outcome.REFUSED))).hasMessage("refused");

        assertThat(create(reaching())).isNotNull();
        assertThat(asked.get()).isEqualTo(2);
    }

    @Test
    void other_clients_and_adapters_without_one_are_not_affected() {
        failsAsNotReached(silent(), "not answered");

        assertThat(create(adapter(new Object(), Duration.ZERO, Outcome.REACHED))).isNotNull();
        assertThat(create(adapter(null, Duration.ZERO, Outcome.REACHED))).isNotNull();
        failsAsNotReached(adapter(null, TIMEOUT, Outcome.NOT_REACHED), "not answered");
        assertThat(create(adapter(null, Duration.ZERO, Outcome.REACHED))).isNotNull();
        assertThat(asked.get()).isEqualTo(5);
    }
}
