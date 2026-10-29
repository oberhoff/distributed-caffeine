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

import io.github.oberhoff.distributedcaffeine.InternalStoreGuard.Operation;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The retries the core owns (VISION.md, "Write retries"): a store classifies its failures and retries nothing; the
 * core repeats what is retryable as far as that is safe, keeps an outcome unknown once it was, and takes anything
 * else a store throws for a store failure.
 */
@NullMarked
@Timeout(30)
@SuppressWarnings("resource") // every cache a test opens is closed after it (@AfterEach)
class InternalRetryTest {

    private static final Duration LONG = Duration.ofHours(1);
    private final StoreException rejected = new StoreException("rejected", null, false, true);
    private final StoreException broken = new StoreException("broken after sending", null, true, true);
    private final StoreException failed = new StoreException("failed for good", null, false, false);

    private final InMemoryDatabase database = new InMemoryDatabase();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    // what the next write calls do: fail before applying, or apply and fail afterwards
    private final Deque<Fault> faults = new ArrayDeque<>();
    private final AtomicInteger puts = new AtomicInteger();
    private final AtomicInteger conditionalWrites = new AtomicInteger();

    private record Fault(RuntimeException failure, boolean applied) {
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    @SuppressWarnings("ReturnValueIgnored") // an applied write's result is dropped: the failure is reported anyway
    private <T> T faulty(Supplier<T> write) {
        Fault fault = faults.poll();
        if (fault == null) {
            return write.get();
        }
        if (fault.applied()) {
            write.get();
        }
        throw fault.failure();
    }

    private DistributedCache<String, String> cache() {
        Adapter<String, String> adapter = settings -> new InternalCoreTest.DelegatingStore(
                database.openStore("cache")) {
            // the writes of values, unconditional (put) or conditional; tombstones (the heartbeat's too) pass
            @Override
            public WriteResult write(Collection<Write> writes) {
                if (writes.stream().allMatch(Write::isTombstone)) {
                    return super.write(writes);
                }
                if (writes.stream().allMatch(Write::isAdvance)) {
                    puts.incrementAndGet();
                } else {
                    conditionalWrites.incrementAndGet();
                }
                return faulty(() -> super.write(writes));
            }
        };
        DistributedCache<String, String> cache = DistributedCaffeine.newBuilder(InternalCoreTest.offered(adapter))
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10))
                        .withMaximumAge(LONG).withMaximumSize(1_000))
                .withCaffeine(CaffeineConfigurer::recordStats)
                .build();
        closeables.add(cache);
        return cache;
    }

    // the decisions, one call at a time

    private static Supplier<String> failing(AtomicInteger calls, RuntimeException... failures) {
        return () -> {
            int call = calls.getAndIncrement();
            if (call < failures.length) {
                throw failures[call];
            }
            return "done";
        };
    }

    @Test
    void a_retryable_failure_is_tried_again_up_to_three_times() {
        AtomicInteger calls = new AtomicInteger();
        assertThat(InternalStoreGuard.retrying(failing(calls, rejected, rejected), Operation.PUT)).isEqualTo("done");
        assertThat(calls).hasValue(3);

        AtomicInteger exhausted = new AtomicInteger();
        assertThatThrownBy(() -> InternalStoreGuard.retrying(failing(exhausted, rejected, rejected, rejected),
                Operation.WRITE)).isInstanceOfSatisfying(StoreException.class,
                        e -> assertThat(e.isOutcomeUnknown()).isFalse())
                .hasMessageContaining("after 3 attempts");
        assertThat(exhausted).hasValue(3);
    }

    @Test
    void a_failure_that_is_not_retryable_is_not_tried_again() {
        AtomicInteger calls = new AtomicInteger();
        assertThatThrownBy(() -> InternalStoreGuard.retrying(failing(calls, failed), Operation.READ)).isSameAs(failed);
        assertThat(calls).hasValue(1);
    }

    @Test
    void an_unknown_outcome_ends_a_put_but_not_a_write_that_may_be_repeated_or_a_read() {
        AtomicInteger put = new AtomicInteger();
        assertThatThrownBy(() -> InternalStoreGuard.retrying(failing(put, broken), Operation.PUT)).isSameAs(broken);
        assertThat(put).hasValue(1);

        AtomicInteger write = new AtomicInteger();
        assertThat(InternalStoreGuard.retrying(failing(write, broken), Operation.WRITE)).isEqualTo("done");
        assertThat(write).hasValue(2);

        AtomicInteger read = new AtomicInteger();
        assertThat(InternalStoreGuard.retrying(failing(read, broken), Operation.READ)).isEqualTo("done");
        assertThat(read).hasValue(2);
    }

    @Test
    void a_write_whose_outcome_was_unknown_in_one_attempt_stays_unknown() {
        AtomicInteger calls = new AtomicInteger();
        assertThatThrownBy(() -> InternalStoreGuard.retrying(failing(calls, broken, failed), Operation.WRITE))
                .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isTrue())
                .hasMessageContaining("after 2 attempts");

        AtomicInteger read = new AtomicInteger();
        assertThatThrownBy(() -> InternalStoreGuard.retrying(failing(read, broken, failed), Operation.READ))
                .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isFalse());
    }

    @Test
    void anything_else_a_store_throws_is_a_store_failure_that_is_not_retried() {
        IllegalStateException bug = new IllegalStateException("a driver failing unexpectedly");
        AtomicInteger write = new AtomicInteger();
        assertThatThrownBy(() -> InternalStoreGuard.retrying(failing(write, bug), Operation.WRITE))
                .isInstanceOfSatisfying(StoreException.class, e -> {
                    assertThat(e.isOutcomeUnknown()).as("a write may have been applied").isTrue();
                    assertThat(e.isRetryable()).isFalse();
                    assertThat(e).hasCause(bug);
                });
        assertThat(write).hasValue(1);

        AtomicInteger read = new AtomicInteger();
        assertThatThrownBy(() -> InternalStoreGuard.retrying(failing(read, bug), Operation.READ))
                .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isFalse());
    }

    // through a cache

    @Test
    void a_put_rejected_twice_is_written_by_the_third_attempt_and_counted_once() {
        DistributedCache<String, String> cache = cache();
        faults.add(new Fault(rejected, false));
        faults.add(new Fault(rejected, false));

        cache.put("k", "v");

        assertThat(puts).hasValue(3);
        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).isEqualTo("v");
        assertThat(cache.distributedPolicy().stats().storeFailureCount()).isZero();
    }

    @Test
    void a_put_whose_outcome_is_unknown_is_not_repeated_and_the_caller_is_told() {
        DistributedCache<String, String> cache = cache();
        faults.add(new Fault(broken, true));

        assertThatThrownBy(() -> cache.put("k", "v"))
                .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isTrue());

        assertThat(puts).hasValue(1);
        assertThat(cache.distributedPolicy().stats().unknownOutcomeCount()).isEqualTo(1);
        assertThat(cache.policy().getIfPresentQuietly("k")).as("not held: whether it was written is unknown")
                .isNull();
    }

    @Test
    void a_conditional_write_whose_outcome_is_unknown_is_repeated_and_finds_its_own_row() {
        DistributedCache<String, String> cache = cache();
        faults.add(new Fault(broken, true)); // applied, then broken: the repetition conflicts with its own write

        // the repetition cannot tell its own row from another writer's: the value counts as present already
        // (VISION.md, "Write retries": an open point)
        assertThat(cache.asMap().putIfAbsent("k", "v")).isIn(null, "v");

        assertThat(conditionalWrites).hasValue(2);
        assertThat(cache.getIfPresent("k")).isEqualTo("v");
        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).isEqualTo("v");
    }
}
