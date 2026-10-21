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
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A cache's delivery queue: in order, batched, and bounded; a cache that falls too far behind revalidates instead
 * of queueing without bound.
 */
class InternalDeliveryTest {

    // runs what is handed to it only when asked: a store executor too busy to drain meanwhile
    private static final class HeldExecutor implements Executor {

        private final Queue<Runnable> held = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            held.add(command);
        }

        void runAll() {
            Runnable next;
            while ((next = held.poll()) != null) {
                next.run();
            }
        }
    }

    private final HeldExecutor executor = new HeldExecutor();
    private final List<String> events = new ArrayList<>();
    private final InternalDelivery delivery = new InternalDelivery(executor,
            messages -> events.add("messages " + messages.size()), () -> events.add("gap"),
            () -> events.add("lost"));

    private static Change written(int index) {
        return new Change(Change.Kind.WRITTEN, "h" + index, index + 1L);
    }

    @Test
    void messages_are_delivered_in_order_merged_into_batches_and_never_across_a_marker() {
        delivery.onChanges(List.of(written(1)));
        delivery.onChanges(List.of(written(2), written(3)));
        delivery.onLost(new IllegalStateException("lost"));
        delivery.onChanges(List.of(written(4)));
        delivery.onGap();

        executor.runAll();

        assertThat(events).containsExactly("messages 3", "lost", "messages 1", "gap");
        assertThat(delivery.isIdle()).isTrue();
    }

    @Test
    void a_cache_fallen_too_far_behind_drops_what_is_pending_and_revalidates() {
        delivery.onLost(new IllegalStateException("lost"));
        for (int index = 0; index <= InternalDelivery.MAXIMUM_PENDING; index++) {
            delivery.onChanges(List.of(written(index)));
        }
        delivery.onGap();

        executor.runAll();

        assertThat(events).as("the markers kept in order, the messages replaced by a gap")
                .containsExactly("gap", "lost", "gap");
        assertThat(delivery.isIdle()).isTrue();

        // and it goes on as usual
        delivery.onChanges(List.of(written(1)));
        executor.runAll();
        assertThat(events).endsWith("messages 1");
    }

    @Test
    void an_executor_refusing_the_drain_does_not_fail_the_stores_listening_thread() {
        Executor refusing = command -> {
            throw new RejectedExecutionException("shut down");
        };
        List<String> received = new ArrayList<>();
        InternalDelivery refused = new InternalDelivery(refusing, messages -> received.add("messages"), () -> {
        }, () -> {
        });

        // the cache is closing; the store's thread delivering to it must not fail with it
        refused.onChanges(List.of(written(1)));
        refused.onGap();

        assertThat(received).isEmpty();
    }
}
