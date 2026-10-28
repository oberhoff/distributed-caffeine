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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The heartbeat's decisions, one tick at a time: a beat whose announcement did not arrive by the next tick makes the
 * feed silent, once; the next announcement of the reserved key ends the silence, once.
 */
class InternalHeartbeatTest {

    // the version of the last beat sent
    private final AtomicLong version = new AtomicLong();
    private final AtomicInteger silenced = new AtomicInteger();
    private final AtomicInteger resumed = new AtomicInteger();
    private final InternalHeartbeat heartbeat = new InternalHeartbeat(beat -> {
        version.set(beat);
        return true;
    }, silenced::incrementAndGet, resumed::incrementAndGet);

    @Test
    void a_beat_announced_before_the_next_tick_keeps_the_feed_up() {
        heartbeat.tick();
        heartbeat.received(version.get());
        heartbeat.tick();
        heartbeat.received(version.get());
        heartbeat.tick();

        assertThat(heartbeat.isSilent()).isFalse();
        assertThat(silenced).hasValue(0);
    }

    @Test
    void the_beats_continue_one_incarnation() {
        heartbeat.tick();
        long first = version.get();
        heartbeat.tick();

        assertThat(version.get()).isEqualTo(InternalVersion.next(first));
    }

    @Test
    void a_beat_of_another_instance_proves_nothing_about_the_own_one() {
        heartbeat.tick();
        heartbeat.received(InternalVersion.fresh()); // another incarnation: no order against the own beat

        heartbeat.tick();

        assertThat(heartbeat.isSilent()).isTrue();
        assertThat(silenced).hasValue(1);
    }

    @Test
    void a_later_own_beat_proves_the_earlier_one_arrived() {
        heartbeat.tick();
        heartbeat.received(InternalVersion.next(version.get())); // per key in commit order

        heartbeat.tick();

        assertThat(heartbeat.isSilent()).isFalse();
    }

    @Test
    void a_missed_beat_silences_the_feed_once_and_the_next_announcement_resumes_it_once() {
        heartbeat.tick();
        heartbeat.tick();
        heartbeat.tick();

        assertThat(heartbeat.isSilent()).isTrue();
        assertThat(silenced).hasValue(1);

        heartbeat.received(version.get());
        heartbeat.received(InternalVersion.fresh());

        assertThat(heartbeat.isSilent()).isFalse();
        assertThat(resumed).hasValue(1);
    }

    @Test
    void no_beat_while_the_store_does_not_answer_and_so_no_silence() {
        InternalHeartbeat failing = new InternalHeartbeat(beat -> {
            throw new IllegalStateException("store down");
        }, silenced::incrementAndGet, resumed::incrementAndGet);
        InternalHeartbeat refused = new InternalHeartbeat(beat -> false, silenced::incrementAndGet,
                resumed::incrementAndGet);

        failing.tick();
        failing.tick();
        refused.tick();
        refused.tick();

        assertThat(failing.isSilent()).isFalse();
        assertThat(refused.isSilent()).isFalse();
        assertThat(silenced).hasValue(0);
    }
}
