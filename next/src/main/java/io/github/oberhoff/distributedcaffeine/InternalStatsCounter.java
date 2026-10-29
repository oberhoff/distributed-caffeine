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

import org.jspecify.annotations.Nullable;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static java.util.Objects.requireNonNull;

// The counters behind DistributedStats; a no-op unless the cache records stats.
final class InternalStatsCounter {

    enum Counter {
        STORE_READ,
        STORE_READ_TIME,
        STORE_HIT,
        STORE_MISS,
        SOURCE_LOAD,
        SOURCE_RELOAD,
        STORE_WRITE,
        STORE_WRITE_TIME,
        PUT,
        CONDITIONAL_WRITE,
        CONFLICT,
        STORE_FAILURE,
        UNKNOWN_OUTCOME,
        REFUSED,
        GUARD_OPEN,
        WRITTEN_MESSAGE,
        INVALIDATED_MESSAGE,
        EXPIRED_MESSAGE,
        INVALIDATED_ALL_MESSAGE,
        IGNORED_MESSAGE,
        REPLACED,
        INVALIDATED,
        EXPIRED,
        POPULATION_READ,
        GAP,
        INTERRUPTION,
        REVALIDATED,
        REVALIDATION_KEPT,
        REVALIDATION_DROPPED,
        UNREADABLE_ROW,
        ABSENT_HIT,
        VERSION_REFUSAL,
        UNCACHED_LOAD,
        CLOCK_AHEAD
    }

    private static final InternalStatsCounter DISABLED = new InternalStatsCounter(false);

    private final @Nullable Map<Counter, LongAdder> adders;

    private InternalStatsCounter(boolean enabled) {
        if (enabled) {
            Map<Counter, LongAdder> created = new EnumMap<>(Counter.class);
            for (Counter counter : Counter.values()) {
                created.put(counter, new LongAdder());
            }
            this.adders = created;
        } else {
            this.adders = null;
        }
    }

    static InternalStatsCounter of(boolean enabled) {
        return enabled ? new InternalStatsCounter(true) : DISABLED;
    }

    void add(Counter counter, long amount) {
        if (adders != null && amount != 0) {
            requireNonNull(adders.get(counter)).add(amount);
        }
    }

    void increment(Counter counter) {
        add(counter, 1);
    }

    DistributedStats snapshot() {
        if (adders == null) {
            return DistributedStats.empty();
        }
        // in the order of the counters, which is the order of DistributedStats
        long[] values = adders.values().stream().mapToLong(LongAdder::sum).toArray();
        return new DistributedStats(values);
    }
}
