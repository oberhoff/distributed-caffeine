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
package io.github.oberhoff.distributedcaffeine.postgresql;

import io.github.oberhoff.distributedcaffeine.testing.Suite.Kind;
import io.github.oberhoff.distributedcaffeine.testing.Topology;
import org.jspecify.annotations.NullMarked;

import java.util.EnumSet;
import java.util.Set;

/**
 * The PostgreSQL topologies the suites run on (see {@link PostgresTestDatabase} for how each is built).
 */
@NullMarked
enum PostgresTopology implements Topology {

    SINGLE_SERVER,
    PRIMARY_WITH_ASYNCHRONOUS_STANDBY,
    PRIMARY_WITH_SYNCHRONOUS_STANDBY,
    SINGLE_SERVER_BEHIND_TRANSACTION_POOLER,
    // a notification queue of 256 pages (PostgreSQL 17 or later), filled by its suite: no other suite would survive it
    SINGLE_SERVER_WITH_SMALL_NOTIFY_QUEUE;

    @Override
    public Set<Kind> suites() {
        return switch (this) {
            case SINGLE_SERVER -> EnumSet.of(Kind.STORE_CONTRACT, Kind.CHANGE_FEED_CONTRACT,
                    Kind.CORE, Kind.PROPERTY, Kind.ADAPTER);
            case PRIMARY_WITH_ASYNCHRONOUS_STANDBY, PRIMARY_WITH_SYNCHRONOUS_STANDBY -> EnumSet.of(Kind.STORE_CONTRACT,
                    Kind.CHANGE_FEED_CONTRACT, Kind.FAILOVER);
            case SINGLE_SERVER_BEHIND_TRANSACTION_POOLER -> EnumSet.of(Kind.STORE_CONTRACT, Kind.CHANGE_FEED_CONTRACT,
                    Kind.POOLER);
            case SINGLE_SERVER_WITH_SMALL_NOTIFY_QUEUE -> EnumSet.of(Kind.NOTIFY_QUEUE);
        };
    }

    boolean hasStandby() {
        return this == PRIMARY_WITH_ASYNCHRONOUS_STANDBY || this == PRIMARY_WITH_SYNCHRONOUS_STANDBY;
    }
}
