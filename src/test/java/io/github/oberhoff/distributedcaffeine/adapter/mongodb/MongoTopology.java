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
package io.github.oberhoff.distributedcaffeine.adapter.mongodb;

import io.github.oberhoff.distributedcaffeine.testing.Suite.Kind;
import io.github.oberhoff.distributedcaffeine.testing.Topology;
import org.jspecify.annotations.NullMarked;

import java.util.EnumSet;
import java.util.Set;

/**
 * The MongoDB topologies the suites run on (see {@link MongoTestDatabase} for how each is built).
 *
 * @author Andreas Oberhoff
 */
@NullMarked
enum MongoTopology implements Topology {

    SINGLE_NODE_REPLICA_SET,
    THREE_NODE_REPLICA_SET,
    THREE_NODE_REPLICA_SET_WITH_ARBITER,
    SHARDED_CLUSTER,
    // an oplog of 1 MB truncated as soon as MongoDB can: no other suite would survive it
    SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG,
    // a server that is no replica set: only the refusal at start runs on it
    STANDALONE_SERVER;

    @Override
    public Set<Kind> suites() {
        return switch (this) {
            case SINGLE_NODE_REPLICA_SET -> EnumSet.of(Kind.STORE_CONTRACT, Kind.CHANGE_FEED_CONTRACT,
                    Kind.CORE, Kind.PROPERTY, Kind.ADAPTER);
            case THREE_NODE_REPLICA_SET -> EnumSet.of(Kind.STORE_CONTRACT, Kind.CHANGE_FEED_CONTRACT, Kind.FAILOVER,
                    Kind.OUTAGE);
            case THREE_NODE_REPLICA_SET_WITH_ARBITER -> EnumSet.of(Kind.STORE_CONTRACT, Kind.CHANGE_FEED_CONTRACT,
                    Kind.OUTAGE);
            case SHARDED_CLUSTER -> EnumSet.of(Kind.STORE_CONTRACT, Kind.CHANGE_FEED_CONTRACT, Kind.SHARDING,
                    Kind.OUTAGE);
            case SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG -> EnumSet.of(Kind.OPLOG);
            case STANDALONE_SERVER -> EnumSet.of(Kind.STANDALONE);
        };
    }
}
