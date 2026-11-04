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
package io.github.oberhoff.distributedcaffeine.adapter.redis;

import org.jspecify.annotations.NullMarked;

/**
 * Replicated servers whose primary can be changed on demand ({@link RedisSentinel}, {@link RedisCluster}), for the
 * failover tests. Each method is about the primary holding the given key and its replica: in a cluster, the shard
 * owning the key's slot; behind Sentinel, the one primary.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
interface RedisTopology {

    /**
     * The node holding the key as primary now, as an index of the topology's nodes.
     *
     * @param key the key
     * @return the node
     */
    int primary(String key);

    /**
     * Waits until the replica has everything the primary replicated: its link up, its offset at the primary's.
     *
     * @param key the key
     */
    void awaitInSync(String key);

    /**
     * Makes the replica primary, once it has everything (nothing lost), and waits until the old primary serves as its
     * replica.
     *
     * @param key the key
     */
    void failover(String key);

    /**
     * Each node of the key's shard: role, replication link and offset, and its copy of the key (what a failover left).
     *
     * @param key the key, a hash
     * @return the description
     */
    String describe(String key);

    /**
     * Kills the node holding the key as primary (no handover: the others notice only once it stays silent) and waits
     * until its replica took over.
     *
     * @param key the key
     */
    void kill(String key);

    /**
     * Starts a node killed before again, on an empty data set (no persistence), and waits until it serves as the
     * replica of its old shard, in sync.
     */
    void revive();

    /**
     * Whether a killed node returns to the topology as a replica once started again ({@link #revive()}).
     *
     * @return whether it does
     */
    default boolean revives() {
        return true;
    }

    /**
     * Whether a write can be kept from the replica on demand ({@code redis.set_repl} in a script), for the tests of a
     * write the failover loses.
     *
     * @return whether it can
     */
    default boolean losesWritesOnDemand() {
        return true;
    }

    /**
     * Whether the topology fails over (and, in a cluster, moves slots) on the tests' demand.
     *
     * @return whether it does
     */
    default boolean failsOver() {
        return true;
    }
}
