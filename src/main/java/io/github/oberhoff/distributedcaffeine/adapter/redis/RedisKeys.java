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

import io.github.oberhoff.distributedcaffeine.spi.Store;

import static java.util.Objects.requireNonNull;

/**
 * The keys of one cache: under its keyspace's hash tag, {@code {dc:<keyspace>}}, then its discriminator, so that a
 * cluster keeps a keyspace's caches in one slot (one node, one reader of their streams, VISION.md "Redis adapters") and
 * a script may touch all of a cache's keys; caches of other keyspaces spread over the nodes. A discriminator has no
 * colon, so no cache's keys begin with another's prefix. Per cache: a hash per row, two sorted sets of the rows by
 * write time (values; tombstones and expired rows), which maintenance ranges over instead of scanning, the stream of
 * changes with the counter numbering its entries, the results of recent write requests, and the schema marker. Reserved
 * rows (the control row, the heartbeat's) are in neither set, so maintenance never sees them
 *
 * @author Andreas Oberhoff
 */
record RedisKeys(String keyspace, String discriminator) {

    RedisKeys {
        requireNonNull(keyspace, "keyspace cannot be null");
        requireNonNull(discriminator, "discriminator cannot be null");
    }

    // the cache, for messages
    String name() {
        return keyspace + "/" + discriminator;
    }

    String prefix() {
        return "{dc:" + keyspace + "}:" + discriminator + ":";
    }

    String row(String hash) {
        return rowPrefix() + hash;
    }

    String rowPrefix() {
        return prefix() + "r:";
    }

    String values() {
        return prefix() + "values";
    }

    String dead() {
        return prefix() + "dead";
    }

    String control() {
        return row(Store.CONTROL_HASH);
    }

    String stream() {
        return prefix() + "changes";
    }

    String sequence() {
        return prefix() + "sequence";
    }

    // where a write request keeps its result for some time (RedisScripts#WRITE)
    String request(String id) {
        return prefix() + "request:" + id;
    }

    String schema() {
        return prefix() + "schema";
    }

    // the stream of the probe at store creation (RedisScripts#PROBE), deleted again
    String probe() {
        return prefix() + "probe";
    }
}
