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

import org.jspecify.annotations.Nullable;

import java.util.List;

/**
 * The commands the Redis store needs from a client library, for one cache. Nearly everything runs in Lua scripts
 * (VISION.md, "Redis adapters"), so a binding has little to do: run a script, read the stream blocking on a connection
 * of its own, describe the server. Every command is bounded by the session's timeout; a command that fails throws a
 * RedisCommandException, whose kind tells the store what happened.
 *
 * @author Andreas Oberhoff
 */
interface RedisSession extends AutoCloseable {

    // runs a script by its SHA-1, sending it in full only when the server does not have it (NOSCRIPT). Keys are
    // UTF-8, arguments bytes. The result in Redis's terms: Long for an integer, byte[] for a bulk string, String for
    // a status, List<@Nullable Object> for an array, null for a nil (Lua false)
    @Nullable Object eval(RedisScript script, List<String> keys, List<byte[]> arguments);

    // what the server is: product, version, mode, eviction policy
    RedisServer server();

    // a reader of streams on a connection of its own, as a blocking read holds its connection; closed by its owner. In
    // a cluster, its reads go to the node owning their streams' slot, as the client library routes them
    RedisStreamReader openReader();

    // whether the server is a cluster, where one read may only read streams of one slot (a reader per keyspace)
    default boolean clustered() {
        return false;
    }

    // closes the connections the session opened; the client stays open
    @Override
    void close();
}
