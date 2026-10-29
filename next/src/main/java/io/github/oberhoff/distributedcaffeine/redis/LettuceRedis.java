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
package io.github.oberhoff.distributedcaffeine.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.ByteArrayCodec;

import java.time.Duration;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * The entry for Lettuce ({@code io.lettuce:lettuce-core}): wraps the user's client for this library. Only this class
 * refers to Lettuce, so Lettuce is needed on the classpath only where it is used.
 * <p>
 * The library opens connections of its own through the client (one for a cache's commands, one per stream it reads)
 * and closes them again; the client stays the user's, with its options (URI, TLS, credentials, socket options). Each
 * command is bounded by the cache's operation timeout, also where caches with different timeouts share the client.
 */
public final class LettuceRedis {

    private LettuceRedis() {
    }

    /**
     * Wraps a client of a single server or of a server behind Sentinel (its URI says which).
     *
     * @param client the client
     * @return the driver
     */
    public static RedisDriver of(RedisClient client) {
        requireNonNull(client, "client cannot be null");
        return new Driver(() -> client.connect(ByteArrayCodec.INSTANCE), false);
    }

    /**
     * Wraps a client of a cluster: each command goes to the node owning its keys' slot. Lettuce follows a redirect by
     * itself, and refreshes its view of the cluster on one (adaptive triggers, on by default since Lettuce 7; before,
     * to be enabled in {@code ClusterTopologyRefreshOptions}); a periodic refresh as well notices a new primary
     * sooner where no command was redirected yet.
     *
     * @param client the client
     * @return the driver
     */
    public static RedisDriver of(RedisClusterClient client) {
        requireNonNull(client, "client cannot be null");
        return new Driver(() -> client.connect(ByteArrayCodec.INSTANCE), true);
    }

    private static final class Driver extends RedisDriver {

        private final Supplier<StatefulConnection<byte[], byte[]>> opener;
        private final boolean clustered;

        Driver(Supplier<StatefulConnection<byte[], byte[]>> opener, boolean clustered) {
            this.opener = opener;
            this.clustered = clustered;
        }

        @Override
        RedisSession open(Duration timeout) {
            return new LettuceSession(opener, clustered, timeout);
        }

        @Override
        public String toString() {
            return "Lettuce";
        }
    }
}
