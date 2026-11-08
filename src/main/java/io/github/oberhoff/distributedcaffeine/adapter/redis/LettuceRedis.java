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

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.ByteArrayCodec;

import java.time.Duration;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * The entry for Lettuce ({@code io.lettuce:lettuce-core}): wraps the application's client for the cache. Lettuce is
 * needed on the classpath only where this class is used.
 * <p>
 * The cache opens its own connections through the client and closes them again; the client and its options (URI, TLS,
 * credentials) stay the application's.
 *
 * @author Andreas Oberhoff
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
     * Wraps a client of a cluster. Lettuce follows slot moves and failovers by itself through adaptive topology
     * refresh, on by default.
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
