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

import io.github.oberhoff.distributedcaffeine.adapter.redis.JedisSession.Lease;
import io.github.oberhoff.distributedcaffeine.adapter.redis.JedisSession.Source;
import org.jspecify.annotations.Nullable;
import redis.clients.jedis.Connection;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.RedisSentinelClient;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.Pool;

import java.time.Duration;
import java.util.Iterator;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * The entry for Jedis ({@code redis.clients:jedis}, 8 or later): wraps the application's client for the cache. Jedis is
 * needed on the classpath only where this class is used.
 * <p>
 * The cache borrows a connection from the client's pool per command, and holds one for as long as it listens for
 * changes: size the pool one connection larger (in a cluster, the pool of the node holding the keyspace).
 *
 * @author Andreas Oberhoff
 */
public final class JedisRedis {

    // what the class Javadoc leaves out: each command's socket timeout is set to the cache's operation timeout on the
    // borrowed connection and restored before it goes back; a connection that timed out or broke goes back as broken,
    // and the pool replaces it.

    private static final String CLIENT_REQUIRED = "client cannot be null";

    private JedisRedis() {
    }

    /**
     * Wraps a client of a single server.
     *
     * @param client the client
     * @return the driver
     */
    public static RedisDriver of(RedisClient client) {
        requireNonNull(client, CLIENT_REQUIRED);
        return new Driver(new Pooled(client::getPool), "Jedis");
    }

    /**
     * Wraps a client of a server behind Sentinel: commands go to the primary Sentinel names at the time.
     *
     * @param client the client
     * @return the driver
     */
    public static RedisDriver of(RedisSentinelClient client) {
        requireNonNull(client, CLIENT_REQUIRED);
        return new Driver(new Pooled(() -> {
            Iterator<Pool<Connection>> pools = client.getPrimaryNodesConnectionMap().values().iterator();
            if (!pools.hasNext()) {
                throw new IllegalStateException("Sentinel names no primary");
            }
            return pools.next();
        }), "Jedis (Sentinel)");
    }

    /**
     * Wraps a client of a cluster; Jedis follows slot moves and failovers by itself.
     *
     * @param client the client
     * @return the driver
     */
    public static RedisDriver of(RedisClusterClient client) {
        requireNonNull(client, CLIENT_REQUIRED);
        return new Driver(new Source() {
            // a command without keys (INFO) to the owner of slot 0, any node serving as well
            @Override
            public Lease borrow(@Nullable String key) {
                return new Lease(client.getConnectionFromSlot(key == null ? 0 : JedisClusterCRC16.getSlot(key)),
                        null);
            }

            // quietly: where no node answers either, the command's own failure tells
            @Override
            public void changed() {
                try {
                    client.refreshClusterTopology();
                } catch (RuntimeException ignored) {
                    // the next failure refreshes again
                }
            }

            @Override
            public boolean clustered() {
                return true;
            }
        }, "Jedis (Cluster)");
    }

    // one pool for every command, whatever its key
    private record Pooled(Supplier<Pool<Connection>> pool) implements Source {

        @Override
        public Lease borrow(@Nullable String key) {
            Pool<Connection> current = pool.get();
            return new Lease(current.getResource(), current);
        }

        @Override
        public void changed() {
            // no cluster: the pool reconnects, Sentinel's names the new primary
        }

        @Override
        public boolean clustered() {
            return false;
        }
    }

    private static final class Driver extends RedisDriver {

        private final Source source;
        private final String name;

        Driver(Source source, String name) {
            this.source = source;
            this.name = name;
        }

        @Override
        RedisSession open(Duration timeout) {
            return new JedisSession(source, timeout);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
