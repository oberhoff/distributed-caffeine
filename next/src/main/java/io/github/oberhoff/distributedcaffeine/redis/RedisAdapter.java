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

import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;

import java.lang.System.Logger;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Where one cache lives in Redis, Valkey or Dragonfly: in a keyspace (shared by several caches through the
 * discriminator), reached through the user's client.
 * <pre>{@code
 * RedisAdapter<String, Order> adapter = RedisAdapter.newBuilder(LettuceRedis.of(redisClient), "app")
 *     .withDiscriminator("orders")
 *     .<String, Order>build();
 * }</pre>
 * One adapter per cache: {@code build()} of a second cache with the same adapter object fails while the first is
 * open.
 * <p>
 * A keyspace is a hash tag ({@code {dc:<keyspace>}}), so in a cluster all its caches live in one slot, on one node,
 * and every application instance reads their changes with one connection; caches of different keyspaces spread over
 * the nodes, at a connection per keyspace each. On a single server or behind Sentinel all caches of a client share
 * one connection for their changes, whatever their keyspaces.
 * <p>
 * Requires Redis 6.2 or later, Valkey, Dragonfly 1.4 or later, or Kvrocks, and a client library through its entry
 * class:
 * {@link LettuceRedis}, {@link JedisRedis} or {@link RedissonRedis}. Each command costs about the same through each;
 * under load from many threads Lettuce carries the most, as it multiplexes one connection (measured: about 40 % more
 * than the others). On Dragonfly, Jedis is the Java client Dragonfly itself supports; Lettuce and Redisson pass this
 * library's suites there as well.
 * <p>
 * Rows must not be evicted under memory pressure: the server's {@code maxmemory-policy} should be {@code noeviction}
 * or a {@code volatile-*} policy, which never touches the cache's keys (they carry no TTL), and Dragonfly must not run
 * in {@code --cache_mode}. An evicting server is logged as a warning when the store is created (read from
 * {@code INFO memory}), since rows may then vanish silently. Without persistence, a restart of the server loses every
 * row; the caches notice (a gap) and drop what they hold, which costs loads, never a wrong answer.
 * <p>
 * A replica is refused when the store is created: its writes would fail, or, where it accepts writes, vanish. In a
 * cluster, wrap the library's cluster client ({@code RedisClusterClient} of Lettuce or Jedis, a Redisson client
 * of a cluster configuration); a plain client of one node is refused when the store is created, as it would reach only
 * that node's slots.
 * <p>
 * A server lacking a stream command the cache needs is refused when the store is created, whatever Redis version it
 * reports (Dragonfly before 1.4 reports Redis 6.2.11 but has no {@code XREAD}).
 * <p>
 * On Dragonfly, start the server with {@code --lock_on_hashtags}. Each of the cache's scripts touches several keys,
 * which Dragonfly spreads over its threads and, by default, locks one by one across them; the cache's keys share one
 * hashtag ({@code {dc:<keyspace>}}), by which the flag locks them together instead. Measured on one server with a
 * thread per core (Dragonfly 2.0.2): a write about twice as fast, and two instances on one hot key at 94,700
 * operations per second, as on Redis, instead of between 330 and 14,800, varying from run to run. Correct either
 * way: a script stays atomic.
 * <p>
 * A user restricted by ACL needs, on the keys {@code {dc:*}} only and no channels: {@code EVALSHA}, {@code EVAL},
 * {@code XREAD}, {@code INFO}, and what the scripts call, {@code TIME}, {@code GET}, {@code SET}, {@code DEL},
 * {@code INCR}, {@code HGET}, {@code HMGET}, {@code HSET}, {@code HDEL}, {@code XADD}, {@code XREVRANGE}, {@code ZADD},
 * {@code ZCOUNT}, {@code ZRANGEBYSCORE}, {@code ZREM}; besides what the client library sends on connecting
 * ({@code HELLO}, {@code AUTH}, {@code PING}, {@code SELECT}, {@code CLIENT SETNAME} and {@code CLIENT SETINFO}).
 * {@code SCRIPT LOAD} is optional: a script the server lost is loaded with it and run by its digest again, and sent
 * in full where it is refused (on Kvrocks, which caches no script {@code EVAL} runs, each call then sends it in
 * full).
 * <p>
 * Managed services: by their documentation (read 2026-11-02, not yet run against them), Amazon ElastiCache
 * (Serverless included), Azure Cache for Redis and Upstash offer what is needed. Their eviction policy must spare
 * keys without a TTL (Azure's default, {@code volatile-lru}, does; Upstash's eviction, off by default, may not). A
 * service sharding behind one endpoint (Azure Managed Redis under its Enterprise cluster policy) refuses a read of
 * several keyspaces' changes at once ({@code CROSSSLOT}); the changes are then read per keyspace, as in a cluster.
 * Where every command is billed (Upstash pay-as-you-go), the reading of changes costs a blocking read per keyspace
 * and application instance about every second. The tests' {@code RedisManagedServiceTest} runs the suites against
 * such a service, given its URI.
 * <p>
 * Tested, through each of the three client libraries: a single server (Redis 6.2, 7, 8; Valkey 7.2, 8, 9; Dragonfly
 * 1.4, the latest 1.x, 2; Kvrocks 2.16, 2.17), also through a user with no more than the permissions above (Kvrocks
 * has no users); a primary with a replica behind Sentinel and a cluster of three primaries with a replica each
 * (Redis, Valkey and Kvrocks: a failover, the primary killed, in the cluster also the cache's slot moving to another
 * primary while it writes; Redis and Valkey also a write the failover loses, also one written by a primary whose
 * clock was ahead of the new one's); a Dragonfly cluster of three nodes, and one emulated by a single node (the
 * latest 1.x and 2). A restart with and without persistence (Redis and Valkey, through Lettuce). Kvrocks behind
 * Sentinel works in these tests, but Kvrocks does not document Sentinel (its high availability is the cluster with a
 * controller). Not supported: Dragonfly behind Sentinel (which Dragonfly does not document either; a killed primary
 * was not always failed over) and a failover or a slot moved in a Dragonfly cluster (after a takeover, Lettuce's
 * cluster client could not refresh its view for over a minute). Not tested: Kvrocks's own controller (only the
 * layouts it would push), replication without Sentinel, TLS (the client's, passed through untouched), managed
 * services, and other servers speaking the protocol.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 */
public final class RedisAdapter<K, V> implements Adapter<K, V> {

    private static final Logger LOGGER = System.getLogger("io.github.oberhoff.distributedcaffeine.redis");
    // the layout of this version (RedisKeys, RedisScripts); a cache written by another one is refused. 2: keyspace and
    // discriminator (2026-11-01), the name alone before
    private static final String MARKER = "2";

    /**
     * The discriminator of a cache that names none.
     */
    public static final String DEFAULT_DISCRIMINATOR = "default";

    private final RedisDriver driver;
    private final RedisKeys keys;

    private RedisAdapter(Builder builder) {
        this.driver = builder.driver;
        this.keys = new RedisKeys(builder.keyspace, builder.discriminator);
    }

    /**
     * Starts an adapter for a cache in a keyspace.
     *
     * @param driver   the client, wrapped by its library's entry class (such as {@link LettuceRedis#of})
     * @param keyspace the keyspace (letters, digits, {@code -}, {@code _}, {@code .} and {@code :}, at most 100)
     * @return the builder
     */
    public static Builder newBuilder(RedisDriver driver, String keyspace) {
        return new Builder(driver, keyspace);
    }

    @Override
    public Store createStore(StoreSettings settings) {
        RedisSession session = driver.open(settings.operationTimeout());
        try {
            RedisServer server = session.server();
            if (!server.supported()) {
                throw new StoreException(server + " is not supported; distributed-caffeine needs Redis 6.2 or later, "
                        + "Valkey, or Dragonfly 1.4 or later", false);
            }
            // a plain client of one node of a cluster reaches only the slots that node owns, the others answering
            // MOVED: refused at once rather than failing on some caches only
            if (server.mode().equals("cluster") && !session.clustered()) {
                throw new StoreException(server + " is a cluster: wrap a cluster client of the library (Lettuce's "
                        + "RedisClusterClient, for instance)", false);
            }
            // a client reaching a replica: every write would fail with READONLY, or, where the replica accepts writes
            // (replica-read-only no), apply there alone and vanish with the next resynchronisation. Retryable: behind
            // Sentinel it is a failover in progress (the old primary demoted, or Sentinel's view not yet updated),
            // which passes; a plain client of a replica's address is refused at every attempt. A cluster client sends
            // every command to a slot's primary, whichever node it asked
            if (server.replica() && !session.clustered()) {
                throw new StoreException(server + " is a replica; distributed-caffeine needs the primary (reach it "
                        + "through Sentinel, or by its own address; behind Sentinel, a failover may be in progress)",
                        null, false, true);
            }
            if (evicts(server)) {
                LOGGER.log(Logger.Level.WARNING, "{0} evicts keys under memory pressure (maxmemory-policy {1}): rows "
                        + "of {2} may vanish silently; noeviction or a volatile-* policy keeps them", server,
                        server.evictionPolicy(), keys.name());
            }
            probe(session, server);
            Object marker = session.eval(RedisScripts.MARKER, List.of(keys.schema()),
                    List.of(MARKER.getBytes(StandardCharsets.UTF_8)));
            String found = new String((byte[]) requireNonNull(marker), StandardCharsets.UTF_8);
            if (!found.equals(MARKER)) {
                throw new StoreException("The cache " + keys.name() + " was written by another version of "
                        + "distributed-caffeine (layout " + found + "); delete its keys (" + keys.prefix()
                        + "*) while no cache uses them, and they are created anew", false);
            }
            return new RedisStore(driver, session, keys);
        } catch (RedisCommandException e) {
            session.close();
            throw RedisStore.failure(e, false, keys.name());
        } catch (RuntimeException e) {
            session.close();
            throw e;
        }
    }

    // refuses a server lacking a command the store needs, in a script: its version alone does not tell (Dragonfly
    // before 1.4 reports Redis 6.2.11 but has no XREAD; Garnet reports Redis 7.4.3, runs no scripts unless started
    // with --lua, and has no streams), and without the check the cache would start and fail at its first write or
    // read of changes
    private void probe(RedisSession session, RedisServer server) {
        try {
            session.eval(RedisScripts.PROBE, List.of(keys.probe()), List.of());
        } catch (RedisCommandException e) {
            if (e.kind() != RedisCommandException.Kind.SCRIPT && e.kind() != RedisCommandException.Kind.REFUSED) {
                throw e;
            }
            throw new StoreException(server + " lacks commands the Redis adapter needs (Lua scripts calling XREAD, "
                    + "XREVRANGE and XADD with MINID): " + e.getMessage(), e, false);
        }
    }

    // whether the server may evict the cache's keys, which carry no TTL: an allkeys-* policy does, and Dragonfly in
    // --cache_mode (reported as eviction); volatile-* never touches keys without a TTL (warned of, not refused: the
    // user's choice, VISION.md "Redis adapters"); unknown where the server reports no policy
    static boolean evicts(RedisServer server) {
        String policy = server.evictionPolicy();
        return policy != null && (policy.startsWith("allkeys-") || policy.equals("eviction"));
    }

    @Override
    public String toString() {
        return "RedisAdapter[" + keys.name() + ", " + driver + "]";
    }

    /**
     * Builds a {@link RedisAdapter}.
     */
    public static final class Builder {

        private final RedisDriver driver;
        private final String keyspace;
        private String discriminator = DEFAULT_DISCRIMINATOR;

        private Builder(RedisDriver driver, String keyspace) {
            this.driver = requireNonNull(driver, "driver cannot be null");
            this.keyspace = requireNonNull(keyspace, "keyspace cannot be null");
            if (!keyspace.matches("[A-Za-z0-9_.:-]{1,100}")) {
                throw new IllegalArgumentException("keyspace must be 1 to 100 letters, digits, -, _, . or : (no "
                        + "braces, which would break the hash tag): " + keyspace);
            }
        }

        /**
         * Sets the discriminator, which tells the caches sharing the keyspace apart. Optional:
         * {@value #DEFAULT_DISCRIMINATOR} by default.
         *
         * @param discriminator the discriminator (1 to 100 letters, digits, {@code -}, {@code _} and {@code .})
         * @return this builder
         */
        public Builder withDiscriminator(String discriminator) {
            requireNonNull(discriminator, "discriminator cannot be null");
            if (!discriminator.matches("[A-Za-z0-9_.-]{1,100}")) {
                throw new IllegalArgumentException("discriminator must be 1 to 100 letters, digits, -, _ or . (no "
                        + "colon, which would let one cache's keys begin with another's): " + discriminator);
            }
            this.discriminator = discriminator;
            return this;
        }

        /**
         * Builds the adapter.
         *
         * @param <K> the key type of the cache
         * @param <V> the value type of the cache
         * @return the adapter
         */
        public <K, V> RedisAdapter<K, V> build() {
            return new RedisAdapter<>(this);
        }
    }
}
