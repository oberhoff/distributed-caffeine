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

import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;

import java.lang.System.Logger;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Stores a cache in Redis (also Valkey, Dragonfly, Kvrocks), through the application's client. Several caches can share
 * a keyspace, told apart by their discriminator.
 * <pre>{@code
 * RedisAdapter<Key, Value> adapter = RedisAdapter.newBuilder(LettuceRedis.of(redisClient), keyspaceName)
 *     .withDiscriminator(discriminator)
 *     .build();
 * }</pre>
 * An adapter serves one cache: building a second cache with the same adapter fails while the first is open.
 * <p>
 * Requires Redis 6.2 or later, Valkey, Dragonfly 1.4 or later, or Kvrocks, and a client library through its entry
 * class: {@link LettuceRedis}, {@link JedisRedis} or {@link RedissonRedis}. A single server, a primary behind Sentinel,
 * or a cluster through the library's cluster client; a replica is refused, as is a server lacking a command the cache
 * needs. In a cluster, all caches of a keyspace live on one node, and each application instance reads their changes
 * over one connection; caches of different keyspaces spread over the nodes.
 * <p>
 * The cache's keys must not be evicted: set {@code maxmemory-policy} to {@code noeviction} or a {@code volatile-*}
 * policy (the keys have no TTL), and do not run Dragonfly with {@code --cache_mode}; an evicting server is logged as a
 * warning. Without persistence, a server restart loses the stored values; the caches notice and drop what they hold.
 * <p>
 * On Dragonfly, start the server with {@code --lock_on_hashtags}, which makes writes much faster. Not supported:
 * Dragonfly behind Sentinel, and failovers in a Dragonfly cluster.
 * <p>
 * A user restricted by ACL needs, on the keys {@code {dc:*}} and no channels: {@code EVALSHA}, {@code EVAL},
 * {@code XREAD}, {@code INFO}, and for the scripts {@code TIME}, {@code GET}, {@code SET}, {@code DEL}, {@code INCR},
 * {@code HGET}, {@code HMGET}, {@code HSET}, {@code HDEL}, {@code XADD}, {@code XREVRANGE}, {@code ZADD},
 * {@code ZCOUNT}, {@code ZRANGEBYSCORE}, {@code ZREM}; besides what the client library sends on connecting
 * ({@code HELLO}, {@code AUTH}, {@code PING}, {@code SELECT}, {@code CLIENT SETNAME}, {@code CLIENT SETINFO}).
 * {@code SCRIPT LOAD} is optional.
 * <p>
 * Managed services (Amazon ElastiCache, Azure Cache for Redis, Upstash) offer what is needed, provided their eviction
 * policy spares keys without a TTL; not yet tested against them.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
public final class RedisAdapter<K, V> implements Adapter<K, V> {

    // what the class Javadoc leaves out: A keyspace is a hash tag ({dc:<keyspace>}); on a single server or behind
    // Sentinel all caches of a client share one connection for their changes. Client libraries: each command costs
    // about the same through each; under load from many threads Lettuce carries about 40 % more, multiplexing one
    // connection; on Dragonfly, Jedis is the client Dragonfly supports, Lettuce and Redisson pass the suites too.
    // Dragonfly before 1.4 reports Redis 6.2.11 but has no XREAD, hence the command check. --lock_on_hashtags: each
    // script touches several keys, which Dragonfly locks one by one across its threads; the shared hash tag lets the
    // flag lock them together. Measured on one server, Dragonfly 2.0.2: a write about twice as fast, two instances on
    // one hot key at 94,700 operations per second as on Redis, instead of 330 to 14,800. Correct either way. SCRIPT
    // LOAD: a script the server lost is loaded with it and run by its digest again, sent in full where it is refused
    // (Kvrocks caches no script EVAL runs). Managed services, by their documentation (read 2026-11-02): ElastiCache
    // (Serverless included), Azure and Upstash offer what is needed; Azure's default volatile-lru spares keys without a
    // TTL, Upstash's eviction (off by default) may not; Azure Managed Redis under its Enterprise cluster policy refuses
    // a read of several keyspaces' changes (CROSSSLOT), which are then read per keyspace; Upstash pay-as-you-go bills a
    // blocking read per keyspace and application instance about every second; RedisManagedServiceTest runs the suites
    // against a service given its URI. Tested through each client library: a single server (Redis 6.2, 7, 8; Valkey
    // 7.2, 8, 9; Dragonfly 1.4, the latest 1.x, 2; Kvrocks 2.16, 2.17), also through a user with only the ACL
    // permissions listed (Kvrocks has no users); a primary with a replica behind Sentinel and a cluster of three
    // primaries with a replica each (Redis, Valkey, Kvrocks: a failover, the primary killed, in the cluster also the
    // slot moving while it writes; Redis and Valkey also a write the failover loses, also one by a primary whose clock
    // was ahead); a Dragonfly cluster of three nodes and one emulated by a single node; a restart with and without
    // persistence (Redis and Valkey, through Lettuce). Kvrocks behind Sentinel works in the tests but is not documented
    // by Kvrocks. Dragonfly behind Sentinel: a killed primary was not always failed over; in a Dragonfly cluster, after
    // a takeover, Lettuce's cluster client could not refresh its view for over a minute. Not tested: Kvrocks's
    // controller, replication without Sentinel, TLS, managed services, other servers speaking the protocol.

    private static final Logger LOGGER = System.getLogger(RedisAdapter.class.getPackageName());
    // the layout of this version (RedisKeys, RedisScripts); a cache written by another one is refused
    private static final String MARKER = "1";

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
     * @param driver       the client, wrapped by its library's entry class (such as {@link LettuceRedis#of})
     * @param keyspaceName the keyspace (letters, digits, {@code -}, {@code _}, {@code .} and {@code :}, at most 100)
     * @return the builder
     */
    public static Builder newBuilder(RedisDriver driver, String keyspaceName) {
        return new Builder(driver, keyspaceName);
    }

    @Override
    public Store createStore(StoreSettings settings) {
        RedisSession session = driver.open(settings.operationTimeout());
        try {
            RedisServer server = session.server();
            if (!server.supported()) {
                throw new StoreException(server + " is not supported; distributed-caffeine needs Redis 6.2 or later, "
                        + "Valkey, Dragonfly 1.4 or later, or Kvrocks implementing Redis 6.2 or later", false);
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
        return "Redis " + keys.name();
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
