# Redis adapter

Details of the Redis adapter beyond its configuration, which the [main README](../../../../../../../../../README.md)
describes. It serves Redis (also Valkey, Dragonfly, Kvrocks). Every operation runs as one Lua script, and distribution
uses Redis Streams. Building a cache fails right away with an explanation if the setup cannot support it.

## Client libraries

The adapter works through the application's client, wrapped by the entry class of its library; only the library in use
has to be on the classpath:

| Library  | Entry class     | Clients                                                     |
|----------|-----------------|-------------------------------------------------------------|
| Lettuce  | `LettuceRedis`  | `RedisClient` (single server or Sentinel), `RedisClusterClient` |
| Jedis 8+ | `JedisRedis`    | `RedisClient`, `RedisSentinelClient`, `RedisClusterClient`  |
| Redisson | `RedissonRedis` | `RedissonClient` (single server, Sentinel or cluster)       |

The client and its options (URI, TLS, credentials) stay the application's; what the cache opens through it, it closes
again. Each command is bounded by the cache's operation timeout, also where caches with different timeouts share a
client.

* Lettuce: The cache opens connections of its own through the client. In a cluster, Lettuce follows slot moves and
  failovers through adaptive topology refresh, on by default since Lettuce 7 (before, enable it in
  `ClusterTopologyRefreshOptions`).
* Jedis: The cache borrows a connection from the client's pool per command, and holds one for as long as it listens for
  changes, so the pool needs one connection more (in a cluster, the pool of the node holding the keyspace).
* Redisson: Its own timeouts and retries apply within the cache's operation timeout. Redisson replaces a connection the
  network silently stopped carrying only when its ping fails (`pingConnectionInterval`, 30 seconds by default); until
  then, commands on it time out. A shorter interval, such as one second, recovers sooner.

## Topologies

A single server, a primary behind Sentinel, or a cluster through the library's cluster client. A replica is refused,
since its writes would fail or vanish, and so is a plain client of one node of a cluster, which would reach only that
node's slots.

A keyspace is a hash tag (`{dc:<keyspace>}`), so in a cluster all caches of a keyspace live in one slot, on one node,
and each application instance reads their changes with one connection; caches of different keyspaces spread over the
nodes. On a single server or behind Sentinel, all caches of a client share one connection for their changes.

## Memory and persistence

The cache's keys must never be evicted under memory pressure: set `maxmemory-policy` to `noeviction` or a `volatile-*`
policy, which spares the cache's keys (they have no TTL), and do not run Dragonfly with `--cache_mode`. An evicting
server is logged as a warning when the cache is built. Without persistence, a restart of the server loses the stored
values; the instances notice and drop what they hold, which costs loads, never a wrong answer.

## Dragonfly

Start Dragonfly with `--lock_on_hashtags`. Each of the cache's scripts touches several keys, which Dragonfly locks one
by one across its threads by default; with the flag, the keys of a keyspace are locked together, which makes writes
about twice as fast and keeps a hot key from slowing down by orders of magnitude. The cache stays correct either way.

Not supported: Dragonfly behind Sentinel, and failovers in a Dragonfly cluster.

## Access control

A user restricted by ACL needs, on the keys `{dc:*}` and no channels: `EVALSHA`, `EVAL`, `XREAD`, `INFO`, and for the
scripts `TIME`, `GET`, `SET`, `DEL`, `INCR`, `HGET`, `HMGET`, `HSET`, `HDEL`, `XADD`, `XREVRANGE`, `ZADD`, `ZCOUNT`,
`ZRANGEBYSCORE`, `ZREM`; besides what the client library sends on connecting (`HELLO`, `AUTH`, `PING`, `SELECT`,
`CLIENT SETNAME`, `CLIENT SETINFO`). `SCRIPT LOAD` is optional: where it is refused, a script the server lost is sent in
full.

## Managed services

By their documentation, Amazon ElastiCache (Serverless included), Azure Cache for Redis and Upstash offer what is needed
(not tested against them yet). Their eviction policy must spare keys without a TTL: Azure's default, `volatile-lru`,
does; Upstash's eviction, off by default, may not. Where every command is billed (Upstash pay-as-you-go), reading the
changes costs a blocking read per keyspace and application instance about every second.
