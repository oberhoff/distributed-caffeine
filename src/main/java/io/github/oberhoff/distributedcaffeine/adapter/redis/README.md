# Redis adapter

Details of the Redis adapter beyond its configuration, which the [main README](../../../../../../../../../README.md)
describes. It serves Redis (also Valkey, Dragonfly, Kvrocks). Every operation runs as one Lua script, and distribution
uses Redis Streams (versions: see
[Requirements](../../../../../../../../../README.md#requirements)). A server without Lua scripts that call stream commands, such
as Garnet, is refused. Building a cache fails right away with an explanation if the setup cannot support it.

## Client libraries

The adapter works through the application's client, wrapped by the entry class of its library; only the library in use
has to be on the classpath:

| Library  | Entry class     | Clients                                                         |
|----------|-----------------|-----------------------------------------------------------------|
| Lettuce  | `LettuceRedis`  | `RedisClient` (single server or Sentinel), `RedisClusterClient` |
| Jedis 8+ | `JedisRedis`    | `RedisClient`, `RedisSentinelClient`, `RedisClusterClient`      |
| Redisson | `RedissonRedis` | `RedissonClient` (single server, Sentinel or cluster)           |

The client and its options (URI, TLS, credentials) stay the application's; what the cache opens through it, it closes
again. Each command is bounded by the cache's operation timeout, also where caches with different timeouts share a
client.

* Lettuce: The cache opens connections of its own through the client. In a cluster, Lettuce follows slot moves and
  failovers through adaptive topology refresh, on by default.
* Jedis: The cache borrows a connection from the client's pool per command, and holds one for as long as it listens for
  changes, so the pool needs one connection more (in a cluster, the pool of the node holding the keyspace). A command
  whose connection broke, such as one the server closed while it sat in the pool after a restart, is sent once more on
  another, as Lettuce and Redisson do. Its sockets block and have no write timeout: a write too large for the socket's
  buffer, such as a large `putAll`, sent to a server that vanished without closing the connection, waits until the
  operating system gives up (about 15 minutes on Linux, `net.ipv4.tcp_retries2`). Lettuce and Redisson bound it.
  A `RedisSentinelClient` or `RedisClusterClient` cannot be created while its servers are out (its constructor
  connects and throws), so an application starting during such an outage fails before any cache exists; create the
  client where that failure is retried, or use Lettuce or Redisson, whose clients are created without a server.
* Redisson: Its own timeouts and retries apply within the cache's operation timeout. Redisson replaces a connection the
  network silently stopped carrying only when its ping fails (`pingConnectionInterval`, 30 seconds by default); until
  then, commands on it time out. A shorter interval, such as 1 second, recovers sooner. `Redisson.create(...)`
  connects to the servers and throws while they are out; with `Config#setLazyInitialization(true)` the client is
  created without them and connects on its first command, within which it tries again on its own (`retryAttempts`,
  `retryInterval`), so building a cache while the servers are out waits for those attempts (about 16 seconds by
  default) before the cache starts without its store.

## Names

The keyspace name has 1 to 100 letters, digits, `-`, `_`, `.` and `:` (no braces, which would break its hash tag);
the discriminator has 1 to 100 letters, digits, `-`, `_` and `.` (no colon, so that one cache's keys never begin with
another's). A name outside these is rejected when the adapter is built.

## Topologies

A single server, a primary behind Sentinel, or a cluster through the library's cluster client. A plain client of one
node of a cluster is refused, since it would reach only that node's slots. A replica is refused too, since its writes
would fail or vanish, but as retryable: behind Sentinel it is a failover in progress, so a cache built then starts
without its store and connects once the primary answers (with `withFailFast()` the build fails). A plain client of a
replica's address keeps the cache `DEGRADED` (logged every 5 minutes) until its address reaches a primary.

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

## Kvrocks

Kvrocks keeps its data on disk (in RocksDB) and evicts nothing, so the advice on memory above does not apply; it has no
ACL users either, only a password. Behind Sentinel it works (tested, though Kvrocks does not document it). A Kvrocks
cluster takes its layout, and its failovers, from a controller such as `kvrocks-controller`; the cache follows them as
in any cluster (tested with the layout pushed by the tests, not with `kvrocks-controller` itself).

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
