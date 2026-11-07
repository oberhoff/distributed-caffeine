[![ci/cd](https://img.shields.io/github/check-runs/oberhoff/distributed-caffeine/main?logo=github&logoColor=ffffff&label=ci%2Fcd&style=for-the-badge&color=009539)](https://github.com/oberhoff/distributed-caffeine/actions?query=branch%3Amain)
[![maven central](https://img.shields.io/maven-central/v/io.github.oberhoff.distributed-caffeine/distributed-caffeine?logo=apachemaven&logoColor=ffffff&label=maven%20central&style=for-the-badge&color=009539)](https://central.sonatype.com/artifact/io.github.oberhoff.distributed-caffeine/distributed-caffeine)
[![javadoc](https://img.shields.io/maven-central/v/io.github.oberhoff.distributed-caffeine/distributed-caffeine?logo=openjdk&logoColor=ffffff&label=javadoc&style=for-the-badge&color=009539)](https://javadoc.io/doc/io.github.oberhoff.distributed-caffeine/distributed-caffeine)
[![license](https://img.shields.io/github/license/oberhoff/distributed-caffeine?logo=apache&logoColor=ffffff&label=license&style=for-the-badge&color=009539)](https://github.com/oberhoff/distributed-caffeine/blob/main/LICENSE)

# Distributed Caffeine

Distributed Caffeine adds distribution and a shared second level to [Caffeine](https://github.com/ben-manes/caffeine),
through a store the application may already use: MongoDB, PostgreSQL, Redis (also Valkey, Dragonfly, Kvrocks), MariaDB
or MySQL.

* **Distribution:** a change on one instance reaches the copies on all others.
* **Second level:** a miss reads the shared store before calling the source.
* **Compatibility:** Caffeine's cache interfaces, configured with Caffeine's builder methods.
* **Consistency:** versions keep racing writes, slow loads and late messages from bringing back outdated values.
* **Resilience:** if the store fails, instances keep serving what they hold and recover by themselves.

## Table of contents

* [Introduction](#introduction)
* [Usage](#usage)
* [Remarks](#remarks)
* [Requirements](#requirements)
* [Installation](#installation)

## Introduction

Each instance has its own local Caffeine cache (first level, L1), which answers hits with Caffeine's
[near optimal](https://github.com/ben-manes/caffeine/wiki/Efficiency) hit ratio. All instances share a store (second
level, L2):

* **Reads:** the local cache first, then the store, then the loader or mapping function (if any).
* **Writes:** the store first, then the local cache.
* **Changes:** writes, invalidations and expiries make the other instances drop, replace or keep their copy, depending
  on the [distribution mode](#configuration-of-the-distribution-mode).

With distribution on, an instance serves an outdated copy only until the change reaches it. If distribution stops, the
instances notice and check their copies against the store until it works again.

If the store stops answering, the instances keep serving what they hold, misses load straight from the source, and
writes fail. Once the store answers again, the instances recover by themselves and check what they hold against the
store, for changes they may have missed.

## Usage

### TL;DR

Build an adapter for the store, then the cache as with Caffeine, and use it as a Caffeine cache:

```java
MongoAdapter<String, Value> adapter = MongoAdapter.newBuilder(mongoClient, databaseName, collectionName).build();

DistributedLoadingCache<String, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withCaffeine(caffeine -> caffeine.maximumSize(10_000)) // the local cache of each instance
        .withStore(store -> store.withMaximumAge(Duration.ofHours(1))) // the shared store
        .build(key -> loadExpensiveValue(key)); // called on a miss in both

Value value = cache.get(key); // local cache, then the store, then the loader
cache.

invalidate(key); // gone on every instance

cache.

close(); // on shutdown, releases the cache's connections
```

The other stores differ only in the adapter (see [Adapters](#adapters)). Keys of type `String`, `Long`, `Integer` or
`UUID` and values of any type work by default; other key types need a hash (see
[Configuration of hashing](#configuration-of-hashing-for-keys)).

### Adapters

An adapter connects a cache to its store and says where in the store the cache lives. Each store below has one, and
each adapter supports every distribution mode. MongoDB, PostgreSQL and Redis announce changes themselves; MariaDB and
MySQL are polled:

| Store                                   | Adapter                          | Distribution via                 | Details                                                                                                   |
|-----------------------------------------|----------------------------------|----------------------------------|-----------------------------------------------------------------------------------------------------------|
| MongoDB                                 | `MongoAdapter`                   | change streams                   | [MongoDB adapter](src/main/java/io/github/oberhoff/distributedcaffeine/adapter/mongodb/README.md)         |
| PostgreSQL                              | `PostgresAdapter`                | `LISTEN`/`NOTIFY`                | [PostgreSQL adapter](src/main/java/io/github/oberhoff/distributedcaffeine/adapter/postgresql/README.md)   |
| Redis (also Valkey, Dragonfly, Kvrocks) | `RedisAdapter`                   | Redis Streams                    | [Redis adapter](src/main/java/io/github/oberhoff/distributedcaffeine/adapter/redis/README.md)             |
| MariaDB, MySQL                          | `MariaDbAdapter`, `MySqlAdapter` | polling (each second by default) | [MariaDB and MySQL adapters](src/main/java/io/github/oberhoff/distributedcaffeine/adapter/jdbc/README.md) |

Every adapter is built from the client or data source the application might already use, and a name for the place of the
cache (a collection, a table or a keyspace). Several caches can share that place, told apart by a discriminator. The
collection, table or keyspace is created as needed, and a setup that cannot work is refused right away with an
explanation.

```java
MongoAdapter<Key, Value> adapter = MongoAdapter.newBuilder(mongoClient, databaseName, collectionName)
        .withDiscriminator(discriminator) // optional (used if different caches share a collection)
        .build();
```

```java
PostgresAdapter<Key, Value> adapter = PostgresAdapter.newBuilder(dataSource, schemaName, tableName)
        .withDiscriminator(discriminator) // optional (used if different caches share a table)
        .build();
```

```java
// with LettuceRedis, JedisRedis or RedissonRedis, whichever client library is in use
RedisAdapter<Key, Value> adapter = RedisAdapter.newBuilder(LettuceRedis.of(redisClient), keyspaceName)
                .withDiscriminator(discriminator) // optional (used if different caches share a keyspace)
                .build();
```

```java
MariaDbAdapter<Key, Value> adapter = MariaDbAdapter.newBuilder(dataSource, tableName) // or MySqlAdapter
        .withDiscriminator(discriminator) // optional (used if different caches share a table)
        .build();
```

Note: An adapter belongs to exactly one cache: building a second cache with it fails while the first is open.

### Distributed Caffeine caches

The caches are represented by `DistributedCache`, `DistributedLoadingCache`, `DistributedAsyncCache` and
`DistributedAsyncLoadingCache`, which extend Caffeine's `Cache`, `LoadingCache`, `AsyncCache` and `AsyncLoadingCache`
and offer the same usage: a built cache can replace a plain Caffeine cache wherever one is used (drop-in replacement).
Where a method behaves differently, its Javadoc says so. Please refer to the official
[Caffeine documentation](https://github.com/ben-manes/caffeine/wiki) for use and integration.

#### Minimal configuration

A cache is configured with a builder returned by `DistributedCaffeine.newBuilder(adapter)` and built by one of its
`build(...)` or `buildAsync(...)` methods.

```java
DistributedLoadingCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .build(key -> loadExpensiveValue(key));
```

Note: Building connects to the store and checks it, so a misconfiguration fails at once. A cache is closed with
`close()`, which releases its connections.

#### Configuration of the local cache

Caffeine's builder methods configure the local cache of each instance, with their usual meaning. The store has bounds
of its own, and the two relate as follows:

* **Age:** the store's maximum age overrules a longer local expiry: a copy expires at its local expiry or at the store's
  maximum age after this instance got it, whichever comes first (unless `withMaximumAgeForStoreOnly()`).
* **Size:** the bounds are independent: only the local `maximumSize` or `maximumWeight` bounds the local cache, and the
  store pruning to its maximum size leaves the local copies alone.

```java
DistributedLoadingCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withCaffeine(caffeine -> caffeine
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofMinutes(10))
                .refreshAfterWrite(Duration.ofMinutes(5))
                .recordStats())
        .withStore(store -> store
                .withMaximumAge(Duration.ofHours(1))
                .withMaximumSize(1_000_000))
        .build(key -> loadExpensiveValue(key));
```

Note: The store's bounds are optional, as Caffeine's are, but recommended (see
[Configuration of the store](#configuration-of-the-store)).

#### Configuration of the store

The store's settings have defaults that suit most uses:

* `withMaximumAge(...)`: how long the store serves a value after its write, which bounds the local copies too (default:
  unbounded).
* `withMaximumAgeForStoreOnly()`: the maximum age bounds the store's values but not the local copies, for which only the
  local expiry counts; a miss after the maximum age calls the source (default: off). For a store too small or too
  expensive for the working set. Requires a maximum age and distribution.
* `withMaximumSize(...)`: how many values the store keeps (default: unbounded).
* `withOperationTimeout(...)`: how long a call waits for the store before it fails (default: 10 seconds).
* `withExecutor(...)`: the executor of the blocking store calls, such as virtual threads (default: daemon threads).
* `withMaintenanceInterval(...)`: how often values beyond the maximum age or size are removed (default: one minute).
* `withHeartbeatInterval(...)`: how often an instance checks that distribution still works (default: ten seconds). A
  stop is noticed within about two intervals, after which the instance checks its copies against the store until
  distribution works again.
* `withPollInterval(...)`: how often an instance polls MariaDB or MySQL for changes (default: one second).

#### Configuration of the distribution mode

The distribution mode decides what a change made elsewhere does to the copies of an instance:

* `INVALIDATE`: The copy is dropped; the next read takes the value from the store, or else from the loader or mapping
  function (if any). This is the default distribution mode.
* `UPDATE`: A write replaces the copy; an invalidation or an expiry drops it, and the next read then takes the value
  from the store, or else from the loader or mapping function (if any).
* `NONE`: No distribution; a copy changed elsewhere stays until it expires or is evicted, and the next read then takes
  the value from the store, or else from the loader or mapping function (if any).

```java
DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withDistributionMode(DistributionMode.UPDATE)
        .build();
```

Note: No mode creates a copy on an instance that doesn't hold the key yet; `UPDATE` only refreshes existing copies.

#### Configuration of hashing (for keys)

Keys are identified across instances by a hash. Keys of type `String`, `Long`, `Integer` or `UUID` are hashed out of the
box. For other types of keys two ways are supported.

If the key class cannot or should not be changed, a hash provider can be configured. The supplied hasher computes the
hash from the values of the key.

```java
DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withHashProvider((key, hasher) -> hasher.get()
                .putUUID(key.getId())
                .putLong(key.getVersion())
                .putString(key.getName())
                .getHash())
        .build();
```

Alternatively, the key class can implement the `Hashable` interface.

```java
public class Key implements Hashable {
    @Override
    public String getHash(Supplier<Hasher> hasher) {
        return hasher.get()
                .putUUID(this.id)
                .putLong(this.version)
                .putString(this.name)
                .getHash();
    }
}
```

Note: Hashing must be equivalent to key equality (`equals()`-semantics) and stable across instances, so all values
relevant for key equality (and only those) should be put into the hasher. A hash provider takes precedence over
`Hashable`.

#### Configuration of serialization

Keys and values are serialized for the store. `ForySerializer` is the default for both: it handles every type without
configuration and stores bytes, using [Apache Fory](https://github.com/apache/fory). `JacksonSerializer` stores JSON, as
text or, on MongoDB and PostgreSQL, in the store's own JSON type, using [Jackson](https://github.com/FasterXML/jackson).
`JavaObjectSerializer` uses
[Java Object Serialization](https://docs.oracle.com/en/java/javase/17/docs/specs/serialization/index.html), and
`StringSerializer.ofString()`, `ofLong()`, `ofInteger()` and `ofUUID()` store those types readably. A custom serializer
must implement `ByteArraySerializer`, `StringSerializer` or `JsonSerializer`, so the store holds its result as bytes,
text or JSON.

```java
DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withSerializers(serializers -> serializers
                .withKeySerializer(new JacksonSerializer<>(Key.class, false)) // as text
                .withValueSerializer(new JacksonSerializer<>(Value.class, true))) // as the store's JSON type, if any
        .build();
```

Note: Values come from a shared store, so with the default serializer whoever can write to the store can make every
instance deserialize classes of their choice. If the store's access cannot be trusted, use
`ForySerializer.requiringRegistration(...)` or `JavaObjectSerializer.allowing(...)`, which accept only the listed
classes.

#### Configuration of negative caching

Negative caching remembers, for a short time, keys whose load found nothing, so that repeated requests for a missing key
reach neither the store nor the source. A value written through any instance ends the absence at once (without
distribution, only through the same instance); while distribution is interrupted, an absence is checked against the
store, as copies are. An absence belongs to the key: it answers every later load of it, whichever loader or mapping
function that load would call. It is off by default, as Caffeine caches no missing value.

```java
DistributedLoadingCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withNegativeCaching(Duration.ofSeconds(30))
        .build(key -> loadExpensiveValue(key));
```

#### The distributed policy

`distributedPolicy()` gives access to the distributed side of a cache instance: its synchronization state
(`SYNCHRONIZED`, `DEGRADED` or `CLOSED`), its stats of store and synchronization activity (only recorded with
`recordStats()`), and `getFromStore(...)` and `getAllFromStore(...)`, which read the store directly, bypassing the
instance's copies.

## Remarks

* **Same configuration on every instance:** all instances of a cache must be configured alike, in particular the
  store's bounds, the serializers and the hashing. After a change of the key or value classes, their serializers or the
  hashing, the stored values may no longer be
  readable: remove them first (`invalidateAll()`), or give the cache a new place.
* **No reference-based eviction:** Caffeine's
  [weak or soft references](https://github.com/ben-manes/caffeine/wiki/Eviction#reference-based) are not supported.
* **What is local and what is shared:**
    * Only this instance's copies are reported by `estimatedSize()`, the sizes and iteration of `asMap()` and its
      views, `stats()`, `cleanUp()` and `policy()`.
    * Every instance is reached by changes through the views or `policy()`, by `invalidateAll()` and by
      `asMap().clear()`.
    * Eviction is not shared: each local cache evicts by its own bounds, the store by its own.
    * The first read of a key on an instance reads the store; `getAll(keys)` warms an instance up with one store read.
* **Refresh timing:** `refreshAfterWrite` counts from when an instance got its copy, not from the write. A copy that is
  already older when read from the store is refreshed at once, so a value is refreshed at an age of at most twice the
  duration. Instances refreshing a key at the same time each call the loader; the first result written wins.
* **Invalidating everything:** `invalidateAll()` is not atomic with operations in flight on other instances. Two kinds
  can write their result after it:
    * a load that read the source before it;
    * an atomic `asMap()` method (`compute`, `merge`, `replace`, ...) that decided on a value present before it.

  Such a result can even be older than a write made before the `invalidateAll()`. It stays until the key is written
  again or reaches the store's maximum age. Caffeine, too, leaves loads in flight during `invalidateAll` undefined.
* **Store not answering:** the cache answers what it can without the store, and the rest fails with a
  `StoreException`:
    * Reads serve held values. A miss is loaded from the loader or mapping function, if there is one, or else stays a
      miss; a refresh calls the loader as usual. A loaded value is held unconfirmed, not written, and checked against
      the store once it answers again.
    * Atomic `asMap()` methods that write nothing for a held key (`putIfAbsent` on a present key, `replace` or `remove`
      whose value does not match) answer from the held value.
    * Writes and removals fail (a removal drops the local copy first), and so do `getFromStore` and `getAllFromStore`.
      `AsyncCache.put(key, future)` cannot tell its caller: the key is absent here, and the failure is logged.
    * Why loads succeed and writes fail: a loaded value is a copy of the source, which any instance can load again. A
      written value or a removal exists only in the cache; kept locally, it would be lost, or the other instances
      would serve the old value without an error.
    * After repeated failures, calls to the store are refused at once instead of waiting for it.
    * `distributedPolicy().getSynchronizationState()` cannot tell beforehand whether a write will succeed: `DEGRADED`
      also covers an interrupted change feed, and the store can fail right after the check. A caller that must not
      fail catches the exception: `try { cache.put(key, value); } catch (StoreException e) { ... }`.
* **Unknown write outcome:** a write whose outcome the store leaves unknown (a timeout, or a connection lost after
  sending) drops the key locally. The call may throw a `StoreException` whose `isOutcomeUnknown()` is `true`:
    * `put` throws, since repeating it could overwrite a newer value.
    * The atomic `asMap()` methods (`putIfAbsent`, `replace`, `compute`, `merge`, ...) repeat the write. They throw
      only if they cannot recognize their own write (on stores that assign versions themselves, or when another write
      raced), since deciding again would apply the remapping twice.
    * Loads and refreshes repeat the write.
* **CPU allocation:** synchronization runs in background threads of the application. Platforms that allocate CPU only
  while a request is handled (common for serverless deployments) need it allocated permanently, or idle instances fall
  behind.

## Requirements

* Java 17 or newer.
* One of the stores, with its Java driver on the classpath (the drivers are optional dependencies, so only the one in
  use is needed):
    * MongoDB 4.4 or newer, as a replica set or a sharded cluster, with `org.mongodb:mongodb-driver-sync` (5.2 or
      newer).
    * PostgreSQL 9.6 or newer, through a data source, with `org.postgresql:postgresql` (42.7.6 or newer).
    * Redis 6.2 or newer, Valkey 7.2 or newer, Dragonfly 1.4 or newer, or Kvrocks 2.16 or newer, with
      `io.lettuce:lettuce-core` (7.8 or newer), `redis.clients:jedis` (8 or newer) or `org.redisson:redisson` (4 or
      newer).
    * MariaDB 10.2 or newer with `org.mariadb.jdbc:mariadb-java-client` (3 or newer).
    * MySQL 5.7 or newer with `com.mysql:mysql-connector-j` (8 or newer) or `org.mariadb.jdbc:mariadb-java-client` (3.1
      or newer).
* For `JacksonSerializer`: `tools.jackson.core:jackson-databind` (3 or newer).

## Installation

Go to
[Distributed Caffeine on Maven Central](https://central.sonatype.com/artifact/io.github.oberhoff.distributed-caffeine/distributed-caffeine),
select your preferred build tool, copy the snippet provided and paste it into your project at the appropriate location.
Then add the driver of your store (see [Requirements](#requirements)).
