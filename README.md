[![ci/cd](https://img.shields.io/github/check-runs/oberhoff/distributed-caffeine/main?logo=github&logoColor=ffffff&label=ci%2Fcd&style=for-the-badge&color=009539)](https://github.com/oberhoff/distributed-caffeine/actions?query=branch%3Amain)
[![maven central](https://img.shields.io/maven-central/v/io.github.oberhoff.distributed-caffeine/distributed-caffeine?logo=apachemaven&logoColor=ffffff&label=maven%20central&style=for-the-badge&color=009539)](https://central.sonatype.com/artifact/io.github.oberhoff.distributed-caffeine/distributed-caffeine)
[![javadoc](https://img.shields.io/maven-central/v/io.github.oberhoff.distributed-caffeine/distributed-caffeine?logo=openjdk&logoColor=ffffff&label=javadoc&style=for-the-badge&color=009539)](https://javadoc.io/doc/io.github.oberhoff.distributed-caffeine/distributed-caffeine)
[![license](https://img.shields.io/github/license/oberhoff/distributed-caffeine?logo=apache&logoColor=ffffff&label=license&style=for-the-badge&color=009539)](https://github.com/oberhoff/distributed-caffeine/blob/main/LICENSE)

# Distributed Caffeine

Distributed Caffeine adds distribution and a shared second level to [Caffeine](https://github.com/ben-manes/caffeine),
through a store the application may already use: MongoDB, PostgreSQL, Redis (also Valkey, Dragonfly, Kvrocks), MariaDB
or MySQL.

* **Distribution:** a change on one instance reaches all others within moments.
* **Second level:** misses are read from the shared store before the source is called.
* **Consistency:** versions keep racing writes and late messages from bringing back outdated values.
* **Compatibility:** a drop-in replacement for Caffeine's caches.

## Table of contents

* [Introduction](#introduction)
* [Usage](#usage)
* [Remarks](#remarks)
* [Requirements](#requirements)
* [Installation](#installation)

## Introduction

Each instance of a cache has a local Caffeine cache of its own (first level, L1): a hit is answered locally, and
Caffeine's eviction keeps a [near optimal](https://github.com/ben-manes/caffeine/wiki/Efficiency) hit ratio. Behind it,
all instances share a store (second level, L2):

* A read that finds nothing in the local cache reads the store, and if the store holds nothing either, the loader or
  mapping function, if any.
* A write goes to the store first, then into the local cache.
* A change made on one instance — a write, an invalidation or an expiry — makes the other instances drop or replace
  their copy of the key, or leaves it alone, depending on the
  [distribution mode](#configuration-of-the-distribution-mode).

Every write carries a version, so a slow load, a late message or a racing write never overwrites a newer value. With
distribution on, no instance serves an outdated copy for longer than distributing the change takes. If distribution
stops, an instance notices within about two heartbeat intervals and checks its copies against the store. The store is
bounded by a maximum age and a maximum size, and no instance keeps a copy beyond the maximum age.

If the store stops answering, the instances keep serving what they hold and loads go straight to the source, while
writes fail; they recover by themselves once the store answers again. Changes that may have been missed meanwhile make
the instances check what they hold against the store.

## Usage

### Adapters

An adapter tells a cache where it lives in the store. Each store has one, and each supports every distribution mode, via
the store's own way of announcing changes or by polling:

| Store                                   | Adapter                          | Distribution via                 | Details                                                                                                   |
|-----------------------------------------|----------------------------------|----------------------------------|-----------------------------------------------------------------------------------------------------------|
| MongoDB                                 | `MongoAdapter`                   | change streams                   | [MongoDB adapter](src/main/java/io/github/oberhoff/distributedcaffeine/adapter/mongodb/README.md)         |
| PostgreSQL                              | `PostgresAdapter`                | `LISTEN`/`NOTIFY`                | [PostgreSQL adapter](src/main/java/io/github/oberhoff/distributedcaffeine/adapter/postgresql/README.md)   |
| Redis (also Valkey, Dragonfly, Kvrocks) | `RedisAdapter`                   | Redis Streams                    | [Redis adapter](src/main/java/io/github/oberhoff/distributedcaffeine/adapter/redis/README.md)             |
| MariaDB, MySQL                          | `MariaDbAdapter`, `MySqlAdapter` | polling (each second by default) | [MariaDB and MySQL adapters](src/main/java/io/github/oberhoff/distributedcaffeine/adapter/jdbc/README.md) |

Every adapter is built from the client or data source the application already uses, and a name for the place of the
cache (a collection, a table or a keyspace). Several caches can share that place, told apart by a discriminator. The
collection, table or keys are created as needed, and a setup that cannot work is refused right away with an explanation.

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

The key and value types of a cache come from the adapter's variable, as above, or from the variable the cache is
assigned to, so an adapter can also be built inline within `DistributedCaffeine.newBuilder(...)`. A hash provider or
serializers that work with the key or value type need the typed adapter variable.

### Distributed Caffeine caches

The caches are represented by `DistributedCache`, `DistributedLoadingCache`, `DistributedAsyncCache` and
`DistributedAsyncLoadingCache`, which extend Caffeine's `Cache`, `LoadingCache`, `AsyncCache` and `AsyncLoadingCache`
and offer the same usage: a built cache can replace a plain Caffeine cache wherever one is used (drop-in replacement).
Where a method behaves differently, its Javadoc says so. Please refer to the official
[Caffeine documentation](https://github.com/ben-manes/caffeine/wiki) for use and integration.

#### Minimal configuration

A cache is configured with a builder returned by `DistributedCaffeine.newBuilder(adapter)` and built by one of its
`build(...)` or `buildAsync(...)` methods. The store's maximum age and maximum size are required.

```java
DistributedLoadingCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withStore(store -> store
                .withMaximumAge(Duration.ofHours(1))
                .withMaximumSize(1_000_000))
        .build(key -> loadExpensiveValue(key));
```

Building connects to the store and checks it, so a misconfiguration fails at once. A cache is closed with `close()`,
which releases its connections.

#### Configuration of the local cache

Caffeine's builder methods configure each instance's local cache and keep their meaning. They apply to the copies of
this instance only; the store has its own bounds. No copy is kept longer than the store's maximum age, whatever its
expiry.

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

#### Configuration of the store

Besides the required maximum age and maximum size, the store's settings have defaults that suit most uses:

* `withOperationTimeout(...)`: how long a call waits for the store before it fails (30 seconds).
* `withExecutor(...)`: the executor of the blocking store calls, such as virtual threads (a pool of daemon threads).
* `withMaintenanceInterval(...)`: how often values beyond the maximum age or size are removed (one minute).
* `withHeartbeatInterval(...)`: how often an instance checks that distribution still works (ten seconds).
* `withPollInterval(...)`: how often an instance polls MariaDB or MySQL for changes (one second).

#### Configuration of the distribution mode

The distribution mode decides what a change made elsewhere does to the copies of an instance:

* `INVALIDATE`: The copy is dropped, and the next read gets the value from the store. This is the default distribution
  mode.
* `UPDATE`: A write replaces the copy at once, which saves the next read a round trip, at one store read per write and
  instance holding the key; an invalidation or an expiry still drops it.
* `NONE`: No distribution; a change elsewhere reaches an instance only when its copy expires or is evicted.

```java
DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000_000))
        .withDistributionMode(DistributionMode.UPDATE)
        .build();
```

Note: No mode adds a key to an instance that does not hold it.

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
        .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000_000))
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
`StringSerializer.ofString()`, `ofLong()`, `ofInteger()` and `ofUUID()` store those types readably. A serializer of
one's own implements `ByteArraySerializer`, `StringSerializer` or `JsonSerializer`.

```java
DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withSerializers(serializers -> serializers
                .withKeySerializer(new JacksonSerializer<>(Key.class, false)) // as text
                .withValueSerializer(new JacksonSerializer<>(Value.class, true))) // as the store's JSON type, if any
        .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000_000))
        .build();
```

Note: Values come from a shared store, so with the default serializer whoever can write to the store can make every
instance deserialize classes of their choice. If the store's access cannot be trusted, use
`ForySerializer.requiringRegistration(...)` or `JavaObjectSerializer.allowing(...)`, which accept only the listed
classes.

#### Configuration of negative caching

Negative caching remembers, for a short time, keys whose load found nothing, so that repeated requests for a missing key
reach neither the store nor the source. A value written through any instance ends the absence at once (without
distribution, only through the same instance). It is off by default, as Caffeine caches no missing value.

```java
DistributedLoadingCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000_000))
        .withNegativeCaching(Duration.ofSeconds(30))
        .build(key -> loadExpensiveValue(key));
```

#### The distributed policy

`distributedPolicy()` gives access to the distributed side of a cache instance: its synchronization state
(`SYNCHRONIZED`, `DEGRADED` or `CLOSED`), its stats of store and synchronization activity (recorded with
`recordStats()`), and `getFromStore(...)` and `getAllFromStore(...)`, which read the store directly, bypassing the
instance's copies.

## Remarks

* All instances of a cache must be configured alike, in particular the serializers, the hashing and the store's bounds.
* Changing the key or value classes, their serializers or the hashing can make the stored values unusable: remove them
  first (`invalidateAll()`), or give the cache a new place. A serializer that stores another format (bytes, text or
  JSON) needs a new table on PostgreSQL.
* Reference-based eviction using Caffeine's
  [weak or soft references](https://github.com/ben-manes/caffeine/wiki/Eviction#reference-based) is not supported.
* `refreshAfterWrite` counts from when an instance got its copy, and a value read from the store that is already older
  is refreshed at once, so a value is refreshed by an age of at most twice the duration.
* The store is the cache's bound: there is no eviction across instances, and the first read of a key on each instance
  reads the store.
* Synchronization runs in background threads of the application. Platforms that allocate CPU only while a request is
  handled, as is common for serverless deployments, should allocate it permanently, otherwise instances fall behind
  while idle.

## Requirements

* Java 17 or newer.
* One of the stores, with its Java driver on the classpath (the drivers are optional dependencies, so only the one in
  use is needed):
    * MongoDB 4.4 or newer, as a replica set or a sharded cluster, with `org.mongodb:mongodb-driver-sync`.
    * PostgreSQL 9.6 or newer, through a data source, with `org.postgresql:postgresql`.
    * Redis 6.2 or newer, Valkey 7.2 or newer, Dragonfly 1.4 or newer, or Kvrocks 2.16 or newer, with
      `io.lettuce:lettuce-core`, `redis.clients:jedis` (8 or newer) or `org.redisson:redisson`.
    * MariaDB 10.2 or newer with `org.mariadb.jdbc:mariadb-java-client`, or MySQL 5.7 or newer with
      `com.mysql:mysql-connector-j` or `org.mariadb.jdbc:mariadb-java-client`.
* `tools.jackson.core:jackson-databind` if `JacksonSerializer` is used.

## Installation

Go to
[Distributed Caffeine on Maven Central](https://central.sonatype.com/artifact/io.github.oberhoff.distributed-caffeine/distributed-caffeine),
select your preferred build tool, copy the snippet provided and paste it into your project at the appropriate location.
Then add the driver of your store (see [Requirements](#requirements)).
