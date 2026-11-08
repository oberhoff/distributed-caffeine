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
* **Changes:** writes, invalidations and expiries in the store make the other instances drop, replace or keep their
  copy, depending on the [distribution mode](#configuration-of-the-distribution-mode). Local expiry and eviction affect
  only that instance.

With distribution on, an instance serves an outdated copy only until the change reaches it. If distribution stops, the
instances notice and check their copies against the store until it works again.

If the store stops answering, the instances keep serving what they hold, misses load straight from the source (what the
loader or mapping function reads), and writes fail. Once the store answers again, the instances recover by themselves
and check what they hold against the store, for changes they may have missed.

## Usage

### TL;DR

Build an adapter for the store, then the cache as with Caffeine, and use it as a Caffeine cache:

```java
MongoAdapter<String, Value> adapter = MongoAdapter.newBuilder(mongoClient, databaseName, collectionName)
        .build();

DistributedLoadingCache<String, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withCaffeine(caffeine -> caffeine.maximumSize(10_000)) // the local cache of each instance
        .withStore(store -> store.withMaximumAge(Duration.ofHours(1))) // the shared store
        .build(key -> loadExpensiveValue(key)); // called on a miss in both

Value value = cache.get(key); // local cache, then the store, then the loader
cache.invalidate(key); // gone on every instance
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
cache (a collection, a table or a keyspace). Instances with the same place and discriminator share one cache. Different
caches can use the same place with different discriminators. The collection, table or keyspace is created as needed,
and a setup that cannot work is refused right away with an explanation.

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
and offer the same usage. Integration is as with plain Caffeine: wherever an application or a framework takes a
Caffeine cache, it takes a distributed one, built with `DistributedCaffeine.newBuilder(adapter)` instead of
`Caffeine.newBuilder()`. A distributed cache behaves as Caffeine's except where it has to reach the store, which
matters when replacing one:

* `getIfPresent` and `getAllPresent` read the store on a local miss, so they can wait up to the operation timeout. The
  future the asynchronous `getIfPresent` returns is never `null`; it completes with `null` when nothing is found.
* Calls that need the store throw the unchecked `StoreException` while it does not answer, and a write whose outcome is
  unknown may too. `AsyncCache.put(key, future)` cannot report such a failure, so it is logged.
* The sizes, iteration and stats concern this instance's copies only, and eviction stays local.
* Eviction by weak and soft references is not supported.

Where a method behaves differently, its Javadoc says so. For everything else, the official
[Caffeine documentation](https://github.com/ben-manes/caffeine/wiki) applies, including how frameworks integrate a
Caffeine cache.

#### Minimal configuration

A cache is configured with a builder returned by `DistributedCaffeine.newBuilder(adapter)` and built by one of its
`build(...)` or `buildAsync(...)` methods.

```java
DistributedLoadingCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .build(key -> loadExpensiveValue(key));
```

Note: Building connects to the store and checks it, so a misconfiguration fails at once. A cache is closed with
`close()`, which releases its connections and threads. That is optional for a cache that lives as long as the
application, but needed for one discarded earlier.

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
  expensive for the working set. Requires a maximum age, distribution, and a heartbeat interval of at most a fifth of
  the maximum age (so with the default ten seconds, a maximum age of 50 seconds or more).
* `withMaximumSize(...)`: how many values the store keeps: trimmed each maintenance run to the most recently written
  ones, so it can briefly hold more; not Caffeine's frequency-based eviction (default: unbounded).
* `withOperationTimeout(...)`: how long a call waits for the store before it fails (default: 10 seconds).
* `withExecutor(...)`: the executor of the blocking store calls, such as virtual threads (default: daemon threads).
* `withMaintenanceInterval(...)`: how often values beyond the maximum age or size are removed (default: one minute).
* `withHeartbeatInterval(...)`: how often an instance checks that distribution still works (default: ten seconds). A
  stop is noticed within about two intervals, after which the instance checks its copies against the store until
  distribution works again.
* `withPollInterval(...)`: how often an instance polls MariaDB or MySQL for changes (default: one second).

#### Configuration of the distribution mode

The distribution mode decides what a change made elsewhere does to the copies of an instance:

* `INVALIDATE` (the default): a change elsewhere drops the copy.
* `UPDATE`: a write elsewhere replaces the copy; an invalidation or an expiry drops it.
* `NONE`: changes are not distributed; a copy stays until it expires or is evicted.

A dropped copy is read again from the store on its next access, or else loaded by the loader or mapping function
(if any).

```java
DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withDistributionMode(DistributionMode.UPDATE)
        .build();
```

Note: No mode creates a copy on an instance that doesn't hold the key yet; `UPDATE` only refreshes existing copies.

#### Configuration of hashing (for keys)

Keys are identified across instances by a hash. Keys of type `String`, `Long`, `Integer` or `UUID` are hashed out of the
box; other keys need one of the two ways below, or their first use fails with an `IllegalStateException`.

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

Negative caching remembers, for a short time, keys whose load found nothing (no value in the store, and the loader or
mapping function returned `null`), so that repeated requests for a missing key reach neither the store nor the source.
A value written through any instance ends the absence at once (without distribution, only through the same instance);
a value that appears in the source only is found once the absence expired. While distribution is interrupted, an
absence is checked against the store, as copies are. An absence belongs to the key: it answers every later load of it,
whichever loader or mapping function that load would call. It is off by default, as Caffeine caches no missing value.

```java
DistributedLoadingCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withNegativeCaching(Duration.ofSeconds(30))
        .build(key -> loadExpensiveValue(key));
```

#### The distributed policy

`distributedPolicy()` gives access to the distributed side of a cache instance: its synchronization state
(`SYNCHRONIZED`, `DEGRADED` or `CLOSED`), its stats of store, synchronization and maintenance activity (only recorded
with `recordStats()`), and `getFromStore(...)` and `getAllFromStore(...)`, which read the store directly, bypassing the
instance's copies.

## Remarks

#### Local and shared

* **Local only:** `estimatedSize()`, the sizes and iteration of `asMap()` and its views, `stats()`, `cleanUp()` and
  `policy()` report only this instance's copies.
* **Shared:** changes through the views or `policy()`, `invalidateAll()` and `asMap().clear()` reach every instance.
* **Eviction:** not shared; each local cache evicts by its own bounds, the store by its own.
* **First reads:** the first read of a key on an instance reads the store; `getAll(keys)` warms an instance up with one
  store read.

#### Consistency

* **Outdated copies:** a copy can be outdated until the change reaches its instance, and at the latest until its local
  expiry or the store's maximum age.
  <details><summary>Details</summary>

  A copy is outdated at most until the first of:
    * the change reaches its instance: within milliseconds with MongoDB, PostgreSQL and Redis, within about one poll
      interval with MariaDB and MySQL, and never if its message is lost unnoticed or the distribution mode is `NONE`;
    * its local expiry (Caffeine's `expireAfterWrite` or `expireAfter`);
    * the store's maximum age: once the value reaches it, the next maintenance run expires it on every instance, at
      most about one maintenance interval later; a copy that misses this expires at the latest the maximum age after
      its instance got it.

  Without local expiry and maximum age, a copy whose change never reaches its instance stays outdated for good.
  </details>
* **Changes only through the cache:** a change made in the store by hand (a `DELETE`, a `TRUNCATE`, a `DEL`) reaches
  no instance; use `invalidate(...)` or `invalidateAll()`.
* **Atomic operations:** `compute`, `merge`, `putIfAbsent`, `replace` and their kin are atomic across instances. Loads
  are shared within an instance only: instances missing the same key at once each call the source, and the value
  written first is kept.
* **`invalidateAll()`:** operations still running on other instances can write after it.
  <details><summary>Details</summary>

  `invalidateAll()` is not atomic with operations already running on other instances. Two kinds can write their result
  after it: a load that read the source before it, and an atomic `asMap()` method (`compute`, `merge`, `replace`, ...)
  that decided on a value present before it. Such a result may even be older than a write made before the
  `invalidateAll()`; it stays until the key is written again or reaches the store's maximum age. Caffeine, too, leaves
  loads in flight during `invalidateAll()` undefined.
  </details>
* **Refresh:** `refreshAfterWrite` counts from when an instance got its copy, so a value is refreshed at an age of up to
  twice the duration. Instances refreshing a key at once each call the loader; the first result written wins.

#### Failures

* **Store not answering:** reads keep working, writes fail, and the instances recover by themselves once it answers
  again.
  <details><summary>Details</summary>

    * Reads serve the copies held. A miss calls the loader or mapping function, if there is one, and otherwise stays a
      miss; a refresh calls the loader as usual. A loaded value is kept locally without being written, and checked
      against the store once it answers.
    * Atomic `asMap()` methods that would write nothing for a held key (`putIfAbsent` on a present key, `replace` or
      `remove` whose value does not match) answer from the held value.
    * Writes and removals fail with the unchecked `StoreException` (`io.github.oberhoff.distributedcaffeine.spi`); a
      removal drops the local copy first. `getFromStore` and `getAllFromStore` fail too. `AsyncCache.put(key, future)`
      cannot report the failure to its caller: the key is left absent, and the failure is logged.
    * Loads go on while writes fail because a loaded value comes from the source, so any instance can load it again,
      while a written value or a removal exists only in the cache: kept locally, it would be lost, or the other
      instances would serve the old value without an error.
    * After three failures in a row, calls to the store are refused at once instead of waiting, and one call per
      second checks whether it answers again (`refusedCount`, `guardOpenCount`).
    * `distributedPolicy().getSynchronizationState()` cannot tell beforehand whether a write will succeed: `DEGRADED`
      also covers interrupted distribution, and the store can fail right after the check. A caller that must not fail
      catches the exception: `try { cache.put(key, value); } catch (StoreException e) { ... }`.
  </details>
* **Unknown write outcome:** if the store may or may not have applied a write, the key is dropped locally, and the
  write is either repeated or reported.
  <details><summary>Details</summary>

  This happens on a timeout or on a connection lost after sending. `put` throws a `StoreException` whose
  `isOutcomeUnknown()` is `true`, since repeating it could overwrite a newer value. The atomic `asMap()` methods repeat
  the write and throw only if they cannot recognize their own write (on stores that assign versions themselves, or
  when another write raced), since deciding again would apply the remapping twice. Loads and refreshes repeat the
  write.
  </details>

#### Operations

* **Same configuration:** all instances of a cache need the same store bounds, serializers and hashing. After changing
  the key or value classes, their serializers or the hashing, remove the stored values (`invalidateAll()`) or give the
  cache a new place; on PostgreSQL, a serializer storing another format always needs a new place.
* **Rolling updates:** a value an instance cannot read counts as a miss and is loaded again (`unreadableRowCount`), so
  a rolling update keeps working, at the cost of loads.
* **Clocks:** keep them synchronized (NTP). Correctness does not depend on them, but a clock running ahead costs writes
  one more round trip (`versionRefusalCount`, `clockAheadCount`).
* **Connection pools:** keep a JDBC pool's wait for a connection (HikariCP's `connectionTimeout`, 30 seconds by
  default) at most the operation timeout (10 seconds by default). The PostgreSQL listener and Jedis each hold one
  connection for good (see the adapter READMEs).
* **Logging:** through `System.Logger`, under `io.github.oberhoff.distributedcaffeine`. Add your logging framework's
  bridge: `org.slf4j:slf4j-jdk-platform-logging` for SLF4J, `org.apache.logging.log4j:log4j-jpl` for Log4j.
* **CPU:** synchronization runs in background threads, so platforms that allocate CPU only while a request is handled
  (common for serverless deployments) need it allocated permanently.

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
