[![ci/cd](https://img.shields.io/github/check-runs/oberhoff/distributed-caffeine/main?logo=github&logoColor=ffffff&label=ci%2Fcd&style=for-the-badge&color=009539)](https://github.com/oberhoff/distributed-caffeine/actions?query=branch%3Amain)
[![maven central](https://img.shields.io/maven-central/v/io.github.oberhoff.distributed-caffeine/distributed-caffeine?logo=apachemaven&logoColor=ffffff&label=maven%20central&style=for-the-badge&color=009539)](https://central.sonatype.com/artifact/io.github.oberhoff.distributed-caffeine/distributed-caffeine)
[![javadoc](https://img.shields.io/maven-central/v/io.github.oberhoff.distributed-caffeine/distributed-caffeine?logo=openjdk&logoColor=ffffff&label=javadoc&style=for-the-badge&color=009539)](https://javadoc.io/doc/io.github.oberhoff.distributed-caffeine/distributed-caffeine)
[![license](https://img.shields.io/github/license/oberhoff/distributed-caffeine?logo=apache&logoColor=ffffff&label=license&style=for-the-badge&color=009539)](https://github.com/oberhoff/distributed-caffeine/blob/main/LICENSE)

# Distributed Caffeine

Distributed Caffeine adds distribution and a shared second level to [Caffeine](https://github.com/ben-manes/caffeine),
through a store the application may already use: MongoDB, PostgreSQL, Redis (also Valkey, Dragonfly, Kvrocks), MariaDB
or MySQL.

* **Distribution:** a change made on one instance reaches the copies held by all other instances.
* **Second level:** a local miss is answered from the shared store, or else from the source.
* **Compatibility:** configured and used like any Caffeine cache: the same builder methods, the same interfaces.
* **Consistency:** all instances end up with the same, latest value of every key, even when writes and loads race.
* **Resilience:** local copies keep being served when the store or distribution fails; instances recover automatically.

## Table of contents

* [Introduction](#introduction)
* [Usage](#usage)
* [Remarks](#remarks)
* [Requirements](#requirements)
* [Installation](#installation)

## Introduction

Each instance has its own local Caffeine cache (first level, L1), which answers hits with Caffeine's
[near optimal](https://github.com/ben-manes/caffeine/wiki/Efficiency) hit ratio. All instances share a store (second
level, L2). Behind both is the source, where values come from: the loader or mapping function, if the cache has one;
otherwise values enter only through writes such as `put`. The two levels split the work:

1. **The store holds the values.** Its maximum age decides how long a value is valid, on every instance; its maximum
   size, how many values it keeps.
2. **Each instance holds copies.** Caffeine's settings decide which copies it keeps and for how long, never beyond
   their values' validity.
3. **When a copy goes, the instance asks the store,** and only if the store has no valid value, the source.

So a local expiry or eviction sends an instance back to the store; only the store's maximum age makes a value go
everywhere, so that the next read calls the source. In detail:

* **Reads:** the local cache first, then the store, then the source. A value loaded from the source is written to the
  store for the other instances.
* **Writes:** the store first, then the local cache, removals included. A written value the store did not take is
  never cached.
* **Changes:** a write or invalidation on one instance makes the other instances drop, replace or keep their copy,
  depending on the [distribution mode](#distribution-mode). Local expiry and eviction affect only that instance.
* **Storage:** keys and values are [serialized](#serialization) for the store, and keys identified by a
  [hash](#hashing-for-keys); the store keeps them until its maximum age or size, if set (both unbounded by default).

With distribution, a change made on one instance reaches the others shortly after; until it has arrived there, they
still serve the previous value. If the distribution or the store fails, the instances keep serving what they hold and
recover automatically; only writes fail while the store does not answer. The details are under [Remarks](#remarks).

## Usage

### TL;DR

Configure and build an adapter for the store, configure and build the cache with the adapter, then use the built cache
like any Caffeine cache:

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
`UUID` and values of any type work by default; other key types need a hash (see [Hashing](#hashing-for-keys)).

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

Every adapter is built from the client or data source the application might already use, and the place of the cache: a
collection, a table or a keyspace, created as needed. Instances with the same place share one cache; several caches can
share one place, each with its own discriminator. A setup that cannot work is refused right away with an explanation.

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

### Caches

#### Building and closing

A cache is configured with the builder from `DistributedCaffeine.newBuilder(adapter)` instead of
`Caffeine.newBuilder()` and built as in Caffeine:

| Build method         | Cache                          | Extends             |
|----------------------|--------------------------------|---------------------|
| `build()`            | `DistributedCache`             | `Cache`             |
| `build(loader)`      | `DistributedLoadingCache`      | `LoadingCache`      |
| `buildAsync()`       | `DistributedAsyncCache`        | `AsyncCache`        |
| `buildAsync(loader)` | `DistributedAsyncLoadingCache` | `AsyncLoadingCache` |

Every setting is optional. Without any, both levels are unbounded and values never expire, as with
`Caffeine.newBuilder()`, so most caches set bounds (see [Configuration](#configuration) and
[Settings by goal](#settings-by-goal)).

```java
DistributedLoadingCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .build(key -> loadExpensiveValue(key));
```

Note: Building checks the store, and a store that answers but cannot be used fails it at once; one that cannot be
reached does not (see [Failures](#failures)). A cache is closed with `close()`, which releases its connections and
threads. That is optional for a cache that lives as long as the application, but needed for one discarded earlier.

#### Differences from Caffeine

A distributed cache can be used wherever an application or a framework expects a Caffeine cache. It behaves as
Caffeine's except where it has to reach the store:

* Local lookups in Caffeine, such as `getIfPresent`, `getAllPresent`, `asMap().get(key)` or `asMap().containsKey(key)`,
  read the store on a local miss; only `policy().getIfPresentQuietly(key)` looks at the local copy alone.
* The asynchronous `getIfPresent` never returns `null`, as Caffeine's does when nothing is held: it returns a future,
  which completes with `null` if there is no value.
* Writes and removals, `invalidate` and `invalidateAll()` included, go to the store and, with distribution, to every
  instance; only eviction and expiry by the local bounds stay local.
* With distribution, the removal listener also reports copies dropped or replaced by changes made on other instances.
* Sizes, iteration and stats cover only the copies of the instance they are called on. In Caffeine's stats, a value
  found in the store counts as a miss and a load (see [Distributed policy](#distributed-policy)).
* While the store does not answer, reads go on from what is held or, on a miss, from the source; writes and removals
  throw the unchecked `StoreException`.
* Eviction by weak and soft references is not supported.

Where a method behaves differently, its Javadoc says so. For everything else, the official
[Caffeine documentation](https://github.com/ben-manes/caffeine/wiki) applies, including how frameworks integrate a
Caffeine cache.

### Configuration

#### Local cache

Caffeine's builder methods configure the local cache of each instance. They keep their meaning for the local copies,
but a copy that goes sends the instance back to the store, not to the source. The store has bounds of its own, and the
two relate as follows:

* **Age:** a copy expires at its local expiry or at the store's maximum age after its value's write, whichever comes
  first (unless `withMaximumAgeForStoreOnly()`, see [Store](#store)); a copy of a value already old when read therefore
  expires sooner. `expireAfterWrite` or `expireAfterAccess` longer than the maximum age fails the build: the local
  copies would outlive their values' validity, so it would never apply. A maximum age lowered at runtime (see
  [Distributed policy](#distributed-policy)) below `expireAfterWrite` lowers it instead, as `policy()` shows;
  without any local expiry set (`expireAfterWrite` or `expireAfter`), `policy()` shows `expireAfterWrite` at the
  maximum age.
* **Size:** the bounds are independent: only the local `maximumSize` or `maximumWeight` bounds the local cache, and the
  store pruning to its maximum size leaves the local copies alone. An atomic `asMap()` method then sees a pruned key as
  absent, as Caffeine does after an eviction, even while a local copy still serves reads.

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

#### Store

Most caches bound the store (unbounded by default):

* `withMaximumAge(...)`: how long a value is valid after its write: the store serves it no longer, and the local copies
  go then too (default: unbounded).
* `withMaximumSize(...)`: how many values the store keeps: trimmed each maintenance run to the most recently written
  ones (default: unbounded).
* `withMaximumAgeForStoreOnly()`: the maximum age bounds the store's values but not the local copies, for which only the
  local expiry counts; a miss after the maximum age calls the source (default: off). For local copies meant to live
  longer than the store should keep values. Requires distribution and a maximum age of at least five heartbeat intervals
  (see `withHeartbeatInterval(...)` below): 50 seconds with the default interval.

Further settings, whose defaults suit most uses:

* `withOperationTimeout(...)`: how long a call waits for the store's answer before it fails (default: 10 seconds).
* `withFailFast()`: building fails when the store cannot be reached, instead of starting without it and connecting in
  the background (default: off). For applications that should not start while their store is out of reach, rather than
  run `DEGRADED` until it is reached (see [Failures](#failures) and the health checks under [Deployment](#deployment)).
* `withMaintenanceInterval(...)`: how often values beyond the maximum age or size are removed (default: 1 minute).
* `withPollInterval(...)`: how often an instance polls for changes (default: 1 second). Only for adapters that poll to
  support distribution (see [Adapters](#adapters)).
* `withHeartbeatInterval(...)`: how often an instance checks that distribution still works (default: 10 seconds). A
  stop is noticed within about two intervals, after which the instance checks its copies against the store until
  distribution works again.
* `withExecutor(...)`: where the blocking store calls run, never on the calling thread (default: daemon threads).

#### Distribution mode

The distribution mode decides what a change made elsewhere does to the copies of an instance:

* `INVALIDATE` (the default): a change elsewhere drops the copy; the new value is read on demand.
* `UPDATE`: a write elsewhere replaces the copy right away; an invalidation or an expiry drops it.
* `NONE`: changes are not distributed; a local copy stays until it expires or is evicted.

```java
DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withDistributionMode(DistributionMode.UPDATE)
        .build();
```

Note: No mode creates a copy on an instance that doesn't hold the key yet; `UPDATE` only refreshes existing copies.

#### Hashing (for keys)

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
    ...
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

#### Serialization

Keys and values are serialized for the store, as bytes, text or JSON:

* **Bytes:** `ForySerializer`, the default for both, handles every type without configuration, using
  [Apache Fory](https://github.com/apache/fory). `JavaObjectSerializer` uses
  [Java Object Serialization](https://docs.oracle.com/en/java/javase/17/docs/specs/serialization/index.html).
* **Text:** `StringSerializer.ofString()`, `ofLong()`, `ofInteger()` and `ofUUID()` store those types readably.
* **JSON:** `JacksonSerializer` stores JSON as text or, on MongoDB and PostgreSQL, in the store's own JSON type, using
  [Jackson](https://github.com/FasterXML/jackson).

A custom serializer implements `ByteArraySerializer`, `StringSerializer` or `JsonSerializer`, for bytes, text or JSON.

```java
DistributedCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withSerializers(serializers -> serializers
                .withKeySerializer(new JacksonSerializer<>(Key.class, false)) // as text
                .withValueSerializer(new JacksonSerializer<>(Value.class, true))) // as the store's JSON type, if any
        .build();
```

Note: The byte serializers take the class to create from the stored bytes, so whoever can write to the store can choose
it. If the store's access cannot be trusted, use `ForySerializer.allowing(...)` or `JavaObjectSerializer.allowing(...)`,
which accept only the listed classes; the text and JSON serializers decode only into the type they were given.

#### Negative caching

Negative caching remembers keys whose load found nothing (no value in the store, and the loader or mapping function
returned `null`) for the duration set with `withNegativeCaching(...)`, so that repeated requests for a missing key reach
neither the store nor the source. A value written through any instance ends the absence at once (without distribution,
only through the same instance); a value that appears in the source only is found once the absence expired. While
distribution is interrupted (see [Failures](#failures)), an absence is checked against the store, as copies are. An
absence belongs to the key: it answers every later load of it, whichever loader or mapping function that load would
call. It is off by default, as Caffeine caches no missing value.

```java
DistributedLoadingCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withNegativeCaching(Duration.ofSeconds(30))
        .build(key -> loadExpensiveValue(key));
```

### Distributed policy

`distributedPolicy()` gives access to the distributed side of a cache instance: its synchronization state
(`getSynchronizationState()`), its stats of store, synchronization and maintenance activity (`stats()`, only recorded
with `recordStats()`), and `getFromStore(...)` and `getAllFromStore(...)`, which read the store directly, bypassing the
instance's copies. The state is one of:

* `SYNCHRONIZED`: the store answers and changes made elsewhere arrive.
* `DEGRADED`: the store does not answer or changes made elsewhere do not arrive (see [Failures](#failures)); the
  instance recovers by itself.
* `CLOSED`: the cache instance was closed.

Caffeine's stats (`cache.stats()`) count a value found in the store as a miss and a load, so their `hitRate()` is the
rate of local hits and their load counts and times include the store reads; the distributed stats count these values
as `storeHitCount`, and the calls to the source alone as `sourceLoadCount`.

`storeMaximumAge()` and `storeMaximumSize()` adjust the store's bounds at runtime, as Caffeine's
`policy().expireAfterWrite()` and `policy().eviction()` adjust the local ones. Each is present only if the cache was
built with that bound, and each applies to the instance it is called on, so a change is made on every instance, for
example from a configuration service.

* **Maximum age:** applies at once to the instance's reads of the store, to its maintenance and to its copies (not
  with `withMaximumAgeForStoreOnly()`). A longer age lengthens the copies held as far as their local expiry allows,
  except under `expireAfter(...)`. With `withMaximumAgeForStoreOnly()`, it must stay at least five heartbeat
  intervals.
* **Maximum size:** applies from the instance's next maintenance run.
* **While instances differ,** as while a change reaches them one after another, values stay consistent: the shortest
  age expires values for every instance, which then load them again, and the store is pruned to the smallest size.
  The stats show it as expired values (`maintenanceExpiredCount`, `expiredCount`), store misses and loads.

### Settings by goal

Which setting serves which goal; the rest can stay at their defaults:

| Goal                                                                                            | Setting                                                                         |
|-------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| Every value loaded from the source again at the latest a given time after its write             | the store's `withMaximumAge(...)`                                               |
| Read values reloaded in the background, the old value served meanwhile                          | Caffeine's `refreshAfterWrite(...)` (a loading cache)                           |
| A value with a validity of its own, such as a token's expiry                                    | a check on read and `invalidate(key)` (see below)                               |
| Other instances' copies dropped on a write, read again on their next access (the default)       | `withDistributionMode(DistributionMode.INVALIDATE)`                             |
| Other instances' copies replaced on a write, not dropped                                        | `withDistributionMode(DistributionMode.UPDATE)`                                 |
| Other instances' copies kept on a write, until they expire or are evicted                       | `withDistributionMode(DistributionMode.NONE)`                                   |
| Other instances' writes seen after a given time, without distribution (`DistributionMode.NONE`) | Caffeine's `expireAfterWrite(...)`                                              |
| Missing keys not looked up in the store and the source again and again                          | `withNegativeCaching(...)`                                                      |
| Less memory per instance                                                                        | Caffeine's `maximumSize(...)`, `maximumWeight(...)` or `expireAfterAccess(...)` |
| A smaller store                                                                                 | the store's `withMaximumSize(...)`                                              |
| Local copies kept longer than the store keeps values                                            | the store's `withMaximumAgeForStoreOnly()`                                      |
| The maximum age or size changed at runtime                                                      | `distributedPolicy().storeMaximumAge()` or `storeMaximumSize()`                 |

A value with a validity of its own, such as a token, can't be expired by an `expireAfter(...)` alone: it only drops
the local copy, and the next read finds the value in the store again. Treat an invalid value read as missing:
`invalidate(key)` removes it on every instance and in the store, and the next read loads a fresh one.

## Remarks

### Local and shared

* **Local only:** `estimatedSize()`, the sizes and iteration of `asMap()` and its sets, `stats()`, `cleanUp()` and
  what `policy()` reports concern only the copies of the instance they are called on, as do the synchronization state
  and the stats of `distributedPolicy()`.
* **Shared:** every write and removal goes to the store, and with distribution to the other instances, whichever
  method makes it: also the sets of `asMap()` (such as `keySet().remove(key)` or `values().removeIf(...)`), the writes
  of `policy().expireVariably()`, and `invalidateAll()` or `asMap().clear()`.
* **Eviction and expiry:** an instance evicting or expiring a copy (by Caffeine's bounds) leaves the other instances'
  copies and the store's value alone, and the store removing values to stay within its maximum size leaves the
  instances' copies alone. Only the store's maximum age drops the copies on every instance (unless
  `withMaximumAgeForStoreOnly()`).
* **Removal listener:** it reports only the copies of its own instance. With distribution, that includes a copy dropped
  or replaced because of a change made elsewhere; another instance evicting its own copy is not reported here.
* **First reads:** an instance starts with an empty local cache and loads nothing ahead, so its first read of a key
  goes to the store, and later reads of it are local. To warm an instance up with many keys at once, `getAll(keys)`
  reads them in one call to the store.

### Consistency

* **Versions:** every write carries a version, so a racing write, a slow load or a change arriving late never replaces
  a newer value, neither in the store nor in a copy.
* **Outdated copies:** a copy can be outdated until the change reaches its instance, and at the latest until its local
  expiry or the store's maximum age.
  <details><summary>Details</summary>

  A copy is outdated at most until the first of:
    * the change reaches its instance: within milliseconds with MongoDB, PostgreSQL and Redis, within about one poll
      interval with MariaDB and MySQL, and never if it is lost unnoticed on its way or the distribution mode is `NONE`;
    * its local expiry (Caffeine's `expireAfterWrite`, `expireAfterAccess` or `expireAfter`);
    * the store's maximum age (not with `withMaximumAgeForStoreOnly()`): every copy expires on its own at the maximum
      age after its value's write, in every distribution mode and also if a change never arrives.

  Without local expiry and maximum age, a copy whose change never reaches its instance stays outdated for good.
  </details>
* **Changes only through the cache:** the store's data belongs to the cache. A change made to it directly, by hand, by
  a script or by another application, is not guaranteed to reach the instances, so their copies may stay outdated;
  use the cache instead, such as `invalidate(...)` or `invalidateAll()`.
* **Atomic operations:** `compute`, `merge`, `putIfAbsent`, `replace` and their kin are atomic across instances: they
  write only if the value they decided on is still current, and decide again otherwise.
* **Loads:** shared within an instance only, as in Caffeine. Instances missing the same key at once each call the
  source; the value written first is kept, and the others return it.
* **Invalidating everything:** operations still running on other instances can write after `invalidateAll()`.
  <details><summary>Details</summary>

  `invalidateAll()` is not atomic with operations already running on other instances. Two kinds can write their result
  after it: a load that read the source before it, and an atomic `asMap()` method (`compute`, `merge`, `replace`, ...)
  that decided on a value present before it. Such a result may even be older than a write made before the
  `invalidateAll()`; it stays until the key is written again or reaches the store's maximum age. Caffeine, too, leaves
  loads in flight during `invalidateAll()` undefined.
  </details>
* **Refresh:** `refreshAfterWrite` counts from when an instance got its copy, so a value is refreshed at an age of up to
  twice the duration. Instances refreshing a key at once each call the loader; the first result written wins.

### Failures

* **Store not reached when building:** the cache starts without it, as during an outage, and connects in the
  background; `withFailFast()` makes building fail instead.
  <details><summary>Details</summary>

  Misses load from the source, writes fail, and the state is `DEGRADED` until the store is reached. Values loaded
  meanwhile are checked against the store then. So an application restarted during an outage still starts, while a
  misconfiguration (a wrong host, a blocked network) shows as a `DEGRADED` instance rather than a failed rollout,
  unless `withFailFast()` is set. A store that answers but cannot be used (a schema that does not match, missing
  rights, wrong credentials) fails the build either way.
  </details>
* **Store not answering:** reads keep working, writes fail, and the instances recover automatically once it answers
  again.
  <details><summary>Details</summary>

    * Reads serve the copies held. A miss loads from the source, or stays a miss without one; a refresh calls the
      loader as usual. A loaded value is kept locally without being written, and checked against the store once it
      answers.
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
* **Distribution interrupted:** while changes made elsewhere stop arriving (a lost connection, failing polls on
  MariaDB and MySQL, or a silent connection the heartbeat notices), the store still answers, but an instance checks
  every copy it reads against the store, and all of them once changes arrive again. Its state is `DEGRADED` meanwhile.

### Deployment

* **Same configuration:** all instances of a cache need the same store bounds (changed at runtime on every instance,
  see [Distributed policy](#distributed-policy)), serializers and hashing. After changing the key or value
  classes, their serializers or the hashing, remove the stored values (`invalidateAll()`) or give the cache a new
  place; on PostgreSQL, a serializer storing another format always needs a new place.
* **Unreadable values:** a value an instance cannot read, such as one written with other value classes or
  serializers, counts as a miss: it is loaded again and the loaded value replaces it in the store, so the cache keeps
  working, at the cost of loads (logged, and counted as `unreadableRowCount`). While instances with different
  serializers run side by side, they may replace each other's values in turn. `asMap().containsKey(key)` checks only
  whether a value exists, so it answers `true` for an unreadable one.
* **Clocks:** keep them synchronized (NTP). Correctness does not depend on them, but a clock running ahead costs writes
  one more round trip (`versionRefusalCount`, `clockAheadCount`).
* **Health checks:** keep the store and the cache's state out of liveness checks. A store outage is what `DEGRADED`
  is for: running instances ride it out, and instances restarted meanwhile start without the store.
  <details><summary>Details</summary>

    * A liveness check that fails while the store does not answer (a health indicator of its database included, or
      `getSynchronizationState()` checked) has the instances restarted one after another during an outage, for
      nothing. With `withFailFast()` none can be built again until the store answers, and the whole application goes
      down, although the running instances would have served their copies and loaded misses from the source.
    * Building waits for the store up to the operation timeout per check when the network drops packets rather than
      refusing the connection (a partition, a firewall): allow for it in a startup probe. Caches sharing a client
      wait once: those built within an operation timeout after one that waited start without the store at once.
    * Readiness may reflect what the application needs to serve requests, such as the loader's source. Leave the store
      out of it too if `DEGRADED` instances should keep receiving requests.
  </details>
* **CPU:** synchronization runs in background threads, so platforms that allocate CPU only while a request is handled
  (common for serverless deployments) need it allocated permanently.
* **Logging:** through `System.Logger`, under `io.github.oberhoff.distributedcaffeine`. Each message begins with the
  cache's name (the store, the place and the discriminator), or for a connection that serves several caches with the
  names of those caches or the database.

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
* Only if `JacksonSerializer` is used: `tools.jackson.core:jackson-databind` (3 or newer) on the classpath, an optional
  dependency like the drivers.

## Installation

Go to
[Distributed Caffeine on Maven Central](https://central.sonatype.com/artifact/io.github.oberhoff.distributed-caffeine/distributed-caffeine),
select your preferred build tool, copy the snippet provided and paste it into your project at the appropriate location.
Then add the driver of your store (see [Requirements](#requirements)).
