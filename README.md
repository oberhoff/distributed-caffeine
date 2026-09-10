[![ci/cd](https://img.shields.io/github/check-runs/oberhoff/distributed-caffeine/main?logo=github&logoColor=ffffff&label=ci%2Fcd&style=for-the-badge&color=009539)](https://github.com/oberhoff/distributed-caffeine/actions?query=branch%3Amain)
[![maven central](https://img.shields.io/maven-central/v/io.github.oberhoff.distributed-caffeine/distributed-caffeine?logo=apachemaven&logoColor=ffffff&label=maven%20central&style=for-the-badge&color=009539)](https://central.sonatype.com/artifact/io.github.oberhoff.distributed-caffeine/distributed-caffeine)
[![javadoc](https://img.shields.io/maven-central/v/io.github.oberhoff.distributed-caffeine/distributed-caffeine?logo=openjdk&logoColor=ffffff&label=javadoc&style=for-the-badge&color=009539)](https://javadoc.io/doc/io.github.oberhoff.distributed-caffeine/distributed-caffeine)
[![license](https://img.shields.io/github/license/oberhoff/distributed-caffeine?logo=apache&logoColor=ffffff&label=license&style=for-the-badge&color=009539)](https://github.com/oberhoff/distributed-caffeine/blob/main/LICENSE)

# Distributed Caffeine

Distributed Caffeine is a [Caffeine](https://github.com/ben-manes/caffeine)-based distributed cache using
[MongoDB change streams](https://www.mongodb.com/docs/manual/changeStreams) for near real-time synchronization between
multiple cache instances, especially across different machines.

## Table of contents

* [Introduction](#introduction)
* [Usage](#usage)
* [Remarks](#remarks)
* [Requirements](#requirements)
* [Installation](#installation)

## Introduction

Distributed Caffeine takes advantage of Caffeine's famous
[near optimal](https://github.com/ben-manes/caffeine/wiki/Efficiency) cache implementation and additionally provides
synchronization between multiple cache instances by distributing cache operations for
[population](https://github.com/ben-manes/caffeine/wiki/Population)
([manual](https://github.com/ben-manes/caffeine/wiki/Population#manual) or
[loading](https://github.com/ben-manes/caffeine/wiki/Population#loading)),
[invalidation](https://github.com/ben-manes/caffeine/wiki/Removal#explicit-removals) (explicit removal) and
[eviction](https://github.com/ben-manes/caffeine/wiki/Eviction) (size- or time-based removal), optionally combined with
persistence of cache entries. Therefore, related cache instances share a dataset in an underlying store. 

By default, persistence is not used: no cache entries are retained in the underlying store, which then serves only the
synchronization between cache instances by distributing cache operations. A fresh cache instance therefore starts empty,
but immediately takes part in synchronization. Which types of cache operations are considered for distributed
synchronization, and which are explicitly not, can be configured through the various distribution modes provided.

With configured persistence, cache entries are retained in the underlying store, separately configurable for cached and
evicted entries: cached entries can warm up a fresh cache instance, and evicted entries remain available after eviction
(passivation) and can be reloaded on demand (activation). Combined as needed, both provide an adjustable mix of
in-memory (first-level, L1 or client-side) and database (second-level, L2 or server-side) caching.

To summarize some advantages: Distributed Caffeine combines established and widely used technologies that many
developers are already familiar with or that are already in the tech stack of many applications. This combination
effectively enables distributed caching and the implementation of many common use cases without the overhead of usually
more complex or more expensive tools with comparable features.

## Usage

### Adapters

Before the actual (store-agnostic) Distributed Caffeine cache instances are specified, an adapter must be configured to
provide the connection to the underlying store. An already built-in `MongoAdapter` for MongoDB (technically based on
MongoDB change streams, which provide [near real-time](https://www.mongodb.com/docs/manual/changeStreams) access to data
changes) can be used or custom adapters can be implemented based on the `Adapter` interface.

#### Configuration of an adapter for MongoDB

The configuration of the MongoDB-based adapter always starts with a builder returned by invoking the
`MongoAdapter.newBuilder(mongoClient, databaseName, collectionName)` method and ends with finalizing the builder by
invoking one of the `build()` methods to construct the adapter instance. The `mongoClient`, `databaseName` and
`collectionName` parameters refer to the MongoDB client, database name and collection name used for distributed
synchronization and persistence. Optionally, a discriminator can be specified (using the `withDiscriminator(...)`
method) to distinguish between cache entries from different caches that share a collection in MongoDB.

```java
MongoAdapter<Key, Value> adapter = MongoAdapter.newBuilder(mongoClient, databaseName, collectionName)
        .withDiscriminator("discriminator") // optional (used if different caches share a collection)
        .build();
```

Note: An adapter instance belongs to exactly one cache instance and cannot be shared between them.

### Distributed Caffeine Caches

Distributed Caffeine cache instances are represented by `DistributedCache` and `DistributedLoadingCache` interfaces
which are derived from Caffeine's `Cache` and `LoadingCache` interfaces and therefore offer almost the same usage and
integration options (drop-in replacement possible). Please refer to the official
[Caffeine documentation](https://github.com/ben-manes/caffeine/wiki) for more details on use and integration.

#### Minimal configurations of distributed (loading) caches

The configuration of a cache always starts with a builder returned by invoking the
`DistributedCaffeine.newBuilder(adapter)` method and ends with finalizing the builder by invoking one of the
`build(...)` methods to construct the cache instance. The `adapter` parameter refers to the adapter instance build like
described above.

```java
DistributedCache<Key, Value> distributedCache = DistributedCaffeine.newBuilder(adapter)
        .build();
```

```java
DistributedLoadingCache<Key, Value> distributedLoadingCache = DistributedCaffeine.newBuilder(adapter)
        .build(key -> loadExpensiveValue(key));
```

#### Configuration of the Caffeine cache used internally

The configuration of the Caffeine cache used internally also starts with a builder returned by invoking its own
`Caffeine.newBuilder()` method, however the builder is not finalized by invoking one of its own `build(...)` methods
(this construction is done internally by the outer `build(...)` methods instead). If the configuration of the Caffeine
cache is skipped, a default (empty) configuration is used. Please refer to the official
[Caffeine documentation](https://github.com/ben-manes/caffeine/wiki) for more details on configuration.

```java
DistributedCache<Key, Value> distributedCache = DistributedCaffeine.newBuilder(adapter)
        .withCaffeine(Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofMinutes(10)))
        .build();
```

#### Configuration of hashing (for keys of cache entries)

Cache entries need to be identifiable during distributed synchronization and for persistence by a hash computed for
their keys. Keys of type `String`, `Long`, `Integer` or `UUID` are hashed out of the box. For other types of keys two
ways for computation of hashes are supported.

If the implementation of the key class cannot or should not be changed, a hash provider can be configured. The supplied
hasher can be used to compute and return a hash based on values of the given key.

```java
DistributedCache<Key, Value> distributedCache = DistributedCaffeine.newBuilder(adapter)
        .withHashProvider((key, hasher) -> hasher.get()
                .putUUID(key.getId())
                .putLong(key.getVersion())
                .putString(key.getName())
                .getHash())
        .build();
```

Alternatively, the key class can implement the `Hashable` interface and override the `getHash(hasher)` method. The
supplied hasher can be used to compute and return a hash based on values of the key object.

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

Note: Hashing must be equivalent to key equality (`equals()`-semantics) and stable across cache instances, so all values
relevant for key equality (and only those) should be put into a hasher. A specified hash provider always takes
precedence over the alternatives listed above.

#### Configuration of the distribution mode

Distribution modes include/exclude different types of cache operations (population, invalidation, eviction) which are
then considered or not considered for distributed synchronization between cache instances. The following distribution
modes are provided:

* `POPULATION_AND_INVALIDATION_AND_EVICTION`: Includes population (manual or loading), invalidation (explicit removal)
  and eviction (size- or time-based removal). This is the default distribution mode.
* `POPULATION_AND_INVALIDATION`: Includes population (manual or loading) and invalidation (explicit removal), but
  excludes eviction (size- or time-based removal).
* `INVALIDATION_AND_EVICTION`: Includes invalidation (explicit removal) and eviction (size- or time-based removal), but
  excludes population (manual or loading).
* `INVALIDATION`: Includes invalidation (explicit removal), but excludes population (manual or loading) and eviction
  (size- or time-based removal).

```java
DistributedCache<Key, Value> distributedCache = DistributedCaffeine.newBuilder(adapter)
        .withDistributionMode(DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION)
        .build();
```

Note: Invalidations are distributed to other cache instances independently of what the invalidating cache instance
holds.

#### Configuration of persistence

Persistence can retain cache entries in the underlying store beyond what distributed synchronization itself requires. It
is configured separately for cached and evicted entries.

Persistence of cached entries either retains (unless invalidated or evicted) all of them for as long as they are cached
(configuring cache residency) or is limited by size and time, which can be combined with each other but not with cache
residency. Retained cache entries are synchronized back into a cache instance as warm-up, unless a cold start is
configured explicitly, which cache residency does not allow. Persistence of cached entries requires a distribution mode
including population, and including eviction as well if cache residency is combined with an eviction policy.

Persistence of evicted entries retains (unless invalidated) recently evicted cache entries and is limited by size and/or
time, which is also required for enabling loading strategies. Using the loading strategy for cache loader means that a
provided cache loader is only invoked to obtain missing cache entries if these could not be reloaded from the underlying
store beforehand, which requires the cache to be built as a loading cache. Persistence of evicted entries requires at
least one eviction policy, but works regardless of whether the distribution mode includes eviction.

Alternatively, the `getFromStore(...)` or `getAllFromStore(...)` methods (via `cacheInstance.distributedPolicy()`) can
be used to load retained cache entries directly from the underlying store bypassing the cache instance.

```java
DistributedLoadingCache<Key, Value> distributedLoadingCache = DistributedCaffeine.newBuilder(adapter)
        .withCaffeine(Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofMinutes(10)))
        .withPersistence(configurer -> configurer
                .withCachedEntries(cachedEntries -> cachedEntries
                        .withCacheResidency()) // as long as cached (mutual exclusive with size and/or time limits)
                        //.withMaxiumumSize(1_000) // limited by size
                        //.withMaximumTime(Duration.ofDays(1)) // limited by time
                        //.withColdStart() // no warm-up (mutual exclusive with cache residency)
                .withEvictedEntries(evictedEntries -> evictedEntries
                        .withMaximumSize(1_000_000) // limited by size
                        .withMaximumTime(Duration.ofDays(10)) // limited by time
                        .withLoadingStrategies(CACHE_LOADER))) // loading strategy
        .build(key -> loadExpensiveValue(key)); // cache loader
```

Note: No persistence is used unless configured.

#### Configuration of serialization

Keys and values of cache entries must be serialized for storing and deserialized when loaded back into the cache
instances. Already built-in serializers are `ForySerializer` (default, no explicit configuration needed, stores objects
in binary format using [Apache Fory](https://github.com/apache/fory)), `JavaObjectSerializer` (stores objects in binary
format using classic
[Java Object Serialization](https://docs.oracle.com/en/java/javase/17/docs/specs/serialization/index.html)) and
`JacksonSerializer` (stores objects as JSON or BSON for better readability/accessibility using
[Jackson](https://github.com/FasterXML/jackson)). Serialization can be customized by extending the aforementioned
built-in serializers or by implementing one of the `ByteArraySerializer`, `StringSerializer` or `JsonSerializer`
interfaces.

```java
DistributedCache<Key, Value> distributedCache = DistributedCaffeine.newBuilder(adapter)
        .withSerializers(configurer -> configurer
                .withKeySerializer(new JacksonSerializer<>(Key.class, storeAsBinaryJson))
                .withValueSerializer(new JacksonSerializer<>(Value.class, storeAsBinaryJson)))
        .build();
```

## Remarks

* Distributed Caffeine only supports the
  [synchronous variants](https://github.com/ben-manes/caffeine/wiki/Population#manual) of Caffeine, the
  [asynchronous variants](https://github.com/ben-manes/caffeine/wiki/Population#asynchronous-manual) are not supported.
* Reference-based eviction using Caffeine's
  [weak or soft references for keys or values](https://github.com/ben-manes/caffeine/wiki/Eviction#reference-based) is
  not supported. Even when using Caffeine (stand-alone), it is advisable to use the more predictable size- or time-based
  eviction instead.
* Manipulating cache entries or their metadata directly in the MongoDB collection should be done with caution.
  Corresponding cache instances might attempt to reflect certain changes immediately, which may fail if the changed data
  cannot be interpreted correctly anymore.
* Adjusting the configuration of cache instances (includes changes to key and value objects) should be done with
  caution. The newly configured cache instances attempt to synchronize any existing legacy data from the corresponding
  MongoDB collection, which may fail if the legacy data cannot be interpreted correctly anymore. Corresponding MongoDB
  collections should be cleaned up (or perhaps migrated) beforehand.
* Related cache instances (sharing the same MongoDB collection and discriminator) must be configured in the same way to
  prevent unpredictable behavior or even the loss of cache entries.
* Each cache instance requires its own connection to MongoDB for watching change streams. If many cache instances are
  used or many connections are used elsewhere, the connection pool might be enlarged. The default pool size is 100,
  which is sufficient for most cases.

## Requirements

* Java 17 or newer
* MongoDB 4.2 or newer (MongoDB 5.1 or newer is recommended due to change stream optimizations)
* MongoDB must be configured to run as a replica set (single node replica set would be sufficient)

## Installation

Go to
[Distributed Caffeine on Maven Central](https://central.sonatype.com/artifact/io.github.oberhoff.distributed-caffeine/distributed-caffeine),
select your preferred build tool, copy the snippet provided and paste it into your project at the appropriate location.
