# Distributed Caffeine

Distributed Caffeine is a [Caffeine](https://github.com/ben-manes/caffeine)-based distributed cache
using [MongoDB change streams](https://www.mongodb.com/docs/manual/changeStreams/) for near real-time synchronization
between multiple cache instances, especially across different machines.

## Table of contents

* [Introduction](#introduction)
* [Usage](#usage)
* [Remarks](#remarks)
* [Requirements](#requirements)
* [Installation](#installation)

## Introduction

Distributed Caffeine takes advantage of Caffeine's famous
[near optimal](https://github.com/ben-manes/caffeine/wiki/Efficiency) caching strategy, but additionally provides
synchronization (also known as distribution or replication) of cache entries between multiple cache instances,
especially when cache entries are
[populated](https://github.com/ben-manes/caffeine/wiki/Population)
([manual](https://github.com/ben-manes/caffeine/wiki/Population#manual) or
[loading](https://github.com/ben-manes/caffeine/wiki/Population#loading)),
[invalidated](https://github.com/ben-manes/caffeine/wiki/Removal#explicit-removals) (explicit removal) or
[evicted](https://github.com/ben-manes/caffeine/wiki/Eviction) (size- or time-based removal). Therefore, different
distribution modes are supported to configure
which types of cache operations are considered for synchronization and which are explicitly not.

<!--
Furthermore, mainly
depending on the configured distribution mode, persistence of cache entries is provided as well as an adjustable mix
between in-memory (also known as first-level, L1 or client-side) caching and database (also known as second-level, L2 or
server-side) caching.
-->

The synchronization of the cache instances is based on MongoDB change streams, which make it possible to access data
changes in [near real-time](https://www.mongodb.com/docs/manual/changeStreams/#change-streams) and react to them
immediately. Therefore, related cache instances share a MongoDB collection in order to distribute cache entries or
mentioned cache operations accordingly as well as to provide persistence. To distribute or persist various contents (for
keys and values) within cache entries, several types of serialization formats are available.

To summarize some of the advantages: Distributed Caffeine combines two established and widely used technologies that
many developers are already familiar with or that are already in the tech stack of many applications. This combination
effectively enables distributed caching and the implementation of many common use cases without the overhead of usually
more complex or more expensive tools with comparable features.

## Usage

Distributed Caffeine cache instances are represented by the two interfaces `DistributedCache` and
`DistributedLoadingCache` which are derived from Caffeine's `Cache` and `LoadingCache` interfaces and therefore offer
nearly the same usage. Please refer to the official
[Caffeine documentation](https://github.com/ben-manes/caffeine/wiki) for more details about its usage.

The configuration of a cache always begins with a builder returned by invoking the `newBuilder(mongoCollection)` method
and ends with finalizing the builder by invoking one of the `build(...)` methods to construct the cache instance. The
`mongoCollection` parameter refers to the MongoDB collection used for synchronization and persistence. Please note the
additional generic type parameters for key and value directly before the`newBuilder(...)` method.

##### Minimal configuration of a distributed cache

```java
DistributedCache<Key, Value> distributedCache = DistributedCaffeine.<Key, Value>newBuilder(mongoCollection)
        .build();
```

##### Minimal configuration of a distributed loading cache

```java
DistributedLoadingCache<Key, Value> distributedLoadingCache = DistributedCaffeine.<Key, Value>newBuilder(mongoCollection)
        .build(key -> loadExpensiveValue(key));
```

##### Configuration of a distributed cache with additional configuration of the Caffeine cache used internally

```java
DistributedCache<Key, Value> distributedCache = DistributedCaffeine.<Key, Value>newBuilder(mongoCollection)
        .withCaffeineBuilder(Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofMinutes(5)))
        .build();
```

Please note that the configuration of the Caffeine cache used internally also begins with a builder returned by invoking
its own `newBuilder()` method, but that the builder is not finalized by invoking one of its own `build(...)` methods
(this construction is done internally by the outer `build(...)` methods instead). If the configuration of the Caffeine
cache is skipped, a default (empty) configuration is used. Please also refer to the official
[Caffeine documentation](https://github.com/ben-manes/caffeine/wiki) for details about its configuration options.

##### Configuration of a distributed cache with additional configuration of the distribution mode

```java
DistributedCache<Key, Value> distributedCache = DistributedCaffeine.<Key, Value>newBuilder(mongoCollection)
        .withDistributionMode(DistributionMode.POPULATION_AND_INVALIDATION_AND_EVICTION)
        .build();
```

Distribution modes include/exclude different types of cache operations (population, invalidation, eviction) which are
then considered or not considered for distributed synchronization between cache instances. The following distribution
modes are provided:

* `POPULATION_AND_INVALIDATION_AND_EVICTION`: Includes population (manual or loading), invalidation
  (explicit removal) and eviction (size- or time-based removal). This is the default distribution mode and
  corresponds to a full replication.
* `POPULATION_AND_INVALIDATION`: Includes population (manual or loading) and invalidation (explicit removal), but
  excludes eviction (size- or time-based removal).
* `INVALIDATION_AND_EVICTION`: Includes invalidation (explicit removal) and eviction (size- or time-based removal), but
  excludes population (manual or loading).
* `INVALIDATION`: Includes invalidation (explicit removal), but excludes population (manual or loading) and eviction
  (size- or time-based removal).

##### Configuration of a distributed cache with additional configuration of JSON (or BSON) serialization

```java
DistributedCache<Key, Value> distributedCache = DistributedCaffeine.<Key, Value>newBuilder(mongoCollection)
        .withJsonSerializer(new ObjectMapper(), Key.class, Value.class, storeAsBson)
        .build();
```

Keys and values of cache entries must be serialized in order to store them in the MongoDB collection and deserialized
when they are synchronized back into the cache instances. By default, these objects are stored in binary format using
[Apache Fury](https://github.com/apache/fury). However, storage in JSON format sometimes makes more sense: The JSON
format is more readable and can even be converted into MongoDB's own BSON format. JSON serialization is done internally
using [Jackson](https://github.com/FasterXML/jackson). A customized object mapper can be passed if required, but can
also be omitted if a default object mapper is sufficient. In addition, either the object classes or type references of
the key and the value objects of a cache entry must be provided. The boolean flag `storeAsBson` indicates if the JSON
data should be converted to BSON or stored as a string. Furthermore, the classic
[Java Object Serialization](https://docs.oracle.com/en/java/javase/11/docs/specs/serialization/index.html) can also be
configured for serialization.

##### Configuration of a distributed cache with additional configuration of customized serialization

```java
DistributedCache<Key, Value> distributedCache = DistributedCaffeine.<Key, Value>newBuilder(mongoCollection)
        .withCustomKeySerializer(new CustomSerializer())
        .withCustomValueSerializer(new CustomSerializer())
        .build();
```

Serialization can be customized by implementing one of the `ByteArraySerializer`, `StringSerializer` or `JsonSerializer`
interfaces.

## Remarks

* Distributed Caffeine only supports the
  [synchronous variants](https://github.com/ben-manes/caffeine/wiki/Population#manual) of Caffeine, the
  [asynchronous variants](https://github.com/ben-manes/caffeine/wiki/Population#asynchronous-manual) are not supported.
* Reference-based eviction using Caffeine's
  [weak or soft references](https://github.com/ben-manes/caffeine/wiki/Eviction#reference-based) for keys or values is
  not supported. Even for the use of Caffeine (stand-alone), it is advised to use the more predictable size- or
  time-based eviction instead.
* Manipulating cache entries or their metadata directly in the MongoDB collection should be done with caution.
  Corresponding cache instances might attempt to reflect certain changes immediately, which may fail if the changed data
  cannot be interpreted correctly anymore.
* Adjusting the configuration of cache instances (includes the definition of key and value objects) should be done with
  caution. The newly configured cache instances attempt to synchronize any existing legacy data from the corresponding
  MongoDB collection, which may fail if the legacy data cannot be interpreted correctly anymore. Corresponding MongoDB
  collections should be cleaned up (or perhaps migrated) beforehand.
* Related cache instances (sharing the same MongoDB collection) must be configured in the same way to prevent
  unpredictable behavior.
* Each cache instance requires its own connection to MongoDB for watching change streams. If many cache instances are
  used or many connections are used elsewhere, the connection pool might be enlarged. The default pool size is 100 which
  is sufficient for most cases.

## Requirements

* Java 11 or newer
* MongoDB 4.0 or newer (MongoDB 5.1 or newer is recommended due to change stream optimizations)
* MongoDB must be configured to run as a replica set (single node replica set would be sufficient)

## Installation

Go to
[Distributed Caffeine on Maven Central](https://central.sonatype.com/artifact/io.github.oberhoff.distributed-caffeine/distributed-caffeine/overview),
select your preferred build tool, copy the snippet provided and paste it into your project at the appropriate location.
