# Spring integration

Details of the Spring integration, listed in the [main README](../../../../../../../../../README.md#integrations).

`DistributedCaffeineCacheManager` replaces Spring's `CaffeineCacheManager`: the caching annotations (`@Cacheable`,
`@CachePut`, `@CacheEvict`) stay as they are, and each cache is built from a builder the application supplies by the
cache's name. For example, with MongoDB (each cache a collection; any other adapter works the same way):

```java
@Bean
DistributedCaffeineCacheManager cacheManager(MongoClient mongoClient) {
    return new DistributedCaffeineCacheManager(name -> DistributedCaffeine.newBuilder(MongoAdapter
                    .newBuilder(mongoClient, databaseName, name)
                    // or all caches in one collection, told apart by name:
                    // .newBuilder(mongoClient, databaseName, collectionName).withDiscriminator(name)
                    .build())
            .withCaffeine(caffeine -> caffeine.maximumSize(10_000))
            .withStore(store -> store.withMaximumAge(Duration.ofHours(1))));
}
```

## Caches and names

The function gets a cache's name and returns a new builder with a new adapter on every call, since an adapter belongs
to one cache. The name usually becomes the cache's collection, table or keyspace, as Spring's caches are separate
regions: each can then be inspected and dropped in the store on its own, and the discriminator stays free to keep
applications sharing a database apart. Alternatively, the caches share one collection, table or keyspace, told apart by
the name as discriminator.

Either way, cache names must follow the store's naming rules for that part (see the adapters' details in the
[main README](../../../../../../../../../README.md#adapters)); on MariaDB and MySQL, for example, a table name has
letters, digits and `_` only, and on Redis a discriminator has no `:`. A name outside them fails when the cache is
created. A renamed cache starts empty; the values under its old name are no longer used, and can be removed from the
store.

Caches are created by name on first use, or all at startup with `setCacheNames(...)`, as on `CaffeineCacheManager`.
With a manager bean of its own, Spring Boot's cache auto-configuration backs off, so `spring.cache.cache-names` does not
apply: use `setCacheNames(...)` instead.

## Settings

The manager's settings work as on `CaffeineCacheManager`: `setCacheNames`, `setAllowNullValues`, `setAsyncCacheMode`
(distributed asynchronous caches, for `CompletableFuture` and reactive return types), `setCacheLoader`,
`setAsyncCacheLoader` and `registerCustomCache`.

Caches are configured on the builders; `setCaffeine` is not supported and throws, rather than being ignored. A spec set
with `setCaffeineSpec` or `setCacheSpecification` applies to every builder after its own settings, and a setting made by
both fails when the cache is built. With a manager bean of its own, Spring Boot does not pass
`spring.cache.caffeine.spec` on by itself: hand it over with
`manager.setCacheSpecification(cacheProperties.getCaffeine().getSpec())`, given Boot's `CacheProperties`.

## Keys

Keys of Spring's default key generator are hashed without configuration. A method's single argument is the key as it
is; several arguments, or none, become a `SimpleKey`, hashed by its parts. A key, and each part of a `SimpleKey`, is
hashed as:

* a `String`, `Long`, `Integer`, `UUID` or an enum constant: as the cache hashes it without the integration;
* a `Hashable`: by its own hash (a `SimpleKey` subclass implementing `Hashable` included);
* `null`: as a part of a `SimpleKey`.

A key or part of another type (a date, a class of the application's) fails on first use, naming its type.
For these, implement `Hashable` in the class, give the method a key of the types above
(`@Cacheable(key = "#order.id")`), or set a hash provider for them on the builder, the usual way. The manager asks it
only for keys, and parts of a `SimpleKey`, of other types, and hashes everything above itself as before:

```java
new DistributedCaffeineCacheManager(name -> DistributedCaffeine.newBuilder(adapter(name))
        .withHashProvider((key, hasher) -> {
            if (key instanceof LocalDate date) {
                return hasher.get().putLong(date.toEpochDay()).getHash();
            }
            throw new IllegalStateException("cannot hash " + key.getClass());
        }));
```

Keys are serialized too, for diagnostics only. Where the builder sets no key serializer, the manager sets Fory's with a
`SimpleKey`, a subclass included, written by its params. Fory would otherwise write it through its Java serialization
path, as `SimpleKey` customizes Java serialization, and log a warning once ("uses custom JDK serialization"). A key
serializer set on the builder is kept as it is.

## Values

Values are serialized into the store (see [Serialization](../../../../../../../../../README.md#serialization)), so
values of the application's classes need a serializer that can handle them; the default (Fory) handles every type.
With `setAllowNullValues(true)`, Spring's default, a `null` result is cached as Spring's marker for it and read back as
`null` on every instance. The cache's own negative caching (`withNegativeCaching(...)`) is independent of it and
applies to the cache's loader only.

## Distribution and failures

`@CachePut` and `@CacheEvict` act on every instance through the store; `@CacheEvict(allEntries = true)` and
`resetCaches()` clear a cache on every instance. A change reaches the other instances with the store's delivery, so
for a moment they may still serve the previous value, as described under
[Consistency](../../../../../../../../../README.md#consistency).

While the store does not answer, reads keep working and writes fail with `StoreException` (see
[Failures](../../../../../../../../../README.md#failures)). For Spring this means:

* `@Cacheable` reads the cache, calls the method on a miss and then writes the result: the write fails, and with
  Spring's default `CacheErrorHandler` so does the call. A `CacheErrorHandler` that logs instead (such as Spring's
  `LoggingCacheErrorHandler`, set through a `CachingConfigurer`) keeps the methods working.
* `@Cacheable(sync = true)` loads through the cache instead, which goes on during an outage (the result is kept locally
  and checked against the store once it answers), so it keeps working without an error handler.
* `@CachePut` and `@CacheEvict` fail, or are logged by such an error handler; an eviction drops the local copy first.

## Lifecycle

The manager closes the caches it built when the application context closes. Spring also drops caches on its own:
`removeCache(...)`, `resetCaches()` (the caches created on first use), a custom cache registered under a built cache's
name, and the setters that rebuild the caches (`setAllowNullValues`, `setAsyncCacheMode`, `setCacheLoader`,
`setAsyncCacheLoader`). The manager closes each cache it built once Spring has let go of it, releasing its connections.
A custom cache registered with `registerCustomCache(...)` stays the application's to close.

## Versions

Spring Framework 6.2 or newer, that is Spring Boot 3.4 or newer (see
[Requirements](../../../../../../../../../README.md#requirements)). The manager builds on `CaffeineCacheManager`'s
protected methods for creating caches, which it overrides.
