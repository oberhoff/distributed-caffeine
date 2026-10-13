# MongoDB adapter

Details of the MongoDB-based adapter beyond its configuration, which the
[main README](../../../../../../../../../README.md) describes. Synchronization uses MongoDB's change streams, which keep
a connection open for watching. The adapter relies on the defaults of the client it is given, and starting
synchronization fails right away if the setup cannot support it.

## Watcher sharing

How cache instances (using the same client) share connections for watching is specified using
`withWatcherSharingMode(...)`: `DATABASE` for one connection per database (default), `COLLECTION` for one per collection
or `INSTANCE` for one per cache instance. With `DATABASE`, a cache instance whose collection is not watched yet takes a
moment longer to start, because watching is restarted to include it.

## Read concern

Change streams do not support every read concern. If the client is configured with one they do not support, such as
`local`, starting synchronization fails right away.

## Broken connections

A watching connection that dies silently, for example during a failover, is detected and replaced automatically.
Operations for reading and writing on such a connection fail once TCP keepalive gives up on it, after several minutes
by default. If that is too long, `socketTimeoutMS` or `timeoutMS` can be set on the client to fail them sooner.

## Failovers

Cache instances stay consistent with the client's defaults. A write may occasionally fail with an exception while the
primary changes, because the driver retries it only once. Setting `timeoutMS` on the client above the duration of an
election (for example 30 seconds) makes the driver retry until the timeout instead, and also bounds writes that would
otherwise wait forever while no majority of data-bearing members is available.

## Sharded clusters

The collection may live in a sharded cluster, but must not be sharded itself. Building an adapter for a sharded
collection fails.
