# MongoDB adapter

Details of the MongoDB adapter beyond its configuration, which the [main README](../../../../../../../../../README.md)
describes. Distribution uses MongoDB's change streams. The adapter works with the client it is given, and building a
cache fails right away with an explanation if the setup cannot support it.

## Deployments

The adapter needs a replica set (a single node replica set is sufficient) or a sharded cluster through `mongos`, since
change streams and majority reads need one (versions: see
[Requirements](../../../../../../../../../README.md#requirements)). A standalone server is refused, as is an older
one. In a sharded cluster, the collection itself must not be sharded; a sharded collection is refused.

## Names

The database name has up to 63 bytes, without `/`, `\`, `.`, space, `"`, `$`, `*`, `<`, `>`, `:`, `|` or `?`; the
collection name has no `$` and does not begin with `system.`; together they have up to 255 bytes. The discriminator is
not blank. A name outside these is rejected when the adapter is built.

## Consistency

The cache reads from the primary and reads and writes with majority concern, writes also journaled, whatever the
client's defaults are. A write the store has confirmed therefore survives a failover and a crash. Majority writes also
keep changes fast: change streams deliver only majority-committed writes, so with `w:1` every change would wait for a
periodic journal flush and arrive about a hundred times later.

## Collection and change streams

The collection and its index are created when the first cache is built. A collection created by another version of this
library is refused. `withSchemaReset()` is meant for the migration from 0.x: it drops and recreates a collection of 0.x
(or an empty one), never one of another layout version from 0.8 on or one holding other documents. During a rolling
update, the instances of 0.x still running go on among themselves, apart from the new ones, so their values can be
outdated until the last of them is stopped. Documents they write after the drop are removed when a cache is built, with
or without `withSchemaReset()`; this cleanup is temporary and goes away in a later release. Remove `withSchemaReset()`
once the migration is done.

The caches on one client share one change stream per database, whatever the number of caches and collections.

The user needs the built-in role `readWrite` on the database (through `mongos`, also `read` on the `config` database);
without it, building a cache fails naming the role.

## Timeouts

The cache's operation timeout (`withOperationTimeout(...)`, 10 seconds by default) becomes the driver's client-side
operation timeout for the cache's own operations, so caches sharing a client may have different ones, and the driver
retries a write within it while the primary changes. A timeout set on the client itself (the connection string option
`timeoutMS`) applies instead. The driver's wait for a pooled connection is covered by the timeout as well.

The timeout does not bound sending: Java sockets have no write timeout, so with the driver's default transport a write
too large for the socket's buffer, such as a large `putAll`, sent to a primary that vanished without closing the
connection (a crashed host, a cut network), waits until the operating system gives up (about 15 minutes on Linux,
`net.ipv4.tcp_retries2`) and fails only then ([JAVA-1269](https://jira.mongodb.org/browse/JAVA-1269)).

## Failovers

While the primary changes, writes are retried by the driver until the operation timeout runs out, so a timeout above the
duration of an election lets them through. The default of 10 seconds covers a planned step-down, but not always a
primary that fails, which the others notice only after `electionTimeoutMillis` (10 seconds by default); 30 seconds lets
writes ride that out as well. Confirmed writes survive the failover (see [Consistency](#consistency)). On MongoDB 4.4.0,
a new primary waits for catching up with an unreachable old one indefinitely by default; `catchUpTimeoutMillis` bounds
that. A cache built while the replica set has no primary, such as without the majority of its members, starts without
its store and connects once one is elected (with `withFailFast()` the build fails).

## Change history

If an instance falls behind further than the oplog reaches, its change stream cannot resume. The instance notices,
checks what it holds against the store and goes on, which costs loads, never a wrong answer. A larger oplog
(`minSnapshotHistoryWindowInSeconds` on MongoDB 5.0 or newer, or the oplog size) lets instances fall further behind.

## Managed and cloud databases

MongoDB Atlas and other managed replica sets are replica sets like any other, so the above applies (not tested against
them). Where an endpoint is switched over by DNS, `networkaddress.cache.ttl` of 60 seconds or less in the JVM lets
reconnecting reach the new primary in time. Services that only speak MongoDB's protocol need change streams and majority
reads; where those are missing, building a cache fails.
