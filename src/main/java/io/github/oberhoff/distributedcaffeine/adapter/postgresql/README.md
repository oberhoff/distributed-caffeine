# PostgreSQL adapter

Details of the PostgreSQL adapter beyond its configuration, which the
[main README](../../../../../../../../../README.md) describes. Distribution uses PostgreSQL's `LISTEN`/`NOTIFY`, which
keeps a connection open for listening. In short:

* Any data source that reaches the primary works (versions: see [Requirements](../../../../../../../../../README.md#requirements)).
* Behind a pooler in transaction mode, listening needs a direct or session-mode connection, specified using
  `withListenerDataSource(...)`.
* Building a cache checks both, and the server's version, and fails right away with an explanation if one is not met.

## Table

The table, its index and a sequence for its versions are created when the first cache is built. If the user lacks the
rights to create them, building fails with the DDL to run instead, which also suits migration tools. A table created by
another version of this library is refused. `withSchemaReset()` is meant for the migration from 0.x: it drops and
recreates a table of 0.x (or an empty one), never one of another version of this generation or one holding other rows.
Instances of 0.x still running fail on the new table, so stop them before the first new instance starts, or accept
their errors until they are replaced. Remove `withSchemaReset()` once the migration is done.

A table's `key` column holds one format and its `value` column one format (bytes, text or JSON, as `bytea`, `text` or
`jsonb`), after what the key and value serializers of its first cache store; the two may differ. Caches sharing a table
need serializers of the same formats, and a cache whose serializers switch format needs another table (removing the
values with `invalidateAll()` is not enough); building refuses a table created for other formats.

## Names

The schema and the table names have up to 63 bytes each and are quoted, so their case is kept (`Cache` and `cache` are
two tables); the schema is created if missing. The discriminator is not blank. A name outside these is rejected when
the adapter is built.

## Listening

The caches on one data source (the same object) share one connection for listening, held for as long as they listen. A
pool's leak detection (such as HikariCP's `leakDetectionThreshold`) reports it as a leak, which it is not. Every
`NOTIFY` wakes every listening session in the database, at about 33 µs of PostgreSQL CPU each, so sharing the data
source keeps that cost to one session per application instance.

## Connection poolers

Listening needs a session of its own. A pooler in transaction mode lends a connection for a single transaction only, so
it cannot be listened on. Such poolers are common in front of managed cloud databases and database clusters, often as
their default endpoint. In that case, keep the pooler for reading and writing and specify a direct or session-mode
connection for listening using `withListenerDataSource(...)`. If listening does not work, building a cache fails right
away.

Through a pooler in transaction mode, the prepared statements of the PostgreSQL JDBC driver need the pooler to support
them. PgBouncer does from version 1.21 with `max_prepared_statements` above zero, which is the default from version
1.24, while several managed poolers ship with it disabled (see
[Managed and cloud databases](#managed-and-cloud-databases)). Otherwise `prepareThreshold=0` on the data source turns
them off.

## Standby servers

Reading has to reach the primary, because a standby may not yet have what the primary has notified. A data source
reaching a standby, always or for some connections only (for example with several hosts and `targetServerType=any` in
the connection URL, or a load balancer in front of the servers), fails building a cache or the operations that reach it;
each connection is checked once. `targetServerType=primary` in the connection URL keeps it on the primary. What the
check cannot see is a load balancer that routes every statement on its own, such as Pgpool-II with statement-level load
balancing, because the check and the read may then reach different servers — the connections of the caches have to stay
on the primary there.

## Timeouts

The cache's operation timeout (`withOperationTimeout(...)`, 10 seconds by default) becomes the network timeout of each
connection borrowed for an operation, restored before the connection goes back, so an operation waiting for an answer
on a connection the network silently stopped carrying fails in time, and caches sharing a data source may have
different timeouts. The wait for a pooled connection is the pool's own (such as HikariCP's `connectionTimeout`, 30
seconds by default): set it no higher than the operation timeout, or an exhausted pool keeps callers waiting longer. A
listening connection that dies silently is detected by the cache's heartbeat and replaced.

The network timeout bounds waiting for an answer, not sending: Java sockets have no write timeout, so a write too large
for the socket's buffer, such as a large `putAll`, sent to a server that vanished without closing the connection (a
crashed host, a cut network), waits until the operating system gives up (about 15 minutes on Linux,
`net.ipv4.tcp_retries2`) and fails only then.

## Failovers

Writes fail while there is no primary. Once listening resumes on the new primary, the instances check what they hold
against it.

With asynchronous replication, the default, a failover can lose writes the primary had already confirmed; the instances
drop them as well. The same holds for a server that runs with `synchronous_commit = off` and crashes. With synchronous
replication nothing confirmed is lost, but every write waits for the standby, so the operation timeout bounds writes
that would otherwise wait for as long as the standby is gone.

## Managed and cloud databases

Managed PostgreSQL services offer the same few ways of connecting, and what matters is which of them is used for what.
Reading and writing may go through a pooler in transaction mode, listening needs a session of its own, and both have to
reach the primary.

| Service                 | Reading and writing                                 | Listening                          |
|-------------------------|-----------------------------------------------------|------------------------------------|
| Cloud SQL, AlloyDB      | direct, Auth Proxy, Java Connector, managed pooling | direct, Auth Proxy, Java Connector |
| RDS, Aurora             | instance or writer endpoint, RDS Proxy              | instance or writer endpoint        |
| Azure (Flexible Server) | port 5432, built-in PgBouncer (port 6432)           | port 5432                          |
| Supabase                | direct, session (5432) or transaction pooler (6543) | direct, session pooler (5432)      |
| Neon                    | direct or pooled (`-pooler`) hostname               | direct hostname                    |

* Managed pooling: Cloud SQL (Enterprise Plus edition, on port 6432, or 3307 through the Auth Proxy) and AlloyDB (on
  port 6432) pool in transaction mode by default, so listening has to bypass it.
* Prepared statements: The JDBC driver prepares a statement on the server once it has run five times, which a pooler in
  transaction mode has to support. The managed poolers of Cloud SQL, AlloyDB and Azure ship with
  `max_prepared_statements` at 0, and Supabase's transaction pooler does not support them, so there either the setting
  is raised or `prepareThreshold=0` is set on the data source. Neon's pooler supports them.
* Read replicas: Reader endpoints and read replicas are standbys, which are refused for reading as well as for listening
  (see [Standby servers](#standby-servers)).
* RDS Proxy: Listening through it works, but pins the session to a server connection for as long as it listens, so the
  instance or cluster endpoint is the better choice for it.
* Failovers: Where these services offer high availability, they replicate synchronously or at the storage level, so a
  failover loses no confirmed writes (see [Failovers](#failovers)). Several services switch the endpoint over by DNS,
  which the JVM caches: `networkaddress.cache.ttl` of 60 seconds or less lets reconnecting reach the new primary in
  time.
* Authentication: Short-lived credentials, such as IAM authentication on Google Cloud or AWS, work as long as the data
  source obtains them for every new connection, as the Cloud SQL Java Connector and the AWS Advanced JDBC Wrapper do,
  because listening reconnects through the data source as well.
* Wrapping drivers: Listening uses the connection of the PostgreSQL JDBC driver underneath, so a wrapping driver or
  instrumentation on the listener data source has to support `unwrap(...)` to it, as common ones do.
* Scaling to zero: An open listening connection keeps a serverless database (Neon, Aurora Serverless v2 scaled to zero
  capacity) from suspending for as long as a cache instance listens.
* Compatible databases: Databases speaking the PostgreSQL protocol without supporting `LISTEN`/`NOTIFY`, such as
  CockroachDB, make building a cache fail.
