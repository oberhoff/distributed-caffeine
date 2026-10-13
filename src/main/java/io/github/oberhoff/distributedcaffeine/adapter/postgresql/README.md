# PostgreSQL adapter

Details of the PostgreSQL-based adapter beyond its configuration, which the
[main README](../../../../../../../../../README.md) describes. Synchronization uses PostgreSQL's `LISTEN`/`NOTIFY`,
which keeps a connection open for listening. In short:

* Any data source that reaches the primary works.
* Behind a pooler in transaction mode, listening needs a direct or session-mode connection, specified using
  `withListenerDataSource(...)`.
* Starting synchronization checks both and fails right away with an explanation if either is not met.

## Listener sharing

How cache instances (using the same data source) share connections for listening is specified using
`withListenerSharingMode(...)`: `DATABASE` for one connection per database (default), `TABLE` for one per table or
`INSTANCE` for one per cache instance. A single connection delivers notifications promptly up to the order of a
thousand per second, so write-heavy cache instances are better spread over several connections.

## Connection poolers

Listening needs a session of its own. A pooler in transaction mode lends a connection for a single transaction only, so
it cannot be listened on. Such poolers are common in front of managed cloud databases and database clusters, often as
their default endpoint. In that case, keep the pooler for reading and writing and specify a direct or session-mode
connection for listening using `withListenerDataSource(...)`. If listening does not work, starting synchronization fails
right away. The same goes for a pooler in statement mode, which refuses the transaction every write needs, because a
cache entry and its notification are committed together.

Through a pooler in transaction mode, the prepared statements of the PostgreSQL JDBC driver need the pooler to support
them. PgBouncer does from version 1.21 with `max_prepared_statements` above zero, which is the default from version
1.24, while several managed poolers ship with it disabled
(see [Managed and cloud databases](#managed-and-cloud-databases)). Otherwise `prepareThreshold=0` on the data source
turns them off.

## Standby servers

Reading has to reach the primary, because a standby may not yet have what the primary has notified. A data source
reaching a standby, always or for some connections only (for example with several hosts and `targetServerType=any` in
the connection URL, or a load balancer in front of the servers), fails building an adapter or the reads that reach it.
`targetServerType=primary` in the connection URL keeps it on the primary. What the check cannot see is a load balancer
that routes every statement on its own, such as Pgpool-II with statement-level load balancing, because the check and the
read may then reach different servers - the connections of the cache instances have to stay on the primary there.

## Broken connections

A listening connection that dies silently, for example during a failover, is detected and replaced automatically.
Operations for reading and writing on such a connection have no time limit by default, so they only fail once the
operating system gives up on the connection. If that is too long, `socketTimeout` can be set on the data source to fail
them sooner, and `loginTimeout` to do the same for opening new connections.

## Failovers

Cache instances stay consistent. Writes fail with an exception while there is no primary, because the driver does not
retry them. Once listening resumes on the new primary, cache instances reconcile against it, which leaves them empty
unless cached entries are persisted.

With asynchronous replication, the default, a failover can lose writes the primary had already confirmed; cache
instances drop them as well. The same holds for a server that runs with `synchronous_commit = off` and crashes. With
synchronous replication nothing confirmed is lost, but every write waits for the standby, so `socketTimeout` bounds
writes that would otherwise wait for as long as the standby is gone.

## Server timeouts

Reading the whole table back, as a cache instance does when it starts or reconciles, runs in a single transaction of
about half a second per 100,000 cache entries. A `transaction_timeout` (PostgreSQL 17 or newer) has to allow for that.
Between two batches of the read the transaction is idle for a few milliseconds only, so
`idle_in_transaction_session_timeout` or a pooler's idle transaction timeout does not interfere unless set that low.

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
* Read replicas: Reader endpoints and read replicas are standbys, which are refused for reading as well as for
  listening (see [Standby servers](#standby-servers)).
* RDS Proxy: Listening through it works, but pins the session to a server connection for as long as it listens, so the
  instance or cluster endpoint is the better choice for it.
* Failovers: Where these services offer high availability, they replicate synchronously or at the storage level, so a
  failover loses no confirmed writes and cache instances reconcile afterwards (see [Failovers](#failovers)). Several
  services switch the endpoint over by DNS, which the JVM caches: `networkaddress.cache.ttl` of 60 seconds or less lets
  reconnecting reach the new primary in time.
* Authentication: Short-lived credentials, such as IAM authentication on Google Cloud or AWS, work as long as the data
  source obtains them for every new connection, as the Cloud SQL Java Connector and the AWS Advanced JDBC Wrapper do,
  because listening reconnects through the data source as well.
* Wrapping drivers: Listening uses the connection of the PostgreSQL JDBC driver underneath, so a wrapping driver or
  instrumentation on the listener data source has to support `unwrap(...)` to it, as common ones do. Reading and
  writing work without, at the cost of checking every read for a standby rather than every connection once.
* Scaling to zero: An open listening connection keeps a serverless database (Neon, Aurora Serverless v2 scaled to zero
  capacity) from suspending for as long as a cache instance listens.
* CPU allocation: Listening, its heartbeat and maintenance run in the background, so platforms that allocate CPU only
  while a request is handled (such as Cloud Run with request-based billing) have to allocate it always.
* Compatible databases: Databases speaking the PostgreSQL protocol without supporting `LISTEN`/`NOTIFY`, such as
  CockroachDB, make starting synchronization fail.
