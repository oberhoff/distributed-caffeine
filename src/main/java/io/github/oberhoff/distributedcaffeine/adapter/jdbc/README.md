# MariaDB and MySQL adapters

Details of the `MariaDbAdapter` and the `MySqlAdapter` beyond their configuration, which the
[main README](../../../../../../../../../README.md) describes. Neither database announces changes to its clients, so
distribution works by polling. Building a cache fails right away with an explanation if the setup cannot support it.

## Databases and drivers

* `MariaDbAdapter`: MariaDB, through MariaDB Connector/J (`org.mariadb.jdbc:mariadb-java-client`). MySQL Connector/J is
  refused on MariaDB, as neither Oracle nor MariaDB supports it there.
* `MySqlAdapter`: MySQL, or a build of it such as Percona Server, through MySQL Connector/J
  (`com.mysql:mysql-connector-j`) or MariaDB Connector/J (`org.mariadb.jdbc:mariadb-java-client`).

Each adapter refuses the other database, naming the right adapter, and a version older than supported (see
[Requirements](../../../../../../../../../README.md#requirements); the table uses the character set `utf8mb4`, which older versions
cannot index). A driver of another name, such as a wrapper around one of these, is accepted but untested.

## Table

The table and its index are created if missing; several caches can share a table, told apart by their discriminator. The
table name consists of letters, digits and underscores and does not start with a digit; the discriminator is not blank
and has up to 200 characters. A name outside these is rejected when the adapter is built. If the user lacks the right to
create the table, building fails with the DDL to run instead, which also suits migration tools. A table without this
version's schema marker (a table comment) is refused, naming the DDL; there is no `withSchemaReset()`, as there is no
table of 0.x to migrate from.

Keys and values are stored as bytes whatever their format (bytes, text or JSON), so caches with different serializers
can share a table.

## Polling

Every poll interval (`withPollInterval(...)`, one second by default), each instance reads the changes made since its
last poll, so changes made elsewhere arrive within about an interval, at one query per interval and instance. A shorter
interval delivers changes sooner. The interval must be at most half the heartbeat interval
(`withHeartbeatInterval(...)`, ten seconds by default), which is checked when the cache is built. While polls fail, the
instance's state is `DEGRADED`, and after the next successful poll it checks what it holds against the store.

## Timeouts

The cache's operation timeout (`withOperationTimeout(...)`, 10 seconds by default) becomes the network timeout of each
connection borrowed for an operation, restored before the connection goes back, and the query timeout of each statement
(rounded up to whole seconds), so caches sharing a data source may have different ones. The wait for a pooled connection
is the pool's own (such as HikariCP's `connectionTimeout`, 30 seconds by default): set it no higher than the operation
timeout, or an exhausted pool keeps callers waiting longer.

## Replication and failovers

A failover that promotes an asynchronous replica, the default replication of both databases, may lose writes the primary
had confirmed. The instances notice that another server answers (each poll reads the server's `server_id`, which is
distinct on every server that replicates) and check what they hold against the store, so they drop the lost writes as
well. MySQL's semi-synchronous replication (`AFTER_SYNC`) loses no confirmed write.

Galera Cluster (MariaDB Galera Cluster, Percona XtraDB Cluster) is refused when a cache is built. Galera can lose
updates and serve stale reads across nodes even in a healthy cluster, as Jepsen found in 2026 and MariaDB now documents
among its known limitations; that would break the conditional writes the cache relies on. The fixes (MDEV-38977,
MDEV-38976) were still open in November 2026. A single writer behind a proxy is untested.

## Managed and cloud databases

Managed MariaDB and MySQL services (such as Amazon RDS and Aurora, Azure Database for MySQL, or Cloud SQL) are servers
like any other, so the above applies (not tested against them). Reading and writing have to reach the primary or writer
endpoint; read replicas and reader endpoints take no writes. Where an endpoint is switched over by DNS,
`networkaddress.cache.ttl` of 60 seconds or less in the JVM lets reconnecting reach the new primary in time. Polling
costs one query per poll interval and application instance, also while nothing changes, which services billed per
request count.
