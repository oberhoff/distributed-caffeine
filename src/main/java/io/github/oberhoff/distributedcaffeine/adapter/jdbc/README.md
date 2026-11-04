# MariaDB and MySQL adapters

Details of the `MariaDbAdapter` and the `MySqlAdapter` beyond their configuration, which the
[main README](../../../../../../../../../README.md) describes. Neither database announces changes to its clients, so
distribution works by polling. Building a cache fails right away with an explanation if the setup cannot support it.

## Databases and drivers

* `MariaDbAdapter`: MariaDB 10.2 or newer, through MariaDB Connector/J (`org.mariadb.jdbc:mariadb-java-client`). MySQL
  Connector/J is refused on MariaDB, as neither Oracle nor MariaDB supports it there.
* `MySqlAdapter`: MySQL 5.7 or newer, or a build of it such as Percona Server, through MySQL Connector/J
  (`com.mysql:mysql-connector-j`) or MariaDB Connector/J (`org.mariadb.jdbc:mariadb-java-client`).

Each adapter refuses the other database, naming the right adapter. A driver of another name, such as a wrapper around
one of these, is accepted but untested. The table uses the character set `utf8mb4`; older versions than these cannot
index it.

## Table

The table and its index are created if missing; several caches can share a table, told apart by their discriminator. The
table name consists of letters, digits and underscores and does not start with a digit.

## Polling

Every poll interval (`withPollInterval(...)`, one second by default), each instance reads the changes made since its
last poll, so changes made elsewhere arrive within about an interval, at one query per interval and instance. A shorter
interval delivers changes sooner. The interval must be at most half the heartbeat interval
(`withHeartbeatInterval(...)`, ten seconds by default), which is checked when the cache is built. While polls fail, the
instance's state is `DEGRADED`, and after the next successful poll it checks what it holds against the store.

## Timeouts

The cache's operation timeout (`withOperationTimeout(...)`, 30 seconds by default) becomes the network timeout of each
connection borrowed for an operation, restored before the connection goes back, and the query timeout of each statement
(rounded up to whole seconds), so caches sharing a data source may have different ones. The wait for a pooled connection
is the pool's own (such as HikariCP's `connectionTimeout`, 30 seconds by default): set it no higher than the operation
timeout, or an exhausted pool keeps callers waiting longer.

## Replication and failovers

A failover that promotes an asynchronous replica, the default replication of both databases, may lose writes the primary
had confirmed. The instances notice that another server answers (each poll reads the server's `server_id`, which is
distinct on every server that replicates) and check what they hold against the store, so they drop the lost writes as
well. MySQL's semi-synchronous replication (`AFTER_SYNC`) loses no confirmed write.

Galera Cluster, where several servers take writes, is not supported: it can lose updates even without faults, which
breaks the conditional writes the cache relies on.
