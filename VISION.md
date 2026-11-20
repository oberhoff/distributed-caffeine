# Distributed Caffeine — vision

The design of the library, the decisions behind it and the work still open, kept current as the library is improved and
maintained. Measured by the experiments in `src/test/java/.../experiments`; what users need is in the README and the
adapters' READMEs. Open work, by kind and status: "Open work" below.

## Why this design

The instances do not keep their *contents* in step; they keep their *copies* correct. Each instance holds whatever
subset of keys it read, and a change made elsewhere only drops or replaces a copy it holds. Two facts make keeping
contents in step the wrong goal:

- Caffeine evicts per instance (W-TinyLFU decides from local access, access and variable expiry are local), so a
  distributed eviction would turn a local decision into a cluster-wide one.
- A value's age has to travel with it: a value read back from the store must not get a fresh lifetime, or it could
  circulate between an instance and the store for ever. So only a write sets a value's write time.

Keeping contents in step is also where a distributed cache's hardest defects come from (eviction echoes, races when
restoring, entries lost silently) and where most of its configuration would go. The lessons are kept as tests and as
the checklist under "Lessons checklist".

## Model

Two levels, as in JetCache:

- **L1** — a Caffeine cache per instance (internally an `AsyncCache` for every variant, see "Shared loads"). The
  user's Caffeine settings (size, weight, every kind of expiry, refresh, stats, listeners) work and stay strictly
  local; only weak and soft references are rejected. An instance holds whatever subset of keys it read.
- **L2** — a shared store (a MongoDB collection, a PostgreSQL table, a Redis keyspace, a MariaDB or MySQL table).
  Every row carries the value, its state, its **write time** and a **version** that orders writes per key. L2 has its
  own bounds: a maximum age counted from the write and a maximum size (oldest writes pruned first).
- **Distribution** — change streams, `LISTEN`/`NOTIFY`, Redis Streams, or polling (MariaDB, MySQL) carry
  invalidations (the default), optionally population, or nothing at all (`DistributionMode`, see "Distribution
  modes").

Operations:

| Operation                              | Behaviour                                                                                                                                                        |
|----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| read                                   | L1 → L2 (only if younger than the maximum age) → loader / mapping function, then write L2 (conditionally) and L1; without a loader (`getIfPresent`) L1 → L2 only |
| refresh                                | source via `CacheLoader.reload`, then write L2 (conditionally) and distribute; never from L2                                                                     |
| write                                  | write L2, then distribute; L1 updated                                                                                                                            |
| invalidate                             | write L2 (removal), then distribute; L1 removed                                                                                                                  |
| local eviction                         | L1 only. Never distributed, never changes L2                                                                                                                     |
| received invalidation or expiry        | drop the key from L1                                                                                                                                             |
| received write, `UPDATE`               | **only if the key is held** and older: read the row (batched) and apply it; never adds a key                                                                     |
| received write, `INVALIDATE` (default) | drop the key if held                                                                                                                                             |
| `NONE`                                 | nothing is received: a copy changed elsewhere stays until Caffeine expires or evicts it                                                                          |

Two lifetimes, deliberately separate: Caffeine's expiry is local memory management, untouched by the library; the
L2 maximum age is how old a value may get anywhere. The store enforces it on reads and distributes it as an
expiry, so L1 copies are dropped too (see "Maximum age").

## Invariants

1. Correctness never depends on which keys an instance holds. Any subset is valid.
2. Local eviction (size, weight, expiry, reference) never produces a message and never changes L2.
3. L2 is the only shared state. Distribution is a hint to drop or replace a held copy, never the data of record.
4. A value older than the L2 maximum age is never served from L2 (checked in the query). L1 copies of it are
   dropped at the latest one pruning interval plus delivery later, however Caffeine is configured; with
   `DistributionMode.NONE` by Caffeine's expiry after write, which defaults to the maximum age.
5. Moving between levels never makes a value younger: only a write sets the write time; reading a row into L1
   never rewrites it, and an L1 copy is never written back.
6. Per key, an older write never replaces a newer one, in L2 or in L1, and a load overtaken by an invalidation or
   a newer write is discarded.
7. After any gap in distribution (reconnect, history lost, restart), held keys count as unconfirmed until checked
   against the store: reads check first, and a background pass drops what does not match (see "States and
   recovery").
8. Population never adds a key to an instance that does not hold it.

## Scope

In scope:

- feature parity with Caffeine's `Cache`, `LoadingCache`, `AsyncCache` and `AsyncLoadingCache` on every user-facing
  method, including `asMap()`, `synchronous()`, `policy()` and `stats()` (see "Feature parity with Caffeine")
- a distributed policy next to Caffeine's `policy()`: the synchronization state and a view of the store
  (`store().get`/`getAll`, its bounds at runtime, `mark()`, `invalidateAllBeforeMark`, `invalidateAllOlderThan`), and
  `distributedStats()` next to `stats()`
- a global `invalidateAll()`
- adapters behind a thin SPI: MongoDB, PostgreSQL, Redis (also Valkey, Dragonfly, Kvrocks; through Lettuce, Jedis or
  Redisson), MariaDB and MySQL
- distribution modes: `INVALIDATE` (default), `UPDATE`, `NONE`
- optional negative caching (a local set of recent misses)
- lifecycle: `build()` fails fast, `close()` (`AutoCloseable`)
- the store guard: a degraded state while the store does not answer, reads and writes guarded

Out of scope (and why):

- eviction distribution and contents synchronization: see "Why this design"
- an evicted tier and record statuses: a row is a value with a write time, nothing more
- configurable loading strategies: read-through L2 is always on
- a mode without a store: L2 is the model

## Configuration

As the README shows it, with every optional part:

```java
DistributedLoadingCache<Key, Value> cache = DistributedCaffeine.newBuilder(adapter)
        .withCaffeine(caffeine -> caffeine              // Caffeine's builder methods, one to one
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofMinutes(5)))
        .withSerializers(serializers -> serializers     // optional, default Fory; per cache
                .withKeySerializer(StringSerializer.ofString())
                .withValueSerializer(new JacksonSerializer<>(Value.class, true)))
        .withStore(store -> store
                .withMaximumAge(Duration.ofHours(1))
                .withMaximumSize(1_000_000))
        .withDistributionMode(DistributionMode.UPDATE)  // optional: INVALIDATE by default
        .withNegativeCaching(Duration.ofMinutes(1))     // optional: local set of recent misses
        .build(key -> source.load(key));                // a DistributedLoadingCache, AutoCloseable
```

No validation between Caffeine's expiry and the maximum age is needed: the maximum age is enforced independently
(see "Maximum age").

## How we work on it

The rules every change follows, beside the frame of "Positioning and the complexity budget":

1. **Robustness before performance.** Correctness under store failures and odd races comes before speed. A
   performance idea goes into "Open work" as *measure first* and is built only for a measured gap.
2. **The complexity is checked early.** Before a core mechanism is built, it is stated whether it gives a full
   guarantee or a partial one; for a partial one, "accept and document" is an option from the start. A core change
   counts as working only once the property test passes on every in-memory store profile.
3. **SPI changes are welcome while on 0.x.** Where changing the SPI makes a design cleaner or more usable (a default
   method that lets the core do the work for every adapter), it is proposed instead of worked around.
4. **Code comments explain the critical parts and the hard-won decisions,** so that the code reads without this
   document.
5. **Driver versions are named in the README's Requirements only** (majors or minors). Exception messages name the
   artifact without a version; server version floors in a refusal ("Redis 6.2 or later") may stay.
6. **Before a change is done:** it compiles once with Error Prone (`-Werror`), the Javadoc builds, SonarQube reports no
   issue (a false positive suppressed with `@SuppressWarnings("java:S…")` and a reason), and IntelliJ's inspections
   are clean.
7. **Testing:** while working, the affected tests on the latest image of each store; the nightly full matrix on
   GitHub is the reference run, and local matrix runs fill the gaps of a change that needs them before the nightly.

## Open work

What is left to do, by kind and status, kept current: done items leave the list, new ones join it. Each is judged on
its own, in the frame of "Positioning and the complexity budget". Status: *next*, a planned step; *in a later
release*, due once its condition holds; *measure first*, performance waiting for a measurement off Docker Desktop;
*waits for upstream releases*; *upstream reports*, ours to file, the user's call; *parked*, decided not now, with its
reasons; *on demand*, only if a user asks for it or hits it. Done items move to History ("Built from the list of
later work").

### Robustness

*In a later release:*
- **remove the cleanup of 0.7.x documents** on MongoDB (`MongoSchema#removeOldGeneration`, its test in
  `MongoAdapterTest` and the step in `MigrationExperimentTest`, the sentence in the MongoDB README; see "Schema
  markers and the reset", built 2026-11-07 as temporary) in a later release, once no rollout from 0.7.x is expected.

*Waits for upstream releases:*
- recheck Dragonfly's failover modes with later releases (recorded 2026-11-02, by the user: "check in future
  releases of libs and dragonfly"; see "Redis adapters", item 18). Not supported as of Dragonfly 2.0.2, Lettuce
  7.9.0, Jedis 8.0.2, Redisson 4.8.0: failover inside a Dragonfly cluster (Dragonfly's old primary answers
  `CLUSTER NODES` without itself for seconds after a takeover; Lettuce's refresh throws on that, Jedis's slot cache
  stays stale; Redisson rides it out) and Dragonfly behind Sentinel (undocumented by Dragonfly; a killed primary
  not always failed over). To recheck: a matrix entry `RedisClusterTests` with
  `RedisCluster.Kind.DRAGONFLY_REPLICATED` (the tests act as Dragonfly's operator), and one `RedisSentinelTests` on a
  Dragonfly image (`RedisSentinel` sets Dragonfly up already); run each a few times, with
  `-Dsurefire.skipAfterFailureCount=1` for the first failure;
- Redisson's silent-connection window (recorded 2026-11-02, left as it is by the user's choice, not to be
  forgotten): Redisson replaces a connection the network stopped carrying only when its ping fails
  (`pingConnectionInterval`, 30 s by default); commands time out within the cache's bound meanwhile, the cache
  serving what it holds. Documented in `RedissonRedis` (a shorter interval advised). Not possible without
  reflection: dropping the connection (no API), warning on a long interval (the server configs' getters are
  protected), setting it (the client is the user's). Revisit if Redisson offers either;

*Upstream reports (the user's call):*
- pgjdbc: a public way to read notifications without `processNotifies`' overhead (its 1 ms peek is hard-coded; see
  "PostgreSQL listening: pgjdbc's stream taken over");
- Dragonfly: `CLUSTER NODES` without `myself` mid-takeover ("Redis adapters", item 18);
- Kvrocks 2.17.0 crashing on `ROLE` while it resynchronises as a demoted replica ("Redis adapters", item 17).

*Parked:*
- the repair of loads, refreshes and atomic map methods racing `invalidateAll` (see "`invalidateAll`
  by the store's time", the repair): built and reverted 2026-11-05; options B (no hot path, a read per atomic
  method) and C (the control row checked in the store's write, free on Redis) recorded there. Accepted until a user
  needs it, as Caffeine (loads), Redis, Hazelcast and FusionCache accept it for loads;
- size pruning in place instead of a silent delete (recorded 2026-10-29, for a later discussion; the silent delete
  was kept with a grace for young rows, see "In-memory store profiles", dimension 0, which leaves only a load
  slower than the grace): a pruned row keeps a marker, no value, state expired, at
  its own version (no new one), deleted later with the old tombstones. Holders of that version keep their copy
  (equal, never dropped), holders of an older one drop it (they missed its last write, which a poll now sees), a
  load's write on "missing" is refused by the marker, and the maximum size still holds (markers carry no value).
  Closes both limits; costs an update and a message per pruned row instead of a delete, and the contract of
  `pruneToSize` in every store. Deferred 2026-11-01: a message per pruned row on every instance for a rare case;
- the grace size pruning spares young rows (recorded 2026-10-29, for a later discussion; built as `max(4 s, 2 ×
  poll interval + 2 s if polled)`, see "In-memory store profiles", dimension 0): with a feed or without
  distribution it only protects slow loads, and 4 s leaves a load slower than that, meeting a cache that turns over
  its whole maximum size meanwhile, stale up to the maximum age. Longer (a minute)
  covers slower loads at the price of the store exceeding its maximum size by that much more while
  thrashing; or tied to something measured (the loads' durations);
- a periodic check of the local copies against the store, in case an announcement was missed (recorded
  2026-10-29, for a later discussion): today a copy goes by a message, a gap (feed or heartbeat), its local expiry
  (now at the store's maximum age at the latest, dimension 7) or size eviction; maintenance works on the store
  only. A check would bound a missed message by its round instead of the maximum age, at a version read of
  everything held per round. Deferred 2026-11-01: steady load on every instance for a rare case;
- **A deadline of the core's own for blocking store calls** (found 2026-11-09). Java sockets have no write timeout, so
  a driver writing through a blocking socket (the JDBC drivers, MongoDB's default transport, Jedis) cannot bound
  sending: a write too large for the socket's buffer, sent to a server that vanished without closing the connection,
  waits until the operating system gives up (about 15 minutes on Linux, `net.ipv4.tcp_retries2`). Seen in
  `MongoFailoverTest` (a 20,000-key `putAll` when the primary is killed: the thread stuck in the driver's
  `SocketStream.write`, the driver checking `timeoutMS` only after a write returns). MongoDB knows it
  ([JAVA-1269](https://jira.mongodb.org/browse/JAVA-1269), backlog since 2014;
  [JAVA-4664](https://jira.mongodb.org/browse/JAVA-4664); [JAVA-6302](https://jira.mongodb.org/browse/JAVA-6302)).
  Consistency holds (no lock held while writing; a late failure has an unknown outcome and drops the keys; a late
  arrival after a healed network is refused by its version if anything newer was written); what breaks is the
  bound on the caller's wait, so it is documented (`withOperationTimeout`, each adapter's Timeouts). The fix: run
  each blocking store call under a deadline (the operation timeout plus a margin) and, past it, fail the caller with
  an unknown outcome (the guard counts it) and leave the stuck thread to fail on its own, as Hystrix's thread
  isolation and Resilience4j's `TimeLimiter` do. Costs: a thread hop per store call (measure it, with platform and
  virtual threads), and a thread and pooled connection held until the operating system gives up. The Netty
  transports bound sending themselves (Lettuce, Redisson, MongoDB's `TransportSettings.nettyBuilder()`), but are not
  required of users.
  **Measured 2026-11-17** (`StoreDeadlineBenchmark`: the end-to-end benchmark direct, then every store call on a
  thread of a cached pool and on a virtual thread, waited for with a deadline; PostgreSQL 18, MongoDB 9, MariaDB 13 in
  local containers, both orders, the machine otherwise idle): on platform threads the hop stays within the noise
  between runs (latency within 10 %, throughput +1 to -7 %; one store measured direct in the two orders differs by up
  to 25 %); virtual threads somewhat worse (up to -10 %, once -23 % throughput on MongoDB). A first run with the
  machine busy, and the direct run always first, had suggested a cost of 10 to 20 %: the run order (JIT warm-up, data
  growing between runs) and the load, not the hop. So a deadline on every blocking store call, on platform threads,
  costs about nothing; to design before building: which adapters need it (the blocking ones), the threads a stuck call
  holds, and thread-bound context of the caller (a transaction bound to the user's `DataSource`).
  **Decided 2026-11-17: not built, documented instead** (option C). Designed: a core wrapper around the adapter's
  store, every adapter alike, a dedicated JVM-wide cached pool of daemon platform threads (not the user's store
  executor, which may be bounded), the deadline twice the operation timeout so that the drivers' own timeouts still
  report first, the stuck thread not interrupted. Its caveats outweigh the rare case: the documented bound becomes twice
  the timeout; every store call moves to another thread (a `DataSource` handing out the caller's transactional
  connection would be decoupled from the transaction, thread-local context lost); a call merely slow becomes an unknown
  outcome; stuck threads and connections are still held until the operating system gives up. The case is defused by
  configuration at its source, so each blocking adapter's README names the remedies: MongoDB's Netty transport, a lower
  `net.ipv4.tcp_retries2` (8, the lowest RFC 1122 allows, about 100 s), smaller batches. Revisit only if a user hits it.
  Found while designing: `PostgresStore`/`JdbcStore` commit a connection whose auto-commit is off, which with a
  `DataSource` handing out the caller's transactional connection (Spring's `TransactionAwareDataSourceProxy`) could
  commit the caller's transaction early. **Verified 2026-11-17** (an experiment with Spring 6.2 as a test dependency, both
  removed again; the regression test needs no Spring, see the plan): a `cache.put` inside a Spring transaction rolled back afterwards, the cache given a
  `TransactionAwareDataSourceProxy` over a Hikari pool: the application's row inserted before the put survived the
  rollback, the one after it did not, the cache's write stayed. `JdbcStore` (MariaDB, MySQL) commits the same way.
  Standard setups are not affected (a plain pool hands out a connection of its own; Spring's transactions bind a
  connection only for code asking through Spring); application servers with JTA forbid the commit on an enlisted
  connection, so there cache writes inside a transaction fail loudly instead. Joining the caller's transaction is no
  fix (the write rolled back while this instance holds it, and others have heard of it); nested transactions neither
  (savepoints commit with the outer transaction; PostgreSQL, MariaDB and MySQL have no autonomous ones). JDBC cannot
  tell whether a transaction is open (auto-commit off is also a legitimate pool setting); the databases report it in
  their protocols, pgjdbc exposes it (`getTransactionState()`), the MariaDB and MySQL drivers only internally.
  **Plan (decided 2026-11-17), built the same day:** the rule in the JDBC adapters' READMEs (a plain pool, never a
  transaction-aware data source); PostgreSQL refusing a borrowed connection with an open transaction, exactly and at no
  cost (`PostgresStore#requireNoTransaction`, for every operation and the schema check at start: pgjdbc's
  `getTransactionState()`, a connection not unwrapping to pgjdbc's left unchecked), with a `StoreException` that is no
  retry's business; `PostgresAdapterTest.a_connection_inside_the_callers_transaction_is_refused_and_the_transaction_left_alone`
  (a data source handing out one connection with an open transaction, as the proxy does inside one; mutation-checked). Not adopted: refusing known
  transaction-aware classes by name (brittle, incomplete, unusual among comparable libraries).

*On demand:*
- Redis `WAIT` as an opt-in (recorded 2026-11-02, left as it is by the user's choice, not to be forgotten): after
  each write, wait for N replicas (`withReplicaAcknowledgement(1, timeout)`), a write fewer confirm failing as
  outcome unknown. Narrows, not closes, the window in which a failover loses acknowledged writes (MongoDB writes
  with majority; Redis replicates asynchronously). Today a lost write is detected by every cache and dropped (a
  miss, never a wrong answer; tested, also with the old primary's clock ahead); only an application using the
  cache as the only copy loses data. Costs a round trip plus replica lag per write, and in a cluster per-node
  connections in all three bindings (`WAIT` has no key to route by). Only on demand;

### Performance

After the Linux baseline (see "Performance: the baseline"), each only if measured worth it.

*Next:*
- **The Linux baseline:** `EndToEndBenchmark`, `MissBenchmark` and the per-store experiments on Linux, the store on a
  host of its own (a real network), against plain Caffeine as the floor; with a contention scenario for batches
  (concurrent `putAll`s on overlapping keys, for the MongoDB items) and threads rising past the pool's size (for read
  coalescing). It replaces the Docker Desktop numbers every item below was judged on.

*Measure first:*
- MongoDB batches, two options (recorded 2026-10-31, for later; performance, so after the robustness work). A
  `putAll` is one unordered bulk write of `updateOne` upserts at the proposed versions, one round trip (step 5:
  45.9 ms for 100 keys with 16 concurrent single writes before, about 19 ms now, of which the constant ~16 ms
  Docker Desktop stall above a few KB is probably most; to confirm on Linux or a native server). A bulk write
  returns no documents, so it cannot assign. Both only if measured worth it:
  - **refused keys retried in the store as single writes**: after the bulk write, each advance it refused is
    written again by `findOneAndUpdate`, which assigns and is never refused; one round trip per refused key
    instead of the core's read and write, and a MongoDB `putAll` never refused. Costs a second write path in
    `MongoStore#write` and a partly applied batch when the follow-ups fail. Worth it only if concurrent `putAll`s
    on overlapping keys refuse measurably (a contention scenario for batches is missing from the benchmark);
  - **large batches split into bulk writes in parallel** over the pool's connections: a batch is per row already
    (profile `mongodb`), so the guarantees stay; costs a thread pool or the asynchronous driver, a chunk size, and
    a batch failing in some chunks only. Worth it only if a batch measures server-bound off Docker Desktop;
- a native upsert in the JDBC adapter's MariaDB dialect (recorded 2026-11-01, for later; performance, no new
  guarantee). Today an upsert is portable SQL: `UPDATE ... WHERE version < ?`, then an `INSERT` where no row
  matched, and the `UPDATE` again when that insert finds the row inserted meanwhile (up to three statements in
  one transaction, one round trip each plus the commit, `JdbcStore#advanceRow`). MariaDB does it in one: `INSERT ... ON DUPLICATE KEY UPDATE` with
  every column guarded by `IF(version < VALUES(version), ...)` (assignments apply left to right, so `version`
  goes last, or the guards compare against the new value). Several rows per statement for a batch would follow
  the same way (`PostgresStore` batches already). The correctness is the same either way: versions are the
  core's, the condition sits in the statement, nothing is read back (so `RETURNING`, MariaDB 10.5+, adds
  nothing). It fits as an optional upsert template in `Dialect`, `STANDARD` keeping the portable path, and costs a
  second write path to test. Only if measured worth it: how often the duplicate path runs under contention, and
  what the round trips cost off Docker Desktop;
- coalescing concurrent store reads (recorded 2026-11-18, for later; performance, only if measured worth it):
  while a read of the cache is in flight, the single-key reads arriving on other threads (misses, read-throughs,
  revalidations on read) queue, and go as one batched `read` when it returns. Opportunistic, no batching window, so
  a lone read waits for nothing; the store's batched read exists already (`getAll`). Saves round trips and pooled
  connections where many threads read at once: the 8-thread throughput is store-bound (about 27,000 ops/s against
  millions in memory). Doubts: a pool runs reads in parallel already, so it gains only where the pool or the network
  is the bound; under moderate load a read waits for the one in flight first, which can be slower than its own
  round trip; a new core mechanism (the complexity budget), and each queued read keeps its own epoch and
  reservation. Measure first, off Docker Desktop: throughput and p99 of store reads with the threads rising past
  the pool's size. The writes' counterpart is write combining (below);
- write combining: concurrent single-key writes of one instance gathered into one batched store call, the writes'
  counterpart of read coalescing (above); only with a measured gap;
- striping the core's read-write lock between the atomic map operations and `invalidateAll` (recorded 2026-10-28):
  64 stripes by the key's hash, `invalidateAll` taking every stripe's write side in order. It spreads the read
  side's shared state over 64 cache lines; the gain is small while atomic map operations are store-bound
  (thousands per second per instance), so it waits for the benchmark (step 6). The stall around an
  `invalidateAll` (the atomic operations in flight draining, about one store round trip, at worst the store's
  timeout) stays the same with stripes;
- the cut-off remembered per instance instead of the control row read with every read (recorded 2026-10-28), only
  if the benchmark (step 6) shows the extra key matters (measured about +10–30 µs on a one-key PostgreSQL read;
  no round trip, it rides in the same batch):
  - needs an absolute time from the store: a read returning write times, or the store's "now" at the read, in
    place of the ages compared within one read (an SPI change);
  - the instance reads the control row once per `INVALIDATED_ALL` (the message carries its version, not its
    time), and again at start and after a gap or a loss; in `NONE` mode (no messages) it keeps reading it with
    every read;
  - it weakens a guarantee held now: a store read right after an `invalidateAll` elsewhere would serve rows from
    before it until the message arrives (store point 3 broken for `invalidateAll`), stale for the delivery
    delay, the bound already accepted for L1, now for store reads and loads too;
- **Async misses without a waiting thread** (measured, see "Cost of the direction" under Async variants). Decided
  2026-10-21: stay on Java 17 and the current design for now; until then, async users with many concurrent slow
  misses pass a virtual-thread executor themselves (`withStore(store -> store.withExecutor(...))`, Java 21+). Two options when it matters:
  1. **Virtual threads as the default store executor when the JVM has them** (detected at runtime, Java 21+): a few
     lines, no duplicated code, covers users who never set an executor. Check first that the drivers do not pin
     carrier threads in `synchronized` blocks on Java 21–23 (fixed in Java 24).
  2. ~~A non-blocking load path for the asynchronous caches~~: built 2026-10-21 (see Async variants).
- **A tie-breaker for equal versions, so that a check transfers no value again** (decided 2026-11-10). A version
  can repeat with other content after a failover that lost a write (dimension 4 of the in-memory store profiles), so
  a held copy is confirmed by its version and its value, compared serialized (`InternalCore#stillStored`): exact,
  free in normal operation, but a check after a gap or while delivery is interrupted transfers the value where it
  used to transfer the version only (100,000 held keys of 1 KB: about 100 MB instead of about 3 MB for the background
  pass after a gap). The two writes sharing a version always come from different instances (an instance proposes
  above every version it learned, its own writes' included), so anything telling writes apart breaks the tie: a
  128-bit hash of the serialized value (recomputed from the held copy, no memory per entry), or an ID drawn per
  instance or the Redis write's request ID (no hashing, but kept per entry and carried by reads and messages). Each
  needs a field per row and an optional one in `VersionsResult`; likely on Redis / Valkey only, the one store where
  the case occurs in practice (MongoDB's majority reads and writes rule it out; PostgreSQL's sequence repeats only if
  its own log record is lost too, unverified). Nothing in the store today breaks the tie (write time, stream counter,
  `xmin` and request keys are lost or repeat with the write). Measure the transfer after gaps first.
- **Installing what the background pass read** (noted 2026-11-12). After a gap the pass reads each held key's row
  (`InternalCore#stillStored` compares versions and values) and drops a copy that does not match; the next access
  then reads the same row again. Installing the row read instead would save that second read, after every gap and
  after a start without the store (where every copy is `UNCONFIRMED` and so mismatches). It installs under the
  reservations as `readThrough` does. Performance, so after robustness.
- **A scan next to a feed, for catching up after a gap** (noted 2026-11-12). A store offering both could answer
  `changedWithin(gap)` after a gap, so only the keys changed meanwhile are dropped, instead of revalidating all held
  keys. Only within what the store still remembers: a gap longer than the tombstone retention, size pruning (silent)
  or a failover that lost writes still needs the full revalidation, so an optimisation with a fallback. A new
  mechanism: only with a measured revalidation cost (as the tie-breaker above). No shipped store is both today.

*Waits for upstream releases:*
- MongoDB 8's client-level bulk write with verbose results (recorded 2026-10-28): it reports per write whether it
  matched, as PostgreSQL's `RETURNING` does, so a write on a version whose document is gone would leave nothing
  behind (no tombstone, no announcement). Only on MongoDB 8.0 and later; worth it once 8.0 is the minimum, rather
  than as a second path beside the classic bulk write;

*Parked:*
- write-behind for load results (proposed 2026-10-27): a loaded value is returned to the caller, and to the
  callers waiting on the same future, before its conditional write to the store has returned. The write follows
  in the background.
  - Saves one store round trip per miss on the caller's path. That matters little against a slow loader, but
    a lot with a slow store.
  - Safe, because a load result is not a user's write. Until the write returns, L1 holds it as unconfirmed, under
    the reservation. A conflict replaces or drops it, as now. Returning it early is a read linearized at the load,
    before a racing change.
  - Costs: other instances do not find the value in the store yet, so they load it themselves (see stampede
    protection across instances). A failed write leaves only the L1 copy, which is fine for a cache.
  - Not for user writes (`put`, `invalidate`, `compute`). Returning before the store confirms would break
    read-your-writes across instances, durability, and the conflict outcome of `compute`. At most an explicit
    opt-in with weaker, documented guarantees, together with write combining;
  - Designed and parked 2026-11-18: see "Write-behind for load results" under Decisions (only the owner answered
    early; the design points below were the first sketch, rejected there).
  - **Measured 2026-11-18** (`MissBenchmark`, experiments per store, local containers; one instance, one thread,
    fresh keys, the loader spinning 0, 1 or 5 ms, values of 3 B and 1 KB): the conditional write is one store call
    per miss and costs about as much as the read or more, 0.25 to 0.4 ms at a fast loader (PostgreSQL 18, MariaDB
    13, Redis 8), 0.7 to 0.8 ms on MongoDB 9 (majority, journaled); after a 5 ms loader both calls take two to three
    times longer (the connection idle meanwhile), the write 0.9 to 1.7 ms. Its share of a miss: over half at a
    loader of 0 ms (two thirds on MongoDB), 21 to 36 % at 1 ms, 13 to 23 % at 5 ms; the size of the value changes
    nothing. In memory 4 to 12 µs, 0.5 % at 1 ms. Docker Desktop's forwarding adds about what a network in one data
    centre would, so the shares should hold there; with a source slower than about 10 ms they fall below 10 %.
    Design points found before building (from `InternalCore#attempt` and `#writeLoaded`): the value must be held
    untrusted (`UNCONFIRMED`, dropped by any message) until the write returns, since later reads no longer wait on
    the load's future; the load's own echo may then drop it before the write's answer is processed; a refused write
    drops the copy instead of starting over (the caller has its value); the refill marks and the epoch apply to
    the held copy as to any;
- **Refill while serving: the calling instance's own writes after the mark kept trusted** (noted 2026-11-17). After
  `invalidateAllBeforeMark` every copy is revalidated, also the ones this instance wrote after the mark, which are
  provably kept. A copy installed after the mark may still hold a row written before it (a read by traffic during the
  fill), so the install time does not tell; a flag per entry would (the local `nanoTime` the write was *sent* at, kept
  only if at least the clock allowance after the mark; replaced on any later install), at 8 bytes per entry on every
  cache and a walk over L1 at the call. Saves one batched read per 1,000 held keys on one instance: only with a
  measured cost. Related, also open: a future for the caller to wait until the local pass is done, and a pass reading
  versions only (see the tie-breaker below).
  **Measured 2026-11-17** (`RefillBenchmark`, experiments per store, local containers; 3 instances each holding every
  key, 90 % of them rewritten, small values): the call takes at most 10 ms whatever the size; the passes read each held
  copy once, 1,000 per read (300 reads for 3 x 100,000 copies), done after 1.7 s (PostgreSQL 18), 2.8 s (MongoDB 9),
  3.4 s (Redis 8) at 100,000 keys, of which 1 s is the pacing pause; the refill itself took 2.0 to 4.4 s. Hot afterwards:
  every rewritten key in `UPDATE` mode, the refilling instance's in `INVALIDATE` mode (the refill's messages dropped the
  others' copies before). Verdict: trusting the caller's own writes would save a third of the rows, about a second of
  background work at 100,000 keys: not needed. With values of 1 KB (same day): the passes at 100,000 keys took 2.9 s
  (PostgreSQL, +1.3 s), 3.9 s (Redis, +0.6 s), 3.0 s (MongoDB, +0.1 s), the refill itself 3.1 to 5.0 s, the call
  still at most 10 ms; in memory unchanged (1.4 s). A pass reading versions only would save at most that difference,
  background work, at about the refill's own cost: not needed either. Local containers: over a real network 100,000
  values of 1 KB are about 100 MB per instance, a second at 1 Gbit/s, still in the background. Both optimisations
  stay recorded, to revisit only with measurements from a real deployment.

*On demand:*
- **MongoDB's reactive-streams driver** was not measured against the sync driver (PostgreSQL's R2DBC was, and lost
  for statements). If it is reconsidered, measure it like the removed `PostgresJdbcVersusR2dbcExperimentTest`
  (writes, conditional writes, batched reads, same replica set) and check its dependencies; the SPI stays blocking
  either way.

### Features

*In a later release:*
- **the Spring integration, its rest** (built 2026-11-19, see "Integrations"): a nightly job on the oldest supported
  Spring version (6.2).

*On demand:*
- **a JCache (JSR-107) provider**, first in line of the further integrations (see "Integrations", "Which framework
  through which integration"): in `...integration.jcache`, it serves several frameworks at once: Micronaut (which uses
  JCache whenever a JSR-107 implementation is present), Hibernate's second-level cache (`hibernate-jcache`, the
  standard way since Hibernate 6), and Spring applications on `JCacheCacheManager`. Hibernate's second-level cache
  needs consistency across instances, which is this library's point: possibly the largest audience gain. Costs a
  second API surface beside Caffeine's (JSR-107's semantics, entry listeners, statistics, configuration objects);
  Caffeine offers `caffeine-jcache` for itself, a model to read first;
- **a Quarkus extension** (see "Integrations"): an artifact of its own, the one exception to the package rule, since
  a Quarkus extension has a runtime and a deployment module with build-time processing, and is expected to run in a
  native image (a cost here: Fory generates code at runtime, and each driver has its own native configuration).
  Before designing it, read `quarkus-cache`'s source for the interface a backend implements (the Quarkiverse
  Infinispan Embedded extension implements Quarkus's cache, keeping `@CacheResult` and the other annotations
  working);
- **a Hibernate `RegionFactory`** in `...integration.hibernate`: thinner than JCache, but Hibernate only; only if
  Hibernate is wanted without the JCache provider;
- **a Micrometer integration** (noted 2026-11-20) in `...integration.micrometer`, `micrometer-core` optional: a
  `MeterBinder` exporting `distributedStats()` (store reads and writes, invalidations sent and received, interruptions)
  and the synchronization state as a gauge, named after Micrometer's own `CaffeineCacheMetrics` (`cache.*`, a `cache`
  tag) so that dashboards line up. The local statistics need nothing of ours: the caches are Caffeine caches, which
  Micrometer's binder reports with `recordStats`. Micrometer serves Spring Boot, Quarkus, Micronaut and plain
  applications alike; for Spring Boot a thin `CacheMeterBinderProvider<CaffeineCache>` beside the manager, so that
  Boot's cache metrics include the distributed ones without wiring;
- **an OpenTelemetry integration** in `...integration.opentelemetry`: the same metrics through OpenTelemetry's metrics
  API, for applications without Micrometer (Micrometer reaches OpenTelemetry through its OTLP registry already); on
  demand;
- **CockroachDB** (planned 2026-11-02, see "CockroachDB adapter (planned, not built)"): the first distributed SQL
  database, chosen as the best fit (a native feed, versions assigned in one statement, no loss of committed writes on
  failover, PostgreSQL's wire protocol);
- an adapter per further SQL database (decided 2026-10-25 and 2026-11-02): no general JDBC adapter, since the SQL fits
  only where it happens to; `JdbcStore` takes a dialect per database, each with an adapter of its own, as MariaDB and
  MySQL have. Each needs its contract tests on that database;
- stampede protection across instances: one load per key cluster-wide, not only per instance (each instance
  already shares its loads through Caffeine's `AsyncCache`, see "Shared loads"). FusionCache has it as an opt-in
  distributed locker (a Redis lock), for efficiency, not correctness ("don't use the distributed stampede protection
  as a locking mechanism"; checked 2026-11-18): of 100,000 requests on 10 nodes, at most 10 loads per-node, 1 with
  the locker;
- tags: entries written with tags, and an invalidation of every entry of a tag (FusionCache's `RemoveByTag`, checked
  2026-11-18: a marker entry with the time of the call, entries created before it hidden on read, tags looked up
  lazily, untagged entries paying nothing; coherent across nodes only with its backplane). Here it would be a cut-off
  per tag, the control row of `invalidateAll` per tag, and a tag set per row; it goes beyond Caffeine's API. A use:
  everything of one tenant or one aggregate at once;
- fail-safe, an opt-in: when the loader fails, serve the last value past its maximum age for a bounded time instead
  of throwing (FusionCache's fail-safe: a maximum duration in total and a throttle before the source is asked again;
  cache2k's resilience policy: a loader exception suppressed for a window while a previous value exists; both checked
  2026-11-18). It contradicts the maximum age as a bound, so only as an explicit, documented weakening;
- loader timeouts, soft and hard: a slow loader answered with the last value after a soft timeout while it completes
  in the background, a hard timeout failing the call (FusionCache's factory timeouts with background completion,
  checked 2026-11-18). Close to `refreshAfterWrite`, which covers held keys; new for misses and expired values;
- variable expiry per key in the store: a maximum age per entry (for example from an `Expiry`-like function or per
  write), not only one per cache. Caffeine's own variable expiry already works in L1 and stays untouched (see
  "Maximum age");
- a fourth distribution mode, `UPSERT` (replicate): a write elsewhere puts the key into every instance's L1, not
  only into the holders'. This breaks invariant 8 knowingly and only in this mode.
  - Cost: N store reads per write with N instances, because messages carry no values. L1 is polluted by keys that
    are never read here, and W-TinyLFU may reject them anyway.
  - Use case: small, read-mostly data that every instance needs.
  - The cheaper alternative to weigh first: a warm-up (`getAll` or a `warmUp(keys)`) plus `UPDATE`.
  - Reviewed 2026-11-05, not built: messages alone cannot make every instance hold every key. A new instance only
    gets the keys written after its start, and after a gap an instance knows which of its own keys to check, but
    not which keys it missed. A correct replicated mode needs a full load at start and a full resync after every
    gap: a store scan (a new SPI operation) and recovery around it, as Hazelcast's Replicated Map and Infinispan's
    replicated mode do (Redisson's `SyncStrategy.UPDATE` inserts best-effort only, with `preloadCache()` as a
    manual call). Against the warm-up plus `UPDATE`, it saves only the first read of a key created elsewhere (one
    store read per instance), at N reads per write. If a use case comes (small reference data every instance
    needs), designed as a replicated cache with full load and resync, not as a fourth message mode;
- a warm-up: filling L1 at startup, from given keys (`warmUp(keys)` or `getAll`) or from the store's most recent
  rows (by the write-time index). With `UPDATE`, the warmed keys then stay current. It is useful on its own, and it
  is the cheaper alternative to `UPSERT`. Given keys work today: `getAll(keys)` reads them in one bulk store read
  and loads only the missing ones. From the most recent rows (reviewed 2026-11-05, not built): a new store
  operation reading the newest N rows by that index; it fills L1 with recent writes rather than hot keys (the
  store knows no access frequency, and Caffeine's admission may reject them), and instances starting together
  read N rows each at once. Another form, Hazelcast's near-cache preloader (checked 2026-11-18): the client stores the
  keys (not the values) of its near cache to a local file in intervals and loads them again after a restart, so the
  instance gets back its own hot keys, which the store cannot know. Built only on demand: without it, the first access
  per key costs one store read;
- caches of different serializers in one PostgreSQL table (recorded 2026-10-31, for later): today a table's `key`
  and `value` columns take the type of its first cache's formats (`bytea`, `text`, `jsonb`), and a cache of other
  formats is refused at start. One column per format (`key_bytea`, `key_text`,
  `key_jsonb`, the same for `value`), each cache filling the pair of its formats, would allow it and keep `jsonb`
  queryable; it costs six sparse columns and the format in every statement and read. Rejected: everything as
  `bytea` with a format column (`storeAsNativeJson` meaningless on PostgreSQL), everything as `jsonb` (base64 for
  binary, a parse per access of the default format). Only if sharing a table is asked for (one table per
  application); MongoDB has no such limit;
- Garnet (recorded 2026-11-02, for later, only on demand; see "Redis adapters", item 15): it has no streams (every
  stream command listed as not supported), runs scripts only with `--lua`, and atomically only with
  `--lua-transaction-mode` (off by default; without it a script locks nothing, which every write script relies
  on). Everything else the scripts call works there (checked on 2.2.5: `TIME`, hashes, sorted sets, `INCR`,
  `SET … PX`). Verified there as well: `PUBLISH` from a script reaches subscribers; keyspace notifications
  (`notify-keyspace-events`) and client tracking (`CLIENT TRACKING`) do not exist. Two ways, either needing the
  transaction mode required and checked (the probe cannot see atomicity):
  - **polled** (`ChangeScan`, as the JDBC adapter on MariaDB): the write scripts without `XADD`, and one script
    ranging over the sorted sets of values and dead rows by write time, which exist already. Simplest; changes
    arrive within the poll interval, every instance scans each interval;
  - **a feed of a sorted set and pub/sub**, the same guarantees as the stream: the write script adds each entry to
    a sorted set scored by its number (from the counter, as now), its member the entry (hash, version, kind,
    number), and publishes the number on the cache's channel as a wake-up only; a reader waits for a wake-up (or
    a second, as `XREAD BLOCK` does) and reads the entries above its number (`ZRANGEBYSCORE`), so a lost wake-up
    costs at most a second, a skipped number is a gap, and trimming (`ZREMRANGEBYSCORE`, by number, with a time
    index if by age) is a gap too. Push latency like the stream, but a second reader per client library (each
    has its own pub/sub API), and the sorted set costs more memory per entry than a stream. Pub/sub alone (no
    replay, every disconnect a gap and a full revalidation) and lists (blocking pops consume, no fan-out) were
    considered and are weaker;
- native JSON values on Redis, Valkey and Dragonfly (recorded 2026-11-01, only on demand: for querying cached
  values on the server, as RediSearch over JSON documents would, not for the cache): today every format is stored
  as bytes there (`TEXT` and `JSON` as UTF-8 text, readable but not queryable). It would cost a JSON key per row
  beside its hash (or the whole row one document), every script changed, more memory per value (a parsed tree),
  a parse on every write, and two layouts (Redis 8 and Dragonfly have JSON built in, Valkey a module, Redis 6.2
  and 7 only with the module);
- further stores (DynamoDB, Cassandra, SQL Server, Oracle, ...): what each would need and lack is surveyed under
  "Design study: one number per write" (History), with a fallback for stores without a global counter;
- session tokens: read-your-writes across instances for one's own writes (a token of the highest version written,
  passed along with a request), beyond the delivery delay that bounds it today;

## Positioning

The frame for every design decision, and the libraries compared with.

### Positioning and the complexity budget

*Decided 2026-10-26; the frame for every design decision.*

**What makes the library unique.** Caffeine's API, on the database an application already has, with no stale value
in L1 beyond the delivery delay, even under races and failures. No other library offers that combination:

| Gap                               | Redisson, JetCache    | Hazelcast, Infinispan    | This library                                                                                               |
|-----------------------------------|-----------------------|--------------------------|------------------------------------------------------------------------------------------------------------|
| Infrastructure                    | Redis required        | a cluster of their own   | the existing database (PostgreSQL, MongoDB) as L2 and feed                                                 |
| API                               | their own             | their own                | Caffeine's, as a drop-in                                                                                   |
| A load racing a write (stale set) | stale until the TTL   | protected                | protected                                                                                                  |
| Lost messages                     | stale until the TTL   | detected per partition   | detected; L1 distrusted until checked                                                                      |
| `compute` across instances        | local only, or a lock | atomic (entry processor) | atomic, one store operation on the hot path                                                                |
| `invalidateAll`                   | racy                  | cluster-wide             | exact, atomic per key as in Caffeine (since the decision for a sweep; before: O(1) and atomic across keys) |

A simpler design that bounds every rare race by a mandatory maximum age in L1 was considered and rejected: it would
be no better than the others.

**The complexity budget: the core's mechanisms, each tied to a guarantee that makes the library unique.**

| Mechanism                                                   | Guarantee                                                                |
|-------------------------------------------------------------|--------------------------------------------------------------------------|
| a version per key, counted by the store; conditional writes | atomic `compute`; no stale sets                                          |
| versions from reads and messages; one reservation registry  | no stale value in L1 beyond the delivery delay; hot keys stay cached     |
| stamp and cut-off, with the basis                           | exact `invalidateAll` in O(1)                                            |
| the epoch on gaps                                           | lost messages never leave stale values                                   |
| a fresh row's start value from the store's counter          | a recreated key is never confused with its old lifetime, without a clock |
| the store guard and retries                                 | robustness when the store fails                                          |

Revised the same day: with one number per write (a section below), the first, third and fifth collapse into one,
which gives four mechanisms.

Revised again the same day, with the thin SPI and `invalidateAll` by a sweep:

| Mechanism                                                                            | Guarantee                                                                             |
|--------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| versions (incarnation, counter) computed by the core; compare-and-set in the store   | atomic `compute`; no stale sets; a recreated key never confused with its old lifetime |
| one reservation registry, ordered within an incarnation and conservative across them | no stale value in L1 beyond the delivery delay; hot keys stay cached                  |
| `invalidateAll` as a sweep of tombstones with one message                            | exact, with Caffeine's semantics (atomic per key)                                     |
| the epoch on gaps                                                                    | lost messages never leave stale values                                                |
| the store guard and retries                                                          | robustness when the store fails                                                       |

The stamp, the cut-off, the basis and start values from the store are gone.

**The rule.** A new mechanism goes in only if it strengthens one of these guarantees or closes a measured
performance gap. The store focus follows from it: databases with a global counter and a native feed (see the
database focus).

**What it means for the alternatives worked through on 2026-10-26.**

- **Kept, as a simplification of today's core** (not a redesign):
  - one reservation registry instead of `minimumVersions` and `writing`/`writeMinimums`;
  - the scattered "drop older" cases folded into one check at release;
  - versions read from the store, as now.
- **Kept as it is:** writes return their version. PostgreSQL returns it for free with `RETURNING`; MongoDB keeps its
  16 concurrent calls per batch.
- **Out:**
  - op ids only for recognising echoes;
  - writes without versions as the rule;
  - parking until the echo;
  - starting fresh rows at 1, with revalidation by age;
  - the stamp as an optional tier for stores without a global counter;
  - the stamp as the version;
  - Lamport and hybrid logical clocks;
  - leases for linearizable L1 reads;
  - the simple design bounded by a maximum age.
- **Later, only if measured or needed:**
  - MongoDB batches in one bulk write: an optional version in write results, and op ids for the unknown ones;
  - op ids for resolving unknown outcomes, making `put` safe to retry: a robustness gain, judged on its own;
  - the polling feed scanning by stamp instead of store time: needed once a store without a native feed is in focus
    again;
  - a periodic scan as a safety net;
  - write combining;
  - session tokens for read-your-writes across instances.

### Comparable libraries: FusionCache (.NET)

*Read 2026-10-26.*

FusionCache keeps an L1 in memory and an L2 behind `IDistributedCache` (Redis, SQL Server and others), with a
backplane (usually Redis pub/sub) to keep the L1s consistent. Like JetCache, it is a comparison point. This summary is
based on its docs (`Tagging.md`, `Clear.md`, `AutoRecovery.md`, `Backplane.md`), not its code.

- **Same as ours:** backplane messages carry no value, only key, action and timestamp. A node acts only if it holds
  the key, and it reloads from L2.
- **Weaker than ours:** a missed message is not detected. The docs advise a short L1 duration and describe no resync.
  There is no documented check that drops a message older than the copy held. Races between a load and a remove are
  not addressed. Our epoch on gaps, our versions and our conflicts cover these cases.
- **`Clear` and `RemoveByTag` in O(1)** (relevant to plan B, the O(1) `invalidateAll`):
  - Each one writes a marker entry with a timestamp. `Clear` uses two special tags: `*` to expire and `!` to remove.
  - A read hides an entry created before the marker. The check is lazy, and untagged entries pay nothing.
  - Hidden entries are not deleted in bulk; they disappear by their own expiry.
  - It confirms that plan B works in practice, and that the O(n) cleanup can be left to maintenance.
  - Its weak points, which we would avoid:
    - It compares wall clocks, and the docs do not mention clock skew. We would use a generation (a counter).
    - The markers are ordinary entries that can expire or be evicted, which would drop an invalidation. Our
      generation would be durable.
- **Ideas worth a look later, each opt-in and judged against the guarantee:**
  - Tags (`RemoveByTag`) built on the same generation mechanism. This goes beyond Caffeine's API.
  - Fail-safe: serving an expired value when the store fails. It contradicts the guarantee, so it could only exist
    as an explicit opt-in.
  - Soft and hard timeouts for loaders.
  - Auto-recovery: failed L2 writes and publishes are queued, only the last per key is kept, and before a replay
    the node checks whether L2 changed in the meantime. This could matter for the later bus SPI (failed publishes).
    Our writes fail to the caller with an unknown outcome, by design.

Checked and not adopted: JetCache's per-key refresh tasks (Caffeine refreshes on access), its non-strict
distributed lock (not a cache concern), result codes instead of the Caffeine API (the store guard covers failures),
per-call TTLs in L2 (Caffeine's `Expiry` covers L1), and custom annotations (a thin Spring `CacheManager` is the route
taken, see "Integrations"). Its local sync (keys only, no versions, at-most-once pub/sub, instance clocks) and
its lock on every L1 hit are weaker than this design.

**Features of the others, checked 2026-11-18** (their documentation; the ones worth having are in "Open work":
tags, fail-safe, loader timeouts, a JCache provider (under further integrations), a preloader of hot keys, stampede
protection across instances).
Not adopted:
- **Write-through and write-behind to a system of record** (Ehcache 3's `CacheLoaderWriter`, with write-behind
  batching and coalescing; Infinispan's cache stores): the store here is a cache, not the system of record, and
  Caffeine removed its `CacheWriter` in 3.0 (map computations and the eviction listener instead). Terracotta's own
  docs say Ehcache clustering is not compatible with cache-through.
- **Max idle per entry in the store** (Redisson's `RMapCache`: an entry expires by its TTL or its max idle time,
  checked on every read): every read would become a store write. Infinispan warns that max idle with a near cache is
  unsupported.
- **Distributed locks** (JetCache, Redisson) and **queries over cached values** (Infinispan, Hazelcast): not a cache
  concern.
- **Reconnection by an invalidation log** (Redisson's `ReconnectionStrategy.LOAD`: invalidated hashes kept 10 minutes
  and replayed after a reconnect): the same idea as "a scan next to a feed" in "Open work", there best effort; here a
  gap revalidates, exact.
- **Eager refresh at a share of the duration** (FusionCache's `EagerRefreshThreshold`, acting only on a request
  after the threshold): Caffeine's `refreshAfterWrite` does the same, and is supported.
- **Auto refresh stopping after the last access** (JetCache's `stopRefreshAfterLastAccess`): Caffeine refreshes on
  access only, so a key not accessed is not refreshed anyway.
- **A Micrometer binder for `distributedStats()`**: decided against before (the value can be polled); Caffeine's own
  binder covers `stats()`. Whether others ship one was not checked; to ask users again.
No Java library named HyperCache was found (only Go libraries of that name).

## Keys, versions and clocks

### Key identity: hashes

A row is identified by a hash of its key. Java's `hashCode()` is not stable across
JVMs, and keys are not required to be readable by the store, so the identity must be computed deterministically on
every instance:

- XXH3-128 via hash4j, rendered as the row id together with the cache discriminator (MongoDB `_id` as `{d, h}`,
  PostgreSQL primary key `(discriminator, id)`; see "Adapter setup"), held compactly in `InternalKey` (see "How a
  message finds the held key").
- Out of the box for `String`, `Long`, `Integer`, `UUID` and enum constants; other keys implement `Hashable` or the
  cache is given a `HashProvider`. Other value types (dates and the like) stay the application's: each has several
  plausible encodings, and every built-in kind is a stored format to keep.
- **The stored format, fixed for good** (`Hasher.builtInHash`, recorded in every store as `xxh3-128/1`; built
  2026-11-19, see "The stored hash format" below): a value's hash is XXH3-128, seeded by its kind, over its bytes.
- Messages identify keys by hash (all kinds except `INVALIDATED_ALL`), which also keeps PostgreSQL notification
  payloads small.
- Collisions are treated as impossible (128 bits); this is stated in the docs rather than handled.
- The serialized key is still stored in the row, for diagnostics and readability of the store; nothing in the
  library reads it back (keys are matched by hash).

#### The stored hash format

*Built 2026-11-19, for 0.10.0, a breaking change.*

Stored hashes must stay unchanged for years, so the format specifies every input of a stored hash: the bytes that go
into XXH3, not the code that produces them. XXH3 hashes the same bytes alike streamed or in one shot, so any later
implementation (one-shot, a lighter `Hasher`, a pool) changes no stored hash; a built-in kind added later was refused
before, so it changes none either. Only another algorithm would break again, and XXH3-128 is frozen.

- **Why:** before, the built-in types were encoded with hash4j's stream conventions and no type marker, so values of
  different types collided: `""` and `Integer` `0` hash alike (both the four zero bytes: a string is its chars, then
  its length as an `int`), as do a 2-char `String` and some `Long`s from 2^33 up, or a 6-char `String` and some
  `UUID`s; and a `Hashable` hashing `putString(id)` equals the built-in `String` key `id`. Two colliding keys in one
  cache share a row: one gets the other's value, and a key from untrusted input could aim at another key's row.
  It needs keys of several kinds in one cache, rare in typed caches, common under Spring (one cache for
  `findById(Long)` and `findByName(String)`, `SimpleKey` params). Since 0.7.x. And those conventions are hash4j's,
  with no stability promise (its README makes none) and no outside reference.
- **The specification** (in `Hasher`'s Javadoc as the stored format, and here), one rule:
  **every built-in value's hash is XXH3-128, seeded by its kind, over its natural bytes.** One shot, the length is
  part of the input, so no length is added; the seed separates the kinds, so bytes alike in two kinds never hash
  alike.
  1. **Form:** the stored hash is the 32 lowercase hex characters of the 16 bytes, as `Hash.toHex()` gives them.
     Ids beginning with `#` are the library's own rows; no key's hash can take that form.
  2. **Kinds and their bytes** (each a fixed seed, listed in the Javadoc):
     - `String`: its chars, UTF-16LE;
     - `Long`, `Integer`: its 8 or 4 bytes, little-endian (the order the usual CPUs and XXH3's own reading use, so
       nothing is swapped); `UUID`: its least significant `long`, then its most significant, each little-endian;
     - enum constant: the name of its declaring class (a constant with a body of its own is an anonymous subclass),
       the char U+0000 (which no class name contains), the constant's name, as chars;
     - `SimpleKey` (Spring) is no kind here: `SimpleKeyHashProvider` is a hash provider, so its hash is a custom
       one, built with a `Hasher` (see "Spring" under "Integrations"). It was a composite kind of this format first
       (its params' 16-byte hashes under a seed of its own, `null` and custom hashes as element kinds), which made
       the format class public API for one integration (decided 2026-11-20); the integration was not released, so no
       stored hash changed.
  3. **`hasher.putHash(Hasher.builtInHash(part))`:** puts the part's 16-byte hash into the hasher, the same idea for a
     provider combining built-in parts; `putHash` puts any `Hash`, a part's own `Hashable` hash too, an instance
     method like every `put…` (decided 2026-11-20).
  4. **Custom hashes of whole keys:** the `Hash` a `Hashable` or hash provider returns, built with a `Hasher`
     (`getHash()`), in the same 32-hex form, so every row id has one form (decided 2026-11-20; custom hashes were
     strings of any form before, which kept a second key representation and a reserved-prefix check). `Hasher`'s
     `put…` methods write hash4j's stream encoding themselves, pinned by golden values (see "Custom hashes: `Hasher`
     encodes them itself" below).
- **What hash4j provides:** XXH3, whose output is fixed by its specification (frozen since xxHash 0.8.0, seeded variant
  included) and cross-checked by hash4j against the reference implementation; nothing else, since `Hasher` encodes
  custom hashes itself. hash4j stays: pure Java, no `sun.misc.Unsafe` (byte-array `VarHandle`s), maintained;
  Zero-Allocation-Hashing builds on `Unsafe`, Guava and Commons Codec lack XXH3-128. XXH3-128 stays the algorithm: the
  fastest measured, as stable as Murmur3-128 (36.6 / 6.3 ns against 34.8 / 4.7 ns one-shot over an encoded `String` /
  `Long`); SHA-256 took 92 / 47 ns and about 300 bytes, worth it only against deliberately crafted collisions, which is
  not the goal.
- **Known answers in the build:** the official xxHash XXH3-128 sanity vectors (seeded and unseeded, every length
  class: 0, 1–3, 4–8, 9–16, 17–128, 129–240 and longer, with the boundaries), an independent oracle for hash4j's
  XXH3; golden values for every part of the specification (each kind and edge case: empty, BMP and surrogate chars,
  long strings, extreme numbers, an enum constant with a body of its own, `SimpleKey`s with every kind of param) and
  for `Hasher`'s common `put…` methods. A change of stored hashes, ours or a hash4j upgrade's, fails the build.
- **A known-answer check at runtime:** an application's build may resolve another hash4j version than ours (Maven
  takes the nearest declaration, Gradle the highest). The first cache build of a JVM hashes its known answers
  (every length class and kind, every common `put…` method, a chain) and refuses to start on a mismatch, naming the
  hash4j version this release accepts. Microseconds, once; the README's Requirements say so.
- **Migration by the hash format recorded in the schema marker** (decided 2026-11-19): the schema version stays 1
  until 1.0, and every marker records the hash format of the row ids (`Store.HASH_FORMAT`, in the SPI, since the
  adapters see nothing else): `distributed-caffeine schema=1 hash=xxh3-128/1` (see "Schema markers and the reset"). A
  marker without it is a place of an earlier version, whose ids are hashed in the earlier format: refused with the
  remedies named, or, on PostgreSQL and MongoDB, dropped and recreated with `withSchemaReset()` (Redis and
  MariaDB/MySQL have no reset: stop the instances, delete the keys or drop the table). A later format stays refused
  even with the reset: the newer version wins. Old and new instances never share a place (a silent hash change
  would let each serve values the other changed), and a forgotten migration is a refused start, not stale values. Rows
  are not re-hashed in place: the stored key is for diagnostics, not read back, and a cache may be emptied. The release
  notes: one cold start per cache (also for custom hashes, whose values do not change, since the reset drops the whole
  place), a provider putting built-in parts uses `putHash(Hasher.builtInHash(part))`, `Hashable` and
  `HashProvider` return a `Hash` (a hash of another form, such as `"order-42"`, is built with a `Hasher` now), the
  steps per store.
  Tests: `PostgresAdapterTest` and `MongoAdapterTest` (`a_..._of_an_earlier_version_is_refused_unless_it_is_reset`,
  `a_reset_never_drops_a_..._of_a_later_hash_format_revision`).
- **The first implementation:** built-in values hashed one-shot (no stream, so no
  thread-local, no `Thread.isVirtual()` check and no pool; virtual threads hash as fast as platform threads),
  measured (`HashStreamBenchmarkTest`, JDK 25, a loaded laptop, 1 / 4 threads): a 19-char `String` 15.9 / 16.3 ns
  against 23.0 / 21.5 ns with the stream per thread before, a `Long` 4.6 / 4.9 against 7.3 / 7.2 ns, a 100-char
  `String` 66.2 / 70.1 against 62.0 / 62.1 ns (hash4j reads chars four at a time through `charAt`); the way before on a
  virtual thread, a fresh stream, takes 31–42 ns and 416 bytes.

- **Decided in the build:** the seeds are the 64-bit XXH3 hashes of the kinds' names (`"distributed-caffeine:String"`
  and so on), computed once and written into the code and the Javadoc as literals: self-explaining, clash-free, never
  computed at runtime (`HasherBuiltInTest` recomputes them). The runtime check accepts any hash4j version that gives the
  known answers and names the version it resolved. The xxHash vectors come from xxHash v0.8.2's own sanity test data
  (`Xxh3VectorsTest`: 63, every length class boundary with its three seeds; BSD license), checked into the tests with
  attribution.
- **Custom hashes: `Hasher` encodes them itself** (built 2026-11-19, no stored hash changes): the `put…` methods write
  hash4j's stream encoding (every value little-endian, a string's chars then its length, UTF-8 with `?` for an unpaired
  surrogate, and so on; an unordered iterable through a funnel is encoded our own way, see below) into a small array (64
  bytes, doubling, sized to a string at once), hashed by `getHash()` in one shot; beyond 4 KB the array goes into a
  hash4j stream as raw bytes, so memory stays bounded. hash4j then contributes XXH3 alone, so another hash4j version can
  change no encoding. Before, every hasher was a fresh hash4j stream with a 264-byte buffer, which the core creates for
  every access to a key with a custom hash.
  - Bit for bit hash4j's: `HasherDifferentialTest` puts random sequences of every put method (sizes across
    both buffers, every kind of char, every way hash4j takes for unordered iterables) into both and compares (3,000
    sequences per build, 100,000 in the nightly); the known answers in the build and at runtime hold the common methods.
  - A string's chars are copied in bulk (`String.getChars` into a scratch array of up to 256 chars, made on first
    use), then written little-endian through the `VarHandle`, plain stores: char by char through `charAt` took about
    1.5 ns per char, slower than the stream, and a little-endian `CharBuffer` view took two objects (about 100 bytes)
    per string; the one-shot XXH3 itself took 6 / 17 / 117 ns for 19 / 100 / 1000 chars against the stream's
    31 / 69 / 509.
  - Measured (`HashStreamBenchmarkTest`, JDK 25, a loaded laptop, `putString(id)` then `putLong`, the `Hash` as the
    result, 1 thread): a 19-char id 18.7 ns and 200 bytes against 37.5 ns and 520 bytes with a fresh hash4j stream; a
    100-char id 44.0 ns and 664 bytes against 77.4 ns and 520 bytes; a 1000-char id 253 ns and 2736 bytes against
    520 ns and 520 bytes (the bytes held at once, short-lived).
  - Not taken: a pool of hashers (needs a lifetime rule for user code, a hasher valid only during the call, which
    cannot be enforced, and brings back the question of threads: measured 19.4 / 20.5 ns for the short id with a
    reused stream, but every key writing shared memory or a free list per thread).
- **No hash4j type in the API** (decided 2026-11-20): hash4j computes XXH3 inside `Hasher`, and nothing of it shows, so
  that a change of hash4j's API (or another XXH3 library) changes none of ours. `Hasher` returns `Hash`, a record of the
  two `long`s with the stored form (`toHex`), and offers the put methods of hash4j's `HashSink` by the same names and
  encodings without implementing it, with its own nested `Hasher.Funnel` in place of hash4j's `HashFunnel`. Left out:
  bytes read through hash4j's `ByteAccess` (a `ByteBuffer`'s, a memory segment's), a type of their own for one method
  (decided 2026-11-20). Public in the `hasher` package: `Hasher` (with `Funnel`), `Hashable` and `HashProvider` for
  users, and `Hash`. All XXH3 logic is in `Hasher` (decided 2026-11-20): the custom hashes and the built-in kinds
  (`builtInHash`), so the stored format is specified and computed in one place. The runtime check of the resolved hash4j
  is the core's (`InternalHasher.verify`, run on the first cache build): its known answers go through `Hasher`'s public
  methods, so it needs no API of its own.
  - **Unordered iterables:** equal sets must hash alike whatever their iteration order, so each element is hashed on its
    own and the element hashes are sorted. hash4j lets the element hash be a function (to 64 bits, sorted as signed
    `long`s, then their number: kept here as hash4j encodes it, a JDK type), or a hasher or stream of its own over a
    funnel, which could only be named with hash4j's types. That one is `putUnorderedIterable(Iterable, Funnel)` here, an
    encoding of our own (decided 2026-11-20): each element's XXH3-128 over its encoding, as two `long`s (the least
    significant first), sorted as signed `long`s by that one, then by the other (a private comparator, so `Hash` has no
    `compareTo` that would be part of the format), then their number. 128 bits, so that a key that is a set keeps the
    guarantee of every other key: with 64-bit element hashes a set of one element would be as unique as 64 bits (a
    collision about every 2³² keys by the birthday bound). XXH3-128, so that every hash here uses one algorithm, and a
    port of XXH3, should hash4j ever fall away, is of XXH3-128 alone (XXH3-64 differs in the short-input paths and the
    final mix). `HasherDifferentialTest` writes the encoding out on hash4j's stream as the oracle; a known answer pins
    it.
  - **A hint on hash4j's API:** `HasherApiTest` compares `Hasher`'s put methods with `HashSink`'s, hash4j's types
    mapped to ours, and fails when hash4j adds one (to be added here with its encoding, or listed as left out) or
    drops one (kept here, its encoding being ours, and listed as own); it also checks that no public signature of the
    `hasher` package names a hash4j type.

### Clocks: ages from the store, versions from the instances

Ages and expiry run on the store's clock alone; no instance clock is compared with a stored write time. The instances'
clocks only propose versions (a hybrid logical clock per instance, see "Monotonic versions"), whose order
the store enforces, so a wrong instance clock costs refused writes, never a wrong answer; a version seen more than a
minute ahead of the instance's clock is reported, one more than a day ahead is not followed (`InternalVersion`).

- The **write time** is set by the store in the write itself (`$$NOW` in a Mongo update pipeline, `now()` in
  PostgreSQL, `TIME` in a Redis script, the server's time in the JDBC statements).
- The **maximum-age check** runs inside the query against the store's clock
  (`write_time > now() - max_age`, `$expr` with `$$NOW`).
- Where an instance needs the age (refresh rule 2, `store().get`), the store returns the **age**
  (`now() - write_time`), not the timestamp.
- Messages carry no time. A holder that reads a row after a message gets its age like any other read.
- Accepted: on Mongo the clock is the current primary's, so a failover may shift it slightly. Harmless for ages,
  irrelevant for ordering (below).

### Ordering: per-key versions, enforced by the store

**Updated 2026-11-18.** A row's version only ever grows: the core proposes it and the store enforces the order, or
assigns it where it can (see "Monotonic versions" and "Versions applied as well as each
store can"). A write not decided on a read applies only above the stored version; a write decided on a read keeps its
equality condition, at a version above the one read. So commit order equals version order, and any two versions of a
key compare: the larger is the newer state. Until 2026-10-28 the store assigned `old.version + 1`, a new row starting
at the server's time in microseconds; the rules below kept their numbers (the code cites them, see `InternalCore`).

Rules on the instance:

1. Every L1 value carries its version: the one its write applied at (proposed, or assigned and returned by the store),
   or the one read.
2. A message or a load result is applied only if its version is higher than the version held. This covers
   out-of-order delivery, duplicates, and the writer's own echo (same version, so ignored).
3. Local writes to the same key commit through `compute` with the same comparison, so a slower thread cannot
   overwrite a newer value. A write in flight is registered by hash before it goes to the store: a message for the
   key arriving before the write has committed to L1 records its version (whether or not the key is held), and
   after the commit a value older than it is dropped again. Otherwise a key not held yet would keep the older value
   forever (the newer message found nothing to drop, the writer's own echo is not newer). The epoch is taken before
   the write, so an `invalidateAll()` landing in between leaves the committed value untrusted. Found while building
   `asMap()`; it affected `put` and `putAll` too.
4. A load overtaken by a message with a higher version is discarded. A key with a load in flight is not held yet,
   so a message for it records the version as the minimum the load must reach; a load result below it is discarded.
   The version marks overtaken single-key loads, so no separate tokens are needed.
5. A write that fails with an unknown outcome drops the key from L1. If it landed, the next read finds it in L2.
6. Puts are written to L2 unconditionally; results of loads and refreshes only if the row still has the version
   seen when the load started, or is still missing (`... WHERE version = $expected` on PostgreSQL; a filter on `_id`
   and version on MongoDB, where a duplicate-key error counts as a conflict). Otherwise an older source value could
   overwrite a newer put that landed meanwhile. On a conflict, a refresh discards its result (the newer value
   arrives through its own message) and a load on a miss reads the row and returns it (it is newer).
7. The writer's own writes need no bookkeeping: a confirmed write is durable, or lost only in an event every
   subscription sees as a loss or gap (store contract point 3), and every resume is a gap (see "Supervision"), so
   whatever the writer holds is checked like everything else. Until 2026-10-23 the writer remembered its writes until
   their own message arrived and dropped those still waiting on a gap; that covered MongoDB with journal default
   `false`, where the crash test lost an acknowledged write that the stream resumed past silently. Dropped because the
   bookkeeping raced with fast messages (a message arriving before its write call returned left the write
   unconfirmed for good, found by a flaky recovery test on PostgreSQL 13), and because the store contract now
   excludes the case: the MongoDB adapter writes journaled.

Invalidation writes a **tombstone** (state `TOMBSTONE`, no value, new version) instead of deleting the row, because
deleting loses the version that later messages and in-flight loads are compared against. Tombstones are pruned after a
retention window; a message delayed beyond that window only happens with a gap, and revalidation after a gap
catches it anyway (invariant 7).

`invalidateAll` writes one control row and sends one message (see "`invalidateAll` by the store's
time, hidden on read"). Receivers clear L1 and increment the revalidation epoch (see "States and recovery"); a load
started before the increment completes with an older epoch, so it counts as unconfirmed and is checked against the
store on its next read. One counter serves revalidation and `invalidateAll`; global ordering is needed only locally.

So none of these is needed: operation ids, activation ids, an ordering remembered by the publishing instance, echo
handling, refresh or load tokens.

**A put starved on a hot key** (found and built 2026-11-17). On a store applying proposed versions
conditionally (MariaDB, MySQL, MongoDB's batches), a refused advancing write read the row and proposed just above it
(`InternalVersion#next`), in the same millisecond, where the versions' node bits order two instances: one instance
lost every tie to the other while both wrote the key, and its put failed after its 100 attempts (8 threads on 2
instances putting one key: 1 put in 10^4 to 10^6 failed at store latencies up to 50 µs, every failure on one
instance; `HotKeyPutExperimentTest`). A retry now proposes in the millisecond after the row read
(`InternalVersion#beyond`), the clock following: no failure since, the refusals of first attempts unchanged.
Rejected: giving the put up after a second refusal as overwritten by a concurrent write (that write can be a load
that read the source before the put, whose stale value would then stay).

### Monotonic versions, proposed by the core, enforced by the store

*Decided and built 2026-10-28, build plan step 6a; superseded the incarnations of step 2.*

**Why.** The benchmark of step 6 showed that versions of incarnations and counters (step 2) do not hold under
contention: when several instances write the same few keys, their versions belong to different incarnations, no
instance can order them, and L1 drops instead of keeping. Measured (2 instances x 4 threads, 5 s, local containers):

| Scenario                                | Store                | Before step 2 (store versions) | Incarnations    | + draining, unconditional puts |
|-----------------------------------------|----------------------|--------------------------------|-----------------|--------------------------------|
| 1 hot key, 80 % gets / 20 % puts: ops/s | PostgreSQL 18        | 29,043                         | 21,194          | 16,552                         |
|                                         | MongoDB 8            | 25,324                         | 14,620          | 11,545                         |
| the same: store reads per get           | PostgreSQL / MongoDB | 0.00 / 0.01                    | 1.00 / 1.00     | 0.77 / 0.84                    |
| 10 hot keys, 80/20: ops/s               | PostgreSQL / MongoDB | 34,586 / 25,499                | 25,438 / 17,511 | 25,618 / 17,724                |
| the same: store reads per get           | both                 | 0.15                           | 0.30–0.32       | 0.31                           |
| 1 key, `compute` increments: ops/s      | MongoDB              | 337                            | 779             | 725                            |

Every write of a key not held started a new incarnation (1,000 of 1,000 on the hot key); the version hint (conditional
puts on the last version seen) brought that to about 210, but the conditional puts became a compare-and-set storm and
halved the throughput again; draining the reservation's poison by tickets helped L1 only a little. Each was a
workaround for one root: versions of one key that are not comparable. Elsewhere (single operations, the 1,000-key mix,
propagation) nothing changed, and MongoDB's bulk write (step 5) gained clearly.

**The concept.** A row's version only ever grows, enforced by the store; the versions come from the core.

1. **The store accepts only larger versions.** Every write carries the version the core chose. A write that is not
   decided on a read (a `put`, a tombstone, expiry, the control row) applies only if its version is larger than the
   stored one, or no row exists ("advance": `WHERE version < :new`, MongoDB's `{version: {$lt: new}}` with an upsert,
   a DynamoDB condition expression, a Lua comparison on Redis). A write decided on a read keeps its equality condition
   (only if the version is still the one read, or the row still missing), with a version above it. The store
   compares numbers and nothing else: no `+1`, no start values, no returns.
2. **The core's versions come from a hybrid logical clock per instance.** A version is the time in milliseconds, a
   logical counter and a few random bits per instance (so that two instances never propose the same one). Every
   version the instance sees (a message, a read, its own write) moves its clock forward, so whatever it writes is
   larger than whatever it has seen.
3. **A refused advance is retried above what is stored.** A writer whose clock is behind, or that raced a write with a
   larger version, learns the stored version (from the refusal's read, or its message), moves its clock past it and
   writes again: one more round trip, only then.

**Why it is enough.**

- **Commit order equals version order, guaranteed by the store:** a write going backwards is refused, so the stored
  version only increases, and any two versions of a key are comparable; the larger is always the newer state. No
  incarnations, no order unknown.
- **Correctness does not depend on clocks:** a clock behind only costs refusals and retries. The clocks decide how
  often that happens, never what a reader sees.
- **A put is one write, never a compare-and-set storm:** it is refused only by a stored version larger than its own,
  and its retry wins.
- **Ordering in L1 is the plain rule of before step 2:** the newer version wins, an older message is ignored. The
  reservation registry of step 4 stays, holding the highest version announced meanwhile.

**What goes:** incarnations and the order unknown, dropping both values of an unknown order, the "anything" poison and
its tickets, the version hint, puts conditional on the held version with their retries. **What stays:** the thin SPI
(conditions instead of computed versions; "unconditional" becomes "advance"); `invalidateAll` by the store's time with
the control row and the clear lock (step 3); MongoDB's bulk write in one round trip (a stored larger version makes the
advance fail on the duplicate key, reported per write, as for "missing"); the reservation registry.

**The edge, stated in the contract.** A deleted row cannot defend its version: after maintenance deleted a tombstone,
a write could go below the version it had. That needs a clock behind by more than the tombstone retention (minutes),
and holders dropped the key when the tombstone was announced. A bound like the clock allowance of step 3.

**Compared.** Hazelcast and Infinispan order versions per key through a cluster that issues them; here the store is the
only arbiter and the clocks only propose. Hybrid logical clocks for the same purpose: CockroachDB, and MongoDB's own
cluster time.

**Built and measured (2026-10-28).** The version is one `long`: 43 bits of milliseconds since 2020 (until 2298),
10 bits of logical counter, 10 random bits per instance. One clock per cache instance, not per JVM (changed
2026-10-29): versions are compared only within one key of one store, so sharing adds nothing, and with one clock per
JVM two instances of a cache in one JVM (every test and benchmark) ordered their versions for free, hiding the case of
two JVMs. It showed at once what the common clock had hidden (Lincheck, two instances): two instances may propose
the same version (the same millisecond and counter, node bits drawn alike, one pair in 1,024); the second is refused
as not larger, and a refused write that found its own version stored took it for its own (a retry after an unknown
outcome) and was lost. Now the content decides too: the same version, state and value are the same state, whoever
wrote them. The base cannot move with time: stored versions are compared
with new ones forever, so a later base would make new versions smaller and refused. A clock jumping far ahead would
push every version of the keys it writes ahead of all other instances (each of their writes then refused once and
retried above it); the milliseconds are clamped to the 43 bits, and every refusal is counted
(`DistributedStats#versionRefusalCount`), so a clock far off shows as a steady count. Contention, as in the table above
(local containers, 2 instances x 4 threads, 5 s):

| Scenario                                   | Store                | Before step 2      | Incarnations     | Monotonic versions |
|--------------------------------------------|----------------------|--------------------|------------------|--------------------|
| 1 hot key, 80/20: ops/s                    | PostgreSQL / MongoDB | 29,043 / 25,324    | 21,194 / 14,620  | 17,364 / 23,801    |
| the same: store reads per get, L1 hit rate | PostgreSQL / MongoDB | 0.00 / 0.01, 100 % | 1.00 / 1.00, 0 % | 0.24 / 0.09, 100 % |
| 1 hot key, 50/50: ops/s                    | PostgreSQL / MongoDB | 11,442 / 9,067     | 10,787 / 10,785  | 7,000 / 9,306      |
| the same: store reads per get              | PostgreSQL / MongoDB | 0.00 / 0.02        | 1.00 / 1.00      | 0.94 / 0.35        |
| 10 hot keys, 80/20: ops/s                  | PostgreSQL / MongoDB | 34,586 / 25,499    | 25,438 / 17,511  | 33,738 / 27,033    |
| throughput, 8 threads, 80/20               | PostgreSQL / MongoDB | 35,476 / 25,786    | 34,251 / 26,883  | 35,408 / 26,995    |

L1 holds the hot key again (hit rate 100 %, no new incarnations); 10 hot keys are back at the level before step 2. The
single hot key still reads the store more than before. Measured by where the reads come from (2026-10-29): nearly all
are the reads of refused puts. Concurrent puts of one key propose their versions when called and land in any order;
one landing after a larger one is refused, reads the row and writes again above it (PostgreSQL: 15,575 refusals of
16,236 puts in 5 s; MongoDB: 7,843 of 18,457). Before step 2 the store counted the version up itself, so no put was
ever refused. Not a correctness question; resolved by "Versions applied as well as each store can".

**Separate from it:** a load whose conditional write conflicts again and again (a removal of its key on every attempt)
gives up after three attempts and returns its value without storing it; decided 2026-10-29 in "The uncached load".

### Versions applied as well as each store can

*Decided 2026-10-29, built 2026-10-29 to 2026-10-31, build plan step 6b; refines monotonic versions.*

**Decided 2026-10-29:** one version domain, one `write`, and every store applies an advancing write as well as it can
within its one round trip. Stores that can assign and return a version are never refused; stores that cannot keep
proposal and refusal. The core sees one result for both.

**Why.** Monotonic versions (above) hold under every store, but their price showed with the clocks per instance (one
hot key, 2 instances x 4 threads, 5 s, local containers):

| ops/s                                                     | Versions assigned by the store (before step 2) | Proposed by the core, refused by the store |
|-----------------------------------------------------------|------------------------------------------------|--------------------------------------------|
| 1 hot key 80/20, PostgreSQL                               | 29,043                                         | 14,297 (17,857 refusals of 14,262 puts)    |
| 1 hot key 80/20, MongoDB (single-document updates before) | 25,324                                         | 16,343 (10,697 refusals of 16,354 puts)    |
| 10 hot keys 80/20, PostgreSQL / MongoDB                   | 34,586 / 25,499                                | 30,919 / 23,268                            |

A store assigning versions decides under its lock of the row: each write takes "stored + 1" and is never refused. A
version proposed before the write knows nothing of the writers racing it; the store can only refuse the loser, which
reads and writes again. Not every store can return what it assigned, which is why step 2 left store versions: MongoDB
returns only per document (no bulk), generic JDBC and DynamoDB batches not at all. Two ways of resolving a refusal
without writing again were measured and dropped: deciding by the row's age (written after the put began: ordered just
before it) resolves 4–10 % of the hot-key refusals at the store's clock allowance of 10 ms (83–96 % only without any
allowance, which a store clock stepping back would turn into lost puts); a rule for races within one instance covers
only those.

**The requirements**, as set 2026-10-29: a thin SPI; the best performance for each store; stores that can return
versions and stores that cannot; stores with a global counter and stores without.

**The design.**

1. **The invariant the core relies on (unchanged):** a key's version is a 64-bit number that only grows, enforced by
   the store; any two versions of a key are comparable, the larger the newer state. The core needs nothing else, and
   to know the version of what it wrote or read.
2. **One domain.** Every version has the format of the core's clocks (milliseconds since 2020, logical counter, node
   bits), whoever chose it: the ways one store writes (MongoDB's single put and its bulk `putAll`, on the same key)
   must give comparable numbers.
3. **One `write`, two ways to apply an advance.** The core sends `Write(hash, key, value, proposed version,
   condition)`, the condition one of advance, missing, or equal to a version. The store answers per write: **applied
   at version v**, or **refused**. How it applies an advance is its choice, per call:

   | Applied     | Rule                                                        | Version                 | Refused                                    |
   |-------------|-------------------------------------------------------------|-------------------------|--------------------------------------------|
   | conditional | only if proposed > stored                                   | v = proposed            | when a version at least as large is stored |
   | assigning   | always: v = max(stored + 1, proposed, server time if any)   | v returned by the store | never                                      |

   The core treats both alike: it takes the version reported back (in L1, in its clock, as what its messages announce);
   on a refusal it reads and proposes again above (as now). No capability flags: a store that can do more is simply
   never refused. Missing and equal conditions apply as now, at the proposed version (a store may assign there too).
4. **Each store does its best per call:**
   - PostgreSQL: always assigning, single and bulk (`ON CONFLICT … SET version = GREATEST(r.version + 1,
     excluded.version) … RETURNING`);
   - MongoDB: assigning for a single write (`findOneAndUpdate` with a pipeline: `$max` of version + 1 and the
     proposal), conditional for `putAll` (the bulk write of step 5, one round trip);
   - Redis / Valkey (a candidate): assigning in bulk (one Lua script, the server's `TIME`);
   - generic JDBC, DynamoDB batches: conditional; dialects with `RETURNING` / `OUTPUT` may assign later.
5. **A global counter is a way to assign, not a dimension of the SPI.** It helps only inside the write (an extra round
   trip to draw it costs more than a refusal). Its cheapest form is the server's clock in the version format, taken
   into the maximum of an assigning write. It adds two guarantees the instances' clocks cannot give: the edge of a
   deleted row that cannot defend its version closes (a new row lands above everything the server issued), and
   instances' clock skew no longer matters for that store. A true counter leaves the version domain, so it serves
   only a store whose every path assigns: PostgreSQL's sequence did, before step 2, drawn with every write (`nextval`
   is not transactional and cheap; a single key counted up with Redis `INCR` would be a hot spot, `TIME` serves
   better there). See point 6 for which source each store takes.

6. **The floor of a new row** (added 2026-10-29). A row created anew, above all after maintenance deleted the old
   one's tombstone, has no stored version: "stored + 1" alone would start it at 1, below what holders still hold (a
   value at 57 ignores a message at 1 as older, and is served stale forever). Something global must lie above every
   earlier version of the key. Before step 2 the stores had it themselves, without a clock of the instances
   (ee681c4): PostgreSQL started a new row at `nextval()` of the table's sequence and drew `nextval()` for every
   write's stamp too, so the sequence ran ahead of every count of a key ("so that neither depends on a clock: a clock
   stepping back, a failover to a standby whose clock is behind"); MongoDB started a new document at the server's
   `$$NOW` in microseconds (above every earlier version while a key is written less than once a microsecond on
   average), every later write adding 1, returned by `findOneAndUpdate` one document at a time. Hence two cases:
   - **A store that assigns on every path and has a floor of its own** (PostgreSQL with its sequence, Redis / Valkey
     with `TIME`) ignores the proposal: its versions may be a domain of their own (sequence numbers), as no
     conditional path writes into it, and no instance's clock reaches its rows. The core sends the proposal anyway;
     the SPI does not change.
   - **A store that mixes assigning and conditional paths** (MongoDB: single writes assigning, `putAll` conditional)
     needs one domain for both: its floor in the format of the core's versions (the server's milliseconds shifted as
     theirs, not microseconds), and the proposal in the maximum.

   Conditional stores have only the proposal.
7. **A bound on clocks far ahead** (added 2026-10-29). Where the proposal counts, one instance's clock far ahead puts
   its numbers into rows; the other instances observe them and follow, and stored versions are compared for as long
   as their rows live, so the whole domain moves, for good. Harmless for minutes or hours (278 years of range); not
   for a clock grossly wrong: the milliseconds are clamped at the end of the range (2298), but the counter goes on,
   and there only about 1,000 versions are left before `last + TICK` overflows and the clock repeats one version
   (every write then refused until the attempts give up, as a store failure), reached by every instance that
   observes one such version (found 2026-10-29, in the clamp of 6a). As hybrid logical clocks do elsewhere
   (CockroachDB's maximum clock offset):
   - in the core, a version more than a maximum offset ahead of the instance's own time is neither observed nor
     proposed, but counted and logged; the store still enforces the order (a write lands above such a row), the clock
     only does not follow it into the future (built 2026-10-29 with two thresholds: reported from a minute ahead, not
     followed from a day ahead, a write on such a row just above it; see step 6b);
   - in an assigning store that keeps the proposal, `v = max(stored + 1, min(proposed, server time + offset))`, so that
     no instance's clock moves its rows (a conditional store cannot portably, the condition would need the server's
     time);
   - the end of the range an explicit error instead of a version repeated.

**Why it is the best under these requirements.** Assigning in the store is the only way a racing put costs no extra
round trip; proposing is the only way stores without returns can order at all. Each store gets the first wherever it
can, the second only where it must, and the core one code path for both. Correctness needs only the invariant on
every path, no clock: the clocks only decide how often a conditional store refuses. Expected (to be measured): one hot
key back to the level before step 2 on PostgreSQL, on Redis / Valkey, and for single MongoDB writes; stores without
returns keep the measured cost of one more read and write per race.

**What changes:** the SPI's result, from conflicting hashes to applied version or refused per write; the core takes
the reported version (advance, the L1 install, messages); PostgreSQL and MongoDB (single writes) get the assigning
path; the contract tests check both ways, and the in-memory store runs in both modes, so that Lincheck and the property
tests cover both.

## Distribution

### Distribution: messages carry no values

On both stores a message is only **(kind, hash, version)**. A holder that needs the new value reads the row; values
never travel through change streams or notifications.

| Kind              | Holders do (only if the key is held and the message version is higher)                         |
|-------------------|------------------------------------------------------------------------------------------------|
| `WRITTEN`         | population on: read the row and apply what it holds (value or tombstone); population off: drop |
| `INVALIDATED`     | drop, no read                                                                                  |
| `EXPIRED`         | drop, no read (the maximum age was exceeded; distinct from `INVALIDATED` for stats only)       |
| `INVALIDATED_ALL` | clear L1, increment the epoch (no hash, no version)                                            |

- **Reads happen in the delivery path, batched:** per delivered batch, keep the held and older hashes and read them
  in one query (`WHERE id = ANY(...)` / `$in`). This does not stall delivery on a dead store in a way that
  matters: the messages come from the same store. A failed read drops the affected keys.
- **The read decides, the kind is a shortcut.** Whatever the row holds is applied under the version rule, so a
  message of the wrong kind costs at most one read, never correctness.
- **Age expiry sends a message (`EXPIRED`); size pruning, tombstone pruning and local eviction do not.** Only facts
  the whole cluster agrees on are distributed: an exceeded maximum age is decided by the store's clock and is the
  same for everyone, while size pruning only concerns the store's space and eviction is a local decision.
- **Rows carry an explicit state** (`VALUE` / `TOMBSTONE` / `EXPIRED`) rather than "value is null", which also keeps
  the pruning queries simple (one index on discriminator, state and write time). `EXPIRED` behaves like a
  tombstone.

How the adapters derive the kind:

- **PostgreSQL:** the library writes the `NOTIFY` itself, so the payload is `kind|hash|version`.
- **MongoDB:** from the event itself (measured, see below). A pipeline upsert is reported as `insert`, `update` or
  `replace`, so the adapter handles all three: `insert` and `replace` carry the whole document, so the state is
  read from `fullDocument`; `update` carries `updatedFields`, where the version is always present and the state
  only when it changed. `TOMBSTONE` means `INVALIDATED`, `EXPIRED` means `EXPIRED`, anything else (including no
  state in an `update`) means `WRITTEN` (two tombstones in a row look like a write; the read finds the tombstone).
  The value is projected out of events on the server (`fullDocument.value` and
  `updateDescription.updatedFields.value`), so watchers never receive it. No `UPDATE_LOOKUP`, no pre-/post-images.
- **Deletes carry no meaning, on both stores.** A delete event does not say why a row was deleted, so one from size
  pruning would look exactly like one from expiry, and dropping L1 copies on it would distribute size pruning after
  all. The Mongo watcher filters deletes out on the server (`operationType` in `insert`, `update`, `replace`);
  PostgreSQL does not notify them. Size pruning, tombstone pruning and the cleanup of expired rows delete
  silently. A key written again after its row was deleted gets the server time as its version, which is higher
  than the old one because time has passed since that write.

Why not values in messages: they spare holders one round trip, but PostgreSQL cannot carry them (`NOTIFY` is limited
to about 8 KB), every watcher would receive every value whether held or not, and "no value" would become ambiguous
(tombstone or omitted). (The 16 MB event limit turned out not to matter for this write shape, see the measurement.)
For comparison, today's MongoDB delivery already reads on every event (`UPDATE_LOOKUP`, per watcher, held or not).

Accepted cost: with population on, a write to a key held by N instances causes N reads (batched per instance), and
holders serve their old value for one round trip longer than with values in messages.

Measured (`MongoChangeEventExperimentTest`, MongoDB 4.4.29, 5.0.31, 6.0.28, 7.0.45, 8.3.13, 9.0.4, single-node replica
set):

1. **Event types of a pipeline upsert** (`$set` with `$$NOW` and the version expression, `$unset` of the value for
   tombstones and expiry):
   - 4.4 reports every pipeline update as `replace`, with the whole document.
   - 5.0 and later report it as `update` when the set of fields stays the same (same or new value, tombstone after
     tombstone) and usually as `replace` when fields are added or removed (value → tombstone or expired, and back).
     Not deterministic: 8.3 once reported "value after expired" as `update` with the state in `updatedFields`.
   - The version is always present (`updatedFields` or `fullDocument`); the state is in `fullDocument` for `insert`
     and `replace`, and in `updatedFields` exactly when it changed. Fields set to an unchanged value are omitted from
     `updatedFields` (a `writeTime` within the same millisecond was missing), so the key never appears in `update`
     events.
   - The server-side projection removes the value from all event types on all versions.
   - `upsert = true` changes neither the event types (identical sequences with and without it, every write of the
     sequence repeated with both settings) nor the time per write (differences within noise, 2,000 writes per
     variant). What triggers `replace` is adding or removing fields, not the flag.
2. **The 16 MB limit:** a document at the maximum size, written with a new value and then turned into a tombstone,
   produced `insert`, `update`/`replace` events without errors on every version, raw and projected. Without
   `UPDATE_LOOKUP` and pre-images an event is the document plus a little metadata, which the server accepts.

Measurement 2 (`MongoPopulationExperimentTest`, MongoDB 8.3, single-node replica set, one writer, N holders of one hot
key, each with its own client and change stream, 1 KB values changing on every write, 1,000 writes per variant,
each variant twice; "applied" is the time from the write's start until a holder has the new value, over all
holders; A takes the value from the event, B projects it out and reads the rows of each delivered batch in one
query):

| Journal default | Holders | A applied p50 / p99          | B applied p50 / p99          | B reads per write | Writer's ack p50, A / B  |
|-----------------|---------|------------------------------|------------------------------|-------------------|--------------------------|
| `true`          | 1       | 762–920 / 2,175–2,517 µs     | 1,067–1,071 / 2,107–2,259 µs | 1.00              | 707–862 / 778–824 µs     |
| `true`          | 4       | 1,064–1,069 / 2,266–2,871 µs | 1,305–1,310 / 2,297–4,508 µs | 4.00              | 988–1,001 / 995–1,009 µs |
| `true`          | 16      | 1,195–1,215 / 2,392 µs       | 1,940–2,089 / 3,106–3,840 µs | 15.94             | 971–976 / 1,206–1,307 µs |
| `false`         | 1       | 270–272 / 417–469 µs         | 603–613 / 970–991 µs         | 0.55              | 211–215 / 202–207 µs     |
| `false`         | 4       | 381–399 / 614–705 µs         | 791–802 / 1,233–1,409 µs     | 2.27              | 281–304 / 276–281 µs     |
| `false`         | 16      | 774–812 / 2,192–2,409 µs     | 1,511–1,681 / 4,673–5,757 µs | 8.79–8.94         | 520–545 / 519–581 µs     |

- **B costs holders about one read round trip more:** +0.15–0.3 ms with the default journal setting at 1–4
  holders, +0.8 ms at 16; about twice A's time with journal default `false`. Small in absolute terms,
  1.2–2 times in relative terms.
- **B loads the store with one read per holder and write** (fewer when events arrive in batches), and at 16
  holders this slows the writer itself by about 25 % with the default journal setting.
- **A measurement trap, fixed:** a holder's read can return a later write that is applied in memory but not yet
  majority-committed (no event yet). Counting that as applied made B look faster than A; only writes whose events
  arrived are counted.
- **Here every holder held the key**, the best case for A and the worst for B; the follow-up below measures the
  realistic case.

Follow-up for non-holders (`MongoNonHolderExperimentTest`, MongoDB 8.3, single-node replica set, 10,000 keys, 8
instances each holding 1 % or 10 % of them, 4 writers writing 20,000 times to random keys; A raw reads events as
raw BSON and decodes only held keys, A decoded decodes every event, B as before; each variant twice):

| Value | Held | Writes/s, A raw / A decoded / B         | CPU per instance, A raw / A decoded / B | Received per instance, A / B | Held p50, A / B    | B reads per write |
|-------|------|-----------------------------------------|-----------------------------------------|------------------------------|--------------------|-------------------|
| 1 KB  | 1 %  | 3,539–3,750 / 3,651–3,710 / 3,617–3,623 | 324–347 / 313–358 / 320–342 ms          | 29.6 / 8.8 MB                | ~1,300 / ~1,670 µs | 0.08              |
| 1 KB  | 10 % | 3,582–3,612 / 3,637–3,650 / 3,553–3,690 | 329–358 / 350–365 / 324–338 ms          | 29.6 / 8.8 MB                | ~1,330 / ~1,620 µs | 0.70              |
| 10 KB | 1 %  | 2,982–3,144 / 3,096–3,119 / 3,272–3,285 | 428–437 / 467–470 / 355–359 ms          | 214 / 8.8 MB                 | ~1,470 / ~1,690 µs | 0.08              |
| 10 KB | 10 % | 2,852–3,057 / 2,705–2,823 / 2,719–3,008 | 444–451 / 449–500 / 429–466 ms          | 214 / 8.8 MB                 | ~1,500 / ~1,880 µs | 0.70              |

- **Holders gain about 0.2–0.4 ms with A** (20–25 %), as in measurement 2.
- **Every instance pays for it in bytes:** A sends each instance every value, 3.4 times B's traffic with 1 KB values
  and 24 times with 10 KB. Here that is localhost; across a network, 8 instances at 3,000 writes/s of 10 KB values
  are about 240 MB/s leaving the store, against about 10 MB/s with B.
- **CPU and writer throughput barely differ;** raw versus decoded events makes no clear difference. With 10 KB
  values A costs instances up to about 25 % more CPU at 1 % held.
- **B's store reads follow the holders** (0.08 reads per write at 1 % held, 0.70 at 10 %), negligible unless many
  instances hold the same hot keys (measurement 2: at 16 holders of one key the writer slowed by about 25 %).

**Conclusion (confirmed 2026-10-21): messages carry no values, on MongoDB too.** Values in events win only when many
instances hold the same keys, and cost traffic that grows with instances × writes × value size; reading held keys
costs in proportion to what instances actually hold. Values in events could become a MongoDB opt-in for hot keys
if there is ever demand; not built in advance.

### Distribution modes

*Decided 2026-10-21.*

`withDistributionMode(DistributionMode)`, set at most once, replaces `withPopulation()`:

| Mode                   | Subscription                   | A write elsewhere           | An invalidation or expiry elsewhere |
|------------------------|--------------------------------|-----------------------------|-------------------------------------|
| `NONE`                 | none (no listening connection) | not seen                    | not seen                            |
| `INVALIDATE` (default) | yes                            | held copy dropped           | held copy dropped                   |
| `UPDATE`               | yes                            | held copy re-read (batched) | held copy dropped                   |

- **One setting, three levels:** population is built on invalidation (same subscription, same messages; only a
  write is applied differently), so "population without invalidation" would distribute writes but not removals.
  An enum makes that combination unrepresentable and leaves room for a level.
- **Names (renamed 2026-10-22, were `INVALIDATION` and `POPULATION_AND_INVALIDATION`):** each value says what a
  holder does on a change elsewhere, and a write is a change too. That is how comparable libraries name it: Redisson's
  `SyncStrategy` (`NONE`, `INVALIDATE`, `UPDATE`), Infinispan's invalidation mode, Hazelcast's
  `invalidate-on-change`. The combined name read as if invalidation were a feature added on top. `UPDATE` rather than
  `UPSERT`: a holder's copy is updated, never inserted (invariant 8; Redisson's `UPDATE` does insert), and upsert
  already names the store write.
- **`INVALIDATE` is the default:** the cheapest level that keeps L1 correct (a dropped key costs a store read only
  if it is read again), with the versions, gap detection and revalidation that JetCache's best-effort sync lacks.
- **Population stays opt-in:** it reads eagerly, one store read per write and holding instance, whether the copy is
  read again or not (measured on MongoDB: 16 holders, 16 reads per write, the writer about 25 % slower).
- **`NONE` is JetCache without `syncLocal`:** for short-lived local copies or data that rarely changes, saving the
  listening connection and its cost on the store (on PostgreSQL about 33 µs of CPU per write and listening session).
  Nothing bounds how long a copy changed elsewhere is served except Caffeine's expiry, so if neither
  `expireAfterWrite` nor `expireAfter` is configured, L1 gets `expireAfterWrite` of the store's maximum age (as
  JetCache's local expiry defaults to the cache's); `policy()` shows it. `expireAfterAccess` alone does not bound
  the age, so the default is added next to it. Without a subscription there are no gaps, no revalidation and no
  messages for negative caching; held values count as trusted. `invalidateAll()` and `invalidate` reach only the store and this instance's L1.
- The property test covers `INVALIDATE` and `UPDATE`; its convergence oracle does not
  apply to `NONE` by design, which has unit tests of its own.

### How a message finds the held key: `InternalKey` compares by hash

Messages carry only the hash, so L1 must be searchable by it. `InternalKey` compares by its hash instead of by
key: a message builds a lookup key from the hash alone and finds the held entry directly in Caffeine, and the
registry of minimum versions for loads in flight is keyed the same way. Whatever Caffeine holds is exactly what
messages find, including during evictions, expirations and loads in flight.

- **Identity is the `Hash`; `InternalKey` holds its two `long`s** (decided after the benchmark below):
  - Every key's hash is a `Hash` of 128 bits (built-in key types, and custom ones from the `Hasher`), whose two `long`s
    the `InternalKey` copies and compares; its hex is made once, on demand, from a `Hash` of them. Copied rather than
    the `Hash` kept (measured 2026-11-20, `HitPathBenchmarkTest`, one session): the JIT allocates no `Hash` on a hit
    then (escape analysis), 24 bytes less per hit (72 against 96 bytes per `String` key), and a held key is one object,
    not two; the time per hit was the same, the cache of the benchmark fitting the CPU's caches. Built-in key types
    (`String`, `Long`, `Integer`, `UUID`, enums) are hashed by `Hasher.builtInHash`, one shot, seeded per kind (see "The
    stored hash format"), and the bits are read directly, so no string is created on the hit path; the hex the stores
    see is the same `Hash`'s (`toHex()`). One shot needs no stream, so no thread-local, no `Thread.isVirtual()` check
    and no pool. Measured 2026-11-19 (`HashStreamBenchmarkTest`, JDK 25, a loaded laptop: numbers to repeat on the Linux
    baseline), the ways before and those considered:
    - one shot, as built: 16 ns per 19-char `String` key, 5 ns per `Long` key, alike on platform and virtual threads;
    - a stream per platform thread, as before: 20–23 ns per `String` key, 7 ns per `Long` key, 32 bytes (the result);
    - a fresh stream: 33–42 ns and 416 bytes (a 264-byte buffer inside), what a virtual thread would pay per key;
    - a stream per thread on virtual threads too: never reused, so a stream per request kept for the thread's life,
      about 416 bytes per live virtual thread, which JEP 444 advises against;
    - a pool of streams striped by thread id: 10 to 18 times slower under 4 threads unpadded (slots sharing cache
      lines), padded close for `Long` but twice the time for `String`; every key writes shared memory.
    Custom keys return their `Hash`, which the core holds alike; no hex is made or parsed on the hit path.
  - A whole hit (`HitPathBenchmarkTest`, 2026-11-20, JDK 25, a laptop, 1 thread, `getIfPresent` on 10,000 keys): plain
    Caffeine 22 / 22 ns per `String` / `Long` key; a `DistributedCache` with built-in hashing 72 / 48 ns and 72 / 96
    bytes; with a hash provider 76 / 47 ns and 208 / 152 bytes; the hex string the way before, as a model, 118 / 91 ns
    and 497 bytes. To repeat on the Linux baseline, where 4 threads can be measured too.
  - A lookup by another string, one of the library's own rows (`#…`, such as the heartbeat's), is kept and compared
    as the string. Which representation a lookup uses follows from the hash string alone, so every instance decides
    it the same way, and a 32-hex string never equals any other string, so the two never need to be compared.
  - The hex string is produced only for the store and messages, and cached in the wrapper. Row ids and messages
    carry it; a message lookup parses it back into the `Hash`.
- **The user API:** `Hashable` and `HashProvider` return a `Hash`, computed with the supplied `Hasher`, so every row
  id has one form (see "The stored hash format").
- **Documented contract:** keys that are `equals` must produce the same hash (already required in practice).
- **Cost:** hashing on every user call, since a new wrapper has no cached hash: 2–4 times a plain hit and 32 bytes
  per call (measured below), against 5–7 times and about 500 bytes for the hash string.
- **Possible refinements, only if needed:** avoiding the `HashValue128` allocation, or a faster hash for built-in
  key types, which would have to give the same bits as today's stream so that stored rows stay compatible.

Measured (`HitPathBenchmarkTest`, JMH 1.37, JDK 25, `getIfPresent` on a filled cache of 10,000 keys, keys like
`user:000123:profile` or random `Long`s, one fork, 3 × 1 s warm-up, 5 × 1 s measurement, average time per call
and thread):

| Variant                                               | `String` key, 1 / 4 threads | `Long` key, 1 / 4 threads | Allocated per call  |
|-------------------------------------------------------|-----------------------------|---------------------------|---------------------|
| plain Caffeine                                        | 21.1 / 17.3 ns              | 17.0 / 15.0 ns            | 0                   |
| wrapper compared by key (today)                       | 18.1 / 14.3 ns              | 18.1 / 12.8 ns            | 0 (escape analysis) |
| by hash string, as today's `Hasher.getHash()`         | 117.9 / 102.6 ns            | 85.0 / 76.3 ns            | 496 B               |
| by hash string, reused stream, hex from the two longs | 186.7 / 168.4 ns            | 149.2 / 130.4 ns          | 168 B               |
| by the two longs (compact form)                       | 65.9 / 62.5 ns              | 45.5 / 29.0 ns            | 32 B                |

- **The hash string costs too much on the hit path:** 5–7 times a plain hit and about 500 bytes of garbage per
  call (at a million hits per second, 500 MB/s). Formatting the hex from the two longs is even slower.
- **The two longs cost 2–4 times a plain hit** (about 30–65 ns instead of 15–20) and 32 bytes per call, most of it
  hashing the key through the stream. Small next to a write (0.5–1 ms), so the compact form is the decision.

Rejected alternatives:

- **A side index from hash to `InternalKey`:** every L1 change, including evictions and expirations inside
  Caffeine, would have to update it in the same step; a path that forgets it makes an invalidation miss a held
  key, which then serves an outdated value without any error.
- **Reading every notified row to get the key, as the old version does:** it keeps comparing by key, but store
  reads then grow with instances × writes whether anyone holds the key or not (16 instances at 5,000 writes/s are
  80,000 row reads/s), invalidations and expiries need reads too, and with population off all of them only serve
  to drop keys. Not needed: the compact form keeps hashing on the hit path affordable.
- **The key in the message:** Mongo update events contain only changed fields, so the key would need
  `UPDATE_LOOKUP` or post-images.

### Connection sharing: always per database

Listening connections are shared between caches, always per database: one session per listener `DataSource` on
PostgreSQL (R2DBC's `ConnectionFactory` until 2026-10-28), one change stream cursor per `MongoClient` and database
on MongoDB. There is no sharing mode to choose: today's `ListenerSharingMode` / `WatcherSharingMode` (`INSTANCE`,
`TABLE`/`COLLECTION`, `DATABASE`) are dropped. On PostgreSQL the reason for the narrower levels was the pgjdbc
limit of about 750 notifications/s per session, which R2DBC did not have (one session carried 30,000/s, below), nor
has the stream taken over; on MongoDB `DATABASE` was the default already; what remained was "isolation", with no
concrete case behind it.

- **One shared session / cursor per database**, reference-counted in a registry; every cache has its own subscription
  with its own worker queue (coalescing hash set, generous overflow limit with drop and `onGap()`). The shared thread
  never calls into a cache, so it cannot deadlock on a cache's lock (the constraint that shaped today's design).
- **MongoDB as today:** widening and narrowing reopen with `resumeAfter`; no discriminator filter on shared cursors (the client
  drops scopes nobody subscribed to); a cache whose collection is already covered registers directly.
- **A gap reaches every subscriber:** an interrupted shared session or cursor means `onGap()` for every cache on it.

What changes:

- **PostgreSQL's sharing key** is the listener `DataSource` (by identity; the R2DBC `ConnectionFactory` until
  2026-10-28);
  MongoDB's is `MongoClient` and database.
- **As few sessions as possible is also cheapest for PostgreSQL:** every listening session costs it about 33 µs per
  write.
- **PostgreSQL channel per cache** (table and discriminator), dispatched by channel name, so the payload stays
  `kind|hash|version`. Channel names are limited to 63 bytes, longer ones are shortened with a hash. No extra
  PostgreSQL CPU: every `NOTIFY` wakes every listening session anyway.
- **Joining without a poll on PostgreSQL:** R2DBC pushes notifications, so today's 100 ms poll and its join wake-up
  are gone; a join is a `LISTEN` on the shared connection.
- **Failed applies are dropped on both stores:** MongoDB re-queues today because of the old persistence semantics;
  in this model dropping a key is always safe (as for failed reads).

Measured (`PostgresR2dbcCeilingExperimentTest`, removed 2026-10-28 with R2DBC; PostgreSQL 18, one R2DBC session
listening on 20 channels as for 20 caches, writes spread over all channels by 8 or 16 paced writers, each rate twice):

| `synchronous_commit`     | Target writes/s         | Reached             | Arrival p50 / p99                        | Lost | PostgreSQL CPU per 1,000 writes |
|--------------------------|-------------------------|---------------------|------------------------------------------|------|---------------------------------|
| `on` (default)           | 5,000 / 10,000–40,000   | 5,000 / 7,370–7,610 | 0.3 / 1.3–1.5, then 1.0–1.1 / 1.4–1.5 ms | 0    | 91–101 ms                       |
| `off` (measurement only) | 5,000 / 10,000 / 20,000 | as targeted         | 0.5–0.6 / 1.0–1.6 ms                     | 0    | 95–110 ms                       |
| `off` (measurement only) | 40,000 / 80,000         | 29,700–30,300       | 0.5–0.6 / 1.2–1.5 ms                     | 0    | 88–90 ms                        |

- **One session carried every rate PostgreSQL could write** (about 7,500 single-row writes/s with default commits
  on this machine, about 30,000/s without commit flushes) at about 0.5–1 ms p50, under 1.5 ms p99, nothing lost.
  Its own limit lies above 30,000 notifications/s; PostgreSQL's write path is the bottleneck, not the listener.
- **So sharing per database has no throughput downside on PostgreSQL** with R2DBC.

### Heartbeat: a silent feed is noticed by the core

*Decided 2026-10-23.*

A change feed can stop delivering without any error (a silent feed: a network that silently stopped carrying, a
cursor or listening session stuck without failing), and the cache would believe it is in step while it misses every
change. Only a heartbeat notices that, and it sits in the core, so that no adapter has to build one:

- **The beat is an ordinary write:** every interval (`withHeartbeatInterval`, 10 s by default, ±20 % jitter) each
  instance with distribution invalidates a reserved key (hash `#heartbeat`, which no built-in hash can be; a custom
  hash equal to it is refused) and expects the announcement before its next beat. No SPI method: any `ChangeFeed`
  that fulfils its contract carries it. One tombstone row per cache, pruned and recreated like any other.
- **Per key in commit order:** the reserved key announced at this instance's version or a later one (another
  instance's beat) proves that its own beat arrived.
- **A missed beat** makes the feed silent: counted as an interruption, the state `DEGRADED`, nothing held trusted
  (every read checks the store), as when an adapter reports `onLost()`; and the core resumes the subscription, which
  re-establishes it although it seems alive (see Supervision). The next announcement of the reserved key ends the
  silence with a gap: what was announced meanwhile may be lost, so what is held is revalidated.
- **No beat while the store guard is open** or the store does not answer: that is the guard's business, not the
  feed's.
- **Beats are invisible:** neither applied to L1 nor counted in the message stats.
- **Bound:** a silent feed is noticed within about two intervals; until then, copies changed elsewhere can be served
  (as without the heartbeat, but bounded).
- **Tested:** the decisions per tick (unit), a paused in-memory delivery (fails with the beats disabled), and a
  MongoDB change stream held from reading.

### Supervision: the core resumes the feed

*Decided 2026-10-23.*

A feed does not heal itself: it reports a lost delivery (`Listener.onLost(cause)`) and re-establishes it when asked
(`Subscription.resume()`). When to ask, how long to wait, and noticing a feed that stopped without a word are the
core's, the same for every adapter:

- **On `onLost`:** the cache is interrupted (`DEGRADED`, every access checks the store) and resumes at once, then
  with backoff (250 ms, doubling, at most 30 s, ±20 % jitter) until `resume()` returns. **Every resume is a gap**
  (revalidation): the feed need not deliver what was written meanwhile, and the core keeps no second, lossless path.
  Until 2026-10-23 `resume()` returned whether nothing was missed (MongoDB from its token), which saved one
  version-only pass over the held keys per resume at the cost of a second recovery path in the core and in every
  adapter. A loss reported during an attempt asks for another.
- **On a missed heartbeat:** the same resume, although the feed seems alive: a connection the network stopped
  carrying without a word is re-established instead of waited for.
- **Shared sessions:** a feed shared by several caches re-establishes it once per loss, whoever asks first; the
  others find it done (a generation per connection or stream). Only a cache not told of a loss can suspect the
  session of silence: for one told, a session there now is a newer one (otherwise a cache resuming after another
  already reconnected tore down the fresh session, failing the probe of a cache joining meanwhile; found by
  `PostgresConnectionTest`). One that re-establishes a session that seemed alive tells the other caches still
  delivering on it a gap where the store keeps no history (PostgreSQL).
- **What went from the adapters:** both reconnect loops with their backoff, and the PostgreSQL listener's own
  heartbeat (a self-notification every 10 s, with its timer and worker threads); the probe on subscribing stays (it
  detects a transaction-mode pooler). MongoDB still reopens from the token, so that the other caches on the shared
  stream miss nothing.
- **Contract:** `ChangeFeed` point 5, tested by the change feed contract (a loss induced, `resume()` restores
  delivery; resuming a delivery that seems alive keeps it delivering).

### The polling feed

*Worked through 2026-10-24.*

A generic `ChangeFeed` for stores without a native one, from the musts only. One poller per instance and cache.

**What it polls:** the rows of the cache written since its cursor, by write time, without values: key, state,
version. The kind comes from the state (`VALUE` → `WRITTEN`, `TOMBSTONE` → `INVALIDATED`, `EXPIRED` → `EXPIRED`), a
change of the control key is `INVALIDATED_ALL`. **Every write must set the write time, also expiry:** today expiry
keeps the old write time (so an expired row is already old enough for the next tombstone pruning, and its grace
period is in effect zero); with the poll it must take the store's time, so that the poll finds it and the grace
period counts from the expiry. Ages only matter for value rows, so nothing else changes. Size pruning and the delete
of hidden rows are silent, and the poll does not see them, as intended.

**One index for poll and maintenance:** `(discriminator, write_time)` instead of today's
`(discriminator, state, write_time)`. The poll is a range on it; expiry (value rows older than the maximum age) and
tombstone pruning become ranges with the state as a filter, scanning the few tombstones and expired rows along.

**The overlap: why, and how small.** A write gets its time when its statement runs and becomes visible at commit, a
moment later (the window found for `invalidateAll`). A poll advancing its cursor to "now" would miss a row committed
just after with an earlier write time. So each poll reads from `cursor - overlap`, and duplicates are no-ops by the
version rule. **The overlap stays small (default 2 s), and the writers cover what it does not:** a write whose call
took longer than the overlap, or failed with its outcome unknown, may have committed too late for every poll; the
writer recognises that (it timed its own call) and touches the key again once the write must be over (after twice
the store timeout for an unknown outcome): an invalidation, announced through the next poll. A rare case, one extra
write, a miss for holders.

**Loss without loss:** a failed poll keeps its cursor and tries again; nothing is lost, since rows persist. The only
limit is the grace period: tombstones and expired rows are deleted after it (5 minutes), so a poller that could not
poll for longer than the grace period minus the overlap may have missed some, and reports `onLost`; the core
resumes, which is a gap. Below that limit, a failing poller stays silent, and the core's heartbeat notices the
silence after its interval and checks every access against the store meanwhile, as for any silent feed. **The poll
interval must be well below the heartbeat interval** (defaults: poll 1 s, heartbeat 10 s), checked at build time.

**The contract of `ChangeFeed`:** points 1, 2, 4, 6 and 7 hold as they are (a message per change found, its version,
from subscribing until closing, reads through the same store, the own cache only). **Point 3 does not:** a poll sees
only the latest state of a key, not every write since the last poll. The core needs no more than that: the version
rule, the minimum versions of loads and writes in flight, the heartbeat and negative caching all compare with the
highest version, and population reads the row anyway. So point 3 can be relaxed to "per key, at least the latest
write since the last delivery arrives, and none older after a newer one", which the native feeds fulfil as well.
**This weakens the contract; it adds no condition.**

**Cost, core side: depends on the write rate, and can be high.** Every instance runs one indexed range query per
interval, returning every change of the last interval plus overlap: about `instances × write rate × (interval +
overlap)` rows of key and version per second across the cluster. With 10 instances and 100 writes per second:
3,000 small rows per second; with 50 instances and 1,000 writes per second: 150,000. Polling suits moderate write
rates and few instances; above that a native feed is the way (both databases here have one). Latency: up to the poll
interval, against milliseconds for a native feed.

**Built 2026-10-24 (step 4), in the core:** `InternalPollingStore` wraps a store that offers a `ChangeScan` (the new
optional SPI primitive: the changes of the cache's rows written within a window, by the store's clock, as messages
without values) but no `ChangeFeed`, and is one: the core chooses a native feed first, else the poller, else only
`DistributionMode.NONE`. Departures from the study above, found while building it:

- **The scan reports no `INVALIDATED_ALL`:** every poll reads the cut-off (a read of no keys) and announces a change of
  it itself, so a slow `invalidateAll` cannot slip past the overlap (re-announcing it would mean another
  `invalidateAll`, which hides the writes in between).
- **The touch rewrites a key as it is, conditionally on its version** (a value with the same bytes, a tombstone or
  expired row as a tombstone), not with an invalidation, so a slow put keeps its value; rows hidden by the cut-off
  are skipped (rewritten, they would come back). A slow expiry, which returns no keys, touches the expired rows a
  scan of its window finds.
- **A failed poll reads from the same point next time;** only failures longer than the grace period of tombstones
  minus the overlap report a loss. `withPollInterval` (1 s by default) must be at most half the heartbeat interval,
  checked at build.

Tests: `ChangeScanContractTest` (the in-memory store: within the window, no deletes, own cache only, no control row),
`InternalPollingTest` (distribution, `invalidateAll` through the cut-off, an expiry older than a poll's window, the
touch of a slow write and of an unknown outcome, a hidden row not touched back, the grace period and resume, the
interval check), and the property test with stores offering only a scan. Mutation-checked: nothing delivered, no
overlap, no touch, no cut-off read, an expiry keeping its old write time each make tests fail. PostgreSQL and MongoDB
keep their native feeds and implement no scan yet (a plain JDBC adapter would).

**Built 2026-10-24 (step 5), the two levels:**

- **`Store`** stays the rich contract, implemented natively by PostgreSQL and MongoDB (one round trip per batch,
  their own feeds); nothing changed for them.
- **`PrimitiveStore`** (after the refactoring below; built as `RowStore` and `GenericStore`) is the floor: the primitives only (read by id with ages, upsert, replace on a version, insert if
  absent, delete on a version, scans without values), atomic per row, versions, write times and stamps by the store.
  It builds the whole `Store` and a `ChangeScan` from them: one primitive per key, maintenance by
  scans in memory (expiry and size pruning read the heads of all value rows of the cache per run: fine for moderate
  sizes, the cost a native adapter avoids), the cut-off as a reserved row. With the core's poller, an adapter
  implementing the primitives only has a distributed cache.
- **The proof: `jdbc.JdbcAdapter`**, plain JDBC in standard SQL (`MERGE` with `NEXT VALUE FOR`, `UPDATE`/`DELETE ...
  WHERE version = ?`, an `INSERT` failing on a duplicate key, `CURRENT_TIMESTAMP`), tested on H2 in memory, so in
  every build without Docker. Versions and stamps come from one sequence: a new row starts above every earlier
  version without any clock, which closes the clock residual of step 3 for it. The table, its index on the write
  time and the sequence are created if missing.
- **Tests:** the store contract (33) and the change scan contract (5) on H2, the property test on H2 with
  distribution by polls only (4 seeds), an adapter test (schema, discriminators, distribution, `invalidateAll`).
  Mutation-checked: old values read, hidden rows counted in size pruning, an upsert keeping its version, an insert
  starting at 1 (the last one survived until a contract test for a key created conditionally after its row was
  deleted was added, which every store now runs).
- **Not done:** the core suites (`InternalCoreTest` and the others) on H2: they wait for messages in milliseconds,
  which polls do not deliver; the property test covers the core on it instead. Databases other than H2 are
  untested; the SQL is standard, but `SELECT CURRENT_TIMESTAMP` without `FROM` and the duplicate-key `SQLState` 23
  are where others may need a dialect.
- **Moved to MariaDB (2026-10-25)**, instead of H2: the tests run on `mariadb:11.8` in a container behind HikariCP
  (so they need Docker now). MariaDB has no `MERGE`, no `TIMESTAMP WITH TIME ZONE`, a `BLOB` of 64 KB, and before
  10.3.2 refused a subquery on the table an `UPDATE` writes: the SQL became portable (an upsert as `UPDATE`, then `INSERT`
  where nothing matched, a concurrent insert updated after all in the same transaction; a conditional insert as
  `INSERT ... SELECT ... FROM (SELECT 1 AS x)` without casts; the cut-off through a derived table), and what SQL
  leaves open comes from a dialect by the product name (MariaDB: `LONGBLOB`, `DATETIME(6)` in UTC,
  `UTC_TIMESTAMP(6)`; otherwise the standard's). All 50 JDBC tests pass on it. The standard dialect (H2, HSQLDB) is
  untested since H2 went. Not supported: MySQL (no sequences), PostgreSQL through JDBC (`nextval`, `bytea`, and a
  duplicate key aborts the transaction; it has its own adapter), SQL Server (`datetimeoffset`, `varbinary(max)`),
  Oracle, DB2, Derby (`SELECT` without `FROM`, their sequence syntax): a dialect each, if ever wanted.
- **Saved for later (2026-10-25): an adapter per SQL database.** A "general JDBC" adapter choosing its dialect
  behind the user's back promises too much. The plan: `JdbcStore` as an internal base (the store logic on the
  primitives, the portable SQL, transactions, error classification), parameterised by a dialect; a small public
  adapter per database (`MariaDbAdapter` first, others only on demand), each with its own documentation and test
  container; the standard dialect dropped, or kept only with H2 back as a tested target. (MySQL would need a counter
  table instead of a sequence: outdated since the thin SPI, which uses no sequence in the JDBC store.) Native
  adapters stay where a database has notifications or `RETURNING`. **Decided 2026-11-02 (user):** MariaDB and MySQL
  are dialects of the one JDBC store, each with a public adapter of its own (see "MariaDB and MySQL: dialects of one
  store, an adapter each"). **The focus
  first: databases that support every distribution mode natively** (a change feed of their own, as PostgreSQL and
  MongoDB), not ones polled. Candidates with a feed of their own, to look into on demand: CockroachDB
  (changefeeds, speaks the PostgreSQL protocol), SQL Server (Query Notifications, Change Data Capture), Oracle
  (Continuous Query Notification), Redis (keyspace notifications: fire and forget, no replay), DynamoDB Streams,
  Couchbase (DCP), etcd (watch).

**SPI refactoring (2026-10-24, after step 5), names only, no behaviour changed:**

- **Feed vocabulary:** `Message` → `Change`, `Kind` → `ChangeKind`, `Listener` → `ChangeListener` (`onMessages` →
  `onChanges`), `Subscription` → `ChangeSubscription`, next to `ChangeFeed` and `ChangeScan`.
- **Row fields:** `Row.written`/`RowHead.written` → `stamp`, `RowHead.id` → `hash`; `ReadResult`/`VersionsResult`
  `invalidatedAll` → `cutoff`, `VersionsResult.written` → `stamps`.
- **Types for loose maps:** `invalidateIfUnchanged` takes `Collection<Expected>` (`Expected.ifVersion(hash, v)`,
  `Expected.ifMissing(hash)`) instead of `Map<String, Long>` with a magic `MISSING`; `WriteResult` has three fields,
  `version` being the written version or, on a conflict, the current one (`0` if none).
- **One abstract class for the floor:** `PrimitiveStore` (implements `Store` and `ChangeScan`) replaces the interface
  `RowStore` and the class `GenericStore`: the primitives are its protected abstract methods, the `Store` methods are
  built on them and an adapter may override any with a native one. `JdbcRowStore` → `JdbcStore`.
- **Fewer files (22 → 18):** `StoreFactory` folded into `Adapter` (which only extended it); nested where a type has one
  owner: `ChangeKind` → `Change.Kind`, `RowHead` → `PrimitiveStore.Head`, `Format` → `StoreSettings.Format`. The
  core's own `InternalCore.Change` (what an atomic update found and left), which hid `spi.Change`, is now `Update`.

**Fixed: the polling store's touch re-stamped the control row (found and fixed 2026-11-12).**
`InternalPollingStore#touch` rewrites the keys of a write that may have come too late for every poll, "as they are
now", conditionally on their version: at once for a write slower than the overlap, two operation timeouts later (20 s
by default) for one with an unknown outcome. The control row of `invalidateAll` is among them when that write was
one. Writing it again gives the row a new write time, and every value row written at or before the control row counts
as missing: so the touch hides every value written between the control row's write and the touch, by any instance
(confirmed writes lost from the store). Twenty seconds later the row may even be a newer `invalidateAll`'s, which the
touch re-stamps all the same. Value rows touched get a new write time too, so they live another maximum age (milder).
Only the polled stores (MariaDB, MySQL). The deterministic test at 40 phases lasts about 17 simulated seconds, so the
20 s touch rarely fired within a run; at 120 phases the existing test fails on jdbc in 49 of 50 seeds, every failure
a lost confirmed write, the profiles with a feed green. Options were a reserved row announcing a late `invalidateAll`
(a new row kind) or re-stamping the rows written since; **the second was built (the user's choice, b):**
`InternalPollingStore#touchValues` never rewrites the control row (a value row the control row hides stays hidden);
`#touchControl` reads the control row and the rows changed since its write in one read (their ages comparable),
rewrites the control row conditionally (refused: a newer `invalidateAll` announces itself, nothing re-stamped), waits
the store's clock allowance (as `invalidateAll` does: a clock counting in milliseconds would stamp the next write
alike), rewrites the rows that were younger than it, then reads once more for rows committed meanwhile. A row whose
commit becomes visible later than the overlap stays hidden (the race accepted, as the scan's overlap is accepted
elsewhere). Found on the way: the test's in-memory clock counted milliseconds where the jdbc profile says
microseconds, and the harness clock ran ahead of simulated time on reads in the MILLIS profiles (capped within the
simulated millisecond). `InternalSimulationTest` covers the late touch (a microsecond clock); the deterministic
property test green on every profile at 600 phases over several batches of seeds.

## Expiry and `invalidateAll`

### Maximum age: expiry is distributed, Caffeine is untouched

The library never touches Caffeine's expiry: no cap, no translation into an internal `Expiry`, `policy()`
unchanged. The maximum age is enforced by the store instead:

- **On read:** the query only returns rows younger than the maximum age, so L2 never serves an expired value, even
  before pruning ran.
- **By pruning:** the maintenance worker marks rows whose maximum age is exceeded as `EXPIRED` with a new version (a
  state update on both stores; a MongoDB TTL index can only delete, and deletes carry no meaning), which sends an
  `EXPIRED` message; holders drop the key. Expired rows are deleted later, silently. A hot value without any Caffeine
  expiry is dropped as well.
- **Idempotent across instances:** several instances prune concurrently, so the update is conditional
  (`state = VALUE AND write_time < now() - max_age`).
- **Dropping is always safe,** so an `EXPIRED` message needs no version to be correct; at worst it costs a reload.

Bound: a value is served at most **maximum age + pruning interval + delivery** after its write, independent of the
Caffeine configuration. Users who need it tighter shorten the pruning interval (configurable, see Operations) or
configure Caffeine's expiry shorter as well.

Accepted: a load that read the row just before it expired can still put it into L1. It was valid when read and is
dropped by the next `EXPIRED` message at the latest.

Rejected alternatives: capping each L1 copy at the remaining age through an internal `Expiry` (as JetCache does
with its absolute per-value expire time). That needs the user's expiry settings, translates fixed expiry into
variable expiry and empties `policy().expireAfterWrite()` / `expireAfterAccess()`. JetCache can afford it because
it configures Caffeine itself; this library keeps the user's Caffeine settings as given. Not caring at all was
rejected too:
a hot value would outlive its maximum age by one Caffeine period, or indefinitely without Caffeine expiry.

### Maximum age for the store only

*Built 2026-11-05.*

`withStore(store -> store.withMaximumAge(age).withMaximumAgeForStoreOnly())`: a value expires in the store after the
maximum age, which answers misses within it; later misses call the source, and the local copies follow their Caffeine
expiry alone. For a store too small or too expensive for the working set, or local copies kept far longer than the
store should keep values. Built first as `withDistributionOnly()` with a fixed internal retention of five minutes,
then expressed through the existing setting: on the store's side it is exactly a maximum age, and the decoupling of
the copies is what is new.

- **The switch, not a second age:** the name starts with `withMaximumAge`, so both show up together. It requires a
  maximum age and distribution (without messages every held value counts as trusted, which the atomic methods rely on).
- **Misses read the store:** the load path is unchanged, so a key loaded or written elsewhere within the maximum age is
  not loaded again (and `UPDATE` re-reads it). Skipping the read would save one round trip per miss but needs a second
  load path with its own version and trust rules.
- **The store's expiry is not a change:** `EXPIRED` messages are ignored, and L1 gets no cap.
- **Atomic `asMap()` methods stay atomic across instances:** they always read (no hot path on the held value, whose row
  has usually expired), and a row the store let go of (expired, pruned to size, or an expired row cleaned up) counts as
  the trusted copy held here, at the row's version, so that the write is conditioned on it. Sound because a trusted
  holder has heard every write older than about two heartbeat intervals with their jitter (the heartbeat notices a
  silent feed), and the store lets go of no younger row: the heartbeat interval is at most a fifth of the maximum age
  and at most a minute (three intervals within the tombstones' five minutes), and size pruning spares three intervals
  in this mode (`DistributedCaffeine#pruneGrace`; the store briefly holds those writes beyond its maximum size). The
  first build took missing rows for the held copy without that grace, which size pruning seconds after a write would
  have broken; found while switching. A removal leaves a tombstone and an `invalidateAll` hides rows as tombstones, so
  neither is taken for the held value; a row deleted by an `invalidateAll` not heard of yet is the race accepted for
  updates in flight. An instance without a copy decides on an absent value, as on a miss.
- **Accepted:** nothing bounds how long a copy is served if a change never reaches its instance (a message lost
  without a gap noticed), as with an unbounded maximum age. After a gap noticed, copies are checked by version and
  those whose row has expired are dropped.
- **Tests:** `InternalCoreTest` (on every store: sharing within the age, copies kept after it, atomic methods on
  expired and on pruned rows) and `DistributedCaffeineTest` (validation, prune grace). Not in the property test.

### Store bounds at runtime

*Built 2026-11-11.*

`distributedPolicy().store().maximumAge()` and `maximumSize()` (`Optional<MaximumAge>` / `Optional<MaximumSize>`, nested
in `DistributedPolicy`; named `storeMaximumAge()`/`storeMaximumSize()` until 2026-11-17) get and set the store's bounds
while the cache runs, as Caffeine's `policy().expireAfterWrite()` and `eviction()` do for its own; empty if the cache
was built without the bound, so that bounded or not stays a decision of the build (L1 gets its capping expiry only with
a maximum age). Asked for by a real use: a TTL kept in ZooKeeper and set on every instance at runtime. Caffeine's own
`setExpiresAfter` alone does not do it: it counts from the install and is lowered to the maximum age, so the store's
maximum age is the actual bound of staleness.

- **Per instance, not shared:** the setter changes the instance it is called on; the configuration service sets every
  instance. Storing the bounds in the store (one call for the cluster) would need a control row, change messages and a
  rule for conflicting local configuration: not built without a need (complexity budget).
- **Disagreement is safe, it only costs loads.** Every atomic decision is conditioned on the row's version, and a read
  of a value too old for the reader reports it as `EXPIRED` at that version (`Store#read`), so instances that see the
  same row as present and absent still decide on one referee: the outcome is that of the age changing between the two
  operations. Expiry is a conditional state update, so pruners with different ages coexist; the shortest expires
  values for everyone. With the age for the store only, `InternalCore#read`'s trust rule needs only the reader's own
  age (or the pruner's) to be at least five heartbeat intervals, which the setter checks against the instance's
  heartbeat (the same on every instance by the configuration rule). A lasting split (an instance restarted with a
  static default) churns: values expired by the shortest, reloaded by the others.
- **The copies held follow:** `InternalExpiry` computes a copy's latest expiry from its install, its row's age and the
  maximum age *now* (no longer stored per value), and `InternalExpiry#reapply` reschedules every held copy, the loop
  `setExpiresAfter` already used. `expireAfterWrite` is kept as set and lowered to the age where read, so a longer age
  lifts it again up to what was set; without one set it follows the age. Under the user's `Expiry`, whose durations
  cannot be computed again, a copy keeps what is left of it, lowered to its new latest expiry: a longer age lengthens
  none. With the age for the store only, L1 is not touched.
- **Stays in `withStore`, for now (decided 2026-11-11):** moving the maximum age to the top-level builder would show
  its cache-wide scope (it is the cluster's bound of staleness and caps the copies too), but changes the API; the
  compromise accepted is documentation: `withMaximumAge`, `expireAfterWrite` and the README say that
  without a local expiry the copies expire at the maximum age (`policy().expireAfterWrite()` shows it and follows it),
  and that a duration set through the policy is lowered to it. Counting `expireAfterWrite` from the store's write was
  rejected: a row older than it would install already expired unless the reads filtered by it too, which merges it
  into the maximum age and gives up Caffeine's meaning.
- **The two-level model, and the alternatives rejected (decided 2026-11-12):** the store decides how long a value is
  valid (its maximum age), each instance which copies it keeps and for how long (Caffeine's settings, never beyond the
  validity), and a copy that goes sends the instance to the store, not the source. That is a second level's role: it
  keeps values longer than the first. Stated at the top of the README, in the class Javadoc and in each Caffeine
  setter, with a goal-first table in the README. Rejected:
  - Symmetric levels (store-only as the default, the age bounding the store alone): a store age alone would no longer
    bring a source change to the copies, a TTL would have to be set twice (and changed twice, in order, at runtime),
    the bound would be their sum, a lost change unbounded, and the trust rule's limits (distribution, at least five
    heartbeat intervals) would apply to everyone.
  - Enforcing a store age longer than every local expiry instead of capping: it covers only `expireAfterWrite`;
    `expireAfterAccess` has no end to compare, an `Expiry` no duration known at build, a copy read late still outlives
    its row, and a staggered runtime change would throw. Taken from it: `expireAfterWrite` or `expireAfterAccess`
    longer than the maximum age fails the build (the inverted hierarchy, almost certainly a mistake: such a duration
    never applies); at runtime it is still lowered.
  - Deriving the store age from `expireAfterWrite`: shrinks the second level to the first's lifetime.
  - A distribution-only mode (the store as a bus, Caffeine's semantics unchanged): gives up shared loads, the atomic
    methods' atomicity across instances and warm starts, and needs rows for the feeds anyway; invalidation buses exist
    elsewhere. Per-entry validity, the one gap it closes, is a check on read plus `invalidate`; an `Expiry`'s time
    written into the row is the targeted fix, if ever needed.
- **Not adjustable:** `withMaximumAgeForStoreOnly()` (a mode: the trust rule of the atomic methods), the heartbeat and
  poll intervals (`pruneGrace` and the five-heartbeat rule are computed from them by the instance that prunes, for every
  instance), the operation timeout (fixed into the drivers when the adapter is built) and the executor; the
  maintenance interval could be, but nobody tunes it live.
- **Tested:** unit tests for the policy (local expiry following, lifted, capped; validation) and the core (one
  instance's shorter age expiring the values for every instance, on every store), and a property-test variant: three
  times a phase one instance's age is set to 50 ms, 200 ms, 1 s or an hour while the instances work, maintenance every
  5 ms, all back at an hour before the check; freshness and convergence hold (presence is not asked for: a confirmed
  write may expire), threaded and deterministic on every store profile (304 runs green), each run asserting that
  values expired.

### `invalidateAll` by the store's time, hidden on read

*Decided and built 2026-10-28, build plan step 3; superseded the sweep.*

**Decided 2026-10-28:** `invalidateAll` writes one time and hides every row written at or before it; maintenance
deletes those rows later. This replaces the synchronous sweep of "The thin SPI" below, and the stamp, the
basis and the basis check of today's cut-off. It is what memcached's `flush_all` and Varnish's bans do, applied to the
store.

**Why not the sweep.** It is O(n) for the caller (about 4–9 µs per row, a minute or so for 10 million rows). An O(1)
`invalidateAll` only moves that work into the background, but the caller no longer waits, and nothing is unusual
about that: every O(1) design in the industry cleans up lazily.

**What others do and accept** (researched 2026-10-28):

| System                                   | Invalidate-all                                                                        | A write pays                    | Accepted                                                                |
|------------------------------------------|---------------------------------------------------------------------------------------|---------------------------------|-------------------------------------------------------------------------|
| memcached `flush_all`                    | one server time ("oldest live"); older items expired on access, reclaimed by the LRU  | nothing                         | the server clock; racing sets survive                                   |
| Varnish bans                             | a ban list with times, checked on lookup; the ban lurker cleans in the background     | nothing                         | the server clock                                                        |
| FusionCache `Clear()`                    | a special tag's timestamp; older entries hidden on read; no purge, entries die by TTL | nothing                         | the writers' clocks (skew loses or keeps entries); racing loads survive |
| Redisson local cached map                | deletes the hash, publishes a clear message                                           | nothing                         | O(n) in Redis; local copies stale for the message delay                 |
| Spring `RedisCache.clear()`              | `SCAN` plus `DEL` in batches                                                          | nothing                         | O(n), half cleared while it runs                                        |
| Hazelcast, Infinispan                    | removes every entry, near caches invalidated by events                                | nothing                         | O(n), not atomic across keys                                            |
| Caffeine, Guava                          | key by key                                                                            | nothing                         | O(n); loads in flight explicitly undefined                              |
| namespace versioning (memcached pattern) | bump a version key carried by every key                                               | **an extra read per operation** | old entries die by TTL                                                  |

Common ground: racing loads and writes survive everywhere; the O(1) designs trust a server clock and clean up lazily;
none of them makes a write read anything extra, except namespace versioning. Requirement here too: **a `put` reads
nothing extra.**

**How.**

1. **`invalidateAll()`:** an unconditional `write` of the cache's control row, a reserved key (the core refuses `#`
   hashes). The time the store writes it at is the cut-off. It announces one `INVALIDATED_ALL`, which clears every
   L1 and moves the epoch on. O(1), whatever the size of the cache.
2. **A `put`** writes as now, stamped with the store's time (its write time). It reads nothing extra.
3. **Reads** carry the control row: the core adds its key to every batch it reads (Redis `MGET`, DynamoDB
   `BatchGetItem`, SQL `IN`, MongoDB `$in`), so it costs no round trip; a read reports the cut-off even when no row
   is found. A row **at least as old as the control row** counts as missing. The comparison uses the ages every store
   already reports for the maximum age, all from one clock reading within one read, so the core needs no absolute
   time and the SPI no new field.
4. **Maintenance** deletes the hidden rows in small paced batches, in the background; size pruning ignores them.
   Nothing stays in the store for ever, and nobody waits for it.

It fits the thin SPI: the control row is one more key, written and read through `write` and `read`. A store has
nothing new to implement.

**What goes:** the stamp column and its sequence (PostgreSQL), the basis and `Store#checksBasis`, the cut-off and
stamps in `ReadResult`/`VersionsResult`/`Row`, `Store#invalidateAll` as a method of its own, `Row#visible`, and the
2ε wait. Ages replace stamps for the comparison.

**What we accept, as the industry does.**

- **A racing load:** one that read before the clear and commits after it is stamped after it, so its
  value survives, stale until the maximum age or the next write of the key. Only the writer knows its decision is
  older than the clear; a timestamp says when a row was written, not when its value was decided. Caffeine documents
  loads in flight during `invalidateAll` as undefined; Redis, Hazelcast and FusionCache let them survive.
  Rare (a load must overlap the clear) and bounded. The same for a racing `compute` (any atomic map method deciding on
  a present value): a repair was built and reverted on 2026-11-05, see below.
- **The clock.** Stores with a time that never goes backwards (MongoDB's cluster time, a monotonic counter) have no
  caveat. With a wall clock (PostgreSQL's `now()`, Redis `TIME`), a step back (an NTP correction, a failover to a
  standby whose clock is behind) could make a `put` just after a clear look older than it and hide it.
  `invalidateAll` therefore waits the documented maximum skew (milliseconds) before returning: still O(1), the `put`
  untouched. memcached and Varnish trust the server clock the same way.
- **PostgreSQL's `now()`** is the transaction's start: a write whose statement began before the clear but committed
  after it counts as before it. Linearizable, since the two overlapped; the window is one short statement.
- **Other instances** clear L1 within the delivery delay, the bound every change has.

**The repair: built and reverted 2026-11-05 (too complex for a partial guarantee).** Reproduced first: five of
`InternalInvalidateAllRaceTest`'s disabled tests failed on every profile (`compute` stored `v+`, `merge` `v1`,
`replace` `x`); `computeIfAbsent` and `merge` on an absent key are valid in both orders (only the source's freshness
is open, as for loads). Built: a log per instance of the atomic methods' writes derived from a present value (hash,
version applied, the cut-off the decision knew), repaired by a conditional tombstone when a newer cut-off was
learned, settled by the reads the instance made anyway (every read carries the control row; a feed orders per key
only, so messages cannot settle), with a capacity after which the next operation read instead of taking the hot
path. Its tests found two more windows (a settling read returning the row under repair; a message between a decision
and its write), both closed. **Then the property test found the flaw:** a decision on a held value (the hot path, and
every refresh) knows the cut-off this instance had heard of, not when its value was written against an
`invalidateAll` not heard of yet. A value this instance wrote after such an `invalidateAll` and then decided on was
tombstoned, a confirmed write lost without any race (23 of 50 deterministic seeds failed). Only decisions that read
the store can be judged exactly; repaired alone, they pass, but they close only the window between one read and one
write, while the hot path's window (the delivery delay) stays open. Reverted by the user's choice ("it's getting
complex"), the race accepted and documented in the README. To close it fully, if a user needs it:
- **B: no hot path for the atomic methods**, every one reads first (one more store read each), plus the log above for
  the window between read and write; a refresh would read before it writes too.
- **C: the store checks the control row in the conditional write**, with no window and no log: free in a Redis
  script; `FOR SHARE` on the control row in SQL (a lock hotspot, a MultiXact per operation on PostgreSQL); a
  transaction on MongoDB. An SPI capability; Redis alone would be the cheap start.
- **Others (from memory, 2026-11-05):** memcached (`flush_all` makes a later `cas` fail) and Redis (`FLUSHDB` touches
  watched keys) close it in the store, Hazelcast serializes `clear` and entry processors per partition, Caffeine
  holds the bin's lock from read to write; FusionCache, JetCache and Redisson have no atomic `compute` across
  instances. Caffeine discards a refresh whose entry was invalidated meanwhile; here a refresh racing an
  `invalidateAll` elsewhere survives it.
- **Reaching further back (found 2026-11-06 by the property test with size pruning, accepted the same day):** a load
  whose write is conditioned on "missing" is refused by a newer write elsewhere, but maintenance deletes the rows an
  `invalidateAll` hides at once (`pruneTombstones`, no grace), that write's row too; the load's value then lands,
  older than the write before the `invalidateAll`. Reproduced in `InternalInvalidateAllRaceTest` (disabled as
  accepted); the property test's freshness oracle allows every value the source held before an `invalidateAll`; the
  README says so. Closing it: `pruneTombstones` sparing hidden rows for the prune grace, an SPI change in every
  adapter that narrows the race without ending it.

**Compared.**

|                 | Caller waits | Extra per `put` | Extra per row                           | Racing loads                                                                                                                           |
|-----------------|--------------|-----------------|-----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| today's cut-off | no           | nothing         | stamp and basis                         | exact                                                                                                                                  |
| the sweep       | yes, O(n)    | nothing         | nothing                                 | exact                                                                                                                                  |
| **this design** | **no**       | **nothing**     | **nothing (write time and age reused)** | **survive until the maximum age, as everywhere; the repair later makes them exact on SQL and bounded by the delivery delay elsewhere** |

### Conditional writes racing an `invalidateAll`

*Worked through 2026-10-24.*

**The yardstick: Caffeine.** Its `invalidateAll` is weakly consistent across keys, but per key every operation is
atomic: a `compute` and the removal by `invalidateAll` happen in one order or the other. So for one key the outcome
must equal either "the operation, then `invalidateAll`" (the key is absent) or "`invalidateAll`, then the operation"
(the operation as on an absent key). The race (the operation reads before T, writes after it) gives a third outcome
unless something prevents it.

| Operation                                                 | Race outcome without a check                                             | Valid?                                                                     |
|-----------------------------------------------------------|--------------------------------------------------------------------------|----------------------------------------------------------------------------|
| load, refresh, `get(k, fn)`, `computeIfAbsent`            | a value from the source or function as it was before the `invalidateAll` | no: the source may have changed before it (the reason for `invalidateAll`) |
| `compute`, `computeIfPresent`, `merge` on a present value | `f(old)`: the old value survives in what is written                      | no: after the `invalidateAll`, `f` would have seen no value                |
| `compute`, `merge` on an absent key                       | `f(null)` or the given value                                             | yes as values go, but `f` may read a source like a loader                  |
| `replace(k, v)`, `replace(k, old, new)`                   | written because a value was present                                      | no: after the `invalidateAll` the key is absent and nothing is written     |
| `putIfAbsent` on an absent key                            | the caller's value                                                       | yes: "`invalidateAll`, then `putIfAbsent`"                                 |
| `remove(k)`, `remove(k, v)`, `invalidate`                 | a tombstone                                                              | yes: absent in both orders                                                 |
| `put`, `putAll` (unconditional)                           | linearised at their write                                                | yes                                                                        |

**Rule: every conditional write of a value is checked; tombstones and unconditional writes are not.** Exact would be
"only where the written value depends on a present value or on a function", which exempts `putIfAbsent` on an absent
key; one rule for every value written conditionally keeps a single code path (`writeConditionally`), and costs
`putIfAbsent` the check where the adapter cannot fold it in.

**The check, and the repair:**

- **Folded in** (SQL adapters): the conditional write carries the cut-off it read,
  `... WHERE id = ? AND version = ? AND (SELECT write_time FROM ... WHERE id = <control>) = ?`. If T moved before the
  write, nothing is written: a conflict, handled as every conflict (read again, start over). If T moves after the
  write, the write is at or before T, so hidden: the order "operation, then `invalidateAll`". Exact, no extra round
  trip.
- **After the write** (stores that cannot read another key inside a write: MongoDB, DynamoDB, most key-value
  stores): read the control key again (one primary-key read). Unchanged: done. Moved: **undo and start over**:
  tombstone the own write conditionally on its version (announced, so holders that took it drop it), then run the
  operation again from its read, which now finds the tombstone (absent) and gives "`invalidateAll`, then the
  operation". A load calls its source again; that is the cost of the rare case.
- **No clock comparison:** only whether T changed. A T that moved after the write as well is handled the same way
  (start over), which is also a valid order.
- **Bounded:** at most 3 starts; after that a load returns its value without holding it (as today on a conflict that
  persists), a compute method fails with a retryable `StoreException`.
- **Without a feed:** unchanged; the check never needs a message. **With a feed:** L1 is cleared by `INVALIDATED_ALL`
  anyway, and the result of the repeated operation is held as usual.

**Residual:** the instance dies between its write and the check (only where the check is not folded in): the written
value stays until the maximum age. Accepted, documented; the folded check has no such window.

**Built 2026-10-24** with the check after the write on every store (`InternalCore.writeConditionally`, and for bulk
loads `checked`): one read of no keys per conditional write of a value. Folding it into PostgreSQL's statement (with
the share lock) is left as an optimisation. Tested by `InternalInvalidateAllRaceTest` (one test per row of the table,
plus the commit window of the two-write `invalidateAll`, deterministic) and the property test; mutation-checked: a
single control write, no check, a check without the undo each make tests fail.

**Three corrections the property test forced while building it:**

- **The check compares with the cut-off of the read the decision was based on, never a remembered one.** A value an
  instance wrote itself after another instance's `invalidateAll`, whose message was still on its way, carried the old
  cut-off: a compute on it was undone although it raced nothing, and with it the value it replaced (found by
  `presence`). So the compute methods decide on a read from the store, not on a held value (one read more where L1
  held it trusted), and a load starting over after a conflict reads again instead of deciding on the conflict's row
  (one read more per conflict, which a conflict's row does not carry the cut-off of). This changes "`asMap()` compute
  methods read through L2": through L2 always. A refresh, which starts from a held value as well, reads the cut-off
  (a read of no keys) before it calls the source (found in step 4 by the property test: a refresh of a value this
  instance had put after another's unannounced `invalidateAll` was undone, and the put's value with it).
- **A conditional write with its outcome unknown is checked too.** It may have landed after an `invalidateAll`
  without telling its version: if the cut-off moved, a row written after it that holds exactly the value written is
  taken for it and undone (someone else's equal value costs a miss; a different one is left alone).
- **The in-memory store takes a version when it applies a write,** not when a delayed write took its time: pinned to
  the start, a key recreated after a prune started below a version written meanwhile, a window of microseconds in
  PostgreSQL (the version is computed in the statement that finds the row) that the 1 ms delay widened to a failure
  every few runs. The stamp and the write time stay pinned, which is the window the cut-off is about.

**What the store must add for this: nothing, after one correction.** The check needs a read that sees every
confirmed `invalidateAll` (must 4), a conditional tombstone (must 3), and one clock for write times (must 5). But a
write's time is taken when the statement runs, and the write becomes visible at commit, a moment later: a load
writing between the `invalidateAll`'s time and its commit, and checking before that commit, would find T unchanged
and keep a value from before it. Excluding that in the store would need write times taken at commit, which hardly
any store guarantees. **Closed in the core instead: `invalidateAll` writes the control key twice.** The first write
marks the change (every check after it starts over); the second, after the first is committed, sets the effective
cut-off T. A load write in the first write's window happened before that commit, so it lies below the second write's
time: hidden (as long as the store's clock does not go backwards, the residual under "Clocks"). Between the two
commits a racing value is visible for a moment: instances with a feed drop it on the second `INVALIDATED_ALL`;
without one, a copy taken in that moment stays until the maximum age, the staleness `NONE` accepts anyway. **The
folded SQL check** has the same window under READ COMMITTED (the subselect sees the state at the statement's start);
exact only with the control row locked in share mode (`FOR SHARE` on PostgreSQL, `LOCK IN SHARE MODE` on MySQL), so
that the write waits for a running `invalidateAll` and sees its T. An adapter's matter, not a store condition.

### Rows hidden by the cut-off: deleted silently

*Worked through 2026-10-24.*

**Silently, without announcing them as `EXPIRED`.** An announcement would tell nobody anything new: holders with a
feed have cleared L1 on `INVALIDATED_ALL` already, a holder that missed it is in a gap and revalidates (the store
reports the row missing), and holders without a feed are not reachable anyway. Announcing would bring back the storm
of one event per row the cut-off removed. Each case of a silent delete:

| Who                                                                      | What happens                                                                                                                        | Valid?                    |
|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| a reader                                                                 | the row counted as missing before the delete, and after it                                                                          | unchanged                 |
| a load in flight that read the row before T                              | its conditional write finds no row: a conflict, it reads again (or, if the write came first, its check finds T moved and undoes it) | yes                       |
| a load that read the row after T (as missing, with its physical version) | its conditional write conflicts, it reads again                                                                                     | yes, one extra round trip |
| a holder in a gap                                                        | revalidates, the row is missing, the copy dropped                                                                                   | yes                       |
| a later write of the key                                                 | starts at the store's time, above every earlier version                                                                             | yes                       |

**No grace period:** tombstones keep a grace period so that loads in flight conflict; a hidden row needs none, since
any load that read it either conflicts on the missing row or is caught by its check against T.

**Maintenance, from the musts only:** an indexed query by write time (must 6) finds rows at or before T, deleted in
batches by key and version (must 3), so that a row rewritten meanwhile is kept; rate-limited, in the background, like
expiry today. **Size pruning counts only visible rows** (`write_time > T`), so that rows waiting for this delete do not
push visible ones out. **The control key is never deleted:** without it T would be lost and hidden rows would come
back; it is one row per cache.

**Cost, core side: acceptable.** One indexed range query and one batch delete per maintenance run while hidden rows
remain, proportional to the cache's size once per `invalidateAll` but spread over runs; the sweep it replaces did
the same amount of work at once, in the caller's thread, with one event per row. Nothing on the common paths.

### Refill while serving: `invalidateAllBeforeMark` and `invalidateAllOlderThan`

*Decided 2026-11-14, revised 2026-11-16 and 2026-11-17, built 2026-11-17.*

**The use case:** a job refills the cache with many puts (it does not load through the cache; where its values come
from is its own business) and drops whatever it did not put, while the cache keeps serving. An `invalidateAll()` first
is not an option: every instance would miss until the refill got to its keys.

```java
StoreMark mark = cache.distributedPolicy().markStore();   // the store's time, read from the store
for (Map<K, V> batch : batches) {
    cache.putAll(batch);
}
cache.distributedPolicy().invalidateAllBeforeMark(mark);   // only after a complete fill
```

Two methods, one mechanism (the backdated cut-off below), differing in where the cut-off comes from:

| Method                              | Cut-off                                       | For                                                                                                |
|-------------------------------------|-----------------------------------------------|----------------------------------------------------------------------------------------------------|
| `invalidateAllBeforeMark(StoreMark)` | just before a time read from the store earlier | a refill: exact against latency, the store's clock on both sides                                  |
| `invalidateAllOlderThan(Duration)`   | a mark taken by the call, minus `olderThan`    | a one-off sweep ("drop everything not written within 24 hours")                                   |

A `retainOnly(keys)` (`invalidateAllExcept`) was considered and not adopted: with the values put anyway, the cut-off
covers it; it would only pay for "valid keys known, values unchanged", by renewing the retained rows' ages (rewritten
conditionally on their version, churning every holder's copy, or a store-side touch that must not revive hidden or
too old rows).

**How.** It is the cut-off of "`invalidateAll` by the store's time", backdated:

- **In the store:** one write of the control row, its write time `max(current cut-off, cut-off asked for)` by the
  store's clock. Every row the refill did not rewrite is older, so it is hidden at once, on every read of every
  instance (reads carry the control row), and `pruneTombstones` deletes it. No scan, no tombstone and no message per
  row, whatever the size of the cache.
- **Every write renews a row's age** (checked 2026-11-14): `put` and `putAll` advance every key unconditionally at a
  fresh version (`InternalCore#putValues`), equal values included, and every adapter sets the write time from the
  store's clock. Loads by traffic during the refill write too, so they are younger than the cut-off and kept, which
  is right: they came from the source. Reads renew nothing.
- **On every instance, the calling one included: no clear, the epoch moves and the pass after a gap runs (decided
  2026-11-17).** The goal is a cache that stays hot, so L1 keeps its copies. The epoch move makes every held copy
  untrusted, which is what keeps it correct: an untrusted copy is never served while the store answers without being
  checked against it (`InternalCore#confirmOrDrop`, one row read, the new control row with it), so a hidden row drops
  it and a row the refill wrote confirms it. The background pass of a gap (`InternalCore#revalidateAll`) then makes
  the cache hot again ahead of the accesses: the held keys in batches of 1,000 per read, paced, every copy the store
  still has confirmed, the others dropped; an access before the pass reaches its key pays the single check. Its work
  is bounded by what an instance holds, not by the store. It runs in the background on the calling instance too, so
  that the call does not grow with the cache: correctness does not wait for it, only hotness. While the store does
  not answer, copies are served unconfirmed rather than missed. Loads from the source only for the keys hidden.
  Rejected: clearing as `invalidateAll()` does (correct, but cold: every key one read again, and an outage right after
  the call turns every access into a miss); the pass inside the call (a round trip per 1,000 held keys before it
  returns); keeping the calling instance's own writes after the mark trusted (a flag per entry; see "Options for
  later").
- **On the calling instance** the rest is `invalidateAll()`'s: the atomic map operations wait (`clearing`), the epoch
  moves. **The cut-off lies one clock allowance before the mark** (built 2026-11-17, replacing a wait measured from
  the mark): a fill write stamped before the mark by the store's clock stepping back within its allowance is never
  hidden (found while building: the stepping profile hid it), and a write after the call, sent after the mark was
  read, is always later than the cut-off by more than the allowance, so the call never waits and moves the epoch once.
  A row written within the allowance before the mark is kept: as current as the refill.
- **Polling stores (MariaDB, MySQL; found 2026-11-17):** a poll reports the rows written within its window by the
  store's clock, and a backdated control row is stamped before every window, so no poll would ever report it.
  Re-stamping it (`InternalPollingStore#touchControl`) would make the cut-off "now" and rewrite every row the refill
  wrote; a cut-off of its own beside the write time would change every comparison in every adapter and schema; a read
  of the control row per poll is a round trip per poll forever. So every scan reports the control row, whatever its
  write time (a rule of `ChangeScan`, one `OR` on the primary key in the poll's query), and the poller keeps the
  control row's last version to deliver each new one once. It also covers a full `invalidateAll()` committed later
  than the overlap, so the control-row part of `touchControl` goes, with its accepted race. Instances of a release
  without marks on these stores miss a backdated call until their copies expire or a gap revalidates them.
- **`NONE`:** the store hides the rows, and the calling instance moves its epoch and runs the pass (without a
  subscription `InternalCore#feedUp` is true, so trust is the epoch alone). Other instances keep their copies until
  they expire locally, as for `invalidateAll()` and for the refill's writes themselves.
- **`withMaximumAgeForStoreOnly()`:** a hidden row reads as a tombstone there too (see that section), so the copies go.

**Why a mark from the store for a refill, not a duration measured by the caller (2026-11-16).** A duration is turned
into a cut-off by the store when the control write *executes* (`now − olderThan`). Whatever lies between measuring it
and that execution (network latency, queueing in the store, a GC pause after measuring) moves the cut-off later than
the refill's start, always in the same direction: into the refill's first writes, which are then hidden. Without a
loader a hidden key is gone until its next write. Widening by the clock allowance only bounds this by an assumption
(10 ms by default), never guarantees it. A mark read from the store before the first write does not have this error:
every write of the fill is sent after `markStore()` has returned, so the store stamps it at or after the mark, and a
cut-off just before the mark never hides it, whatever the latency. The two remaining sources of error compare as
follows; the mark wins because latency is common and a clock step is rare:

|                           | Duration (`invalidateAllOlderThan`)                       | Mark (`invalidateAllBeforeMark`)          |
|---------------------------|-----------------------------------------------------------|-------------------------------------------|
| Latency, pause            | cut-off moved into the fill, bounded only by an assumption | no effect                                 |
| Store clock steps forward | fill rows hidden (misses)                                 | stale rows kept (the safe direction)      |
| Store clock steps back    | stale rows kept (the safe direction)                      | fill rows hidden (misses)                 |
| Cost                      | none                                                      | one store round trip at `markStore()`     |

A duration also stays a duration: an `Instant` would compare the caller's wall clock with the store's, the skew this
design never takes on ("Clocks: ages from the store, versions from the instances"). The mark is the store's own time, so
the same holds for it.

**The mark.** `StoreMark` is opaque: the store's time as the adapter reads it, with the same clock expression it uses
for write times (so mark and write times are comparable), plus the local `System.nanoTime()` of the read (for skipping
the allowance wait). Not an `Instant` in the API, so that nobody compares it with a wall clock. A mark belongs to the
cache instance it was taken from (decided 2026-11-17: the coordinator takes it and makes the call; relaxable to every
instance of the cache later); one of another instance is rejected. A mark older than the current cut-off changes
nothing (the maximum keeps the later one). `markStore()` fails while the store does not answer, like the fill's writes
would.

**`invalidateAllOlderThan(olderThan)` is a mark minus the duration** (decided 2026-11-17): the call takes a mark and
backdates it by `olderThan`. One form of the backdated write in the SPI, and the latency error of a duration
(below) is gone too, for one round trip a sweep does not notice.

**The SPI change: two `Store` methods.**

- **Reading the store's time** for the mark: `now()` or `clock_timestamp()` as the adapter's write times
  (PostgreSQL, JDBC), `$$NOW` (MongoDB, cluster time), `TIME` (Redis). On a Redis cluster, from the node holding the
  control row, the clock the cut-off is compared against there; how rows on other nodes compare follows what
  `invalidateAll()` does today, to check when built. Checked 2026-11-17: a cache's keys share one hash tag
  (`RedisKeys`), so they live on one node, one clock.
- **Writing the control row backdated:** as `write` does (the version proposed by the core, advanced as for any row,
  announced as one change), but with the write time `max(current write time, cut-off)`, atomically, so that the
  cut-off only ever moves forward: a backdated call landing just after a concurrent full `invalidateAll()` must not
  reveal the rows that one hid. The cut-off is the mark (the adapter writes one unit of its write-time resolution
  before it, since rows written at or before the control row are hidden: a fill row stamped at the mark's very tick
  stays visible, and a stale row at that tick survives, the safe direction). Per adapter: `GREATEST` in the statement
  (PostgreSQL; JDBC through `PrimitiveStore`'s primitives), the maximum in the Redis script, a pipeline update with
  `$max` on MongoDB.
- A field on `Write` instead was rejected: it would mean something for one hash only.

**Built 2026-11-17.** `Store#time()` and `Store#writeControl(Write, Instant before)` (PrimitiveStore:
`advanceControl`; PostgreSQL an upsert with `GREATEST`, JDBC `GREATEST` in its update, MongoDB `$max` in the pipeline,
Redis condition `B` of the write script, its time from a `TIME` script on the cache's node, MongoDB's from `$$NOW` by a
write to the start document on the primary), `ChangeScan` point 2 (JDBC: the control row by a second `SELECT` of a
`UNION ALL`, so both indexes are used), `touchControl` removed. In the core: `markStore()`, `invalidateAllBeforeMark`,
`invalidateAllOlderThan`, the pass of a gap reused (`revalidateAfter`, `revalidateInBackground`). An announced
`invalidateAll` moves the epoch at once and starts the pass one allowance later (the second epoch move, so that the pass
is not stopped by it); its first read tells the kind, and a **full one drops only the copies from before its message**
(`dropOlder`), not all of L1: a clear that late would also drop values written after it (found by
`InternalSimulationTest`). A poller delivers the control row's version found by its first poll even if it is older
than the subscription (nothing tells; once more is safe). Tests: the contracts (`time`, `writeControl` hiding and
keeping, never moving back, creating; the scan reporting the control row; the feed announcing it), every adapter on its
latest image; `InternalRefillTest` (every profile, both distribution modes, `NONE`, the maximum age for the store
only, a foreign mark, a later full `invalidateAll` not undone, the clock stepping back within its allowance; each
mechanism mutation-checked); a refill operation in `InternalPropertyTest` (presence oracle: no value of the fill
hidden); two Lincheck scenarios (a read through racing a backdated call, a backdated call racing a full one).
Failover on the real stores (latest images): a refill riding out the primary failing between the fill and the call,
the call retried (MongoDB replica set, Redis Sentinel and cluster per library, PostgreSQL, MySQL replication); a
backdated control row the failover loses (PostgreSQL's asynchronous standby, MySQL replication): the caches converge on
the store, which hides nothing again, and a later call takes effect on every cache, also where the new primary may give
the control row a version a cache saw already.

**Decided 2026-11-17: the kind in the control row's value, read by the pass.** A control-row message carries only a
version, so an instance cannot tell a full `invalidateAll()` (clear L1) from a backdated one (keep L1, revalidate).
The kind goes into the control row's value (today `controlBytes`, never read; empty, or JSON `null`, means full, so
that the control rows of instances of a release without marks read as full ones, and those instances clear on every
control message, which is correct for both). On the message, an instance moves the epoch and starts the pass; its first
read carries the control row anyway (`readRows`), so the kind costs no read of its own: full clears L1 and ends the
pass, backdated continues it. A remote full `invalidateAll()` thus clears one read later than before, its copies
untrusted meanwhile, never served unchecked. Any mix-up errs on the safe side: an instance reading a backdated value
written over a full one keeps revalidating, which is correct for both. Rejected: always revalidating (after a full one,
a read of every held row only to drop it).

**When to call it.**

- **A refill job:** `markStore()` before its first write, `invalidateAllBeforeMark(mark)` after its last write has
  returned. Rows written between the mark and the first write are other writers' or loads', as current as the
  refill's, and kept.
- **A refill spread over several workers or machines:** one coordinator takes the mark before it starts the first
  worker, and makes the call after the last one has finished. The workers' clocks do not matter, nor does the
  coordinator's.
- **A one-off sweep without a refill:** `invalidateAllOlderThan(duration)`, a one-off maximum age that also works on a
  cache built without one, and hides at once instead of expiring by maintenance. Its latency error (milliseconds)
  does not matter against a duration of hours.
- **Only after a complete fill.** A `put` that failed or ended with an unknown outcome is retried until it succeeds
  (it is idempotent); if the fill cannot complete, the call is skipped. Called after a partial fill, the cut-off hides
  every key the fill did not reach: without a loader, missing until their next write. Relevant at startup, where a pod
  may start without the store and its puts fail.
- A wrapper `refill(Supplier<Map<K, V>>)` (mark, call the supplier, `putAll`, the call only if every put succeeded)
  is possible later, so that the order cannot go wrong; the two methods stay the primitives.

**The motivating case (2026-11-14, clarified 2026-11-16):** a database is the source of truth, about 1,000 entries,
and every pod used to read all of them into a local cache at every restart. Changes go directly to the database and
are announced over a pub/sub the application cannot change, each event reaching only one pod (at most once, no
replay). All pods can be down at once, so events can be lost unnoticed. No loader (a miss is a miss), no maximum age,
no source versions, and the entries should always be hot. The coordination across pods is the cache's job. The
recipe (the README keeps one row in "Settings by goal", decided 2026-11-17; the Javadoc has the details):

1. `UPDATE` mode: a write on one pod replaces the copies the others hold.
2. On start: `markStore()`, read all entries from the database, `putAll`, then `invalidateAllBeforeMark(mark)` once
   the fill has completed. Keys deleted while nobody listened are hidden on every pod; the filling pod's L1 is hot.
3. On an event (one pod): `asMap().compute(key, (k, current) -> database.find(k))`, `null` removing the key. The
   value is read from the database inside the atomic operation (atomic across instances), so events handled out of
   order or twice never leave an older value: whichever applies last reads the database last.

Costs per restart at this size: 1,000 writes and, on each running pod, at most 1,000 copies dropped or read again. A
key created by an event on another pod costs one store read on its first access here (`UPDATE` replaces held copies,
it adds none); zero store reads would take a replicated mode (see "Open work", `UPSERT`). Rolling deploys: an
old-version pod restarting after new ones fills with its values and hides keys only the new version writes, until the
next new-version start or event.

**Windows left open (worked through 2026-11-14 and 2026-11-16, by reasoning; the property-test variant below is to
confirm them):**

1. **A refill's put over a newer value** (inherent to any refill, not to the drop): the job computes K's value,
   another writer puts or invalidates K (in the motivating case, an event handled on another pod between the fill's
   database read and its put), then the job's `put` lands. `put` is unconditional, so the job's older value wins,
   until K's next write or its expiry. Absent if the job is the only writer; otherwise reading each batch right before
   putting it keeps the window short (milliseconds at 1,000 entries); closing it needs source versions and a
   conditional write (`compute` keeping the newer version), not part of this design. Documented with the method.
2. **A racing load** (only with a loader): a load that read the source before a key was deleted there and writes
   after the cut-off counts as touched and is kept. It has to overlap the call itself (a load writing during the
   refill is written after the cut-off and kept, rightly). The race accepted for `invalidateAll()` (2026-10-28), with
   its bound: the key's next write or the maximum age.
3. **The delivery delay:** other instances serve their copies until the control-row message arrives: milliseconds
   with a native feed, up to a poll interval on MariaDB and MySQL, until local expiry in `NONE`. The bound every
   change has; a gap moves the epoch, so a lost message is covered by the revalidation.
4. **The store's clock stepping** by J during the refill. With a mark: back by more than its allowance (within it,
   nothing: the cut-off lies one allowance before the mark), up to J of the refill's earliest writes are stamped
   before the cut-off and hidden (misses); forward, nothing is hidden wrongly. With a duration (beyond its
   allowance): forward, up to J of the refill's earliest writes are hidden; back, untouched rows written up to J
   before the start are kept. Not on MongoDB (cluster time); possible on PostgreSQL and Redis, for example on a
   failover to a server whose clock differs. Documented, as for `invalidateAll()`.
5. ~~**`invalidateAllOlderThan` only: the time between measuring and executing**~~: closed 2026-11-17, the call
   takes a mark and backdates it by the duration.

**Windows closed, provided it is built as described:**

6. **A concurrent full `invalidateAll()`** is never undone by a backdated call landing after it, only because each
   store applies `max(current cut-off, cut-off asked for)` atomically; never the core reading the cut-off and writing
   it back. The refill's writes before the full one are lost, which is what `invalidateAll()` means.
7. **A read in flight on another instance** that read a now hidden row before the control write and installs it after
   the epoch moved installs it at the older epoch, untrusted, checked on its next read: the mechanism that covers it
   for `invalidateAll()`.
8. **A mix-up of the two kinds** (the kind in the control row) ends in a revalidation, always correct; an instance
   clears only where a full one was written.
9. **Overlapping refills:** each call's cut-off is its own mark, and it only moves forward, so the later call hides
   only rows written before its mark, none it wrote itself.
10. **A write with an unknown outcome:** if it never landed, its key is hidden, a miss (without a loader, until its
    next write: hence the retry before the call, see "Only after a complete fill"); if it lands later, it is written
    after the cut-off and kept, with the refill's value.
11. **Latency for a refill:** closed by the mark (see "Why a mark from the store").

**Alternatives considered (2026-11-14, 2026-11-16):**

- **Enumerating the store (a `store().getAll()` without keys), then invalidating what the refill did not write:** a
  scan with keys and values is a new SPI method in every adapter (per node on a Redis cluster), the result can be
  millions of rows on the heap, and traffic's writes during the refill need tracking. Rejected.
- **Tombstones for every row older than the age** (`Store#expire` with the target state `TOMBSTONE`, returning the
  hashes so that the caller drops its copies at once): exact, and every instance drops only what goes, but a tombstone
  and a message per dropped row, and four adapters to change, plus the polling window
  (`InternalPollingStore#expiredWithin` selects `EXPIRED` only) and PostgreSQL's `expireCursor` (one per state). The
  fallback if the backdated cut-off proves unworkable. `Store#expire` itself, unchanged, is not a removal: under
  `withMaximumAgeForStoreOnly()` its messages are ignored and an expired row leaves the trusted copy in place
  (`InternalCore#ignoredExpiry`, `InternalCore#read`), sound only because no row expires younger than the maximum age.
- **A duration for the refill too** (the 2026-11-14 version, `System.nanoTime()` around the fill, later with a local
  mark and widening by the clock allowance): no extra round trip and SPI method, but the latency error above, in the
  harmful direction. Replaced by the store mark.
- **A second cache, switched over (blue-green):** no library change, but the store holds two copies, and the switch
  needs coordination across instances, which then read different generations while it spreads.
- **The application remembering the previous refill's key set** and calling `invalidateAll(previous − current)`:
  keys loaded by traffic are in neither set, so a key deleted at the source stays until it expires.
- **Waiting for the maximum age:** no code, but only with a maximum age and only if waiting that long is acceptable.

**To test when built:** per store, rows written before the mark (or older than the age) hidden and later ones kept,
including a row stamped at the mark's own tick; the cut-off never moving back under a concurrent full
`invalidateAll()` or an older mark; copies on other instances revalidated (hidden dropped, rewritten kept) in
`INVALIDATE`, `UPDATE` and with the maximum age for the store only; `NONE` on the calling instance; a mark of another
cache rejected; a property-test variant with a refill phase under traffic and injected store latency: after the call,
no value served that was written before the mark, none of the fill's values hidden, except the racing load accepted
above.

## L1 and loads

### Wrappers in L1: `InternalKey` and `InternalValue`

L1 is a Caffeine `AsyncCache` of `InternalKey<K>` to `InternalValue<V>` (wrappers), so
metadata travels with keys and values without changing Caffeine's types.

- **`InternalKey<K>` caches the key's hash**, computed at most once per wrapper or taken over from a row. The hash
  is needed for every L2 call and every message match.
- **`InternalValue<V>` carries the version** (replacing today's `operation` and `activationId`), which every
  comparison under the ordering rules uses, and the epoch at which it was last confirmed (see "States and
  recovery").

### Shared loads: Caffeine's `AsyncCache` as L1 for every variant

L1 is a Caffeine `AsyncCache<InternalKey, InternalValue>` internally, for the synchronous variants too; the core uses
it directly (future, then `join()` for the synchronous API) rather than through `synchronous()`.

- **One load per key and JVM, outside any lock:** on a miss Caffeine puts an incomplete future into the map
  atomically and the load runs outside the lock; every other caller for the key gets the same future, so one L2
  read and one loader call per key and JVM. This replaces today's own single-flight map with Caffeine's.
- **Messages find loads in flight through the same map:** an incomplete future is a load in flight, and the
  minimum-version registry of ordering rule 4 attaches to it. When a newer message arrives during the load, the load
  reads L2 again before completing the future, since the callers waiting on it still need a value, and the newer one.
- **One code path:** "async-first, synchronous waits" reaches down to L1.
- **Recursive loads:** a loader requesting its own key in the same thread would wait on its own future forever. As
  today, a set per thread of the hashes being loaded makes such a request throw "Recursive load of ..." at once.
  Recursion across threads (a loader on the executor waiting for the same key) cannot be detected; Caffeine has the
  same limitation and documents it, and so does this library.
- **Cycles of synchronous loads across threads** (the load of a waits for b, whose load waits for a) are resolved:
  the core knows which thread runs each synchronous load and which load each waiting
  thread waits for, and a thread whose wait would close a cycle asks the source itself instead, without holding or
  writing the answer (the owner's load does both). Without it, every thread of the cycle hung for good.
- **`getAll`:** L1 hits first; keys already in flight wait for their futures (`AsyncCache.getAll` does that); the
  rest in one batched L2 read (rows younger than the maximum age); what is still missing goes to `loadAll` if the
  loader overrides it, else to `load` per key, as today; the results are written to L2 in one batch, conditionally
  on the versions read (ordering rule 6), the newer row winning on a conflict.
- **Extra keys `loadAll` returns unasked** (Caffeine caches them) have no version to compare against, since they
  were never read: they are written only if no row exists for them (`ifMissing`), and discarded, not held,
  otherwise, so an older source value never overwrites a newer put. This replaces today's write counter for extras.
  **Corrected by the property test (2026-10-21):** the first version wrote them wherever L2 held no value (missing,
  tombstone or expired), but a tombstone written after the loader read the source means the key was changed or
  deleted meanwhile, so the extra's value may be outdated; it stayed in L2 until the maximum age. A tombstone or an
  expired row now prevents the write (a missing row is only possible once tombstones are pruned, after a retention
  no loader outlasts). The written extras are held by the core under the write registry, not handed to Caffeine:
  Caffeine inserts a `getAll` result's extras with a plain put, outside the version rule and unseen by messages
  announced during the load, which left a stale extra held for good. `ExpectedEntry.ifNoValue` had no other user and
  is gone from the SPI.
- **`refresh`** is single-flight per key in Caffeine already.

**The internal `AsyncCacheLoader<InternalKey, InternalValue>`** wraps the user's loader, so Caffeine runs `get`,
`getAll`, `refresh`, `refreshAll` and `refreshAfterWrite` natively through it and the distributed logic sits in one
place:

- **`asyncLoad`:** L2 first (maximum-age filter); only on a miss the user's loader; then the conditional write
  (ordering rule 6); returns an `InternalValue` with version and epoch. An L2 value at least R old is refreshed at
  once (refresh rule 2).
- **No `asyncLoadAll`:** Caffeine's `refreshAll` refreshes key by key, and every `getAll` passes the core's own
  function, so Caffeine never loads through the wrapper in bulk. Bulk loads (one batched L2 read, `loadAll` for the
  rest, one batched conditional write, extra keys only where no row exists) run in the core's `getAll`. A test guards
  that Caffeine keeps refreshing key by key.
- **`asyncReload`:** no L2 read (a refresh goes to the source); the user's `reload` / `asyncReload` with the old
  value; the write is conditional on the held version, and a conflict discards the result.
- **Recursion detection:** the per-thread set above.
- **Both kinds of user loader:** Caffeine's `CacheLoader` extends `AsyncCacheLoader` (its async methods run the
  synchronous ones on the executor), so the wrapper calls the user's async methods and covers both alike.
- **Bulk loading only if the user's loader overrides it:** Caffeine checks by reflection whether `loadAll` /
  `asyncLoadAll` is overridden and otherwise loads key by key. The core's `getAll` checks the user's loader the same
  way and, if not overridden, loads key by key itself.
- **Executors:** the user's loader runs on the executor Caffeine passes (the user's); store I/O on the store
  executor.
- **Exceptions and `null`:** a loader exception fails the future, so Caffeine caches nothing and nothing is written;
  `null` means absent, as in Caffeine (remembered as an absence if negative caching is on).
- **Mapping functions of non-loading caches** (`get(key, function)`, `getAll(keys, function)`) are wrapped into the
  same pipeline, in Caffeine's `BiFunction<K, Executor, CompletableFuture<V>>` form.

Measured (`HitPathAsyncBenchmarkTest`, JMH 1.37, JDK 25, precomputed two-long keys so only the cache differs, filled
cache of 10,000 keys, average time per call, 1 / 4 threads; no allocation in any variant):

| Variant                            | `getIfPresent` | `get(key, function)` on a hit |
|------------------------------------|----------------|-------------------------------|
| plain `Cache`                      | 16.9 / 13.2 ns | 15.6 / 13.0 ns                |
| `AsyncCache`, future then `join()` | 18.8 / 14.3 ns | 20.2 / 14.5 ns                |
| `AsyncCache.synchronous()`         | 23.6 / 17.8 ns | 22.1 / 15.1 ns                |

The `AsyncCache` costs 1–5 ns per hit used directly (up to 7 ns through `synchronous()`), against 30–65 ns for
hashing the key; negligible.

### The uncached load

*Decided 2026-10-29.*

**Decided 2026-10-29:** a load met by a removal of its key on every one of three attempts returns its last loaded
value without caching it, as before; now documented (`DistributedLoadingCache#get`, `DistributedCache#get(K,
Function)`), counted (`DistributedStats#uncachedLoadCount`), and the one exception the Lincheck tests leave out.

**What it is.** A load reads the store, calls the source on a miss, and writes the loaded value only if the row is
still at the version read (or still missing). The condition keeps a slow load from writing a value an invalidation
during it made outdated (the stale set, which keeps an old value cached until it expires). A conflict reads again: a
value written meanwhile is returned (a put causes no retry); no value (a removal) means loading again. After three
attempts the value goes to the caller uncached. Viewed from outside it was never a state of the key, so a scenario
with three removals inside one load is not linearizable (Lincheck finds it, as it puts every operation of three
threads on one key).

**What others do** (verified 2026-10-29), each after the first removal, none retrying:

| System                                    | An invalidation during the load                                                                         | Source                                                   |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| Caffeine 3.3.1 `AsyncLoadingCache`        | the waiter gets the value, the cache keeps nothing                                                      | measured against the jar                                 |
| Caffeine 3.3.1 synchronous `LoadingCache` | the removal waits for the load (the key's lock), then removes: linearizable, within one JVM             | measured                                                 |
| Hazelcast Near Cache                      | the fetched value is returned, published only if the reservation still holds                            | `NearCachedMapProxyImpl`, `AbstractNearCacheRecordStore` |
| Infinispan Hot Rod near cache             | a placeholder before the fetch, `replace(key, placeholder, value)` after; the value returned either way | `InvalidatedNearRemoteCache`                             |
| Redis client-side caching                 | a placeholder; an invalidation deletes it, the reply is not stored                                      | redis.io, "Avoiding race conditions"                     |
| Facebook memcache leases                  | a delete invalidates the lease, the later set is ignored                                                | MIT 6.824 FAQ on the paper                               |

**Why three attempts, not a time limit.** Every attempt calls the source again (after a removal the value may be
outdated). A count bounds the calls to the source; a time limit bounds only the caller's wait and lets a storm of
removals hammer the source until it expires. Any finite bound is exceeded by a storm long enough, so the count stays.

**How rare.** Removals of one key at a rate λ, an attempt open for W (the source's time and the store's round trips):
a conflict per attempt p = 1 − e^(−λW), three in a row about p³ (removals assumed independent; bursts make it more
likely). With a source taking 10 ms: one removal a minute, about one load in 120 million; one a second, one in
580,000; ten a second, one in 700. With 100 ms: one a second, one in 1,100. Noticeable only for a key removed about as
often as it loads, where a cached value would hardly outlive its load anyway. The counter shows such keys.

**Tested:** the exception on its own (`InternalCoreTest`, with the counter); the Lincheck tests raise the attempts
above the operations of a scenario (`withLoadAttempts`, package-private), so that they check every other case.

### Refresh

Caffeine's `refreshAfterWrite` stays as configured; the library adds four rules around it.

1. **A refresh always goes to the source.** It calls the user's `CacheLoader.reload`, never takes the value from L2
   (which would usually return the same value), writes the result to L2 under ordering rule 6 and sends `WRITTEN`.
   The same applies to explicit `refresh(key)` and `refreshAll`, also for a key this instance does not hold (it
   reads the store only for the version to write on), and a source answering `null` removes the value everywhere,
   as Caffeine's refresh removes it. The result is committed to L1 by the core as a write in flight, not left to
   Caffeine: Caffeine installs a refresh only over the value it started from, and the write's own echo would drop that
   value first (found by the parity suite).
2. **A value read from L2 that is at least R old is served and refreshed at once** (`cache.refresh(key)` in the
   background), where R comes from `policy().refreshAfterWrite()`. Without this, Caffeine would count R from the
   insertion into L1, so a value loaded at 50 minutes old with R = 10 minutes would only be refreshed at 60. The
   age comes with every read from the store.
3. **A refresh resets the timers on every instance** through the usual messages: with population, holders read the
   new row and replace their copy (a write for Caffeine); without, they drop the key and the next access loads the
   fresh value.
4. **Concurrent refreshes on several instances** each call the source, but only the first conditional write wins;
   the others discard their result. No distributed lock.

Accepted: a value younger than R when loaded is refreshed at an age below 2R rather than at R (Caffeine has no
per-entry refresh interval), documented as "refreshed by an age of at most 2R". Parallel source calls across
instances happen only when timers line up.

While the store does not answer, a refresh degrades as a load does: the source's value is held unconfirmed (checked on
the first read with the store back, which takes the store's value unless the write landed after all), and `null`
removes the key on this instance only. Failing the refresh instead would keep the old value with its timer, so that
every read past R called the source again.

### Negative caching: a local set of recent misses (opt-in)

For keys that do not exist but are requested repeatedly (unknown IDs, optional per-user data, slow "not found"
answers, deliberate cache penetration with random keys). Off by default, so the default behaves exactly like
Caffeine, which never caches `null`; switched on with `withNegativeCaching(Duration)` (named `withAbsentCaching` until 2026-11-01).

- **A separate, purely local set per cache:** a small Caffeine cache of hashes (two `long`s) with its own short
  `expireAfterWrite` and a size limit. Not in L1, not in L2, not distributed.
- **Filled when a load finds nothing:** neither L2 nor the loader (or mapping function) has a value, the loader
  returning `null`, or a bulk loader not returning the key; also while the store does not answer, from the loader
  alone. An exception records nothing.
- **Checked before every L2 read:** in the internal loader, in `getAll`, in `getIfPresent` and in `containsKey`; a
  hit answers `null` (or `false`) at once, without an L2 read and without the loader. Only while the feed delivers,
  as a held value is trusted only then: otherwise the store is read first (a value found wins and ends the absence,
  none confirms it without the loader), and while the store does not answer the absence is served unconfirmed. A
  refresh asks the source regardless (refresh rule 1) and ends the absence.
- **Only loads fill it; `getIfPresent`, `getAllPresent` and `containsKey` only read it.** An entry means "the source
  had nothing recently, and nobody has written the key since" (any write anywhere would have removed it by its
  message), so L2 holds no value either and the read paths may answer from it, saving the round trip. Without a
  loader, though, a read path only learns that the key is not cached, not that the source lacks it; recording that
  would make a later `get(k, loader)` return `null` without loading a value the source has.
- **Consistency by existing mechanisms:** a value appearing through the cache anywhere (a `put`, or another
  instance's load writing L2) produces a `WRITTEN` message, which removes the hash from the set (messages for keys
  not held, ignored otherwise, are checked against the set by hash); a local `put` removes it at once; if a message
  for the key arrives while the load runs (the minimum-version registry of rule 4), the absence is not recorded;
  after a gap the set is cleared (small, cheap to rebuild, messages may have been missed). A message reaches a load
  only through its future in L1, so an absence is also not recorded if that future was dropped meanwhile
  (`invalidate`, `invalidateAll`) or the epoch moved (a gap during the load; the gap moves the epoch before it
  clears the set), and never by loads Caffeine runs itself (property test, 2026-10-21). A value appearing only in
  the source is found after the set's expiry, which is negative caching's semantics, so the expiry should be short.
- **Each instance learns its own absences:** a key that does not exist costs one load per instance and expiry
  interval instead of one per cluster; the price of distributing nothing.
- **Stats:** a hit in the set counts as `negative hits` in the distribution stats; Caffeine's `stats()` sees a load
  returning `null`, as Caffeine counts it.

Rejected alternatives: an absent marker in L1 plus an `ABSENT` row state in L2 (JetCache's `cacheNullValue`): a new
row state, a second maximum age in the read filter, markers hidden in the views, sizes counting them, listeners and
weigher special-cased, all as a feature Caffeine deliberately does not offer. Caching `Optional<V>` as the value (the
pattern Caffeine recommends) works with the library unchanged and stays documented, but absences then share the
store's maximum age.

### Write-behind for load results (parked)

*Designed 2026-11-18, parked the same day: the owner answered at the source's answer.*

**Parked 2026-11-18 by the user.** A small win: only the owner of a miss everywhere gains, misses are a minority of
calls at a healthy hit rate, and with sources slower than about 10 ms the write is under 10 % of a miss (MongoDB
gains most, its majority write 0.8 to 1.7 ms). Against it: a guarantee users can reason about gets a "shortly after"
(the store holds the loaded value only a moment after `get` returns, so a read on another instance or of the table
right after misses), Caffeine's `totalLoadTime` no longer matches the caller's wait, and a second load path (a thread
hop for synchronous caches, a bound, close waiting, logging) with its tests. FusionCache offers the same as an
opt-in, off by default (`AllowBackgroundDistributedCacheOperations`); an opt-in here would double the load paths
(complexity budget). Revisit only if users report miss latency with fast sources, or the Linux baseline shows misses
to be where the library loses time; `MissBenchmark` measures it again. Caveats found in the review, to fold in if it
is built: serialize the value in the owner's thread before answering (else a serializer failure turns into a success
for the owner and an error for later readers); no further attempt once the owner is answered and L1 no longer holds
the load's future (else an invalidation by the owner right after its `get` makes the background call the source again
and write the key back); a failed write behind nobody waits for logged at most once a minute, as `asyncWriteLost`; the
bound measured with a burst of misses (pooled connections: the owner's next call and the write behind each hold one);
with a hung store, outages surface to callers only once the bound is full; tests reading another instance or the
store right after a `get` (about 60 places read within three lines of one) must wait for the write.

**The gain, measured** (`MissBenchmark`; numbers under "Open work", "write-behind"): a miss everywhere is a
store read, the loader, and a conditional write of the loaded value, which the caller waits for today. The write
costs about as much as the read or more (0.25 to 0.4 ms on PostgreSQL, MariaDB and Redis, 0.7 to 0.8 ms on MongoDB at
a fast loader; 0.9 to 1.7 ms after a 5 ms loader, the connection idle meanwhile): 21 to 36 % of a miss at a loader of
1 ms, 13 to 23 % at 5 ms.

**The rule: only the load's owner returns early; L1, the reservation, messages and the attempts stay as they are.**
The owner is the caller whose miss created the future in L1 (`getOrLoad`, `getOrLoadAsync`). It gets the loaded value
as soon as the source answers; the store steps after it (the conditional write, a conflict's read, further attempts)
run on as today, and the future in L1 completes when they are done, as today. So:
- **Nothing held changes.** Readers arriving meanwhile, on this instance, find a load in flight and wait for its
  outcome, as now; a message meanwhile raises the reservation and is decided at the release, as now; a conflict reads
  again, and an invalidation in between calls the source again for those waiting, as now. No new L1 state, no change
  to `apply`, the trust rules or the epoch. The load is reserved until its future completed, as now.
- **The owner's answer is linearizable.** Its call spans the loader's call, so the loaded value is the source's at an
  instant within the owner's call; a write or invalidation racing the load is concurrent with the call, so either
  order is valid (Caffeine answers a load racing an invalidation the same way). What the conditional write protects,
  the store and L1 from a value outdated by an invalidation during the load, it still protects.
- **The other instances see no difference.** The row lands when it does today: the write starts at the source's
  answer either way, only the owner no longer waits for it. A miss elsewhere meanwhile loads, as it does today until
  the write lands.
- **Not early:** the waiters on the same future (they arrived during the loader's call; see "Not taken" for
  them); a refresh (Caffeine runs it in the background, nobody waits); a load finding nothing (no write follows, except
  a refresh's tombstone); a load while the store does not answer (`sourceOnly`, no write); a hit in the store.

Rejected first (sketched 2026-11-18 before reading the paths): completing L1's future early with the value held
untrusted. Then later readers no longer wait for the write, so `apply` would have to drop a completed copy despite the
reservation, the load's own echo could drop it before the write's answer, and a refusal could no longer start over
for those waiting (a waiter arriving after an invalidation elsewhere would get the value from before it, where today
the attempt calls the source again). A new L1 state and its rules, for no gain to the owner.

**Where it goes** (`InternalCore`):
- `load` and `attempt` take an `answered` future, completed with the loaded value in `attempt` when the source answers
  a value (the first answer only; not for `fromSource`). The owner waits on `answered` or the load's result,
  whichever comes first: an absence, a store hit or `sourceOnly` completes the result without `answered`.
- Synchronous caches run the load's steps inline (`INLINE`), so that the write would block the owner: after the
  source's answer, `attempt` continues on the store executor (the measured hop costs about nothing on platform threads,
  `StoreDeadlineBenchmark`). `getOrLoad` then waits on the earlier of the two; `loadOwners` and the cycle check end with
  the owner's return (the rest no longer waits for anything this thread does).
- Asynchronous caches run the steps on the store executor already: `getOrLoadAsync` returns the earlier of the two to
  the creating caller only, every other caller L1's future.
- Bulk loads (`getAll` with `loadAll`): the same split in `loadAll` and `writeAll`, the owner answered with the loaded
  map; a second step, once the single-key path is in.
- **A bound on writes behind:** today the owner's thread is the back-pressure; a thread missing in a loop could pile up
  writes on an unbounded executor (Redis multiplexes, no pool limits it). At most a fixed number of writes behind per
  cache (64, or the store executor's own bound); beyond it the owner waits for its write, as today. Counted.
- **Close:** writes behind still running when the cache closes fail against the closed store (a lost write of a load
  result, nothing wrong: the next miss loads); `close()` waits for them at most the operation timeout, so that a
  short-lived application leaves its loads in the store.
- **Stats:** Caffeine's load time (`totalLoadTime`) keeps measuring L1's future, so it still includes the write;
  documented. The store counters count as now (the write is still a `CONDITIONAL_WRITE`).

**No setting.** Nothing held or written changes and the owner's answer is one of the answers it may get today, so it
is the behaviour, not an option (the complexity budget: a toggle would double the paths to test).

**Tests:**
- `InternalLoadTest` (or the distribution suite): the owner returns before the write (a store whose write blocks until
  released: `get` returns, a reader on the same instance waits, then gets the stored value); a conflict after the
  owner returned (a write elsewhere between read and write): the owner got the loaded value, the instance holds the
  store's; an invalidation elsewhere during the write: a waiter gets the source's answer of the next attempt; the
  bound (writes beyond it block the owner); close waits.
- Lincheck (`InternalInterleavingTest`): the existing load scenarios with the owner's answer as an operation of its
  own; the invariant unchanged (what a read of the holder serves once delivered is nothing or the store's value), plus
  the owner's answer one the source held during its call.
- Property test, both modes: unchanged oracles; the owner's answer checked against the source's history (any value
  the source held during the call).
- Mutations: L1's future completed early (with the owner's answer): caught by the waiting reader and the conflict
  scenario; the reservation released at the owner's return: caught by the message scenarios; `answered` completed on a
  later attempt for a waiter: caught by the invalidation-during-write test.
- `MissBenchmark` again: the owner's p50 should drop by about the write's time.

**Not taken (measured first if ever):** the waiters answered early too. They arrived during the loader's call, so the
loaded value is linearizable for them as well, but only while no message has raised the reservation (the delivery
bound holds then, not after), and answering them without completing L1's future means a second future per load in
flight. Only with a measured stampede of one key on one instance.

## The API

### Reading and writing methods

**Principle:** the distributed cache contains what L2 holds. Methods about a single key read through L2; methods
about the whole local map (views, sizes, iteration) show this instance's L1.

**Reading through L2:**

| Method                                                         | Behaviour                                                                                                                                                                                                                                                                                                   |
|----------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `getIfPresent`, `getAllPresent`, `asMap().get`, `getOrDefault` | L1, then L2 (a hit is held in L1), never the loader                                                                                                                                                                                                                                                         |
| `asMap().containsKey`                                          | L1 if held and trusted; otherwise an existence check (`readVersions`: a `VALUE` row younger than the maximum age, version only, no value transferred); nothing is held; an unconfirmed copy is confirmed or dropped by the same query; a store that does not answer counts as a miss, as for `getIfPresent` |

Today `getIfPresent` stays local because L1 holds everything; with L1 a subset it would be almost useless without
L2. Caffeine's "without loading" still holds: only the loader is skipped. JetCache's `get` without a loader goes
local, then remote, and fills the local level the same way (`MultiLevelCache.do_GET`); JetCache has no
`containsKey`, and Redisson's `RLocalCachedMap` and Hazelcast's Near Cache (from memory, not verified here) check
locally first and otherwise ask the shared level for existence only, without filling the local level.

**Conditional `asMap()` methods, atomic across instances** (like `merge`: read the current value through L1 and L2,
decide, write conditionally on the version read; on a conflict read again and decide again):

| Method                                                    | Condition                       | Effect                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|-----------------------------------------------------------|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `putIfAbsent(k, v)`                                       | L2 holds no value               | written (`putIfUnchanged` with "missing" or on the version of a tombstone); otherwise the existing value is returned (not held: a raw read held without a load in flight would escape ordering rule 4)                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `replace(k, v)`                                           | L2 holds a value                | written conditionally on its version                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `replace(k, old, new)`                                    | the current value `equals(old)` | written conditionally on its version                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `remove(k, v)`                                            | the current value `equals(v)`   | tombstone conditionally on its version (`invalidateIfUnchanged`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `computeIfAbsent`, `compute`, `computeIfPresent`, `merge` | see `asMap()` compute methods   |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `asMap().put(k, v)`, `asMap().remove(k)`                  | none                            | written / tombstone conditionally on the version read, so the returned previous value is exact across instances; `put` always writes, also an equal value, so the row's age renews; `remove` always writes a tombstone, also where there is no value (conditionally on the tombstone's version, or on no row existing), because like Caffeine's `remove` it must defeat a load in flight: without it, a record deleted at the source and removed from the cache while a load was in flight was cached again from the load's older source read (found by the property test). `invalidateIfUnchanged` therefore matches a row of any state by its version, or `MISSING` |

A remapping that returns the current value instance unchanged writes nothing; if that decision was made on a held
value, it is first checked against the store (a read: the same version and value), and on a mismatch the decision is
made again on the store's row. Without that check a decision that writes nothing never meets the conflict that corrects
an outdated held value, so `replace(k, old, new)` answered `false` and `putIfAbsent` returned a value invalidated
elsewhere while the messages were on their way (found by an intermittent test failure). The cost is that of a read and a
conditional write, the same as reading the previous value and writing unconditionally; a trusted held value saves the
read. Built: deciding on an outdated held value (its message not yet arrived) made a non-atomic `put` return the
outdated previous value, which the conditional write corrects through its conflict.

**Unconditional writes,** as decided: `put`, `putAll`, `asMap().putAll`, `invalidate`, `invalidateAll(keys)`, and
`clear()` as a global `invalidateAll()`.

**Views and sizes are local** (documented deviation): `keySet`, `values`, `entrySet`, iteration, `size`,
`estimatedSize` and `isEmpty` show what this instance holds; iterating L2 would mean millions of rows and is not
what a local view is for. Removals through the views (`iterator.remove`, `removeIf`, `removeAll`, `retainAll`,
`keySet().remove`) invalidate globally; `entrySet().remove(entry)` is conditional on the value, like
`remove(k, v)`; `entry.setValue(v)` writes, like `put`.

**`AsyncCache`:** its `asMap()` (`ConcurrentMap<K, CompletableFuture<V>>`) has the same semantics, with writes and
conditional checks once the futures complete. As built: a future in flight in L1 is returned as it is; otherwise the
map answers as the synchronous view does, waiting for the store where the answer is synchronous (`get` returns
`null` for absence, as a map must; `containsKey`, `remove`), and the conditional methods wait for the futures they are
given, since the condition is decided in the store. `put` (returning the previous value seen when called) and
`computeIfAbsent` do not wait. Views, sizes and iteration show L1 including loads in flight, as in Caffeine. `put(k, future)` writes L2 when the future completes; a failed future
(or `null`) is removed and writes a tombstone, because in Caffeine the put has replaced the previous value at once,
so it must be gone everywhere (changed 2026-10-22 after the parity suite; it used to write nothing, leaving the
previous value readable through L2). Two `put`s whose futures complete in reverse order behave like
two concurrent `put`s: the store's order decides and L1 follows it through the version comparison; Caffeine's local
"last call wins" does not hold across instances (documented).

**`policy()`:**

| Method                                                                                                 | Behaviour                                                                                            |
|--------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `VarExpiration.put` / `putIfAbsent` / `compute` with a duration                                        | written like `put` / `putIfAbsent` / `compute`; the duration applies to this instance's L1 copy only |
| `eviction().setMaximum`, `setExpiresAfter`, `setRefreshesAfter`, `VarExpiration.setExpiresAfter(k, d)` | local, as in Caffeine (L1 configuration)                                                             |
| `getIfPresentQuietly`, `getEntryIfPresentQuietly`                                                      | L1 only: "quietly" means without side effects, so no L2 read                                         |
| `refreshes()`                                                                                          | L1's refreshes in flight, unwrapped                                                                  |
| `refreshAfterWrite().setRefreshesAfter(d)`                                                             | local; refresh rule 2 follows the new duration                                                       |
| `coldest`, `hottest`, `oldest`, `youngest`, `getEntryIfPresentQuietly`                                 | L1 snapshots, unwrapped and immutable                                                                |

`VarExpiration.setExpiresAfter(k, d)` leaves entries resident but invisible (memory note
`caffeine-setexpiresafter-stuck-entries`); it is Caffeine's behaviour, passed through unchanged and never used
internally.

**SPI additions:** `readVersions(hashes, maxAge)` (shared with revalidation) and `invalidateIfUnchanged` (any
state by version, or `MISSING`).

### Feature parity with Caffeine

**Goal:** the user-facing methods of Caffeine's `Cache`, `LoadingCache`, `AsyncCache` and `AsyncLoadingCache` (and
their `asMap()`, `synchronous()` and `policy()`) are all supported and behave as in Caffeine, plus the distributed
semantics of this document. Deviations exist only where the model forces them, and each is documented.

- **The caches implement Caffeine's interfaces**, as today (`DistributedCache extends Cache`,
  `DistributedLoadingCache extends LoadingCache`), now also `DistributedAsyncCache extends AsyncCache` and
  `DistributedAsyncLoadingCache extends AsyncLoadingCache`, so code written against Caffeine's types works unchanged.
- **Every Caffeine builder setting the user passes keeps its meaning for L1** (size, weight, every expiry, refresh,
  listeners, scheduler, executor, ticker, stats); see "The builder: Caffeine's methods, no reflection".
- **`stats()` is Caffeine's,** counted from the user's view (an L2 hit is a successful load, as for any loader);
  the per-level detail is in the distribution stats.
- **A parity test suite** (`CaffeineParityTest`, built 2026-10-22) runs differentially: the same random sequence of
  about 45 calls (`Cache`, `LoadingCache`, `AsyncCache`, `asMap()` and its views, `policy()`, null arguments,
  failing and `null` loaders and functions) goes to a plain Caffeine cache and to distributed caches with the same
  settings, and every result (value or exception type) is compared after every call; for a single instance also the
  map, `stats()` and the removal events. Variants: synchronous and asynchronous loading caches (with and without a
  bulk loader), and two instances answering for one Caffeine cache in both distribution modes (global methods only).
  12 seeds × 150 calls per variant by default (`-Dparity.seeds=300` for more; 300 passed). Not compared: size
  eviction (Caffeine's admission is randomized) and expiry (L1 on Caffeine's ticker, L2 on the store's clock).
- **Found and fixed by it:**
  - `asMap().get` counted a hit or miss (Caffeine counts nothing for `asMap()` reads);
  - `compute`, `computeIfPresent` and `merge` counted no load (Caffeine counts each remapping as a load, failed on
    `null` or an exception); the core now records into Caffeine's stats counter, which it captures when L1 is built
    (wrapped, `InternalCaffeineStats`);
  - an `AsyncCache.put` of a completed future counted a load (the core holds an incomplete future until the write
    has its version); the async `getAll` fast path counted no hits;
  - a refresh of a key held only elsewhere returned the store's value instead of the source's (refresh rule 1), and a
    `null` from the source left the value in place;
  - a refresh's own echo dropped the refreshed key (see refresh rule 1);
  - a failed `put(k, future)` left the previous value readable (see `AsyncCache` above);
  - a `put` of the identical instance reported `REPLACED` (Caffeine does not, also not for two completed futures of
    the same instance): the replaced wrapper is marked silent;
  - **removal events of message-driven removals were lost** whenever L1 is Caffeine's unbounded cache (no
    `maximumSize`): it reports the key the caller passed, which for a message is a lookup key holding only the hash,
    so the listener wrapper threw and Caffeine logged and dropped the event. Each L1 value now carries its user key.

Known deviations, each forced by the model:

- **Functions may run more than once** in `compute`, `computeIfPresent` and `merge` after a conflict (see `asMap()`
  compute methods).
- **Recursion across threads** cannot be detected (same as Caffeine).
- **Writes are refused while the store is down** (see States and recovery); Caffeine never refuses a write.
- **After `close()`** every operation throws; Caffeine has no `close()`.
- **`weakKeys()`, `weakValues()` and `softValues()` are rejected** (see the builder section).

How each method behaves distributed: see "Reading and writing methods".

### The builder: Caffeine's methods, no reflection

**Decision:** the library mirrors Caffeine's builder methods one to one in a nested configurer,
`DistributedCaffeine.newBuilder(adapter).withCaffeine(caffeine -> caffeine.maximumSize(...).expireAfterWrite(...))`
(the configurer pattern, which keeps the top-level builder small and keeps Caffeine's method names unchanged), instead
of taking the user's Caffeine builder and rewriting its private fields by reflection. Built:
`CaffeineConfigurer` passes every setting at once to a real Caffeine builder of `InternalKey` to `InternalValue`, so
Caffeine's own validation applies unchanged (a setting made twice, `maximumSize` with `maximumWeight`, ...), and wraps
callbacks at the call.

- **Why:** L1 is an `AsyncCache<InternalKey, InternalValue>`, while the user's callbacks are typed for `K` and `V`;
  passed through unwrapped they would fail with a `ClassCastException`, and Caffeine's builder has no public getters
  to read and wrap them in a supported way. Reflection breaks with any change to Caffeine's private fields (today's
  "Incompatible Caffeine version"), fails on the module path unless the package is opened (Caffeine 3 is a named
  module), and needs the `<K1 extends K, V1 extends V>` workaround in `build()`. Building the user's builder once and
  reading `policy()` yields sizes and durations but no callbacks.
- **Generic types:** `newBuilder(adapter)` fixes `K` and `V` as today, so callbacks are typed correctly and the
  `K1`/`V1` workaround disappears.
- **Parity enforced by a test:** it checks by reflection (in the test only) that the configurer has a counterpart
  with the same name and parameter types for every public configuration method of Caffeine's builder (21 in
  Caffeine 3.3.1); a method added by Caffeine fails the build until it is followed.
- **Caffeine's spec strings, parsed (decided 2026-11-18, built 2026-11-20):**
  `caffeine.withSpec("maximumSize=10000,expireAfterWrite=10m")`, or `withSpec(CaffeineSpec)`, applies a spec as
  `Caffeine.from(String)` does (the configurer's own methods, beside the parity, which leaves Caffeine's static `from`
  out). A spec carries no callbacks, so it can be read without reflection, unlike a builder; Spring Boot configures
  Caffeine this way (`spring.cache.caffeine.spec`), which the Spring manager applies to every builder (see
  "Integrations"). Checked by `CaffeineSpec.parse` first, so that a malformed spec fails with Caffeine's own message,
  then read by the configurer, by the grammar Caffeine accepts (comma-separated options, empty ones ignored, `key=value`
  trimmed; numbers with `_` and a sign; durations ISO-8601 or an integer with `d`, `h`, `m` or `s`, either case):
  `initialCapacity`, `maximumSize`, `maximumWeight`, `expireAfterAccess`, `expireAfterWrite`, `refreshAfterWrite`,
  `recordStats`, each applied through the configurer's own method, so that the core sees each setting and a setting made
  twice fails as in Caffeine; `weakKeys`, `weakValues` and `softValues` rejected as their methods are; a key Caffeine
  accepts that this release does not know refused as unsupported. `CaffeineConfigurerTest` compares the result with
  `Caffeine.from(spec)` for specs over every key and form. Not chosen: building a throwaway cache from
  `Caffeine.from(spec)` and reading its `policy()` (no parser, but `initialCapacity` lost and weak or soft references
  unseen), reading `CaffeineSpec`'s package-private fields, and `toParsableString()` as a canonical form (it returns the
  spec as given).
- **Both optional since 2026-11-05 (the user's choice: a required age can be set to 100 years anyway), as Caffeine's
  bounds are:** without a maximum age the store keeps values until written again (internally the cap
  `Duration.ofNanos(Long.MAX_VALUE)`, about 292 years, the largest every path computes with: `Duration#toNanos` in
  the core, PostgreSQL and the in-memory store; MongoDB milliseconds, Redis microseconds as Lua doubles and
  PostgreSQL intervals all hold it; a larger value is lowered to it); without a maximum size no size pruning runs.
  Maintenance skips a bound not set. Tested on every store's latest image with neither bound and with
  `ChronoUnit.FOREVER`. Was:
- **The store's maximum age and maximum size are both required**
  (`withStore(store -> store.withMaximumAge(...).withMaximumSize(...))`), the maintenance interval optional: without
  a maximum age the store would serve arbitrarily old values, without a maximum size it would grow without bound,
  and implicit defaults would hide either bound.

| Setting                                                                                                          | Handling                                                                           |
|------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `maximumSize`, `maximumWeight`, `initialCapacity`, `expireAfterWrite` / `expireAfterAccess`, `refreshAfterWrite` | passed through to L1                                                               |
| `executor`, `scheduler`, `ticker`                                                                                | passed through                                                                     |
| `weigher`                                                                                                        | wrapped: unwraps `InternalKey` / `InternalValue` before calling the user's weigher |
| `expireAfter(Expiry)`                                                                                            | wrapped (as today's `InternalExpiry`)                                              |
| `removalListener`, `evictionListener`                                                                            | wrapped, with the causes below                                                     |
| `recordStats(...)`                                                                                               | passed through; the distribution stats come on top                                 |
| `weakKeys`, `weakValues`, `softValues`                                                                           | rejected with a clear error                                                        |
| `build(...)` / `buildAsync(...)`                                                                                 | always `buildAsync` internally, with the internal `AsyncCacheLoader`               |

**Why weak and soft references are rejected** (invariant 1 alone would allow them): `weakKeys()` compares keys by
identity (`==`), which cannot hold across instances where keys are identified by hash; `weakValues()` /
`softValues()` would reference the `InternalValue` wrapper, which only L1 holds, so with weak values the collector
would take it at the next GC however long the user keeps the value. Supporting them would need a wrapper holding
the value weakly itself, and Caffeine's `COLLECTED` cause would be lost.

**Removal causes for the user's listeners:**

| Event                                         | Cause                                                 |
|-----------------------------------------------|-------------------------------------------------------|
| own `invalidate`, or a received `INVALIDATED` | `EXPLICIT` (invalidated, here or elsewhere)           |
| received `EXPIRED` (maximum age)              | `EXPIRED`                                             |
| population replaces a held value              | `REPLACED`                                            |
| revalidation drops an outdated copy           | `EXPLICIT`                                            |
| a load or refresh result discarded by version | `REPLACED` (as Caffeine does for discarded refreshes) |
| L1's own eviction and expiry                  | as Caffeine reports them (`SIZE`, `EXPIRED`)          |

### `asMap()` compute methods read through L2

Answered from L1 only, `asMap().computeIfAbsent(k, f)` would behave differently from `get(k, f)`, which Caffeine
treats as the same operation, and `compute`, `merge`, `computeIfPresent` would compute against a missing L1 value
while L2 holds one, then overwrite it (a `merge` counter would lose updates).

- **`computeIfAbsent`** behaves exactly like `get(k, f)`: L1 → L2 → function, the result written conditionally
  (ordering rule 6).
- **`compute`, `computeIfPresent`, `merge`** read the current value from L2 (since 2026-10-24 always, not from a trusted
  L1 copy: the check against `invalidateAll` needs the read's cut-off), write conditionally on the version read, and on
  a conflict read again and retry. This makes them **atomic across instances** (optimistic concurrency): two instances
  merging into the same counter do not lose an update.
- **Store I/O runs outside Caffeine's lock** ("loads outside the lock"); atomicity comes from the conditional write
  in L2.
- **Documented deviation from Caffeine:** Caffeine applies the function at most once per call. After a conflict it
  can run again (as with `ConcurrentMap`'s default implementations), so the function must be free of side
  effects. Throwing on a conflict instead was rejected as awkward to use.

### Inspection API

`distributedPolicy().store().get(key)` and `getAll(keys)` read L2 directly, without filling L1: for debugging, for
batch jobs that should not pollute L1, and to see how old a value is. The distributed policy follows Caffeine's
`policy()` (aligned 2026-11-17, see "The distributed API aligned with Caffeine's" below): a
direct query of the instance (`synchronizationState()`) next to views per concern (`store()`, with the bounds, `mark()`,
`invalidateAllBeforeMark`, `invalidateAllOlderThan`), and `distributedStats()` on the cache next to `stats()`.

- They return `StoredValue<V>`: the value and its age.
- They return what a miss would get: only `VALUE` rows younger than the maximum age.
- No state, version or tombstones: those are internals and would become part of the contract.
- Costs no new SPI method: `Store.read` plus deserialization.

**The distributed API aligned with Caffeine's** (decided and built 2026-11-17, after the refill was committed):
`cache.distributedStats()` next to Caffeine's `cache.stats()` (moved from `distributedPolicy().stats()`);
`distributedPolicy()` as Caffeine's `policy()`, a direct query of the instance next to views per concern:
`synchronizationState()` (was `getSynchronizationState()`) and `store()`, a view `DistributedPolicy.Store` with
`get`/`getAll` (were `getFromStore`/`getAllFromStore`), `maximumAge()`/`maximumSize()` (`Optional` views, were
`storeMaximumAge()`/`storeMaximumSize()`), `mark()`, `invalidateAllBeforeMark(mark)`, `invalidateAllOlderThan(d)`.
The synchronization state is no stat: always available (not only with `recordStats()`), a condition, not a counter.
`spi.Store` keeps its name (adapter authors never meet the view). No deprecated delegates: an old-to-new table in
the release notes. Considered: `store()` on the cache instead of the policy (the state would be misfiled), views only
(`synchronization().state()`: aligned with itself, not with Caffeine).
`distributedStats()` is on `DistributedCache` only: an asynchronous cache reaches it through `synchronous()`, as
Caffeine's `AsyncCache` reaches `stats()`. The nested types lost their prefix (`Store`, `MaximumAge`, `MaximumSize`,
`Mark`, as Caffeine's `Policy.Eviction`). Old to new: the release notes.

### Async variants

`AsyncCache` and `AsyncLoadingCache` are supported from the start, because they shape decisions that would
be expensive to change later.

- **The core is built async-first:** every load, store read and conditional write is internally "submit and
  complete" on the configurable executor. The synchronous caches use the same paths and wait for the result, so
  ordering, versions and the store guard exist once, not twice.
- **The SPI stays blocking.** JDBC is blocking anyway, and a blocking SPI keeps adapters simple; the core runs SPI
  calls on the executor (with virtual threads on 21+ if the user passes such an executor). A reactive SPI is not
  planned.
- **L1 holds futures** (as Caffeine's `AsyncCache` does; for every variant, see "Shared loads"). An incomplete future
  counts as a load in flight: messages for its key set the minimum version the load must reach (ordering rule 4); a
  load result below it is discarded. Messages for a key whose future has completed apply as for any held value.
- **Failed futures are not cached** (Caffeine's behaviour) and write nothing to L2.
- **Loaders and mapping functions in their async forms** (`AsyncCacheLoader`, `BiFunction<K, Executor,
  CompletableFuture<V>>`) feed the same read-through and conditional-write path.
- **`invalidateAll`'s epoch increment** also covers incomplete futures: a future started before it completes with
  an older epoch and is checked against the store on its next read.
- **The future-valued `asMap()` of an async cache** follows the same semantics as the synchronous one (below);
  see the `AsyncCache` paragraph under "Reading and writing methods" for how it maps onto futures.

Built (2026-10-21): the core's paths are the synchronous ones, and the asynchronous caches run them on the store
executor; one code path either way, as intended, with the direction reversed ("synchronous paths, run
asynchronously" rather than "async-first, synchronous waits"):

- **Hits stay non-blocking:** a trusted completed value or a load in flight is answered with L1's future at once;
  only a miss (load on the store executor) or an untrusted value (checked there) involves the store executor.
- **The user's code runs on Caffeine's executor,** as in Caffeine: `AsyncCacheLoader` methods get it, a
  `CacheLoader`'s async methods run the synchronous ones there, and `get(key, Function)` / `getAll(keys, Function)`
  run the function there; the store executor waits for the result. Recursion across threads is not detected (as in
  Caffeine).
- **`AsyncCache.getIfPresent` never returns `null`** (documented deviation): a miss in L1 is read through L2, so
  absence is known only when the future completes, with `null`. Caffeine's contract already lets a returned future
  complete with `null` (a load in flight that finds nothing), and Spring's `Cache.retrieve` treats such a future as
  a miss.
- **`getAll`** is answered at once if every key is held, completed and trusted; otherwise the synchronous bulk path
  runs on the store executor.
- **`put(key, future)`** holds the future at once; it is in flight until it completes, so ordering rule 4 covers it
  without the write registry.

**Cost of the direction, measured** (`AsyncLoadThreadsExperimentTest`, JDK 25, a burst of N concurrent misses on
distinct keys, the loader completing after 100 ms without holding a thread, every store call blocking for 1 ms; two
runs each, both shown where they differ):

| N      | as built, platform pool: all done / peak threads | as built, virtual threads | non-blocking load, platform pool |
|--------|--------------------------------------------------|---------------------------|----------------------------------|
| 100    | 107 ms / 102–103                                 | 108–109 ms / 1            | 111 ms / 48–72                   |
| 1,000  | 167–172 ms / 1,002–1,003                         | 117–124 ms / 1            | 121–126 ms / 110–157             |
| 5,000  | 777–781 ms / 1,651–1,652                         | 158–194 ms / 1–2          | 156–157 ms / 226–257             |
| 10,000 | 1,369–1,462 ms / 1,750–1,865                     | 179–187 ms / 1            | 196–207 ms / 273–327             |

- **With platform threads the cost is real:** one store-executor thread waits per async miss in flight, so a burst
  of 1,000 holds about 1,000 threads, and from 5,000 on, creating them dominates (5–7 times slower than either
  alternative).
- **Virtual threads remove it completely** (as fast as the non-blocking load, no platform threads), on Java 21 or
  later.
- **A non-blocking load** (store read on the pool, the loader's future, the write on the pool) also removes it, on
  Java 17 with platform threads; what remains are the threads for the blocking store calls themselves.
- Hits and the synchronous caches are not affected either way.
- **Decided (2026-10-21): the non-blocking load is built (Java 17); virtual threads as a default stay under
  "Open work".**

**Built: one load implementation, chained steps.** `load` and `loadAll` are chains of store steps around the user's
future: the store read, then the source, then the conditional write (and its retries). The steps run on a step
executor: `INLINE` (`Runnable::run`, the current thread) for the synchronous caches, the store executor for the
asynchronous ones. The synchronous loaders are wrapped as already completed futures, so a synchronous cache runs
the whole chain in the caller's thread as before (recursion detection included); an asynchronous cache waits for
the user's future without a thread, the same for `getAll` (bulk and per key), the mapping-function variants, the
loads Caffeine runs itself and `asyncReload`. The synchronous view of an asynchronous cache (`synchronous()`) still
waits in the caller's thread. The ordering-critical logic (conditional writes, conflicts, retries, extras, absences)
exists once; only the retry skeleton is written as a chain.

Measured again with it (same experiment, two runs):

| N      | before: all done / peak threads | non-blocking load, as built | modelled non-blocking load |
|--------|---------------------------------|-----------------------------|----------------------------|
| 1,000  | 167–172 ms / 1,002–1,003        | 117–131 ms / 146–238        | 129–130 ms / 178–236       |
| 5,000  | 777–781 ms / 1,651–1,652        | 157–160 ms / 365–451        | 155–158 ms / 215–232       |
| 10,000 | 1,369–1,462 ms / 1,750–1,865    | 229–242 ms / 703–834        | 195–206 ms / 267–328       |

The remaining threads serve the blocking store calls (the SPI stays blocking); the core's own message processing
runs on the same executor. Tests assert that pending loader futures hold no store-executor thread (single, bulk,
reload); a mutant waiting for the future instead fails them.

**A pending `AsyncCache.put` kept against messages** (built 2026-11-17: the held future is a `PendingPut`, which a
message leaves in L1; `InternalAsyncTest.a_message_while_a_put_is_pending_leaves_the_put_held`, mutation- checked)
(found 2026-11-16, `InternalAsyncTest` flaking under load on GitHub). `putAsync` holds its future in L1 at once but
reserves the key only when the store work starts, on the store executor; a message arriving in between finds a future in
flight without a reservation and drops it, like a load's (see reservations, build plan step 4). The written value is
then not held here, though nothing stale is served (the next read loads it). Seen with the instance's own echo of an
earlier write, delivered late. Unlike a load's, a put's future needs no drop: its write applies above whatever version
is stored, so a message arriving before its store work starts announces an older write (one arriving after meets the
reservation, as now). Options: reserve in `putAsync` before the future enters L1 (a reservation for a future that may
never complete must then end with it), or mark put futures for `apply` to leave alone. Costs a missed cache entry per
race, so not urgent; the test waits for its own echoes instead.

### Serialization: part of the cache configuration

The serializer describes the user's types, not the store, so it is configured per cache (one for keys, one for
values), not on the adapter. Caches sharing a table or collection each bring their own.

Shipped serializers: `ForySerializer`, `JavaObjectSerializer`,
`JacksonSerializer`, and the interfaces `ByteArraySerializer`, `StringSerializer`, `JsonSerializer` for custom
ones.

**Defaults: a cache runs with no serializer configured.**

- **Keys and values default to `ForySerializer` without required class registration**, stored as `BINARY`. It
  handles every type, the built-in ones included (compactly and fast), as today.
- **No type-dependent default:** the format is fixed per cache, and `K` and `V` are erased at runtime, so a built-in
  `TEXT` default would need either declared types or a type marker per value (which destroys the readability that
  is `TEXT`'s point). Declared types (`newBuilder(adapter, String.class, Value.class)`) were rejected: they lengthen
  the most common call, and generic value types (`List<Order>`) cannot be expressed as a `Class` cleanly.
- **Readable or native storage is a one-line choice** for keys and values, with simple serializers shipped for
  `String`, `Long`, `Integer` and `UUID` (`TEXT`) and `byte[]` (`BINARY`, unchanged), and Jackson (`JSON`) for other
  types, e.g. `.withKeySerializer(StringSerializer.ofString())`, `.withValueSerializer(new
  JacksonSerializer<>(Value.class))`.

**Hardening is optional and prominently documented** (this closes the JetCache proposal of a deserialization
filter; out of the box wins over secure by default, with the threat stated plainly):

- `ForySerializer` with class registration (only registered classes are deserialized);
  `JavaObjectSerializer` with an `ObjectInputFilter` allow list; `JacksonSerializer` is safe as long as polymorphic
  (default) typing stays off.
- The README states the threat: values come from a shared store, so with the default, whoever can write to the store
  can make every instance deserialize arbitrary classes. The store's access rights are part of the trust boundary;
  whoever cannot guarantee that registers classes. Fory logs its own warning when registration is not required.

**Built:** the serializer API carried over unchanged (`Serializer`, `ByteArraySerializer`, `StringSerializer`,
`JsonSerializer`, `ForySerializer`, `JacksonSerializer`, `JavaObjectSerializer`), plus
`StringSerializer.ofString()`, `ofLong()`, `ofInteger()`, `ofUUID()` and `ByteArraySerializer.ofByteArray()`,
`ForySerializer.allowing(...)`, and `JavaObjectSerializer.allowing(...)` / a constructor taking an `ObjectInputFilter` (the allow list also admits
`java.lang`, `java.util` and `java.time`, which the objects commonly contain; the known gadget chains need classes
outside the JDK). `withSerializers(serializers -> serializers.withKeySerializer(...).withValueSerializer(...))` in the
builder; the core's `InternalSerializer` derives the format and moves text as UTF-8.

**Unreadable rows** (a class changed after a deploy, a switched serializer, corrupt data) are a miss: the loader
runs and its result is written conditionally on that row's version (ordering rule 6), so the fresh value replaces
the unreadable row without separate repair logic. A population read of an unreadable row drops the held key;
`store().get` throws a clear exception, since the inspection API
reports what is there. Visible as a counter in the distribution stats and one rate-limited warning per cache naming
the class or format. Switching the serializer of an existing table works the same way: old rows are replaced one by
one as they are accessed.

The core serializes and deserializes, and only what it uses (held keys, misses). The adapter never sees
`K` or `V`; the core translates the serializer once into a **format**, passed when the cache's adapter creates its
store:

| Serializer                                                               | Format   | MongoDB    | PostgreSQL | Redis, Valkey, Dragonfly; plain JDBC |
|--------------------------------------------------------------------------|----------|------------|------------|--------------------------------------|
| `ByteArraySerializer` (Fory, Java object)                                | `BINARY` | `BinData`  | `bytea`    | bytes                                |
| `StringSerializer`, `JsonSerializer` with `storeAsNativeJson() == false` | `TEXT`   | string     | `text`     | UTF-8 bytes                          |
| `JsonSerializer` with `storeAsNativeJson() == true` (Jackson)            | `JSON`   | BSON value | `jsonb`    | UTF-8 bytes                          |

`storeAsNativeJson` was `storeAsBinaryJson` until 2026-11-01: named after MongoDB's BSON, it chose `jsonb` on
PostgreSQL as well and nothing on the stores without a JSON type of their own, so it was renamed after what it means on
every store, its Javadoc a table per store.

How often it happens, and three rules that keep it at the minimum:

| Operation                         | Serialize           | Deserialize                                          |
|-----------------------------------|---------------------|------------------------------------------------------|
| L1 hit                            | –                   | –                                                    |
| put                               | key and value, once | –                                                    |
| miss answered by L2               | –                   | value, once (not the key; the caller has it)         |
| population read by a holder       | –                   | value, once per holder, only if the version is newer |
| invalidation, expiry, any message | –                   | – (messages carry no values)                         |
| `putIfUnchanged` conflict         | –                   | value of the newer row, once                         |

1. **Keys are matched by hash and never deserialized.**
2. **Compare the version before deserializing.** `Row` holds bytes; the core deserializes only what it applies.
3. **Messages never carry values**, so receiving costs nothing; only holders that actually read deserialize.

Why the core serializes rather than the adapter (with plain objects and the serializer in the SPI):

- **Only the core knows what a row is needed for.** An adapter returning objects would deserialize every row it
  reads, including those the core then discards (version not newer, key not held), unless it returned lazy rows,
  in which case it only carries the serializer around for the core.
- **Listening sessions and cursors are shared between caches** (listener/watcher sharing, discriminator), each
  cache with its own adapter. With the core serializing, the shared layer handles bytes and a format tag per cache
  instead of a serializer per cache to pick for every row.
- **Serialization code exists once, not per adapter**: null handling, exceptions, and a stored value that can no
  longer be read after a deploy changed a class (handled once in the core, e.g. as a miss). Today's shared
  `SerializerAware` helper exists because both adapters need exactly that.
- **The adapter contract stays small**: bytes, strings and a format, no serializer interfaces.

The one real advantage of adapter-side serialization: the adapter knows the store's native form. With `JSON` on
MongoDB the serializer produces JSON text and the adapter parses it into BSON (as `convertJsonToBson` does today),
and the reverse on reads, so one extra parse per write and per read. PostgreSQL parses `jsonb` on the server, so no
client cost there. Accepted for now and measured while building; if it matters, a `JsonSerializer` may
optionally produce BSON directly (e.g. via Jackson's BSON support) and the SPI carries that tree for the format,
still without the adapter seeing a serializer.

Kept simple on purpose: the key is serialized on every put, although only an insert needs it, so the upsert stays
one statement.

### Settings: configured on the cache, handed to the store

*Decided 2026-10-23.*

What the cache needs from every store is configured once on the cache, not per adapter, and handed to the adapter
when it creates the store: `createStore(StoreSettings(keyFormat, valueFormat, operationTimeout))`. So far the
formats (from the serializers) and the operation timeout (`withStore(store -> store.withOperationTimeout(...))`, 10 s
by default since 2026-11-06, 30 s before: a hung store costs a caller up to the timeout until the store guard opens,
and a cache read slower than that is worse than the source; 10 s still rides out a typical failover and is twice the
longest maintenance batch measured; it was `withTimeout` on each adapter's builder before, then `withTimeout` on the store configurer). The
adapter applies the timeout to every operation and to its checks on creation, to the cache's own operations only, so
caches sharing a client or data source may have different ones: MongoDB as the client-side operation timeout of the
cache's database handle, unless the client sets its own (`timeoutMS`), PostgreSQL as the network timeout of each
borrowed connection, restored before it goes back, JDBC likewise (where the driver supports it) plus the query
timeout of each statement. The query timeout alone is no bound on a silent network: drivers enforce it by sending a
cancel, which such a network never delivers. The core relies on the bound where it polls (a write with an unknown
outcome has landed or never will twice the timeout later), so a store implementing `ChangeScan` must keep it. What only one kind of store has (a discriminator, a
listener connection factory, the schema reset) stays on its adapter. Timings internal to an adapter (MongoDB's
watcher answering within 10 s, the PostgreSQL listener's connect and probe timeouts) are not settings: the core's
heartbeat and supervision bound what they would otherwise have to.

## Lifecycle and failures

### Lifecycle: build, run, close

**Start: `build()` connects synchronously within a timeout.**

- Within a timeout (10 s by default, 30 s until 2026-11-06) `build()` checks the connection, the table's schema marker
  and the subscription, and returns a cache in `SYNCHRONIZED` state when all of it is in place.
- Configuration errors always throw: a missing driver, wrong credentials, a schema that does not match, no DDL
  rights without an existing table.
- An unreachable store returns a cache in `DEGRADED` that connects in the background, unless `withFailFast()`, with
  which it throws (see "Starting without the store", built 2026-11-12/13).
- A failed `build()` leaves nothing behind (sessions and threads released), so a retry starts clean.

**End: the distributed caches implement `AutoCloseable`.**

- `close()` unsubscribes, stops maintenance and releases the shared sessions and cursors (counted per user; the
  last one closes them). It is idempotent; loads in flight are discarded.
- After `close()` every operation throws `IllegalStateException`, including `asMap()` and its views: a closed cache
  that went on working locally would serve outdated values without any error.
- Frameworks close it themselves (Spring calls `close()` on `AutoCloseable` beans; tests use try-with-resources).

**No stop and restart.** Today's `stopSynchronization()` / `startSynchronization()` are dropped: a stopped cache
would get no invalidations and serve outdated values without any error, "close and build anew" does the same more
clearly, and it removes a state (`STOPPED`) and the class of errors behind the partial-activation freeze.

**JVM shutdown:**

- Background threads are daemon threads, so a forgotten `close()` never keeps the JVM alive.
- **No shutdown hook.** At process exit the operating system closes every socket, so MongoDB and PostgreSQL release
  the sessions at once; there is nothing to flush, since writes are synchronous; and on `kill -9` no hook runs
  anyway. A hook would cost more than it gains: hooks run concurrently, so one closing the user's `MongoClient` or
  `DataSource` first makes ours fail at every shutdown, and a registered hook keeps the cache and its classloader
  alive across redeploys until it is removed, which needs `close()` anyway.

**Ownership:** the user owns what they pass in (`MongoClient`, `DataSource`, executor)
and closes it; the library owns only what it opens (listening sessions, cursors, threads) and closes it with the
last cache using it.

### Starting without the store (the default; `withFailFast()` opts out)

*Decided 2026-11-12, built 2026-11-12 and 2026-11-13.*

**The case, seen in practice:** the store goes down; the running instances turn `DEGRADED` and serve through the
loader, as designed. But a health check that includes the store (or the cache's state) marks the pod unhealthy,
Kubernetes restarts it, and the new pod cannot `build()` and loops in `CrashLoopBackOff`. Pod by pod the application
goes down completely, although its loader's source is fine: the instances that would have ridden out the outage are
the ones killed. Restarts happen during an outage for other reasons too (an OOM kill, a node drained, a deploy, the
autoscaler). Retrying `build()` is no answer for users: the application holds the cache from its start (a bean, a
field), so it would need a wrapper switching from a fallback to the real cache.

**Two parts:**

1. **Documentation:** liveness must never depend on the store or on the cache's state; a store outage is what
   `DEGRADED` is for. Readiness may reflect the source the application needs. This prevents the restarts that the
   cache's own health causes, not the others.
2. **Starting without the store becomes the default; `withFailFast()` opts out** (decided 2026-11-12). The cascade
   hits exactly the users who do not know of an option, and starting lazily and healing on their own is what the
   database clients do (Spring's convention). `withFailFast()` keeps today's behaviour, for users who want a
   misconfiguration to stop the rollout.

**Scope: every adapter** (MongoDB, PostgreSQL, Redis, MariaDB, MySQL). Polled adapters need nothing more in the
core than fed ones: `InternalPollingStore` only delegates to the store and its `ChangeScan` (`changedWithin`), and it
already copes with failing polls (`Poller#isFailing`: degraded meanwhile; failed past the grace period, a loss and
then a gap). What the polled adapters add is tests.

**Behaviour:**

- `build()` throws whatever is told without the store or by a store that answered (configuration, a missing driver,
  credentials, a schema that does not match, missing rights). Only "the store was not reached" returns a cache in
  `DEGRADED`, the guard open from the start, and connects in the background.
- Until connected the cache behaves as in any outage ("States and recovery", `DEGRADED`, store down): misses go to
  the loader only and are held unconfirmed (`InternalCore.UNCONFIRMED`, single and bulk through `sourceOnly`),
  nothing is written to L2, writes are refused.
- Once connected, the checks of `Adapter#createStore` have run, the feed is subscribed (or polled), the guard
  closes, and the connect counts as a gap: the epoch moves on and the background pass checks what is held.
- A check that fails once the store answers (another layout, missing rights, a MongoDB standalone, a Redis cluster
  reached by a plain client) leaves the cache `DEGRADED` for good: logged at `ERROR` on every attempt, retried every
  5 minutes (rights can be granted while it runs). **The reason is not exposed** (decided 2026-11-12): no API
  beyond `DEGRADED`. Nothing automatic can act on it (a restart grants no rights and fixes no layout), so it is for
  humans, who read the log; a state per reason was rejected in "States and recovery" for the lack of such a use.
  Added later if a health indicator shows a real need; API taken away is harder.

**Design:**

1. **A switching stand-in store** (core-internal), used only when `build()` does not reach the store; a store that
   answers at `build()` is wired as today. The stand-in is always a `Store` and a `ChangeFeed`: the core sees only
   feeds anyway, since `InternalPollingStore` turns a scan into one. Until connected, every operation fails at once,
   retryable and with nothing sent (so never an unknown outcome), and its subscription reports a loss at once, so
   the existing resume supervision takes over (`superviseResume`, 250 ms doubling to 30 s). The order holds as
   today: `attemptResume` resumes first and announces the gap after, so the background pass reads after the
   subscription.
2. **The kind of change source is decided at connect, by the created store, as `DistributedCaffeine#core` decides
   today:** a `ChangeFeed` is delegated to directly; a `ChangeScan` without a feed is wrapped in
   `InternalPollingStore` first (its name, executor and environment as in `core`), then delegated to; a store
   implementing both gets its feed. Neither is a late configuration error (`DEGRADED` for good, `ERROR`). No SPI
   change: a declaration on `Adapter` (considered first) is not needed, and could not express a kind that depends on
   the server, which only connecting tells: a MongoDB standalone without change streams, or a PostgreSQL behind a
   pooler that cannot hold a `LISTEN` session, could be polled rather than refused, should a store ever choose by
   what it reached. The kind is decided once per cache, as today: `createStore` runs once, reconnects happen inside
   the store, and a server losing its feed later fails as any feed does (interruption, resume, `DEGRADED`).
   **One decision, in one place:** the rule moves out of `DistributedCaffeine#core` into an internal function taking
   the created store and the builder's settings and returning what the core subscribes to (the store's feed, or its
   scan wrapped in `InternalPollingStore`). The normal `build()` and the stand-in at connect both call it, so they
   cannot drift apart. A future choice of the user plugs in there: a flag forcing polling (`withPolling()`, say)
   would wrap a store's scan even where it has a feed, and refuse a store without a scan (thrown from `build()` with
   the store up, a late configuration error when started without it). An explicit flag rather than "a poll interval
   is set": the interval has a default and tunes the scan adapters, so setting it in a shared configuration must not
   switch a feed store to polling. Not built until a use case comes (no store is both feed and scan today). A
   message bus, once built ("What follows: pluggable message buses"), plugs in there too: a bus configured, else the
   store's feed, else its scan polled. The stand-in stays the store's alone; a bus not reached at start is a
   subscription lost, resumed by the existing supervision.
   What the stand-in covers for it:
   - **the prune grace**, final in the core today and depending on polling (`DistributedCaffeine#pruneGrace`),
     becomes adjustable and is set at connect; nothing is pruned before, as maintenance fails until then;
   - **the poller's status**: `InternalCore#feedUp` asks `subscription instanceof InternalPollingStore.Poller`
     whether the latest poll failed, so the stand-in's subscription passes that status through from the poller it
     wraps;
   - **the polled checks at connect**: the poll interval at most half the heartbeat interval, checked at `build()`
     today once the store is known to be polled.
3. **A connector of its own** in the background (on the store executor, timed by the `InternalEnvironment`, so the
   simulation reaches it): calls `Adapter#createStore` with backoff (250 ms doubling to 30 s, with jitter), installs
   the real store in the stand-in, closes the guard. Not the guard's probe: `createStore` waits up to the operation
   timeout per check and may run DDL, which must never block a user's read; the guard's probe is refused at once by
   the stand-in until then.
4. **"Not reached" defined for `createStore`:** a retryable `StoreException` from it means the store was not
   reached (refused, unknown host, timeout); anything else throws from `build()`. Adapters align on it, as the build
   checks are idempotent and a timeout is as good as a refusal there: Redis marks `TIMEOUT` not retryable today
   (`RedisStore#failure`), so a Redis behind a partition dropping packets would throw; the JDBC adapters classify by
   SQL state `08` plus elapsed time and `Connection#isValid`, to be checked for creation. A Redis replica (a failover
   in progress behind Sentinel) is retryable and so starts degraded, which is right.
5. **A bound on copies loaded without the store** (decided 2026-11-12). A pod that never connects sees no writes of
   other instances, so only the lifetime of its copies bounds their staleness. That lifetime: Caffeine's expiry if
   set, else the maximum age, also under `withMaximumAgeForStoreOnly()`. The maximum age is a local bound per se;
   the store-only mode relaxes it because messages keep the copies current, which does not hold for a copy no store
   confirmed. So copies at version `UNCONFIRMED` expire at the maximum age at the latest, in any outage, not only
   after a start without the store. With the store-only mode, L1 is built with Caffeine's fixed expiry today
   (`CaffeineConfigurer#applyExpiry`): it then needs `InternalExpiry` to cap only unconfirmed copies (to check).
   `withSchemaReset()` stays allowed (refusing it was planned first, but the core cannot see an adapter's setting,
   and need not): the reset acts only on a table or collection without the current marker (one of 0.x, or empty and
   unmarked, `PostgresSchema#requireResettable`, `MongoSchema#requireResettable`), so a deferred reset never meets
   one that current instances have used since; on one of 0.x it does what it would have done at `build()`. **A cache bounded neither way** (no maximum age, no
   Caffeine expiry: the builder's default) starts without the store like any other (decided 2026-11-12): its copies
   then live until the store is reached. Refusing it (proposed first) would make two defaults, the builder's own
   configuration landing on the exception, and would guard only the start: the risk, copies missing writes of
   other instances, needs a partial partition (in a full outage nobody writes), and a running instance cut off by
   one is in exactly that state today ("States and recovery", accepted until recovery). A start adds only a new way
   into it, a misconfiguration at deploy; no bound is the user's choice of copies that live for good. The repeated
   warning says so (step 6).
6. **Logs:** one `WARNING` at the start ("started without the store"), repeated every 5 minutes while not connected,
   so a misconfiguration does not hide; for a cache bounded neither way it says that copies live until the store
   is reached. Maintenance, heartbeat and resume do not each warn before the first connect;
   `INFO` once connected. The name in messages: the adapter's `toString()` is taken after `createStore` today
   because Redis names the product (Valkey, Kvrocks) only after asking the server, so a name without the product
   until connected.
7. **`withFailFast()` lives under `withStore(...)`** (decided 2026-11-12): `withStore(store -> store.withFailFast())`,
   next to `withOperationTimeout`, which bounds the first connect too. It is about how the cache treats its store at
   `build()`; on the builder itself it would sit among Caffeine's settings and read as one of them.
8. **The first attempt stays in `build()`, bounded by the operation timeout** (decided 2026-11-12). A store that
   answers is wired as today and configuration errors throw from `build()`. A refused connection or a name not
   resolving (a pod down, a service gone) fails within milliseconds; dropped packets (a partition, a firewall) take
   up to the operation timeout per check, per cache, one cache after another: five caches during a partition delay
   the start by 50 s or more, which a Kubernetes startup probe must allow for (documented). Rejected: a shorter
   timeout for the first attempt (another constant, and a slow but healthy store would start degraded), and no
   attempt in `build()` (configuration errors would no longer throw even with the store up, and the normal path
   would change). **Built 2026-11-13, at the user's request:** caches sharing a client reuse the first
   one's "not reached" instead of each waiting. An adapter names the client it shares (`Adapter#client()`, a default
   method returning `null`: an addition to the SPI, the user's choice over a public helper each adapter would wrap
   its `createStore` in), and the core applies the rule around every creation, at `build()` and at a connect
   (`InternalReachability`, on the `InternalEnvironment`'s clock): a creation that failed retryable after waiting at
   least a tenth of the operation timeout is remembered per client (a weak map, by the client's equality, identity
   for every driver's client); for one operation timeout after it, creations through the client fail at once as not
   reached. A creation that reaches the store, or that the store answers otherwise, forgets it; a fast failure (a
   refusal, an unknown host) is not remembered. The adapters name `MongoClient`, the `DataSource`, and the Redis
   library's client, which `RedisDriver#client` now carries, so that caches wrapping one client in drivers of their
   own are known. While the store is silent, one creation per client and operation timeout asks it; once it answers,
   the caches connect within about one operation timeout and their backoff. Tested: `InternalReachabilityTest` (on
   the simulated clock), and in `AdapterConformanceTest` four caches on one client built behind a silenced proxy (the
   first waits, the others start at once, all connect).
   That test found a race older than this work, in creating a new table or collection while caches start together
   (pods of a rollout, say): one read "no marker", then saw the table another had just committed, and refused it for
   good as a table without the marker. PostgreSQL creates table and marker in one transaction, so `PostgresSchema`
   reads the marker again once it sees the table. MongoDB's marker upsert creates the collection a step before the
   marker, so `MongoSchema#markerArrives` gives an empty collection without a marker up to a second for it (one with
   documents answers at once). MariaDB and MySQL create table, index and marker in one statement and recognised a
   concurrent creation already.
9. **`close()` during a connect** closes a store created after it and releases the claim; a failed connect leaves
   nothing behind, as a failed `build()` does.

**Where it is built** (2026-11-12, build step 2):

- `DistributedCaffeine#core`: a retryable `StoreException` from `createStore` (and not `withFailFast()`) builds the
  core on an `InternalPendingStore` with an `InternalCore.Connector`; `#distributing` is the one decision of feed or
  scan, for `build()` and for `#connect` (which closes a store it cannot use).
- `InternalPendingStore`: fails every call at once, not retryable (a retry would only wait) and never with an unknown
  outcome; its subscription subscribes the store's feed (or poller) on the first resume after the connect and passes
  the poller's status on (`InternalPollingStore.PollStatus`, which `InternalCore#feedUp` asks instead of the
  `Poller` class).
- `InternalCore`: the guard open from the start (`InternalStoreGuard#openWithoutStore`), the feed taken for lost
  by the core itself (the stand-in reporting `onLost` made `InternalDelivery` log a warning with a stack trace at
  every start); `#attemptConnect` with the resumes' backoff and jitter, `#notReached` warning every 5 minutes,
  `#failedCheck` at `ERROR` (with a stack trace only for what is neither a `StoreException` nor the core's own
  `IllegalStateException`) and again after 5 minutes; on success the prune grace set, the guard closed quietly
  (`InternalStoreGuard#connected`), the feed resumed at once (`#resumeNow`, also from an attempt that failed while the
  connect happened); resume failures not logged before the connect; the start warning names the unbounded case.
- `InternalExpiry#unconfirmedMaximumAge`, through `CaffeineConfigurer#caffeine(maximumAge, unconfirmedMaximumAge)`:
  with the maximum age for the store only, L1 now always runs on the library's expiry, so copies at version
  `UNCONFIRMED` expire at the maximum age (and follow it when set at runtime).
- `StoreConfigurer#withFailFast()`; `CaffeineConfigurer#hasExpiry()` for the unbounded case.
- `InternalStartWithoutStoreTest` (in-memory, feed and polled): the degraded start, the connect taking the store's
  value and distributing both ways under a partial partition, `withFailFast()`, a non-retryable failure still
  throwing, an unusable store found at the connect (`ERROR`, closed, `DEGRADED`), `close()` before the connect, the
  unbounded warning, and the cap on unconfirmed copies.

**Accepted:**

- **Two configuration checks come late when started without the store:** a distribution mode the created store
  cannot support (neither feed nor scan; none of the shipped adapters), and the poll interval at most half the
  heartbeat interval. Both depend on the kind of change source, known only at connect; both fail on every start with
  the store up, so they show in any environment where it is.
- **A misconfiguration looks like an outage.** A firewall or `NetworkPolicy` blocking the store, or a wrong host
  name, no longer stops the rollout: the pod runs `DEGRADED`, logged and visible in its state, its copies bounded by
  L1's lifetime (see design step 5). The price of the default; `withFailFast()` restores the stop.
- **A change of behaviour on upgrade:** `build()` no longer throws when the store is not reached. An application
  retrying `build()` itself, or relying on its exception to stop the start, gets a `DEGRADED` cache instead: in the
  release notes, with `withFailFast()` as the way back. The README's "Building connects to the store and checks it,
  so a misconfiguration fails at once" changes with it.
- **The recovery is a deferred cold start.** Every value loaded meanwhile has version `UNCONFIRMED` (0), which
  `InternalCore#stillStored` never matches, so the background pass drops all of L1 once connected and every key
  costs an L2 read again (a source load where the store lacks it). Pods restarted during an outage do this together
  when the store returns; the pass is rate-limited per instance, not across them.
- **Only caches that load gain:** puts and invalidations are refused until connected, so a cache filled by `put`,
  or startup code writing to it, gains nothing; and with the loader's source in the store's database, misses fail
  anyway.
- **The user's client must be creatable while the store is down.** MongoDB's `MongoClients.create` connects lazily;
  a `HikariDataSource` built from a `HikariConfig` fails at construction with the default
  `initializationFailTimeout` (Spring Boot's lazily started one does not); Redisson connects in `Redisson.create`.
  To verify per driver and document in each adapter's README.
- **It helps only where the application itself starts without the database.** Spring Boot, for instance, starts
  without MongoDB (the client connects lazily) and with plain JDBC on HikariCP (its pool starts on the first
  connection), but not with Flyway or Liquibase (migrations at startup) and usually not with JPA (Hibernate reads
  JDBC metadata at bootstrap unless the dialect is configured). A cache whose store is the database the application
  migrates or maps with JPA gains nothing from starting without it; it gains with MongoDB, plain JDBC, or a store
  separate from the application's main database. The README remark says so. `build()` today behaves like Flyway (a
  schema check at startup); the new default moves it to the clients' model (start, degrade per operation, heal on
  its own), Spring's convention.

**For later** (see "Open work"): installing the row the background pass read instead of dropping a
mismatched copy; a scan next to a feed for catching up after a gap; a flag forcing polling and the message bus,
both plugging into the decision of design step 2.

### Adapter ownership: one adapter per cache

- **An adapter describes where one cache lives in the store:** the user's client (`MongoClient`, or `DataSource`),
  database, table or collection, and discriminator. At `build()` it creates the cache's internal `Store` (the SPI,
  invisible to users).
- **What is shared is not the adapter but what is expensive underneath:** listening sessions and cursors, through
  the registry keyed by client identity and database ("Connection sharing"). A shared adapter object was
  rejected: a second lifecycle object (who closes it, what happens to caches still attached), sharing done twice
  (explicitly and through the registry), more API for no gain.
- **One adapter per cache is enforced**, as today by a global claim: an adapter object used for two caches is almost
  always a mistake (two caches, possibly with different types and serializers, on the same table and
  discriminator, corrupting each other's rows), so `build()` rejects it with a clear error. The claim is released on
  `close()` and on a failed `build()`, so the adapter can be reused afterwards.
- **Two caches on the same location with separate adapter objects stay allowed:** they behave like two instances,
  which is useful in tests and consistent with the model.
- **`DistributedPolicy.getAdapter()` is dropped:** it exposed `activate()`/`deactivate()`, the cause of the
  partial-activation freeze, and the adapter is now pure configuration that whoever needs it already has.

### States and recovery

Three states: `SYNCHRONIZED`, `DEGRADED`, `CLOSED`. What the cache does while degraded depends on which side fails;
both report `DEGRADED`, and the reason goes into the log (a state per reason would add API without a clear use).

|                | `SYNCHRONIZED`     | `DEGRADED`, store down                                   | `DEGRADED`, subscription down        | `CLOSED` |
|----------------|--------------------|----------------------------------------------------------|--------------------------------------|----------|
| L1 hits        | served (confirmed) | served unconfirmed                                       | each access checks its copy first    | throws   |
| Misses         | L1 → L2 → loader   | loader only, held in L1, not written to L2               | L2 → loader                          | throws   |
| Writes         | normal             | refused (`invalidate` drops the local copy, then throws); an atomic method writing nothing answers from a held copy | normal                               | throws   |
| Back to normal | –                  | guard closes, reconnect, revalidation                    | reconnect, revalidation              | –        |

- **Store down** (the store guard is open; the subscription is usually down too): nobody can write, so held values
  rarely go stale; in a partial partition, where others still reach the store, they can, until recovery (accepted,
  as today). A miss goes to the loader at once, because the guard refuses store calls without waiting for timeouts;
  the result is held in L1 and confirmed or dropped by revalidation later. Writes are refused: a put kept only
  locally would be lost, and a lost invalidation would leave others serving the old value without any error. A held
  value awaiting its check still tries the store on every access: the guard refuses at once or lets it through as
  its probe, so the first access after the store is back checks it (found by a PostgreSQL failover that loses a
  confirmed write: otherwise the cache served the lost value until the next maintenance run).
- **Only the subscription down, the store answering:** every access checks the held copy against the store (its
  version and value, see below), because a copy confirmed now can go stale the next moment while no message can
  arrive. Writes work normally; the store announces them to the others.
- **`CLOSED`:** after `close()` every cache operation throws, except `distributedPolicy().synchronizationState()`, so
  health checks can query a closed cache without catching exceptions.

**Revalidation instead of clearing L1** (after a short interruption almost everything held is still correct):

- Each instance has an epoch counter; every `InternalValue` records the epoch at which it was last confirmed. A held
  value is trusted only if its epoch is current and the subscription is up.
- On a gap the counter is incremented, which makes everything held unconfirmed without touching an entry.
- An unconfirmed value is checked when read: the store returns the row, under the maximum-age filter. Same version
  and the same value (compared serialized, as the store holds it) → kept and confirmed; different, too old or missing
  → a miss (L2, then the loader, as usual). The version alone does not tell: after a failover that lost a write, a
  store assigning versions can give a later write the lost one's version (found 2026-11-10, see dimension 4 of the
  in-memory store profiles).
- A background pass checks all held keys in batches (rows, e.g. 1,000 per query, rate-limited so it does
  not load a store that has just recovered) and drops what does not match, so `asMap()`, iteration and
  `estimatedSize()` are clean again soon, not only once each key has been read. It covers what never goes through a
  read: views and iteration, sizes, quiet reads (`getIfPresentQuietly`, L1 only), the user's removal listener, and
  memory held by outdated entries. Before dropping, it compares the version again under `compute`, like every other
  path. Kept after reviewing it as possible over-engineering: without it, views would either show possibly outdated
  entries after a gap until read (a deviation visible only in rare, hard-to-understand moments) or hide unconfirmed
  entries (and look almost empty right after a gap); its cost arises only after gaps and reuses the read path's
  batched reads.
- **Order:** subscribe again first, then check. Every change after the check arrives as a message and is handled by
  the version comparison; checking first could miss a change in between.
- After a crash that lost acknowledged writes, the lost versions held by peers no longer match the store and are
  dropped.
- A MongoDB resume within the token's reach is no gap, so the epoch stays; while the cursor is down, the per-access
  check applies anyway, since messages are delayed.
- Costs: one `int` per `InternalValue`; a row read (its value transferred) on the first access per key after a gap,
  and on every access while the subscription is down; one query per 1,000 held keys per gap for the background
  pass.

**Store guard, as today (measured and tested):** opens after 3 consecutive failures; while open, store calls are
refused at once; after 1 s the next call is the probe; a successful call, or the maintenance worker's success,
closes it. It covers reads as well as writes.

### Write retries

A retry is safe when the outcome is known (certainly not committed) or does not matter (the write is conditional);
it is unsafe for an unconditional put whose outcome is unknown. (The PostgreSQL adapter once retried deadlocked or
aborted writes assuming every write is idempotent; with versions that assumption does not hold.)

**The core retries, the store classifies** (2026-10-23, step 2 of the SPI rework): a store tries once and reports a
failure as `StoreException` with two flags, `isOutcomeUnknown()` (a write may have been applied, wholly or in part)
and `isRetryable()` (another attempt may succeed soon). The core (`InternalStoreGuard`) tries a call up to 3 times
with jitter (20 ms base) while the failure is retryable and repeating it is safe by the table below; a write whose
outcome was unknown in any attempt stays unknown; anything else a store throws is a failure that is not retryable,
with a write's outcome unknown. One logical call counts once for the store guard and in the stats. So an adapter
contains no retry policy, and every adapter gets the same one. A store reports a batch that failed after part of it
was written as unknown (`StoreException.afterPartlyApplied()`), since repeating a partly written `put` could
overwrite a newer write of another instance.

**Two kinds of failure:**

- **Certainly not committed:** on PostgreSQL the server rejected the statement, which in autocommit rolls it back
  completely (deadlock `40P01`, serialization failure `40001`, lock not available `55P03`), or the connection could
  not be established (`08001`, `08004`), so nothing was sent. On MongoDB errors before sending and those the driver
  labels retryable.
- **Outcome unknown:** a timeout or broken connection after sending (on PostgreSQL the other `08...` errors during
  execution and socket timeouts); the write may have committed.

| Write                                                                                                         | Certainly not committed | Outcome unknown                                                                                                                                                                                                                                                                                                                                                        |
|---------------------------------------------------------------------------------------------------------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| conditional write of a load or a refresh                                                                      | retry                   | retry: if the first attempt landed, the retry sees a changed version, a conflict, and reads the row (the "newer" row is then its own, the value it loaded)                                                                                                                                                                                                             |
| conditional write of an atomic map method (`putIfAbsent`, `replace`, `remove(k, v)`, `compute`, `merge`, ...) | retry                   | retry (since 2026-11-04): a refusal after an attempt of unknown outcome is checked against the row. At the write's version with its content it is the write itself, applied; otherwise the call throws, the key dropped from L1, as for `put`. Deciding again on its own row would apply the remapping twice (`merge` adding twice, reproduced by `InternalRetryTest`) |
| `invalidate`                                                                                                  | retry                   | retry: at worst it removes a value written in between, which costs that key one load from the source but never leaves an outdated value                                                                                                                                                                                                                                |
| `put` (unconditional)                                                                                         | retry                   | **no retry:** if the first attempt landed and another instance wrote in between, the retry would overwrite their newer value with the older one. The key is dropped from L1 (ordering rule 5), and the call throws, since the caller cannot know whether the write happened                                                                                            |
| expiry, pruning, `invalidateAll`                                                                              | retry                   | retry: all are conditional (`state = VALUE`, ...) and idempotent, and maintenance runs again anyway                                                                                                                                                                                                                                                                    |

**MongoDB: the driver retries.** Single-document writes are retryable writes: the driver retries them and the server
deduplicates by the session's transaction number, so a retry never applies twice, which removes the unknown case
even for unconditional puts. With CSOT, every read iterates its cursor with the timeout per batch
(`TimeoutMode.ITERATION`), not for the whole read, which a large pruning would exceed. Without a client timeout,
every store operation gets the cache's (`withStore(store -> store.withOperationTimeout(...))`, 10 s by default), since the driver waits for good on a
silent network otherwise. Nothing that timed out is retried (it waited its whole time already: for a server, a
pooled connection, or the answer); a write that timed out has an unknown outcome, because the driver's timeout
does not tell waiting for a server from waiting for the answer. The README recommends CSOT
(`timeoutMS`), since without it the driver retries only
once, and that retry can still land inside a failover (a lesson, see the checklist). A CSOT timeout may strike
after sending, so it is an unknown outcome. Multi-document updates
(`invalidateAll`, expiry, pruning) are not retryable writes; the core repeats those, safely, since they are
conditional. The adapter classifies what the driver labels transient as retryable, a write broken after sending as
unknown, and a timeout as neither.

**PostgreSQL: classified by SQLState:** a rejected statement (`40P01`, `40001`, `55P03`) or a connection that could
not be obtained is retryable and certainly not applied; a connection lost during a statement is retryable with a
write's outcome unknown; a statement without an answer within the timeout, a pool that waited its whole timeout for a
connection, and every other error are not retryable. A missing table is not retryable either (see "The PostgreSQL
adapter": changing the table by hand is not supported).

**Closed 2026-11-04 (was the open point):** a conditional write whose outcome was unknown and that the core repeated
could not tell its own row from another writer's. For loads and refreshes that is harmless (deciding again returns the
value written); for the atomic map methods it applied the remapping twice, a wrong value in the store and every cache.
Fixed in `InternalCore#decide` (the guard tells it that an attempt of unknown outcome was repeated): a refused
repetition reads the row, and the row at the write's version with its content counts as the write itself (the check
`advance` makes); anything else throws, outcome unknown. So it throws only where the store applied a version of its
own (PostgreSQL, Redis; MongoDB mostly applies the proposal, and its driver's retryable writes absorb most lost
answers) or another write raced the lost answer. Rejected first, the small fix (never repeating these writes after an
unknown outcome): it threw also where the repetition would have applied cleanly. Accepted: two instances proposing
the same version and content (one pair in 1,024 within one millisecond and counter) read as one write there, one
remapping lost; it needs a lost answer and such a race together. Never throwing would need an id of the write in the
row, returned with a refusal (as DynamoDB returns the item on a failed condition): an SPI and schema change in every store, only if throwing on an unknown outcome turns out too strict.

### Operations

**Logging** (`System.Logger`, named after the package, derived from `DistributedCaffeine` or the adapter class;
settled 2026-11-06): every message of a cache begins with its name, its adapter's `toString()` (`MongoDB
orders.cache/default`: product, place, discriminator, never connection details); what is thrown is not logged, the
caller decides; what happens without a caller is logged once per incident, not per call, with the
recovery at INFO: the change feed lost and resumed, polling failing and succeeding again, listener callbacks
failing, the store not answering (the store guard opening; reads fall back to the source without a trace, so an
outage would go unnoticed otherwise) and answering again, and maintenance failing as many runs in a row as the guard
needs to count the store down, while the store otherwise answers (a batch always timing out, a missing right to
delete), and succeeding again. Plus one-off notices (a schema reset dropping a table, a missing index, an evicting
Redis, a clock far ahead).

**Threads** (all library threads daemon, named `distributed-caffeine-<role>`):

- One listening thread per shared session or cursor, not per cache (follows from connection sharing).
- One shared maintenance scheduler per JVM for all caches (expiry, pruning, background revalidation), instead of one
  thread per cache as today; it only triggers, the store calls run on the store executor.
- No delivery thread per cache: each cache's subscription processes its messages serially on the store executor
  (an ordered queue per cache), with the same ordering guarantees and fewer threads.

**Executors:**

- **Store executor** (blocking store I/O): configurable (e.g. virtual threads on 21+, see Java baseline); default a
  library-owned cached daemon pool whose idle threads end after 60 s, concurrency bounded by the callers and the
  user's connection pool. Not `ForkJoinPool.commonPool`, which blocking I/O would starve.
- **Caffeine's executor** (the user's loader, listeners, refresh) stays the user's choice as in Caffeine (default
  `commonPool`); the internal `AsyncCacheLoader` hands store calls to the store executor, so a user loader never
  waits behind store I/O.

**Logging:** `System.Logger` as today (no dependency, routed to SLF4J or Log4j by the usual bridges), one logger per
component (`...distributedcaffeine.core`, `....mongodb`, `....postgresql`) instead of one shared logger.

- `WARNING`: entering `DEGRADED` with the reason (store or subscription); retries exhausted; unreadable rows
  (rate-limited per cache).
- `INFO`: leaving `DEGRADED`; the summary of a revalidation (checked, kept, dropped); a resume after a reconnect.
- `DEBUG`: the configuration at `build()`.
- Never per operation; repeated warnings of one kind are rate-limited (e.g. once a minute, with a count). Tests do
  not count log lines (a lesson, see the checklist); they check states and stats.

**Stats:** `stats()` stays Caffeine's. `distributedStats()` returns a `DistributedStats` value (`plus` /
`minus`), gated by `recordStats()`, with counters re-derived for this model; no Micrometer binder (the value
can be polled and exported).

| Area               | Counters                                                                               |
|--------------------|----------------------------------------------------------------------------------------|
| L2 reads           | hits, misses, rejected by maximum age, read time                                       |
| Source             | loads (distinct from Caffeine's load count, which includes L2 hits), reloads           |
| Writes             | puts, conditional writes, conflicts, refused (store down), unknown outcome, write time |
| Messages           | received by kind, applied, ignored (version not newer), population reads               |
| Removals by others | invalidated, expired, replaced                                                         |
| Recovery           | gaps, reconnects, revalidated (checked, kept, dropped), store guard openings           |
| Data               | unreadable rows, negative hits                                                         |

Built (2026-10-21), 31 counters as named accessors on an immutable `DistributedStats` (`empty()`, saturating
`plus`, `minus` never below zero):

- **Store calls** are counted and timed in the store guard (`storeReadCount` / `totalStoreReadTime`,
  `storeWriteCount` / `totalStoreWriteTime`; each time with its own matching count, for window averages), plus
  `storeFailureCount`, `unknownOutcomeCount`, `refusedCount` (all calls refused while the guard is open, not only
  writes) and `guardOpenCount`. Maintenance calls the store directly and is not counted: which instance prunes is
  timer luck (the lesson of `prunedEntryCount`).
- **Per key:** `storeHitCount` / `storeMissCount` (loads and `getIfPresent`), `sourceLoadCount`,
  `sourceReloadCount`, `putCount`, `conditionalWriteCount`, `conflictCount`, `populationReadCount`,
  `revalidatedCount` = `revalidationKeptCount` + `revalidationDroppedCount`.
- **Messages:** received by kind (`written`, `invalidated`, `expired`, `cutoff`), and what they did to a
  held completed value: `replacedCount`, `invalidatedCount`, `expiredCount` (the "removals by others"), or
  `ignoredMessageCount` (not newer: echoes, duplicates, delayed messages). "Applied" is their sum, so it is not a
  counter of its own. A message for a key with a load in flight counts only by kind.
- **Not built:** "rejected by maximum age" (the store omits value rows older than the maximum age from reads, so
  they are indistinguishable from missing rows without an SPI change; they count as misses).

**Caffeine's `stats()` from the user's view,** two corrections found while building:

- `getIfPresent` and `getAllPresent` no longer read the store through Caffeine's loading `get` (which counted every
  miss as a miss plus a failed load): a key not held is read through the store directly and its value held under the
  write registry, so each key counts exactly one hit (found, locally or in the store) or one miss, never a load.
- L1 writes (commits of puts, held store values, population) use compare-and-swap on `putIfAbsent` / `replace` /
  `remove(k, v)`. Measured on Caffeine 3.3.1: on an `AsyncCache`'s map, `compute`, `computeIfPresent` and `merge`
  count a successful load; `put`, `putIfAbsent` and `replace(k, old, new)` count nothing. So every `put` showed up as a
  load.

**Messages for a key with a write in flight** (the write registry) are not applied to L1 directly: the version rule
is applied when the write commits. The writer's own echo, often faster than its commit to L1, then costs neither a
drop nor a population read (it did before, wasted but correct). Within a delivery batch, a key is read for
population once, however many messages it has.

**Timings** (only what affects the semantics is configurable; the rest stays internal and is re-measured with R2DBC
before the release, the heartbeat in particular):

| Timing                                         | Default                                                         | Configurable                                      |
|------------------------------------------------|-----------------------------------------------------------------|---------------------------------------------------|
| Maintenance interval (expiry, pruning)         | 1 min ±20 % jitter                                              | yes: part of the maximum-age bound                |
| Tombstone retention                            | 5 min                                                           | no                                                |
| `build()` timeout                              | 10 s                                                            | yes                                               |
| Store guard                                    | opens after 3 failures, probe after 1 s                         | no                                                |
| Feed heartbeat (reserved-key invalidate, core) | every 10 s ±20 % jitter                                         | yes: bounds how long a silent feed goes unnoticed |
| Resume after a loss (core)                     | at once, then from 250 ms, doubling, at most 30 s, ±20 % jitter | no                                                |
| MongoDB shared watcher `maxAwaitTime`          | 100 ms                                                          | no                                                |

**README for operations,** carried forward and updated: CSOT (`timeoutMS`) for MongoDB, socket timeouts, DNS TTL,
managed services, `writeConcernMajorityJournalDefault` (graded), R2DBC and poolers, the cost of `NOTIFY` per
listening session, the store as a trust boundary, and the DDL for migration tools.

### Delivery and cleanup details

- **Tombstone retention:** decided under Operations (5 minutes, fixed).
- **PostgreSQL delivery:** the notification carries hash and version, the receiver reads the row; if the row is
  already newer than the notification, the newer row is taken (consistent with rule 2).
- **`putAll`:** each key gets its own version, no atomicity across keys, as in Caffeine.
- **Cleanup on MongoDB:** the maintenance worker deletes tombstones and expired rows, as on PostgreSQL. A partial TTL
  index was considered and dropped: its filter would need "state is not `VALUE`", which partial indexes do not
  support as `$ne`, and the worker is needed anyway.
- **Pruning interval:** decided under Operations (1 minute ±20 %, configurable, since it is part of the age bound).

## Stores

### SPI: a write is the message

The stores derive messages from the write itself (change streams see only committed writes, `NOTIFY` is sent in the
writing transaction, a Redis script appends to its stream in the same step), so there is no separate publisher: an
adapter is a store whose writes announce themselves, plus a subscription. An adapter binds one kind of store
(connection, statements, conditional writes, sharing of listening connections, the discriminator).

**Capabilities, each optional beyond storage** (`spi`, authoritative over this summary):
- **`Store`:** storage only: reads, writes, maintenance. Every store has it.
- **`ChangeFeed`:** a store that announces its writes (MongoDB, PostgreSQL, Redis). Every distribution mode.
- **`ChangeScan`:** a store that can list what was written within a window (MariaDB, MySQL); the core polls it
  (`InternalPollingStore`, see "The polling feed"), so every distribution mode works there too.
- Neither: only `DistributionMode.NONE`; `build()` with another mode fails at once, naming the adapter.
- **`PrimitiveStore`:** a base class for a store offering only portable primitives (read rows, advance or replace one
  conditionally, delete, scan), which implements `Store` and `ChangeScan` on top of them (see "Two levels").

```java
interface Adapter<K, V> {                                    // typed for the builder only
    Store createStore(StoreSettings settings);               // checks connection, schema marker and shape; fails fast
    default @Nullable Object client() { return null; }       // caches sharing a client wait for an outage once
}
record StoreSettings(Format keyFormat, Format valueFormat, Duration operationTimeout) {}

interface Store extends AutoCloseable {                      // one per cache instance; non-generic, bytes and hashes
    Map<String, Row> read(Collection<String> hashes, Duration maxAge);           // value rows younger than maxAge
    VersionsResult readVersions(Collection<String> hashes, Duration maxAge);    // versions and ages, no values
    default Duration clockAllowance() { ... }                // how far the store's clock may step back or how coarse
    Instant time();                                          // the store's clock (marks)
    WriteResult write(Collection<Write> writes);             // per key: advance, ifVersion or ifMissing
    WriteResult writeControl(Write control, Instant before); // the control row of invalidateAll and the marks
    int expire(Duration maxAge, long version, int limit);    // maintenance, in batches, idempotent across instances
    int pruneToSize(long maxSize, Duration minAge, int limit);
    int pruneTombstones(Duration retention, int limit);
}
record Write(String hash, byte @Nullable [] key, byte @Nullable [] value, long version, long expected) {}
                                                     // a value or a tombstone; expected: ADVANCE, MISSING, a version
record WriteResult(Set<String> refused, Map<String, Long> assigned) {}  // refused keys; versions a store assigned
record Row(String hash, byte[] key, byte @Nullable [] value, State state, long version, Duration age) {}
record VersionsResult(Map<String, Long> versions, Map<String, Duration> ages) {}

interface ChangeFeed { ChangeSubscription subscribe(ChangeListener listener); }
interface ChangeScan { List<Change> changedWithin(Duration within); }
interface ChangeListener { void onChanges(List<Change> changes); void onGap(); void onLost(Throwable cause); }
interface ChangeSubscription extends AutoCloseable { void resume(); }
record Change(Change.Kind kind, String hash, long version) {}     // WRITTEN, INVALIDATED, EXPIRED
class StoreException extends RuntimeException { boolean isOutcomeUnknown(); boolean isRetryable(); }
```

- **Writes:** an *advancing* write (a put, a tombstone, expiry, the control row) applies only above the stored version
  or where no row exists; a write decided on a read applies only if the row still has the version read, or is still
  missing, at a version above it. A store applies the version the core proposed, or assigns one itself (above the
  stored one) and reports it in `assigned`; refused keys are listed. See "Versions applied as well as each store can".
- **Reserved rows:** hashes starting with `#` are the core's (the control row, `Store.CONTROL_HASH`, and the
  heartbeat's row); a store reads and writes them like any other, and maintenance never touches them.
- **Batch methods only:** a single key is a collection of one; keys of a batch are independent (a result per key).
- **Maintenance runs on every instance, idempotently,** no leader election; in batches of `limit` rows, paced by the
  core (build plan, step 6d, "Maintenance in paced batches").
- **No `start`/`stop`:** subscribing starts, `close()` ends; the synchronization state follows.
- **`client()`** (an SPI default method, 2026-11-13): caches whose adapters share a client do not each wait for a store
  that does not answer (`InternalReachability`).

**The contracts,** written out on the interfaces (authoritative) and checked by the tests named after each point
(`contractN_...`):

| Store | Point                                                                                                                                                                                          | Tests                                                    |
|-------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| 1     | Versions: a row's version only grows; a write applies at the proposed version (only above the stored one) or at one the store assigns and reports; a deleted row no longer defends its version | `StoreContractTest.contract1_*`                          |
| 2     | Clock: ages from the store's clock, set by every write; one read measures every age at the same instant, a negative age reported as it is; `time()` reads the same clock                       | `contract2_*`                                            |
| 3     | Visibility and durability: a read on any instance reflects every write confirmed before it began; a confirmed write is lost only in an event every subscription sees as a loss or gap          | `contract3_*` (durability per adapter)                   |
| 4     | Atomic conditional writes: of concurrent writes conditioned on one version, or on the key missing, one wins                                                                                    | `contract4_*`                                            |
| 5     | Idempotent maintenance on several instances at once                                                                                                                                            | `contract5_*`                                            |
| 6     | Keys and values byte for byte, in the formats of `StoreSettings`; only the store's own cache, also in a shared table or collection                                                             | `contract6_*`                                            |
| 7     | Every method as documented, for every state of a row                                                                                                                                           | `contract7_*`                                            |
| 8     | Failures only as classified `StoreException` (applied or not, retryable or not), a write applied in part reported unknown, no retries in the store                                             | each adapter's own, and `InternalRetryTest` for the core |
| 9     | The `StoreSettings` applied: no operation waits longer than the timeout                                                                                                                        | each adapter's silent-connection tests                   |

| ChangeFeed | Point                                                                                                                                                   | Tests                                |
|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------|
| 1          | Exactly the committed writes: never before the commit, never a rolled-back or refused write; deletes announce nothing                                   | `ChangeFeedContractTest.contract1_*` |
| 2          | The version the write got, as returned to the writer                                                                                                    | `contract2_*`                        |
| 3          | Per key at least the latest write since the last delivery, none older after a newer one; duplicates and reordering across keys allowed; otherwise a gap | `contract3_*`                        |
| 4          | Every write committed after `subscribe` returned, nothing after closing                                                                                 | `contract4_*`                        |
| 5          | A loss reported (`onLost`), restored only by `resume()`, from then on (the core counts every resume as a gap)                                           | `contract5_*`                        |
| 6          | A read after a delivered change reflects the announced write                                                                                            | `contract6_*`                        |
| 7          | Only the own cache's writes, also in a shared table or collection                                                                                       | `contract7_*`                        |

| ChangeScan | Point                                                                                                             | Tests                                |
|------------|-------------------------------------------------------------------------------------------------------------------|--------------------------------------|
| 1          | Every row written within the window, by the store's clock, with its latest version and kind; earlier ones allowed | `ChangeScanContractTest.contract1_*` |
| 2          | The control row in every scan, whatever its write time                                                            | `contract2_*`                        |
| 3          | Deletes report nothing                                                                                            | `contract3_*`                        |
| 4          | Only the own cache's rows                                                                                         | `contract4_*`                        |

Every store runs the store contract, every store with a feed the feed contract, every scanning store the scan contract;
the fixtures open a second cache in the same table or collection (another discriminator) for the "own cache" points.
Store point 3 and feed point 6 are what MongoDB needs causally consistent sessions for; feed point 7 is what a shared
change stream or listening session must filter for.

**The reference store** (`src/test/java/.../testing`, `InMemoryDatabase`): an in-memory database shared by several
stores as several instances share one store, with an injectable clock, controllable faults (write failures before or
after the commit, the store down, gaps, paused, delayed, duplicated and reordered delivery) and profiles acting as each
kind of real store (see "In-memory store profiles"). It runs every contract, also as a storage-only wrapper
(`StorageOnlyStore`) and as a scan-only one (`ScanOnlyStore`).

| Core                                                                     | Adapter                                                                  |
|--------------------------------------------------------------------------|--------------------------------------------------------------------------|
| hashes, serialization, L1, versions (proposed, compared), loads, refresh | storage per format, statements, conditional writes                       |
| when maintenance runs (interval with jitter, paced batches)              | how it runs, safe on several instances at once                           |
| the store guard and retries (exceptions → degraded state)                | classifying failures, reconnecting when asked                            |
| revalidating after a gap; the heartbeat; resuming the feed; polling      | reporting losses (Mongo: resume token lost; PostgreSQL: every reconnect) |
|                                                                          | sharing listening connections between caches, the discriminator          |

### The thin SPI, versions in the core

*Decided 2026-10-26, built 2026-10-28, build plan step 2; superseded the earlier target designs (see History).*

**Superseded in part 2026-10-28:** `invalidateAll` by a sweep is replaced by the store's time, hidden on read; see
"`invalidateAll` by the store's time, hidden on read" above. The rest of this section stands, and
its part on versions in the core is built (build plan, step 2).

**Decided the same day: `invalidateAll` with Caffeine's semantics, by a sweep.** This supersedes, below:

- the control row;
- visibility in the core;
- the cut-off and the basis;
- the 2ε wait and the measured skew;
- the capabilities "checks the cut-off when writing" and "global counter" (for `invalidateAll`).

Why:

- `invalidateAll` is the only operation that spans keys. Every store makes a single key atomic cheaply, but a
  statement about all keys at once needs a global barrier, and a global barrier is always coordination.
- Every rule that existed only for an atomic O(1) `invalidateAll` goes:
  - a stamp and a basis on every row, and the rules for visibility;
  - the control row read with every store read;
  - the cut-off check and the hot path depending on it;
  - the 2ε wait and the skew check;
  - purging hidden rows.
- What remains is what Caffeine itself promises: atomic per key, not across keys.

How:

- **A tombstone for every key**, written by the store, in batches the core controls and paces. In SQL that means
  batches, not one `UPDATE` holding locks over the whole table.
  - Tombstones, not physical deletes: a load decided before the sweep must conflict with the tombstone's version, as
    with any `invalidate`. A physical delete would let it through.
  - Tombstones carry no value, and are purged after the grace period as now.
- **Synchronous by default**, as in Caffeine: `invalidateAll()` returns when the sweep is complete, so no instance
  reads an older value from the store afterwards. The asynchronous caches get it as a future.
  - During the sweep, a mixed state is allowed: some keys removed, some not yet. Caffeine behaves the same.
- **One message instead of n.**
  - The sweep's writes are marked so that they produce no per-key messages: a flag in the row that the trigger or
    the change-stream filter skips, settled per adapter.
  - At the end, one `INVALIDATED_ALL` clears every L1. After the call returns, the other instances are cleared within
    the delivery delay, the same bound as for any write.
- **A failure partway** leaves the cache partly invalidated, and the call fails with its outcome unknown, as any
  failed write does. Another `invalidateAll` completes it; no sweep state is kept to resume.

What it gives:

- Still exact: no stale value. A racing load conflicts with its key's tombstone.
- **The hot path on every store,** no cut-off to check: MongoDB's `compute` drops to one operation.
- No extra key on store reads.
- Every store equal for `invalidateAll`; no clock, no counter.

What it costs:

- O(n) writes in the store on a rare operation, typically an admin action (after a deployment, a migration, a
  reset).
  - Reconsidered for very large caches (2026-10-26), with the sizes of future users unknown: at about 4–9 µs per row
    (measured at 100,000 rows), 10 million rows take a minute or so. An O(1) generation would not remove that work;
    it would move it, because the old rows still have to be deleted by maintenance, which is O(n) in the store as
    well. What an O(1) `invalidateAll` changes is who waits (the caller or the background) and how long the mixed
    state lasts. The total work in the store stays about the same.
  - So the sweep stays, and it is built like maintenance: in batches the core paces, so that it does not overload
    the store. The store-side bulk sweep (optional capability) keeps round trips down. An O(1) generation is kept
    as plan B, a nice-to-have for later ("Open work").
- The two-instance Lincheck test assumes an atomic `invalidateAll`. Its model changes to Caffeine's semantics: for
  example, `invalidateAll` checked only together with operations on one key, or modelled as a series of removals.

The SPI after this decision:

- Required: `read(keys)`; `write(entries)` with "if the version is X / if absent / unconditionally", returning
  applied or conflict; `delete(keys)` for the purge; a way to enumerate a cache's keys in batches, for the sweep.
- Optional capabilities:
  - a native feed;
  - a scan by time, for the polling feed and gap repair;
  - server time;
  - a global counter, only as an optimisation of the scan's order;
  - a bulk sweep done in the store (SQL `UPDATE … WHERE` in batches, MongoDB's `updateMany`), instead of the core
    enumerating and writing.

**Accepted the same day: L1 is stale for at most the delivery delay** (in `NONE`, until expiry), as the distribution
modes already work.

- That is the best a cache with a local copy can do without slowing writes. Attiya and Welch (1994): with a message
  delay d, a linearizable register cannot make both reads and writes faster than d.
- Closing the window would cost one of these:
  - every write waiting for every instance's confirmation, or for their leases to run out;
  - reads from the store (`store().get`);
  - for one's own writes only, session tokens (an option for later).
- Others (JetCache, Redisson's local cached map, Hazelcast's near cache) have the same window in the normal case,
  but they leave it under lost messages and racing loads, often until the TTL. Holding the bound under races and
  failures too is what sets this library apart.

**Why.** The library should stay open to many stores, and new ones keep coming. So either fallbacks, or one solution
that fits all, and either way a thin SPI. The answer: **the store only does compare-and-set on a version it does not
interpret; the core does everything else, once, for every store.** The model built for stores without a global
counter becomes the default. The global counter becomes an optional upgrade.

**The SPI.** Required, which every key-value store can do:

1. `read(keys)`: the rows as stored (value, state, version, time). The control row is one more key in the same
   batch.
2. `write(entries)`: per entry, "write this row if the stored version is X / if absent / unconditionally". It returns
   applied or conflict, nothing else.
3. `delete(keys)`: for the purge.

The store does not interpret versions: no `+1`, no returns, no rules for visibility. MongoDB, DynamoDB (condition
expressions), Cassandra (lightweight transactions), Redis (Lua or `WATCH`), etcd and every SQL database can do this.

Optional capabilities, each an upgrade:

| Capability                                              | Gain                                                                                                                                                                                 |
|---------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| a native feed                                           | `INVALIDATE` and `UPDATE` without polling                                                                                                                                            |
| a scan by time                                          | the polling feed, gap repair                                                                                                                                                         |
| server time                                             | the cut-off from the store's clock instead of the instance's                                                                                                                         |
| a global counter (sequence, cluster time, `rowversion`) | an exact `invalidateAll`, without the wait and without an assumption about skew                                                                                                      |
| checks the cut-off when writing                         | the hot path: `compute` on a value held in L1 in one operation, a decision under an older cut-off refused. Without it, `compute` reads first (two operations), as MongoDB does today |

**The core.**

- **Versions:** (incarnation, counter), computed by the core.
  - A conditional write sends `expected` and `(inc, c + 1)`.
  - An insert, or a `put` on a key not held, sends `(new random incarnation, 1)`.
  - A `put` on a key held writes conditionally on its known version.
  - The core always knows what it wrote, so nothing is returned.
  - Across incarnations the order is unknown, so L1 acts conservatively: a message removes what is held, a
    reservation counts as poisoned, a read from the store wins.
- **The control row:** the record of the last `invalidateAll`, holding the cut-off.
  - It is written only by `invalidateAll`, as one write whatever the cache's size. Normal writes never touch it.
    Rows the cut-off hides are purged later by maintenance.
  - Its value is the time plus ε, with a wait of 2ε, or a counter value with the global counter capability.
- **Visibility, decided in the core.** A row is visible if it was written after the cut-off and its basis (the
  cut-off its decision was made under, a field of the row) is not older.
  - The control row is read with every batch from the store, so that a read from the store never returns a value
    just invalidated by another instance whose `INVALIDATED_ALL` has not arrived yet.
  - The SQL adapters can join it into the statement, as today. Filtering in the store is only an optimisation.
- **Reservations, the epoch, the store guard:** as decided.

**What it solves.**

- **MongoDB's bulk writes in one round trip:**
  - conditional: `updateOne({_id, v: expected}, {$set: {v: new, …}})`;
  - insert: an upsert, where a duplicate key means conflict;
  - unconditional: `replaceOne` with upsert.
  - No read back, no marker.
- DynamoDB's batches, Cassandra, Redis: the same pattern, without special cases.
- A new store implements three methods, and adds capabilities when it has them.

**What it costs.**

| Cost                                                                                                                         | Weight                                                                                 |
|------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| a `put` on a key not held starts a new incarnation, so its order against other writes is unknown, and L1 acts conservatively | low: a hot key is usually held, and then the `put` is conditional on the known version |
| without the global counter, `invalidateAll` waits 2ε and is exact only while the measured skew stays within ε                | only on the rare operation; PostgreSQL and MongoDB declare the counter                 |
| without the cut-off check, `compute` reads first                                                                             | as MongoDB today                                                                       |
| the control row read with every batch                                                                                        | one more key in the same round trip                                                    |
| the refactor: versions move from the store to the core                                                                       | high, once                                                                             |

**Compared with the designs below.**

|                                             | One number from the store                   | A thin SPI, versions in the core                   |
|---------------------------------------------|---------------------------------------------|----------------------------------------------------|
| what a store must do                        | count, return, filter                       | compare-and-set on a version it does not interpret |
| returns                                     | mandatory (MongoDB's bulk: two round trips) | none                                               |
| stores without a global counter             | a fallback, a special case                  | the normal case                                    |
| exact `invalidateAll` without an assumption | everywhere with a counter                   | where the counter capability exists                |

The one-number design and its fallback below remain as the reasoning, and as what the global counter capability
gives.

### What a store must deliver

1. **A batch read** by key, returning per row: key, value, state, version, write time; and the store's current time.
2. **An atomic upsert with a per-key counter:** a new row gets the store's time (microseconds) as its version, an
   existing one `version + 1`; the version comes back. One statement on PostgreSQL (`ON CONFLICT`), MySQL
   (`ON DUPLICATE KEY UPDATE`, the version through `LAST_INSERT_ID(...)`), Oracle and SQL Server (`MERGE ...
   RETURNING` / `OUTPUT`), MongoDB (`findOneAndUpdate`, an update pipeline), DynamoDB (`UpdateItem` with
   `if_not_exists` and `ReturnValues`, the start value from the client), Redis (a Lua script).
3. **Conditional writes and deletes on the version** (compare-and-set): `UPDATE ... WHERE version = ?`, an insert
   that fails on an existing key; every store above has them (Cassandra through lightweight transactions).
4. **Read-your-writes across instances once a write is confirmed, and durability:** guarantees of the store that no
   instance can repair (MongoDB: causal sessions, majority journaled writes).
5. **The write time set by the store** in the write itself, for ages and for `invalidateAll`.
6. **Indexed queries for maintenance:** by state and write time.

**Can,** used where available: a monotonic counter (a PostgreSQL sequence, `$$CLUSTER_TIME`) instead of the clock for
the start value; a change feed. **Not native expiry (TTL):** it deletes hard and silently, so holders are not told
and loads in flight do not conflict; expiry stays a soft delete by maintenance (decided 2026-10-24).

**Why the counter starts at the store's time:** a key recreated after its row was deleted must get a version above
every earlier one. Otherwise a holder still holding version 5 would take the new row's version 1 for an older one and
serve its copy for good, and a load that read version 3 could, after a deletion and recreation, write conditionally
on a *different* row that reached version 3 again (ABA). The start value closes both; `+1` keeps every later write
of the key strictly ordered without any clock. Instances never make versions; their clocks are not used at all.

**Built 2026-10-24 (step 3):** all three stores start a new row at the store's time and add 1 for every later write
(PostgreSQL `INSERT ... ON CONFLICT DO UPDATE SET version = r.version + 1`, MongoDB `$cond` on a missing version in
the update pipeline, the in-memory store likewise); a contract test covers a key written again after size pruning
removed it silently, and a mutant starting new rows at 1 makes six tests fail. **No monotonic counter for the start
yet:** MongoDB cannot turn `$$CLUSTER_TIME` into a number inside an update pipeline before 5.1 (`$tsSecond`,
`$tsIncrement`), and 4.4 is supported; a PostgreSQL sequence needs a schema change (marker 2). Both stay candidates
for closing the clock residual below.

### Deletes

- **Invalidation:** a tombstone, announced (`INVALIDATED`), deleted after the grace period (5 minutes), so that loads
  in flight conflict and holders are told.
- **Expiry:** soft (`EXPIRED`, announced, with the store's time as its new write time, so that the grace period
  counts from the expiry), deleted after the grace period.
- **Size pruning: silent and immediate, no stubs** (decided 2026-10-24). A holder's copy is still the last value
  written; a later write of the key starts above it (the store's time), so its message is newer and the copy is
  dropped; a load in flight finds no row and conflicts; a revalidation finds nothing and drops the copy. Nothing piles
  up with high-cardinality keys.

### Distribution: optional

- **Without a feed** (`NONE`): everything above holds; other instances' copies age out, as today.
- **Native feeds** stay as they are; their contract (ChangeFeed points 1-7) is unchanged. `INVALIDATED_ALL` is the
  control key's change.
- **A generic polling feed** for stores without one: an indexed query of rows written since the last poll, minus an
  overlap of the store timeout (a write still in flight cannot take longer before it fails, so nothing commits
  later with an earlier write time); messages from the rows' states (`WRITTEN`, `INVALIDATED`, `EXPIRED`), the
  control key's change as `INVALIDATED_ALL`; duplicates are harmless (the version rule), a failed poll is a gap.
  Latency: the poll interval; cost: one indexed query per instance and interval. Needs an index on the write time per
  cache.

### Two levels

1. **The contract:** operations with guarantees, phrased so that a store with only the primitives above can meet
   them.
2. **A generic base implementation** builds the operations from the primitives (upsert, compare-and-set, batch read,
   scans). A plain JDBC or key-value adapter implements only the primitives; PostgreSQL and MongoDB override the
   operations with their native statements (one round trip each, `NOTIFY` in the same statement, the feed).

### Adapter setup

For MongoDB and PostgreSQL; Redis, MariaDB and MySQL follow the same rules in their own sections ("Redis adapters",
"MariaDB and MySQL").

**Layout and indexes.**

- **MongoDB: the discriminator is part of `_id`, as a subdocument `{d: discriminator, h: hash}`.** With several caches
  in one collection, an `update` event must be dispatched to its cache, but unchanged fields are left out of
  `updatedFields` and `documentKey` holds only `_id`; only `UPDATE_LOOKUP` would supply the discriminator, a lookup
  per event. With the discriminator in `_id` every event's `documentKey` carries it, and upserts by `_id` are
  atomic, so the duplicate-key retry disappears. Point reads filter on `_id` (always built in the same field order);
  one index `{"_id.d": 1, state: 1, writeTime: 1}` serves expiry, size pruning and tombstone cleanup. The reasons
  against it in earlier layouts (memory note `upsert-duplicate-key-retry-cost`) do not apply: no `ObjectId`
  documents (fresh tables), no separator ambiguity (a subdocument, not `"hash@discriminator"`); a slightly
  larger `_id` index remains, accepted.
- **PostgreSQL:** primary key `(discriminator, id)` and one index `(discriminator, state, write_time)` for the same
  three queries; no partial indexes.

**Setup without DDL rights.** The documentation carries the complete DDL (table, indexes and the schema marker
`COMMENT ON TABLE ...` for PostgreSQL; indexes and the marker document for MongoDB) for setups where a migration tool or
a DBA creates the schema. On start the library checks the marker and the table's shape (columns and types): matching →
used; missing and not creatable → refuse with an error pointing to the DDL. Missing indexes only warn, since they cost
speed, not correctness. (The library once created the table, falling back to an existing one when `CREATE` was refused.)

**Replicas: reads from the primary.**

- PostgreSQL refuses standbys: `pg_is_in_recovery()` once per physical connection (a standby lags, so its
  reads would contradict the version rules; writes and `LISTEN` fail there anyway).
- MongoDB forces read preference primary and read concern `majority` on the library's own operations, whatever the
  user's client defaults to (measurement 2: a holder must not read writes that may still be rolled back). Writes stay
  `majority`. Start fails fast if `majority` read concern is not supported.
- MongoDB reads are also **causally consistent**: every operation of a store runs in a causally consistent session
  advanced to the latest cluster time the store knows of (its own operations', and that of every change event,
  recorded before the event's message is handed to the cache), so a member answers only once its majority snapshot
  includes everything the cache already saw. Found by the rollback test on MongoDB 5: a member that rolled back and
  was elected primary again answered majority reads from a snapshot older than a majority-committed write whose event
  the cache had already received, and the cache would have held the older value for good. A store also seeds its
  cluster time at creation with a majority write of its own (a start time on the control document, which announces
  nothing): the primary acknowledging it has a majority snapshot including everything majority-committed before, so
  even a cache that has seen nothing yet cannot read older than that.

**PostgreSQL probe:** on start the listener notifies itself and waits for it; if it does not arrive, `build()` fails
("the listener connection received no notification (transaction-mode pooler?); listen through a data source that
reaches the database directly or through a session-mode pooler (`withListenerDataSource`)"). A silently dead
connection is found by the core's heartbeat (it was a periodic self-notification of the listener until the core
supervised the feed).

**MongoDB: sharded collections are refused**; a sharded cluster through `mongos` with an unsharded collection
works. The composite `_id` could become the shard key later; out of scope for now.

**Names:** mandatory (PostgreSQL schema and table, MongoDB database and collection; the discriminator optional,
`default` if not given, and not blank). A default name would hide where the data lives.

- **User-given PostgreSQL names** (schema, table) over 63 bytes are rejected with an error: PostgreSQL would
  truncate them silently, and shortening them with a hash would leave the user unable to find the table under the
  name they gave. They are quoted, so case is kept as given.
- **Derived PostgreSQL names** (indexes, notification channels) are shortened with a hash when over 63 bytes;
  channel names derive from schema, table and discriminator.
- **MongoDB database and collection names** are checked against MongoDB's limits (namespace length, forbidden
  characters), with a clear error at `build()`.

### Schema markers and the reset

**Names: given by the user.** The place of a cache is a mandatory adapter parameter (PostgreSQL schema and table,
MongoDB database and collection, a Redis keyspace, a MariaDB or MySQL table); only the discriminator has a default.
So there are no default names that two layouts could collide on. What users do when moving from 0.x is in
the adapters' READMEs.

**A schema marker per place,** one string on every store (`StoreMarker` in the SPI, decided 2026-11-20): a table comment
on PostgreSQL, MariaDB and MySQL, the `schema` field of a metadata document on MongoDB (a string `_id`, which never
equals the `{d, h}` subdocuments of rows, filtered out of reads and change streams), the marker key's value on Redis.
Each store keeps it in the place's own metadata where it has some: a table comment, created with the table (one
statement on MariaDB and MySQL, one transaction on PostgreSQL) and out of reach of row operations (`DELETE`, `TRUNCATE`,
scans, counts); otherwise in a reserved entry, which reads and feeds filter out and which a race at creation and a
user's delete must be handled for (MongoDB has no metadata of a collection to hold it, a Redis keyspace is no
container). It is the library's name, then `key=value` pairs: `distributed-caffeine schema=1 hash=xxh3-128/1`, the
layout and the hash format of the row ids (see "The stored hash format"), which change independently. One line, values
of letters, digits and `-./`, written in a fixed order, parsed by a few lines of the SPI; a later version adds pairs and
never changes the meaning of one. `java.util.Properties` was weighed and not taken: a pair per line, `store()` writes
the date and no fixed order, an escaping language of its own, and the library's rules (the name, the order of versions)
are ours anyway.
The numbering starts at 1 (set back on 2026-11-05: the numbers 2 and 3 named layout changes during development, none
released); a later layout change counts up from there.

- On start: nothing there → create the place and its marker; a matching marker → use it; a place without a matching
  marker → **refuse to start** with a clear error, naming the remedy (stop the instances of the other layout, then
  drop it or configure another name, or `withSchemaReset()`).
- **Opt-in `withSchemaReset()`** drops and recreates the place when no matching marker is found. Concurrent starts are
  serialized with an advisory lock on PostgreSQL; on MongoDB the drop is idempotent, and creating the collection plus
  inserting the marker is safe to race.
- **The newer version wins (decided 2026-11-20):** versions are ordered by the schema, then by the hash format's
  revision (the number after its `/`); a marker this version cannot order or read (another algorithm of the same
  revision, a pair it does not know) counts as later. The reset drops a place of an **earlier** version (a marker of a
  lower order; the markers without pairs of the versions before the hash format, `distributed-caffeine schema 1` and
  Redis's `1`; a table or collection without one but of the earliest layout: MongoDB a document with a top-level
  `discriminator`, or one of the current layout whose marker was lost, `_id.d`; PostgreSQL no comment and the columns
  `discriminator`, `hash`, `status`, `timestamp`) or an empty one. A place of a **later** version is never dropped,
  also with the option, so that two versions left with it on cannot drop each other's place in turns; the older one
  fails, naming the later marker, and going back means dropping the place by hand. Foreign data is refused, with or
  without the option. Redis and MariaDB/MySQL have no reset: they refuse a place of another version, naming whether
  it is earlier or later. Tests: `StoreMarkerTest` (the order), `MongoAdapterTest` and `PostgresAdapterTest`
  (`a_..._of_an_earlier_version_is_refused_unless_it_is_reset`, `a_reset_never_drops_...`).
- **Why not drop automatically:** instances of the other layout still running on the place fail (errors on
  PostgreSQL, an `invalidate` event ending their change streams on MongoDB) and may recreate it, so the two layouts
  would drop each other's place in turns; caches sharing the place through the discriminator would be hit too;
  concurrent new instances would race; a foreign table of the same name could be deleted; and the application's user
  often has no `DROP` permission.

**Instances of 0.x and of this layout do not see each other.** Different places and messages: an invalidation in one
does not reach the other. A bridge is out of scope: it would bring the old protocol back. A rolling update on the
same MongoDB collection with `withSchemaReset()` was tested (2026-11-05, `MigrationExperimentTest`, the published 0.7.0
in a class loader of its own against MongoDB 8): the first new instance drops the old collection; the old instances
still running resume their change streams and go on among themselves (their documents filter on a top-level
`discriminator`, which new ones lack); the new ones ignore old documents (an ObjectId `_id`, no `{d, h}`); neither
sees the other's writes. An old instance starting after the reset fails: the new layout has two documents at once
(marker and start document), so the old unique index on `(discriminator, hash)` finds two `null` pairs and its build
fails (E11000), creating nothing; the new instances are unaffected. So going back to 0.x needs the collection dropped
first.

**The leftovers of 0.x removed on every start (MongoDB, built 2026-11-07, temporary):** each `build()` of a MongoDB
cache, with or without `withSchemaReset()`, deletes the documents with an ObjectId `_id` after the marker check
(`MongoSchema#removeOldGeneration`). Only 0.x documents have one (the server assigns it to their upserts; the current
layout's are the marker's string and `{d, h}` documents). The built-in `_id` index bounds `$type: "objectId"` to the
ObjectId range: `explain` on MongoDB 4.4 and 9 with 100,000 new and 1,000 old documents examined the 1,000 old ones
only, and no key once they were gone, so a start costs one index probe. Considered first: the cleanup in the reset
(finds nothing: the drop has just removed everything, the leftovers come later) and in maintenance (a run forever for
a one-time migration). Running old instances lose their store copies, as with the drop, so no grace period; a start
after the rollout removes what the last old instances wrote. Best effort: a failure (a timeout after a long rollout)
only warns, the next start goes on. Deletes are outside the change stream's filter, so no new cache hears of them;
0.7.0 instances keep their local copies (`MigrationExperimentTest`, and `MongoAdapterTest` without the release). To be
removed once no rollout from 0.x is expected (listed under "Open work").

### Supported store versions

Newer store features are used whenever they simplify the design; old MongoDB and PostgreSQL versions are dropped
without hesitation. Concretely so far: **MongoDB 4.4 or later** (server-computed values in `find` projections; 4.2 is
end of life anyway), **PostgreSQL 9.6 or later** (decided 2026-10-22). PostgreSQL: the floor is what the adapter
needs, not what PostgreSQL still supports. It needs `jsonb` (9.4), `INSERT ... ON CONFLICT` (9.5) and
`to_regclass(text)` (9.6; 9.5 has only a `cstring` variant, and below 9.5 the writes would need a rewrite). The
others, with their reasons in their sections: Redis 6.2, Valkey 7.2, Dragonfly 1.4, Kvrocks 2.16 ("Redis adapters"),
MariaDB 10.2 and MySQL 5.7 ("MariaDB and MySQL"). The README's Requirements list them with the drivers' minimums.

### MongoDB write concern: majority, journaled

The library writes with majority and `j: true`, always (since 2026-10-23, see ordering rule 7: the store contract
requires durable confirmed writes). There is no w:1 option. Until then it left journaling to the server's
`writeConcernMajorityJournalDefault`, and the README was to recommend `false` for faster writes and propagation; the
measurements below are kept for that history and for a possible opt-in later.

Measured (`MongoWriteConcernExperimentTest`, MongoDB 5.0.31, 6.0.28, 7.0.45, 8.3.13, 9.0.4, single-node replica set,
1,000 sequential writes to one hot key per variant, each variant twice; 8.3 shown, the others alike):

| Variant                                              | Writer's ack p50 / p99 | Event at a watcher p50 / p99 |
|------------------------------------------------------|------------------------|------------------------------|
| majority, journal default `true` (MongoDB's default) | 546 / 1,641 µs         | 590 / 1,687 µs               |
| w:1, journal default `true`                          | 168 / 332 µs           | 52,174 / 103,886 µs          |
| majority, journal default `false`                    | 196 / 309 µs           | 244 / 404 µs                 |
| w:1, journal default `false`                         | 198 / 300 µs           | 246 / 377 µs                 |

- **w:1 makes propagation about 100 times slower.** Change streams deliver only majority-committed writes, which
  with journal default `true` means journaled. Without a majority write nothing forces a journal flush, so events
  wait for the periodic one (every 100 ms by default). This corrects the earlier assumption that w:1 merely does
  not speed propagation up; a w:1 option (and its safeguard of waiting for the writer's own message) would slow
  down exactly what a distributed cache needs.
- **Journal default `false` gives the writer the speed of w:1 and delivers events faster than today**: majority is
  then reached in memory, the journal no longer sits in the path.
- **Its cost:** acknowledged majority writes can be lost if a majority of nodes crash at the same time (on a single
  node: any crash, up to the writes of one journal interval, about 100 ms). Peers may already have received
  messages for those writes; the crash test below shows they revalidate. It is a server setting, so the user
  decides, and managed services may not allow changing it.
- No warning about the setting appeared in the server log.

Crash test (`MongoCrashExperimentTest`, MongoDB 8.3, single node, `mongod` killed with `kill -9` after about 1,000
majority writes and restarted on the same data, three rounds per setting):

| Journal default | Round | Last acknowledged     | Stored after restart | Acknowledged lost | Events delivered for lost writes | Change stream afterwards                                                                                  |
|-----------------|-------|-----------------------|----------------------|-------------------|----------------------------------|-----------------------------------------------------------------------------------------------------------|
| `false`         | 1     | 1,052                 | 832                  | 220               | 220                              | error 280 `ChangeStreamFatalError` ("resume token was not found"), resuming from the last token fails too |
| `false`         | 2     | 1,071                 | 1,070                | 1                 | 0                                | resumes silently, next event arrives                                                                      |
| `false`         | 3     | 1,051                 | 659                  | 392               | 391                              | error 280, as in round 1                                                                                  |
| `true`          | 1–3   | 1,018 / 1,018 / 1,021 | same                 | 0                 | 0                                | resumes silently                                                                                          |

- **Peers are protected by the gap path:** whenever events for lost writes had been delivered, the resume token
  points into the lost part of the oplog and the stream fails with error 280, so the adapter reports `onGap()` and
  the peer revalidates its L1: the lost versions it holds no longer match the store and are dropped (contract
  point 4).
- **The writer had a hole (round 2):** no event for the lost write was delivered, the stream resumed silently,
  nobody reconciled, and only the writer held the lost value. Closed by ordering rule 7 then, by journaled writes
  now.
- **Former README recommendation:** journal default `false` for faster writes and propagation, at the cost of losing
  up to the writes of one journal interval per crash of a majority of nodes. Withdrawn: the adapter writes
  journaled.

3-member replica set (`MongoReplicaSetExperimentTest`, MongoDB 5.0.31, 8.3.13, 9.0.4, all members on one Docker host,
same writes and variants, each twice; 8.3 shown, 9.0 alike, 5.0 slower across the board):

| Variant                           | Writer's ack p50 / p99 | Event at a watcher p50 / p99 |
|-----------------------------------|------------------------|------------------------------|
| majority, journal default `true`  | 725 / 1,119 µs         | 801 / 1,260 µs               |
| w:1, journal default `true`       | 361 / 569 µs           | 1,342 / 2,590 µs             |
| majority, journal default `false` | 545 / 867 µs           | 637 / 930 µs                 |
| w:1, journal default `false`      | 377 / 597 µs           | 830 / 1,855 µs               |

- **w:1 still delivers events later than majority** (about 1.7 times), though no longer 100 times: the secondaries'
  replication moves the majority point along without a periodic journal flush. The decision against a w:1 option
  stands.
- **Journal default `false` gains much less with three members:** about 25 % on the writer's ack and 20 % on the
  event, against about 2.8 and 2.4 times on a single node. With replication in the path, the journal is no longer
  the main cost. Across a real network, replication time grows and fast disks make the journal cheaper, so the
  gain there is likely smaller still; Docker Desktop's slow fsync inflates it here.
- **So the README recommendation is graded:** worth it mainly for single-node deployments; for replica sets a
  moderate gain that has to be weighed against losing acknowledged writes when a majority crashes.

### The MongoDB adapter

*Built 2026-10-22.*

`io.github.oberhoff.distributedcaffeine.adapter.mongodb`, using nothing of the library but the SPI (enforced by
`ArchitectureTest`):

```java
// names checked against MongoDB's limits
MongoAdapter<Key, Value> adapter = MongoAdapter.newBuilder(mongoClient, databaseName, collectionName)
        .withDiscriminator("orders")  // optional, "default"
        .withSchemaReset()            // optional
        .build();
```

- **Documents:** `_id: {d: discriminator, h: hash}`, `key`, `value` (by format: `BinData`, string, BSON parsed from
  the JSON), `state`, `version`, `writeTime` (the primary's `$$NOW`); one index `{"_id.d": 1, state: 1, writeTime: 1}`;
  the schema marker and a start document per cache with a string `_id`. Serialized keys and values enter update
  pipelines as `$literal`, so a value such as `"$set"` is never read as a field path or operator (a test covers it).
- **The control row** of `invalidateAll` and the marks (`Store.CONTROL_HASH`) is a document like any other: the core
  writes it and reads it with the keys and compares the ages of one read, all from one `$$NOW`; maintenance never
  touches reserved documents and leaves the documents it hides to `pruneTombstones`. Until 2026-10-24 `invalidateAll`
  swept every document with `updateMany`; from 2026-10-24 to 2026-10-28 every write also carried a stamp from
  `$$CLUSTER_TIME` (see History).
- **Writes:** a single write assigns its version under the document's lock and returns it (`findOneAndUpdate` with an
  update pipeline: `max(stored + 1, floor, min(proposed, floor + 1 min))`, the floor the server's milliseconds in the
  core's format), so a racing advance is never refused; a batch is one unordered bulk write of `updateOne` upserts at
  the proposed versions, one round trip, whose outcome MongoDB reports per write (a duplicate key on "missing" is the
  conflict). See "Versions applied as well as each store can". Size pruning deletes the documents beyond the newest
  `maxSize`, each conditional on the version it had when found.
- **One change stream per client and database**, filtered on the server to the subscribed collections and to inserts,
  updates and replaces, values and keys projected out; a cache joining with another collection reopens it from the
  resume token (widening), and when the last cache of a collection (all discriminators) closes, the watching thread
  reopens it the same way without that collection (narrowing; closing does not wait for it, and a cache joining the
  collection again before the reopen finds it still covered). A lost connection is `onLost()`, and the stream reopens from the token when a cache resumes it (a gap for that
  cache, as every resume);
  errors 280, 286 and 136
  are a gap, whether the server reports them when opening or resuming the stream or on a getMore of the open one
  (CappedPositionLost there arrives as a query exception; taken for an interruption, it cost a failed resume
  before the gap). Every operation of the watcher answers within 10 s, so a getMore on a network that silently
  stopped carrying fails and the stream resumes on a new connection, instead of blocking for good. Tested with a
  real oplog rollover (1 MB oplog, a checkpoint every second, no snapshot history kept: otherwise MongoDB holds the
  truncation back for minutes). Dropping or renaming a subscribed collection removes every document without a write to announce it, so
  the stream also carries those events and reports them as a gap to the caches of that collection; dropping the
  database ends the stream (an invalidate event), which reopens fresh, a gap for every cache. A cache joining with
  another collection while the stream recovers cuts the backoff short and waits for
  that attempt (failing if it fails), as on PostgreSQL. Documents of no cache in the collection (an unknown state, a
  discriminator that is no string) are ignored by the stream; an event that still cannot be dispatched is a gap, and
  should the watching thread ever end unexpectedly, every cache is interrupted rather than left trusting it. Reads of
  such documents fail as a `StoreException`, like every unexpected failure of either adapter.
- **Found by the property test on MongoDB 4.4 (about one run in four):** a stream opened without a token starts at
  the latest *applied* write, while the reads use the *majority* snapshot; a write applied but not yet
  majority-committed when the stream opened was then in neither (lost to the stream as before its start, invisible
  to the revalidation read after the gap). Not the server's fault in isolation (`MongoFreshStreamExperimentTest`: 0 of
  200 writes after a fresh open lost on 4.4 and 8). **Fix:** a fresh stream (first open and after a lost history)
  starts at the operation time of a majority read in a causally consistent session (`startAtOperationTime`), and so
  does the per-subscription "since" filter (a shared stream may still carry events of writes before a cache
  subscribed): every write outside that snapshot is in the stream, every write inside is visible to the following
  reads; at worst an event arrives twice. 0 of 20 runs on 4.4 afterwards.
- **Refused at start:** standalone servers, deployments without majority read concern, sharded collections (a sharded
  cluster through `mongos` with an unsharded collection works).
- **Tested** on every topology of "Test setup" (single-node and three-node replica sets, with an arbiter, a sharded
  cluster, a 1 MB oplog for a real rollover): the contracts, the adapter's own tests, failover, the property test; the
  change stream's widening, narrowing and resuming were mutation-checked (resuming, widening or narrowing without the
  token needed the event-counting tests, and narrowing only after a lag hook held the watching thread back, since a
  thread that is never behind loses nothing even when it reopens fresh). Versions: see "Supported store versions".

### The PostgreSQL adapter

*Built 2026-10-22.*

`io.github.oberhoff.distributedcaffeine.adapter.postgresql`, using nothing of the library but the SPI (enforced by
`ArchitectureTest`):

```java
// names mandatory, at most 63 bytes, quoted
PostgresAdapter<Key, Value> adapter = PostgresAdapter.newBuilder(dataSource, schemaName, tableName)
        .withDiscriminator("orders")                 // optional, "default"
        .withListenerDataSource(listenerDataSource)  // optional: behind a transaction-mode pooler
        .withSchemaReset()                           // optional
        .build();                                    // the timeout is the cache's (see Settings)
```

- **A full NOTIFY queue** (a session listening in the database, any application's, left in a transaction) makes
  PostgreSQL refuse every write that notifies at its commit: nothing is written unannounced, and the write fails as
  certainly not committed, with a message naming the cause and how to find the session. Open: PostgreSQL 18 kept
  refusing after that session ended, in every recovery attempted (another session listening anew, VACUUM, both;
  the queue's usage already back near 0); only no listening session at all and a VACUUM freed it. Nothing the
  adapter could do on its own did so reliably.
- **The table is the library's; changing it by hand is not supported** (decided 2026-10-24). A `TRUNCATE` is not
  announced, and a table dropped under the caches fails every statement with a clear, non-retryable error until a
  cache is built again, which creates it. Until then a statement trigger announced a `TRUNCATE` as invalidateAll on a
  channel of the whole table (a second subscription per cache), and the first statement missing the table created it
  again; dropped because operations the library does not initiate cannot be covered fully anyway (an `UPDATE` or
  `DELETE` by hand announces nothing either), and each case cost DDL, a channel and a code path. MongoDB keeps
  treating a dropped or renamed collection as a gap: its change stream reports it at no cost.
- **Every statement waits at most the timeout** (the connection's network timeout, set per statement and restored
  for the pool): a connection the network silently stopped carrying would otherwise block its caller for good, since
  pgjdbc has no socket timeout by default. A timed-out statement is not retried (another attempt would most likely
  wait as long again); a timed-out write has an unknown outcome. The listener's JDBC probe is bounded by the probe
  timeout the same way.
- **Listening goes through the `DataSource`** (built 2026-10-28, see "PostgreSQL listening: pgjdbc's stream taken
  over"): one of its connections, whose pgjdbc stream the listener takes over, held while a cache listens through it and
  shared by every cache given the same `DataSource` object. With `DistributionMode.NONE` nothing subscribes and nothing
  is held. A pool's leak detection reports the held connection; at the end it is aborted and closed, which hands it back
  to a pool as broken (HikariCP evicts it). A transaction-mode pooler cannot carry `LISTEN`, which the start's probe
  detects: such a setup gives the adapter a listener `DataSource` of its own (`withListenerDataSource`), reaching the
  database directly or through a session-mode pooler. Later `LISTEN`s and `UNLISTEN`s go over the taken stream as simple
  queries, answered through the listener's thread; an `ErrorResponse` ends the session as a loss.
- **Table:** `(discriminator, id, key, value, state, version, write_time)`, primary key `(discriminator, id)`, one
  index `(discriminator, state, write_time)`, the marker as table comment; `key` and `value` typed by the formats
  (`bytea`, `text`, `jsonb`), so a table refuses caches with other serializers, naming the column. `key` is nullable:
  a tombstone for a key never written has no key. Created with its schema under an advisory lock; without the right
  to create it, the start fails with the complete DDL in the message.
- **Table:** besides the columns above, a version sequence of its own (created with the table, checked at start).
- **Statements:** every kind of write in one statement per chunk of 1,000 rows, so that a batch mixing them is one round
  trip: advancing writes as an upsert (never refused), writes conditioned on a version as an `UPDATE` matching it,
  writes conditioned on no row as an `INSERT ... ON CONFLICT DO NOTHING`, each one atomic step per key, in hash order
  (concurrent batches lock in the same order). Every write applies at a version assigned under the row's lock,
  `GREATEST(stored + 1, nextval())`, the proposed version ignored: the sequence is drawn by every write, so it lies
  above every version of the table, and a row created anew starts above whatever an instance still holds. The write time
  is the server's `now()`, which the control row's is compared with. Every row written is announced (`pg_notify` over
  the rows the CTE returns, payload `kind|hash|version`) and returned at its version in the same statement; the ids not
  returned conflicted. Size pruning keeps the newest `maxSize` values by `OFFSET`, so overlapping runs on several
  instances never prune more. Until 2026-10-24 `invalidateAll` gave every row a new version; until 2026-10-28 every read
  also returned a cut-off stamp (see History).
- **Retries** as decided (SQLState `40P01`, `40001`, `55P03` and a failed `getConnection` retried; `08...` during a
  statement: unknown outcome, retried except for `put`); the standby check once per physical connection
  (`PGConnection` unwrapped from the pool's proxy).
- **Listener:** one session per listening `DataSource` (the adapter's, or `withListenerDataSource`), a channel per
  cache (`dc_<schema>.<table>.<discriminator>`, shortened with a hash past 63 bytes), dispatched by channel; a probe
  notification sent through JDBC must arrive within 5 s on every subscription (a transaction-mode pooler fails the build
  with the message decided above); a lost session tells every cache (`onLost`) and is reconnected when the first of
  them resumes it (see Supervision), a gap for each. A subscription while the session is down (or broken before its
  loss was noticed) connects at once, so the build waits for it; a broken connection is closed without waiting. A pool
  that timed out waiting for a connection is not asked again (it waited its whole timeout already). Until 2026-10-28
  the session was R2DBC's (see History).
- **The core merges consecutive message batches while draining** (up to 1,000 messages, never across a gap or
  interruption marker): PostgreSQL delivers one notification at a time, and population reads stay batched.
- **A cache fallen too far behind** (more than 100,000 messages pending: its executor busy, its store slow) drops
  what is pending, keeps the markers in order, and revalidates as after a gap, instead of queueing without bound.
- **Tested** on every topology of "Test setup" (single server, asynchronous and synchronous standbys, PgBouncer in
  transaction mode, a small notification queue) and every PostgreSQL major from 9.6 to 18: the contracts (a gap induced
  by terminating the listening backend), the adapter's own tests (every mode and format, discriminators, async, schema
  creation, refusal, reset, other serializers, missing rights, names, a broken session degrading and revalidating, a
  connection inside the caller's transaction refused), the retry rules with injected SQLStates, failover, and the
  property test (faults: terminated session, rejected statement, connection broken after the statement committed); a
  conditional write ignoring the expected version fails the property test and the contract.

### PostgreSQL listening: pgjdbc's stream taken over

*Measured 2026-10-26: notifications after the commit, batched.*

**Why.** R2DBC brings Reactor, Netty and SCRAM (about 20 artifacts) and a second connection configuration. pgjdbc
alone hands out only about 750 notifications/s per listening session.

**Drained pgjdbc: no help.** After the first notification, a query was run to read every notification that arrived
before its response, repeated while more kept coming. Measured with an empty query and with `SELECT 1`, 4 instances:

- p50 at 208–947 ms (1,500 writes/s), 617–2,002 ms (3,000/s) and 712–3,075 ms (6,000/s);
- a hot key at 489–1,878 ms.

**Notifications after the commit, batched per writer.**

- The write's statement no longer calls `pg_notify`.
- After the commit, each writer queues its writes. It sends at most one notification per window, carrying every write
  queued meanwhile (up to 8000 bytes, about 150 writes).
- When idle it sends at once.
- The listener is plain pgjdbc.

Measured with `PostgresR2dbcListenerExperimentTest` (now `PostgresListenerExperimentTest`), PostgreSQL 18, 4
instances, 2 writers:

| Variant                         | 1,500 writes/s | 3,000 writes/s | 6,000 writes/s (reached) | hot key, 1,500/s | PostgreSQL CPU per 1,000 writes |
|---------------------------------|----------------|----------------|--------------------------|------------------|---------------------------------|
| batched, 2 ms window            | 3.7 / 7.7 ms   | 3.6 / 6.0 ms   | 3.3 / 5.5 ms (5,678/s)   | 3.4 / 5.4 ms     | 95–215 ms                       |
| batched, 5 ms window            | 5.1 / 9.1 ms   | 4.9 / 8.3 ms   | 4.7 / 8.1 ms (5,990/s)   | 5.0 / 8.6 ms     | 68–111 ms                       |
| R2DBC, `pg_notify` in the write | 0.5 / 1.7 ms   | 0.5 / 0.7 ms   | 0.5 / 0.8 ms (3,934/s)   | 0.4 / 0.6 ms     | 161–194 ms                      |

The figures are p50 / p99 arrival.

- **Latency:** a few milliseconds, which is about the window, at every rate and for a hot key. R2DBC stays at 0.5 ms.
  pgjdbc's limit no longer bites: it counts notifications, not writes.
- **Writes get faster:** writers reached about 6,000/s, against R2DBC's 3,900/s with `pg_notify` inside the write.
  Notifying is no longer part of the transaction.
- **PostgreSQL's CPU falls by up to about two thirds** (68 against 194 ms per 1,000 writes at 6,000/s). Every
  notification wakes every listening session, and there are far fewer notifications.

**What it needs: a writer's death counts as a gap.**

- A write committed and not yet announced is lost if its writer dies before sending, and nobody notices.
- So every instance tracks the last heartbeat it saw from every other instance; the heartbeats reach the others
  through the feed. A heartbeat silent longer than a timeout moves the epoch.
- The cases:
  - a crash: detected by the silence;
  - a writer cut off from the database: silent too, and it keeps its queue, sending once it reconnects;
  - a long pause: a needless but harmless gap;
  - a clean shutdown: the queue is sent first.
- The bound in that case: the heartbeat timeout (seconds), instead of the delivery delay. It holds only for the rare
  crash between a commit and its notification.
- General: it works for every store where the library sends its own messages (Redis's `PUBLISH`, own streams), not
  only for PostgreSQL.

**Not decided yet:** whether to build it, and so drop R2DBC; and the window (2 ms gives lower latency, 5 ms less
CPU; or adaptive, by the number of writing instances).

**pgjdbc's stream taken over: as fast as R2DBC (measured 2026-10-26).**

- How it works:
  - the listening connection is taken from the user's `DataSource` (HikariCP in the measurement, so through a pool's
    proxy);
  - pgjdbc runs `LISTEN`;
  - the listener then reads pgjdbc's own stream (`PGStream`) through its public methods (`receiveChar`,
    `receiveInteger4`, `receiveString`, `skip`), parsing notifications (`A`) and skipping every other message;
  - at the end, the connection is aborted (`Connection.abort`), not closed, so the pool evicts it.
- Reuse: authentication and TLS stay pgjdbc's, with whatever the `DataSource` is configured for. No bytes are lost at
  the handover, since pgjdbc's buffer is read as it is.
- Reaching the stream: `QueryExecutorBase.pgStream` is `protected`, with no getter, so it takes reflection on that
  one field. The executor is reached through `unwrap(BaseConnection.class).getQueryExecutor()`.
- Measured with the writes notifying inside their transaction, as with R2DBC:

| Listener (4 instances)   | 1,500 writes/s | 3,000 writes/s | 6,000 writes/s (reached) | hot key, 1,500/s | PostgreSQL CPU per 1,000 writes |
|--------------------------|----------------|----------------|--------------------------|------------------|---------------------------------|
| pgjdbc stream taken over | 0.4 / 0.6 ms   | 0.5 / 0.7 ms   | 0.5 / 0.8 ms (3,914/s)   | 0.4 / 0.6 ms     | 155–194 ms                      |
| R2DBC                    | 0.4 / 0.7 ms   | 0.5 / 0.8 ms   | 0.5 / 0.8 ms (3,873/s)   | 0.4 / 0.6 ms     | 156–198 ms                      |

  The figures are p50 / p99 arrival.

- So the limit was in pgjdbc's `getNotifications` path (`processNotifies`), not in PostgreSQL, the socket or the
  protocol.
- Taking over the stream gives R2DBC's latency with no dependency, no second configuration, and the exactness of
  notifying inside the transaction (no crash window, no tracking of writers' liveness).
- Costs:
  - reflection on one internal field, checked at startup with a clear error, and tested against the pgjdbc version
    in use;
  - one pool connection held per instance (to document, with HikariCP's leak detection);
  - transaction poolers still cannot carry `LISTEN`.
- Worth reporting upstream: a public way to read notifications without `processNotifies`' overhead.

**Decided (2026-10-26): R2DBC is ruled out. Sources of changes, by kind.**

| Source                                                                                        | Used                                                                                       | How                                                                                                                                                                                      |
|-----------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| PostgreSQL: `NOTIFY` inside the write's transaction, the listener taking over pgjdbc's stream | the PostgreSQL adapter's default                                                           | the listening connection from the user's `DataSource`; exact (no crash window), 0.5 ms; no dependency beyond pgjdbc                                                                      |
| a generic message bus                                                                         | stores without a native feed; or, optionally, to save PostgreSQL's CPU with many instances | the library sends after the commit, batched, numbered per sender, with heartbeats. Gap detection, including a writer's death, is in the core. A thin bus SPI: publish a batch, subscribe |
| a native feed                                                                                 | where a store has one (MongoDB's change streams, a binlog, DynamoDB Streams)               | an optional capability of the store SPI; it sees writes from outside the library too                                                                                                     |

Consequences, to build:

- **The PostgreSQL listener on pgjdbc.**
  - The adapter takes only the `DataSource`. `withListener(ConnectionFactory)` and the URL convenience go; listening
    is switched on without a second configuration.
  - The listener takes over one connection's stream.
  - At startup it checks the field it reaches through reflection, with a clear error if pgjdbc changed.
  - Its connection is aborted, not closed.
- **The pom:** `r2dbc-postgresql`, `r2dbc-pool`, and the managed Reactor, Netty and SCRAM versions go.
- **The experiments built on R2DBC** stay as recorded measurements, but their R2DBC code goes with the dependency:
  - `PostgresJdbcVersusR2dbcExperimentTest` and `PostgresR2dbcCeilingExperimentTest` are removed;
  - the listener experiment keeps the pgjdbc variants.
- **The README** documents one held pool connection per instance (and HikariCP's leak detection), and that
  transaction poolers cannot carry `LISTEN`.
- **Later:** the bus SPI and its first implementations. In-transaction `NOTIFY` with pgjdbc's own `getNotifications`
  is not offered, being limited to about 750 notifications a second.

**What follows: pluggable message buses (recorded 2026-10-26, not built).** When the library sends its own messages
after the commit, the transport no longer has to be the store. It can be a bus of the user's choice, as JetCache's
broadcast manager is: Kafka, NATS, RabbitMQ, Redis pub/sub, JGroups, a Hazelcast topic, or `LISTEN/NOTIFY` itself.

- **The bus SPI stays thin:** publish a batch of bytes, and subscribe. Optionally the bus reports a lost connection
  itself.
- **Gap detection is the core's job, whatever the bus.**
  - Every instance numbers its batches (a sequence per sender) and publishes heartbeats over the bus, carrying its
    last number.
  - A receiver that sees a number missing, or a sender's heartbeat silent, moves the epoch.
  - So even a bus that drops messages without a word (Redis pub/sub, core NATS) keeps our guarantee: no stale value
    beyond the delivery delay, and a loss always detected. That is what JetCache's broadcast lacks; Hazelcast does
    the same with its sequences per partition.
  - Order across senders comes from the versions in the messages, as now.
- **Two kinds of source, side by side:**
  - a store's native feed (change streams, triggers that notify, binlog) sees every write, including writes from
    outside the library;
  - a bus sees only the library's own writes, but is fast, cheap, and works for any store.
  - Writes from outside the library (a manual fix) reach the instances through a bus only after expiry. That is as
    with JetCache, and to be documented.
- **What it opens up:** a store without a native feed plus any bus gives every distribution mode, without the
  polling feed: MariaDB plus Redis pub/sub, MySQL plus Kafka, DynamoDB plus SNS.
- **It fits the thin SPI:** the store SPI is unchanged (a native feed stays an optional capability), and the bus is a
  second, equally thin SPI. Sequences, heartbeats, batching and gap detection are built once, in the core.
- **The source is chosen per cache, whatever the store (added 2026-10-29).** By default a store's own feed, else its
  scan (polled); a bus instead where the user configures one, also for a store that has a feed. One source per
  cache: a feed and a bus at once would work (the versions drop the duplicates) but serve no purpose beyond a
  migration. Why a PostgreSQL user would choose a bus over `LISTEN/NOTIFY`: transaction poolers cannot carry
  `LISTEN` (today a separate listener data source); `NOTIFY` goes through one queue for the whole server and
  serializes commits, and every instance holds a listening connection; a bus may already run anyway.
- **What a feed and a bus differ in.** Not in what they see, for PostgreSQL: its feed is the library's own too (the
  write and its `pg_notify` are one statement, no trigger), so a manual `UPDATE` is announced by neither; MongoDB's
  change streams do see every write. They differ in atomicity with the commit: `pg_notify` is part of the
  transaction (committed, announced), a bus publishes after the commit.
- **Open: an instance failing between its commit and its publish.** The write is stored and never announced. Sequences
  and heartbeats detect messages lost on the way, not messages never sent; the others notice the sender's heartbeat
  falling silent, but not which keys it wrote. Two ways, to be decided when the bus is built: an outbox (the message
  written in the same transaction as the write, forwarded by a relay: atomic again, at the cost of a second write
  and a relay), or revalidating what is held when a sender falls silent (no extra write, but all of it is checked,
  for every instance that stops, crashes or is merely partitioned).

### Redis adapters

*Decided 2026-10-26, settled and built 2026-11-01.*

Redis gets adapters. JetCache keeps Caffeine as L1 and nothing but Redis as L2 (Jedis, Lettuce, Redisson or Spring
Data Redis as the client), and keeps L1 consistent by Redis pub/sub on a best-effort basis: no versions, no replay, no
repair after a lost message. Redisson's `RLocalCachedMap` is the other comparison on Redis. Two points set us apart:
our guarantee on the same store, and more stores beyond it.

Sketch, to be settled against the thin SPI ("The thin SPI, versions in the core"):

- **Write and message in one Lua script:** compare the version, write the row (a hash: value, state, version, write
  time), then `XADD` to the cache's stream. The script is atomic, so the feed's order is the write order, and no write
  goes without its message.
- **The feed is a Redis Stream,** not pub/sub and not keyspace notifications (both fire and forget, no replay; see
  "The polling feed"). The listener runs `XREAD BLOCK` from the last ID it has seen, and the stream is trimmed with
  `MAXLEN ~`.
  - A gap is detected, not prevented: the stream was trimmed past the last seen ID, or its last ID is below ours
    after a failover. Either one means an epoch.
- **Durability:** replication is asynchronous, so a failover can lose acknowledged writes. That loss shows up as the
  gap above. `WAIT` is optional.
- **Cluster:** a script may only touch keys in one slot. A hash tag `{cache}` puts a whole cache
  in one slot, so the cache is not sharded. Several streams per cache (`{cache:i}`) can come later.
- **No native TTL,** as for every store (see "What a store must deliver").
- **Clients:** one internal Redis store over a small command interface (scripts by SHA, `XREAD`, `SCAN`), with one
  thin binding per client. Lettuce comes first (asynchronous, cluster support, Spring Boot's default), then Jedis and
  Redisson on demand.
- **When:** later. MongoDB and PostgreSQL come first (decided 2026-10-26). Not before the thin SPI (step 2 of the
  build plan), so the adapter is built once and against the target. The exact place in the order and a binding for
  Spring Data Redis are decided when Redis comes up.

**Settled 2026-11-01** (the sketch above stands where not changed here):

- **Layout:** per cache, in one slot (`{dc:<cache>}`): a hash per row (`…:r:<hash>`: key, value, state, version,
  write time), a sorted set of the rows by write time (`…:t`, for expiry, pruning and tombstones without a `SCAN`),
  and the stream (`…:s`). The control row and the heartbeat row are ordinary rows with the reserved `#` prefix.
- **A full `Store`,** not a `PrimitiveStore`: one Lua script per operation, so a batch is atomic and the version
  and the write time (`TIME`, microseconds) are assigned in the same step, like MongoDB's single writes (profile
  `redis`: assigning in the core's domain).
- **Trimming by age:** `XADD … MINID ~ <now − retention>`, the retention of tombstones; an instance falling further
  behind sees a gap. Not `MAXLEN`, which a write burst would turn into gaps under load.
- **Failover:** nothing beyond detecting the gap. A write lost by the failover leaves instances holding its version;
  the revalidation after the gap keeps a held value only at exactly the store's version and value
  (`InternalCore#confirmOrDrop`), so they drop it, as on MongoDB and PostgreSQL. `WAIT` is not built (see "Open work").
  To check in the failover tests: a write after the failover never gets the version of a lost one (a new primary's
  clock behind the old one's).
- **Eviction:** at start, `maxmemory_policy` from `INFO memory` (since 2026-11-01; `CONFIG GET` before, which managed
  services often block); an `allkeys-*` policy, or Dragonfly's `eviction` in its `--cache_mode`, is logged as a
  warning (rows may then vanish silently, profile `redis-hostile`), and the cache starts (user's choice: warn, not
  refuse). The requirement (`noeviction` or `volatile-*`, which never touch our keys without a TTL; Dragonfly not in
  `--cache_mode`) is documented.
- **Versions:** Redis 6.2 or later (`MINID`) and Valkey. Matrix: Redis 6.2 and latest, Valkey latest; a single server,
  a replica with Sentinel failover, a cluster with the cache in one slot.
- **Drivers:** one artifact, the drivers optional Maven dependencies (as for MongoDB and PostgreSQL), one
  `RedisAdapter` over one internal `RedisStore`, and one small entry class per driver (`LettuceRedis.of(client)`,
  `JedisRedis.of(jedis)`), so only the driver in use must be on the classpath; no overloads by client type, which
  would break compiling without the other drivers. A binding runs scripts (`EVALSHA`, `EVAL` on a miss) and the
  blocking `XREAD` on a connection of its own, and checks the server at start. Lettuce and Jedis first; Redisson and
  Spring Data Redis on demand. JetCache, for comparison (checked 2026-11-01): one artifact per client
  (`jetcache-redis` for Jedis, `jetcache-redis-lettuce`, `jetcache-redisson` since 2.7.0.M2, Spring Data since 2.6.0),
  each a full cache implementation of its own, with its own local-cache invalidation broadcast.

**Build steps** (agreed 2026-11-01), each green before the next:

1. The command interface and the Lettuce binding. **Built 2026-11-01:** `RedisDriver` (opaque, made by
   `LettuceRedis.of`), `RedisSession` (scripts by SHA with the full script once on `NOSCRIPT`, the server's
   description, stream readers on connections of their own), failures classified driver-neutrally
   (`RedisCommandException`). Each command bounded by the cache's timeout, also on a shared client; a connection that
   timed out is dropped and opened anew. Redis 6.2 wraps an error of a command a script called in its own
   ("ERR Error running script …: -OOM …"), so the code is looked for inside it too (matters for `READONLY`).
2. `RedisStore` without a feed. **Built 2026-11-01:** one Lua script per operation; versions as two exact Lua numbers
   (milliseconds, low 20 bits), since Lua's doubles hold 53 bits and a core version needs 63; every number stored
   with `%.0f`, as Lua's default keeps 14 digits; rows as hashes, indexed by write time in a sorted set of values and
   one of dead rows (reserved rows in neither); every write assigned like MongoDB's single writes; a schema marker
   (`{dc:<name>}:schema`). The store contract green on Redis 6.2, Redis 8 and Valkey 9 (a mutation of the version
   arithmetic caught).
3. The stream feed. **Built 2026-11-01:** every write and expiry appends an entry in its script (`XADD … MINID ~`,
   5 minutes, the core's grace period of tombstones), numbered by a counter the same script increments; a number
   skipped is a gap (entries trimmed before they were read, or lost with a failover), which a stream ID alone would
   not show. One reader per driver (`RedisFeed`) reads every subscribed stream with one `XREAD BLOCK` (1 s) on a
   connection of its own, its commands bounded by a fixed 10 s (as MongoDB's watcher); a subscription starts at the
   stream's position taken in one script, so it misses nothing while the reader picks its stream up. A failed read
   is a loss for every subscription; the first `resume` opens a new connection; a resume while delivering (the
   heartbeat's suspicion) replaces the connection and tells no one of a loss. The change feed contract green on all
   three images (a mutation of the gap check caught).
   **Found:** Lettuce works at least once by default: after a connection reset it reconnects by itself and sends
   again what was sent and not answered. For the reader that is harmless (the read is repeated from the same
   position), so a reset is no loss with Lettuce (the contract's loss is simulated by closing the reader's
   connection). For a write it is not: a script that ran before the reset runs again, and a conditional write then
   finds its own result and reports a conflict although it applied (an advance is applied twice, at two versions).
   A timeout closes the connection first, so only a reset between running and answering does it. **Decided and built
   2026-11-01:** a request ID per call of `write` (a UUID): the script keeps its result under
   `{dc:<name>}:request:<id>` for a minute and answers a request found there from it, writing nothing, so a write is
   applied at most once whatever the client library sends again (rather than Lettuce's client-wide `replayFilter`,
   which is the user's). A retry by the core is a new call, a new ID. Maintenance needs none (run again, it only
   maintains once more). Tested by sending a request twice; without the kept result the two replay tests fail. Under
   a `volatile-*` eviction policy a kept result may be evicted early, which only reopens this rare case.
4. The core suites on Redis. **Built 2026-11-01:** core, distribution, recovery, Caffeine parity and the property test
   (four seeds, faults: a gap, round trips slowed by a proxy) on Redis 6.2, Redis 8 and Valkey 9, beside the binding,
   the store's own tests and both contracts: 465 tests, 63 skipped (21 per image: those needing the in-memory store's
   faults, the parked repair of loads racing `invalidateAll`, the contract test of a conditional store), green twice,
   about 3.5 minutes. The backend's barrier (`RedisFeed.barrier`) passes once every subscription of the stream has
   handed its listener the stream's last entry; a subscription's position moves only after its listener ran.
5. Failover (Sentinel; a lost write, a new primary's clock behind), the cluster with the cache in one slot, the
   eviction warning. **Built 2026-11-01, without the cluster:** the eviction decision tested (an `allkeys-*` policy
   warned of, the cache still starting; `volatile-*` not). A primary, a replica and one Sentinel per image (Redis
   6.2, Redis 8, Valkey 9), each node on a free host port published as the same port, Lettuce's resources mapping
   every address Sentinel reports to `127.0.0.1` with its port. Two caches ride out a failover; a write the failover
   loses is dropped by both, and the next write gets a higher version. A write is lost by running the store's write
   script with replication switched off (`redis.set_repl(redis.REPL_NONE)`): pausing the replica's container does
   not lose it, as the kernel keeps taking the replication stream and the replica applies it once resumed (found:
   the first version of the test kept the "lost" write). Not tested: a new primary's clock behind the old one's
   (the nodes share the host's clock); a write after such a failover may get a version below a lost one, which a
   holder drops anyway (a message below its version is checked against the store, a revalidation keeps only the
   store's exact version); only an exactly equal version with another value would be kept, which needs the same
   millisecond and the same low bits on both primaries. 474 tests green, about 5 minutes.
   **The cluster deferred (2026-11-01, user's choice: later).** It adds no guarantee (a cache lives in one slot
   anyway; Sentinel covers availability), and costs: an `XREAD` cannot span slots (`CROSSSLOT`), so the shared
   reader becomes one per cache (a thread and a connection each) or one per node (following slot moves on
   resharding); a second Lettuce entry (`LettuceRedis.of(RedisClusterClient)`, another connection type); a topology
   of at least three primaries announcing reachable addresses. The adapter refuses a cluster until then.
   **Superseded 2026-11-01:** the user asked for the cluster after all; built as item 11.
6. The Jedis binding, the contract suites over it. **Built 2026-11-01:** Jedis 8 (`redis.clients:jedis`, optional),
   `JedisRedis.of(RedisClient)` and `JedisRedis.of(RedisSentinelClient)` (the current primary's pool). A command
   borrows a pooled connection, runs under the cache's timeout as its socket timeout, restored before it goes back;
   one that timed out or broke goes back broken. Commands through Jedis's `CommandObjects` for the connection's
   protocol (RESP2 or RESP3); a script's Lua false read as nil either way. The server-error classification moved to
   `RedisCommandException`, shared by both bindings. The binding tests and both contracts run over Lettuce and Jedis
   on every image. **Found:** Jedis 8 brings `slf4j-api` 1.7, which broke the tests' SLF4J 2 bridge (pinned in this
   build's `dependencyManagement`); Jedis waits for a blocking command (`XREAD BLOCK`) with no socket timeout (its
   client-wide one for blocking commands), so the reader has a watchdog disconnecting a connection that did not
   answer within block time and timeout (the first run hung there); Jedis's typed stream reads decode fields as
   UTF-8, so stream entries are text in both bindings (the store writes nothing else into them); a pool failing to
   take a broken connection back threw over the command's own failure (now given back quietly). Sentinel refused a
   failover right after another (`NOGOODSLAVE`, its view of the new replica stale): the tests ask again. All Redis
   suites: 660 tests, 66 skipped, about 6 minutes; the Sentinel suites green twice more after the fix.
7. The Redisson binding (decided 2026-11-01, built after Lettuce and Jedis, before any demand: its own retries,
   3 by default and possibly past the cache's timeout, put the request IDs to a real test). No Spring Data Redis
   binding (decided 2026-11-01): Spring Data's `RedisConnectionFactory` sits on Lettuce or Jedis, and Redisson has its
   own Spring Data connector, so a Spring application passes the client underneath (`LettuceConnectionFactory`'s
   native client, Jedis's client, or its `RedissonClient` bean). **Built 2026-11-01:** Redisson 4.8 (optional),
   `RedissonRedis.of(RedissonClient)`, a single server or one behind Sentinel (a cluster configuration refused).
   Commands through Redisson's async API, bounded by the cache's timeout and cancelled after it; scripts on the
   primary, their replies decoded by the type each script now declares (`RedisScript.Reply`: array, integer, bulk),
   which Lettuce and Jedis ignore; Redisson's typed exceptions classified directly. The contracts passed at once, so
   it decodes the nested script replies as the others do. **Found:** Redisson replaces a connection the network
   silently stopped carrying only when its own ping fails (`pingConnectionInterval`, 30 s by default), and offers no
   way to drop one: commands on it time out meanwhile, within the cache's bound (documented, a shorter interval
   advised; the binding tests wait up to 10 s for recovery and ping every second); a server it cannot reach fails as
   a timeout, as it retries past the bound.
8. The version matrix (agreed 2026-11-01), as for MongoDB and PostgreSQL: each major version's first release and its
   latest, Redis 6.2, 7, 8 and Valkey 7.2, 8, 9. A first release runs the suites where versions differ
   (`RedisCompatibilityTests`: the three bindings, both contracts over each, the store's own tests), the latest every
   suite (`RedisServerTests`) and the failover behind Sentinel, which also runs on Redis 6.2.0. Valkey 7.2.4 was never
   published as an image: 7.2.5 stands for it. Two matrix groups, Redis and Valkey, about 10 minutes each in
   parallel, green: per latest image 280 tests (23 skipped), per first release 192 (3 skipped).
9. Dragonfly, chunking and declared keys (2026-11-01). A trial on Dragonfly 2.0.2 passed everything but two things:
   tests setting `maxmemory-policy` (Dragonfly has none; its eviction is `--cache_mode` at start), and a batch of
   25,000 writes, one script of 250,000 arguments, which Dragonfly refuses (Redis takes it, and runs nothing else
   meanwhile). **Built:** reads and writes in chunks of 1,000 keys per script (a read's later chunks measure ages at
   its first chunk's instant; each write chunk a request of its own, a failure after the first leaving the outcome
   unknown). **Decided and built (option A of three):** every script declares every key it touches, as Redis
   documents and Dragonfly enforces (it refuses undeclared keys unless started with
   `--default_lua_flags=allow-undeclared-keys`, a server-wide default that would lock more broadly for all scripts):
   maintenance in two steps, the candidates read from the sorted sets, then a script per operation with those rows
   declared, each checked again before acting (still due, not hidden, still beyond the size), so it stays idempotent
   across instances; one more round trip per maintenance batch, on every server alike, which also prepares the
   cluster. Rejected: requiring the flag (C), and flagging only the maintenance scripts (`#!lua flags=…`, Redis 7
   syntax that 6.2 rejects: two versions of each script, B). **Built:** the eviction policy from `INFO memory`
   (above); Dragonfly recognised (`dragonfly_version`, supported by the Redis version it implements, 7.4). Matrix:
   Dragonfly 2.0.0 (the compatibility suites) and 2.0.2 (every suite), on its defaults, single server only (no
   `redis.set_repl` for the lost-write failover test, its Sentinel support young). Green: Redis, Valkey and Dragonfly
   groups, about 10 minutes each in parallel.
10. Benchmark and the remaining setups (2026-11-01). **Benchmark** (`RedisBenchmarkExperimentTest`, single servers in
    local containers on Docker Desktop): per store call about 0.2–0.3 ms through each library (Redis 8: a read from
    the store 0.19–0.23 ms, a put 0.26–0.29 ms, propagation to another instance 0.30 ms p50); under load Lettuce
    carries about 40 % more (8 threads, 80 % gets: 62,900 ops/s against 43,700 with Jedis and 47,200 with Redisson; one
    hot key on two instances 101,500 against 72,500 and 77,500), as it multiplexes one connection; Valkey 9 as
    Redis 8; Dragonfly 2.0.2 about a third of Redis (18,300 ops/s, `getAll` of 100 3.1 ms against 0.6, 45 µs of server
    time per script against 16–22), which this setup cannot tell from its strengths on many cores. A `putAll` of 100
    takes about 16 ms on every library and server alike: the Docker Desktop stall above (the server spends 16–22 µs per
    script on average); to confirm on Linux or a native server. **Tested since:** Sentinel through Jedis and Redisson
    (their address mappers: Jedis's `hostAndPortMapper`, Redisson's `NatMapper`), and a restart with persistence (every
    value kept, both caches going on) and without (every row lost, both caches dropping what they held at the next
    entry's restarted number, then going on), on Redis and Valkey through Lettuce. **Found:** right after a failover
    the new replica resynchronises with its link down; a test writing then and failing over again lost its own value
    (the caches answered "absent", correctly): the tests now wait for the replica in sync before a failover. **Not
    tested** (documented in `RedisAdapter`): Dragonfly's cluster modes (its emulated mode, a single server answering as
    a cluster, may be refused needlessly if it reports cluster mode: to probe), replication without Sentinel,
    Dragonfly with replicas, TLS and authentication, managed services, other servers speaking the protocol.
    **Rerun 2026-11-03** (after the cluster, the CROSSSLOT split, `SCRIPT LOAD` on `NOSCRIPT`, READONLY, Kvrocks),
    p50 per store call and 8 threads at 80 % gets:

    |                                          | `getIfPresent` | `put`  | 1 hot key    | 10 hot keys  |
    |------------------------------------------|----------------|--------|--------------|--------------|
    | Redis 8, Lettuce                         | 261 µs         | 323 µs | 93,781 ops/s | 62,680 ops/s |
    | Redis 8, Jedis                           | 175 µs         | 263 µs | 58,836 ops/s | 43,601 ops/s |
    | Redis 8, Redisson                        | 206 µs         | 304 µs | 68,547 ops/s | 45,051 ops/s |
    | Valkey 9, Lettuce                        | 193 µs         | 293 µs | 92,237 ops/s | 58,667 ops/s |
    | MariaDB 13 (after step 6's first change) | 251 µs         | 489 µs | 21,631 ops/s | 47,093 ops/s |

    No regression: ten hot keys as on 2026-11-01 (62,900, 43,700, 47,200), one hot key 8–19 % lower (101,500,
    72,500, 77,500), within what the order of back-to-back runs moves. `putAll` of 100 still ~16 ms everywhere (the
    Docker Desktop stall). **Dragonfly 2.0.2 unstable here:** the same scenario in three runs gave 330, 8,689 and
    14,826 ops/s (one hot key) and 11,414, 9,005 and 3,747 (ten), its L1 hit rate 90–94 % (others 100 %), 67 µs of
    server time per script (Redis 16–23) and 958 µs per `XREAD` (Redis 6); the image is native arm64. **Traced (the
    user's request, 2026-11-03): Dragonfly's locking per key across its threads.** Fewer threads helped
    (`--proactor_threads=2`: 24,862 and 29,926 ops/s, 28 µs per script) but four were worse than two (23,827, 42
    µs): not starvation alone. Each script touches several keys (the row, the stream, the counter), which Dragonfly
    shards over its threads by hash and locks one by one, a transaction across threads for every write, queued on a
    hot key. **`--lock_on_hashtags`** locks by hashtag, and a cache's keys share `{dc:<keyspace>}`: one hot key
    94,669 ops/s, ten 58,734, L1 hit rate 100 %, 4.7 µs per script, a put 317 µs (691 before), as on Redis (BullMQ
    advises the same for its scripts). With `--cluster_mode=emulated` too, the plain client is refused, as it should
    be. **Built:** `RedisAdapter` advises the flag (correct either way: a script stays atomic); the matrix keeps
    Dragonfly on its defaults and adds `Dragonfly_2_latest__LockOnHashtags` (every suite: 346 tests, 30 skipped,
    green); the benchmark measures Dragonfly both ways; `RedisTestServer` takes Dragonfly's flags.
11. Keyspace, discriminator and the cluster (2026-11-01, user's choice: support the cluster, every library and
    server). **Keyspace and discriminator,** as on MongoDB and PostgreSQL: `RedisAdapter.newBuilder(driver, keyspace)
    .withDiscriminator(…)` (default `default`); keys `{dc:<keyspace>}:<discriminator>:…`, so a keyspace's caches share
    one slot, and caches of other keyspaces spread over the nodes (layout marker 2). **The feed:** an `XREAD` cannot
    span slots (`CROSSSLOT`), even on one node, so one reader per keyspace in a cluster, one per client otherwise
    (`RedisSession#clustered`, the route passed on subscribe). Sharing a thread across readers was asked for: reads
    are asynchronous where the library allows (Lettuce's futures, Redisson's `RFuture`, both bounded by
    `orTimeout`), the answer handed to a shared pool; Jedis blocks a thread per read in flight (a cached pool). A
    keyspace costs a connection, a thread only on Jedis. **Bindings:** `LettuceRedis.of(RedisClusterClient)` (one
    connection type for both, commands through `RedisClusterAsyncCommands`; Lettuce routes by key, follows redirects,
    and refreshes adaptively by default since Lettuce 7); `JedisRedis.of(RedisClusterClient)` (a connection from the
    pool of the slot's node, given back by `close()`; a `MOVED`/`ASK` answer refreshes Jedis's view and the command
    goes once more, as nothing ran; a reader holding a connection to the old owner gives it back);
    `RedissonRedis.of` accepts a cluster configuration (Redisson routes by first key, `INFO` from a primary). `MOVED`
    and `ASK` count as unavailable (nothing applied, retryable) where a library passes them on. A plain client of a
    cluster node is refused at store creation (it would reach that node's slots only). **Tests:** `RedisCluster`
    (three primaries with a replica each, `redis-cli --cluster create` by container IPs, every address mapped to
    `127.0.0.1` by each client; Dragonfly: three nodes configured by `DFLYCLUSTER CONFIG`, or one in
    `--cluster_mode=emulated`), `RedisClusterTests` beside each Sentinel entry and for Dragonfly: both contracts and
    the failover tests through each library, core, distribution and recovery through Lettuce, and a new failover
    test, the cache's slot moving to another primary while a writer writes (`MIGRATING`/`IMPORTING`, `MIGRATE`,
    `SETSLOT NODE`). The failover tests now run on a `RedisTopology` (Sentinel or cluster). Not on Dragonfly: its
    failover and slot moves are the control plane's work. Green: Redis 6.2 latest, 8 and Valkey 9 clusters (241 tests
    each, 23 skipped), both Dragonfly clusters (9 more skipped: the failover tests); Redis 8 and Dragonfly single and
    Sentinel suites after the reader restructuring.
12. Dragonfly 1.x (2026-11-01, user's question "why not v1.x"). Probed: 1.0–1.2 lack `XADD … MINID`, 1.3 lacks
    `XREAD` (the feed failed for good, the cache started anyway), 1.4 has every stream command the store uses; before
    1.7 Dragonfly reports `dfly_version` (not `dragonfly_version`) beside Redis 6.2.11, and in `--cache_mode` only
    `cache_mode:cache` (no `maxmemory_policy`). **Built:** a probe at store creation (`RedisScripts.PROBE`: `XREAD`,
    `XREVRANGE`, `XADD … MINID` in a script on a scratch stream, deleted again) refuses a server lacking one, whatever
    version it reports; both Dragonfly fields recognised. Matrix: Dragonfly 1.4.0 (compatibility suites; the lost
    script test skipped, no `SCRIPT FLUSH` before 1.7), 1.40.2 (every suite, both cluster modes), 1.3.0 (refused,
    through each library). Found on the way: Jedis's Sentinel client failed to build right after Sentinel logged its
    monitor ("All sentinels down"); the test topology builds it again until it does. Green: every Dragonfly 1.x entry,
    Redis 6.2.0 with the probe.
13. Aligned with the other adapters' tests (2026-11-02, user's choice of four gaps). **Built:** (1) the stream trimmed
    (`XTRIM MINID`) past a cache held back on its reader: a gap, after which the changed value is found (as MongoDB's
    oplog rolling over); (2) on the shared reader, a listener failing leaves the others undisturbed, and a cache slow
    to apply (a value deserialised in 2 s, `UPDATE` mode) holds up no other (as PostgreSQL's and MongoDB's); both in
    `RedisFeedTests`, on single servers and clusters; (3) the primary killed (`docker kill`, no handover) behind
    Sentinel and in the cluster, through each library, the node started again as a replica (`RedisTopology#kill`,
    `#revive`); (4) a replica refused at store creation (`INFO replication` role; a replica accepting writes,
    `replica-read-only no`, would keep them alone), through each library behind Sentinel; cluster clients exempt
    (they send writes to primaries). **Found:** Jedis's cluster view was refreshed on redirects only, so after a
    primary died its cache never recovered (Jedis's own executor refreshes on connection failures; the binding
    bypasses it): now refreshed after a connection failure or timeout as well, the command not sent again. Sentinel
    kept a stale entry of a revived node (`:<port>`, no address), which later failovers picked and timed out on: the
    test topology resets Sentinel after a revive (`SENTINEL RESET`, as its docs advise after topology changes).
    Redis 6.2.0's Sentinel never registers the old primary as a replica after the switch, so never converts it when
    it returns; under the full matrix's load the topology then stayed broken (Sentinel naming a replica as the
    primary): the kill test is skipped on 6.2.0 (`RedisTopology#revives`), and runs on every later release. The
    replica refusal is retryable (decided 2026-11-02): behind Sentinel a replica reached at store creation is a
    failover in progress. A manual cluster failover gives up silently when the replica does not catch up in time
    (found twice under the matrix's load): the test topology asks again every few seconds.
14. The setups marked "not tested", three of them tested (2026-11-02, user's choice). **A lost write from a clock
    ahead:** the old primary's version floor raised by an hour (its script's `floor`, the write time and the stream's
    trimming left alone), so every write after the failover gets a lower version than the lost one; every cache drops
    the lost value and takes the later, lower writes, behind Sentinel and in the cluster through each library (the
    nodes share the host's clock, so a container's clock cannot be set). **A restricted user:** an ACL user with only
    the adapter's commands (its own, those its scripts call, which Redis checks against the user too, and the client
    libraries' handshakes) on `{dc:*}`, both contracts through Lettuce and a cache pair through each library, on
    every single server; the permissions are documented in `RedisAdapter`. Dragonfly takes no subcommand rules
    (`+client|setname`), so `CLIENT` as a whole there, and no `reset` in `ACL SETUSER`. **MongoDB: a batch the primary dies under**
    (`MongoFailoverTest`, 20,000 keys rewritten, the primary killed 100 ms in): every key old or new in the store,
    both caches agreeing with it, whether the driver carried the batch over or it failed as outcome unknown. Green:
    Redis 8 (Sentinel, cluster, single server), Redis 6.2 and Dragonfly 2 single servers, MongoDB 9 three-node.
15. Garnet (2026-11-02, a trial; KeyDB left out, its development stalled). Garnet 2.2.5 reports Redis 7.4.3 but runs
    no scripts on its defaults ("Lua scripting support disabled") and, started with `--lua`, has no streams (`XADD`,
    `XREAD` unknown, also from a script). Both refused at store creation by the probe, through each library; the
    refusal now names both requirements ("Lua scripts calling XREAD, XREVRANGE and XADD with MINID"). Garnet is not in
    the matrix (decided the same day): the case it stood for, a server running no scripts, is tested on every single
    server with users instead (`RedisAclTests`, a user without `EVALSHA` and `EVAL`, refused at store creation through
    each library); a stream command missing in a script is covered by Dragonfly 1.3.0 and Kvrocks 2.15.0. Its docs list every stream command
    as not supported, with no reason or roadmap given. A further gap the probe cannot see: Garnet runs a script as a
    transaction (its keys locked) only with `--lua-transaction-mode`, off by default; otherwise a script is not
    atomic, which every write script here relies on. Should Garnet gain streams, or be wanted without them (a polled
    store), it would need that flag required and checked, not only the probe.
16. Managed services (2026-11-02, read from their documentation, not run against them). Amazon ElastiCache
    Serverless: Lua, `XADD`, blocking `XREAD`, `XREVRANGE`, `INFO` supported (`SCRIPT FLUSH` a no-op); blocking
    timeouts, `INFO` sections and the eviction policy not documented. Azure: Azure Cache for Redis (being retired)
    disables `CONFIG`, `ACL` and others (none we use), defaults to `volatile-lru` (sparing our keys), Lua implied
    by its settings; Azure Managed Redis's pages say nothing of Lua, streams or eviction, but its Enterprise cluster
    policy refuses commands across slots (`CROSSSLOT`) behind one endpoint. Upstash: Lua atomic by default (a global
    lock), `XADD … MINID` and `XREAD BLOCK` documented, eviction off by default (its own policy when on,
    "optimistic-volatile", may evict our keys), $0.20 per 100,000 commands on pay-as-you-go: the reader's blocking
    read about every second, about 86,400 a day per keyspace and instance (some $5 a month), timed-out reads' billing
    not documented. **Built:** the shared reader splits by keyspace on `CROSSSLOT` (`RedisFeed#split`: each
    subscription moves with its position, nothing missed or reported; the driver's later subscriptions go per
    keyspace at once), tested with a driver refusing reads across hash tags (`RedisSlotEnforcingDriver`) on a single
    server and in the cluster; the services' notes in `RedisAdapter`; `RedisManagedServiceTest`, both contracts, the
    feed tests and the core suite against a URI given (`-Dredis.external`, `-Dredis.external.cluster`), the run's
    keys deleted after it (test cache names now carry a run id), skipped without the URI; tried against a local
    Redis with a password (196 tests, no keys left). Open: running it against the services themselves (needs accounts).
17. Kvrocks (2026-11-02, a trial on 2.17.0, Apache's server on RocksDB, on disk). It reports Redis 7.0.0 and its own
    `kvrocks_version` (recognised as "Kvrocks"), passes the probe, runs every command the scripts use and blocking
    `XREAD`, reports no eviction policy (nothing to evict), keeps a failing script's earlier writes as Redis does.
    **Found and fixed:** Kvrocks caches no script `EVAL` runs (only `SCRIPT LOAD`), so every call would have missed
    by digest and sent the script in full, two round trips per write: on `NOSCRIPT` every binding now loads the
    script where the key is served (`SCRIPT LOAD`; Lettuce's cluster client on every node, Jedis and Redisson by key)
    and runs it by digest again, falling back to `EVAL` where loading is refused or the script is still missing; a
    failing script's reply on Kvrocks (`ERR running script …`, not Redis's `Error running script`) is now classified
    as a script failure (outcome unknown). Tests skip what Kvrocks lacks: `maxmemory-policy`, the restart with
    Redis's persistence flags, users (`ACL`; Kvrocks has a password and namespaces). Matrix: Kvrocks 2.17.0, every
    suite (340 tests, 82 skipped). **Releases and modes (same day, user's choice: a matrix of its own and the modes as
    robust as the others):** images exist from 2.7.0; before 2.15 they report Redis 4.0.0 (refused by the version),
    2.15 reports 7.0.0 but refuses an approximate `MINID` (refused by the probe: a refusal test), 2.16.0 is the first
    working release (compatibility suites), 2.17.0 the latest (every suite). Behind Sentinel (Redis's, monitoring two
    Kvrocks nodes, the replica given its primary by IP, `slaveof` one argument): the failover, the primary killed and
    revived, the replica refused, through each library; the lost-write tests skipped (no `redis.set_repl`). As a
    cluster of three primaries with a replica each, which Kvrocks runs only by a pushed layout (no gossip, no failover
    of its own): `RedisCluster.Kind.KVROCKS` acts as its controller (node IDs by `CLUSTERX SETNODEID`, the layout by
    `CLUSTERX SETNODES` under a version, a failover and a kill by the roles swapped in a new layout, a slot moved by
    `CLUSTERX MIGRATE` and `SETSLOT … NODE`), both contracts, the core and feed suites, failover, kill, revive and the
    slot moving, through each library. **Found:** Kvrocks 2.17.0 crashed (segmentation fault in
    `Storage::LatestSeqNumber`, from `ROLE`) when asked `ROLE` while it resynchronised as a demoted replica; the
    adapter never sends `ROLE` (it reads `INFO replication`), the test topology now does the same; anything else
    sending `ROLE` then would crash such a node (worth an upstream report, the user's call). Kvrocks's replicas refuse
    `SCRIPT LOAD` (`READONLY`), which Lettuce's cluster client sends to every node: the load now goes to the primary
    serving the key (`LettuceConnection#run` with a key), and any refusal of it falls back to `EVAL` in all three
    bindings. Green: the Sentinel entry four runs in a row, the cluster entry (251 tests, 29 skipped). Not tested:
    Kvrocks's own controller (`kvrocks-controller`), only its effect.
18. Dragonfly's modes, as robust as the others? (2026-11-02, user's choice; tried, partly kept.) **Behind Sentinel:**
    Dragonfly does not document Sentinel as supported (its HA is the Dragonfly Operator, or `REPLICAOF NO ONE` by
    hand), though it works with Redis's Sentinel in principle (replica by `--replicaof`, failover, conversion of the
    old primary, kill and revive all seen by hand). In the test sequence a killed primary was sometimes not failed
    over: Sentinel found "no good slave", the replica answering its pings too late while its primary was gone, and
    the restarted, empty node became primary again, the replica following it. Not in the matrix. **Found on the way,
    a bug of ours, fixed:** after a failover the Lettuce binding stayed on the demoted primary, every write refused
    with `READONLY` and the cache's store guard refusing calls; Redis's Sentinel closes a demoted primary's client
    connections, Dragonfly does not: `LettuceConnection` now drops a connection answered `READONLY`, the next
    command reconnecting to the primary Sentinel names. **A cluster with replicas:** every operation works by hand
    (replicas named in `DFLYCLUSTER CONFIG` and started by `REPLICAOF`; a planned failover by `REPLTAKEOVER`, which
    locks the old primary until it is told to follow, through `redis-cli`, as Lettuce cannot even connect to a node
    answering `LOADING`; a kill by `REPLICAOF NO ONE` and a new layout; a revive by `REPLICAOF`; a slot moved by a
    `migrations` entry, then the final layout once `SLOT-MIGRATION-STATUS` says `FINISHED`), and the tests can play
    the operator (`RedisCluster.Kind.DRAGONFLY_REPLICATED`, kept, no matrix entry). But after a takeover Lettuce's
    cluster client found no usable view for over a minute: its topology refresh failed ("Cannot determine own
    partition", a node's `CLUSTER NODES` without itself, at some moment of the takeover), also when forced once no node
    answered `LOADING` any more (a bounded experiment). **Checked further (same day):** (1) `CLUSTER NODES` polled
    every 50 ms through a takeover: the old primary answers `LOADING` for about 1.3 s, then, following the new
    primary, lists only it, without itself (no `myself`), until the controller pushes it the new layout (about 3 s
    here). Redis never answers so; a protocol deviation of Dragonfly's, which makes Lettuce's refresh throw, and
    worth reporting to Dragonfly (the user's call). (2) The failover tests through each library alone: Redisson
    rides out the failover, the kill and the slot moving (seconds each); Jedis does not (its slot cache stays on the
    old primary, `MOVED` on every command although the binding asks it to refresh after each, the store guard
    refusing calls for over a minute), nor does Lettuce. So two of three libraries fail to recover from Dragonfly's
    transient state, which a few seconds of it would not explain alone: their refreshes are strict (Lettuce discards
    a refresh with a view lacking itself) or not repeated soon enough (Jedis). The Dragonfly
    cluster without replicas stays as tested before (contracts, core suites; green again). **Support levels stated
    plainly** (`RedisAdapter`): Kvrocks behind Sentinel is tested and green but not documented by Kvrocks either (its
    HA is the cluster with a controller); Dragonfly behind Sentinel is not supported; failover inside a Dragonfly
    cluster is not supported.

### MariaDB and MySQL: dialects of one store, an adapter each

*Decided 2026-11-02, built 2026-11-02 to 2026-11-03.*

The question: own adapters for MariaDB and MySQL, or dialects of the JDBC store, given that the JDBC store already
does without what MariaDB could offer (`RETURNING`, sequences, a native upsert)? A store of its own is justified here
by a capability the shared path cannot use; PostgreSQL got one for two: a native feed (`LISTEN`/`NOTIFY`) and
versions assigned in one statement (`RETURNING`, about twice the throughput on one hot key). Neither MariaDB nor
MySQL has a feed reachable through JDBC (the binlog needs external CDC), so both stay polled whatever the adapter;
what remains are performance gains, unmeasured: a one-statement upsert (`ON DUPLICATE KEY UPDATE`, both), versions
assigned by the store (MariaDB only, `UPDATE … RETURNING` from 13.0; MySQL has neither `RETURNING` nor sequences). By
the complexity budget (a mechanism only for a guarantee or a measured gap), no store of their own.

**Decided:**
- **One internal store, dialects:** `JdbcStore` stays the store, parameterised by a dialect (the types of bytes and
  times, the time now, and what a benchmark may later justify: a native upsert, assigned versions on MariaDB 13+, as
  dialect capabilities, not separate stores). Versions stay the core's, conditional, as now (profiles `mariadb`,
  `mysql`: conditional).
- **A public adapter per database:** `MariaDbAdapter` and `MySqlAdapter`, each with its documentation (supported
  versions, polling, replication caveats) and its tested matrix, choosing its dialect itself, not by the product name
  behind the user's back; a data source of another product refused at the start, naming the right adapter.
  `JdbcAdapter` and the untested standard dialect go (then unreleased: no deprecation needed). Another database
  (SQL Server, Oracle, H2) gets a dialect and an adapter only on demand, tested.
- **MySQL's caveats documented:** changes polled (the poll interval, not milliseconds); committed writes lost on a
  failover unless semi-synchronous replication (`AFTER_SYNC`) or Group Replication; Galera's known anomalies stay
  documented for MariaDB (above).

**Build order:**
1. **MySQL trial:** MySQL images in the JDBC matrix (8.0, 8.4 LTS, the latest 9.x), the store and change scan
   contracts and the core suites under polling, the MariaDB dialect chosen for "MySQL" as a first guess; what differs
   (error 1093 on a subquery of the table written, error codes, isolation) fixed in a dialect of its own.
2. **The split:** `MariaDbAdapter`, `MySqlAdapter` in the `jdbc` package over the package-private `JdbcStore`,
   `JdbcAdapter` and the standard dialect removed, tests and documentation moved; the product check at the start.
   **Built (2026-11-03):** `MariaDbAdapter` and `MySqlAdapter` (public, the same builder: table name,
   discriminator) over `JdbcStore`, whose `Dialect` (`MARIADB`, `MYSQL`, the same SQL for now) carries the product,
   its adapter's name and its floor. The schema's check reads `SELECT VERSION(), @@server_id`: the product told by
   the version string (MariaDB's names itself; not the driver's product name, which Connector/J gives as "MySQL"
   whatever it reaches), the other product refused naming its adapter, a version below the floor (MariaDB 10.2,
   MySQL 5.7) refused. Javadoc per adapter: polling, the refusals, failovers (lost writes dropped by the
   server-identity check; MySQL's semi-synchronous `AFTER_SYNC` loses none; Galera unsupported). Tests pick the
   adapter by `-Djdbc.image` (`JdbcTestDatabase.adapter`), the failover topology by its own image; new: the other
   product's adapter refused, versions parsed. Green: the JDBC suites (49) on MariaDB 11.8 with `utf8mb4` and
   MySQL 8.4, the failover test on both.
   **Driver floors tested 2026-11-05** (each adapter's suites on the latest image, the first release of each driver
   major and bisected where it failed): MongoDB driver 5.2 (5.0 and 5.1 lack API the adapter uses), pgjdbc 42.7.6
   (42.7.5 and older hang on concurrent size pruning, below 42.3.1 it does not compile), Lettuce 7.8 (7.0 to 7.7
   fail about half the suites, "script marker failed"; the test classpath needs one Netty version for all three
   clients), Jedis 8.0, Redisson 4.0, MariaDB Connector/J 3.0 on MariaDB and 3.1 on MySQL (3.0 refuses MySQL 8.4's
   `caching_sha2_password`), MySQL Connector/J 8.0.31 (the first release of `com.mysql:mysql-connector-j`). Connector/J
   8 and 9 failed two robustness tests at first: they report a killed session and a silent connection both as "No
   operations allowed after statement closed." (SQLState S1009), which `JdbcStore` took as a plain failure, a write
   that may have landed then certainly not applied. Fixed driver-neutrally: a failure the exception does not explain
   is a timeout if it waited the whole timeout, and a connection lost if the connection is no longer valid
   (`Connection#isValid`, asked only then, before the connection goes back); both branches mutation-checked.
   **Drivers: strict plus one (the user's decision, 2026-11-03).** MariaDB through MariaDB Connector/J only (MySQL
   Connector/J refused by its driver name: neither Oracle nor MariaDB supports that pairing); MySQL through MySQL
   Connector/J or MariaDB Connector/J (MariaDB supports it, and its LGPL licence is why users pick it over the GPL
   Connector/J). A driver of another name (a wrapper such as AWS's) is taken, untested. Failures are classified by
   the server's error, not the driver's mapping of it: SQLState classes 08 (connection lost), 40 (rolled back),
   23 (duplicate) and the shared codes 1213 (deadlock), 1205 (lock wait timeout, SQLState HY000, which a driver may
   remap or not) and 1290 (read-only server: a failover in progress) as rejected, retryable; the exception classes
   stay as a fallback. Tests: `-Djdbc.driver=mariadb` for MySQL through MariaDB Connector/J; the refusal of
   Connector/J on MariaDB; the classification on fabricated errors. Green: the JDBC suites (51) on all three
   pairings (MariaDB 11.8 with `utf8mb4`, MySQL 8.4 with either driver).
3. **Matrices:** each adapter's Javadoc listing what is tested. **The schema (the user, 2026-11-03): each major
   version's earliest and latest release**, from the floor (step 4: MariaDB 10.2, MySQL 5.7, below which `utf8mb4`
   keys are refused), with `utf8mb4`:
   - MariaDB: 10.2.6 (the floor's first GA; 10.2.31 tested, below), 10.11.19, 11.0.2, 11.8.9, 12.0.2, 12.3.3,
     13.0.2;
   - MySQL: 5.7.9 (the floor's first GA; 5.7.29 tested, below), 5.7.44, 8.0.11 (8.0.19 tested), 8.4.13, 9.0.0,
     9.7.4, 26.7.0, 26.10.0.
   MySQL with both drivers (MySQL Connector/J, MariaDB Connector/J) on each version.
   10.2.6, 5.7.x and 8.0.11 have x86 images only (MySQL native on arm64 from 8.0.29): emulated on Apple Silicon. The
   JDBC suites on every version; the core suites and robustness tests on each product's latest; the failover test
   on each floor and latest (MySQL 5.7 needing `CHANGE MASTER`/`START SLAVE`, renamed in 8.0.22/8.0.23). Not yet
   run on these exact tags.
   **Built (2026-11-03): `JdbcIntegrationTest`**, as Redis's: per image a nested class, one at a time; a first
   release runs `JdbcCompatibilityTests` (the contracts, the adapter tests, the property test), the latest of a major
   (floating tags: `mariadb:10`, `mysql:8`, ...) `JdbcServerTests` (those and the cache suites, polled:
   `JdbcCacheBackend`, idle once every store scanned twice after the last write, a gap induced by a failing poll),
   `JdbcReplicationTests` the failover on the floor and on each latest (MySQL 26 also through MariaDB Connector/J).
   Every MySQL entry twice, once per driver. Found on the way:
   - **First releases that do not start:** MariaDB 10.2.6 to 10.2.30, MySQL 5.7.9 to 5.7.28 and 8.0.11 to 8.0.18
     crash in their gosu (`fatal error: newosproc`) when emulated (x86 only; on Apple Silicon), MySQL before 5.7.13
     cannot even be pulled (manifest v1). As PostgreSQL's 9.6.5, the first that starts stands for each: **MariaDB
     10.2.31, MySQL 5.7.29, 8.0.19**; on x86 the true first releases would start. **Both kept (the user, 2026-11-03):**
     each such entry twice, the true first release (10.2.6, 5.7.13 as the first pullable, 8.0.11) with
     `@DisabledOnOs(architectures = "aarch64")`, the stand-in with `@EnabledOnOs(architectures = "aarch64")`
     (disabled on ARM rather than enabled on x86, which Java names `amd64` or `x86_64`). The x86 entries run in the
     nightly full matrix on GitHub. **Changed 2026-11-13:** MySQL 5.7.13 cannot be pulled on GitHub's Ubuntu 26.04
     runners (the containerd image store fails to unpack the 2016 image: "failed to extract layer … NotFound"), so
     5.7.29 runs on every architecture and 5.7.13 is gone; and MongoDB 9.0.2 refuses to start on Linux 6.19 and
     newer (SERVER-121912, a tcmalloc fix), so 9.0.4 is the first MongoDB 9 tested (locally the arm64 image started).
     Found by the nightly of 2026-11-12, with the timing failures of the contracts (windows of 60 to 300 ms that a
     slow runner overruns: every "still younger than" window in the store, feed and scan contracts widened to 1 s
     after a pause of 1.5 s) and model checking cancelled at the 120-minute limit (now two groups of its own).
   - **MySQL Connector/J on MariaDB 11.0 does not connect** (it asks for `transaction_isolation`, MariaDB's from 11.1):
     the refusal test accepts a failed connection too.
   - **Writing one row per transaction is slow:** 25,000 rows took about 2 minutes alone on MariaDB 13 (a few round
     trips each: auto-commit off, the write, commit, auto-commit and network timeout restored), up to 20 under the
     matrix's load; the store's maximum size pruned the oldest rows meanwhile, which the core test of large batches
     took for a loss. The test now sizes its batch by the backend (`CacheBackend#largeBatch`: 5,000 on MariaDB and
     MySQL, five read chunks) and has room for every row. The gap goes to step 6, measured.
   **Green (2026-11-03):** all 35 entries in eight parallel groups (~40 min), MySQL 26's group after a rerun alone
   (`contract7_pruneTombstones_keeps_values_and_young_tombstones` failed once under the load, the ages test known to);
   each adapter's Javadoc lists the tested versions.
4. **Robustness as for the other adapters** (the user's request, 2026-11-02; the JDBC side has only the contracts,
   the property test, a polling smoke test and the silent network so far), for both:
   - version matrices: MariaDB's LTS lines from the oldest supported (10.6, 10.11, 11.4, 11.8) and the latest,
     MySQL 8.0, 8.4 LTS and the latest 9.x, each line's first release and its latest as elsewhere. **Outdated
     (2026-11-03):** Docker Hub has MariaDB 12.3 and 13.0, MySQL 9.7 and 26.10 (year.month numbering); and the
     floor was upstream's support, not the code's, unlike PostgreSQL 9.6.5, MongoDB 4.4.0, Redis 6.2. Probed
     (the JDBC suites, 47 tests, 1 skipped as on every version): green on MariaDB 10.0, 10.1, 10.2, 10.3, 10.5,
     12.3, 13.0 and MySQL 5.6, 5.7, 9.7, 26.10 (5.6 and 5.7 under x86 emulation). The 5.5 images of both cannot
     start here (their Go entrypoint helper crashes under emulation, `fatal error: newosproc`); MySQL 5.5 lacks
     `DATETIME(6)` anyway (5.6.4). Those older lines passed only because their images default to `latin1`:
     **with `utf8mb4` (`-Djdbc.charset=utf8mb4`), MariaDB 10.0 and 10.1 and MySQL 5.6 refuse the table**
     ("Specified key was too long; max key length is 767 bytes": the primary key, discriminator and id of
     `VARCHAR(200)` each, is up to 1,600 bytes), while MariaDB 10.2 and MySQL 5.7, the first lines with large index
     prefixes by default, pass. **Floors proposed (2026-11-03): MariaDB 10.2, MySQL 5.7**, without
     narrowing the key columns for lines long out of support. Matrices to run with `utf8mb4`, as deployments do. MySQL
     has native arm64 images from 8.0.29 only: 5.7 and early 8.0 run emulated on Apple Silicon (green, slower);
   - the core suites under polling (core, distribution, recovery, Caffeine parity) on the latest of each;
   - retries as `PostgresRetryTest` does them: a deadlock (1213) and a lock wait timeout (1205) retried, certainly
     not applied; a connection lost during a statement an unknown outcome; a batch failing in part an unknown outcome;
   - a server restart (InnoDB durable: every value kept, the caches going on);
   - **a failover with a replica promoted** (asynchronous replication by default): writes ride it out, and a write
     the failover loses is held by no cache. **Open question, the most important of these:** with polling, a lost row
     never shows in a scan (no newer write replaces it); a failed poll counts as a loss only past the grace of
     tombstones, so an outage shorter than that may leave a holder of the lost version stale up to the maximum age.
     If the test shows it, the fix is the core's, for every polled store (a revalidation after a poll failed at all,
     or after the store's identity changed, for instance);
   - Galera (MariaDB, several writers) stays documented as unsafe (Jepsen), not supported, not tested.
   **Built (2026-11-03), in the compatibility suites (every version, both drivers):** `JdbcRetryTest`, injected
   failures as PostgreSQL's (rejected, no connection, broken connection, other errors, a batch failing after its
   first row, the core's retries through a cache) and the server's own, through the driver under test: a deadlock
   between two sessions (1213), a lock wait timeout on the store's write (1205, sessions waiting 1 s; not applied),
   the store's session killed while its write waits (outcome unknown, retryable). Sessions are found by
   `CONNECTION_ID()` and the process list as root sees it (MariaDB Connector/J's server-prepared statements do not
   show their text; `INNODB_TRX` is a snapshot naming sessions gone). `JdbcRestartTest`: the server stopped and
   started on its data (a fixed host port, as PostgreSQL's): writes fail within 15 s, hits served, what was written
   kept, writing and polling resume. **Found:** a cache that only reads stayed SYNCHRONIZED while the server was down
   (failed polls count only past their grace; the heartbeat leaves a store that does not answer to the store guard,
   which only the cache's own operations open). **Fixed (the user's go, a small core change):** a polled cache is
   DEGRADED while its latest poll failed (`InternalPollingStore.Poller#isFailing`, read by `InternalCore#state`; no
   SPI change), and the run of failed polls is logged once, as its end. Green: the oldest (MariaDB 10.2.31, MySQL
   5.7.29 and 8.0.19 through both drivers) and newest first releases, the in-memory suites on the `jdbc` profile.
   **The JDBC matrix with all of this (2026-11-03, eight groups, ~45 min):** green but for one timing failure on
   emulated MySQL 5.7 under the load, `a_slow_load_does_not_block_other_keys` (100 loads in 2.16 s against a 2 s
   bound); green rerun alone. The bound measured the store's speed rather than blocking (blocked, the loop waits out
   the slow loader's 5 s): widened to 4 s.
   The failover test early, right after the trial: if polling misses lost writes, the fix is the core's and comes
   before the adapters built on it.
   **Trial done (2026-11-02):** with the MariaDB dialect chosen for the product name "MySQL" too, the JDBC tests
   (store contract 31, change scan contract 5, adapter 7, property 4) pass unchanged on MySQL 8.0, 8.4 and 9
   (Connector/J 26.10.0) and still on MariaDB 11.8; the only failure was a test looking the table up by its name in
   upper case, which MySQL on Linux keeps case-sensitive (the test fixed). The test database is chosen by
   `-Djdbc.image` until the matrix comes with the split. **Found by reading `InternalPollingStore`, before the
   failover test:** polls failing for less than the grace (5 minutes minus the overlap) count as nothing, and a write
   a failover loses never shows in a scan (the row falls back to its older version and write time), so a holder of
   the lost value keeps it up to the maximum age; with a driver or proxy switching fast, the polls may not even fail.
   The guarantee of no stale value beyond the delivery delay is broken for every polled store whose failover can
   lose writes. Options (to decide after the failover test shows it): A. the server's identity read with each poll
   (MySQL `@@server_uuid`; MariaDB has none, another identity needed), a change a gap; B. numbered writes as on Redis
   (a hot row per cache, serialising writers on SQL); C. any failed poll a loss (misses a fast switch). Proposed: A.
   **Confirmed by the failover test (`JdbcFailoverTest`, MySQL 8.4 primary with an asynchronous replica, the
   applications' address a proxy the failover moves):** with the value the failover falls back to written 6 s before
   the lost write, the holder served the lost value for the whole 30 s of the check, with the heartbeat at its 10 s
   default and with it practically off alike (its beats after the failover went through as usual). Written moments
   before, it passed: the fallen-back row was still within the polls' overlap, reported again, and the holder
   checked its newer version against the store; timing luck, which the test now excludes.
   **Fixed without an SPI change (the user's decision, 2026-11-02; A and C together):** (1) the core: any failed
   poll is a gap, reported at the next poll that succeeds (`InternalPollingStore`; the poll reads from the same
   point as before, the gap makes the core check what caches hold against the store); (2) the JDBC store: each scan
   reads `@@server_id` with the clock (MySQL and MariaDB alike: replication needs a distinct one on every server),
   and a scan answered by another server than the last fails once, retryably, which the core takes as a gap by (1).
   `ChangeScan` only gained documentation: a store may fail a scan for this purpose alone. The identity is first
   taken by the schema check, before the store reads anything (found: with it learnt by the first scan, caches whose
   first poll came after the failover held a key the failover lost, intermittently in a full-class run). Cost: one
   more column per scan, a check of the held values per outage of the store. `JdbcFailoverTest` covers a lost update
   of an existing key and a lost first write of a key (no row on the new primary, so no scan could report it); green
   five runs in a row, the JDBC suites still green on MariaDB 11.8 and MySQL 8.4. **MariaDB's own replication
   (2026-11-03):** `JdbcReplication` builds either product's topology by the image (`-Djdbc.failover.image`, MySQL
   8.4 by default): MariaDB with its GTIDs (`gtid_slave_pos`, `CHANGE MASTER … MASTER_USE_GTID = slave_pos`,
   `MASTER_GTID_WAIT` for being in sync, `read_only` as it has no `super_read_only`). The same three tests pass
   unchanged on MariaDB 11.8 and 10.6 (`@@server_id` serves as the identity there too); MySQL 8.4 still green. Both
   products in the failover test's matrix with the split's matrices (step 3).
5. **Checkpoint: one store or two?** (the user's question, 2026-11-02, whether differing robustness justifies
   adapters of their own.) After the trial and the robustness tests, each difference between MariaDB and MySQL is
   sorted: data (error codes, the replica check's query, documentation of failover URLs) stays in the dialect; a
   different algorithm (a write path one of them needs to stay safe, a feed for one, a failover detection only one
   supports) gives that product a store of its own. Expected so far: data only (both share the error codes 1213 and
   1205 and SQLState `08…` for lost connections, REPEATABLE READ and 50 s lock waits by default; either driver may
   reach either server, so nothing ties to a driver's exception classes). The public adapters are separate anyway,
   so the outcome changes no API.
   **Outcome (2026-11-03): one store.** Every difference found in steps 1 to 4 is data or none:
   - data, in `Dialect`: the product (told by `VERSION()`), its adapter's name, its floor (10.2, 5.7), the driver
     refused (MySQL Connector/J on MariaDB);
   - none: the SQL (types, clock, `@@server_id`), the error codes and SQLStates (1213, 1205, 1290; 08, 40, 23), the
     failover detection (the server's identity, a failed poll a gap), the polling and its DEGRADED state;
   - outside the store: the replication statements differ in the test topology only; Galera's and semi-synchronous
     replication's properties are documentation.
   No algorithm differs: no write path one of them needs to stay safe, no feed for one (both polled), no failover
   detection only one supports. The one candidate is a performance capability, not safety: MariaDB 13's
   `UPDATE … RETURNING` for assigned versions (step 6, only if measured worth it), which stays an optional
   capability of the dialect as decided above. Revisit only if a difference appears that one product needs for
   correctness; a feed (the binlog) would be such a one, and would justify a store of its own.
6. **Later, only measured:** writes in fewer round trips (measured 2026-11-03: one transaction per row, 25,000 rows
   in about 2 minutes on MariaDB 13; a JDBC batch per chunk, or no auto-commit toggling per row); the native upsert
   and MariaDB 13's assigned versions, benchmarked against today's path.
   **Step 1 built (2026-11-03): writes in auto-commit, no transaction per row.** Every write statement is
   conditional by itself, and on MariaDB and MySQL a failed statement (the upsert's insert finding the row) leaves
   the session usable, so the transaction added three round trips (auto-commit off, commit, on) and no safety. A
   connection that comes without auto-commit is still committed or rolled back and goes back as it came.
   `JdbcBenchmarkExperimentTest` (`EndToEndBenchmark`, `-Dexperiments=true`), p50 before → after:

   |                        | MariaDB 13            | MySQL 26              |
   |------------------------|-----------------------|-----------------------|
   | `put`                  | 874 → 489 µs          | 1,312 → 720 µs        |
   | `invalidate`           | 708 → 283 µs          | 1,007 → 518 µs        |
   | `compute`, hot         | 677 → 249 µs          | 1,048 → 451 µs        |
   | `putAll` of 100        | 88.0 → 45.8 ms        | 136.0 → 75.9 ms       |
   | 1 hot key, 80 % gets   | 10,329 → 21,631 ops/s | 6,975 → 11,645 ops/s  |
   | 10 hot keys, 80 % gets | 19,786 → 47,093 ops/s | 12,439 → 21,410 ops/s |
   | `compute` increments   | 418 → 1,913 /s        | 311 → 870 /s          |

   Reads unchanged (`getIfPresent` ~200–250 µs, as PostgreSQL's 253). Left: a batch is still a round trip or two per
   row (`putAll` of 100 at 46 ms against PostgreSQL's 18 ms with Docker Desktop's stall); fewer would need a JDBC
   batch or the native multi-row upsert, each needing a per-row outcome (refused or applied) that both drivers report
   differently (`continueBatchOnError`, rewritten batches): a design step of its own, only on demand
   (the hot-key scenario, as for PostgreSQL in 6b(4)).

### CockroachDB adapter (planned, not built)

*Planned 2026-11-02; to be approved.*

The first of the distributed SQL databases (the user's list: CockroachDB, YugabyteDB, TiDB, TiKV, ScyllaDB; chosen
first as the best fit: a native feed, versions assigned in one statement, no loss of committed writes on failover,
PostgreSQL's wire protocol). Facts from CockroachDB's documentation (v26.4, read 2026-11-02) unless marked
**unverified**.

- **Entry and dependency:** `CockroachAdapter.newBuilder(dataSource, schemaName, tableName)`, the same shape as
  `PostgresAdapter` (discriminator, schema reset; names as there), over pgjdbc (PostgreSQL's driver, which CockroachDB
  documents for Java; no driver of its own needed). Its own package, depending on nothing of the library but the
  SPI, as every adapter.
- **Code: its own store, written from `PostgresStore`, not shared with it** (proposed): the statements differ in the
  feed (no notification in the write), in retries and locks (below), and PostgreSQL's adapter keeps its tuning
  (`SKIP LOCKED`, `OFFSET` pruning, `pg_notify` per row). A shared internal base only once a third SQL store asks for
  it, as for JDBC dialects.
- **Table:** as PostgreSQL's (`discriminator, id, key, value, state, version, write_time`, primary key
  `(discriminator, id)`, an index on `(discriminator, state, write_time)`, the marker as table comment); `BYTES`,
  `STRING`, `JSONB` by format. Created with `CREATE TABLE IF NOT EXISTS`: CockroachDB has only transaction-scoped
  advisory locks, not session-scoped ones, which `PostgresStore` uses for its creation.
- **Versions, assigned:** each write one statement, `greatest(old + 1, the transaction's timestamp in µs)`, as on
  PostgreSQL (there `clock_timestamp()`); the row read in the same serializable statement keeps it increasing per
  key, whatever the timestamps of concurrent transactions (`now()` "has no relationship with the commit order of
  concurrent transactions"). Not `cluster_logical_timestamp()` (**unverified** in the docs read; it would also pin the
  commit timestamp and cause retries), not a sequence (a single hotspot). Profile `cockroachdb` as the survey names it
  (`StoreProfile.Clock.MONOTONIC` only if the version ever comes from the commit timestamp; with the transaction's
  timestamp the clock is `MICROS`, decided when built).
- **Writes:** `INSERT … ON CONFLICT … DO UPDATE … RETURNING` (**unverified** in combination; `RETURNING` and
  `ON CONFLICT DO UPDATE` each documented), conditional writes as on PostgreSQL; batches in chunks (CockroachDB warns
  of large multi-row inserts: "transaction deadline exceeded"); single statements, so the server retries contention
  itself; a `40001` ("restart transaction") reaching the client is retried by the store as on PostgreSQL (already in
  its retry rules), its outcome certainly not applied.
- **Feed: a sinkless changefeed** (`CREATE CHANGEFEED FOR TABLE …` without a sink: rows streamed "to the SQL client
  indefinitely" over one connection; `EXPERIMENTAL CHANGEFEED FOR` is deprecated since v25.2), with `updated`,
  `resolved` and `min_checkpoint_frequency` (both at about a second), on a dedicated connection taken from the
  `DataSource` and held (the docs: "dedicated connections … instead of using a connection pool"), shared per
  `DataSource` like PostgreSQL's listener; one changefeed may watch several tables, so tables are added by reopening
  it at the last resolved timestamp (as MongoDB's stream widens). Guarantees, and what they mean here:
  - **at-least-once, per-key order, duplicates in any order, no order across keys:** fine for the core, which orders
    by version and checks a message below what it holds against the store;
  - **a resolved timestamp** "guarantees that no previously unseen rows with an earlier update timestamp will be
    emitted": the position. After a disconnect the feed reopens with `cursor` at the last resolved timestamp, missing
    nothing; a cursor older than the garbage collection window fails, which is a gap (default window
    **unverified**); a resolved timestamp missing for longer than the heartbeat is a silent feed;
  - **deleted rows** (pruning, expiry's deletes) are emitted as deletes, which the store ignores (the SPI: deletes
    report nothing);
  - **the value columns** travel with every change, which a cache's message does not need: a changefeed over a query
    (`CREATE CHANGEFEED … AS SELECT id, version, state …`, documented for sinks; for sinkless **unverified**)
    would keep them off the feed; failing that, the whole row, its value ignored;
  - **all discriminators of a table** arrive on its changefeed, filtered by the store (or by the query's `WHERE`).
- **Prerequisites, checked at the start:** `kv.rangefeed.enabled` must be on (on by default only in CockroachDB's
  cloud; self-hosted needs `SET CLUSTER SETTING kv.rangefeed.enabled = true`, costing "about a 5–10% increase in
  write latencies"): the start refuses with that statement in the message, as it does for a missing table right; the
  user needs `SELECT` on the table for a sinkless changefeed.
- **Licensing** (the user's decision for the documentation, not the adapter's): since 24.3 CockroachDB runs under its
  own software license; a self-hosted cluster without a key is throttled to five concurrent transactions seven days
  after initialisation (single-node clusters are exempt, and need no key); "Enterprise Free" for businesses under
  $10M revenue requires telemetry. Tests: single-node without a key; multi-node test clusters live minutes, far
  within the grace period. The adapter states this in its documentation and does nothing about it.
- **Failover:** committed writes are not lost (consensus), so no lost-write test as on Redis; a node killed under a
  three-node cluster: writes ride it out (retries), the changefeed is reopened at its resolved timestamp, nothing
  missed; tested as on MongoDB (`MongoFailoverTest`'s cases where they apply).
- **Test matrix** (to be fixed when built, by the supported releases then): the oldest supported release with
  sinkless `CREATE CHANGEFEED` (from v25.2; earlier only the deprecated form) and the latest, single node (every
  suite) and three nodes (failover).
- **To verify before building:** `ON CONFLICT DO UPDATE … RETURNING`; a sinkless changefeed over a query; resolved
  timestamps on a sinkless changefeed; the error of a cursor past the garbage collection window, and the default
  window; `SKIP LOCKED` and `FOR SHARE`; `COMMENT ON TABLE`; how a changefeed connection behaves on a node failure.
- **Build order** (as for Redis): 1. the verifications above, by hand against a container; 2. the store and its
  contract tests on one node; 3. the changefeed and the change feed contract; 4. the core suites; 5. three nodes and
  failover; 6. the version matrix, documentation, benchmark against PostgreSQL.

## Packaging

### Coordinates, packaging and version

- **One artifact:** `io.github.oberhoff.distributed-caffeine:distributed-caffeine`.
- **The store drivers (MongoDB, PostgreSQL, Lettuce, Jedis, Redisson) and Jackson are `optional` dependencies.**
  Users add what they use (usually they have the driver already); the driver version is always the user's own.
  Minimum supported versions are documented and covered by the test matrix.
- **Fory is a regular dependency** (about 2.7 MB plus Janino), because it is the default serializer that lets a cache
  run with no configuration (see Serialization).
- **A clear error when a driver is missing:** an adapter checks on construction that its driver is present and
  names the missing dependency (e.g. "MongoAdapter requires org.mongodb:mongodb-driver-sync 5.x on the
  classpath"), instead of a `NoClassDefFoundError` later. Same for the Jackson serializer.
- **Packages:** the base package stays `io.github.oberhoff.distributedcaffeine`; the SPI gets its own package, the
  adapters live under `...adapter` (`...adapter.mongodb`, `.postgresql`, `.redis`, `.jdbc`), the integrations with
  frameworks under `...integration` (`...integration.spring`; see "Integrations").
- **The boundary is enforced by a test** instead of separate build modules: an architecture test (ArchUnit or a
  plain check over the class files) fails if an adapter package uses anything but the SPI package.
- **Version: the 0.x line** until the API is settled for 1.0; a minor may break the API, and the breaking
  changes and how to move are in the release notes. Two versions cannot be on one classpath (same coordinates
  and packages); an application moves all its caches at once, and the compile errors show what to
  change.
- **Splitting later stays possible** without breaking anything (e.g. with many adapters or adapters on their own
  release cadence): the core artifact keeps its name.
- **The Javadoc inherits Caffeine's text (decided 2026-10-27):**
  - The rule: methods that override Caffeine's interfaces keep Caffeine's text through `{@inheritDoc}`, and add
    only what differs or happens in addition. A method where nothing differs gets no comment at all.
  - What the build needs: the javadoc tool copies inherited text only from source files. Caffeine's doc comments
    are only in its sources jar, and by default `maven-javadoc-plugin` puts only our own `src/main/java` on the
    sourcepath. Without configuration, `{@inheritDoc}` comes out empty, leaving our addition and a "Specified by"
    link (checked 2026-10-27).
  - The fix: unpack `caffeine-<version>-sources.jar` (`dependency:unpack`, classifier `sources`) into `target/`, and
    add that directory to the plugin's `sourcepath`. With `-Dsourcepath=src/main/java:<the unpacked sources>`,
    Caffeine's text appeared, followed by ours (checked 2026-10-27). `includeDependencySources` given on the
    command line did not help.
  - `-link` to Caffeine's Javadoc only makes "Specified by" a link and copies no text. It is still useful on top.
  - IDEs resolve `{@inheritDoc}` from Caffeine's sources jar by themselves. Only the published Javadoc needs this.
  - When: together with the javadoc plugin, which this module does not have yet and which Maven Central requires
    for a release.

### Integrations: one subpackage per framework

Decided 2026-11-18. `DistributedCache` is a Caffeine `Cache`, so code using the cache needs no change; a framework
that builds its caches itself from a `Caffeine` builder (Spring's `CaffeineCacheManager`) does, since that builder
knows nothing of a store. An integration closes that gap for one framework: a drop-in for Caffeine's configuration
there, not only for Caffeine's API.

- **Layout:** `...integration.<framework>` (`spring` first, after the 0.9.0 release; `jcache` next, on demand, see "Open
  work"), in the one artifact, like the adapters. The exception is a framework whose integration has a form of its own,
  as a Quarkus extension does (below): an artifact of its own, depending on this one.
- **Its framework is an `optional` dependency**, as the drivers are: users without it get none of its classes. An
  integration whose entry point is a framework subclass (Spring's manager) cannot check for its framework itself,
  since the class fails to load without it; an integration that can check fails on construction with a message
  naming the artifact (without a version, as for the drivers).
- **Public API only:** an integration uses `DistributedCaffeine`, the `Distributed*` interfaces, hashers and
  serializers, never an `Internal*` class or the SPI. It stays thin, and could move to an artifact of its own
  without a change in the core. `ArchitectureTest` is extended to enforce it, as it does the adapters' boundary. One
  exception, by reflection: the Spring manager reads and replaces the builder's hash provider (see "Spring" below).
- **A framework's quirks stay in its package:** the core knows no framework type. What a framework needs of the cache
  (Spring's `SimpleKey` as a key, its `NullValue` as a value) is handled in its integration.
- **No mechanism of its own:** an integration adds no guarantee and no store access; it builds caches, registers
  them, and closes them.

**Spring: `DistributedCaffeineCacheManager`.**

*Built 2026-11-19; specs deferred (see "Open work"). Tested on Spring 7.0 and 6.2, the oldest supported.*

A subclass of Spring's `CaffeineCacheManager`, which builds each cache
in protected methods made for this (checked on Spring 6.2's and 7.0's source: `createNativeCaffeineCache(String name)`,
`createAsyncCaffeineCache(String name)` since 6.1, and `adaptCaffeineCache` to wrap it). The manager overrides the
two creating methods and takes a function from a cache's name to a `DistributedCaffeine` builder, a new one with a new
adapter on every call (see "Adapter ownership"). The name usually becomes the cache's collection, table or keyspace
(decided 2026-11-19): Spring's caches are separate regions, as Spring Data Redis's `name::` prefix has them, each can
be inspected and dropped in the store on its own, and the discriminator stays free to keep applications sharing a
database apart. It costs no feed: listening is shared per database (see "Connection sharing"), and a polled cache
polls on its own either way. The cost is the store's naming rules (MariaDB and MySQL tables: letters, digits and `_`),
documented in the Spring integration's README; the name as discriminator in one shared table had the same rules on
Redis and none of the other gains.
- **Kept from Spring:** caches created by name on first use (or fixed by `setCacheNames`), `setAllowNullValues`,
  `setAsyncCacheMode` (a `DistributedAsyncCache` then, for `CompletableFuture` and reactive return types),
  `registerCustomCache`, and a loader set with `setCacheLoader` or `setAsyncCacheLoader` (the distributed cache built
  with it). Spring keeps the loader in a private field, so the manager overrides both setters to remember it.
- **Closing:** the manager closes every cache it built at shutdown (`DisposableBean`), which `CaffeineCacheManager`
  does not need and does not do. Spring also drops caches without closing them: `removeCache`, `resetCaches` (the
  caches created on demand), `registerCustomCache` under a name it built, and every setter that rebuilds the caches
  it created (`setAllowNullValues`, `setAsyncCacheMode`, `setCacheLoader`; checked on 6.2, `refreshCommonCaches`).
  The manager closes a cache it built once Spring has let go of it, else its connections and feed subscription would
  leak: it keeps the caches it built by name (Spring's map is private), closes the previous one when it builds a
  name again, and after `removeCache`, `resetCaches` and `registerCustomCache` those Spring no longer holds.
- **The builder refused, specs applied:** `setCaffeine` throws: a builder handed over cannot be read without reflection
  (the reason the library takes a configurer, not a builder), so it would be ignored silently otherwise; its message
  points to the function's builder (`withCaffeine`). A spec (`setCaffeineSpec`, `setCacheSpecification`) applies to
  every builder after the function's own settings, through `CaffeineConfigurer.withSpec` (built 2026-11-20, see "The
  builder: Caffeine's methods, no reflection"); the manager remembers it and lets Spring's setter rebuild the caches,
  which it closes as it does for any setter. A setting made by both fails when the cache is built, naming the cache, the
  spec and Caffeine's message. Spring Boot backs off with a manager bean of the application's, so
  `spring.cache.caffeine.spec` is handed over by the application
  (`setCacheSpecification(cacheProperties.getCaffeine().getSpec())`), as the README says.
- **Keys:** Spring's default key generator passes a single argument as it is and wraps several, or none, in a
  `SimpleKey`, which no built-in hasher takes (a key without a hasher fails on first use, checked by `SpringTypesTest`;
  once hashed, a `SimpleKey` crosses the store, its serialized form included). Where the builder sets no key serializer,
  the manager sets `SimpleKeyForySerializer` (decided 2026-11-20): a `ForySerializer` (extending it, so its defaults
  stay one place's) that registers a Fory serializer factory, asked for every class Fory meets, at the top or nested,
  which gives a `SimpleKey` of any class (a subclass of a key generator of the application's included) a serializer
  writing its params (an `Object[]`, as Fory writes any) and reading `new SimpleKey(params)` (a subclass is read back as
  a `SimpleKey`, its own fields aside); every other type, the params included, as by the default `ForySerializer`. Fory
  would write a `SimpleKey` through its Java serialization path (it customizes Java serialization), logging a warning
  once per JVM that it does, in every application on the defaults. The key serializer is read from the builder by
  reflection, as its hash provider; one set on the builder is kept. Keys are serialized for diagnostics only, so the
  bytes are no stored format. Rejected: a serializer registered for the class `SimpleKey` alone (Fory registers by exact
  class, so a subclass would still warn), a text serializer writing `toString()` (quiet and readable, but `toString()`
  is no defined, faithful form of a key). `ForySerializer` gives a subclass its Fory (`fory()`, protected) to register
  serializers of its own, and `JacksonSerializer` its JSON mapper (`jsonMapper()`), an accessor rather than a protected
  field, so the classes keep how they hold them.
  The manager sets `SimpleKeyHashProvider` on every builder, which hashes a key in this order: a `Hashable` by its own
  hash (a `SimpleKey` subclass of a key generator of the application's included); a `SimpleKey` by its params, a custom
  hash built with a `Hasher` as any provider's: `"distributed-caffeine:SimpleKey"` (so that no other provider's input
  begins alike), the number of params, then per param a tag and its data (`0` for `null`; `1` and its 16-byte built-in
  hash, as `putHash` puts it; `2` and a `Hashable`'s or the fallback's hash; `3` and a nested `SimpleKey`'s hash;
  each hash as its 16 bytes), the tag keeping params of different kinds apart; a built-in kind as the core hashes it
  (`Hasher.builtInHash`); anything else by its fallback, if constructed with one, or refused naming the type and the
  fixes (`Hashable`, an explicit `@Cacheable(key = ...)`, a hash provider on the builder). So the provider changes no
  hash the core computes itself. Its output is part of the stored hashes, pinned by a test against hash4j's stream fed
  the same bytes, so it stays stable across JVMs and releases.
  - `SimpleKey` offers no access to its parts: they are read by reflection, a `VarHandle` on its private `params` looked
    up once, `static final` so that the JIT treats it as a constant on the hit path (Spring on the classpath, or as an
    automatic module, is open to it); a renamed field fails on first use, and the tests pin it. Rejected:
    `SimpleKey.hashCode()` (32 bits, so colliding keys would share a row, and built from the parts' `hashCode()`, which
    is per JVM for enums and identity-hashed classes); `toString()` (types lost, `"a, b"` and `"a", "b"` alike); hashing
    Java-serialized bytes (not canonical, every part `Serializable`); a key generator of the library's own (one more
    setup step); reading the parts by a serialization round trip (a full serialization on every access); encodings of
    further part types in the integration (each a stored format to keep forever; enums are built in, in the core, and
    dates and the like are the application's, by `Hashable` or a fallback); reading `InternalHasher` by reflection for
    the built-in types (a package-private class, a method and an internal key type, against `Hasher.builtInHash`, public
    and shared).
  - Hashing is configured where users know it, on the builder, and only for what the integration cannot hash (decided
    2026-11-19): a key of a supported type, a `Hashable` or a `SimpleKey` of these needs no configuration; a hash
    provider set on the builder becomes the fallback of the `SimpleKeyHashProvider` the manager sets, asked only for
    keys and parts of other types. The core's builder has no getter for its hash provider and refuses a second one, so
    the manager reads and replaces the field by reflection, a `static final` `VarHandle` looked up once (a renamed field
    fails on the manager's first use, and the tests pin it). It is the one exception to "Public API only": the builder
    is ours and in the same artifact, and the alternatives cost more. Rejected: public getters on the builder
    (Caffeine's builder has none, and getters for one integration's defaults would be arbitrary among a dozen settings);
    a hash provider passed to the manager's constructor (a second place to configure hashing); a public
    `SimpleKeyHashProvider` the user wraps their provider in (configuration for what the manager can do itself);
    catching the builder's refusal of a second provider (a builder's own provider then replaced the `SimpleKey`
    hashing).
- **`null` results:** with `allowNullValues` (Spring's default), Spring stores its `NullValue` marker for a `null`
  result, serialized into the store like any value. Settled by `SpringTypesTest` (2026-11-19, Spring 7.0): a null
  written by one instance reads as a cached null on another, with Fory (the default) and with Java serialization,
  synchronous and in async mode, since both honor `NullValue`'s `readResolve` and Spring checks the marker by
  identity. So the manager keeps Spring's default, `allowNullValues=true`; `withNegativeCaching` stays the
  library's own way to remember misses of a loader.
- **What changes for the user,** documented in the Spring integration's README (`integration/spring/README.md`):
  `@CacheEvict` and `allEntries = true` act on every instance (an `invalidate` and an `invalidateAll` through the
  store), as does `resetCaches`; values are serialized; in an outage, writes fail with `StoreException`, so a plain
  `@Cacheable` fails on its write after a miss unless a `CacheErrorHandler` logs it, while `@Cacheable(sync = true)`
  loads through the cache and keeps working (tested in `DistributedCaffeineCacheManagerTest`).
- **Spring versions:** 6.2 or newer (Spring Boot 3.4 on); the build compiles and tests against the latest (7.0),
  and 6.2 passed the Spring tests by hand (`-Dspring.version=6.2.19`), since the overridden methods are protected
  API, not a formal SPI. A nightly job on the oldest supported version is open (see "Open work").

**Rejected:** a separate `CacheManager` from scratch (it would redo what `CaffeineCacheManager` does, and differ from
it); `SimpleCacheManager` with hand-built caches as the documented way (no caches by name, nobody closes them);
`SimpleKey` hashing in the core (a Spring type in the core).

**Which framework through which integration** (checked 2026-11-18, their documentation):

| Framework                                   | How its caches plug in                                                                                                          | Served by                                                         |
|---------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| Spring                                      | its `Cache` abstraction; `CaffeineCacheManager` builds Caffeine caches                                                          | `DistributedCaffeineCacheManager` (after 0.9.0)                   |
| Micronaut                                   | `micronaut-cache-caffeine` builds caches from `micronaut.caches.<name>.*`; with a JSR-107 implementation present it uses JCache | the JCache provider; nothing Micronaut-specific needed            |
| Hibernate (second-level cache)              | `hibernate-jcache`, or a `RegionFactory` of its own                                                                             | the JCache provider; a `RegionFactory` only if wanted without it  |
| Quarkus                                     | `quarkus-cache` (Caffeine by default; Redis and Infinispan as extensions of their own)                                          | an extension of its own, an artifact apart (*on demand*)          |
| Vert.x, Dropwizard, Ktor and the like       | no cache abstraction: the application uses a cache directly                                                                     | the plain API, a Caffeine `Cache`, already a drop-in              |

So the JCache provider comes second: one integration for several frameworks, at the cost of JSR-107's API surface.
A Micronaut integration of its own would add only configuration through `micronaut.caches.*`; whether Micronaut
offers a hook to plug in a cache manager directly is unchecked, left until it is asked for.

### Java baseline: 17

Java 17. None of the dependencies (Caffeine, Jackson 3, the MongoDB and PostgreSQL
drivers, Fory, hash4j) require more
without upgrading Java.

- **What 17 already has:** records, sealed interfaces, `switch` expressions, `instanceof` patterns, and exhaustive
  `switch` over enums. Wherever completeness matters (message kinds, row states, formats, conflict results), use
  enums so the compiler checks every case.
- **What is given up:** `switch` over sealed types and record patterns (21). Convenient, not needed by the design.
- **Virtual threads stay the user's choice:** the executor for store I/O (delivery reads, background work)
  is configurable, so users on 21 or later can pass `Executors.newVirtualThreadPerTaskExecutor()`; the library
  does not depend on them.
- **Re-check before the release:** raise the baseline only if a dependency or a needed feature requires it.

## Testing

### Test setup: one store at a time, per image and topology

*Decided 2026-10-23.*

`MongoIntegrationTest`, `PostgresIntegrationTest`, `RedisIntegrationTest` and `JdbcIntegrationTest` hold one `@Nested`
class per image and topology (for Redis: servers, Sentinel, cluster, failover, restarts, ACLs, each client library; for
MariaDB and MySQL: servers, replication, compatibility), which starts its store in `@BeforeAll` and stops it in
`@AfterAll` (`MongoTopologyTests`, `PostgresTopologyTests`, ...); the suites are nested in it and find the running store
(`MongoTestDatabase.get()`, `PostgresTestDatabase.get()`). No store outlives its class, and no two run at once (the
shared per-JVM databases before, stopped only by a shutdown hook, ran next to the containers of single test classes).

- **Images:** each major version's first release and its latest tag, from MongoDB `4.4.0` and PostgreSQL `9.6.5` (9.6.0
  to 9.6.4 crash in their `gosu`, built with a Go runtime of 2016) to `mongo:9` and `postgres:18`. With
  `-DrunsOnGitHub=true` only the latest image of each store runs, every topology of it.
- **MongoDB topologies:** single-node replica set; three-node replica set (fixed host ports, members announced as
  `memberN:port` and resolved to localhost, member 0 preferred, elections and catch-up shortened to 2 s); the same
  with an arbiter; a sharded cluster (config server, two shards writing a no-op every second, router); a single node
  with a 1 MB oplog.
- **PostgreSQL topologies:** single server (fixed host port); primary with an asynchronous standby, and with a
  synchronous one (`pg_basebackup -R`, promoted by `pg_ctl promote`, which every version has); single server behind
  PgBouncer in transaction mode (the listener directly); single server with a notification queue of 256 pages (17 or
  later).
- **Which suite where** (`Topology.suites()`): the contracts on every topology; core, distribution, recovery, parity,
  property and the adapter's own tests on the single node or server; failover on the replica set and the standbys;
  sharding, oplog, pooler and notification queue on their own topology. A suite that breaks its topology (a promoted
  standby) has it built anew after each test. `-DallSuites=true` runs the general suites on every topology;
  `-Dsuites=STORE_CONTRACT,FAILOVER` runs only those, and a topology with none of them starts no store.
- **What the oldest images found** (2026-10-23): MongoDB 4.4.0 and 4.4.1 do not know `hello` (the adapter's deployment
  check and its clock fallback now ask `isMaster` there); MongoDB 4.4.0 refuses `$$NOW` in the filter of a delete
  (pruning tombstones now finds first and deletes by version, as pruning to size did); a new primary on 4.4.0 waits
  for catch-up with an unreachable old primary indefinitely by default (`catchUpTimeoutMillis`; the replica set of the
  tests bounds it, an operations note for the README).
- **Version-bound tests:** losing a confirmed write in a failover needs an asynchronous standby and PostgreSQL 13
  (`primary_conninfo` reloaded); `minSnapshotHistoryWindowInSeconds` exists from MongoDB 5.0 (the small oplog
  truncates without it on 4.4).

The full matrix runs nightly on GitHub (`nightly-full-matrix-tests.yml`, the reference run, in parallel groups, model
checking included); while working, the image and suites a change touches
(`-Dtest='MongoIntegrationTest$Mongo_9_latest*' -Dsuites=...`), and local matrix runs fill a change's gaps before the
nightly (see "How we work on it").

### The property test

*Built 2026-10-21.*

`InternalPropertyTest`: three instances (a plain one with a 4-entry L1 and a bulk loader returning an extra key, one
with population and negative caching, an asynchronous one) on the reference store; per instance a writer and a loader
thread, 40 phases of 6 operations, every store call and the source with up to 200-300 µs latency; per phase one
fault at a random moment (paused delivery, gap, interruption with the missed messages delivered after the resume or lost, a failed write with
known or unknown outcome, a failed read), plus duplicates, reordering and delivery delay at random. After each phase
the threads are joined, the faults healed, and the test waits until neither the store nor any instance has anything
left to deliver or revalidate (test-only idle checks), then checks two oracles:

- **Freshness (L2):** the source is modelled; writers change it and then the cache under a lock per key (an
  application serializes the writes to one record; `invalidateAll` takes every lock, since the conditional methods
  update the source after the cache), loaders read it. Values are unique tokens. For every key the store holds no
  value or the source's current one.
- **Convergence (L1):** every instance answers `getIfPresent` with exactly what the store holds.

On failure it prints the trace of the key (source changes, every store call with its result, faults). 8 seeds,
about 7 s. It found five defects at once, all fixed with regression tests: `remove(k)` without a value did not
defeat a load in flight (above); extras were written over tombstones and held by Caffeine outside the version rule
(Shared loads); an absence was recorded by a load whose future `invalidateAll` had dropped from L1, so a write
announced meanwhile could not reach it, and likewise across a gap (Negative caching).

Mutation scores (seeds failing of 16, two runs of 8): loads writing unconditionally 15, no minimum version for loads
in flight 11, no write-registry check 4, `remove(k)` without a tombstone 3, everything trusted 3, the
`invalidateAll` message not moving the epoch 1; the absence checks, L1 installs ignoring versions and extras handed to
Caffeine 0. Every one of them is killed by a targeted unit test, and for the last group the defences overlap (the
write registry also covers a commit ignoring versions; the epoch also covers an `invalidateAll` dropping a load's
future). The property test is a net with a low rate per run for rare interleavings (the absence defect showed up
in about one run in eight before it was fixed), not a substitute for the targeted tests: a finding becomes one.

### In-memory store profiles

*Decided 2026-10-29.*

**Decided 2026-10-29:** the in-memory store can act as every kind of store met so far, each behaviour found in a real
store a parameter, so that the core is built and tested against all of them in seconds, before PostgreSQL and MongoDB
are changed. One table with parameters (a `StoreProfile` that `InMemoryDatabase` takes), not a class per store, so
that the variants do not drift apart.

**Already there** (`InMemoryDatabase`, `StorageOnlyStore`, `ScanOnlyStore`): the store down; reads and writes failing,
writes also with their outcome unknown (applied, the answer lost); the feed interrupted (missed messages delivered on
resume or not), a gap, delivery paused, duplicates, reordering across keys, delivery and commit delays; hooks before a
conditional write and before an `invalidateAll` applies; a controllable store clock and a seed; stores without a feed
and with a scan only (polled).

**Missing, as found in real stores so far:**

| Dimension                                 | Values                                                                       | Found in                                                                      |
|-------------------------------------------|------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| Applying an advance                       | conditional / assigning, chosen per call (single, batch)                     | all; MongoDB mixes (single assigning, bulk conditional)                       |
| Floor of a new row                        | the proposal / a sequence of its own / the store's time in the core's format | generic JDBC / PostgreSQL / MongoDB, Redis                                    |
| Batch                                     | atomic / per row, partly failing                                             | PostgreSQL / MongoDB's unordered bulk write                                   |
| Version-conditioned write on a purged row | nothing / a tombstone                                                        | MongoDB (step 5)                                                              |
| Store clock                               | resolution (micro- or milliseconds), stepping back by up to an amount        | PostgreSQL µs, MongoDB ms; NTP                                                |
| Read of several keys                      | one snapshot / not one                                                       | PostgreSQL / MongoDB's `$in` (matters for the control row read with the keys) |
| Failover                                  | acknowledged writes lost, versions back to older ones, a gap in the feed     | MongoDB with `w:1`, Redis's asynchronous replication                          |
| Eviction                                  | none / rows deleted silently, without tombstone or message                   | Redis with `maxmemory`                                                        |
| Feed                                      | per key in commit order / lossy / history lost                               | all / Redis pub/sub / MongoDB's `CappedPositionLost`                          |

**How it is used:**

1. A named profile per real store (PostgreSQL, MongoDB, generic JDBC, Redis / Valkey as a candidate) and hostile ones
   (a store clock stepping back, a failover losing writes, silent eviction, a client clock far ahead).
2. The contract tests run against every profile; Lincheck and the property tests against the real-store and the
   hostile profiles.
3. The contract tests of the real stores check that each behaves as its profile, as far as it can be checked: the
   profile is the documented, tested description of the store.
4. A behaviour newly found in a real store enters as a dimension or a value of the profile first, so that the core is
   fixed against it in seconds, then on the store.

**Not covered:** performance. The in-memory store is more ideal (atomic batches, no lock waiting, no network);
correctness carries over through the contract tests, the cost of a hot key only shows on containers.

**The remaining dimensions, designed 2026-10-29** (versioning is built, see build step 6b; one option of it is still
to add: a refusal that reports what is stored, as Cassandra's lightweight transactions return the current row when
they do not apply, and PostgreSQL could with `RETURNING`, so that a conditional store saves the read after a
refusal; added 2026-10-29). Each is a field of
`StoreProfile` with the value the named profiles take, a fault the tests can trigger where the behaviour is an event
rather than a property, what the core must do, and the tests that should hit it. In the order to build them, by the
risk they carry for the core:

0. **Change source** (`changes`: a feed, a scan the core polls, or none). Added 2026-10-29: until now polling was
   tested beside the profiles (`ScanOnlyStore`, `InternalPollingTest`, a variant of the property tests), and
   `-Dinmemory.profile=jdbc` still ran with a feed, which no JDBC store has. Values: a feed (`postgresql`, `mongodb`,
   `redis`), a scan polled by the core through `InternalPollingStore` (`jdbc`, and every store without a feed of its
   own), none (only `DistributionMode.NONE`, as `StorageOnlyStore`). Built from what exists (the in-memory store
   offering only its scan, as `ScanOnlyStore` does), so that every suite run under a polled profile goes through
   the polling store. Its sensitive points follow the other dimensions, and each of them gets a polling test:
   - the poll window is measured in the store's time (since the last poll, plus an overlap of 2 s): a store clock
     stepping back by more than the overlap puts writes before the window (dimension 1). Expected: less than the
     overlap costs nothing; more, a write of another instance is missed until its maximum age (one of this instance,
     late, is touched again). **Corrected 2026-10-29, when it was built:** the window is a duration back from the
     store's now (the instance's time since its last poll, plus the overlap), open towards the future, so a step
     *back* costs nothing at all (a write after it is young, one before it has a negative age: both inside;
     `InternalStoreProfileTest`). The edge is a step *forward* by more than the overlap: the writes between the last
     poll and the step look older than the window and are missed, by another instance until something else about
     the key arrives (an L1 copy has no age bound of its own, see dimension 7);
   - a write visible only after its time left the window (a long transaction; `setCommitDelay` exists): this
     instance's own late writes are touched again, those of others are bounded by the overlap;
   - a scan that is not one snapshot sees a write and the control row in either order (dimension 2);
   - a row deleted without a tombstone (eviction, dimension 6) never appears in a scan: stale until the maximum age,
     a cost of polling as of a lossy feed; **found when built (2026-10-29), open:** size pruning is such a deletion
     too. The property test with pruning (at most 4 rows for 8 keys, maintenance every 5 ms) fails under the polled
     `jdbc` profile: a row written again and pruned before the next poll never shows in a scan, and the holder keeps
     its older value with no bound (an L1 copy has none, see dimension 7). It needs the cache to turn over its whole
     maximum size within one poll interval. Option: size pruning spares rows younger than the poll window (interval
     plus overlap) under polling, an SPI parameter (`pruneToSize(maxSize, minAge)`); the variant is skipped under
     polled profiles until then. **Found 2026-10-29, same root, also with a feed (open):** the variant failed once on
     the `mongodb` profile: a slow load found the key missing, a put wrote a newer value, size pruning deleted that row,
     and the load's write on "missing" then applied, leaving the stale loaded value in the store (freshness). A silent
     deletion erases what a condition relies on, as an eviction does (dimension 6). Sparing young rows from size
     pruning narrows it to loads slower than the spared age. **Also found and fixed with it:** a poll reports every
     change within its overlap again, and a re-report arriving while an operation on its key was in flight dropped
     that operation's value (the core cannot compare versions yet), seen as a parity failure under `jdbc`; a polled
     subscription now delivers each change (hash and version) once. **Decided 2026-10-29: both let go, documented.**
     Size pruning takes the oldest rows first (contract 7), so a just-written row goes only when the cache turns over
     its whole maximum size within one poll window (the first) or within one load (the second): a maximum size far
     below what is written per second, a cache dropping everything almost as soon as it is written. A pruned row's
     value was valid, its copies elsewhere stay (as Caffeine's own size eviction leaves other caches alone). Since
     every copy expires at the store's maximum age at the latest (dimension 7), both are bounded: a polled holder
     keeps an older copy, and the store an older loaded value, up to the maximum age. The property test's pruning
     variant (at most 4 rows for 8 keys, pruned every 5 ms, thrashing on purpose) allows any value the source once
     held, and is skipped under polling. Researched first: Redis (client-side caching) and Hazelcast (near caches)
     announce an eviction as an invalidation, their protocols knowing only "changed"; the marker below is the
     version-aware form of it, kept for a later discussion (Later). **Revised and built the same day: a grace for
     young rows, older ones let go.** `Store#pruneToSize(maxSize, minAge)`: the rank still counts every value (the
     newest `maxSize` kept), but a row beyond it goes only once it is `minAge` old; the maximum size is exceeded
     meanwhile, by at most what was written within that age. The core passes `max(4 s, 2 × poll interval + 2 s if
     polled)`: polled, the window in which a poll can still find a write (the poll interval with its jitter and the
     poll's own time, plus the overlap for writes committed after their time); with a feed or without distribution
     only the floor, for slow loads, since a message carries its version and needs no row (a design
     whose notifications name a record must keep every written record a while, for each instance to read it). That
     closes the first fully and the second for every load shorter than the grace (a slower one stays bounded
     by the maximum age). Normal operation is unchanged: pruning reaches rows that young only when
     the cache turns over its whole maximum size within the grace. In the four stores a condition on the deleting
     side (PostgreSQL's `DELETE`, MongoDB's found documents by their projected age, the primitive store and the
     in-memory store by the row's age); a new contract test. The property test's pruning variant runs strict again,
     under polling too;
   - a scan failing longer than the tombstones' grace minus the overlap may have missed deletes, reported as a loss
     (tested in `InternalPollingTest`; then under the profile too).

   A fourth value, planned with it but built only after step 6b (robustness first): **a bus**, the library's own
   messages over a transport of the user's choice ("What follows: pluggable message buses"). It needs an in-memory
   bus with faults of its own (messages dropped silently, reordered across senders, duplicated, delayed), and
   tests that writes from outside the library reach the instances only at expiry. A bus message carries the version
   the store reported (`WriteResult`), so an assigning store works with it unchanged; across senders the versions
   order the messages, as with any feed. Polling and a bus stay alternatives for a store without a feed of its own:
   polling also sees writes from outside, slower and at the store's cost; a bus is fast and cheap but sees only the
   library's writes.
1. **Store clock** (`clock`: resolution and stepping back). Today the in-memory store hands out write times strictly
   increasing; a real one counts in its unit and its wall clock can step back. Values: microseconds, repeats allowed
   (`postgresql`, `jdbc`, `redis`), milliseconds, repeats allowed (`mongodb`); none, the writer's time within a bound
   (added 2026-10-29: DynamoDB, partly Cassandra, which take no server time in a write: the fallback "for stores
   without a global counter", decided 2026-10-26, where the writing instance stamps and `invalidateAll` waits 2ε);
   a fault `stepClockBack(Duration)`.
   The core relies on it for the control row (`invalidateAll` waits `clockAllowance()` so that no later write is
   stamped at or before it) and for ages. **Found when built (2026-10-29):** the wait was `toMillis()`, so an
   allowance below a millisecond (a clock in microseconds) was not waited at all; now in nanoseconds. **Found when built, open:** the allowance covers a write after `invalidateAll`
   returns, not one concurrent with it. A load that read the control row while `invalidateAll` still waited out its
   allowance, and wrote then, was stamped before the control row when the clock stepped back between the two (the
   property test under `postgresql-stepping`; a clock repeating within its unit does the same, more rarely): the
   store hides the row, the writing instance holds the value at the version applied, with no bound (an L1 copy has
   none). Option: for the allowance after an `invalidateAll` it applied (its own or announced), an instance installs
   what it writes as untrusted, checked on its next read. The property test steps the clock between phases until
   then. Seen again on the plain `mongodb` profile (2026-10-29, two property cases of 20 in one run, none in
   the run before): a clock in milliseconds stamps a load written in the same millisecond as the control row equal
   to it, which hides it alike; not a hostile case, then. **Fixed 2026-10-29** (`distrustWithinAllowance`): one allowance after an instance
   applied an `invalidateAll` (its own, or announced), its epoch moves once more, so that whatever it installed
   meanwhile is untrusted and checked on its next read (one row read; a hidden row dropped). An operation that
   begins later commits more than the allowance after the control row, since the instance applied it no earlier
   than it was written; one that began before installs under the older epoch anyway. A store without allowance (a
   monotonic clock) needs nothing. Tested deterministically (a clock standing still, a put on another instance right
   after the announcement: served for good without it, dropped after the allowance with it), and the property test
   steps the clock within the phases again. Tests: an `invalidateAll` and a write in the same millisecond, a clock
   stepping back by less than the allowance (nothing may change) and by more (the edge the allowance bounds, stated
   in the contract: a later write hidden as if before the `invalidateAll`). The contract tests of the store clock
   (point 2) run against both resolutions.
2. **Read of several keys** (`snapshotReads`: true or false). Values: true (`postgresql`, `jdbc`: one statement;
   `redis`: one script), false (`mongodb`: a `find` with `$in` reads document by document; `$$NOW` stays one instant
   for the ages, the documents do not). In the in-memory store: a read row by row with a hook between the rows, where
   a test commits a write. What the core must hold: the control row is read with the keys, and a read that saw the
   control row before an `invalidateAll` and a key after a write following it must still decide each key correctly
   (the age compare within one read, Store point 2). Tests: a hook between rows writing the key, the control row,
   or both; the property tests with the hook firing at random. **Found when built (2026-10-29):** PostgreSQL,
   MongoDB and the JDBC store raised a negative age to zero (a row written after the read's instant: MongoDB's
   `$in`, or a clock stepped back). Two such rows then tie, and a tie hides the younger behind the control row: an
   `invalidateAll` and a put landing between the rows of one read hid the put, and a load conditioned on the row
   replaced it (the test fails with the clamp put back). Now every store reports an age as it is (the SPI says so,
   `Store` point "Clock", `Row#age`), and the core raises it to zero only where it shows it or derives an expiry
   from it (`StoredValue`, a load's age).
3. **Batch** (`batch`: atomic, or per row and partly failing). Values: atomic (`postgresql`: one statement; `redis`:
   one script), per row (`mongodb`: the unordered bulk write; `jdbc`: the primitive store writes row by row). A fault
   `failNextWritesPartly(count)`: a batch fails after applying some of its writes, reported with
   `StoreException#afterPartlyApplied` (in the SPI already). What the core must do: treat every key of the batch as
   outcome unknown (`dropOlder` at `ANYTHING`, built 2026-10-29). Tests: `putAll`, `invalidateAll(keys)` and a bulk
   load with a partly applied batch, then a read on every instance.
4. **Failover losing writes** (`failover`: none, or acknowledged writes lost). Values: none (`postgresql` with a
   synchronous standby, `jdbc`), writes of a recent window lost (`mongodb` with `w:1`, `redis`: asynchronous
   replication). A fault `failover(Duration window)`: the rows go back to their state before the window (versions
   back to older ones, rows written in it gone) and every subscription is told about a gap. What the core must do:
   revalidate after the gap and drop what the store no longer holds, also a value at a version the store no longer
   has (equality, not order, decides a revalidation). Tests: a put acknowledged and then lost, held on two
   instances; a write after the failover at a version below the lost one (the clocks are ahead of the store now); a
   write after the failover at the lost one's very version (found 2026-11-10 on Valkey, a test's virtual machine
   resumed: a store assigning versions, onto a clock no further than the old primary's, gives a later write of an
   instance that never saw the lost version that version again), which a holder of the lost value took for its own
   until the value aged out, and a decision to write nothing made on it (`replace` answering `false`) was confirmed
   by the later write's version. So a revalidation, and the read confirming such a decision, compare the value too,
   serialized as the store holds it, not the version alone: exact, at the cost of reading values instead of versions
   when copies are checked (after a gap, while delivery is interrupted). Tests: two in `InternalStoreProfileTest`, and
   the situation in `InternalPathParityTest` (fault `reuseVersion`) through every read path and every atomic decision
   writing nothing, on the profiles with a feed of their own: comparing versions only, 111 of 125 cases failed. In
   the deterministic property test's faults too (a version reused for another value, and instance clocks off the
   store's by up to 90 s): comparing versions only, 399 of 600 runs failed. And a model of the version protocol whose
   every reachable state is searched (`InternalVersionModelTest`: two instances, a store with a replica, writes,
   reads, messages, clocks, a failover, a restart, versions assigned as on Redis or proposed as on the conditional
   stores): comparing versions only, a four-step counterexample (a write, a failover losing it and its message, a
   write by the other instance at its version, a decision on the stale copy); comparing versions and values, none in
   about a million states per way of assigning versions. A
   rule that made assigned versions unique by the proposing instance's node was tried and dropped: it left one case in
   1024, and versions decide what a reader sees only where they are exact.
5. **Version-conditioned write on a purged row** (`tombstoneOnPurgedRow`: true or false). Values: true (`mongodb`,
   the bulk write's upsert, step 5), false (the others). The contract allows both; today the in-memory store only
   produces the second, so the first is tested only on MongoDB's containers. Tests: the contract tests (they accept
   both already) and the property tests with maintenance purging tombstones under writes.
6. **Eviction** (`eviction`: none, or rows deleted silently). Values: none (all but one), silent (`redis` with
   `maxmemory` and an eviction policy, the hostile profile). A fault `evict(hash)`: the row deleted, no tombstone, no
   announcement. What the core must do: nothing wrong follows from a holder keeping a value the store forgot (no
   newer write exists), but the row's version is gone: the floor of a new row must come from elsewhere (the
   proposal, or the store's own sequence or time). Tests: evict, then write on another instance; the holders must
   take the new value. **Found when built (2026-10-29), fixed the same day (below):** a write after an eviction
   applies at whatever its writer proposes (a conditional store; an assigning one at its floor). A proposal from the same
   millisecond as the evicted write, or within the clocks' skew of it, can be *below* the version a holder keeps; the
   holder takes the message for one of an older write and keeps its value until something else about the key
   arrives. Seen on the `jdbc` profile in about one run in two when the write follows within the millisecond; a write
   a few milliseconds later is safe (the test writes so). The same holds for rows deleted by size pruning, though
   pruning deletes the oldest rows, far from that window. Options: a message below the held version makes the holder
   read the version (one `readVersions` per such message, which also arrive for reordered echoes of its own writes);
   or a store with its own floor (a sequence, the server's time beyond the skew); or accepting it for evicting stores,
   as documented. **Fixed with the first:** a message below the held version (`apply`) is no longer
   only ignored; the key is checked against the store in one read per delivered batch (`check`, as after a gap): the
   same version and value are kept and confirmed, anything else dropped, and a store that does not answer leaves the
   value held. It covers a lower tombstone or expiry after an eviction too. Tested deterministically (a write straight
   to the store below the held version after an eviction; the holder kept `v1` for good without it) and by the eviction
   test without its pause. Cost: such messages also come as late echoes of an instance's own quick successive writes to
   one key, each then one read; to be measured with the contention benchmark.
7. **Feed losing messages silently** (`feed`: per key in commit order, or silently lossy). The losses of real feeds
   are reported (a gap, an interruption: `gap()`, `interruptSubscriptions()` exist); a feed losing messages without
   telling (Redis pub/sub across a disconnect it did not notice) is the hostile case. A fault
   `dropMessages(probability)`. What the core must do depends on the change source (dimension 0), and so do the
   tests:
   - a store's feed: the heartbeat notices a silence only if a beat is lost too; otherwise a holder is stale until
     its value's maximum age. Tests: the bound itself (no value older than the maximum age is served), stated as
     what such a feed costs, not as a guarantee the core can give. **Corrected 2026-10-29, when it was built:** with
     distribution, an L1 copy has no age bound of its own (the store's maximum age bounds rows, not copies; only
     `DistributionMode.NONE` sets one by default). The bound is the expiry the application sets
     (`expireAfterWrite`); without one, a holder is stale until the next message about the key or a gap. Tested
     with an expiry. Open: whether a store declaring a lossy feed should make the core bound copies by the maximum
     age. **Decided and built 2026-10-29:** every copy expires locally at the store's maximum age at the latest, in
     every distribution mode, whatever the feed (`CaffeineConfigurer#caffeine(Duration)`): no expiry set gives
     `expireAfterWrite(maximumAge)` (as `DistributionMode.NONE` had), a longer `expireAfterWrite` is lowered to it,
     and a user's `Expiry` is capped at what is left of it since the copy was installed (a read never lengthens it
     beyond). With a reliable feed nothing changes but the lag of up to one maintenance interval: maintenance expires
     the row at that age and its message drops the copy anyway; where that message is lost, the local expiry does
     it. Counted from the install, so a copy of a row already old lived up to twice the maximum age after the write.
     **Made exact 2026-11-11:** every held value carries its row's age as read (`Row#age`, 0 for a value written
     here), and the copy goes at the install plus what was left of the maximum age then: the maximum age after the
     write, late by at most the round trip between the store's read and the install. Caffeine's fixed expiry is one
     duration for all copies, so with a maximum age L1 always expires variably (`InternalExpiry`), emulating
     `expireAfterWrite`/`expireAfterAccess`; `InternalPolicy` shows them as Caffeine's fixed policies (durations,
     ages, order, a new duration applied to the held copies at once) and hides the variable policy unless the user
     set an `Expiry`, whose `setExpiresAfter` is capped too. Without a bound (none set, or for the store only),
     Caffeine's own fixed expiry as before. `policy().expireAfterWrite()` is present on every cache without an
     `Expiry`. Rejected: a periodic check of the held copies against the store (a version read of everything held, every
     round, for nothing with a reliable feed); kept for a later discussion (Later);
   - a bus (once built): every loss is detected, by the sequence each sender numbers its batches with and its
     heartbeats; the epoch moves and what is held is revalidated. Tests: a dropped message noticed within a
     heartbeat, and no stale value beyond the delivery delay.

The named profiles then read: `jdbc` (conditional, polled through its scan, microseconds, snapshot reads, per-row
batch, no failover loss); `postgresql` (its own versions, microseconds, snapshot reads, atomic batch, no failover loss,
feed in commit order); `mongodb` (mixed, milliseconds, no snapshot reads, per-row batch, failover loss, tombstone on a
purged row, feed whose history can be lost); `redis` (the core's domain, microseconds, snapshot reads, atomic batch,
failover loss, feed whose history can be lost, eviction only in its hostile variant). A hostile profile is a real one
with one dimension at its worst.

**The stores surveyed, mapped onto the dimensions (2026-10-29).** Researched from vendor documentation for 18 stores
(Redis, one of the four profiles, checked against its documentation for the first time): MySQL 8, SQL Server, Oracle, CockroachDB, YugabyteDB, Spanner, DynamoDB, Cassandra,
ScyllaDB, Aerospike, etcd, Cosmos DB, Redis 7, Valkey 8, Couchbase, Firestore, Dragonfly, KeyDB. Many single points
the sources did not confirm are marked so in the research and taken here as likely, not as fact; only PostgreSQL,
MongoDB and MariaDB were tested by us.

| Dimension             | Options confirmed                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | New options the survey adds                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Versioning            | conditional: every store; assigning with return, one item: PostgreSQL, CockroachDB, YugabyteDB, SQL Server (`OUTPUT`), Oracle (`RETURNING INTO`), MongoDB, Redis/Valkey/Dragonfly (script), Aerospike (`operate`), DynamoDB (`UpdateItem`, an increment only: no `max`), Firestore (`maximum` transform); in a batch: PostgreSQL family, SQL Server, Redis family (one slot), Aerospike (per key), Firestore (`commit`); none: MySQL (no `RETURNING`; `LAST_INSERT_ID` for one row), Cassandra | **create-then-update**: the advance (`stored < new`, or no row) is two steps where a missing row fails the condition (Cassandra/ScyllaDB `IF v < ?`, Cosmos patch with a filter, Couchbase without a transaction): an insert-if-missing, then an update guarded by the version, the second possibly refused; **refusal reports the row**: DynamoDB (`ReturnValuesOnConditionCheckFailure`), Cassandra LWT, etcd (failure branch), Redis family (script), but none of the SQL databases (at most "0 rows"; MySQL only the 1/2/0 code) |
| Store clock           | microseconds, wall clock: PostgreSQL, MySQL, Oracle, YugabyteDB, Redis family (`TIME`); 100 ns, wall clock: SQL Server; milliseconds: MongoDB, Firestore, Aerospike (last-update time)                                                                                                                                                                                                                                                                                                         | **monotonic store clock**: CockroachDB (HLC, `cluster_logical_timestamp()`), Spanner (TrueTime commit timestamp), Couchbase (CAS: an HLC per vBucket, which a future CAS can poison), etcd (revision: logical, no time at all); **none in a write**: DynamoDB, Cosmos (only in a stored procedure), Cassandra (client timestamps by default)                                                                                                                                                                                         |
| Change source         | feed: PostgreSQL, MongoDB, CockroachDB changefeeds, Spanner change streams, SQL Server CDC, MySQL binlog, Oracle LogMiner, YugabyteDB CDC, DynamoDB Streams, ScyllaDB CDC, etcd watch, Cosmos change feed, Couchbase DCP, Firestore listeners, Redis Streams (written by the script); scan: any SQL database                                                                                                                                                                                   | feeds with **a retention that runs out**, each with a detectable error: DynamoDB 24 h, YugabyteDB 8 h (at most 24), SQL Server CDC 72 h, ScyllaDB 24 h, Spanner 1–30 days, MySQL binlog 30 days, etcd until compaction (`compact_revision`); feeds that **miss changes silently**: Cosmos latest-version mode (no deletes, no intermediate versions), Aerospike XDR (skips versions under lag, ships no expirations), DynamoDB (no record for a write that changes nothing), Redis keyspace notifications (fire and forget)          |
| Read of several keys  | one snapshot: PostgreSQL, MySQL, Oracle, CockroachDB, YugabyteDB, Spanner, etcd, Redis family (`MGET` or a script, one slot), DynamoDB `TransactGetItems`                                                                                                                                                                                                                                                                                                                                      | item by item: MongoDB, DynamoDB `BatchGetItem`, Cassandra, Aerospike, Couchbase, Cosmos; **SQL Server only with snapshot isolation** (READ COMMITTED takes locks; Azure SQL has RCSI on)                                                                                                                                                                                                                                                                                                                                             |
| Batch                 | atomic: every SQL database (per statement), etcd, Redis family (one slot), DynamoDB `TransactWriteItems` (100 items, one outcome), Firestore `commit`, Cosmos (one partition key)                                                                                                                                                                                                                                                                                                              | per item, partly failing: MongoDB, DynamoDB `BatchWriteItem`, Cassandra unlogged, Aerospike, Couchbase, Firestore `batchWrite`                                                                                                                                                                                                                                                                                                                                                                                                       |
| Failover              | no loss of committed writes by default: CockroachDB, YugabyteDB, Spanner, etcd (consensus); by setting: MySQL (semi-sync `AFTER_SYNC`, Group Replication), SQL Server (synchronous commit), Oracle (Maximum Protection), Couchbase (durability levels, with failover limits), Cassandra (`QUORUM`), Aerospike (strong consistency)                                                                                                                                                             | **loss whatever the setting**: Redis and Valkey (`WAIT` and `WAITAOF` do not prevent it, says Redis), Dragonfly; **several writers, the last by time wins**: DynamoDB global tables (default mode: conditions checked against the local copy only), Cosmos with several write regions, KeyDB multi-master. There the store cannot enforce that versions only grow: such a setup is unsupported (documented), or a hostile profile                                                                                                    |
| Deletion by the store | TTL with a delete in the feed: CockroachDB, Spanner, DynamoDB, Cosmos (all-versions mode), Couchbase, etcd (leases), Firestore                                                                                                                                                                                                                                                                                                                                                                 | **late TTL**: DynamoDB ("within a few days"), Firestore (24 h), Couchbase and Redis (on access or in a sweep): an expired item stays readable and writable until then; **eviction**: Redis family (`maxmemory`), Aerospike, Couchbase ephemeral buckets                                                                                                                                                                                                                                                                              |

What follows for the plan:
- **Versioning** gets two options: create-then-update (the advance in two steps) and a refusal that reports the row.
- **The store clock** gets "monotonic" as a value besides "wall clock" and "none": a store with one needs no clock
  allowance for `invalidateAll` at all.
- **The feed** gets "retention runs out, detectably" (a gap, as now) and "misses changes silently" (dimension 7).
- **Failover** gets "several writers, last by time" as a configuration the library refuses or at least documents.
- **Caches are no stores** (recorded 2026-11-02, the user's questions on Pelikan and memcached-compatible servers):
  the memcached protocol has no change feed, no scan by write time (so no polling either), `CAS` on one key only (no
  atomic batch, no version check with an announcement), no ordered structures for maintenance, and no replication;
  its servers (memcached, Pelikan, ElastiCache for Memcached, mcrouter in front of them) evict by design. Even
  `DistributionMode.NONE` needs the store's atomic writes and maintenance. No adapter for them. Couchbase speaks
  memcached's binary protocol too, but is a candidate only through its own SDK (DCP feed, CAS with durability), as
  listed above.
- **Profiles worth naming besides the four**: `cockroachdb` (assigning, monotonic clock, snapshot reads, no loss,
  changefeed), `dynamodb` (conditional with the row on refusal, no store clock, item-by-item reads, per-item batch,
  streams of 24 h), `cassandra` (create-then-update, row on refusal, client clock, item by item, per-row batch, CDC
  hard to consume), `etcd` (row on refusal, a logical clock only, snapshot reads, no loss, watch until compaction).
  Dragonfly fits `redis` (only `EVAL`, no Functions; keys declared; `disable-atomicity` must stay off); KeyDB too,
  but only with one master, and it has not been released since 2023.
- **MySQL and MariaDB differ in assigning** (checked 2026-10-29): MySQL has no `RETURNING` (`LAST_INSERT_ID(expr)`
  returns one value per statement) and no sequence, so `mysql` stays conditional; MariaDB has `INSERT … RETURNING`,
  multi-row and syntactically with `ON DUPLICATE KEY UPDATE` (whether the updated values come back is not stated),
  `UPDATE … RETURNING` only from 13.0 (single-table), and sequences, so `mariadb` could assign from 13.0 and stays
  conditional below, as our JDBC store is now. Both: `NOW(6)` in microseconds (a wall clock), one snapshot per
  statement, atomic statements, the binlog only through external CDC, asynchronous replication by default.
- **Galera Cluster (MariaDB, several writers) is a hostile case**: Jepsen's analysis of 12.1.2 (2026) found lost
  updates and stale reads in a healthy cluster without faults ("read-modify-write patterns … are likely unsafe"),
  and lost committed writes under crashes and partitions; a conditional write by version is exactly such a
  pattern. Its value is "several writers": unsupported, or a hostile profile.

### Thread safety and linearizability

*Since 2026-10-24.*

**Static: `@GuardedBy`** (Error Prone's check, the annotations come with Caffeine) on the lock-guarded state of
`InternalStoreGuard`, the poller, `MongoClusterTime`, `MongoWatcher` and `PostgresListener`, and on the helpers that
run under a lock. A registration's `generation`/`lost` are guarded by its watcher's or session's lock, which the check
cannot express: a comment says so. Found and fixed: `MongoWatcher.Watcher.register` read the watcher's `generation`
without its lock (a registration during a reopen could get an old one), and the watching thread wrote `resumeToken`
without the lock that a reopen on another thread reads it under (now only the current cursor's position, locked).

**Dynamic: Lincheck** (3.7, test scope; `AbstractLinearizabilityTest`): one instance without background threads
(`DistributionMode.NONE`, store and Caffeine tasks on the caller, the key as its hash since model checking does not
support hash4j's `VarHandle`s), its operations from 3 threads against a plain map with a loader. Stress mode in every
build (seconds), model checking on request (`-Dmodelchecking=true`, about 3 minutes). `invalidateAll` only on one key:
like Caffeine's, it is not atomic across keys (model checking showed a reader seeing it on one key, not yet on another).
Found and fixed, all on one instance:

- **A removal racing a write in flight** (`invalidate`, `invalidateAll(keys)`, a compute removing): the write's
  commit put its older value back into L1, where nothing held the removal's version; without distribution nothing
  ever corrected it (until expiry). A removal now records its version for the writes in flight, as a message does
  (`dropOlder`).
- **A window between a commit and `endWrite`:** a reader could see a value older than a removal that had returned.
  Installing into L1 now checks the write registry's minimum under the registry's lock of the key.
- **A load replaced while in flight** (a write's commit replaces a load) completed a future no longer held, leaving
  the older value although it had read the newer: it now installs its value under the version rule.
- **`getIfPresent` during a load in flight** answered `null` after another reader had seen the load's value in the
  store (monotonic reads broken): it now reads the store then.

**Was open, closed by the decision's cut-off (next section): a load racing `invalidateAll`, and a compute between its
write and its check.** The load read the store (and
the source) before the cut-off, writes after it, and its check after the write undoes it by a conditional tombstone;
in between, a compute (any instance) reads the value, builds on it, and its write survives, so the undo conflicts.
A value the load disowned survives through the compute (Lincheck: `compute` and `get` both answer the compute's result
built on the load's). `InternalLinearizabilityInvalidateAllTest` is disabled until decided.

### Linearizability across two instances

*Since 2026-10-25.*

`AbstractTwoInstanceLinearizabilityTest`: two instances on one store, without distribution (they learn of each other
only through the store, so L1 is as stale as it gets, the hot path's hardest case; nothing runs in the background),
three threads, each operation on either instance, three keys. The operations: the ones atomic across instances (the
`asMap()` compute methods, `putIfAbsent`, `put` returning the previous value, `remove`, `remove(k, v)`, `replace`,
`replace(k, old, new)`), `put`, a read of the store (`store().get`), and `invalidateAll`, on several keys this time:
its cut-off is one write, atomic for the store's readers (L1 reads are not among the operations: across instances
they are eventually consistent, by design). Against one plain map. Two variants: a store checking the basis (the hot
path taken) and one not (as MongoDB). Both pass ten stress runs in a row; a mutant store claiming the check without
refusing (the hot path unprotected) fails in every run. Model checking (on request, as for one instance) passes
for both, about two minutes each.

### Deterministic simulation

*Built 2026-11-03.*

**The goal:** a failure reproducible from its seed, the paths that wait for time
(the polls' grace of 5 minutes, the heartbeat's 10 s, the store guard's suspension, the pruning grace, the clock
allowance) tested in milliseconds, and the message timing of the store under one seed.

**The constraint found first:** the core blocks (about a hundred joins, locks and waits in `InternalCore`): a
single-threaded simulation on virtual time, FoundationDB's way, would need the core rewritten around asynchrony. Out
of proportion; the threads stay real, and what they depend on becomes controllable:

- **A. The environment seam** (core, package-private, no API): `InternalEnvironment` gives the time (`nanoTime`, the
  wall clock), sleeping, scheduling and randomness; real by default, a simulated one injected by tests in the core's
  package (`DistributedCaffeine#withEnvironment`, package-private). Its users: `InternalVersion` (wall clock, the
  random node), `InternalStoreGuard` (timing, back-off), `InternalPollingStore` (timing), `InternalCore` (timing, the
  allowance's sleep, the revalidation's pauses), `InternalMaintenance` (the shared scheduler, jitter). Caffeine's own
  clock and scheduler are set through `withCaffeine` already.
- **B. Simulated time** (tests): time moves when a test advances it; sleeps and scheduled tasks wait for it. First
  uses: the tests of time-bound paths that are slow or missing today.
- **C. One seed for the store:** every fault decision of `InMemoryDatabase` (delays, duplicates, reordering, drops)
  drawn from the test's seed.
- **D. Reproducible interleavings:** a spike comparing Lincheck's general-purpose mode (a dependency already) with
  Fray on one property scenario; adopted only if it replays a failure from its seed.

**A and B built (2026-11-03).** `InternalEnvironment` (`REAL`: `System`, the shared scheduler, `ThreadLocalRandom`)
threaded through `DistributedCaffeine` into `InternalCore`, `InternalStoreGuard` (its retries now an instance method),
`InternalVersion`, `InternalPollingStore` and `InternalMaintenance`'s scheduling; no API, no behaviour changed (the
in-memory suites: 422 tests green). `SimulatedEnvironment` (tests): time moves when a test advances it or the core
sleeps; scheduled tasks run in order on the advancing thread; the wall clock from a fixed date, randomness from the
seed. With the store executor direct (`withExecutor(Runnable::run)`), one test thread drives polls, heartbeats,
maintenance and resumes. `InternalSimulationTest`: a polled cache at the production defaults (polls every second,
heartbeat 10 s, the polls' grace about 5 minutes) riding out a store outage of six minutes, DEGRADED at once, its
delivery lost past the grace and not before, a hit served throughout, resumed with a gap, a write reaching it again:
0.3 s instead of minutes; and the same seed proposing the same versions. Then the store guard (three failed calls
open it, calls refused also once the store answers, until its second of suspension has passed, then a probe closes
it) and the heartbeat (a feed answering nothing while writes are made: silenced after the missed beat, every held
value checked against the store meanwhile, so a write elsewhere is not served stale; resumed with a gap). Four tests in
0.34 s. **Mutation-checked:** the heartbeat never silencing, the guard never opening, the polls never counting as
lost: each caught by its test alone.

**C built (2026-11-03).** `InMemoryDatabase#simulate` takes a simulation (`SimulatedEnvironment#forDatabase`): its
time for the journal a failover undoes (it read `System.nanoTime` in the table and the failover), and its deliveries
without threads, each batch a simulated task after the delay drawn from the seed, run on the advancing thread; the
real-thread mode stays the default. Caffeine's executor direct in the simulation's caches. **The replay test:** a
feed delivering up to 50 ms late and 20 % twice, 100 writes on 5 keys at random simulated steps: the holder's view
after every step is the same in two runs of one seed, and differs with another seed; five runs of the class, all
green. Within one JVM; across JVMs not yet checked (nothing hashed by identity is in the trace).

**D's spike: Lincheck's general-purpose mode works (2026-11-03, the user's go; Fray not needed).**
`InternalInterleavingExperimentTest` (`-Dexperiments=true`), `Lincheck.runConcurrentTest` on threads started in a
block:
1. A lost update planted on an `AtomicInteger` (read, then set): found, with the interleaving printed thread by
   thread. On a plain `int` field it was not found in 1,000 runs (the object created in the block, perhaps taken as
   thread-local): races on plain fields are out of its reach.
2. The core: two instances on the simulated in-memory store (Redis profile), a put, a load (`get` with a loader) and
   a delivery (the simulated time advanced) on three threads: 1,000 interleavings green in 59 s.
3. A race planted in the core (a message no longer raising a load's reservation, `reservations.containsKey` for
   `computeIfPresent(… raised …)`): found in both of two runs ("held after the race: old"), **the same interleaving
   both times**; the targeted tests (`InternalDistributionTest`, four) and the property test (four seeds) catch it
   too. A `getIfPresent` instead of the load did not find it in 200: the scenario must reach the mechanism.
Limits found: Lincheck 3.7 fails on hash4j's `VarHandle` on byte arrays (the spike hashes by the key itself), and on
AssertJ inside the block (its instrumentation calls `equals`, which AssertJ's assertions refuse: plain checks
there).

**Scenarios built (2026-11-03): `InternalInterleavingTest`** (`-Dmodelchecking=true`, the spike's file removed): a
writer and a holder on the simulated store, a delivery thread beside the scenario's, 500 interleavings each; one
invariant: once delivered, what a read of the holder serves (not what L1 holds: a value held but untrusted is checked
before it is served; peeking at L1 failed the invalidateAll scenario wrongly) is nothing or the store's value. Five
scenarios, a load racing: a write elsewhere, an invalidation, `invalidateAll` (on a clock without allowance), a write
lost in a gap; and two instances writing one key. Green, about 3 minutes. **Mutations:** a message not raising a
load's reservation, caught ("served old, the store has new"); a gap not moving the epoch, caught; **`invalidateAll`'s
message not moving the epoch (`InternalCore#apply`): survives, here and in every suite** (linearizability,
distribution, property, core, recovery, store profiles). Either redundant (the message also clears L1, and a load
registers its future there before it reads, so a load in flight loses its entry) or guarding a path no test reaches
(negative caching's absences?). **Decided the same day: needed.** Not for absences (`rememberAbsence` drops one whose
load's future left L1, which the clear does), but for a read through the store: `getIfPresent` reads first and
installs after, with no future the clear removes, so a value read before the cut-off stays trusted without the epoch.
A sixth scenario, a read-through racing `invalidateAll`, catches the mutation ("served old, the store has null"), the
first test of that line anywhere; the line's comment now says why.

**Three more (2026-11-03):** a refresh racing a write (the reservation mutation caught: "served loaded, the store has
new"); a load racing a failover that loses the latest write (the gap mutation caught: "served old, the store has
null"); two `compute` increments racing (`compute`'s write made unconditional caught: "two increments, the store has
1"). A refused `compute` installed as if applied was not caught, rightly: the store kept the other write, and its
message or the next revalidation corrects the holder; what the conditional write protects is the store from a lost
update, hence that scenario's own check beside the invariant. Nine scenarios, about 6 minutes, green.

**Two more (2026-11-03):** a load finding nothing (an absence) racing a write elsewhere, with its own check (the key
found afterwards: the invariant passes a stale absence); two `putIfAbsent`s racing (exactly one wins, the store holds
its value, the other is told it). **Found on the way, fixed in the core:** the absences cache (negative caching) was
built on Caffeine's defaults, the common pool and `System.nanoTime`, outside the seam: Lincheck reported a hang (a
thread it did not control), and absences would not expire on simulated time. Now on the store executor and the
environment's clock. **Mutations:** an absence kept despite a raised reservation, caught in 3,000 interleavings within
36 s, missed in 500 (that scenario now runs 3,000; its window: the write's message between the load's read and its
recording the absence); `compute`'s write on an absent key unconditional (the path of `asMap().putIfAbsent`), caught,
though Lincheck 3.7 then failed rendering the trace (an `IndexOutOfBoundsException` in its trace reporter: found, not
shown). Eleven scenarios, about 9.5 minutes, green.

**Timed paths and replay (2026-11-03):** `InternalSimulationTest` now also runs `invalidateAll` on a clock counting
in milliseconds (the call waits out the allowance on simulated time; a value another instance installs just after
applying the message is checked against the store once the allowance has passed: removing the sleep, or the epoch's
move after it, each fails it) and the maximum age of an hour (the copy gone from L1, Caffeine on the simulated clock
through `withCaffeine`; the row expired and announced by maintenance; expiry disabled in maintenance fails it). Seven
tests, 0.25 s. **Replay across JVMs:** the replay test's trace in three separate JVMs, the same hash each time.

**Left, by judgment:** maintenance's pacing (each batch's pause is the last batch's duration, zero on simulated time:
nothing to test there; covered by the real-time tests); the polling store touching a slow write again (covered by
`InternalPollingTest` at shortened times); the core's fallback thread (only when an executor refuses a task, never on
the simulation); an expiry racing a read under Lincheck (the timed test covers the expiry; a race would need
maintenance on a thread of the scenario).

**The property test's deterministic mode (2026-11-03, decided with the user: not a port to Lincheck).** Lincheck's
strength is small scenarios; the property test's breadth (25 to 40 phases, every operation, faults) would explode
under it, and most of this library's bugs were of the protocol (messages late, lost, twice; gaps; failovers; clocks),
not within an operation. So `InternalPropertyTest` has two modes: the threaded one as before (real threads, the net
for what nobody modelled), and a deterministic one: the same operations, faults and oracles on one thread, each
instance's writer and loader taking a step at a time in an order drawn from the seed, simulated time moving 0 to 4
ms between steps (deliveries, polls, maintenance, resumes there), the fault after a step; every store profile in
every run (50 seeds each by default, `-Dproperty.seeds`): 601 runs in 37 s, a run replayed event by event from its
seed. The rule: a bug either mode finds becomes a Lincheck scenario or a simulation test. **Found on the way:**
- the store's clock had to move at least a microsecond per reading (writes stamped equal to an `invalidateAll`'s
  control row were hidden: most seeds failed);
- the fault had to come after a step and without its threaded-mode moment of waiting (else everything was delivered
  before it, and a gap lost nothing);
- only the polled profile (jdbc's, the default) ran at first, where a gap has no feed to interrupt: the gap
  mutation survived 1,000 seeds; across every profile it fails 176 of 600;
- Caffeine's admission breaks ties with `ThreadLocalRandom` (not settable): no small L1 in this mode (the threaded
  mode keeps evicting), or a replay differed one run in four;
- a simulated sleep only moves the clock now (what fell due runs at the next advance, not inside an operation);
- the core logged a stack trace with every failed resume of the feed, every 30 s of an outage: now with the first
  only, the later attempts a line each; and the in-memory store's injected faults carry no stack trace (traces 170
  frames deep on one thread took 95 % of the runs' time).
The reservation mutation survives this mode, as designed (its window lies within an operation): Lincheck's.

**Open:** nothing planned. Candidates on demand: more Lincheck scenarios as bugs are found (the rule above); the property test's scenarios ported, rather than its free threads; the clock allowance
after `invalidateAll`, maintenance's pacing, the polling store touching a slow write again; Caffeine's ticker and the
core's fallback thread on the simulation; replay across JVMs checked.

## Performance

### Performance: the baseline

*Measured 2026-10-25.*

`EndToEndBenchmark` (core test package; run per store by `PostgresBenchmarkExperimentTest` and
`MongoBenchmarkExperimentTest`, `-Dexperiments=true`): two instances on one store in a local container, per operation
p50/p99, the store calls it costs and the time inside store calls (every instance's), the propagation of a put to a
holder, and the throughput of 8 threads (80 % gets, 20 % puts). Local containers have almost no network latency: the
store calls per operation tell what a network adds. After the fix below (PostgreSQL 18, MongoDB 9, µs):

| Operation                         | PostgreSQL p50      | calls | MongoDB p50         | calls               |
|-----------------------------------|---------------------|-------|---------------------|---------------------|
| `get`, held in L1                 | 1                   | 0     | 2                   | 0                   |
| `getIfPresent`, from the store    | 253                 | 1     | 324                 | 1                   |
| `get`, loaded (a miss everywhere) | 567                 | 2     | 787                 | 2                   |
| `getAll` of 100, from the store   | 635                 | 1     | (16,611, see below) | 1                   |
| `put`                             | 300                 | 1     | 415                 | 1                   |
| `putAll` of 100                   | (18,049, see below) | 1     | 45,870              | 1 (100 round trips) |
| `compute`, cold                   | 496                 | 2     | 722                 | 2                   |
| `compute`, hot                    | 300                 | 1     | 711                 | 2 (no hot path)     |
| `putIfAbsent`, present            | 183                 | 1     | 266                 | 1                   |
| `invalidate`                      | 289                 | 1     | 404                 | 1                   |
| `invalidateAll`                   | 562                 | 1     | 641                 | 1                   |
| propagation to a holder           | 292                 |       | 412                 |                     |
| throughput, 8 threads             | 27,377 ops/s        |       | 22,154 ops/s        |                     |

In memory (the core alone) every single operation is a few µs, a batch of 100 about 0.1 ms, the throughput 2.9
million ops/s: the core's own costs are negligible against a store's round trip.

- **Fixed: PostgreSQL's reads by key scanned the whole cache.** With statistics behind a table that grew fast (the
  planner estimated 127 rows of 60,300), it took the discriminator's prefix of the maintenance index and filtered
  every row (`id = ANY (?)`, also when the ids were joined from `unnest`): 60,200 rows read for 100 hits, `getAll` of
  100 at 43.7 ms, growing with the cache. Now each id a lookup of its own (`LATERAL`, kept apart by `LIMIT 1`): 0.64
  ms, independent of the statistics; `read`, `readVersions` and the conflicts' read.
- **Not the library: a constant ~16 ms stall for requests above a few KB, after heavy traffic, on Docker Desktop
  (macOS).** PostgreSQL's `putAll` jumps from 0.7 ms (50 rows) to 17 ms (100 rows), MongoDB's `getAll` from 0.5 ms
  (25 ids) to 16 ms (50, 75 and 100 alike), only after a burst of large writes, and the server is idle meanwhile
  (every session `ClientRead`, the listener not behind, the notification queue empty) while the client waits on
  its socket; a fresh store after the instances closed is fast again. Both drivers set `TCP_NODELAY`. The signature
  of a stall in the forwarding between the host and Docker's VM; to confirm on Linux or a native database.

## Open design questions

New ones go here as they come up while building; decided ones move to their section. None open at the moment.

## Costs, accepted knowingly

- The first read of a key on each instance is one store round trip (population pushes no value to an instance not
  holding the key).
- With population on, a write to a key held by N instances causes N batched reads.
- Every row that exceeds its maximum age costs one write plus a message to every instance.
- The maximum age is precise to one pruning interval plus delivery, not to the millisecond.
- `refreshAfterWrite` refreshes by an age of at most 2R; concurrent refreshes may call the source once per instance.
- No warm-up: L1 starts empty, so the first access per key and instance after a start reads L2.
- A store is mandatory.
- Two lifetimes to configure.
- No global eviction across instances; L2 is the global bound.
- Writes are refused while the store is down; an unconditional put with an unknown outcome throws.
- Starting without the store (the default since 2026-11-12): a misconfiguration that looks like an outage (a blocked
  port, a wrong host name) no longer stops a rollout but runs `DEGRADED`; the recovery is a deferred cold start (every
  copy loaded meanwhile dropped once connected, an L2 read per key again); two configuration checks come late; a build
  waits the client's own connect attempts (Redisson with lazy initialization: about 16 s). Caches sharing a client whose
  store went silent connect up to one operation timeout later than they could (one creation per client asks the store
  per operation timeout). `withFailFast()` restores the old behaviour.
- Hashing on every user call: 2–4 times a plain Caffeine hit (30–65 ns), 32 bytes per call.
- After a gap, a row read per held key, its value transferred (lazily and in a rate-limited background pass).
- With negative caching, each instance learns its own absences.
- Dependencies: Fory always (about 2.7 MB plus Janino); for PostgreSQL only pgjdbc (R2DBC with Reactor and Netty,
  about 6.6 MB, gone 2026-10-28).
- PostgreSQL's listener reads pgjdbc's stream through reflection on one internal field (`QueryExecutorBase.pgStream`),
  checked at the first subscription; a pgjdbc release could change it.
- Every PostgreSQL `NOTIFY` wakes every listening session (about 33 µs of PostgreSQL CPU each).

## Lessons checklist

Each lesson from the library's history is a test, a design rule, or recorded as not applying; a change is checked
against it. Names refer to the working notes where the lesson was learned.

| Lesson                                                                                                                                                                              | Note                                                                                                                                             | Carries over as                                                                                                                                      |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| Overtaken refreshes and loads must be discarded (tokens; the null-operation trap)                                                                                                   | refresh-in-flight-discard                                                                                                                        | test; mechanism replaced by versions (Ordering, rules 4 and 6)                                                                                       |
| Loads run outside the lock; commit rules for extras                                                                                                                                 | refresh-in-flight-discard                                                                                                                        | design rule: `AsyncCache` loads outside the lock; extras only where L2 holds no value (see Shared loads)                                             |
| Stale loads are only visible to a freshness oracle, not to a convergence oracle                                                                                                     | convergence-loads-freshness-oracle                                                                                                               | central test oracle                                                                                                                                  |
| Property tests need publish latency and phased checks to see concurrency                                                                                                            | convergence-property-test-and-its-blind-spots                                                                                                    | test harness: built as `InternalPropertyTest` (below)                                                                                                |
| The store guard must cover the read path, not only writes                                                                                                                           | store-guard-and-health-state                                                                                                                     | test (SPI: `read`, and the read after a `putIfUnchanged` conflict)                                                                                   |
| A failed write may still land; forget its operation                                                                                                                                 | failed-write-suppresses-landed-echo, postgres-single-statement-publish                                                                           | test; mechanism replaced by dropping the key (Ordering, rule 5)                                                                                      |
| `invalidateAll` needs a global marker and global, not per-key, ordering                                                                                                             | invalidate-all-marker-design, publisher-repository-adapter-split                                                                                 | design rule                                                                                                                                          |
| Replayed invalidations can be dropped past ~1 min; history can be lost; revalidate after gaps                                                                                       | store-downtime-robustness-plan, read-based-synchronization-design                                                                                | invariant 7, test                                                                                                                                    |
| Starting synchronization on a partially activated instance froze                                                                                                                    | partial-activation-freeze                                                                                                                        | does not apply: no stop/start and no `getAdapter()` (see Lifecycle, Adapter ownership)                                                               |
| Overlapping size pruning over-prunes                                                                                                                                                | concurrent-size-pruning-over-prunes                                                                                                              | test                                                                                                                                                 |
| Async publishes need one ordered drain per cache                                                                                                                                    | async-publish-single-drain                                                                                                                       | writes are synchronous now; the ordered queue per cache carries over for delivery (see Operations)                                                   |
| The write is the round trip; the global lock caps throughput (fair lock, striping)                                                                                                  | write-path-cost-measured                                                                                                                         | benchmark baseline                                                                                                                                   |
| Duplicate-key retry on upsert is needed though rarely hit                                                                                                                           | upsert-duplicate-key-retry-cost                                                                                                                  | does not apply on MongoDB: upserts by the composite `_id` are atomic (see Adapter setup); a duplicate key on `putIfUnchanged` stays a conflict       |
| Mongo: one connection pair per watcher; filter server-side; watcher sharing; majority write concern, no w:1 (measured: w:1 slows propagation about 100x, see MongoDB write concern) | change-stream-watcher-connection-cost, change-stream-match-pushdown, mongo-watcher-sharing, postgres-single-statement-publish                    | adapter design (sharing: see Connection sharing; failed applies now dropped, not re-queued; dispatch by the discriminator in `_id`)                  |
| Postgres: stream reads in a transaction; heartbeat for half-open listeners; poolers need a listener data source; one session delays above ~750 notifications/s; listener sharing    | postgres-streaming-and-ordered-reads, postgres-listener-half-open-heartbeat, postgres-listener-sharing, pgjdbc-single-session-notification-cliff | adapter design (listener taking over pgjdbc's stream, so the ~750/s limit no longer applies; heartbeat in the core; sharing: see Connection sharing) |
| Multiple caches per collection/table via discriminator                                                                                                                              | discriminator-and-watcher-sharing-design                                                                                                         | adapter design                                                                                                                                       |
| Stats counting rules                                                                                                                                                                | distributed-stats-design                                                                                                                         | design rule                                                                                                                                          |
| Entries expired via `setExpiresAfter` stay resident and invisible                                                                                                                   | caffeine-setexpiresafter-stuck-entries                                                                                                           | design rule: never used internally; passed through to users as Caffeine's behaviour                                                                  |
| `weightedSize()` needs `cleanUp()` first                                                                                                                                            | caffeine-weightedsize-needs-cleanup                                                                                                              | test harness                                                                                                                                         |
| Log-counting tests break on any new log line                                                                                                                                        | logging-breaks-log-counting-tests                                                                                                                | avoid that test style                                                                                                                                |
| 3-member replica set; javadoc verification                                                                                                                                          | mongo-three-member-test-replica-set, javadoc-verification-gotcha                                                                                 | test harness, build                                                                                                                                  |
| Mongo < 4.4 namespace limit (120 bytes)                                                                                                                                             | mongo-namespace-limit-test-names                                                                                                                 | does not apply: MongoDB 4.4 or later                                                                                                                 |
| Eviction echoes race persistence tests                                                                                                                                              | eviction-echo-races-persistence-tests                                                                                                            | does not apply: no eviction distribution                                                                                                             |
| The own-echo re-add is load-bearing                                                                                                                                                 | own-echo-readd-is-load-bearing                                                                                                                   | does not apply: no contents synchronization                                                                                                          |
| Restore marking a value Caffeine is evicting                                                                                                                                        | restore-mark-race-loses-store-backed-keys                                                                                                        | does not apply: no restore                                                                                                                           |
| Silent entry loss (parked)                                                                                                                                                          | silent-entry-loss-open-lead                                                                                                                      | does not apply: any subset is valid (invariant 1)                                                                                                    |
| Store owns values, the mode owns residency; `pruningStatus()`                                                                                                                       | extended-persistence-no-eviction-mode                                                                                                            | does not apply: no statuses                                                                                                                          |
| `PartialCacheEntry` / `CacheEntry` projections                                                                                                                                      | cache-entry-projection-design                                                                                                                    | does not apply: the SPI has plain rows, no projections                                                                                               |

## History

Superseded designs, studies not built, and the build log, condensed where superseded. Kept because the current design
cites them and because they record what was tried and why it was left; the code cites some of them by title.

### `invalidateAll` and loads in flight (decided and built 2026-10-22; replaced 2026-10-24 by the cut-off, see "Design study")

**Condensed 2026-11-18.** The race: a load reads, calls the source, then writes conditionally; `invalidateAll` then
tombstoned only value rows, so a load that had read a tombstone, an expired row or no row still wrote a value from
before the `invalidateAll`, kept until the maximum age. Built then: the sweep gave *every* row a new version, a row
created on "missing" took the read's time as its version, and `INVALIDATED_ALL` carried the sweep's time T; instances
kept the highest T and treated value rows below it as missing (tombstoned in the background). Residuals: under `NONE`
other instances never learn T; an instance that missed the message may serve such a row. Rejected on the way: a marker
row checked by every conditional write (a cross-row check inside a write, hard on MongoDB and custom stores);
compensation relying on messages in commit order (not a contract custom stores can meet); comparing with records of
recent own load writes (not crash-safe); a generation per cache and per row; a placeholder tombstone written by every
cold load (exact, but a write per cold load). Seven mutants of the property test's freshness oracle were all killed.
Replaced by the cut-off ("`invalidateAll`: a cut-off time"), today by "`invalidateAll` by the store's
time, hidden on read".

### `invalidateAll`, logical: one counter and the cache's time T (designed 2026-10-24, not built; parked 2026-10-28 for `invalidateAll` by the store's time)

**Condensed 2026-11-18.** Designed to replace the sweep (a write and, on MongoDB, a change event per row; it also
relied on the store's wall clock never stepping back, which a failover or a clock step breaks). The idea:
1. **One counter of the store for versions and read times,** never going backwards, failovers included: (a) a write's
   version is above every version committed before it began and above the key's old one (`max(old + 1, counter)`);
   (b) a read's time is above every version committed before it began and at most the version of any write beginning
   after it. MongoDB: `$$CLUSTER_TIME` (measured, `MongoClusterTimeExperimentTest`, 4.4.0 and 9, 2,000 rounds with 9
   step-downs: never violated; logical, unlike `$$NOW`; a BSON timestamp, so no formula may add to it). PostgreSQL:
   a sequence per table (`CACHE 1`), whose record is in the WAL before the commit, so a promoted standby continues
   above it (one load racing an `invalidateAll` can survive a failover at worst).
2. **The cache's time T** in one control row: rows at or below T count as missing for every operation, filtered in the
   same query; `invalidateAll` writes only the control row; a conditional write takes its read's time, so a write based
   on a read before the `invalidateAll` lands invisible. No message, memory or clock in the core needed.

**Measured** (`PostgresLogicalInvalidateAllExperimentTest`, `MongoLogicalInvalidateAllExperimentTest`, Docker Desktop,
10,000 keys): common paths unchanged within the noise, except PostgreSQL's single-key read (+10–30 µs) and MongoDB's
`readVersions` (+50–100 µs); `invalidateAll` 70–380 times faster (PostgreSQL 43 ms → 0.65 ms at 10,000 rows, 438 ms →
1.2 ms at 100,000; MongoDB 77 ms and 10,001 events → 2.1 ms and 1 event). Found on the way, valid regardless: MongoDB's
first batch holds 101 documents, so 100 rows plus the control document cost another round trip (+300 µs): the adapter
sets `batchSize(ids + 1)`; and reading 100 values of 100 bytes takes 1–16 ms through Docker Desktop on macOS against
0.4 ms on the server (to check on Linux). Parked for the pragmatic store direction; the cut-off by the store's time
(today's design) took over its T and its visibility filter, not its counter.

### PostgreSQL: statements and propagation measured

**Condensed 2026-11-18** (the statements then wrote and notified in one statement, versions from the server's clock;
today's are in "The PostgreSQL adapter" and "PostgreSQL listening: pgjdbc's stream taken over"). **Statements**
(`PostgresStatementExperimentTest`, PostgreSQL 13 to 18), all checks passed on every version: versions strictly
increasing; no notification before the commit, one after it, none after a rollback; of 8 concurrent conditional writes
on one expected version (`ON CONFLICT ... DO UPDATE ... WHERE r.version = ?`) or 8 creations of a missing row
(`ON CONFLICT DO NOTHING`) exactly one wins, with one notification; invalidating 5 rows in one statement gives 5
notifications; two concurrent expiries expire and notify each row once (the second update re-checks the condition after
the first one's row lock); the read filters by maximum age and returns the age.

**Propagation** (`PostgresPopulationExperimentTest`, PostgreSQL 18, one hot key, 1 KB values): the notification costs
the writer nothing measurable (145–154 µs alone, 143–148 µs with `pg_notify`); below about 750 writes/s holders had
the value within 2–3 ms (p99 5–6 ms); above it propagation collapsed to hundreds of milliseconds (p99 up to 665 ms at
1,500/s). Cause, pgjdbc's (memory note `pgjdbc-single-session-notification-cliff`): `getNotifications` returns only
after a 1 ms gap in incoming notifications, so one session cannot hand out more than about 750/s. It matters more in
this model, as every instance listens to every write of the table: the limit is on the cluster-wide write rate per
table. Removed by taking over pgjdbc's stream ("PostgreSQL listening: pgjdbc's stream taken over").

### PostgreSQL listening: R2DBC

**Superseded 2026-10-26, condensed 2026-11-18:** R2DBC is ruled out; the listener takes over pgjdbc's stream instead,
just as fast (see "PostgreSQL listening: pgjdbc's stream taken over"). What was decided and measured on the way:
- **R2DBC as the whole store path, rejected** (`PostgresJdbcVersusR2dbcExperimentTest`, removed with the dependency;
  PostgreSQL 18, pgjdbc with HikariCP against r2dbc-postgresql 1.1.1 with r2dbc-pool 1.0.2, pools of 10, 8 concurrent):
  upserts with `pg_notify` 142–146 against 313–403 µs p50 (24,100–26,800 against 13,200–14,400/s), conditional
  writes 138–144 against 308–310 µs, a read of 100 rows 356–359 against 1,277–1,307 µs. R2DBC is about half as fast
  for writes and 3.6 times slower for batched reads; functionally it worked (arrays, `bytea`, SQLState `23505`, a
  `postAllocate` hook for the standby check).
- **Channel partitioning, rejected** (`PostgresChannelPartitioningExperimentTest`, 4 instances, a session per channel):
  it fixes the latency once every channel stays below about 750 notifications/s (1.5 ms p50 at 1,500/s with 2
  channels), but every `NOTIFY` wakes every listening backend: about 20 µs of PostgreSQL CPU per write and session, so
  32 sessions cost 0.7 s of CPU per 1,000 writes and the writers reached 3,200 of 6,000/s. A single hot key stays on
  one channel anyway.
- **R2DBC as the listener, measured** (`PostgresR2dbcListenerExperimentTest`, now `PostgresListenerExperimentTest`):
  0.3 ms p50 at every rate up to 6,000 writes/s, the hot key included, one session per instance, the CPU at the
  single-session level; against pgjdbc's 28 ms to 2 s p50 above the cliff. It cost Reactor and Netty (about 20
  artifacts) and connection settings of its own (a `DataSource` cannot be turned into an R2DBC factory: opaque, and
  dynamic credentials such as IAM tokens are computed inside it).
- **What holds for any listener:** every `NOTIFY` wakes every listening session, about 33 µs of PostgreSQL CPU per write
  and session (16 instances at 3,000 writes/s are about 1.6 cores), which listener sharing keeps to one session per
  JVM; transaction-mode poolers cannot carry `LISTEN`.

### Design study: portable store primitives (started 2026-10-24; build order: steps 0 to 5 built 2026-10-24)

**Aim:** a store needs only what many stores offer (plain JDBC/ANSI SQL, the common operations of key-value and
document stores). Distribution is an **optional** feature of an adapter, never a must: a native feed where the store
has one (MongoDB change streams, PostgreSQL `LISTEN/NOTIFY`), a generic polling feed otherwise, or none
(`DistributionMode.NONE`). Races are solved by versions and by the instances themselves; a rare edge case only has
to be recognised, and repairing it may cost something. Adapters with smarter means (PostgreSQL, MongoDB) implement
the same operations natively, so they lose nothing (see "Two levels"). Its parts still in force are under "Stores"
("What a store must deliver", "Deletes", "Clocks: what is left", "Distribution: optional", "Two levels"), under
"Distribution" ("The polling feed") and under "Expiry and `invalidateAll`"; the superseded ones follow here.

### The SPI as first built (2026-10-23)

**Superseded by the thin SPI (build step 2), condensed 2026-11-18.** The first SPI had a method per kind of write, the
version always assigned by the store (`old + 1`, a new row at the server's time) and returned: `read` (with the store's
time), `readVersions`, `put`, `putIfUnchanged` (taking the maximum age, so that a value row older than it, not yet
marked expired, counts as missing: otherwise a load would loop), `invalidate` (a tombstone for a missing row too, so the
version reaches loads in flight elsewhere), `invalidateIfUnchanged`, `invalidateAll` (every row a new version, one
`INVALIDATED_ALL` message, which a store could also announce per row), and the maintenance calls returning counts.
`ChangeListener` had `onChanges` and `onGap`. Its contract tests began as 30 tests in one class; splitting them into a
storage and a feed contract found MongoDB announcing a tombstone or expired row given a new version by `invalidateAll`
as `WRITTEN` (an update leaving the state as it was names no state in its change event): the change stream now flags,
before projecting the value out, whether an update wrote a value. 13 deliberate errors in the reference store were
caught by the contract, after one gap was closed (a young tombstone under "if no value").

### Clocks: what is left

**Superseded in part, moved here 2026-11-18:** versions come from the instances' clocks now, enforced by the
store ("Monotonic versions"); the cut-off's dependence on the store's clock is bounded by the clock allowance
("`invalidateAll` by the store's time, hidden on read").

- Instances use no clock for versions or cut-offs; only the store's clock, and only for write times and the start
  value of recreated keys.
- **A store clock going backwards** (failover to a member with a slower clock, a clock step) can: hide a put right
  after an `invalidateAll` (its write time at or before T; the writer recognises it when it knows T, otherwise a
  loading cache reloads it from the source, a cache without a loader loses that write); and start a recreated key
  below a holder's copy (served until the maximum age). Both need a regression within the skew window at that moment.
  An adapter with a monotonic counter uses it for the start value and the cut-off, and both residuals are gone.

### `invalidateAll`: a cut-off time, repaired by the loaders

**Superseded, condensed 2026-11-18.** The first cut-off: `invalidateAll` upserted a reserved control key whose write
time T hid every row written at or before it (`write_time > max(now - maximumAge, T)`), no row touched, maintenance
deleting hidden rows later; every read included the control key, so instances learned T without a message. A load
racing it was recognised by the loader (a re-read of the control key after its write, or a condition folded into an SQL
write) and undone by a conditional delete; residual: a loader dying in between. Replaced the same day by the cut-off
on every row (option A, below), and later by "`invalidateAll` by the store's time, hidden on read",
which keeps the control row and the hiding.

### The current contract, point by point

**Condensed 2026-11-18.** The study's comparison of the store contract then (versions `max(old + 1, server time)`,
`ifMissing(entry, readTime)` and the sweep) with the study's (a per-key counter starting at the store's time, read
times no longer compared with versions, deleting rows hidden by the cut-off added to maintenance, the polling feed
beside native feeds); points 3, 4, 6, 8 and 9 unchanged. Today's contract: "The thin SPI, versions in the
core" and "What a store must deliver".

### The decision's cut-off on every row (2026-10-24, option A)

**Superseded, condensed 2026-11-18** (the basis, the stamp and `checksBasis` are gone since steps 2 and 3 of the build
plan). Every row carried `basis`, the cut-off its write was decided under, and a row was visible iff
`stamp > cutoff and basis >= cutoff`: a value decided before an `invalidateAll` was hidden whenever written, which
removed the check after every conditional write, its undo and a second control write (that one also made the operation
non-linearizable). Step 2 (built 2026-10-25): `Store.checksBasis()`, the store refusing a conditional write whose basis
is below the cut-off in the same statement (PostgreSQL, JDBC, in memory; MongoDB cannot condition an update on another
document), so a compute on a trusted held value was one store operation.

Still valid, found on the way and kept in the code:
- **The epoch before the read** for every value held from a decision (loads, updates, bulk loads, refreshes), so a value
  racing this instance's own `invalidateAll` is held untrusted and checked on its next read.
- **Lincheck on one key with `invalidateAll`** found a read that saw the store ahead of L1 leaving L1 behind (a later
  read of the same thread answered older). Fixed: an update writing nothing (a compute returning the current value,
  `putIfAbsent` on a present key, `remove(k, v)` not matching) brings L1 up to what it read; `readThrough` finding no
  value at a row's version drops anything older from L1, and what L1 holds at that very version too; `dropOlder` drops
  a load in flight, as Caffeine's `remove` discards it (its waiters get its value, its owner holds it only if the key is
  still there).
- **A hot path deciding on a held value failed twice** before the store checked the basis: a decision writing nothing
  answered from L1 against a newer write elsewhere whose message had not arrived (`InternalMapTest`); and a write
  decided on a held value after a remote `invalidateAll` had returned but before its message arrived was hidden by its
  old basis, a lost `asMap().put` (the property test's presence oracle). See "Atomic `asMap()` methods stay atomic
  across instances" for today's rule (no hot path).
- **How others do it** (sources read 2026-10-25): every near cache decides on its local value on the hot path.
  Hazelcast's near-cached `compute` writes `replace(key, old, new)` on the owner, which fails after a `clear()` because
  that deletes, and answers a decision writing nothing unvalidated from the near cache; Redisson's
  `RLocalCachedMap.compute` takes the key's lock and writes unconditionally (a value derived before a `DEL` survives);
  generation namespaces (memcached style) lose a write decided under an old generation. A write committing at once with
  an `invalidateAll` may land and be hidden: the two overlap, so either order is linearizable.

### Robustness fixes before the performance part (2026-10-25)

**Condensed 2026-11-18.** Still in place: **PostgreSQL's sequence per table** (`<table>_stamps` then; a sequence never
goes back, not after a failover either, so a row created again starts above every earlier version of its key; today
the floor of the versions PostgreSQL assigns, see "Versions applied as well as each store can");
**JDBC hands a connection back as it came** (a write switched auto-commit off and left it so, and a data source not
resetting connections then gave the next caller one in an open transaction, reading an old snapshot: restored after
every write, and a read on a connection without auto-commit ends its transaction). Gone with the basis: MongoDB
documents without one counted as decided on nothing read in maintenance too. The full matrix of 2026-10-25 (six
parallel groups, 31 minutes instead of about three hours, 19,804 tests) found only test timing under load and MongoDB
4.4.0's three-node replica set electing no primary within 60 s, older than this work.

### Performance step 1: MongoDB's batches concurrently (2026-10-25)

**Superseded by step 5 (one unordered bulk write per batch), condensed 2026-11-18.** A MongoDB batch was a round trip
per key, one after another; up to 16 calls in flight cut a put of 100 keys from 45.5 to 13.6 ms on the store alone
(45.9 to 28.0 ms through the core with change streams open; 64 in flight was slower again, 36.5 ms, as each write waits
for the journal under `majority`).

### Design study: one number per write, the stamp as the version (recorded 2026-10-26, not built)

**Not built, condensed 2026-11-18** (versions became monotonic and proposed by the core instead: "Monotonic versions").
The idea, as etcd's `revision` or a log sequence number: one number from the store per write as the per-key version, the
order in L1, the cut-off and the feed's position, with writes returning it. Checked on PostgreSQL 17: a waiting `UPDATE`
draws from the sequence again once it has the row lock, so per key the number follows commit order. Ways a writer learns
its write's place in the order: the store returns it (etcd, Spanner, Kafka, TAO); it arrives with the echo (Hazelcast:
an op id in every message, unknown in `NONE` mode); the writer keeps nothing (Redis client-side caching: fewer hits);
the writer decides the order (Cassandra, Lamport and hybrid logical clocks: the path taken later). Costs found:
PostgreSQL's sequence must keep `CACHE 1`; MongoDB's bulk writes return no numbers (a write marker and a read back, two
round trips); a global counter is a point of coordination that grows with scale-out (free on one primary, cheap with
MongoDB's cluster time, expensive on distributed SQL, a 1,000-writes/s hotspot as a DynamoDB counter row). Not adopted:
a version from 1 plus a server timestamp (a clock behind after a failover hides writes after an `invalidateAll`; `now()`
is the transaction's start in PostgreSQL); starting at the last version seen plus one (needs the version after the
tombstone's purge).

**The fallback for stores without a global counter** (decided 2026-10-26, not built; for DynamoDB, Cassandra,
distributed SQL): per key a counter and a random 64-bit incarnation (a different incarnation means the order is
unknown, and L1 acts conservatively); `invalidateAll` by server time plus ε, waiting 2ε before it returns (as Spanner's
commit wait), with the skew measured by the heartbeat (a warning above ε/2, `invalidateAll` refused above ε). Writes
unchanged; `invalidateAll` O(1) but taking 2ε. Not adopted for these stores: a sweep, a generation checked on every
write, a generation in the key alone, one acknowledged by every instance (needs membership with leases).

**Drawbacks per store** (as known on 2026-10-26; "to check" not verified), for "capabilities for further stores":
- **PostgreSQL:** `NOTIFY` payloads capped at 8,000 bytes; a connection per instance stays open for `LISTEN`.
- **MongoDB:** bulk writes return no versions; a write cannot read a second document (so no cut-off check in a filter).
- **MariaDB:** no `RETURNING` on `UPDATE`; no native feed (the binlog through a replication client, or polling);
  sequences not globally ordered under Galera.
- **MySQL:** no sequences (a ticket table on `AUTO_INCREMENT`); no native feed.
- **SQL Server:** `rowversion` as a global counter; the feed is Change Tracking or CDC, seconds behind.
- **Oracle:** a sequence keeps its order under RAC only with `ORDER`; the feed depends on edition and licence (to
  check).
- **CockroachDB:** `cluster_logical_timestamp()` pins the transaction (more restarts, `SERIALIZABLE` only); a sequence
  is a hotspot of one range; changefeeds need `kv.rangefeed.enabled`. See "CockroachDB adapter".
- **DynamoDB:** the fallback; no server time in writes; `BatchWriteItem` cannot increment; Streams keep 24 hours.
- **Cassandra:** the fallback; increments only through lightweight transactions (Paxos); no usable feed; `TRUNCATE`
  removes without tombstones.
- **Redis:** a script does counter, row and message atomically; in a cluster one slot per cache; asynchronous
  replication can set the counter back on a failover (a new `run_id` or primary must count as a gap); keyspace
  notifications carry no version, so the script appends to a stream (built so: "Redis adapters"). Of the Redis-like
  stores, AWS MemoryDB fits best (a durable log before the acknowledgement); stores with several writable primaries
  (KeyDB active replication, Redis Enterprise Active-Active) can hand out a counter value twice.

### Versions known to the writer instead of reported by the store: not adopted (decided 2026-10-26)

**Not adopted then, condensed 2026-11-18** (taken up in another form two days later: "Monotonic
versions", versions proposed by the core). The question: must a store report the version it wrote, or may the writer
work it out (`expected + 1` for a conditional write, an incarnation from the writer's hybrid logical clock for a fresh
row)? Pros: MongoDB batches in one bulk write (a conflict as a duplicate-key error at its index); stores without
`RETURNING`; a smaller SPI. Against it then: correctness would depend on clocks for the first time (no jump larger than
the grace); the change would land in the Lincheck-verified write registry; unconditional writes held with an unknown
version; a new version format; PostgreSQL gains nothing. What holds either way: "highest version wins" orders by
arrival, not decision, so the conditions stay (a load must lose against a removal after its read; `compute` needs a
compare-and-set); a timestamp never orders, as a clock can go back after a failover; a fresh row must start above every
version its key had. Decided then: the store reports the version; MongoDB's batches by a bulk write plus a read back of
an op id inside the adapter (later built differently, step 5).

### Design study: L1 by invalidation and reservation instead of version order (recorded 2026-10-26; only its reservation registry built, build plan step 4)

**Only the reservation registry built (step 4), condensed 2026-11-18.** The goal was a simpler L1, following others in
production:

| System                              | L1 kept current by                          | Loads racing writes                                                       | Lost messages                                       |
|-------------------------------------|---------------------------------------------|---------------------------------------------------------------------------|-----------------------------------------------------|
| Redis client-side caching           | invalidations from the server, no versions  | a "load in progress" placeholder; an invalidation removes it              | clear L1 on reconnect                               |
| Hazelcast Near Cache                | invalidations with a sequence per partition | a reservation with an id; installed only if still one's own               | gaps in the sequence detected; a repair task clears |
| Facebook memcache                   | deletes                                     | a lease per miss; a set counts only with a valid lease; a delete revokes  | (no L1)                                             |
| Redisson local cached map, JetCache | pub/sub invalidation or update, no versions | none (accepted)                                                           | clear, or replay a log of 10 minutes                |

None of them orders versions inside L1: they protect a load by a reservation and treat a gap by clearing. The study's
mechanism: a held value's version as a token compared for equality only; every operation that installs reserves the
key first, and a message, a local removal, the epoch or a second reservation of the same key poisons the reservation;
an operation installs only if unpoisoned. Safe because every message arrives after its commit and the reservation is in
place before the read or write is sent; two operations on one key in one instance poison each other (otherwise the one
whose write committed first could install last). Its cost: extra misses under contention (a late older message removes
a newer value; two operations on one key install nothing). Kept: the one reservation registry (in place of
`minimumVersions` and `writing` with `writeMinimums`), raising a minimum version instead of a poison flag, since the
versions stayed ordered (`InternalCore#reservations`).

### Further alternatives (recorded 2026-10-26, not built)

**Not built, condensed 2026-11-18.**
- **Revalidation by version** (as HTTP's ETag): a value older than T checked in the background by a read of versions
  only, bounding staleness even after a message lost without notice; it works without a feed. After a failover that
  lost a write an equal version can hold another value, so it would need the value or a fingerprint too. Replaced as
  the safety net by the polling feed and the heartbeat.
- **Writes never install in L1** (cache-aside, delete on write): simplest, but a read after every write goes to the
  store; an option for read-heavy workloads.
- **Leases in the store** (Facebook memcache): one load per key across instances; a write on every miss and lease logic
  in every adapter. Only if stampedes across instances are measured ("Open work", stampede protection).
- **Not a fit:** one ordered log as the only truth (PostgreSQL logical replication: heavy to run, one's own writes wait
  for the log); ownership per key (Hazelcast, Infinispan: needs cluster membership beside the store, which is meant to
  be the only coordinator); order by timestamp (correctness would depend on clocks).

### Target design: reservations in L1, versions read but never returned by writes (recorded 2026-10-26; only its reservation registry built, build plan step 4)

**Only its reservation registry built, condensed 2026-11-18.** After the complexity budget the same day, only the one
reservation registry and versions read from the store were kept; writes kept returning their version. The target had
four parts: reservations in L1, versions from reads and feeds, op ids to recognise one's own unconditional writes and
resolve unknown outcomes, and writes returning only applied or conflict. Why not op ids alone: without order a reload
under a reservation is cancelled by every message during it, so a key written by others faster than one round trip
would never be cached; parking a result until its echo needs commit order, which the polling feed's re-scan breaks.
Settled then and still true:
- **One registry, a reservation per key** (operations in flight and the highest version announced meanwhile), replacing
  `minimumVersions` for loads and `writing` with `writeMinimums` for writes; a load's future is compare-and-removed if
  older than the reservation's highest version, a write installs under the registry's lock only if not older; a gap or
  `invalidateAll` is the epoch. With versions read from the store this was today's core simplified, not a rebuild.
- **The polling feed scans by a store-assigned position, not by store time** (a clock stepping back further than the
  overlap would lose changes silently); the overlap measured on the scanner's own monotonic clock (`System.nanoTime`),
  and a write slower than the overlap, or with an unknown outcome, touched again by its writer (`InternalPollingStore`).
  The index `(discriminator, stamp)` cost about 1 µs per single-row update inside PostgreSQL 17 (200,000 rows).
- **Store first, then L1** costs one miss per write elsewhere in `INVALIDATE` mode and one reload per holder in `UPDATE`
  mode in every design; avoiding it would need values in messages, not adopted.
- **A scan is a bonus, not needed for correctness, beside a native feed:** correctness needs every lost message noticed,
  not replayed, and the native feeds report their losses (a broken `LISTEN` connection, a resume point gone from the
  oplog, a full `NOTIFY` queue failing the writer's transaction, a silent feed caught by the heartbeat); every reported
  loss moves the epoch on. See "Open work" for a scan next to a feed.
Left out on purpose then: op ids, writes without versions, 128-bit op ids replacing versions in rows (16 bytes per row
and message, a rebuild of contract, adapters and tests), for later only if MongoDB's bulk writes were measured worth it.

### Costs of the cut-off, and optimisation candidates (recorded 2026-10-24, after step 2)

**Superseded 2026-10-25, condensed 2026-11-18:** the cut-off as built then cost a read of no keys after every
conditional write, a read before a compute on a held key and before a refresh, two control writes, messages and L1
clears per `invalidateAll` (the commit window), and the cut-off with every read. Option A removed most of it, and the
store-time cut-off the rest. Of its candidates, the one still open in spirit: a conflict reporting what a load needs, so
that a load starting over saves its read; and folding a check into PostgreSQL's conditional write. The performance part
started from a new baseline ("Performance: the baseline").

### What the property test needs (worked through 2026-10-24)

**Built since, condensed 2026-11-18** (see "The property test" and "In-memory store profiles"). The list, as worked out
then: a presence oracle (a key last set by a confirmed write, not invalidated since, must be in the store with exactly
that value), besides freshness, which allows "no value"; a commit-delay fault in the in-memory store, so that the window
between a write taking its time and becoming visible opens; deterministic tests for conditional writes racing an
`invalidateAll` (a pause between read and write), since the property test's compute methods hold their key's lock;
size pruning and the deletion of hidden rows exercised with a small maximum size and fast maintenance; the polling feed
as a backend, with the commit delay above and below the overlap and collapsed messages; clock regressions tested on
their own, not in the property test; and a mutation check of each mechanism.

### Open in the study

Nothing: the study was decided and built (build plan, steps 1 to 6).

### Build plan (recorded 2026-10-26; steps 1 to 6 built 2026-10-28 to 2026-10-31; what follows: "Open work")

The decisions of 2026-10-26, in the order to build them. Every step stays green under the existing tests: the
contract, the property test, Lincheck on one and on two instances, the minimal runs per change, and the full matrix
at the end.

1. **The PostgreSQL listener without R2DBC.** Built 2026-10-28. Deviation: an optional listener `DataSource`
   (`withListenerDataSource`) stays, for a `DataSource` behind a transaction-mode pooler; by default listening goes
   through the one `DataSource`. The README items are in the adapter's Javadoc (there was no README yet). See
   "PostgreSQL listening: pgjdbc's stream taken over" and its decision.
   - `PostgresListener` reads pgjdbc's stream: reflection on `QueryExecutorBase.pgStream`, reached through
     `unwrap(BaseConnection.class).getQueryExecutor()`, and checked at startup.
   - It parses `A` and skips other messages; `E` ends it with a loss.
   - Its connection comes from the `DataSource` and is aborted at the end.
   - The prototype is in `PostgresR2dbcListenerExperimentTest` (the `TAKEOVER` variant).
   - `PostgresAdapter`: `withListener(ConnectionFactory)` and the URL convenience go.
   - The pom drops `r2dbc-postgresql`, `r2dbc-pool`, and the managed Reactor, Netty and SCRAM versions.
   - Tests: the PostgreSQL tests that build an R2DBC factory are `PostgresConnectionTest`, `PostgresFailoverTest`,
     `PostgresPoolerTest`, `PostgresPropertyTest`, `PostgresRestartTest` and `PostgresTestDatabase`. Also
     `simplelogger.properties`.
   - The R2DBC experiments are removed; their results stay recorded here.
   - The README documents one held pool connection per instance, and that transaction poolers cannot carry `LISTEN`.
2. **The thin SPI, with versions in the core.** See "The thin SPI, versions in the core", with the
   decisions at its start. Built 2026-10-28, with these decisions:
   - The cut-off, the stamp, the basis and the control row stay until step 3, which replaces them with the sweep;
     only the store's versions go now.
   - One write method, `write(Collection<Write>)`: a value or a tombstone per key, at the core's version, conditioned
     on a version, on the key missing, or on nothing; it returns only the hashes that conflicted. It replaces `put`,
     `putIfUnchanged`, `invalidate` and `invalidateIfUnchanged` (and `Entry`, `ExpectedEntry`, `Expected`,
     `WriteResult`). `expire` takes the version its rows get.
   - Superseded 2026-10-28 by "Monotonic versions" (the incarnations do not hold under contention,
     measured in step 6).
   - Versions are packed into the one `long` (`InternalVersion`): 39 random bits of incarnation, 24 bits of counter.
     Schemas, messages and the polling feed are unchanged. Incarnation 0 is below every other (a value from the
     source alone); a new incarnation once the counter is exhausted.
   - Compared with others (checked 2026-10-28 in their sources): the epoch plus a counter within it is an
     established pattern. What differs is who issues the epoch, and so whether epochs are ordered:

     |                      | Epoch                                                  | Order across epochs                                                               | Issued by                        |
     |----------------------|--------------------------------------------------------|-----------------------------------------------------------------------------------|----------------------------------|
     | Hazelcast near cache | partition UUID, plus a sequence per partition          | none: a new UUID resets the tracking, the data is distrusted (`RepairingHandler`) | the cluster                      |
     | Infinispan           | topology id, plus a counter (`SimpleClusteredVersion`) | yes: compared topology first, then counter                                        | the cluster                      |
     | Raft                 | term, plus an index                                    | yes: terms increase                                                               | an election                      |
     | this library         | random incarnation, plus a counter                     | none: L1 drops what it cannot order                                               | the writer, without coordination |

     Increasing epochs need a coordinator or the store to issue them; that is what store-assigned, returned versions
     (memcached's and Couchbase's CAS, DynamoDB's ETags, the earlier design here) buy back: every version ordered, no
     drop for an unknown order. Without them this is about the best available, and close to Hazelcast's. How often
     the drops across incarnations happen is for the benchmark (step 6).
   - A read returns a value row older than the maximum age as `EXPIRED` at its version, instead of omitting it, so
     that a write can be conditioned on it without a conflict reporting the row.
   - A conflict reports no row: a load starts over with a read, a refresh reads the row to answer the newer value.
   - A `put` on a held key writes conditionally on the held version, continuing its incarnation; on a conflict it
     reads the rows' versions and writes on them again, at a new incarnation only after the third conflict. So
     nearly every write continues the key's one incarnation, and L1 can order it.
   - L1 across incarnations: a value not known to be at least as new is dropped (a message, a refused commit, two
     held values meeting). A commit refused or dropped for an unknown order also drops a load in flight and blocks
     every write still in flight on the key until they have all ended (the minimum becomes "anything").
   - The heartbeat beats at versions of an incarnation of its own and counts only its own beats as arrived.
   - Required: `read`, `write` with a condition, returning applied or conflict; `delete`; `keys` in batches.
   - Versions are (incarnation, counter), computed by the core.
   - The stamp, the cut-off, the basis and start values from the store go.
3. **`invalidateAll` by the store's time, hidden on read** (decided 2026-10-28, replacing the sweep). See "Target
   design: `invalidateAll` by the store's time, hidden on read". Built 2026-10-28, with these decisions:
   - The control row is a value row under `Store.CONTROL_HASH` (`#invalidated`), written by the core through `write`
     and added to every read; its write is announced as an ordinary `WRITTEN`, which the core takes as
     `invalidateAll` (`Change.Kind.INVALIDATED_ALL` is gone). Reserved rows (`#` hashes) are never expired or pruned.
   - `read` returns the rows only (`ReadResult` is gone); `VersionsResult` carries versions and ages; `Row` and
     `Write` have no stamp and no basis. PostgreSQL and MongoDB refuse tables of the previous layout (schema
     marker 2) unless reset; the JDBC adapter refuses a table with a `stamp` column.
   - `Store#clockAllowance()` (10 ms by default, zero for the in-memory store): what `invalidateAll` waits before it
     returns, so that no `put` pays for a wall clock's skew or millisecond resolution.
   - The atomic map operations of one instance are atomic per key against its own `invalidateAll`, as in Caffeine:
     they hold the shared side of `InternalClearLock` from decision to commit, `invalidateAll` the exclusive side
     while it writes the control row and clears L1. Monitor-based, not `ReentrantReadWriteLock`: Lincheck 3.7's model
     checker reports correct uses of the latter as hung once more locking happens inside (reproduced with a probe),
     and models monitors correctly. `invalidateAll` from inside an atomic operation is refused with a clear error.
     Puts, reads and loads take no lock; loads keep the accepted race, as in Caffeine.
   - The hot path (deciding on a trusted held value) applies on every store now.
   - Tests: the race tests that closed the accepted race are disabled with a reason (the acceptance tests of its
     repair); the two-instance Lincheck model no longer has `invalidateAll` (an instance not having heard of one is
     the accepted race); the property test's freshness oracle allows a token the source held when an
     `invalidateAll` began, and a held copy of one after size pruning.
   - The control row becomes a reserved key the core writes through `write` and adds to every read; a row at least as
     old as it counts as missing (ages, not stamps).
   - `invalidateAll` waits the maximum skew of a wall clock before returning (not with MongoDB's cluster time).
   - Maintenance deletes hidden rows in paced batches.
   - The stamp, its sequence, the basis, `checksBasis`, `Row#visible`, the cut-off fields and `Store#invalidateAll`
     go.
   - A racing load is accepted as the industry does (its repair is under "Later"); the Lincheck models and the
     property test's oracle change to Caffeine's semantics: atomic per key, loads in flight during `invalidateAll`
     allowed to survive.
4. **One reservation registry,** in place of `minimumVersions` and `writing` with `writeMinimums`. See "Design study:
   L1 by invalidation and reservation", with the order rules of "Target design: reservations in L1, versions read but
   never returned by writes". Built 2026-10-28:
   - One map per hash of (holders, minimum): the operations in flight that will put into L1, and the least version L1
     must hold when they do ("anything" where the order is not known). Messages and removals raise the minimum; an
     install checks it under the key's entry; every release drops what is held below it.
   - Every operation that installs reserves: loads (single, bulk, and Caffeine's own refresh load), `putAsync`,
     writes, reads of the store and refreshes. A load reserves right after its future entered L1; a message finding a
     future in flight without a reservation (that window) drops it: its waiters still get its value, its owner does
     not hold it, at most one miss.
   - Gone: `checkMinimumVersion` and its callback per message on a load in flight; negative caching asks the
     reservation's minimum instead of `minimumVersions`.
   - Order stays by version within an incarnation (the design study's equality-only rule is not adopted): a late
     message of an older version still leaves a newer held value in place.
5. **MongoDB's bulk writes in one round trip:** a filter on the version, an upsert, a duplicate key meaning conflict.
   The hot path becomes one operation on every store.
   Built 2026-10-28:
   - One unordered `bulkWrite` per batch of single-document upserts, in place of one `updateOne` per key (up to 16
     concurrently, on a thread pool of their own, now gone). MongoDB's classic bulk write reports per write only its
     errors and its upserts (by index), not whether an update matched, so every write is an upsert whose filter
     carries its condition: unconditional on the id; "missing" on the id and a missing version; a version on the id
     and that version. A document with another version, or any for "missing", fails the insert on the duplicate key:
     the conflict, by index.
   - A write on a version whose document is gone (deleted by maintenance since it was read) inserts, reported as an
     upsert: a conflict too. Its pipeline, finding no write time (only the filter's fields), writes a tombstone at the
     write's version instead of the value, and the change stream announces it. The store contract allows exactly this
     (`Store#write`): it reads as missing, holders only drop a key whose row is gone, maintenance prunes it.
   - Rejected: updates without upsert and a read when fewer matched than sent: a write applied and at once
     overwritten by another instance would look like a conflict, and a `compute` would run twice.
6. **The benchmark:** `EndToEndBenchmark` with a hot key and a contention scenario, before and after. Run 2026-10-28,
   with a contention section added (one hot key, ten hot keys, `compute` on one key; throughput, store reads per get,
   conflicts, new incarnations per 1,000 writes). Result: versions of incarnations do not hold under contention; see
   "Monotonic versions". The interim fixes measured there (the version hint, draining by tickets,
   unconditional puts on keys not held) are not kept.
6a. **Monotonic versions** (decided 2026-10-28): see "Monotonic versions, proposed by the core,
   enforced by the store". Built first on the core and the in-memory store, measured with the benchmark's contention
   section; ported to PostgreSQL, MongoDB and JDBC only if the hot-key numbers return to the level before step 2.
   **Built 2026-10-28**, on all stores; measured in "Built and measured" under the target design. Found with it, older
   than it (step 4): a deadlock between L1 and the reservation registry. Caffeine calls the loader of a refresh of a
   key not held under its lock of the key, and the loader reserved and released there, while install, release and
   messages take the registry's lock before L1's (both ConcurrentHashMap bins, so even two keys could meet). Both now
   run on the store executor; the store executor must run tasks on other threads (stated in its Javadoc). The property
   tests had hung about once in two to five runs; 25 runs in a row after the fix without one.
6b. **Versions applied as well as each store can** (decided 2026-10-29): see "Versions applied as well
   as each store can". In this order:
   1. the SPI's result per write (applied version or refused) and the core taking the reported version, all stores
      conditional, nothing measured to change;
   2. the in-memory store profiles (see "In-memory store profiles"): `StoreProfile`, the missing dimensions, a named
      profile per real store and hostile ones; contract tests across every profile, Lincheck and property tests across
      the real-store and hostile profiles; fixing what they find. **(1) built 2026-10-29**: `WriteResult` (refused, and
      the versions a store assigned), the core taking the reported version (advance, conditional writes, bulk loads,
      refresh), every store still conditional; verified on the full in-memory suite with model checking and on
      PostgreSQL and MongoDB. **First dimension of (2) built 2026-10-29**: versioning (conditional, assigning in a
      domain of its own, assigning in the core's, mixed per call), the profiles `jdbc`, `postgresql`, `mongodb`,
      `redis`, the whole in-memory suite runnable under one (`-Dinmemory.profile=…`), the contract tests asking the
      fixture whether it assigns. Found and fixed with it: the heartbeat compared the version it proposed with the one
      announced, never equal where the store assigns its own (now the version the beat was applied at); `UNCONFIRMED`
      was 1, the first version of a store numbering its own, so a value loaded while the store was down was confirmed
      by the first row written (now 0: below every version a store issues, equal to none; a refresh of such a value
      is conditioned on no row). The other dimensions follow. **Cleanup checked 2026-10-29**: every helper structure
      that could grow (the reservation registry, the claims of adapters, PostgreSQL's listening sessions, MongoDB's
      watchers, the loads per thread, the remembered absences, the delivery queue) is bounded or emptied on every
      path. Fixed: a store executor refusing a task left a reservation behind (the release now runs on a thread of
      its own then, never the caller's), a future put reserved its key while pending, for ever if never completed
      (now only for its store work), and Caffeine's stats counter of a failed build stayed on its thread. Tested as
      invariants: the registry is empty after every Lincheck invocation (`@Validate`) and every property phase, no
      session or watcher is left after every adapter test, and an adapter is released on close and on a failed build;
      the one bound by design is a user's loader future that never completes, whose key stays reserved while
      Caffeine holds that future; **A stale read found and fixed 2026-10-29**: `invalidate` (and
      `invalidateAll(keys)`) removed whatever L1 held at its end, a value written after its tombstone too; a slower
      put written between the tombstone and that value then installed its older value in the gap, served stale on
      this instance (without distribution no echo corrects it). Hidden among the Lincheck failures counted as the
      uncached load (about one stress run in five) until that one was left out; caught by a trace of every install
      and removal in L1. Now only what is older than the tombstone goes; with the outcome unknown, everything goes and
      nothing in flight installs (`dropOlder` at `ANYTHING`), also for `update` and `putAll`;
      **The other dimensions of (2) built 2026-10-29** (0 to 7, `StoreProfile` and `InMemoryDatabase`): the change
      source (a polled profile hands out stores with a scan only, the core polls them, and the database counts as
      idle once every polled store began two scans after the last commit; `jdbc` is polled now), the store clock
      (monotonic, microseconds or milliseconds with repeats, a declared step back, the fault `stepClockBack`), reads
      row by row with a hook between rows (`mongodb`), batches applied in part (`failNextWritesPartly`), a failover
      undoing the last window's writes with a gap (`failover`), the tombstone on a purged row (`mongodb`), silent
      eviction (`evict`) and messages lost silently (`setDropProbability`); named hostile profiles `redis-hostile`
      and `postgresql-stepping`. `InternalStoreProfileTest` pins each behaviour; the property test adds the faults
      its oracles hold under (clock steps within the allowance, pauses between rows, partial batches). Found with
      it: the allowance below a millisecond was not waited, three stores raised negative ages to zero (a put hidden
      and replaced by a load), the direction of the polling edge (a step forward, not back), no age bound on L1
      copies under a lossy feed, and a write after an eviction below a held version (open; see the dimensions).
      And, by running the whole suite polled (the parity tests failed): a poll reports every change within its
      overlap of 2 s again, the control row's too, and the core applied every message of the control row as a new
      `invalidateAll`, so a polled instance cleared its L1 on every poll for 2 s after any `invalidateAll`, dropping
      what it put meanwhile. Now each control row's version is applied once (the latest remembered, its own write
      included), which also settles the double clear by its own echo (Later). Tests of the feed's own faults or of
      its messages counted one by one run only under a profile with a feed (`CacheBackend#requireFeed`).
      Not built yet: create-then-update, a refusal reporting the row, a store without a clock, feed retention, the
      bus, and the real stores' contract tests checking their profile;
   3. the bound on clocks far ahead and the end of the range as an error, in the core, tested on the controllable
      clock (its hole exists today). **Built 2026-10-29**, with two thresholds instead of one (a single bound would
      have left a key written by a clock beyond it unwritable: the retry after a refusal, still below the row,
      refused until its attempts ran out): a version seen more than a minute ahead is counted
      (`DistributedStats#clockAheadCount`) and logged at most once a minute; up to a day ahead the clock follows it,
      beyond it does not, and a write decided on such a row (`next`, also the retry after a refusal) goes just above
      it, the clock left where it is; the end of the range throws instead of repeating a version. The skew meant is
      between the writers' clocks, the instances' and a store server's where it assigns in the core's domain; a store
      numbering its own versions takes no clock into its rows;
   4. PostgreSQL assigning, single and bulk, its own sequence as the floor and the proposal ignored (as before step 2),
      checked against its profile, measured with the contention section. **Built 2026-10-31**: a sequence per table
      (`<table>_versions`, in the DDL, owned by the `version` column, so dropped with the table; its absence refuses
      the start with the DDL); every write, conditional ones and the expiry included, applies at
      `GREATEST(r.version + 1, nextval())` (a new row at `nextval()`), returned with `RETURNING` into `WriteResult`;
      the proposal is no longer bound, the version given to `expire` ignored. As every write draws the sequence, it
      stays above every version of the table, so the `GREATEST` is the sequence itself except over a row above it
      (by hand, a sequence set back). Schema marker 3: a table of marker 2 holds the core's versions, far above a new
      sequence, so a row created anew there would start below what instances hold; it is refused (`withSchemaReset`
      drops it) instead of migrated, as with marker 1 to 2. A failover to an asynchronous standby may promote a
      sequence below the version of a lost write; the gap of the failover revalidates what is held
      (`a_confirmed_write_the_failover_loses_is_held_by_no_cache` green). Contract fixture: assigns, own versions.
      Verified on PostgreSQL 18 (single server, small notify queue, transaction pooler, both standbys).
      Measured (2026-10-31, local container, 2 instances x 4 threads, 5 s; the commit before, c561c9b, measured
      right before on the same machine, slower today than in the tables above):

      | Scenario                                       | Proposed (c561c9b) | Assigned    |
      |------------------------------------------------|--------------------|-------------|
      | 1 hot key, 80/20: ops/s                        | 12,737             | 23,569      |
      | the same: store reads per get, refusals in 5 s | 0.31, 15,550       | 0.00, 0     |
      | 1 hot key, 50/50: ops/s, store reads per get   | 4,625, 1.27        | 9,923, 0.00 |
      | 10 hot keys, 80/20: ops/s                      | 22,097             | 25,781      |
      | compute (increment) only: ops/s                | 1,118              | 1,266       |
      | throughput, 8 threads, 80/20 (1,000 keys)      | 27,073             | 25,735      |
      | put, p50 µs                                    | 313                | 319         |

      A racing put no longer reads and writes again: the single hot key nearly doubles, as before step 2 (29,043
      against 14,297 then); the uncontended rows are unchanged within the noise (`nextval` per write costs nothing
      measurable);
   5. MongoDB assigning for single writes, the server's milliseconds in the core's format as the floor and the bounded
      proposal in the maximum, checked against its profile, measured. **Built 2026-10-31**: a write of one key is a
      `findOneAndUpdate` (a pipeline, returning the document after) at `max(stored + 1, floor, min(proposed, floor +
      1 min))`, the floor `($$NOW - 2020) × 2^20`, the core's format with counter and node bits zero; the bound is the
      core's `WARN_AHEAD_MILLIS`, so an instance's clock more than a minute ahead no longer carries a key's versions
      with it (where a batch, still conditional at the proposal, does not write it). Its condition is its filter: an
      advance filters on the id alone and always applies (a concurrent upsert of a new id the server retries); on
      "missing" a duplicate key is the conflict; on a version there is no upsert, so a document pruned since its read
      is left absent instead of written as a tombstone (only the bulk write needs that, to report the conflict per
      write). The in-memory `mongodb` profile follows: its tombstone on a purged row only in a batch, and the
      assigning profiles (`mongodb`, `redis`) bound the proposal the same way. Contract fixture: assigns for one write.
      Verified: MongoDB 9 (every topology, sharded included, 339 tests) and 4.4.0 (single node and sharded, 243),
      the in-memory suite under `mongodb` and `redis` (the two clock-ahead tests of `InternalStatsTest` now write
      their row ahead in a batch, which a store assigning single writes keeps at the proposal; a new test checks the
      bound of an assigning store). Measured (2026-10-31, local containers, 2 instances x 4 threads, 5 s; c561c9b,
      unchanged for MongoDB, in three runs, the change in two):

      | Scenario                                       | Proposed (c561c9b) | Single writes assigned |
      |------------------------------------------------|--------------------|------------------------|
      | 1 hot key, 80/20: ops/s                        | 14,505–14,954      | 21,398–22,012          |
      | the same: store reads per get, refusals in 5 s | 0.17, 9,592        | 0.01, 0                |
      | 1 hot key, 50/50: ops/s                        | 4,257–6,136        | 8,647–8,878            |
      | 10 hot keys, 80/20: ops/s                      | 19,370–20,383      | 20,086–21,269          |
      | compute (increment) only: ops/s                | 571–680            | 437–692                |
      | throughput, 8 threads, 80/20 (1,000 keys)      | 19,852–21,430      | 18,644–21,157          |

      Compute looked a fifth slower at first; it was the order of the runs (the second of a pair ran faster, either
      way round: 571 before against 692 after, reversed). Assigning only single advances, conditional ones left on
      the bulk path, measured the same (compute 491–692, the rest within the ranges), so conditional single writes
      assign too, which gives a load creating a document anew the server's floor.
6c. **A review of the two adapters** (2026-10-31; robustness, performance, cleanness). Fixed:
   - MongoDB's watcher: a subscription's generation was taken before the stream opened for it raised the generation,
     so the first `resume()` of a cache suspecting the stream silent took itself for outdated (another cache having
     reopened) and did nothing, the stream silent until the next suspicion; now taken once the stream covers it.
   - MongoDB's watcher: a resume reopens the stream on a cache's thread while the watching thread's read waits on the
     old cursor; that read failing later (its timeout on a silent connection) was blamed on the current cursor, the
     one just reopened, which was then lost and every cache on it told. Now a failure counts only for the cursor read.
     `MongoAdapterTest.the_read_of_a_cursor_replaced_by_a_resume_failing_later_loses_nothing` (a silencing proxy)
     fails without either fix, each on its own; the contract test
     `contract5_resuming_a_delivery_that_seems_alive_tells_no_one_of_a_loss` pins the behaviour on every feed.
   - MongoDB's expiry assigns as its single writes do (`max(stored + 1, floor, bounded proposal)`), so a document
     written above the maintaining instance's clock (the server's floor) is expired anyway; the in-memory assigning
     profiles follow, and the contract accepts an assigned expiry.
   - The SPI's Javadoc (`Store`, its versions point and `expire`; `Write`) still said that the store stores the
     core's version and computes none; it now describes both ways of applying.
   - `InternalStoreProfileTest` waited for the database only, not for the caches applying what it delivered (a
     flake, about 1 in 10 runs); now for both, as the property and parity tests do.
   Left: maintenance in paced batches (built since, 6d); a closing cache holding the PostgreSQL listener's global
   lock while its `UNLISTEN` waits up to 10 s on a silent session; fully qualified names in `MongoWatcher` and
   `PostgresListener`; MongoDB 4.4's `$expr` filters of maintenance possibly not using the index (not measured).
6d. **Maintenance in paced batches** (built 2026-10-31, recorded in the review of 6c). Measured first, one call on a
   million rows (local containers, a concurrent put every 20 ms): PostgreSQL 18 `expire` 6.6–9.0 s, `pruneTombstones`
   1.8–3.2 s, `pruneToSize` 1.7–2.0 s; MongoDB 9 12.9–24 s, 36–38 s, 39–44 s. The concurrent puts barely noticed
   (at most 40 ms on PostgreSQL, a few slower ones on MongoDB); the problem was the length of one call against the
   store's default timeout of 30 s: a MongoDB `expire` of more than about 2.5 million rows, a PostgreSQL one of more
   than about 4 million, would have failed every run, the backlog never worked off (reads hid the rows meanwhile, but
   no message went out). Built:
   - the SPI: a limit per call on `expire`, `pruneToSize` and `pruneTombstones`; the core calls again while a call
     reaches it, the next batch after a pause as long as the last took, the run ended by a failure. Two limits
     (`InternalMaintenance`, measured 2026-11-01 on a million rows): 10,000 for expiry, whose messages land in every
     instance at once (beyond 100,000 pending an instance drops them and revalidates); 100,000 for both prunings,
     which are silent, which brought their totals close to one call (PostgreSQL `pruneToSize` 2.7 s against 1.7,
     MongoDB 43 s against 38; with 10,000 up to twice that), the longest call below 5 s; a store whose every change is a step of its own may change more (`PrimitiveStore`, JDBC: a
     limit would only make it scan every row again per batch);
   - PostgreSQL: `LIMIT … FOR UPDATE SKIP LOCKED` for expiry and tombstones (concurrent instances take other rows
     instead of waiting); a keyset cursor per store, where the last full batch ended (expiry by write time,
     tombstones by id), kept only after a full batch committed, so that a batch does not read past the dead index
     entries the batches before it left (from the start each time: 30 and 27 s for a million); pruning to size by the
     rank boundary first, then the oldest beyond it (the newest beyond the rank first read past what earlier batches
     had deleted: 26 s);
   - MongoDB: ids found with a limit, then the update by `_id` (hinted: the planner took the maintenance index and read
     every due document per chunk, 565 s), in chunks of 1,000 (a million ids in one filter exceed the 16 MB of a
     command); the maintenance index extended by the hash, `(_id.d, state, writeTime, _id.h)`, so that pruning to
     size ranks by the index alone, ties now kept by the highest hash (every batch sorted all values in memory
     before: 194 s); a collection with the old index logs the warning about a missing index.
   Paged, a million rows (total, longest call): PostgreSQL `expire` 8.9 s / 0.14 s (one statement 9.0 s),
   `pruneTombstones` 15.3 s / 0.22 s (3.2 s), `pruneToSize` 5.7 s / 0.12 s (1.7 s); MongoDB 29.2 s / 0.37 s (23.9 s),
   46.1 s / 0.72 s (37.6 s), 79.4 s / 1.37 s (39.0 s). Every call is now far below the timeout whatever the backlog
   (the longest grows slowly with it on MongoDB's pruning to size), the messages of an expiry come in batches of
   10,000, and the core's pauses double the time a backlog takes in the background. A lesson of the measurements:
   rows written by one statement share one write time, which no cache has; the experiment writes them apart
   (`MaintenanceExperiment`, `*MaintenanceExperimentTest`, `-Dexperiments=true`).
7. **Later:** see "Open work" (moved there 2026-11-18).

The frame for every step: "Positioning and the complexity budget".

### Starting without the store: the build log (2026-11-12 and 2026-11-13)

**Condensed 2026-11-18;** the design is under "Starting without the store". Built in five steps: the README on liveness
and readiness under a store outage; the stand-in store, the connector and `withFailFast()` on the in-memory store; per
adapter, "not reached" at creation aligned and tested (`AdapterConformanceTest` through `SilencingProxy`: paused,
stopped, unresolvable, unreachable, wrong credentials; `AdapterOutageTest` on topologies of several addresses and
without a primary, with parity to the outages while running); the property test's restarting variant (an instance
closed and built again while its adapter refuses, others writing meanwhile; threaded and deterministic on every
profile) and `InternalSimulationTest` (an hour without the store: about 130 attempts, a warning every 5 minutes);
and the client notes, costs entry, README and release notes. Found and fixed on the way, each kept in the code:
- **Wrong credentials started the cache without its store** on MariaDB/MySQL (HikariCP reports a refused login as its
  own timeout) and on Redis (every library reports `WRONGPASS` as a failure to connect): `JdbcStore#refused`,
  `PostgresStore#refused` (SQLState classes 28 and 3D, codes 1044/1045/1049, through the causes),
  `RedisCommandException#credentialsRefused`.
- **A paused PostgreSQL held `build()` for 65 s** with a pool-less `PGSimpleDataSource` (pgjdbc's `connectTimeout`
  bounds the TCP connect only): documented ("Timeouts"), `loginTimeout`; a pool's `connectionTimeout` bounds it already.
- **A paused MongoDB held `build()` for good, even under `withFailFast()`:** the deployment check's `hello` on `admin`
  and, through mongos, `config` carried no timeout: `MongoSchema#bounded`.
- **Redisson with lazy initialization** throws a failure to connect while submitting the first command, not through its
  future: `RedissonSession#eval` and `#server` classify it as one the future reports.
- **Creation during an outage classified as for a running cache:** MongoDB without its majority (the cluster-time seed
  write timing out), Redis with every server paused (`INFO server` unanswered), a PostgreSQL table created by a collided
  attempt (`duplicate_table`, `duplicate_schema`, a catalog `unique_violation`), MongoDB 4 and 5 stepping the primary
  down during the schema check (codes 11602 and 189), PostgreSQL starting up (57P01 to 57P03): all retryable at
  creation now. A pooler in statement mode (08P01) is a configuration error again, failing the build. A PostgreSQL
  standby is refused as retryable (the user's decision), like MariaDB `read_only` and a Redis replica.
- **Limits of the libraries,** in the client notes: Jedis cannot create a Sentinel or cluster client while the servers
  are out; Redisson with lazy initialization waits its own retries (about 16 s) inside the first command; PgBouncer
  hands out connections broken by a server restart once more (the first statement on each fails, outcome unknown).
- **In the tests only:** Sentinel's restore after an outage without a primary (Redis 6.2.0 lists no replica after
  `+switch-master`, so the restore sends `REPLICAOF` after 15 s; the outage waits until Sentinel left TILT mode);
  Kvrocks has no users, so the credential tests skip it; the property test's backend kept closed polled stores among
  those it waits for; `-Dproperty.phases` added for longer runs.

### Built from the list of later work

**Moved here 2026-11-18** from "Open work", where done items leave the list:
- **built 2026-10-29 with 6b(2)** (a polled instance cleared on every poll of the overlap, see there): an
  `invalidateAll` not cleared twice by its own echo (recorded 2026-10-29): the sending instance clears its L1
  before the call returns, and clears it again when the control row's message comes back (`apply` does not tell
  its own from another's), dropping what it held meanwhile and moving the epoch once more (store reads and
  revalidations, nothing wrong: what it put meanwhile was written after the control row and is read again).
  Remembering the version of its own control row and skipping exactly that message avoids it; a control row of
  another instance has another version and still clears. Safe because whatever this instance holds after its
  own clear was decided after the control row was written: a message of a write before it is older than what
  is held, and a read hides the rows before it anyway;
- **built 2026-11-01** (see "Redis adapters", item 11): the Redis cluster;
- **built 2026-11-01** (see "Redis adapters"): the Redis adapters; no Spring Data Redis binding (decided);
- **built 2026-11-05** in another form: a distribution-only store mode (proposed 2026-10-27), as
  `withMaximumAgeForStoreOnly()` (see "Maximum age for the store only"): the store keeps values for its maximum
  age, and the local copies are no longer bounded by it. Not taken from the proposal: misses skipping the store (a
  second load path with its own version and trust rules, for one round trip per miss), and values not written
  with `INVALIDATE` (`UPDATE`, the atomic methods and the misses within the age read them);
