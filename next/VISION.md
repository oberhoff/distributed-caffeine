# Distributed Caffeine — next generation

Status: design settled (2026-10-21), measured by the experiments in `src/test/java/.../experiments`; built
(2026-10-22): the core with all four cache variants, the PostgreSQL and MongoDB adapters, the property and parity
tests. Standalone module, not part of the root build, not released.

## Why a new generation

The current library keeps the *contents* of all cache instances in step: population pushes every value to every
instance, eviction is distributed so the key sets do not drift apart, and the store keeps a cached and an evicted
tier whose statuses depend on the distribution mode. Most of the hard defects of the last year came from exactly
that contents synchronization (eviction echoes, the restore-mark race, the load-bearing own-echo re-add, silent
entry loss), and most of the configuration surface exists to steer it (six distribution modes, two persistence
tiers, loading strategies). Two structural problems cannot be fixed inside that model:

- Caffeine evicts per instance (W-TinyLFU decides from local access, access and variable expiry are local), so
  distributing an eviction turns a local decision into a cluster-wide one.
- A value reloaded from the store gets a fresh lifetime, because the store does not carry its age, so an entry can
  circulate between the instance and the store forever.

This generation is written from scratch rather than ported, so the assumptions of the old model do not come along.
The lessons do come along, as tests and as the checklist below.

## Model

Two levels, as in JetCache:

- **L1** — a Caffeine cache per instance (internally an `AsyncCache` for every variant, see "Shared loads"). The
  user's Caffeine settings (size, weight, every kind of expiry, refresh, stats, listeners) work and stay strictly
  local; only weak and soft references are rejected. An instance holds whatever subset of keys it read.
- **L2** — a shared store (MongoDB collection, PostgreSQL table). Every row carries the value, its state, its
  **write time** and a store-assigned **version** that orders writes per key. L2 has its own bounds: a maximum age
  counted from the write and a maximum size (oldest writes pruned first).
- **Distribution** — change streams / LISTEN-NOTIFY carry invalidations (the default), optionally population, or
  nothing at all (`DistributionMode`, see "Distribution modes").

Operations:

| Operation | Behaviour |
|---|---|
| read | L1 → L2 (only if younger than the maximum age) → loader / mapping function, then write L2 (conditionally) and L1; without a loader (`getIfPresent`) L1 → L2 only |
| refresh | source via `CacheLoader.reload`, then write L2 (conditionally) and distribute; never from L2 |
| write | write L2, then distribute; L1 updated |
| invalidate | write L2 (removal), then distribute; L1 removed |
| local eviction | L1 only. Never distributed, never changes L2 |
| received invalidation or expiry | drop the key from L1 |
| received write, `UPDATE` | **only if the key is held** and older: read the row (batched) and apply it; never adds a key |
| received write, `INVALIDATE` (default) | drop the key if held |
| `NONE` | nothing is received: a copy changed elsewhere stays until Caffeine expires or evicts it |

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

In scope for the first version:

- feature parity with Caffeine's `Cache`, `LoadingCache`, `AsyncCache` and `AsyncLoadingCache` on every user-facing
  method, including `asMap()`, `synchronous()`, `policy()` and `stats()` (see "Feature parity with Caffeine")
- an inspection API: `getFromStore` / `getAllFromStore`
- global `invalidateAll()`
- MongoDB and PostgreSQL adapters behind a small SPI
- distribution modes: `INVALIDATE` (default), `UPDATE`, `NONE`
- optional absent caching (a local set of recent misses)
- lifecycle: `build()` fails fast, `close()` (`AutoCloseable`)
- store guard: degraded state when the store does not answer, reads and writes guarded
- synchronization state reporting and distribution stats

Explicitly out of scope (and why):

- eviction distribution and contents synchronization — the source of the old model's defects
- an evicted tier and record statuses — a row is a value with a write time, nothing more
- configurable loading strategies — read-through L2 is always on
- a mode without a store — L2 is the model


## Configuration sketch

Names are placeholders.

```java
DistributedCaffeine.newBuilder(adapter)
    .withCaffeine(caffeine -> caffeine                         // Caffeine's builder methods, one to one
        .maximumSize(10_000)
        .expireAfterWrite(Duration.ofMinutes(5)))
    .withSerializers(serializers -> serializers                // optional, default Fory; per cache
        .withKeySerializer(StringSerializer.ofString())
        .withValueSerializer(new JacksonSerializer<>(Value.class, true)))
    .withStore(store -> store
        .withMaximumAge(Duration.ofHours(1))
        .withMaximumSize(1_000_000))
    .withDistributionMode(DistributionMode.UPDATE)  // optional: INVALIDATE by default
    .withAbsentCaching(Duration.ofMinutes(1))  // optional: local set of recent misses
    .build(key -> source.load(key));      // a DistributedLoadingCache, AutoCloseable
```

No validation between Caffeine's expiry and the maximum age is needed: the maximum age is enforced independently
(see "Maximum age").

## Decisions

### Key identity: hashes

A row is identified by a hash of its key, as in the current generation. Java's `hashCode()` is not stable across
JVMs, and keys are not required to be readable by the store, so the identity must be computed deterministically on
every instance:

- XXH3-128 via hash4j, rendered as the row id together with the cache discriminator (MongoDB `_id` as `{d, h}`,
  PostgreSQL primary key `(discriminator, id)`; see "Adapter setup"), held compactly in `InternalKey` (see "How a
  message finds the held key").
- Out of the box for `String`, `Long`, `Integer` and `UUID`; other keys implement `Hashable` or the cache is given a
  `HashProvider`.
- Messages identify keys by hash (all kinds except `INVALIDATED_ALL`), which also keeps PostgreSQL notification
  payloads small.
- Collisions are treated as impossible (128 bits); this is stated in the docs rather than handled.
- The serialized key is still stored in the row, for diagnostics and readability of the store; nothing in the
  library reads it back (keys are matched by hash).

### Clock: only the store's clock

No instance clock is ever compared with a stored time.

- The **write time** is set by the store in the write itself (`$$NOW` in a Mongo update pipeline, `now()` in
  PostgreSQL).
- The **maximum-age check** runs inside the query against the store's clock
  (`write_time > now() - max_age`, `$expr` with `$$NOW`).
- Where an instance needs the age (refresh rule 2, `getFromStore`), the store returns the **age**
  (`now() - write_time`), not the timestamp.
- Messages carry no time. A holder that reads a row after a message gets its age like any other read.
- Accepted: on Mongo the clock is the current primary's, so a failover may shift it slightly. Harmless for ages,
  irrelevant for ordering (below).

### Ordering: a per-key version assigned by the store

Every write sets, atomically in the same upsert as the value:

```
version = max(old.version + 1, server time in microseconds)
```

- `old.version + 1` makes the version strictly increasing per key, because the store serializes writes to one row
  (row lock / document atomicity), even if the server clock goes backwards.
- The server-time term keeps it increasing when a key is recreated after its row is gone.
- The order the store applied writes in *is* the order. There is no other order everyone can agree on.

Rules on the instance:

1. Every L1 value carries its version. The writer gets it back from its own upsert (`RETURNING`,
   `findOneAndUpdate` returning the document after the update).
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

`invalidateAll` marks all rows of the cache as tombstones in one bulk update, then sends one message. Receivers
clear L1 and increment the revalidation epoch (see "States and recovery"); a load started before the increment
completes with an older epoch, so it counts as unconfirmed, is checked against the store on its next read and finds
the tombstone. One counter serves revalidation and `invalidateAll`; global ordering is needed only locally.

This replaces, from the current generation: operation ids, activation ids, the ordering remembered by the
publishing instance, echo handling, and refresh/load tokens.

### Distribution: messages carry no values

On both stores a message is only **(kind, hash, version)**. A holder that needs the new value reads the row; values
never travel through change streams or notifications.

| Kind | Holders do (only if the key is held and the message version is higher) |
|---|---|
| `WRITTEN` | population on: read the row and apply what it holds (value or tombstone); population off: drop |
| `INVALIDATED` | drop, no read |
| `EXPIRED` | drop, no read (the maximum age was exceeded; distinct from `INVALIDATED` for stats only) |
| `INVALIDATED_ALL` | clear L1, increment the epoch (no hash, no version) |

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

| Journal default | Holders | A applied p50 / p99 | B applied p50 / p99 | B reads per write | Writer's ack p50, A / B |
|---|---|---|---|---|---|
| `true` | 1 | 762–920 / 2,175–2,517 µs | 1,067–1,071 / 2,107–2,259 µs | 1.00 | 707–862 / 778–824 µs |
| `true` | 4 | 1,064–1,069 / 2,266–2,871 µs | 1,305–1,310 / 2,297–4,508 µs | 4.00 | 988–1,001 / 995–1,009 µs |
| `true` | 16 | 1,195–1,215 / 2,392 µs | 1,940–2,089 / 3,106–3,840 µs | 15.94 | 971–976 / 1,206–1,307 µs |
| `false` | 1 | 270–272 / 417–469 µs | 603–613 / 970–991 µs | 0.55 | 211–215 / 202–207 µs |
| `false` | 4 | 381–399 / 614–705 µs | 791–802 / 1,233–1,409 µs | 2.27 | 281–304 / 276–281 µs |
| `false` | 16 | 774–812 / 2,192–2,409 µs | 1,511–1,681 / 4,673–5,757 µs | 8.79–8.94 | 520–545 / 519–581 µs |

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

| Value | Held | Writes/s, A raw / A decoded / B | CPU per instance, A raw / A decoded / B | Received per instance, A / B | Held p50, A / B | B reads per write |
|---|---|---|---|---|---|---|
| 1 KB | 1 % | 3,539–3,750 / 3,651–3,710 / 3,617–3,623 | 324–347 / 313–358 / 320–342 ms | 29.6 / 8.8 MB | ~1,300 / ~1,670 µs | 0.08 |
| 1 KB | 10 % | 3,582–3,612 / 3,637–3,650 / 3,553–3,690 | 329–358 / 350–365 / 324–338 ms | 29.6 / 8.8 MB | ~1,330 / ~1,620 µs | 0.70 |
| 10 KB | 1 % | 2,982–3,144 / 3,096–3,119 / 3,272–3,285 | 428–437 / 467–470 / 355–359 ms | 214 / 8.8 MB | ~1,470 / ~1,690 µs | 0.08 |
| 10 KB | 10 % | 2,852–3,057 / 2,705–2,823 / 2,719–3,008 | 444–451 / 449–500 / 429–466 ms | 214 / 8.8 MB | ~1,500 / ~1,880 µs | 0.70 |

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

### `invalidateAll` and loads in flight (decided and built 2026-10-22)

**The race:** a load reads the store, calls the source, and writes conditionally on what it read. A per-key
`invalidate` always writes a new tombstone, so a load in flight conflicts. `invalidateAll` used to tombstone only the
rows holding a value: a load that had read a tombstone, an expired row or no row at all still wrote the value it read
from the source *before* the `invalidateAll`, and that value stayed in the store until the maximum age (if the
application changed the source and then called `invalidateAll`).

**Rejected on the way** (each discussed): a marker row checked by every conditional write (a cross-row check inside a
write, hard on MongoDB and on most custom stores); compensation after the fact relying on messages arriving in commit
order (true for PostgreSQL and MongoDB, not for custom stores such as partitioned logs, so not a contract one can
ask for); compensation by comparing versions with records of recent own load writes (memory and a retention window,
not crash-safe); a generation per cache with a generation per row (an extra row, an extra column, and invalidated rows
kept until they expire); a placeholder tombstone written by every load of a key without a row (exact, but a write
per cold load; kept in mind).

**Built, by distribution mode:**

- **Every mode: the sweep gives every row a new version,** not only value rows (tombstones and expired rows keep their
  state). A load that read a tombstone or an expired row then conflicts, exactly, as after a per-key `invalidate`.
- **`INVALIDATE` and `UPDATE`: a row created on "missing" takes the time of the read before the source as its
  version** (`ReadResult.time()`, `ExpectedEntry.ifMissing(entry, readTime)`), and `INVALIDATED_ALL`
  carries the sweep's time `T` (`Message.invalidatedAll(T)`, also returned by `invalidateAll()`). After the sweep,
  every row that existed has a version of at least `T`, so a value row with a version below `T` can only be a load that
  read before the `invalidateAll`. Every instance keeps the highest `T` it knows (its own calls and the messages; it
  only grows, so no delivery order is needed) and treats such a row as missing: loads overwrite it on its version,
  `getIfPresent` and population tombstone it conditionally in the background, L1 never takes it. No extra row, no
  extra column, no memory beyond one long, nothing on the common path.
- **`NONE`:** no messages, so other instances do not learn `T` (the invalidating instance does). A racing load's value
  can stay in the store until the maximum age: the same kind and bound of staleness `NONE` accepts anyway (its L1
  expires after the maximum age by default). Documented.
- **Residual, documented:** an instance that does not know `T` (started after the `invalidateAll`, or whose gap lost
  the message) may serve such a row until another instance tombstones it or it expires. It takes a load overlapping
  the `invalidateAll` on a key without a row, read first by such an instance. The placeholder variant would close it
  at the cost of a write per cold load.

**SPI changes:** `read` returns `ReadResult(rows, time)` (the store's time, also without rows; MongoDB can return it
in the same round trip by projecting `$$NOW`); `ExpectedEntry.ifMissing(entry, readTime)` creates the row with that
version; `invalidateAll()` bumps every row and returns its time, announced in `INVALIDATED_ALL`. Three new contract
tests; no ordering requirement. Tested by two deterministic regression tests (missing row, tombstone) and the property
test, whose `invalidateAll` now changes the source first; mutation-checked (seven mutants, all killed, the PostgreSQL
sweep restricted to value rows by the property test's freshness oracle).

### Distribution modes (decided 2026-10-21)

`withDistributionMode(DistributionMode)`, set at most once, replaces `withPopulation()`:

| Mode | Subscription | A write elsewhere | An invalidation or expiry elsewhere |
|---|---|---|---|
| `NONE` | none (no listening connection) | not seen | not seen |
| `INVALIDATE` (default) | yes | held copy dropped | held copy dropped |
| `UPDATE` | yes | held copy re-read (batched) | held copy dropped |

- **One setting, three levels:** population is built on invalidation (same subscription, same messages; only a
  write is applied differently), so "population without invalidation" would distribute writes but not removals.
  An enum makes that combination unrepresentable, maps the current generation's modes almost one to one, and leaves
  room for a level.
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
  messages for absent caching; held values count as trusted. `invalidateAll()` and `invalidate` reach only the store and this instance's L1.
- The property test covers `INVALIDATE` and `UPDATE`; its convergence oracle does not
  apply to `NONE` by design, which has unit tests of its own.

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

### Serialization: part of the cache configuration

The serializer describes the user's types, not the store, so it is configured per cache (one for keys, one for
values), not on the adapter. Caches sharing a table or collection each bring their own.

All serializers of the current generation are shipped: `ForySerializer`, `JavaObjectSerializer`,
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
`ForySerializer.requiringRegistration(...)`, and `JavaObjectSerializer.allowing(...)` / a constructor taking an `ObjectInputFilter` (the allow list also admits
`java.lang`, `java.util` and `java.time`, which the objects commonly contain; the known gadget chains need classes
outside the JDK). `withSerializers(serializers -> serializers.withKeySerializer(...).withValueSerializer(...))` in the
builder; the core's `InternalSerializer` derives the format and moves text as UTF-8.

**Unreadable rows** (a class changed after a deploy, a switched serializer, corrupt data) are a miss: the loader
runs and its result is written conditionally on that row's version (ordering rule 6), so the fresh value replaces
the unreadable row without separate repair logic. A population read of an unreadable row drops the held key;
`getFromStore` throws a clear exception, since the inspection API
reports what is there. Visible as a counter in the distribution stats and one rate-limited warning per cache naming
the class or format. Switching the serializer of an existing table works the same way: old rows are replaced one by
one as they are accessed.

The core serializes and deserializes, and only what it uses (held keys, misses). The adapter never sees
`K` or `V`; the core translates the serializer once into a **format**, passed when the cache's adapter creates its
store:

| Serializer | Format | MongoDB | PostgreSQL |
|---|---|---|---|
| `ByteArraySerializer` (Fory, Java object) | `BINARY` | `BinData` | `bytea` |
| `StringSerializer`, `JsonSerializer` with `storeAsBinaryJson() == false` | `TEXT` | string | `text` |
| `JsonSerializer` with `storeAsBinaryJson() == true` (Jackson) | `JSON` | BSON value | `jsonb` |

How often it happens, and three rules that keep it at the minimum:

| Operation | Serialize | Deserialize |
|---|---|---|
| L1 hit | – | – |
| put | key and value, once | – |
| miss answered by L2 | – | value, once (not the key; the caller has it) |
| population read by a holder | – | value, once per holder, only if the version is newer |
| invalidation, expiry, any message | – | – (messages carry no values) |
| `putIfUnchanged` conflict | – | value of the newer row, once |

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

### SPI: a write is the message

Both stores derive messages from the write itself (change streams see only committed writes, `NOTIFY` is sent in
the writing transaction), so there is no separate publisher: an adapter is a store whose writes announce themselves,
plus a subscription. The adapter concept stays, slimmer: it binds one kind of store (connection, queries,
conditional writes, listener/watcher sharing, discriminator).

**Storage and change feed are separate capabilities** (2026-10-23): `Store` is storage only (reads, writes,
maintenance); a store that can announce its writes also implements `ChangeFeed` (`subscribe`). A store without one
(a plain key-value store without notifications, PostgreSQL without a listener) supports only
`DistributionMode.NONE`: `build()` with another mode fails at once, naming the adapter, and closes the store. So an
adapter for a new kind of store can start with storage alone, and the SPI asks nothing of distribution from it.

```java
interface Store extends AutoCloseable {                 // one per cache, created by its adapter; non-generic

    // reads: value rows only if younger than maxAge; tombstones and expired rows without value; plus the store's time
    ReadResult read(Collection<String> hashes, Duration maxAge);
    Map<String, Long> readVersions(Collection<String> hashes, Duration maxAge); // value rows: version only

    // writes: each one announces itself on commit, the version comes from the store
    Map<String, Long> put(Collection<Entry> entries);                         // unconditional
    Map<String, WriteResult> putIfUnchanged(Collection<ExpectedEntry> entries, Duration maxAge); // rule 6
    Map<String, Long> invalidate(Collection<String> hashes);                  // tombstones, also for missing rows
    Map<String, Long> invalidateIfUnchanged(Map<String, Long> expected);      // remove: if same version, any state, or MISSING
    long invalidateAll();                                // every row a new version; one INVALIDATED_ALL with its time

    // maintenance: scheduled by the core, idempotent in the adapter
    int expire(Duration maxAge);                 // marks EXPIRED, announces
    int pruneToSize(long maxSize);               // deletes, silent
    int pruneTombstones(Duration retention);     // deletes, silent
}

interface ChangeFeed {                           // implemented by a store that can announce its writes
    Subscription subscribe(Listener listener);   // every write committed after it returns is delivered
}

interface StoreFactory { Store createStore(StoreSettings settings); }   // what an adapter provides

record StoreSettings(Format keyFormat, Format valueFormat, Duration timeout) {}   // what the cache configured

interface Listener {
    void onMessages(List<Message> messages);     // batched, per key in commit order
    void onGap();                                // continuity lost: the core revalidates
}

record Message(Kind kind, @Nullable String hash, long version) {}   // WRITTEN, INVALIDATED, EXPIRED, INVALIDATED_ALL
record Row(String hash, byte[] key, byte @Nullable [] value, State state, long version, Duration age) {}
record Entry(String hash, byte[] key, byte[] value) {}              // format fixed per store
record ExpectedEntry(Entry entry, long expectedVersion, long createdVersion) {}  // ifVersion(entry, v) or ifMissing(entry, readTime)
record WriteResult(boolean written, long version, @Nullable Row current) {}  // the newer row on a conflict
class StoreException extends RuntimeException { boolean isOutcomeUnknown(); }  // after the adapter's own retries
```

Built (`src/main/java/.../spi`, authoritative over this sketch). Settled while writing it:

- **`putIfUnchanged` takes the maximum age:** a value row older than it, not yet marked `EXPIRED`, counts as missing
  for reads, so "if no value" must count it as no value too; otherwise a load would loop (the read finds nothing,
  the conditional write conflicts).
- **"If no value" means missing, tombstone, expired, or a value older than the maximum age;** "if version" matches
  that exact version whatever the state. `NO_VALUE` is `0`, which no version can be.
- **`invalidate` writes a tombstone for a missing row too,** so the version reaches loads in flight elsewhere.
- **`invalidateAll` announces one `INVALIDATED_ALL`;** an adapter may additionally announce `INVALIDATED` per row
  (MongoDB's change streams do), and the core must accept both.
- **`StoreException.isOutcomeUnknown()`** carries the distinction the write-retry rules need to the core.
- **Maintenance methods return counts** (for stats and tests).

**The reference store** (`src/test/java/.../testing`, `InMemoryDatabase`): an in-memory database shared by several
stores as several instances share one store, with an injectable store clock and controllable faults: write failures
before or after commit, the store down (operations fail, undelivered messages lost, a gap when back), gaps, paused
delivery, delivery delays, duplicates, reordering across keys (per key and around `INVALIDATED_ALL` the order
stays), and `INVALIDATED` per row on `invalidateAll`.

**The contract tests** (abstract; an implementation provides a fixture opening several stores on one fresh cache),
split like the SPI: `StoreContractTest` (26 tests: the storage contract points and the semantics of every method,
asserting nothing about announcements) and `ChangeFeedContractTest` (15 tests: what each write announces, with
which version, delivery and gaps). Every store runs the first (also a storage-only wrapper of the reference store
and PostgreSQL without a listener), every store with a feed the second. Originally 30 tests in one class; the
reference store passes them, and 10 more tests cover its fault injection. 13 deliberate errors in the reference store were caught,
after one gap in the tests was closed (a young tombstone under "if no value"). The tests use short real sleeps for
ages (50–100 ms); if they flake under load, the reference store's injectable clock is the remedy.

| Core | Adapter |
|---|---|
| hashes, serialization, L1, version comparison, loads, refresh | storage per format, queries, conditional writes |
| when maintenance runs (interval with jitter) | how it runs, safe on several instances at once |
| the store guard (exceptions → degraded state), on reads too | throwing, reconnecting on its own |
| revalidating after `onGap()` | detecting gaps (Mongo: resume token lost; PostgreSQL: every reconnect) |
| | sharing listeners/watchers between caches, the discriminator |

Decided with it:

- **Batch methods only.** A single key is a collection of one; batches are needed for `getAll` and the delivery
  reads anyway.
- **Maintenance runs on every instance, idempotently**, no leader election (as today).
- **No `start`/`stop` in the SPI.** Subscribing starts, `close()` ends; the synchronization state follows.

The contract, written out on `Store` and `ChangeFeed` (authoritative; 2026-10-23, step 6 of the SPI rework), each
point checked by the contract tests named after it (`contractN_...`):

| Store | Point | Tests |
|---|---|---|
| 1 | Versions from the store, strictly increasing per hash (across instances, after a delete), returned to the writer | `contract1_*` |
| 2 | Ages and read times on the store's clock, the clock of the versions | `contract2_*` |
| 3 | Visibility and durability: a read on any instance reflects every write confirmed before it began; a confirmed write is lost only in an event every subscription sees as a loss or gap | `contract3_*` (visibility; durability per adapter: majority journaled on MongoDB) |
| 4 | Conditional writes (`putIfUnchanged`, `invalidateIfUnchanged`) atomic per key | `contract4_*` |
| 5 | Maintenance idempotent on several instances at once | `contract5_*` |
| 6 | Keys and values byte for byte; only the store's own cache, also in a shared table or collection | `contract6_*` |
| 7 | Every method as documented, for every state of a row | `contract7_*` |
| 8 | Failures only as classified `StoreException`, a partly applied write unknown, no retries in the store | each adapter's own (`PostgresRetryTest`, the timeout tests), and `InternalRetryTest` for the core's side |
| 9 | The `StoreSettings` applied: no operation waits longer than the timeout | each adapter's silent-connection tests |

| ChangeFeed | Point | Tests |
|---|---|---|
| 1 | A message exactly for a committed write; deletes announce nothing | `contract1_*` |
| 2 | The version as returned to the writer | `contract2_*` |
| 3 | At least once, per key in commit order; otherwise a gap | `contract3_*` |
| 4 | Everything committed after `subscribe` returned, nothing after closing | `contract4_*` |
| 5 | A loss reported, restored only by `resume()`, from then on (the core counts every resume as a gap) | `contract5_*` |
| 6 | A read after a message reflects the announced write | `contract6_*` |
| 7 | Only the own cache's writes, also in a shared table or collection | `contract7_*` |

Every store runs the store contract (also a storage-only wrapper of the reference store and PostgreSQL without a
listener), every store with a feed the feed contract; the store fixtures open a second cache in the same table or
collection (another discriminator) for points 6 and 7. Store point 3 and feed point 6 are what MongoDB needed
causally consistent sessions for; feed point 7 is what a shared change stream or listening session must filter for.

Splitting the tests found one violation: MongoDB announced a tombstone or expired row given a new version by
`invalidateAll` as `WRITTEN` (an update leaving the state as it was names no state in its change event, and no state
was read as a value). The change stream now flags, before projecting the value out, whether an update wrote a
value; one that did not is `INVALIDATED`.

Gone compared with the current generation: Publisher and Repository as separate roles, `CacheEntry` with its
statuses, `PartialCacheEntry` and projections, operation and activation ids in the SPI, read-back and handover
protocols, status queries by group, eviction publishing and the async publish drain.

### Java baseline: 17

Java 17, as in the current generation. None of the dependencies (Caffeine, Jackson 3, the MongoDB and PostgreSQL
drivers, r2dbc-postgresql, Fory, hash4j) require more, and everyone on the current generation can move to this one
without upgrading Java.

- **What 17 already has:** records, sealed interfaces, `switch` expressions, `instanceof` patterns, and exhaustive
  `switch` over enums. Wherever completeness matters (message kinds, row states, formats, conflict results), use
  enums so the compiler checks every case.
- **What is given up:** `switch` over sealed types and record patterns (21). Convenient, not needed by the design.
- **Virtual threads stay the user's choice:** the executor for store I/O (delivery reads, background work)
  is configurable, so users on 21 or later can pass `Executors.newVirtualThreadPerTaskExecutor()`; the library
  does not depend on them.
- **Re-check before the release:** raise the baseline only if a dependency or a needed feature requires it.

### Migration from the current generation

**Data: no conversion, start fresh.** The store is a cache, not the system of record, so discarding its rows costs one
load from the source per key. A conversion would also be imprecise: today's timestamp is the publish or eviction time,
not the write time, and evicted rows have no counterpart. The cost is a cold start. An import tool (cached rows from
the old table, written with write time = now) only if someone needs it.

**Names: given by the user, as today; a new name recommended for the switch.**

- Names stay mandatory adapter parameters, as today (PostgreSQL schema and table, MongoDB database and collection;
  only the discriminator has a default), so there are no default names. Recommended for the switch: a new table or
  collection name, so both generations run side by side in the same database and the old table is dropped after
  the switch. The same name works too, through the schema marker below.
- Its tables carry a **schema marker**: a table comment on PostgreSQL (`COMMENT ON TABLE ... 'distributed-caffeine
  schema 1'`), a metadata document on MongoDB (a string `_id`, which never equals the `{d, h}` subdocuments of rows,
  filtered out of reads and change streams). The marker also serves later layout changes within this generation.
- On start: nothing there → create table and marker; matching marker → use it; table present without a matching
  marker → **refuse to start** with a clear error (e.g. "Table `X` has the layout of distributed-caffeine 0.x. Stop
  all old instances, then drop it or configure another name, or enable `withSchemaReset()`.").
- **Opt-in `withSchemaReset()`** drops and recreates the table when no matching marker is found. Concurrent starts
  are serialized with an advisory lock on PostgreSQL; on MongoDB the drop is idempotent and creating the collection
  plus inserting the marker is safe to race. Only when every old instance of every cache in that table is stopped.
- Why not delete automatically: old instances still running on the table fail (errors on PostgreSQL, an
  `invalidate` event ending their change streams on MongoDB) and may recreate it, so the generations would drop
  each other's table in turns; caches sharing the table via the discriminator would be hit too; concurrent new
  instances would race; a foreign table of the same name could be deleted; and the application's user often has no
  `DROP` permission.

**Rollout: the generations do not see each other.** Different tables and messages mean an invalidation in one
generation does not reach the other. Either stop all old instances first (short downtime, never inconsistent), or
roll out without downtime and accept outdated values during the rollout, bounded by Caffeine's expiry in the old
instances. A bridge between the generations is out of scope: it would bring the old protocol back.

**Configuration: a migration guide.**

| Current generation | Next generation |
|---|---|
| `POPULATION_AND_INVALIDATION_AND_EVICTION`, `POPULATION_AND_INVALIDATION` | `DistributionMode.UPDATE` |
| `INVALIDATION`, `INVALIDATION_AND_EVICTION` | `DistributionMode.INVALIDATE` (default) |
| `NONE` | `DistributionMode.NONE` |
| eviction distribution | gone, eviction stays local |
| cached tier `withMaximumSize` / `withMaximumTime` | `withStore(s -> s.withMaximumSize(...).withMaximumAge(...))`, counted from the write |
| evicted tier, loading strategies | gone, L2 read-through is always on |
| restoration / `withColdStart()` | gone: L1 starts empty, read-through fills it (one L2 read per key on first access) |
| serializers on the adapter | serializers on the cache |
| `getFromStore` / `getAllFromStore` (returning `CacheEntry`) | `getFromStore` / `getAllFromStore` returning `StoredValue<V>` (value and age) |
| `withCaffeine(Caffeine.newBuilder()...)` | `withCaffeine(caffeine -> caffeine...)` with the same method names |
| `startSynchronization()` / `stopSynchronization()` | gone; `close()` (`AutoCloseable`) and build anew |
| `DistributedPolicy.getAdapter()` | gone |
| `withListenerSharingMode` / `withWatcherSharingMode` | gone: always shared per database |
| PostgreSQL `withListenerDataSource(...)` | an R2DBC `ConnectionFactory` for listening (or JDBC URL, user and password) |
| `weakKeys()` / `weakValues()` / `softValues()` (rejected today too) | rejected |

Unchanged, so user code carries over: `Hashable`, `HashProvider` and the hash computation, the serializer
implementations (Fory stays the default), the names (schema and table, database and collection, discriminator) and
the MongoDB client and PostgreSQL `DataSource` given to the adapters.

Tied to the naming question: with the same packages and coordinates, both generations cannot be on one classpath.
That does not matter when a service switches at once, only when one application moves cache by cache.

### Async variants from the beginning

`AsyncCache` and `AsyncLoadingCache` are supported in the first version, because they shape decisions that would
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

| N | as built, platform pool: all done / peak threads | as built, virtual threads | non-blocking load, platform pool |
|---|---|---|---|
| 100 | 107 ms / 102–103 | 108–109 ms / 1 | 111 ms / 48–72 |
| 1,000 | 167–172 ms / 1,002–1,003 | 117–124 ms / 1 | 121–126 ms / 110–157 |
| 5,000 | 777–781 ms / 1,651–1,652 | 158–194 ms / 1–2 | 156–157 ms / 226–257 |
| 10,000 | 1,369–1,462 ms / 1,750–1,865 | 179–187 ms / 1 | 196–207 ms / 273–327 |

- **With platform threads the cost is real:** one store-executor thread waits per async miss in flight, so a burst
  of 1,000 holds about 1,000 threads, and from 5,000 on, creating them dominates (5–7 times slower than either
  alternative).
- **Virtual threads remove it completely** (as fast as the non-blocking load, no platform threads), on Java 21 or
  later.
- **A non-blocking load** (store read on the pool, the loader's future, the write on the pool) also removes it, on
  Java 17 with platform threads; what remains are the threads for the blocking store calls themselves.
- Hits and the synchronous caches are not affected either way.
- **Decided (2026-10-21): the non-blocking load is built (Java 17); virtual threads as a default stay under
  "Options for later".**

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

| N | before: all done / peak threads | non-blocking load, as built | modelled non-blocking load |
|---|---|---|---|
| 1,000 | 167–172 ms / 1,002–1,003 | 117–131 ms / 146–238 | 129–130 ms / 178–236 |
| 5,000 | 777–781 ms / 1,651–1,652 | 157–160 ms / 365–451 | 155–158 ms / 215–232 |
| 10,000 | 1,369–1,462 ms / 1,750–1,865 | 229–242 ms / 703–834 | 195–206 ms / 267–328 |

The remaining threads serve the blocking store calls (the SPI stays blocking); the core's own message processing
runs on the same executor. Tests assert that pending loader futures hold no store-executor thread (single, bulk,
reload); a mutant waiting for the future instead fails them.

### `asMap()` compute methods read through L2

With "L1 only" (today), `asMap().computeIfAbsent(k, f)` would behave differently from `get(k, f)`, which Caffeine
treats as the same operation, and `compute`, `merge`, `computeIfPresent` would compute against a missing L1 value
while L2 holds one, then overwrite it (a `merge` counter would lose updates).

- **`computeIfAbsent`** behaves exactly like `get(k, f)`: L1 → L2 → function, the result written conditionally
  (ordering rule 6).
- **`compute`, `computeIfPresent`, `merge`** read the current value through L1 and L2, write conditionally on the
  version read, and on a conflict read again and retry. This makes them **atomic across instances** (optimistic
  concurrency): two instances merging into the same counter do not lose an update.
- **Store I/O runs outside Caffeine's lock** ("loads outside the lock"); atomicity comes from the conditional write
  in L2.
- **Documented deviation from Caffeine:** Caffeine applies the function at most once per call. After a conflict it
  can run again (as with `ConcurrentMap`'s default implementations), so the function must be free of side
  effects. Throwing on a conflict instead was rejected as awkward to use.

### Inspection API

`distributedPolicy().getFromStore(key)` and `getAllFromStore(keys)` read L2 directly, without filling L1: for
debugging, for batch jobs that should not pollute L1, and to see how old a value is.

- They return `StoredValue<V>`: the value and its age.
- They return what a miss would get: only `VALUE` rows younger than the maximum age.
- No state, version or tombstones: those are internals and would become part of the contract.
- Costs no new SPI method: `Store.read` plus deserialization.

### Wrappers in L1: `InternalKey` and `InternalValue` stay

L1 is a Caffeine `AsyncCache` of `InternalKey<K>` to `InternalValue<V>` (wrappers as in the current generation), so
metadata travels with keys and values without changing Caffeine's types.

- **`InternalKey<K>` caches the key's hash**, computed at most once per wrapper or taken over from a row. The hash
  is needed for every L2 call and every message match.
- **`InternalValue<V>` carries the version** (replacing today's `operation` and `activationId`), which every
  comparison under the ordering rules uses, and the epoch at which it was last confirmed (see "States and
  recovery").

### How a message finds the held key: `InternalKey` compares by hash

Messages carry only the hash, so L1 must be searchable by it. `InternalKey` compares by its hash instead of by
key: a message builds a lookup key from the hash alone and finds the held entry directly in Caffeine, and the
registry of minimum versions for loads in flight is keyed the same way. Whatever Caffeine holds is exactly what
messages find, including during evictions, expirations and loads in flight.

- **Identity is conceptually the hash string, as in the current generation; `InternalKey` holds it compactly**
  (decided after the benchmark below):
  - A hash of 32 hex characters (built-in key types, and custom ones using the `Hasher`) is held as 128 bits (two
    `long`s). Built-in key types (`String`, `Long`, `Integer`, `UUID`) go through the same `Hasher` path but read the
    bits directly (package-private), so no string is created on the hit path; the result is bit-for-bit the hash
    `getHash()` would give. Custom keys return their string as today and the core reads it into two `long`s
    without allocating (`HexFormat.fromHexDigitsToLong` per half).
  - Any other string a custom `Hashable` / `HashProvider` returns (e.g. `"order-42"`) is kept and compared as the
    string, exactly as today; that cost stays with those keys.
  - Which representation a key uses follows from the hash string alone, so every instance decides it the same way,
    and a 32-hex string never equals any other string, so the two never need to be compared with each other.
  - The hex string is produced only for the store and messages, and cached in the wrapper. Row ids and messages
    carry it as today; a message lookup picks the representation the same way.
- **The user API does not change:** `Hashable` and `HashProvider` keep returning a `String`; every implementation
  that works today keeps working, with the same store id, including strings that are not hex.
- **Documented contract:** keys that are `equals` must produce the same hash (already required in practice).
- **Cost:** hashing on every user call, since a new wrapper has no cached hash: 2–4 times a plain hit and 32 bytes
  per call (measured below), against 5–7 times and about 500 bytes for the hash string.
- **Possible refinements, only if needed:** avoiding the `HashValue128` allocation, or a faster hash for built-in
  key types, which would have to give the same bits as today's stream so that stored rows stay compatible.

Measured (`HitPathBenchmarkTest`, JMH 1.37, JDK 25, `getIfPresent` on a filled cache of 10,000 keys, keys like
`user:000123:profile` or random `Long`s, one fork, 3 × 1 s warm-up, 5 × 1 s measurement, average time per call
and thread):

| Variant | `String` key, 1 / 4 threads | `Long` key, 1 / 4 threads | Allocated per call |
|---|---|---|---|
| plain Caffeine | 21.1 / 17.3 ns | 17.0 / 15.0 ns | 0 |
| wrapper compared by key (today) | 18.1 / 14.3 ns | 18.1 / 12.8 ns | 0 (escape analysis) |
| by hash string, as today's `Hasher.getHash()` | 117.9 / 102.6 ns | 85.0 / 76.3 ns | 496 B |
| by hash string, reused stream, hex from the two longs | 186.7 / 168.4 ns | 149.2 / 130.4 ns | 168 B |
| by the two longs (compact form) | 65.9 / 62.5 ns | 45.5 / 29.0 ns | 32 B |

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

### Lifecycle: build, run, close

**Start: `build()` connects synchronously and fails fast.**

- Within a timeout (30 s by default, as today) `build()` checks the connection, the table's schema marker and the
  subscription, and returns a cache in `SYNCHRONIZED` state only when all of it is in place.
- Configuration errors always throw: a missing driver, wrong credentials, a schema that does not match, no DDL
  rights without an existing table.
- An unreachable store throws too. An application that must start without its store retries `build()` itself;
  an option to start `DEGRADED` and connect in the background was dropped as not worth a second start path.
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

**Ownership:** the user owns what they pass in (`MongoClient`, `DataSource`, R2DBC `ConnectionFactory`, executor)
and closes it; the library owns only what it opens (listening sessions, cursors, threads) and closes it with the
last cache using it.

### Adapter ownership: one adapter per cache

- **An adapter describes where one cache lives in the store:** the user's client (`MongoClient`, or `DataSource`
  plus R2DBC `ConnectionFactory`), database, table or collection, and discriminator. At `build()` it
  creates the cache's internal `Store` (the SPI, invisible to users).
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

| | `SYNCHRONIZED` | `DEGRADED`, store down | `DEGRADED`, subscription down | `CLOSED` |
|---|---|---|---|---|
| L1 hits | served (confirmed) | served unconfirmed | each access checks the version first | throws |
| Misses | L1 → L2 → loader | loader only, held in L1, not written to L2 | L2 → loader | throws |
| Writes | normal | refused (`invalidate` drops the local copy, then throws) | normal | throws |
| Back to normal | – | guard closes, reconnect, revalidation | reconnect, revalidation | – |

- **Store down** (the store guard is open; the subscription is usually down too): nobody can write, so held values
  rarely go stale; in a partial partition, where others still reach the store, they can, until recovery (accepted,
  as today). A miss goes to the loader at once, because the guard refuses store calls without waiting for timeouts;
  the result is held in L1 and confirmed or dropped by revalidation later. Writes are refused: a put kept only
  locally would be lost, and a lost invalidation would leave others serving the old value without any error. A held
  value awaiting its check still tries the store on every access: the guard refuses at once or lets it through as
  its probe, so the first access after the store is back checks it (found by a PostgreSQL failover that loses a
  confirmed write: otherwise the cache served the lost value until the next maintenance run).
- **Only the subscription down, the store answering:** every access checks the held version against the store
  (version only, no value transferred), because a copy confirmed now can go stale the next moment while no message
  can arrive. Writes work normally; the store announces them to the others.
- **`CLOSED`:** after `close()` every cache operation throws, except `getSynchronizationState()`, so health checks
  can query a closed cache without catching exceptions.

**Revalidation instead of clearing L1** (after a short interruption almost everything held is still correct):

- Each instance has an epoch counter; every `InternalValue` records the epoch at which it was last confirmed. A held
  value is trusted only if its epoch is current and the subscription is up.
- On a gap the counter is incremented, which makes everything held unconfirmed without touching an entry.
- An unconfirmed value is checked when read: the store returns hash and version only, under the maximum-age filter.
  Same version → kept and confirmed, no value transferred; different, too old or missing → a miss (L2, then the
  loader, as usual).
- A background pass checks all held keys in batches (versions only, e.g. 1,000 per query, rate-limited so it does
  not load a store that has just recovered) and drops what does not match, so `asMap()`, iteration and
  `estimatedSize()` are clean again soon, not only once each key has been read. It covers what never goes through a
  read: views and iteration, sizes, quiet reads (`getIfPresentQuietly`, L1 only), the user's removal listener, and
  memory held by outdated entries. Before dropping, it compares the version again under `compute`, like every other
  path. Kept after reviewing it as possible over-engineering: without it, views would either show possibly outdated
  entries after a gap until read (a deviation visible only in rare, hard-to-understand moments) or hide unconfirmed
  entries (and look almost empty right after a gap); its cost arises only after gaps and reuses the read path's
  batched version reads.
- **Order:** subscribe again first, then check. Every change after the check arrives as a message and is handled by
  the version comparison; checking first could miss a change in between.
- After a crash that lost acknowledged writes, the lost versions held by peers no longer match the store and are
  dropped.
- A MongoDB resume within the token's reach is no gap, so the epoch stays; while the cursor is down, the per-access
  check applies anyway, since messages are delayed.
- Costs: one `int` per `InternalValue`; a version read on the first access per key after a gap, and on every access
  while the subscription is down; one query per 1,000 held keys per gap for the background pass.

**Store guard, as today (measured and tested):** opens after 3 consecutive failures; while open, store calls are
refused at once; after 1 s the next call is the probe; a successful call, or the maintenance worker's success,
closes it. It covers reads as well as writes.

### Heartbeat: a silent feed is noticed by the core (decided 2026-10-23)

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

### Supervision: the core resumes the feed (decided 2026-10-23)

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

### Settings: configured on the cache, handed to the store (decided 2026-10-23)

What the cache needs from every store is configured once on the cache, not per adapter, and handed to the adapter
when it creates the store: `createStore(StoreSettings(keyFormat, valueFormat, timeout))`. So far the formats (from
the serializers) and the operation timeout (`withStore(store -> store.withTimeout(...))`, 30 s by default; it was
`withTimeout` on each adapter's builder before). The adapter applies the timeout to every operation and to its
checks on creation: MongoDB as the client-side operation timeout, unless the client sets its own (`timeoutMS`),
PostgreSQL as the connection's network timeout per statement. What only one kind of store has (a discriminator, a
listener connection factory, the schema reset) stays on its adapter. Timings internal to an adapter (MongoDB's
watcher answering within 10 s, the PostgreSQL listener's connect and probe timeouts) are not settings: the core's
heartbeat and supervision bound what they would otherwise have to.

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
- **Cycles of synchronous loads across threads** (the load of a waits for b, whose load waits for a) are resolved,
  as the current generation does: the core knows which thread runs each synchronous load and which load each waiting
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
  `null` means absent, as in Caffeine (recorded in the absent set if absent caching is on).
- **Mapping functions of non-loading caches** (`get(key, function)`, `getAll(keys, function)`) are wrapped into the
  same pipeline, in Caffeine's `BiFunction<K, Executor, CompletableFuture<V>>` form.

Measured (`HitPathAsyncBenchmarkTest`, JMH 1.37, JDK 25, precomputed two-long keys so only the cache differs, filled
cache of 10,000 keys, average time per call, 1 / 4 threads; no allocation in any variant):

| Variant | `getIfPresent` | `get(key, function)` on a hit |
|---|---|---|
| plain `Cache` | 16.9 / 13.2 ns | 15.6 / 13.0 ns |
| `AsyncCache`, future then `join()` | 18.8 / 14.3 ns | 20.2 / 14.5 ns |
| `AsyncCache.synchronous()` | 23.6 / 17.8 ns | 22.1 / 15.1 ns |

The `AsyncCache` costs 1–5 ns per hit used directly (up to 7 ns through `synchronous()`), against 30–65 ns for
hashing the key; negligible.

### Reading and writing methods

**Principle:** the distributed cache contains what L2 holds. Methods about a single key read through L2; methods
about the whole local map (views, sizes, iteration) show this instance's L1.

**Reading through L2:**

| Method | Behaviour |
|---|---|
| `getIfPresent`, `getAllPresent`, `asMap().get`, `getOrDefault` | L1, then L2 (a hit is held in L1), never the loader |
| `asMap().containsKey` | L1 if held and trusted; otherwise an existence check (`readVersions`: a `VALUE` row younger than the maximum age, version only, no value transferred); nothing is held; an unconfirmed copy is confirmed or dropped by the same query; a store that does not answer counts as a miss, as for `getIfPresent` |

Today `getIfPresent` stays local because L1 holds everything; with L1 a subset it would be almost useless without
L2. Caffeine's "without loading" still holds: only the loader is skipped. JetCache's `get` without a loader goes
local, then remote, and fills the local level the same way (`MultiLevelCache.do_GET`); JetCache has no
`containsKey`, and Redisson's `RLocalCachedMap` and Hazelcast's Near Cache (from memory, not verified here) check
locally first and otherwise ask the shared level for existence only, without filling the local level.

**Conditional `asMap()` methods, atomic across instances** (like `merge`: read the current value through L1 and L2,
decide, write conditionally on the version read; on a conflict read again and decide again):

| Method | Condition | Effect |
|---|---|---|
| `putIfAbsent(k, v)` | L2 holds no value | written (`putIfUnchanged` with "missing" or on the version of a tombstone); otherwise the existing value is returned (not held: a raw read held without a load in flight would escape ordering rule 4) |
| `replace(k, v)` | L2 holds a value | written conditionally on its version |
| `replace(k, old, new)` | the current value `equals(old)` | written conditionally on its version |
| `remove(k, v)` | the current value `equals(v)` | tombstone conditionally on its version (`invalidateIfUnchanged`) |
| `computeIfAbsent`, `compute`, `computeIfPresent`, `merge` | see `asMap()` compute methods | |
| `asMap().put(k, v)`, `asMap().remove(k)` | none | written / tombstone conditionally on the version read, so the returned previous value is exact across instances; `put` always writes, also an equal value, so the row's age renews; `remove` always writes a tombstone, also where there is no value (conditionally on the tombstone's version, or on no row existing), because like Caffeine's `remove` it must defeat a load in flight: without it, a record deleted at the source and removed from the cache while a load was in flight was cached again from the load's older source read (found by the property test). `invalidateIfUnchanged` therefore matches a row of any state by its version, or `MISSING` |

A remapping that returns the current value instance unchanged writes nothing; if that decision was made on a held
value, its version is first checked against the store (`readVersions`), and on a mismatch the decision is made again
on the store's row. Without that check a decision that writes nothing never meets the conflict that corrects an
outdated held value, so `replace(k, old, new)` answered `false` and `putIfAbsent` returned a value invalidated
elsewhere while the messages were on their way (found by an intermittent test failure). The cost is that of a read and a
conditional write, the same as reading the previous value and writing unconditionally; a trusted held value saves
the read. Built: deciding on an outdated held value (its message not yet arrived) made a non-atomic `put` return
the outdated previous value, which the conditional write corrects through its conflict.

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

| Method | Behaviour |
|---|---|
| `VarExpiration.put` / `putIfAbsent` / `compute` with a duration | written like `put` / `putIfAbsent` / `compute`; the duration applies to this instance's L1 copy only |
| `eviction().setMaximum`, `setExpiresAfter`, `setRefreshesAfter`, `VarExpiration.setExpiresAfter(k, d)` | local, as in Caffeine (L1 configuration) |
| `getIfPresentQuietly`, `getEntryIfPresentQuietly` | L1 only: "quietly" means without side effects, so no L2 read |
| `refreshes()` | L1's refreshes in flight, unwrapped |
| `refreshAfterWrite().setRefreshesAfter(d)` | local; refresh rule 2 follows the new duration |
| `coldest`, `hottest`, `oldest`, `youngest`, `getEntryIfPresentQuietly` | L1 snapshots, unwrapped and immutable |

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
(the configurer pattern of the current generation, which keeps the top-level builder small and makes migration a
change of the call around unchanged method names), instead of taking the user's Caffeine builder and rewriting its
private fields by reflection as today. Built: `CaffeineConfigurer` passes every setting at once to a real Caffeine
builder of `InternalKey` to `InternalValue`, so Caffeine's own validation applies unchanged (a setting made twice,
`maximumSize` with `maximumWeight`, ...), and wraps callbacks at the call.

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
- **The store's maximum age and maximum size are both required**
  (`withStore(store -> store.withMaximumAge(...).withMaximumSize(...))`), the maintenance interval optional: without
  a maximum age the store would serve arbitrarily old values, without a maximum size it would grow without bound,
  and implicit defaults would hide either bound.

| Setting | Handling |
|---|---|
| `maximumSize`, `maximumWeight`, `initialCapacity`, `expireAfterWrite` / `expireAfterAccess`, `refreshAfterWrite` | passed through to L1 |
| `executor`, `scheduler`, `ticker` | passed through |
| `weigher` | wrapped: unwraps `InternalKey` / `InternalValue` before calling the user's weigher |
| `expireAfter(Expiry)` | wrapped (as today's `InternalExpiry`) |
| `removalListener`, `evictionListener` | wrapped, with the causes below |
| `recordStats(...)` | passed through; the distribution stats come on top |
| `weakKeys`, `weakValues`, `softValues` | rejected with a clear error |
| `build(...)` / `buildAsync(...)` | always `buildAsync` internally, with the internal `AsyncCacheLoader` |

**Why weak and soft references are rejected** (invariant 1 alone would allow them): `weakKeys()` compares keys by
identity (`==`), which cannot hold across instances where keys are identified by hash; `weakValues()` /
`softValues()` would reference the `InternalValue` wrapper, which only L1 holds, so with weak values the collector
would take it at the next GC however long the user keeps the value. Supporting them would need a wrapper holding
the value weakly itself, and Caffeine's `COLLECTED` cause would be lost.

**Removal causes for the user's listeners:**

| Event | Cause |
|---|---|
| own `invalidate`, or a received `INVALIDATED` | `EXPLICIT` (invalidated, here or elsewhere) |
| received `EXPIRED` (maximum age) | `EXPIRED` |
| population replaces a held value | `REPLACED` |
| revalidation drops an outdated copy | `EXPLICIT` |
| a load or refresh result discarded by version | `REPLACED` (as Caffeine does for discarded refreshes) |
| L1's own eviction and expiry | as Caffeine reports them (`SIZE`, `EXPIRED`) |

### Adapter setup

**Layout and indexes.**

- **MongoDB: the discriminator is part of `_id`, as a subdocument `{d: discriminator, h: hash}`.** With several caches
  in one collection, an `update` event must be dispatched to its cache, but unchanged fields are left out of
  `updatedFields` and `documentKey` holds only `_id`; today only `UPDATE_LOOKUP` supplies the discriminator, and this
  design drops it. With the discriminator in `_id` every event's `documentKey` carries it, and upserts by `_id` are
  atomic, so the duplicate-key retry disappears. Point reads filter on `_id` (always built in the same field order);
  one index `{"_id.d": 1, state: 1, writeTime: 1}` serves expiry, size pruning and tombstone cleanup. The reasons this
  was rejected in the current generation (memory note `upsert-duplicate-key-retry-cost`) no longer apply: no legacy
  `ObjectId` documents (fresh tables), no separator ambiguity (a subdocument, not `"hash@discriminator"`); a slightly
  larger `_id` index remains, accepted.
- **PostgreSQL:** primary key `(discriminator, id)` and one index `(discriminator, state, write_time)` for the same
  three queries; no partial indexes.

**Setup without DDL rights.** The documentation carries the complete DDL (table, indexes and the schema marker
`COMMENT ON TABLE ...` for PostgreSQL; indexes and the marker document for MongoDB) for setups where a migration
tool or a DBA creates the schema. On start the library checks the marker and the table's shape (columns and types):
matching → used; missing and not creatable → refuse with an error pointing to the DDL. Missing indexes only warn,
since they cost speed, not correctness. This replaces today's "create, or fall back to an existing table when
`CREATE` is refused".

**Replicas: reads from the primary.**

- PostgreSQL refuses standbys: `pg_is_in_recovery()` once per physical connection, as today (a standby lags, so its
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
("the listener connection receives no notifications (transaction-mode pooler?); point the R2DBC `ConnectionFactory`
directly at the database or at a session-mode pooler"). A silently dead connection is found by the core's heartbeat
(it was a periodic self-notification of the listener until the core supervised the feed).

**MongoDB: sharded collections stay refused**, as today; a sharded cluster through `mongos` with an unsharded
collection stays allowed. The composite `_id` could become the shard key later; out of scope for now.

**Names:** mandatory, as today (PostgreSQL schema and table, MongoDB database and collection; the discriminator
optional with today's default, and not blank). A default name would hide where the data lives.

- **User-given PostgreSQL names** (schema, table) over 63 bytes are rejected with an error: PostgreSQL would
  truncate them silently, and shortening them with a hash would leave the user unable to find the table under the
  name they gave. They are quoted, so case is kept as given.
- **Derived PostgreSQL names** (indexes, notification channels) are shortened with a hash when over 63 bytes, as
  today; channel names derive from schema, table and discriminator.
- **MongoDB database and collection names** are checked against MongoDB's limits (namespace length, forbidden
  characters), with a clear error at `build()`.

### Write retries

A retry is safe when the outcome is known (certainly not committed) or does not matter (the write is conditional);
it is unsafe for an unconditional put whose outcome is unknown. Today PostgreSQL retries deadlocked or aborted
writes assuming every write is idempotent; with versions that assumption no longer holds.

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

| Write | Certainly not committed | Outcome unknown |
|---|---|---|
| `putIfUnchanged` (loads, refresh, `compute`, `putIfAbsent`, ...) | retry | retry: if the first attempt landed, the retry sees a changed version, a conflict, and reads the row (the "newer" row is then its own) |
| `invalidateIfUnchanged` (`remove(k, v)`) | retry | retry, for the same reason |
| `invalidate` | retry | retry: at worst it removes a value written in between, which costs that key one load from the source but never leaves an outdated value |
| `put` (unconditional) | retry | **no retry:** if the first attempt landed and another instance wrote in between, the retry would overwrite their newer value with the older one. The key is dropped from L1 (ordering rule 5), and the call throws, since the caller cannot know whether the write happened |
| expiry, pruning, `invalidateAll` | retry | retry: all are conditional (`state = VALUE`, ...) and idempotent, and maintenance runs again anyway |

**MongoDB: the driver retries.** Single-document writes are retryable writes: the driver retries them and the server
deduplicates by the session's transaction number, so a retry never applies twice, which removes the unknown case
even for unconditional puts. With CSOT, every read iterates its cursor with the timeout per batch
(`TimeoutMode.ITERATION`), not for the whole read, which a large pruning would exceed. Without a client timeout,
every store operation gets the cache's (`withStore(store -> store.withTimeout(...))`, 30 s by default), since the driver waits for good on a
silent network otherwise. Nothing that timed out is retried (it waited its whole time already: for a server, a
pooled connection, or the answer); a write that timed out has an unknown outcome, because the driver's timeout
does not tell waiting for a server from waiting for the answer. The README recommends CSOT
(`timeoutMS`), since without it the driver retries only
once, and that retry can still land inside a failover (the current generation's lesson). A CSOT timeout may strike
after sending, so it is an unknown outcome. Multi-document updates
(`invalidateAll`, expiry, pruning) are not retryable writes; the core repeats those, safely, since they are
conditional. The adapter classifies what the driver labels transient as retryable, a write broken after sending as
unknown, and a timeout as neither.

**PostgreSQL: classified by SQLState:** a rejected statement (`40P01`, `40001`, `55P03`) or a connection that could
not be obtained is retryable and certainly not applied; a connection lost during a statement is retryable with a
write's outcome unknown; a statement without an answer within the timeout, a pool that waited its whole timeout for a
connection, and every other error are not retryable. A missing table is created again and reported retryable.

**Open point:** a conditional write whose outcome was unknown and that the core repeats cannot tell its own row from
another writer's: `putIfAbsent` then reports the value as present already (it is stored and held; the return value
is the only difference). Telling them apart needs an id of the write in the row, a later decision.

### Operations

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
  not count log lines (the current generation's lesson); they check states and stats.

**Stats:** `stats()` stays Caffeine's. `distributedPolicy().stats()` returns a `DistributedStats` value (`plus` /
`minus` as today), gated by `recordStats()`, with counters re-derived for this model; no Micrometer binder (decided
in the current generation; the value can be polled and exported).

| Area | Counters |
|---|---|
| L2 reads | hits, misses, rejected by maximum age, read time |
| Source | loads (distinct from Caffeine's load count, which includes L2 hits), reloads |
| Writes | puts, conditional writes, conflicts, refused (store down), unknown outcome, write time |
| Messages | received by kind, applied, ignored (version not newer), population reads |
| Removals by others | invalidated, expired, replaced |
| Recovery | gaps, reconnects, revalidated (checked, kept, dropped), store guard openings |
| Data | unreadable rows, absent hits |

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
- **Messages:** received by kind (`written`, `invalidated`, `expired`, `invalidatedAll`), and what they did to a
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

| Timing | Default | Configurable |
|---|---|---|
| Maintenance interval (expiry, pruning) | 1 min ±20 % jitter | yes: part of the maximum-age bound |
| Tombstone retention | 5 min | no |
| `build()` timeout | 30 s | yes |
| Store guard | opens after 3 failures, probe after 1 s | no |
| Feed heartbeat (reserved-key invalidate, core) | every 10 s ±20 % jitter | yes: bounds how long a silent feed goes unnoticed |
| Resume after a loss (core) | at once, then from 250 ms, doubling, at most 30 s, ±20 % jitter | no |
| MongoDB shared watcher `maxAwaitTime` | 100 ms | no |

**README for operations,** carried forward and updated: CSOT (`timeoutMS`) for MongoDB, socket timeouts, DNS TTL,
managed services, `writeConcernMajorityJournalDefault` (graded), R2DBC and poolers, the cost of `NOTIFY` per
listening session, the store as a trust boundary, and the DDL for migration tools.

### Absent caching: a local set of recent misses (opt-in)

For keys that do not exist but are requested repeatedly (unknown IDs, optional per-user data, slow "not found"
answers, deliberate cache penetration with random keys). Off by default, so the default behaves exactly like
Caffeine, which never caches `null`; switched on with `withAbsentCaching(Duration)`.

- **A separate, purely local set per cache:** a small Caffeine cache of hashes (two `long`s) with its own short
  `expireAfterWrite` and a size limit. Not in L1, not in L2, not distributed.
- **Filled when a load finds nothing:** neither L2 nor the loader (or mapping function) has a value, the loader
  returning `null`. An exception records nothing.
- **Checked before every L2 read:** in the internal loader, in `getIfPresent` and in `containsKey`; a hit answers
  `null` (or `false`) at once, without an L2 read and without the loader.
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
- **Stats:** a hit in the set counts as `absent hits` in the distribution stats; Caffeine's `stats()` sees a load
  returning `null`, as Caffeine counts it.

Rejected alternatives: an absent marker in L1 plus an `ABSENT` row state in L2 (JetCache's `cacheNullValue`): a new
row state, a second maximum age in the read filter, markers hidden in the views, sizes counting them, listeners and
weigher special-cased, all as a feature Caffeine deliberately does not offer. Caching `Optional<V>` as the value (the
pattern Caffeine recommends) works with the library unchanged and stays documented, but absences then share the
store's maximum age.

### Coordinates, packaging and version

- **One artifact**, as today: `io.github.oberhoff.distributed-caffeine:distributed-caffeine`.
- **The MongoDB driver, the PostgreSQL driver, r2dbc-postgresql (the PostgreSQL listener) and Jackson are
  `optional` dependencies.** Users add what they use (usually they have the driver already); the driver version is
  always the user's own. Minimum supported versions are documented and covered by the test matrix. Today the
  drivers, Fory and Jackson are compile dependencies, so every user gets both drivers and both serialization
  libraries.
- **Fory is a regular dependency** (about 2.7 MB plus Janino), because it is the default serializer that lets a cache
  run with no configuration (see Serialization).
- **A clear error when a driver is missing:** an adapter checks on construction that its driver is present and
  names the missing dependency (e.g. "MongoAdapter requires org.mongodb:mongodb-driver-sync 5.x on the
  classpath"), instead of a `NoClassDefFoundError` later. Same for the Jackson serializer.
- **Packages:** the base package stays `io.github.oberhoff.distributedcaffeine`; the SPI gets its own package, the
  adapters live in `...mongodb` and `...postgresql`.
- **The boundary is enforced by a test** instead of separate build modules: an architecture test (ArchUnit or a
  plain check over the class files) fails if an adapter package uses anything but the SPI package.
- **Version: the next minor of the 0.x line** (e.g. 0.8.0), not 1.0. The 0.x line allows breaking changes; the
  current generation ends with 0.7.x and gets maintenance fixes only.
- **At the switch:** the current code moves to a branch for 0.7.x maintenance, and the content of this module
  becomes the repository root. Until then this module keeps its working coordinates
  (`distributed-caffeine-next`, not released) and stays a single module.
- **Accepted:** both generations cannot be on one classpath (same coordinates and packages); migration happens per
  service anyway, and the compile errors after the upgrade show what to change.
- **Splitting later stays possible** without breaking anything (e.g. with many adapters or adapters on their own
  release cadence): the core artifact keeps its name.

### MongoDB write concern: majority, journaled

The library writes with majority and `j: true`, always (since 2026-10-23, see ordering rule 7: the store contract
requires durable confirmed writes). There is no w:1 option. Until then it left journaling to the server's
`writeConcernMajorityJournalDefault`, and the README was to recommend `false` for faster writes and propagation; the
measurements below are kept for that history and for a possible opt-in later.

Measured (`MongoWriteConcernExperimentTest`, MongoDB 5.0.31, 6.0.28, 7.0.45, 8.3.13, 9.0.4, single-node replica set,
1,000 sequential writes to one hot key per variant, each variant twice; 8.3 shown, the others alike):

| Variant | Writer's ack p50 / p99 | Event at a watcher p50 / p99 |
|---|---|---|
| majority, journal default `true` (MongoDB's default) | 546 / 1,641 µs | 590 / 1,687 µs |
| w:1, journal default `true` | 168 / 332 µs | 52,174 / 103,886 µs |
| majority, journal default `false` | 196 / 309 µs | 244 / 404 µs |
| w:1, journal default `false` | 198 / 300 µs | 246 / 377 µs |

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

| Journal default | Round | Last acknowledged | Stored after restart | Acknowledged lost | Events delivered for lost writes | Change stream afterwards |
|---|---|---|---|---|---|---|
| `false` | 1 | 1,052 | 832 | 220 | 220 | error 280 `ChangeStreamFatalError` ("resume token was not found"), resuming from the last token fails too |
| `false` | 2 | 1,071 | 1,070 | 1 | 0 | resumes silently, next event arrives |
| `false` | 3 | 1,051 | 659 | 392 | 391 | error 280, as in round 1 |
| `true` | 1–3 | 1,018 / 1,018 / 1,021 | same | 0 | 0 | resumes silently |

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

| Variant | Writer's ack p50 / p99 | Event at a watcher p50 / p99 |
|---|---|---|
| majority, journal default `true` | 725 / 1,119 µs | 801 / 1,260 µs |
| w:1, journal default `true` | 361 / 569 µs | 1,342 / 2,590 µs |
| majority, journal default `false` | 545 / 867 µs | 637 / 930 µs |
| w:1, journal default `false` | 377 / 597 µs | 830 / 1,855 µs |

- **w:1 still delivers events later than majority** (about 1.7 times), though no longer 100 times: the secondaries'
  replication moves the majority point along without a periodic journal flush. The decision against a w:1 option
  stands.
- **Journal default `false` gains much less with three members:** about 25 % on the writer's ack and 20 % on the
  event, against about 2.8 and 2.4 times on a single node. With replication in the path, the journal is no longer
  the main cost. Across a real network, replication time grows and fast disks make the journal cheaper, so the
  gain there is likely smaller still; Docker Desktop's slow fsync inflates it here.
- **So the README recommendation is graded:** worth it mainly for single-node deployments; for replica sets a
  moderate gain that has to be weighed against losing acknowledged writes when a majority crashes.

### PostgreSQL: statements and propagation measured

**Statements** (`PostgresStatementExperimentTest`, PostgreSQL 13, 14, 15, 16, 17, 18; each statement writes and
notifies in one statement, `pg_notify` over the rows a CTE returns): all checks pass on every version:

- 1,000 upserts: versions strictly increasing, each at least the server time in microseconds before the write
  (`greatest(r.version + 1, extract(epoch from clock_timestamp()) * 1000000)`).
- No notification before commit, one after commit, none after a rollback.
- 8 concurrent conditional writes with the same expected version (`ON CONFLICT ... DO UPDATE ... WHERE r.version =
  ?`): exactly one wins, exactly one notification; the same for 8 concurrent creations of a missing row
  (`ON CONFLICT DO NOTHING`).
- Invalidating 5 rows in one statement: 5 `INVALIDATED` notifications.
- Two concurrent expiries (`WHERE state = 'VALUE' AND write_time < now() - max_age`): every row expired and notified
  exactly once (the second update re-checks the condition after the first one's row lock).
- The read filters by maximum age and returns the age.

**Propagation** (`PostgresPopulationExperimentTest`, PostgreSQL 18, one writer on one hot key, N holders each with its
own `LISTEN` session and read connection, a holder reads the row once per batch of notifications, 1 KB values, each
variant twice):

| Variant | Writer's ack p50 | Applied p50 / p99 | Reads per write |
|---|---|---|---|
| writer alone, without / with `pg_notify` | 145–154 / 143–148 µs | – | – |
| 1 / 4 / 16 holders, 500 writes/s | 674–683 / 739–793 / 451–459 µs | 2.5 / 2.7–2.9 / 1.9 ms, p99 4.6–6.0 ms | 0.97 / 3.7 / 14.8 |
| 1 / 4 / 16 holders, 1,500 writes/s | 264–289 / 301–335 / 407–408 µs | 26–32 / 25–170 / 336–338 ms, p99 up to 665 ms | 0.01–0.05 |

- **The notification costs the writer nothing measurable.**
- **Below about 750 writes/s holders have the value within about 2–3 ms** (p99 about 5–6 ms), somewhat slower than
  MongoDB's 1–2 ms.
- **Above it, propagation collapses to hundreds of milliseconds.** This is the known pgjdbc limit (memory note
  `pgjdbc-single-session-notification-cliff`): `getNotifications` returns only after a 1 ms gap in incoming
  notifications, so one session cannot hand them out promptly above about 750/s, and everything received meanwhile
  waits in the driver.
- **Why it matters more in this model:** every instance listens to every write of the table (messages are not
  filtered by held keys), so the limit applies to the cluster-wide write rate per table, not per instance.

### PostgreSQL listening: R2DBC

**Decision:** the PostgreSQL adapter listens through r2dbc-postgresql; reads and writes stay on JDBC.

- `r2dbc-postgresql` is an `optional` dependency like the drivers: required when PostgreSQL is used, with a clear
  error if it is missing.
- **No fallback to the pgjdbc listener:** a second code path with a ~750 notifications/s limit would need tests of
  its own, and the difference is extreme.
- **Configuration:** the adapter takes the JDBC `DataSource` plus an R2DBC `ConnectionFactory` for listening (it
  covers TLS and every other option without the library mirroring them). A convenience method takes JDBC URL, user
  and password and builds the listening factory (`jdbc:postgresql://...` becomes `r2dbc:postgresql://...`). A
  factory cannot be derived from an arbitrary `DataSource`: it is opaque (no URL, user or password; `DatabaseMetaData`
  gives no password, and dynamic credentials such as IAM tokens are computed inside it); reading the getters of known
  types (`HikariDataSource`, `PGSimpleDataSource`) would break silently with dynamic credentials, so only on demand.
- **Reads and writes stay on JDBC; R2DBC alone was measured and rejected** (`PostgresJdbcVersusR2dbcExperimentTest`,
  PostgreSQL 18, pgjdbc with HikariCP against r2dbc-postgresql 1.1.1 with r2dbc-pool 1.0.2, both pools of 10, 10,000
  rows of 1 KB, each operation twice):

  | Operation | JDBC p50 / p99, 8 concurrent | R2DBC p50 / p99, 8 concurrent |
  |---|---|---|
  | upsert with `pg_notify` | 142–146 / 211–244 µs, 24,100–26,800/s | 313–403 / 453–689 µs, 13,200–14,400/s |
  | conditional write (conflict) | 138–144 / 200–275 µs, 27,400–30,000/s | 308–310 / 526–561 µs, 14,500/s |
  | read of 100 rows by `= ANY` | 356–359 / 499–511 µs, 7,600/s | 1,277–1,307 / 2,253–2,373 µs, 3,800–3,900/s |

  R2DBC takes about twice as long and does about half as many operations for writes, and about 3.6 times as long
  for batched reads, with default settings. It would have saved one driver and the `DataSource` configuration, and
  needed no store-executor threads for PostgreSQL; not worth halving the store path. Functionally it would have
  worked (array parameters, `bytea`, SQLState `23505` of a unique violation, and a `postAllocate` hook per new
  connection for the standby check all OK).
- **Channel partitioning is dropped.**
- **The README documents the inherent cost:** about 33 µs of PostgreSQL CPU per write and listening session
  (every `NOTIFY` wakes every listening session); listener sharing keeps it to one session per JVM.

Why, as measured:

One pgjdbc session hands out notifications promptly only up to about
750/s, and every instance receives every write of the table, so above about 750 writes/s per table propagation
takes hundreds of milliseconds (see "PostgreSQL: statements and propagation measured"). Options: several
`LISTEN` sessions per instance, each on a channel for a share of the hashes (no new dependency; costs
connections, and every `NOTIFY` wakes every listening backend); a different client just for listening (e.g. an
R2DBC or Vert.x PostgreSQL client, as an optional dependency); a fix in pgjdbc upstream (the 1 ms peek is
hard-coded); or documenting the limit.

Channel partitioning measured (`PostgresChannelPartitioningExperimentTest`, PostgreSQL 18, 10,000 keys written at
random by two paced writers, 4 instances with one session per channel each, channel = bucket of the key, each
variant twice):

| Writes/s (target) | Channels (sessions) | Arrival p50 / p99 | PostgreSQL CPU per 1,000 writes | Writes/s reached |
|---|---|---|---|---|
| 1,500 | 1 (4) | 50–52 / 632–788 ms | 175–177 ms | 1,500 |
| 1,500 | 2 (8) / 4 (16) / 8 (32) | 1.5 / 4.1–4.2, 3.6, 2.3 ms | 233–242 / 387–394 / 658–672 ms | 1,500 |
| 3,000 | 1 (4) | 668–1,082 / 1,388–3,042 ms | 131–134 ms | 3,000 |
| 3,000 | 2 (8) / 4 (16) / 8 (32) | 4.3 / 26.1, 2.4 / 7.5, 1.6–1.7 / 4.6–4.7 ms | 202–205 / 366–379 / 675–678 ms | 3,000 |
| 6,000 | 1 (4) / 2 (8) / 4 (16) / 8 (32) | 627–920 ms, 19–27 ms, 3.1 ms, 1.6–1.7 ms p50 | 142–147 / 217–227 / 384–393 / 729–758 ms | 5,400 / 5,300 / 4,400 / 3,200 |

- **Partitioning fixes the latency** as soon as every channel stays below about 750 notifications/s (1.5 ms p50
  at 1,500/s with 2 channels, 1.6 ms at 3,000/s with 8).
- **But PostgreSQL's CPU grows with the number of listening sessions:** about 20 µs per write and session (every
  `NOTIFY` wakes every listening backend of the database, which then filters by channel). With 32 sessions
  PostgreSQL spends about 0.7 s of CPU per 1,000 writes, and the writers no longer reach 6,000/s (3,200).
  Sessions are instances × channels, so the cost multiplies with both.
- **A single hot key above about 750 writes/s stays on one channel**; partitioning cannot help it.
- **Next:** a client that drains notifications promptly (option 2) would need one session per instance, so its
  CPU stays at the 1-channel level (about 140–175 ms per 1,000 writes for 4 instances) and it also covers hot
  keys.

R2DBC listener measured (`PostgresR2dbcListenerExperimentTest`, r2dbc-postgresql 1.1.1, PostgreSQL 18, one channel,
one listening session per instance, two paced writers, each variant twice):

| Listener | Keys | Instances | Writes/s | Arrival p50 / p99 | PostgreSQL CPU per 1,000 writes |
|---|---|---|---|---|---|
| pgjdbc | 10,000 | 4 | 1,500 / 3,000 / 6,000 | 28–101 / 489–1,646 / 1,604–2,056 ms p50, p99 up to 4.1 s | 176–177 / 133 / 137–140 ms |
| R2DBC | 10,000 | 4 | 1,500 / 3,000 / 6,000 | 0.3 / 1.3–1.4, 0.3 / 0.5, 0.3 / 0.5 ms | 175–176 / 135–140 / 143–145 ms |
| R2DBC | 10,000 | 16 | 3,000 | 0.6 / 0.9–1.0 ms | 528–544 ms |
| pgjdbc | 1 (hot) | 4 | 1,500 | 48–55 / 948–1,070 ms | 178–180 ms |
| R2DBC | 1 (hot) | 4 | 1,500 | 0.3 / 1.1–1.4 ms | 171–174 ms |

- **R2DBC removes the limit:** 0.3 ms p50 at every rate up to 6,000 writes/s (faster than MongoDB's change
  streams here), the hot key included, with one session per instance.
- **PostgreSQL's CPU stays at the single-session level**, not multiplied by channels. What remains is inherent to
  `NOTIFY`: every write wakes every listening session, about 33 µs each (16 instances at 3,000 writes/s are
  about 1.6 cores). Listener sharing (one session per JVM, not per cache) keeps that as low as it can be.
- **Costs:** r2dbc-postgresql brings Reactor and Netty (about 20 artifacts), and the listener needs connection
  settings of its own, since a JDBC `DataSource` (e.g. a pool) cannot be turned into an R2DBC connection.
  Transaction-mode poolers still cannot carry `LISTEN`, as today.

### The PostgreSQL adapter (built 2026-10-22)

`io.github.oberhoff.distributedcaffeine.postgresql`, using nothing of the library but the SPI (enforced by
`ArchitectureTest`):

```java
PostgresAdapter.newBuilder(dataSource, schemaName, tableName)   // names mandatory, at most 63 bytes, quoted
    .withDiscriminator("orders")                                // optional, "default"
    .withListener(connectionFactory)                            // R2DBC; or withListener(jdbcUrl, user, password)
    .withSchemaReset()                                          // optional
    .build();                                                   // the timeout is the cache's (see Settings)
```

- **A full NOTIFY queue** (a session listening in the database, any application's, left in a transaction) makes
  PostgreSQL refuse every write that notifies at its commit: nothing is written unannounced, and the write fails as
  certainly not committed, with a message naming the cause and how to find the session. Open: PostgreSQL 18 kept
  refusing after that session ended, in every recovery attempted (another session listening anew, VACUUM, both;
  the queue's usage already back near 0); only no listening session at all and a VACUUM freed it. Nothing the
  adapter could do on its own did so reliably.
- **What removes every row at once is announced:** a statement trigger announces a `TRUNCATE` as invalidateAll on
  a channel of the whole table, which every store subscribes to besides its own; a table dropped under the caches
  is created again by the first statement that misses it, which announces the same. A table of an earlier start
  gets the trigger at the next start; without the right to add it, a warning names the DDL.
- **Every statement waits at most the timeout** (the connection's network timeout, set per statement and restored
  for the pool): a connection the network silently stopped carrying would otherwise block its caller for good, since
  pgjdbc has no socket timeout by default. A timed-out statement is not retried (another attempt would most likely
  wait as long again); a timed-out write has an unknown outcome. The listener's JDBC probe is bounded by the probe
  timeout the same way.
- **The listener is required only to distribute.** With `DistributionMode.NONE` nothing subscribes, so neither a
  listener nor r2dbc-postgresql is needed: without a listener the store offers no change feed, and `build()` with
  another mode fails naming `withListener`. The listener is typed as
  `io.r2dbc.spi.ConnectionFactory`; `withListener(jdbcUrl, user, password)` creates a new factory per call, so caches
  share a session only when given the same factory object.
- **Table:** `(discriminator, id, key, value, state, version, write_time)`, primary key `(discriminator, id)`, one
  index `(discriminator, state, write_time)`, the marker as table comment; `key` and `value` typed by the formats
  (`bytea`, `text`, `jsonb`), so a table refuses caches with other serializers, naming the column. `key` is nullable:
  a tombstone for a key never written has no key. Created with its schema under an advisory lock; without the right
  to create it, the start fails with the complete DDL in the message.
- **Statements:** each write is one statement that writes and notifies (`pg_notify` over the rows of a CTE), version
  `greatest(old + 1, clock_timestamp() in µs)`; batches in hash order (concurrent batches lock in the same order), in
  chunks of 1,000 rows. Conditional writes are an `UPDATE ... FROM` on the expected version plus an `INSERT ... ON
  CONFLICT DO NOTHING` for "missing", in one statement; the row that prevented a write is read afterwards (advisory:
  the core decides again on it). `invalidateAll` gives every row of the cache a new version (value rows become
  tombstones) and notifies `INVALIDATED_ALL` with its time once, in one statement; the read returns the server's time
  (a `LEFT JOIN` on it, so also without rows); a row created on "missing" takes the given version. Size pruning keeps the newest `maxSize` values by `OFFSET`, so overlapping runs on several
  instances never prune more (the current generation's over-pruning cannot occur).
- **Retries** as decided (SQLState `40P01`, `40001`, `55P03` and a failed `getConnection` retried; `08...` during a
  statement: unknown outcome, retried except for `put`); the standby check once per physical connection
  (`PGConnection` unwrapped from the pool's proxy).
- **Listener:** one R2DBC session per factory, a channel per cache (`dc_<schema>.<table>.<discriminator>`, shortened
  with a hash past 63 bytes), dispatched by channel; a probe notification sent through JDBC must arrive within 5 s
  on every subscription (a transaction-mode pooler fails the build with the message decided above); a lost session
  tells every cache (`onLost`) and is reconnected when the first of them resumes it (see Supervision), a gap for
  each. A subscription while the session is down (or broken before its loss was noticed) connects at once, so the
  build waits for it; a broken connection is closed without waiting. A pool that timed out waiting for a
  connection is not asked again (it waited its whole timeout already).
- **The core merges consecutive message batches while draining** (up to 1,000 messages, never across a gap or
  interruption marker): PostgreSQL delivers one notification at a time, and population reads stay batched.
- **A cache fallen too far behind** (more than 100,000 messages pending: its executor busy, its store slow) drops
  what is pending, keeps the markers in order, and revalidates as after a gap, instead of queueing without bound (the
  current generation reconciles such an instance the same way).
- **Tested:** the contract tests (31 then, 34 now) on PostgreSQL 18 and 13 (a gap induced by terminating the listening backend);
  14 end-to-end tests (every mode and format, discriminators, async, schema creation, refusal, reset, a table for
  other serializers, missing rights, names, a broken session degrading and revalidating); the retry rules with
  injected SQLStates; and the property test on PostgreSQL (4 seeds × 25 phases, faults: terminated session,
  rejected statement, connection broken after the statement committed), about 27 s, stable in 4 runs; a conditional
  write ignoring the expected version fails all 4 seeds and 2 contract tests.
- **Not tested here:** a real standby, a real transaction-mode pooler (PgBouncer), statement and socket timeouts
  (the user's pool settings). Versions: see "Supported store versions" (9.6 to 18 pass).

### The MongoDB adapter (built 2026-10-22)

`io.github.oberhoff.distributedcaffeine.mongodb`, using nothing of the library but the SPI (enforced by
`ArchitectureTest`):

```java
MongoAdapter.newBuilder(mongoClient, databaseName, collectionName)   // names checked against MongoDB's limits
    .withDiscriminator("orders")                                     // optional, "default"
    .withSchemaReset()                                               // optional
    .build();
```

- **Documents:** `_id: {d: discriminator, h: hash}`, `key`, `value` (by format: `BinData`, string, BSON parsed from
  the JSON), `state`, `version`, `writeTime`; one index `{"_id.d": 1, state: 1, writeTime: 1}`; the schema marker as
  a document with a string `_id`. Serialized keys and values enter update pipelines as `$literal`, so a value such as
  `"$set"` is never read as a field path or operator (a test covers it).
- **The control document per cache** (`_id: {d, c: "invalidate-all"}`, adapter-internal) holds the time of the last
  `invalidateAll`; its change event is the `INVALIDATED_ALL` message, and every read includes it, so the read's
  `$$NOW` comes back in the same round trip even without rows. `invalidateAll` takes its time first (the ceiling of
  the millisecond), then sweeps every row with `updateMany` (not atomic across documents: a load in flight conflicts
  on whichever document it read, before or after the sweep reached it).
- **Versions** from `$$NOW` in milliseconds, scaled to microseconds; a read's time is the floor of its millisecond,
  `invalidateAll`'s the ceiling, so the `invalidateAll` rule only errs towards a reload.
- **Writes:** one `findOneAndUpdate` per key with an update pipeline (a batch write cannot return the versions); "only
  if missing" is an upsert filtered on a missing version, whose duplicate key is the conflict. Size pruning deletes
  the documents beyond the newest `maxSize`, each conditional on the version it had when found.
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
- **Refused at start:** standalone servers, deployments without majority read concern, sharded collections.
- **Tested:** the 34 contract tests (the gap simulated: a lost history cannot be provoked on demand), 13 end-to-end
  tests (every mode and format, discriminators and a second collection on the shared stream, async, an interruption
  resumed from the token with the write made during it arriving, caches joining while writes go on with exactly one
  event per write, caches leaving so that the stream narrows while its events are still unread, with exactly one event
  per write of the remaining collection, a lost history, schema, names), and the property test (4 seeds × 25 phases,
  fault: lost history). Mutation-checked (eight mutants, then three for narrowing, all killed; resuming, widening or
  narrowing without the token needed the event-counting tests, and narrowing only after a lag hook held the
  watching thread back, since a thread that is never behind loses nothing even when it reopens fresh). Versions: see "Supported store versions".
- **Not tested here:** a real rollback or oplog rollover (error 280/286 for real), a real network failure, failover in a
  3-member replica set (the current generation's test harness has one), injected write failures (no proxy for the
  client yet), sharded clusters through `mongos`.

### Connection sharing: always per database

Listening connections are shared between caches, always per database: one R2DBC session per `ConnectionFactory` on
PostgreSQL, one change stream cursor per `MongoClient` and database on MongoDB. There is no sharing mode to choose:
today's `ListenerSharingMode` / `WatcherSharingMode` (`INSTANCE`, `TABLE`/`COLLECTION`, `DATABASE`) are dropped. On
PostgreSQL the reason for the narrower levels was the pgjdbc limit of about 750 notifications/s per session, which
R2DBC does not have (one session carried 30,000/s, below); on MongoDB `DATABASE` was the default already; what
remained was "isolation", with no concrete case behind it.

- **One shared session / cursor per database**, reference-counted in a registry; every cache has its own subscription
  with its own worker queue (coalescing hash set, generous overflow limit with drop and `onGap()`). The shared thread
  never calls into a cache, so it cannot deadlock on a cache's lock (the constraint that shaped today's design).
- **MongoDB as today:** widening and narrowing reopen with `resumeAfter`; no discriminator filter on shared cursors (the client
  drops scopes nobody subscribed to); a cache whose collection is already covered registers directly.
- **A gap reaches every subscriber:** an interrupted shared session or cursor means `onGap()` for every cache on it.

What changes:

- **PostgreSQL's sharing key** is the R2DBC `ConnectionFactory` (by identity) instead of the listener `DataSource`;
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

Measured (`PostgresR2dbcCeilingExperimentTest`, PostgreSQL 18, one R2DBC session listening on 20 channels as for 20
caches, writes spread over all channels by 8 or 16 paced writers, each rate twice):

| `synchronous_commit` | Target writes/s | Reached | Arrival p50 / p99 | Lost | PostgreSQL CPU per 1,000 writes |
|---|---|---|---|---|---|
| `on` (default) | 5,000 / 10,000–40,000 | 5,000 / 7,370–7,610 | 0.3 / 1.3–1.5, then 1.0–1.1 / 1.4–1.5 ms | 0 | 91–101 ms |
| `off` (measurement only) | 5,000 / 10,000 / 20,000 | as targeted | 0.5–0.6 / 1.0–1.6 ms | 0 | 95–110 ms |
| `off` (measurement only) | 40,000 / 80,000 | 29,700–30,300 | 0.5–0.6 / 1.2–1.5 ms | 0 | 88–90 ms |

- **One session carried every rate PostgreSQL could write** (about 7,500 single-row writes/s with default commits
  on this machine, about 30,000/s without commit flushes) at about 0.5–1 ms p50, under 1.5 ms p99, nothing lost.
  Its own limit lies above 30,000 notifications/s; PostgreSQL's write path is the bottleneck, not the listener.
- **So sharing per database has no throughput downside on PostgreSQL** with R2DBC.

### Supported store versions

Newer store features are used whenever they simplify the design; old MongoDB and PostgreSQL versions are dropped
without hesitation. Concretely so far: **MongoDB 4.4 or later** (server-computed values in `find` projections; 4.2 is
end of life anyway), **PostgreSQL 9.6 or later** (decided 2026-10-22). PostgreSQL: the floor is what the adapter
needs, not what PostgreSQL still supports. It needs `jsonb` (9.4), `INSERT ... ON CONFLICT` (9.5) and
`to_regclass(text)` (9.6; 9.5 has only a `cstring` variant, and below 9.5 the writes would need a rewrite).

### Test setup: one store at a time, per image and topology (decided 2026-10-23)

As in the current generation: `MongoIntegrationTest` and `PostgresIntegrationTest` hold one `@Nested` class per image
and topology, which starts its store in `@BeforeAll` and stops it in `@AfterAll` (`MongoTopologyTests`,
`PostgresTopologyTests`); the suites are nested in it and find the running store (`MongoTestDatabase.get()`,
`PostgresTestDatabase.get()`). No store outlives its class, and no two run at once (the shared per-JVM databases
before, stopped only by a shutdown hook, ran next to the containers of single test classes).

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

The whole matrix runs before a release (`mvn test`, hours); while working, the image and suites a change touches
(`-Dtest='MongoIntegrationTest$Mongo_9_latest*' -Dsuites=...`).

### Follow-ups of these decisions

- **Tombstone retention:** decided under Operations (5 minutes, fixed).
- **PostgreSQL delivery:** the notification carries hash and version, the receiver reads the row; if the row is
  already newer than the notification, the newer row is taken (consistent with rule 2).
- **`putAll`:** each key gets its own version, no atomicity across keys, as in Caffeine.
- **Cleanup on MongoDB:** the maintenance worker deletes tombstones and expired rows, as on PostgreSQL. A partial TTL
  index was considered and dropped: its filter would need "state is not `VALUE`", which partial indexes do not
  support as `$ne`, and the worker is needed anyway.
- **Pruning interval:** decided under Operations (1 minute ±20 %, configurable, since it is part of the age bound).

## Open design questions

None at the moment. New ones go here as they come up while building.

### Options for later

- **Async misses without a waiting thread** (measured, see "Cost of the direction" under Async variants). Decided
  2026-10-21: stay on Java 17 and the current design for now; until then, async users with many concurrent slow
  misses pass a virtual-thread executor themselves (`withStoreExecutor`, Java 21+). Two options when it matters:
  1. **Virtual threads as the default store executor when the JVM has them** (detected at runtime, Java 21+): a few
     lines, no duplicated code, covers users who never set an executor. Check first that the drivers do not pin
     carrier threads in `synchronized` blocks on Java 21–23 (fixed in Java 24).
  2. ~~A non-blocking load path for the asynchronous caches~~: built 2026-10-21 (see Async variants).
- **MongoDB's reactive-streams driver** was not measured against the sync driver (PostgreSQL's R2DBC was, and lost
  for statements). If it is reconsidered, measure it like `PostgresJdbcVersusR2dbcExperimentTest` (writes, conditional
  writes, batched reads, same replica set) and check its dependencies; the SPI stays blocking either way.

Checked and not adopted: JetCache's per-key refresh tasks (Caffeine refreshes on access), its non-strict
distributed lock (not a cache concern), result codes instead of the Caffeine API (the store guard covers failures),
per-call TTLs in L2 (Caffeine's `Expiry` covers L1), and custom annotations (a thin Spring `CacheManager` adapter
would be the better route later). Its local sync (keys only, no versions, at-most-once pub/sub, instance clocks) and
its lock on every L1 hit are weaker than this design.

## Lessons checklist

Each lesson from the current generation becomes a test, a design rule, or is recorded as not applying. Names refer
to the working notes of the current generation.

| Lesson | Note | Carries over as |
|---|---|---|
| Overtaken refreshes and loads must be discarded (tokens; the null-operation trap) | refresh-in-flight-discard | test; mechanism replaced by versions (Ordering, rules 4 and 6) |
| Loads run outside the lock; commit rules for extras | refresh-in-flight-discard | design rule: `AsyncCache` loads outside the lock; extras only where L2 holds no value (see Shared loads) |
| Stale loads are only visible to a freshness oracle, not to a convergence oracle | convergence-loads-freshness-oracle | central test oracle |
| Property tests need publish latency and phased checks to see concurrency | convergence-property-test-and-its-blind-spots | test harness: built as `InternalPropertyTest` (below) |
| The store guard must cover the read path, not only writes | store-guard-and-health-state | test (SPI: `read`, and the read after a `putIfUnchanged` conflict) |
| A failed write may still land; forget its operation | failed-write-suppresses-landed-echo, postgres-single-statement-publish | test; mechanism replaced by dropping the key (Ordering, rule 5) |
| `invalidateAll` needs a global marker and global, not per-key, ordering | invalidate-all-marker-design, publisher-repository-adapter-split | design rule |
| Replayed invalidations can be dropped past ~1 min; history can be lost; revalidate after gaps | store-downtime-robustness-plan, read-based-synchronization-design | invariant 7, test |
| Starting synchronization on a partially activated instance froze | partial-activation-freeze | does not apply: no stop/start and no `getAdapter()` (see Lifecycle, Adapter ownership) |
| Overlapping size pruning over-prunes | concurrent-size-pruning-over-prunes | test |
| Async publishes need one ordered drain per cache | async-publish-single-drain | writes are synchronous now; the ordered queue per cache carries over for delivery (see Operations) |
| The write is the round trip; the global lock caps throughput (fair lock, striping) | write-path-cost-measured | benchmark baseline |
| Duplicate-key retry on upsert is needed though rarely hit | upsert-duplicate-key-retry-cost | does not apply on MongoDB: upserts by the composite `_id` are atomic (see Adapter setup); a duplicate key on `putIfUnchanged` stays a conflict |
| Mongo: one connection pair per watcher; filter server-side; watcher sharing; majority write concern, no w:1 (measured: w:1 slows propagation about 100x, see MongoDB write concern) | change-stream-watcher-connection-cost, change-stream-match-pushdown, mongo-watcher-sharing, postgres-single-statement-publish | adapter design (sharing: see Connection sharing; failed applies now dropped, not re-queued; dispatch by the discriminator in `_id`) |
| Postgres: stream reads in a transaction; heartbeat for half-open listeners; poolers need a listener data source; one session delays above ~750 notifications/s; listener sharing | postgres-streaming-and-ordered-reads, postgres-listener-half-open-heartbeat, postgres-listener-sharing, pgjdbc-single-session-notification-cliff | adapter design (listener via R2DBC, so the ~750/s limit no longer applies; heartbeat to re-check with R2DBC; sharing: see Connection sharing) |
| Multiple caches per collection/table via discriminator | discriminator-and-watcher-sharing-design | adapter design |
| Stats counting rules | distributed-stats-design | design rule |
| Entries expired via `setExpiresAfter` stay resident and invisible | caffeine-setexpiresafter-stuck-entries | design rule: never used internally; passed through to users as Caffeine's behaviour |
| `weightedSize()` needs `cleanUp()` first | caffeine-weightedsize-needs-cleanup | test harness |
| Log-counting tests break on any new log line | logging-breaks-log-counting-tests | avoid that test style |
| 3-member replica set; javadoc verification | mongo-three-member-test-replica-set, javadoc-verification-gotcha | test harness, build |
| Mongo < 4.4 namespace limit (120 bytes) | mongo-namespace-limit-test-names | does not apply: MongoDB 4.4 or later |
| Eviction echoes race persistence tests | eviction-echo-races-persistence-tests | does not apply: no eviction distribution |
| The own-echo re-add is load-bearing | own-echo-readd-is-load-bearing | does not apply: no contents synchronization |
| Restore marking a value Caffeine is evicting | restore-mark-race-loses-store-backed-keys | does not apply: no restore |
| Silent entry loss (parked) | silent-entry-loss-open-lead | does not apply: any subset is valid (invariant 1) |
| Store owns values, the mode owns residency; `pruningStatus()` | extended-persistence-no-eviction-mode | does not apply: no statuses |
| `PartialCacheEntry` / `CacheEntry` projections | cache-entry-projection-design | does not apply: the SPI has plain rows, no projections |

### The property test (built 2026-10-21)

`InternalPropertyTest`: three instances (a plain one with a 4-entry L1 and a bulk loader returning an extra key, one
with population and absent caching, an asynchronous one) on the reference store; per instance a writer and a loader
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
announced meanwhile could not reach it, and likewise across a gap (Absent caching).

Mutation scores (seeds failing of 16, two runs of 8): loads writing unconditionally 15, no minimum version for loads
in flight 11, no write-registry check 4, `remove(k)` without a tombstone 3, everything trusted 3, the
`invalidateAll` message not moving the epoch 1; the absence checks, L1 installs ignoring versions and extras handed to
Caffeine 0. Every one of them is killed by a targeted unit test, and for the last group the defences overlap (the
write registry also covers a commit ignoring versions; the epoch also covers an `invalidateAll` dropping a load's
future). The property test is a net with a low rate per run for rare interleavings (the absence defect showed up
in about one run in eight before it was fixed), not a substitute for the targeted tests: a finding becomes one.

## Costs, accepted knowingly

- The first read of a key on each instance is one store round trip (today population has pushed it).
- With population on, a write to a key held by N instances causes N batched reads.
- Every row that exceeds its maximum age costs one write plus a message to every instance.
- The maximum age is precise to one pruning interval plus delivery, not to the millisecond.
- `refreshAfterWrite` refreshes by an age of at most 2R; concurrent refreshes may call the source once per instance.
- No warm-up: L1 starts empty, so the first access per key and instance after a start reads L2.
- A store is mandatory.
- Two lifetimes to configure.
- No global eviction across instances; L2 is the global bound.
- Writes are refused while the store is down; an unconditional put with an unknown outcome throws.
- Hashing on every user call: 2–4 times a plain Caffeine hit (30–65 ns), 32 bytes per call.
- After a gap, a version read per held key (lazily and in a rate-limited background pass).
- With absent caching, each instance learns its own absences.
- Dependencies: Fory always (about 2.7 MB plus Janino); for PostgreSQL also r2dbc-postgresql with Reactor and
  Netty (about 6.6 MB).
- Every PostgreSQL `NOTIFY` wakes every listening session (about 33 µs of PostgreSQL CPU each).
