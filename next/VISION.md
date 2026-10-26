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
version = old.version + 1                  if the row exists
version = server time in microseconds      if it does not (a new row)
```

- `old.version + 1` makes the version strictly increasing per key, because the store serializes writes to one row
  (row lock / document atomicity), without any clock.
- The server time as the start keeps it increasing when a key is recreated after its row is gone: it lies above every
  version an earlier row of the key had, as long as the store's clock does not go backwards (the residual; an adapter
  with a counter that never goes backwards uses it instead).
- Until 2026-10-24 every write took `max(old.version + 1, server time)`; the time term matters only for a new row, and
  versions are no longer compared with times (the cut-off of `invalidateAll` compares write stamps), so it went
  (design study, step 3).
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

### `invalidateAll` and loads in flight (decided and built 2026-10-22; replaced 2026-10-24 by the cut-off, see "Design study")

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

### `invalidateAll`, logical: one counter and the cache's time T (designed 2026-10-24, not built yet)

Replaces the sweep of the previous section. The previous design gives every row a new version on `invalidateAll`,
lets every instance learn the sweep's time from messages, and has the core treat value rows below it as missing and
tombstone them in the background. That costs a write per row (one change event per row on MongoDB), logic on several
core paths, and a residual: an instance that missed the message cannot know the time. It also rests on the store's
wall clock never going backwards, which a failover to a member with a slower clock or a clock step breaks: a put
after an `invalidateAll` on a key without a row could get a version below its time and be hidden, a lost write.

**1. One counter for versions and read times.** Versions and the time a read returns come from one counter of the
store that never goes backwards, also across a failover. Contract (replaces "server time in microseconds"):

- (a) a write's version is above every version committed before the write began, and above the key's old version
  (`max(old + 1, counter)`);
- (b) a read's time is above every version committed before the read began, and at most the version of any write
  that begins after the read returned.

Nothing in the core reads a version as a time: it only compares versions with each other and with read times (the
version rule, the heartbeat, `invalidateAll`). Ages stay on the store's clock (`write_time`), as before.

- **MongoDB: `$$CLUSTER_TIME`** (a `Timestamp`, as a long: seconds << 32 | increment), in the update pipelines and in
  the control document's projection of every read. Measured (`MongoClusterTimeExperimentTest`, 4.4.0 and 9, 2,000
  rounds of read, `invalidateAll`, write, in a causally consistent session; on 9 also with a step-down every 200
  rounds, 9 failovers): a read was never at or below the version of the write before it, never above the time of the
  `invalidateAll` after it (equal every time, nothing being written in between), and a write after the
  `invalidateAll` was never at or below its time. The cluster time is logical: it never goes backwards, across members
  and failovers, unlike `$$NOW`. Versions jump from microseconds (about 1.7e15) to the cluster time (about 7.6e18),
  upwards, so existing rows stay ordered.
- **PostgreSQL: a sequence per table** (`nextval` for versions, `last_value + 1` as the read time; `CACHE 1`, so values
  are issued in order). A committed version's sequence record is in the WAL before the commit, so a promoted standby
  continues above it. Residual: a read time handed out from a value not yet in the standby's WAL when the primary
  fails can be reissued as a version, which at worst lets one load racing an `invalidateAll` survive (stale until
  the maximum age); a failover with an asynchronous standby loses writes anyway, reported as a loss.
- **Custom stores** may use a clock (`max(old + 1, server time)`), which fulfils (a) and (b) while the clock does not
  go backwards; the contract says so.

**2. The cache's time T.** One control row per cache holds T, the version of the last `invalidateAll` (0 before any).
MongoDB has the control document already; PostgreSQL gets a reserved row (`id = '#invalidated'`, state `CONTROL`; the
core already refuses reserved hashes, so it can never be a key's).

- **Visibility: a row with a version at or below T counts as missing, for every operation:** reads, `readVersions`,
  the expectations of conditional writes, expiry, and the count of size pruning; `pruneTombstones` deletes such rows.
  The store filters, in the same query: a subselect on the control row by primary key on PostgreSQL; on MongoDB the
  control document is already part of every read's `$in` (added to `readVersions`; maintenance reads it first).
- **`invalidateAll()`:** writes the control row (its version, by the counter, becomes T), announces
  `INVALIDATED_ALL(T)` and returns T. No other row is touched, whatever the size of the cache.
- **Unconditional writes** (`put`, `invalidate`, `invalidateIfUnchanged`): `max(old + 1, counter)`, which is above T
  by (a): a write after an `invalidateAll` is always visible, whatever the clocks do.
- **Conditional writes** (`putIfUnchanged`: loads, refreshes, the compute methods) take the read time of the read they
  are based on, for both expectations (`ExpectedEntry` carries it): the version is `max(old + 1, readTime)`.
  - "missing" means no row, or a row below the read time (a row at or below T at the read is below it by (b); a row
    written after the read is at or above it, which makes the write conflict);
  - "version v" means the row still has version v, as before.
  - `invalidateIfUnchanged` with `MISSING` (`remove(k)` on a key without a value, which must move the version so that
    a load in flight conflicts) decides "missing" the same way and so takes the read time too; its tombstone gets
    `max(old + 1, counter)`, visible.

**Why the race is closed:** a conditional write based on a read before an `invalidateAll` gets
`max(seen + 1, readTime) = readTime` (seen is below the read time by (b)), and the read time is at most T by (b):
the row is written but invisible. Based on a read after the `invalidateAll`, the read time is above T: visible. A key
recreated after its rows were pruned gets a version above every earlier one by (b). The race needs no message, no
memory in the core and no clock, so the residual of the previous design is gone: every read learns T from the store.

**Point 1, settled by case analysis:** MongoDB cannot read the control document inside a single-document write, so a
conditional write must not need T. It does not: "missing" is decided against the read time, which the core passes.
Rows at or below T when the read ran are below the read time; a row written after the read is at or above it.

**What goes:** the sweep (a write per row; on MongoDB a change event per row), the core's handling of rows below the
sweep time on read, load and population paths, their background tombstoning, and the residual. **What stays in the
core:** `INVALIDATED_ALL` still clears L1 and moves the epoch on, and the core keeps the highest T it knows (one long)
so that a load racing an `invalidateAll` does not hold its invisible result in L1.

**Costs:** writes need no T (an unconditional write is above it by (a), a conditional write decides "missing" by
the read time), so only reads (`read`, `readVersions`) and maintenance look T up. Measured 2026-10-24
(`PostgresLogicalInvalidateAllExperimentTest`, `MongoLogicalInvalidateAllExperimentTest`; postgres:18 and mongo:9 in
Docker Desktop on macOS, 10,000 keys, today's statements against the logical ones on the same data, alternating, 3
rounds; p50, ranges over the rounds):

| Operation | PostgreSQL today / logical | MongoDB today / logical |
|---|---|---|
| read, 1 key | 175–183 / 194–206 µs | 280–326 / 274–302 µs |
| read, 100 keys, on the server (`EXPLAIN ANALYZE`) | 0.43 / 0.37 ms, same plan; the two lookups 0.02 ms | – |
| `readVersions`, 100 keys | 251–286 / 241–284 µs | 509–764 / 683–812 µs (with the batch size, see below) |
| put, 1 writer | 172–201 / 172–194 µs | 289–366 / 268–344 µs |
| put, 16 writers | 15,300–18,070 / 15,320–18,220 puts/s | 7,600–9,460 / 9,290–9,860 puts/s |
| `invalidateAll`, 10,000 rows | 43 ms / 0.65 ms | 77 ms and 10,001 events / 2.1 ms and 1 event |
| `invalidateAll`, 100,000 rows | 438 ms / 1.2 ms | 862 ms and 100,001 events / 4.9 ms and 1 event |

- **Common paths unchanged within the noise,** except PostgreSQL's single-key read (+10–30 µs, the two lookups) and
  MongoDB's `readVersions` (+50–100 µs, one more document back); `nextval` costs nothing measurable, also with 16
  writers, and MongoDB's puts are no slower with the cluster time.
- **`invalidateAll` becomes 70–380 times faster** and, on MongoDB, sends one event instead of one per row.
- **MongoDB's first batch holds 101 documents:** 100 rows plus the control document make 101, and the driver then
  asks for another batch it would not need (+300 µs). The adapter must set the batch size to the number of ids
  (`batchSize(ids + 1)`). Today's `read` of 100 keys already returns 101 documents (the control document is part of
  it), so this is a fix for today too.
- **MongoDB versions are BSON timestamps,** not longs: no formula may add to them (`$add` refuses timestamps). None
  needs to: every write gets a later cluster time anyway, which makes `max(old + 1, counter)` simply the counter.
- **Found on the way, not caused by this:** reading 100 keys with 100-byte values takes 1–16 ms (p99 23–27 ms) on
  both databases, against 0.25 ms without the values and 0.4 ms on the server: the transfer of the larger answer
  through Docker Desktop's network on macOS, most likely. To be checked on Linux before it counts for anything.

**To build:** the counter in both adapters (PostgreSQL schema marker 2: sequence and control row); the visibility
filter; `ExpectedEntry` with the read time for both expectations, and the read time for `invalidateIfUnchanged`'s
`MISSING`; `invalidateAll` without the sweep; the core
simplified; contract tests for (a), (b), visibility after `invalidateAll` and the invisible racing write (replacing
the three `invalidateAll` tests); the property test's freshness oracle covers the race already.

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
    Map<String, Long> invalidateIfUnchanged(Collection<Expected> expected);   // remove: if same version, any state, or MISSING
    long invalidateAll();                                // every row a new version; one INVALIDATED_ALL with its time

    // maintenance: scheduled by the core, idempotent in the adapter
    int expire(Duration maxAge);                 // marks EXPIRED, announces
    int pruneToSize(long maxSize);               // deletes, silent
    int pruneTombstones(Duration retention);     // deletes, silent
}

interface ChangeFeed {                           // implemented by a store that can announce its writes
    ChangeSubscription subscribe(ChangeListener listener);   // every write committed after it returns is delivered
}

interface Adapter<K, V> { Store createStore(StoreSettings settings); }  // what an adapter provides, typed for the builder

record StoreSettings(Format keyFormat, Format valueFormat, Duration timeout) {}   // what the cache configured

interface ChangeListener {
    void onChanges(List<Change> changes);        // batched, per key in commit order (at least the latest)
    void onGap();                                // continuity lost: the core revalidates
}

record Change(Change.Kind kind, @Nullable String hash, long version) {}   // WRITTEN, INVALIDATED, EXPIRED, INVALIDATED_ALL
record Row(String hash, byte[] key, byte @Nullable [] value, State state, long version, Duration age, long stamp) {}
record Entry(String hash, byte[] key, byte[] value) {}              // format fixed per store
record ExpectedEntry(Entry entry, long expectedVersion) {}  // ifVersion(entry, v) or ifMissing(entry)
record Expected(String hash, long expectedVersion) {}       // ifVersion(hash, v) or ifMissing(hash)
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
connection, and every other error are not retryable. A missing table is not retryable either (see "The PostgreSQL
adapter": changing the table by hand is not supported).

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
  the core decides again on it). `invalidateAll` upserts the cache's control row (`id = '#invalidated'`, state
  `CONTROL`) and notifies `INVALIDATED_ALL` with its write time, the cut-off, in one statement (until 2026-10-24 it
  gave every row a new version); every read returns the cut-off (a subselect on the control row, joined so that it
  comes back also without rows), and the stamp of a row is its write time in microseconds. Size pruning keeps the
  newest `maxSize` visible values by `OFFSET`, so overlapping runs on several instances never prune more (the current
  generation's over-pruning cannot occur).
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
- **The control document per cache** (`_id: {d, c: ...}`, adapter-internal) holds the stamp of the last
  `invalidateAll` (`t`, the cut-off, since 2026-10-24); its change event is the `INVALIDATED_ALL` message, and every
  read includes it. `invalidateAll` writes only it (until 2026-10-24 it swept every row with `updateMany`).
- **Every write stamps the document with `$$CLUSTER_TIME`** (`s`, `Row.stamp`), which orders writes across
  documents and never goes back; `$$NOW` has millisecond resolution and is the primary's clock, so a put in the same
  millisecond as an `invalidateAll` could not be told from one before it. Measured by
  `MongoClusterTimeExperimentTest` (4.4.0 and 9, also across failovers).
- **Versions** from `$$NOW` in milliseconds, scaled to microseconds (per key; the cut-off compares stamps).
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

## Design study: portable store primitives (started 2026-10-24; build order: steps 0 to 5 built 2026-10-24)

**Aim:** a store needs only what many stores offer (plain JDBC/ANSI SQL, the common operations of key-value and
document stores). Distribution is an **optional** feature of an adapter, never a must: a native feed where the store
has one (MongoDB change streams, PostgreSQL `LISTEN/NOTIFY`), a generic polling feed otherwise, or none
(`DistributionMode.NONE`). Races are solved by versions and by the instances themselves; a rare edge case only has
to be recognised, and repairing it may cost something. Adapters with smarter means (PostgreSQL, MongoDB) implement
the same operations natively, so they lose nothing (see "Two levels").

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

### `invalidateAll`: a cut-off time, repaired by the loaders

- **The cut-off:** `invalidateAll` upserts a reserved control key; its write time is T. A row written at or before T
  counts as missing, exactly like a value older than the maximum age: the visibility filter becomes
  `write_time > max(now - maximumAge, T)`. No row is touched, whatever the size of the cache; maintenance deletes the
  hidden rows later, in the background. A put after the `invalidateAll` has a later write time: visible.
- **Every read includes the control key** (one more key in the batch), so every instance knows T from the store,
  without a message. The core applies the filter (portable); an SQL adapter may filter in the query, but returns the
  versions of hidden rows, so that a conditional write can replace them.
- **The race:** a load reads, the source answers with the old value, `invalidateAll` runs, the load writes: its write
  time is after T, so it would be visible. **Recognised by the loader:** after its write it reads the control key
  again (one primary-key read per load that wrote, small next to the source call it just made); if T moved since its
  read, it deletes its own write conditionally (`deleteIf(version)`). An SQL adapter can fold the check into the
  conditional write itself (`AND (SELECT write_time FROM ... control) = ?`), saving the read. This works without a
  feed and without any comparison of clocks between instances.
- **Residual:** the loader dies between its write and the check: the old value stays until the maximum age. Rare,
  bounded, documented.
- **L1:** `INVALIDATED_ALL` (where there is a feed) clears it and moves the epoch on; a load still in flight completes
  with an older epoch and is checked on its next read. The core's highest-known sweep time goes.

### Clocks: what is left

- Instances use no clock for versions or cut-offs; only the store's clock, and only for write times and the start
  value of recreated keys.
- **A store clock going backwards** (failover to a member with a slower clock, a clock step) can: hide a put right
  after an `invalidateAll` (its write time at or before T; the writer recognises it when it knows T, otherwise a
  loading cache reloads it from the source, a cache without a loader loses that write); and start a recreated key
  below a holder's copy (served until the maximum age). Both need a regression within the skew window at that moment.
  An adapter with a monotonic counter uses it for the start value and the cut-off, and both residuals are gone.

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

### The current contract, point by point

| Point | Today | In the study |
|---|---|---|
| Store 1: versions | `max(old + 1, server time)`, computed by the store | per-key counter, starting at the store's time; still computed by the store, in one upsert |
| Store 2: clock | ages and read times from the store's clock | ages from the store's write time; read time no longer compared with versions |
| Store 3: visibility, durability | store guarantee | unchanged |
| Store 4: atomic conditional writes | `putIfUnchanged`, `invalidateIfUnchanged` | compare-and-set on the version, unchanged in substance |
| Store 5: idempotent maintenance | expire, prune | plus deleting rows hidden by the cut-off |
| Store 6: bytes, own cache | unchanged | unchanged |
| Store 7: semantics | per method | per operation; `ifMissing(entry, readTime)` and the sweep go |
| Store 8, 9: failures, settings | unchanged | unchanged |
| ChangeFeed 1-7 | for native feeds | unchanged, optional; plus the polling feed |

### Conditional writes racing an `invalidateAll` (worked through 2026-10-24)

**The yardstick: Caffeine.** Its `invalidateAll` is weakly consistent across keys, but per key every operation is
atomic: a `compute` and the removal by `invalidateAll` happen in one order or the other. So for one key the outcome
must equal either "the operation, then `invalidateAll`" (the key is absent) or "`invalidateAll`, then the operation"
(the operation as on an absent key). The race (the operation reads before T, writes after it) gives a third outcome
unless something prevents it.

| Operation | Race outcome without a check | Valid? |
|---|---|---|
| load, refresh, `get(k, fn)`, `computeIfAbsent` | a value from the source or function as it was before the `invalidateAll` | no: the source may have changed before it (the reason for `invalidateAll`) |
| `compute`, `computeIfPresent`, `merge` on a present value | `f(old)`: the old value survives in what is written | no: after the `invalidateAll`, `f` would have seen no value |
| `compute`, `merge` on an absent key | `f(null)` or the given value | yes as values go, but `f` may read a source like a loader |
| `replace(k, v)`, `replace(k, old, new)` | written because a value was present | no: after the `invalidateAll` the key is absent and nothing is written |
| `putIfAbsent` on an absent key | the caller's value | yes: "`invalidateAll`, then `putIfAbsent`" |
| `remove(k)`, `remove(k, v)`, `invalidate` | a tombstone | yes: absent in both orders |
| `put`, `putAll` (unconditional) | linearised at their write | yes |

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

### Rows hidden by the cut-off: deleted silently (worked through 2026-10-24)

**Silently, without announcing them as `EXPIRED`.** An announcement would tell nobody anything new: holders with a
feed have cleared L1 on `INVALIDATED_ALL` already, a holder that missed it is in a gap and revalidates (the store
reports the row missing), and holders without a feed are not reachable anyway. Announcing would bring back the storm
of one event per row the cut-off removed. Each case of a silent delete:

| Who | What happens | Valid? |
|---|---|---|
| a reader | the row counted as missing before the delete, and after it | unchanged |
| a load in flight that read the row before T | its conditional write finds no row: a conflict, it reads again (or, if the write came first, its check finds T moved and undoes it) | yes |
| a load that read the row after T (as missing, with its physical version) | its conditional write conflicts, it reads again | yes, one extra round trip |
| a holder in a gap | revalidates, the row is missing, the copy dropped | yes |
| a later write of the key | starts at the store's time, above every earlier version | yes |

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

### The polling feed (worked through 2026-10-24)

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
rule, the minimum versions of loads and writes in flight, the heartbeat and absent caching all compare with the
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
  container; the standard dialect dropped, or kept only with H2 back as a tested target. MySQL would need a counter
  table instead of a sequence. Native adapters stay where a database has notifications or `RETURNING`. **The focus
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

### Thread safety and linearizability (2026-10-24)

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

### The decision's cut-off on every row (2026-10-24, option A)

**Why.** The cut-off is logical: `invalidateAll` moves it and touches no row, so a conditional write on a version
cannot see it. The core therefore checks the cut-off after every conditional write of a value (a read of no keys) and
undoes the write if it moved, with a repair for unknown outcomes and a second control write for the commit window.
That costs a compute three store operations (read, write, check; JetCache has no atomic compute, Redisson takes four
with a lease lock, Hazelcast's optimistic loop two, its EntryProcessor one), and it leaves a window: between the write
and its undo the value is visible, and a compute (any instance) that builds on it survives the undo (Lincheck, one
key). Hazelcast's loop needs no check because its `clear()` deletes, which a `replace(old)` sees.

**The rule.** Every row carries `basis`: the cut-off the write was decided under, i.e. the cut-off reported by the
read its decision was based on. A write decided on nothing read (an unconditional put, a tombstone) carries
`Row.NO_BASIS` (`Long.MAX_VALUE`). A row is visible iff no cut-off was ever set, or

    stamp > cutoff  and  basis >= cutoff

The first half is the old rule (written before the cut-off); the second hides a value decided before the cut-off,
whenever it was written. Equality is visible: decided under the current cut-off.

**What it removes.**

- **The check after a conditional write, its undo, and the start-over:** a value decided before an `invalidateAll`
  is hidden from the moment the cut-off moves, for every reader and every instance, so nothing can build on it. The
  writer keeps its result: it counts as before the `invalidateAll` (Caffeine's order "the operation, then
  `invalidateAll`"). A compute is two operations (read, write), a load one after its read.
- **The repair of unknown outcomes by value:** a write that landed unseen is hidden by the same rule.
- **The second control write:** it caught a write whose check ran inside the commit window of the first; there is
  no check any more, and such a write's basis is below the cut-off. It also hid values decided after the first
  control write (between the two), which made the operation non-linearizable.

**What it costs.** One field per row in every store (PostgreSQL `basis BIGINT`, MongoDB `b` as a timestamp like
`s` and `t`, JDBC `basis BIGINT`, in memory), written by every write, returned in `Row`, `PrimitiveStore.Head` and
`VersionsResult`, and one more condition wherever a store applies visibility (maintenance: expiry, size pruning,
deleting hidden rows). The SPI: `ExpectedEntry` carries the basis (`ifVersion(entry, version, basis)`,
`ifMissing(entry, basis)`); an unconditional `put` writes `NO_BASIS`.

**Unchanged.** The core still applies visibility to what it reads (a hidden row is a tombstone at its version, so a
conditional write can replace it); a load still reads the cut-off before the source, a refresh too; the poller's
touch rewrites a row with its own basis and skips hidden rows; a change is announced for a hidden write as for any
(holders read and find nothing). L1: a value committed by a write decided before this instance's `invalidateAll`
carries an older epoch, so it is checked against the store on its next read and dropped there.

**Built 2026-10-24 (step 1):** the field in all four stores (PostgreSQL column `basis bigint NOT NULL DEFAULT
NO_BASIS` in the shape check; MongoDB `b`; JDBC `basis`; in memory), `Row.visible(stamp, basis, cutoff)` as the one
rule for the core, `PrimitiveStore` and the in-memory store (PostgreSQL and MongoDB in their filters); the check, the
undo, the repair by value, `checked` for bulk loads and the second control write removed. What else it took:

- **The epoch before the read** for every value held from a decision (loads, updates, bulk loads, refreshes): the
  check used to catch a write racing this instance's own `invalidateAll`; now such a value must be held untrusted, so
  that its next read checks it (and finds it hidden).
- **A conflict's row is unfiltered:** its value counts only if visible under the cut-off the decision read.
- **Semantics:** a load overtaken by `invalidateAll` answers its own value and leaves it hidden, as in Caffeine,
  whose `clear()` waits for the load's bin (two tests expected a start-over; one control write, one message).

Lincheck on one key with `invalidateAll` (enabled now) found three more, all of one kind: a read that found the
store ahead of L1 left L1 behind it, so a later read of the same thread answered older.

- **An update writing nothing** (a compute returning the current value, `putIfAbsent` on a present key, `remove(k,
  v)` not matching) now brings L1 up to what it read: holds the value, or drops anything older if it found none.
- **`readThrough` finding no value** at a row's version drops anything older from L1 (a removal in flight here).
- **`dropOlder` drops a load in flight too**, as Caffeine's `remove` discards it after it completed: its waiters get
  its value, its owner holds it only if the key is still there.

**Step 2, the hot path, first attempt: built, measured wrong, taken out.** A compute deciding on a trusted held value (with its
basis held in L1, `InternalValue.basis`) and writing conditionally failed two ways:

- **Nothing written, nothing validated:** a decision that writes nothing (`putIfAbsent` on a present key, `remove(k,
  v)` not matching, a compute returning the current value) answered from L1 against a newer write elsewhere whose
  message had not arrived (`InternalMapTest`); only a write's version condition checks a held value.
- **A lost write:** after an `invalidateAll` elsewhere that had returned, but whose message had not arrived, a write
  decided on the held value was hidden by its old basis; "counts as before the `invalidateAll`" holds only for one
  that overlaps it, so an `asMap().put` started after it vanished (the property test's presence oracle).

What would make it valid: **the store refuses a conditional write whose basis is below the current cut-off** (a
conflict, so the writer reads and decides again), checked in the same statement where the store can (PostgreSQL and
JDBC: a condition on the control row in the `UPDATE`/`INSERT ... SELECT`; in memory), with the visibility rule as the
fallback for an `invalidateAll` committing concurrently (which overlaps, so hiding is linearizable). Then a write
decided on a held value is one operation; one deciding to write nothing still reads. MongoDB cannot condition an
update on another document: there, a read of the control document first (two operations, as the cold path) or a
transaction.

**How others do it** (sources read 2026-10-25): every near cache decides on its local value on the hot path.
Hazelcast's near-cached `compute` reads the near cache and writes `replace(key, old, new)` on the owner, which fails
after a `clear()` because that deletes; a decision writing nothing is answered from the near cache, unvalidated.
Redisson's `RLocalCachedMap.compute` takes the key's lock, reads the local cache, and writes unconditionally (a value
derived before a `DEL` of the hash survives it). Generation namespaces (memcached style), logical like the cut-off,
lose a write decided under an old generation. Taken: Hazelcast's model, its `clear` replaced by the store refusing an
old basis; a decision writing nothing is checked by a read (stricter than Hazelcast, which answers it unvalidated).

**Built 2026-10-25 (step 2, option (b)):**

- **Contract:** `Store.checksBasis()` (default `false`): `putIfUnchanged` then refuses a write whose basis is below the
  cut-off as of the same atomic step, a conflict at the version decided on. PostgreSQL: the cut-off as a CTE of the
  writing statement, the `UPDATE` and the `INSERT` conditioned on it; JDBC: `replaceRow`/`insertRow` conditioned on
  the control row's stamp (`INSERT ... SELECT ... WHERE`); in memory: in the synchronized step (switchable,
  `InMemoryDatabase.setChecksBasis`); MongoDB: not (an update cannot read another document).
- **A write committing at once with an `invalidateAll` passes the check by design, and is correct:** under `READ
  COMMITTED` the statement sees the cut-off as of its start, so an `invalidateAll` committing meanwhile is not seen;
  the write lands with its old basis and is hidden by it for every reader (Caffeine's order "the write, then
  `invalidateAll`"). That order is allowed because the two overlap: the statement started before the commit, and
  `invalidateAll` returns only after it. The check exists for the case without overlap (an `invalidateAll` that
  returned, its message not yet here), which it refuses. Closing the race too (`FOR SHARE` on the control row, or
  `SERIALIZABLE`) would give the same outcome at the cost of contention on one row. Not a residual. The writer may
  serve its value from L1 until the `invalidateAll`'s message arrives, the delay any held value has after a remote
  `invalidateAll` (after its own, the epoch has moved on and the value is checked).
- **Core:** L1 values carry the cut-off they are known visible under (`InternalValue.basis`: the read's, the
  decision's; unknown for an unconditional put, whose row's basis is no cut-off). On a checking store, `update`
  decides on a trusted held value with a known basis: a decided value write is one store operation. A decision to
  write nothing or to remove is checked by a read; the same version under the same cut-off confirms it (the
  remapping, counted by Caffeine as a load, does not run again), otherwise decided again. A conflict at the version
  decided on is a refused basis: read, never the "value too old" shortcut (which would loop on a cut-off known
  behind); loads, bulk loads and refreshes do not take such a conflict's row as a newer value.
- **One more fix from Lincheck:** a read finding no value at version V drops what L1 holds at V too, not only below
  (a row hidden by this instance's `invalidateAll`, before it reached L1, reads as a tombstone at its value's version).
- **Tests:** the contract (the basis stored as written; a checking store refuses, a non-checking one stores and
  hides), the property test with and without the check, stats (a compute on a held value: no read, one write; with
  nothing written: one read).

### Robustness fixes before the performance part (2026-10-25)

- **PostgreSQL without a clock for stamps and versions:** a sequence per table (`<table>_stamps`, created with the
  table, owned by its `stamp` column, checked at start) gives every write its stamp (`Row#stamp`, the cut-off of
  `invalidateAll` included) and a new row its first version. Every write takes one value, so a row created again
  starts above every earlier version of its key; a sequence never goes back, not after a failover either (it may
  jump ahead). The residual under "Clocks" is gone for PostgreSQL as it was for MongoDB (cluster time) and JDBC (a
  sequence). `write_time` stays for ages and expiry, which a clock serves (a step only shifts ages). The column
  defaults to the sequence, so a row written by another program or by hand gets a stamp too.
- **JDBC hands a connection back as it came:** a write switched auto-commit off and left it so; a data source that
  does not reset its connections then gave the next caller one in a transaction never ended (reads on an old
  snapshot). Restored after every write (quietly: the outcome is decided by then); a read on a connection that came
  without auto-commit ends its transaction.
- **MongoDB documents without a basis** (written before bases were stored) count as decided on nothing read in the
  maintenance filters too, as reads already took them; before, `b >= cutoff` excluded them, so after an
  `invalidateAll` they were never expired or pruned.

The full matrix, started after these and stopped after 47 of PostgreSQL's 64 topologies (MongoDB's 50 not reached),
found only tests to fix: one took version 1 for a version no row has (with the sequence a fresh table's first row
has it), one inserted a row by hand without a stamp (now defaulted), and the broken-listener tests waited for
interruption and gap counts above zero, which a cache may have from its start (now above the count before).

**The full matrix, run 2026-10-25** in six parallel groups (each sequential, as the matrix is built: MongoDB 4-5,
6-7, 8-9, PostgreSQL 9-13, 14-18, everything else; 31 minutes instead of about three hours): 19,804 tests, four
failures, all timing under the parallel load. Rerun at half the load, the groups passed but MongoDB 4.4.0's
three-node replica set, whose failover tests wait up to 60 s for a new primary:
`a_write_a_rollback_undoes_is_held_by_no_cache` failed once in two runs alone, and on the last commit before this
session three times in three, each time with no primary elected within the window (the driver's server selection
timing out): the replica set's election, not the cache, and older than this work.

### Linearizability across two instances (2026-10-25)

`AbstractTwoInstanceLinearizabilityTest`: two instances on one store, without distribution (they learn of each other
only through the store, so L1 is as stale as it gets, the hot path's hardest case; nothing runs in the background),
three threads, each operation on either instance, three keys. The operations: the ones atomic across instances (the
`asMap()` compute methods, `putIfAbsent`, `put` returning the previous value, `remove`, `remove(k, v)`, `replace`,
`replace(k, old, new)`), `put`, a read of the store (`getFromStore`), and `invalidateAll`, on several keys this time:
its cut-off is one write, atomic for the store's readers (L1 reads are not among the operations: across instances
they are eventually consistent, by design). Against one plain map. Two variants: a store checking the basis (the hot
path taken) and one not (as MongoDB). Both pass ten stress runs in a row; a mutant store claiming the check without
refusing (the hot path unprotected) fails in every run. Model checking (on request, as for one instance) passes
for both, about two minutes each.

### Performance: the baseline (2026-10-25)

`EndToEndBenchmark` (core test package; run per store by `PostgresBenchmarkExperimentTest` and
`MongoBenchmarkExperimentTest`, `-Dexperiments=true`): two instances on one store in a local container, per operation
p50/p99, the store calls it costs and the time inside store calls (every instance's), the propagation of a put to a
holder, and the throughput of 8 threads (80 % gets, 20 % puts). Local containers have almost no network latency: the
store calls per operation tell what a network adds. After the fix below (PostgreSQL 18, MongoDB 9, µs):

| Operation | PostgreSQL p50 | calls | MongoDB p50 | calls |
|---|---|---|---|---|
| `get`, held in L1 | 1 | 0 | 2 | 0 |
| `getIfPresent`, from the store | 253 | 1 | 324 | 1 |
| `get`, loaded (a miss everywhere) | 567 | 2 | 787 | 2 |
| `getAll` of 100, from the store | 635 | 1 | (16,611, see below) | 1 |
| `put` | 300 | 1 | 415 | 1 |
| `putAll` of 100 | (18,049, see below) | 1 | 45,870 | 1 (100 round trips) |
| `compute`, cold | 496 | 2 | 722 | 2 |
| `compute`, hot | 300 | 1 | 711 | 2 (no hot path) |
| `putIfAbsent`, present | 183 | 1 | 266 | 1 |
| `invalidate` | 289 | 1 | 404 | 1 |
| `invalidateAll` | 562 | 1 | 641 | 1 |
| propagation to a holder | 292 | | 412 | |
| throughput, 8 threads | 27,377 ops/s | | 22,154 ops/s | |

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

### Performance step 1: MongoDB's batches concurrently (2026-10-25)

MongoDB writes one document per call, so a batch (`put`, `putIfUnchanged`, `invalidate`, `invalidateIfUnchanged`,
and so bulk loads) cost a round trip per key, one after another. Now up to 16 calls of a batch are in flight at a
time, each its own round trip and session, on threads of their own; results in the batch's order; a failure leaves
the batch's outcome unknown if another call applied (as a failure partway did before), otherwise it is thrown as it
is. A batch of one key runs on the caller's thread as before.

Measured (MongoDB 9, single-node replica set, local container), a put of 100 keys:

| Calls in flight | On the store alone | Through the core, two instances with change streams |
|---|---|---|
| 1 (before) | 45.5 ms | 45.9 ms |
| 4 | 27.2 ms | |
| 16 (built) | 13.6 ms | 28.0 ms |
| 64 | 9.4 ms | 36.5 ms |

The scaling is sublinear (each write waits for the journal under `majority`, concurrent ones share that only in
part), and with change streams open more concurrency gets slower again: 16 is the choice. One round trip per batch
would need a bulk write, which returns no versions: a store would then have to report a write's version as unknown
(an SPI change: the core would hold such a value untrusted), or read the versions back, which a write meanwhile
makes ambiguous. Not built; decided 2026-10-26 (next section): read back with an op id, inside the adapter.

### Positioning and the complexity budget (decided 2026-10-26; the frame for every design decision)

**What makes the library unique.** Caffeine's API, on the database an application already has, with no stale value
in L1 beyond the delivery delay, even under races and failures. No other library offers that combination:

| Gap | Redisson, JetCache | Hazelcast, Infinispan | This library |
|---|---|---|---|
| Infrastructure | Redis required | a cluster of their own | the existing database (PostgreSQL, MongoDB) as L2 and feed |
| API | their own | their own | Caffeine's, as a drop-in |
| A load racing a write (stale set) | stale until the TTL | protected | protected |
| Lost messages | stale until the TTL | detected per partition | detected; L1 distrusted until checked |
| `compute` across instances | local only, or a lock | atomic (entry processor) | atomic, one store operation on the hot path |
| `invalidateAll` | racy | cluster-wide | exact, atomic per key as in Caffeine (since the decision for a sweep; before: O(1) and atomic across keys) |

A simpler design that bounds every rare race by a mandatory maximum age in L1 was considered and rejected: it would
be no better than the others.

**The complexity budget: the core's mechanisms, each tied to a guarantee that makes the library unique.**

| Mechanism | Guarantee |
|---|---|
| a version per key, counted by the store; conditional writes | atomic `compute`; no stale sets |
| versions from reads and messages; one reservation registry | no stale value in L1 beyond the delivery delay; hot keys stay cached |
| stamp and cut-off, with the basis | exact `invalidateAll` in O(1) |
| the epoch on gaps | lost messages never leave stale values |
| a fresh row's start value from the store's counter | a recreated key is never confused with its old lifetime, without a clock |
| the store guard and retries | robustness when the store fails |

Revised the same day: with one number per write (a section below), the first, third and fifth collapse into one,
which gives four mechanisms.

Revised again the same day, with the thin SPI and `invalidateAll` by a sweep:

| Mechanism | Guarantee |
|---|---|
| versions (incarnation, counter) computed by the core; compare-and-set in the store | atomic `compute`; no stale sets; a recreated key never confused with its old lifetime |
| one reservation registry, ordered within an incarnation and conservative across them | no stale value in L1 beyond the delivery delay; hot keys stay cached |
| `invalidateAll` as a sweep of tombstones with one message | exact, with Caffeine's semantics (atomic per key) |
| the epoch on gaps | lost messages never leave stale values |
| the store guard and retries | robustness when the store fails |

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

### Target design: a thin SPI, versions in the core (decided 2026-10-26, not built; supersedes the target designs below)

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
  - reads from the store (`getFromStore`);
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

| Capability | Gain |
|---|---|
| a native feed | `INVALIDATE` and `UPDATE` without polling |
| a scan by time | the polling feed, gap repair |
| server time | the cut-off from the store's clock instead of the instance's |
| a global counter (sequence, cluster time, `rowversion`) | an exact `invalidateAll`, without the wait and without an assumption about skew |
| checks the cut-off when writing | the hot path: `compute` on a value held in L1 in one operation, a decision under an older cut-off refused. Without it, `compute` reads first (two operations), as MongoDB does today |

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

| Cost | Weight |
|---|---|
| a `put` on a key not held starts a new incarnation, so its order against other writes is unknown, and L1 acts conservatively | low: a hot key is usually held, and then the `put` is conditional on the known version |
| without the global counter, `invalidateAll` waits 2ε and is exact only while the measured skew stays within ε | only on the rare operation; PostgreSQL and MongoDB declare the counter |
| without the cut-off check, `compute` reads first | as MongoDB today |
| the control row read with every batch | one more key in the same round trip |
| the refactor: versions move from the store to the core | high, once |

**Compared with the designs below.**

| | One number from the store | A thin SPI, versions in the core |
|---|---|---|
| what a store must do | count, return, filter | compare-and-set on a version it does not interpret |
| returns | mandatory (MongoDB's bulk: two round trips) | none |
| stores without a global counter | a fallback, a special case | the normal case |
| exact `invalidateAll` without an assumption | everywhere with a counter | where the counter capability exists |

The one-number design and its fallback below remain as the reasoning, and as what the global counter capability
gives.

### Design study: one number per write, the stamp as the version (recorded 2026-10-26, not built)

**The idea, as in etcd's `revision` and a database's log sequence number.** One number, assigned by the store on
every write, does every job a number does today:

| Today | With one number |
|---|---|
| a per-key version (`+1`) | the stamp, which increases per key too |
| a fresh row's start value | gone: a global counter is above every older lifetime by itself |
| the order of messages and reads in L1 | the stamp |
| `invalidateAll`'s cut-off | the control row's stamp, as now |
| the basis in the SPI | gone: the expected stamp is the basis. A decision on a row with stamp `s` stands exactly while `s > cutoff`. SQL checks that in the write's statement; MongoDB stores `s` as `b` internally, with its visibility rule unchanged |
| the feed's position, gap repair | "since stamp X", as now |
| session tokens (later) | free: the highest stamp seen |

Ruled out earlier because it narrows the stores. Under the database focus and the complexity budget that no longer
counts: the cut-off needs a global counter anyway.

**Checked on PostgreSQL 17.**

- Writer A updates a row and holds its lock. B waits for the same row. C draws from the sequence meanwhile.
- Result: A = 2, C = 4, B = 5. A waiting `UPDATE` draws again once it has the lock, so per key the stamp follows
  commit order.

**The core, with one number.**

| Mechanism | Guarantee |
|---|---|
| one number from the store per write; conditional writes on it | atomic `compute`; no stale sets; recreated keys never confused; exact `invalidateAll` in O(1) |
| one reservation registry, ordered by the number | no stale value in L1 beyond the delivery delay; hot keys stay cached |
| the epoch on gaps; repair by a scan from the last number | lost messages never leave stale values |
| the store guard and retries | robustness when the store fails |

Four mechanisms instead of the six in the complexity budget, with the same guarantees.

**Writes return their number.** There are four ways a writer can learn where its write sits in the key's order:

1. The store returns it: etcd, Spanner, Kafka, Facebook TAO.
2. It arrives later with the echo: Hazelcast. That costs an op id in every message, and in `NONE` mode the version
   stays unknown, so the hot path is lost.
3. The writer keeps nothing of its write: Redis client-side caching. That lowers the hit rate.
4. The writer decides the order: Cassandra, Lamport clocks, hybrid logical clocks. That brings clocks, or real-time
   anomalies.

A conditional write with per-key `+1` knows its new version (`expected + 1`), but only it, and that rule goes with one
number. Returning costs nothing on PostgreSQL (`RETURNING`), and nothing on MongoDB's single writes
(`findOneAndUpdate`).

**MongoDB's bulk writes stay inside the adapter: two round trips per batch, whatever its size.**

- Round trip one: one unordered bulk write of `updateOne` with `upsert: true`.
  - The filter carries the condition: `_id`, and `s: expected` for a conditional write.
  - The update sets `s: $$CLUSTER_TIME`, `b`, and a write marker `w`, random per batch.
  - The result tells each index apart: in the upserts, inserted; duplicate key `11000`, conflict; neither, updated.
- Round trip two: one read of `{s, w}` for the applied keys, values not included. Where `w` is the batch's, `s` is
  the write's number. Otherwise someone wrote after it, and it is reported as applied but overwritten, so the core
  installs nothing.
- The rare purge race (a row expected but purged meanwhile, so the upsert inserts) shows in the upserts. It is
  repaired by deleting the document if `w` is still the batch's, and reporting a conflict.
- The marker never leaves the adapter. The SPI and the core are unchanged.

**What it costs.**

| Cost | Weight |
|---|---|
| fewer stores: a global counter, set atomically in the write and returnable | already paid: the cut-off needs one |
| PostgreSQL's sequence must use `CACHE 1`, the default: with a cache per session a later write can get a lower number | new, and a hard requirement: checked at startup, as `PostgresSchema` checks the schema, and documented |
| no dense numbers per key (8, 15, 42 …) | low: the core uses neither a count of writes nor gaps per key; debugging loses a little |
| thicker adapters: MongoDB's bulk path | medium; adapter code, tested there |
| the refactor: the SPI, three adapters, the core, every test that uses versions | high, once; done under the contract, property and Lincheck tests |
| checks before building: `$$CLUSTER_TIME` increasing per MongoDB document and never repeating; MariaDB under row locks | to do |

Not a cost:

- Write speed: the number is drawn today already, and the version column goes.
- Clocks: MongoDB's cluster time is a hybrid clock that never goes back.
- Overflow: a `bigint` sequence lasts about 292,000 years at a million writes a second. MongoDB's cluster time lasts
  until 2106.

**Considered and not adopted.**

- **A version from 1, plus a server timestamp.** The clock would order the cut-off: after a failover to a standby
  whose clock is behind, rows written after an `invalidateAll` would be hidden, so writes would be lost. In
  PostgreSQL, `now()` is also the transaction's start, not its commit. The clock would also tell a key's lifetimes
  apart, which works only while no clock goes back further than the grace period. Its clock-free form is today's
  model: the start from the sequence.
- **Starting at the last version seen plus one.** Someone must remember that version after the tombstone's purge:
  - the writing instance may never have seen it;
  - keeping tombstones forever grows without bound;
  - a watermark of purged versions needs an atomic read across rows, which MongoDB cannot do.
  The cheapest form of "last seen plus one" is global: the sequence.
- **Stores without a global counter: a counter row.** One row per cache, drawn from on every write. It costs two
  round trips per write (or a transaction), and a hotspot: DynamoDB allows about 1,000 writes a second per
  partition, which would cap the whole cache. Every write must also be conditional ("only if lower than mine",
  retried on failure). Drawing blocks brings a time bound back; sharding the counter loses the global order. It
  would live inside an adapter, the core unchanged. Saved for later, at that price.

**The caveat for stores that scale out.** A global order across all writes is always a point of coordination, and
the further a store scales out, the more it costs:

| Store | Global number | Price |
|---|---|---|
| one primary (PostgreSQL, a MongoDB replica set) | sequence, cluster time | about free |
| sharded MongoDB | cluster time | cheap: a hybrid clock, exchanged anyway |
| distributed SQL (CockroachDB, YugabyteDB, Spanner) | commit timestamp, sequence | expensive (restarts, a hotspot, a round trip between regions), or only as safe as the bound on clock skew |
| key-value at scale (DynamoDB, Cassandra) | none, or a counter row | a hotspot, about 1,000 writes a second |

One number ties every write to that order. For the focus (PostgreSQL, MongoDB) that is free. Order across keys is
truly needed for two things only: the cut-off of `invalidateAll`, and a fresh row's start. Order per key is cheap
everywhere, local to a partition.

So the store's requirement is written narrowly: **a number per write that increases per key and can be compared
with the cut-off**. How an adapter gets it is its own business: a sequence, cluster time, a commit timestamp, a
counter row. The core never prescribes a global counter, and a store that scales out pays only what it must.

**The fallback for stores without a global counter (decided 2026-10-26, not built; for DynamoDB, Cassandra,
distributed SQL, when one is wanted).**

- **Per key: a counter and a random incarnation id.**
  - A fresh insert draws a random 64-bit incarnation, and the version is (incarnation, counter `+1`).
  - The same incarnation: the counter orders.
  - A different one: the order is unknown, and L1 acts conservatively:
    - a message removes what is held;
    - a reservation counts as poisoned;
    - a read from the store wins.
  - Lifetimes change rarely, so that is about free; a collision has a chance of 2^-64.
  - No clock, no global counter, and no start values.
- **`invalidateAll`: server time with a wait (as Spanner's commit wait).**
  - The cut-off is the store's time `T` plus ε, the assumed bound on clock skew, and `invalidateAll` waits 2ε before
    it returns. A row is visible if its time is after the cut-off.
  - With skew within ε:
    - every row written before is hidden, so nothing is stale;
    - every row written after the return is visible;
    - writes overlapping the call go either way, which is linearizable.
  - The errors lean the harmless way. A clock behind by more than ε hides writes just after, which is a miss, like an
    eviction. Only a clock ahead by more than ε leaves a row from before visible, which is stale.
  - Hence a generous ε: it only lengthens the rare `invalidateAll`, never a write.
  - **Measured skew.** Every heartbeat compares the store's time with the instance's, bounded by the round trip. Above
    ε/2 the instance warns; above ε, `invalidateAll` is refused (`StoreException`) instead of risking stale values.
  - A store without server time in writes (DynamoDB): the writing instance sets the time, ε covers the instances'
    clocks, and they measure their skew against each other through the heartbeat rows.
- **Cost:** writes are unchanged. `invalidateAll` is O(1) and takes 2ε, one second with CockroachDB's default bound
  of 500 ms. The assumption is the one CockroachDB and Spanner make: bounded skew, here measured.
- **Versions in write results: mandatory in the SPI. Whether the store returns them or the adapter computes them is
  the adapter's business.**
  - With a global counter, the version is the stamp, and the store returns it: `RETURNING`, `findOneAndUpdate`,
    `OUTPUT`. MongoDB's bulk writes read it back with the write marker.
  - In the fallback, the adapter mostly computes it:
    - a conditional write: `expected + 1`;
    - an insert: `(new incarnation, 1)`;
    - an unconditional single write: returned by the store (DynamoDB's `UPDATED_NEW`); Cassandra writes conditionally
      anyway;
    - a bulk `put` that cannot do `+1` (DynamoDB's `BatchWriteItem`): a new incarnation per item, known to the
      writer, at a cost in hits on keys hot through `put`.
  - Since every case delivers a version, the core needs no path for a version left unknown. So none of the
    following: op ids for echoes, parking, a special case in `NONE` mode. That keeps the core within the complexity
    budget, and it supersedes the "optional version in write results" from the target design's settled points.
- **Performance against the global counter: not a degraded mode.** Each store takes the path that is fastest for it.

  | Point | Global counter | Fallback |
  |---|---|---|
  | `invalidateAll` | O(1), returns at once | O(1), waits 2ε (one second, say) |
  | hits on keys written by bulk `put` | full, the order always known | fewer: a new incarnation per item leaves the order unknown, so L1 acts conservatively |
  | hits after a key is recreated | full | one extra miss (rare) |
  | guarantee | exact, without an assumption | exact while skew stays within ε |
  | single writes, conditional writes, inserts, reads | one round trip | one round trip, the version computed |
  | write throughput as the store scales out | every write draws from one counter, a point of coordination | nothing global: writes scale with the partitions |

  - On a single primary (PostgreSQL, MongoDB) the global counter is free and best: exact, no wait, full hits. It
    stays the main path.
  - On a store that scales out, the fallback is the faster one, since a global counter would be its bottleneck. It
    trades a slower `invalidateAll` and some hits on keys hot through `put` for writes that scale freely.
- **Drawbacks per store**, as known on 2026-10-26. Points marked "to check" are not verified yet.

  | Store | Path | Drawbacks |
  |---|---|---|
  | PostgreSQL | global counter | the sequence must keep `CACHE 1` (checked at startup); `NOTIFY` payloads are capped at 8000 bytes; one connection per instance stays open for `LISTEN` |
  | MongoDB | global counter | **bulk writes return no versions**: two round trips (bulk write, then a read of `{s, w}` with the write marker), plus classifying each index and repairing the purge race in the adapter; a write cannot check the cut-off in its filter (no read of a second document), so a decision under an older cut-off is hidden by `b` instead of refused; `$$CLUSTER_TIME` increasing and unique per document (to check) |
  | MariaDB | global counter | no `RETURNING` on `UPDATE`: `PREVIOUS VALUE FOR` in the same session, or a read back in the same transaction (row locks held a little longer); no native feed: the binlog through a replication client (`binlog_format=ROW`, replication privileges) or the polling feed; under Galera, sequences are not globally ordered; a waiting `UPDATE` drawing after its lock (to check) |
  | MySQL | global counter, emulated | no sequences: a ticket table on `AUTO_INCREMENT` (one more small insert per write, `innodb_autoinc_lock_mode=2`, tickets deleted now and then); no native feed (binlog or polling) |
  | SQL Server | global counter (`rowversion`) | the feed is Change Tracking or CDC, filled from the log, so close to polling, with a latency of seconds; Query Notifications are coarse |
  | Oracle | global counter | under RAC a sequence keeps its order only with `ORDER`, which coordinates across nodes; the feed (Continuous Query Notification, Advanced Queuing) depends on edition and licence (to check) |
  | CockroachDB | global counter (commit timestamp) or fallback | `cluster_logical_timestamp()` pins the transaction's timestamp, so more restarts under contention, and only under `SERIALIZABLE`; it must be packed into 64 bits (assumes a logical part below 1000); a sequence is a hotspot of one range; changefeeds need `kv.rangefeed.enabled` and a licence check |
  | DynamoDB | fallback | no server time in writes, so ε must cover the instances' clocks; `BatchWriteItem` cannot `+1`, so bulk `put`s take a new incarnation per item and lose hits on keys hot through `put`; no `updateAll`; Streams keep 24 hours (a longer outage is a gap) |
  | Cassandra | fallback | `+1` only through lightweight transactions (Paxos, several round trips), so every write is a slow conditional one; no usable feed (CDC writes files per node), and a scan by time needs an index of its own; `TRUNCATE` removes without tombstones |

  | Redis | global counter on one node, or with all keys of a cache in one slot (a hash tag); fallback in cluster mode | a Lua script does `INCR` of a counter, the row and the message atomically, and returns the number. In cluster mode a script reaches keys of one slot only, so a global counter means one shard (no scaling out), otherwise the fallback (per key, with `TIME` for the cut-off). Asynchronous replication and AOF `everysec` can lose the last writes on a failover or crash, **and the counter can go back**, a hard risk for every order: a change of `run_id` or of the primary must count as a gap (the epoch), and the counter is raised past anything handed out. Keyspace notifications carry no version and are fire-and-forget, so the script publishes its own message (`PUBLISH`, `SPUBLISH` in a cluster), or appends to a stream (`XADD`: ordered and resumable by ID, so gaps are repaired, but it must be trimmed). A scan in stamp order needs an index of its own (a sorted set) or the stream |

  - **Stores like Redis** (from what is known, not checked in detail). What decides: atomic scripts, whether the
    counter can go back after a failover, and a feed.
    - Valkey, ElastiCache, Azure Cache for Redis: as Redis.
    - **AWS MemoryDB:** the best fit. Every write is committed to a durable log across zones before it is
      acknowledged, so the counter never goes back: as safe as PostgreSQL's sequence, with Redis's API.
    - Dragonfly: multi-threaded on one node, so a global counter needs no cluster and scales up far. Lua needs its
      keys declared. Replication is asynchronous, so failovers must be detected.
    - Apache Kvrocks (on RocksDB, on disk): durable on disk, Lua, pub/sub and streams; replication asynchronous.
      Suits large values or scarce memory.
    - Garnet: Lua in newer versions (to check); young.
    - Upstash: Lua over HTTP, so every call is an HTTP request; slow for a cache.
    - **Several writable primaries** (KeyDB's active replication, Redis Enterprise Active-Active): two primaries can
      hand out the same counter value, or merge rows by their own rules (CRDTs, not last write wins on a version).
      Only the fallback, with care, or without active replication.
  - Common to the fallback: `invalidateAll` waits 2ε, and is exact only while the measured skew stays within ε.
- **SPI:** a store declares which it has: a global counter (exact, no assumption), or time with a bound on skew (the
  wait and the skew check on). PostgreSQL and MongoDB keep the global counter: free there, and exact without any
  assumption.
- **Considered for these stores and not adopted:**
  - a sweep of tombstones (O(n), and not atomic across keys);
  - a generation checked atomically on every write (a transaction per write);
  - a generation in the key alone (not exact);
  - a generation acknowledged by every instance (exact and free for writes, but it needs membership with leases).

### Versions known to the writer instead of reported by the store: not adopted (decided 2026-10-26)

The question: must a store report the version it wrote, or is it enough that it increments the version and assigns
the stamp, with the writer working the rest out? Worked through and decided against, as a redesign of the contract;
the gain is reachable inside an adapter.

**What the alternative would be.**

- The store only increments and checks conditions, and reports per write applied, conflict or inserted, without a
  version.
- The writer computes the versions itself:
  - a conditional write gives `expected + 1`;
  - a fresh row, where none exists (not over a tombstone), gets a version made of an incarnation and a counter
    (`(incarnation, 1)`), with the incarnation taken from the writer's clock (a hybrid logical clock), packed into
    one long (milliseconds × 2^20 plus the counter).
- An unconditional `put` or `remove` (`old + 1`, last write wins) would be held in L1 with its version unknown, and
  dropped on the next message for the key until the writer's own echo, or a read, gives the version.
- MongoDB batches in one bulk write:
  - `updateOne` with the condition in the filter and `upsert: true`, unordered;
  - a conflict is then a duplicate-key error at its index (MongoDB retries such an upsert itself only when the filter
    is the unique key alone);
  - an insert shows in the upserts, an update in neither;
  - the rare case "a row expected, but purged meanwhile" inserts, which shows in the upserts and would be repaired
    after.

**What stays necessary either way.**

- "Last write wins, highest version wins" orders by arrival, not by decision. So the conditions stay:
  - a load must lose against a removal that came after its read (the tombstone gives it something to conflict
    with; the condition makes it lose);
  - `compute` across instances needs a compare-and-set, or it loses updates.
- A timestamp never orders, because a clock can go back after a failover. Order comes from counters: the per-key
  version, and the sequence or cluster time for the stamp.
- A fresh row starts above every version its key ever had (from the store, as now). Starting at 1 would be safe
  only if every L1 copy expired before the tombstone's grace period ended, a timing assumption that a GC pause or a
  stalled feed breaks.

**Pros.**

- MongoDB batches in one round trip, instead of 16 calls in flight. Single operations cost the same either way:
  `findOneAndUpdate` returns the document at no extra cost.
- Stores without `RETURNING` (MariaDB's `UPDATE`). Low priority while per-SQL adapters are saved for later.
- A smaller SPI for adapter authors.

**Cons, which decided it.**

- Correctness would depend on clocks for the first time: it would require no clock jump larger than the grace
  period. That assumption is weak, but it is a new class of failure. Today correctness depends on no clock.
- The change lands in the Lincheck-verified core where it is most delicate: "version unknown" in the write
  registry, where eight races were found.
- Unconditional writes would be held with their version unknown: fewer L1 hits under contention, more cases.
- A new version format in every adapter and every message.
- PostgreSQL gains nothing: `RETURNING` is free.

**Decided.**

- The contract stays: a store reports the version it wrote, and correctness stays free of clocks.
- How an adapter learns the version is its own business. MongoDB's batches can be done in two round trips whatever
  their size, without changing the SPI or the core:
  - conditional writes: the bulk write above, applied, conflict or inserted per index, then `expected + 1` for the
    updated rows;
  - unconditional writes: a bulk write that also stores an op id;
  - then one read of `{v, op}` for the batch's keys (and the inserted rows' versions). Where the op is the
    writer's, the version is its own. Where it is not, someone wrote after it, and the write is reported as
    overwritten, which the core drops as it already does.
  - The op id is what keeps a stranger's version from being taken as one's own.
- The op id is worth having on every store on its own, as a robustness feature: a write whose outcome is unknown is
  then resolved by reading the row (the op is the writer's: it happened), instead of only dropping the key from L1.
  That makes even a `put` safe to retry.
- Reconsider the redesign only if a store comes that can neither return nor derive its versions. So far every
  candidate can (Cassandra's lightweight transactions report applied, DynamoDB's `UpdateItem` returns values, Redis
  scripts return anything).
- Next, in this order:
  1. the op id;
  2. the Docker stall confirmed on native databases;
  3. MongoDB's bulk writes as above, measured against the native numbers.

### Design study: L1 by invalidation and reservation instead of version order (recorded 2026-10-26, not built)

The goal: a simpler core for L1 that is not weaker, following what others do in production. The store side stays
as it is: versions as compare-and-set tokens, stamps, the cut-off and the basis.

**What others do.**

| System | L1 kept current by | Loads racing writes | Lost messages |
|---|---|---|---|
| Redis client-side caching | invalidations from the server, no versions | a "load in progress" placeholder; an invalidation removes it; the reply is stored only if it is still there | clear L1 on reconnect |
| Hazelcast Near Cache | invalidations with a sequence per partition | a reservation with an id; installed only if still one's own | gaps in the sequence detected; a repair task clears |
| Facebook memcache | deletes | a lease per miss; a set counts only with a valid lease; a delete revokes it | (no L1) |
| Redisson local cached map, JetCache | pub/sub invalidation or update, no versions | none (accepted) | clear, or replay a log of 10 minutes |

None of them order versions inside L1. They protect a load by a reservation, and treat a gap by clearing.

**Today, for comparison.**

- L1 keeps the newer of two versions and ignores an older message.
- That needs ordering rules wherever L1 changes:
  - `writeMinimums` and `writing`, the minimum check in `install`;
  - `dropOlder`, including a load in flight, and at the version itself;
  - catch-up when a read finds the store ahead.
- Most of the eight races Lincheck found were in exactly these rules.

**The mechanism.**

1. **A held value keeps its version as a token, not as an order.** A message `(key, v)` leaves a held value in place
   only if its version equals `v`: it announces the very state L1 holds, as one's own echo or a late echo of the row
   just read. Any other `v`, older or newer, removes it. Versions are unique per key, so equality identifies a
   state; no comparison by size anywhere in L1.
2. **Every operation that will install takes a reservation first**, before its read or write is sent: a load after a
   miss, a refresh, a write, `compute`. Reservations live in the per-key registry that exists today (`writing`), not
   in Caffeine, so views, sizes, stats and listeners never see them.
3. **A message for a reserved key poisons the reservation.** So does a local `invalidate`, an `invalidateAll` (the
   epoch), or **a second reservation of the same key**.
4. **An operation installs only if its reservation is unpoisoned, under the registry's lock.** Otherwise it installs
   nothing, and the next read goes to the store.
5. **A gap moves the epoch on, as today.** Revalidation compares the store's version with the held one, for equality
   again.

**Why it is safe.**

- Every message arrives after its commit: `NOTIFY` at commit, change streams, and the polling feed's scan.
- The reservation is in place before the read or write is sent. So every write that commits after it reaches the
  instance as a message, and poisons the reservation or removes the installed value.
- A write that committed before is seen by the read. Its late message only removes a value of another version: an
  extra miss, never a stale value.
- Two operations on one key in one instance poison each other: neither installs.
  - Without that rule, the operation whose write committed first could install last, a stale value until the next
    message, or for good in `NONE` mode.
  - This is the one subtle case; Lincheck must cover it.
- The hot path is unchanged:
  - a trusted held value is unpoisoned, and has seen no gap and no message for another version;
  - the decision writes conditionally on its version, and a wrong one conflicts.

**What it removes.**

- The minimums (a flag instead of a version per key), `dropOlder`, catch-up, `tooOld`, and the ordering branches
  in `install` and the message handling.
- The registry stays, but it holds a flag. One rule replaces several: "anything about this key while I was busy
  means I install nothing."

**What it costs.**

- **Extra misses where today's design keeps a value.**
  - A late message of an older version removes a newer held value; today it is ignored.
  - Two concurrent operations on one key in one instance install nothing.
  - Both happen only under contention on the same key. The benchmark has to show how often.
- **`UPDATE` mode becomes "remove, then load again"** under a fresh reservation. That is how it works already, in
  effect.
- **A rebuild of the most delicate part of the core**, verified against the same tests: the contract, the property
  test, Lincheck on one and on two instances, the full matrix.

**Decision points before building.**

- Accept the extra misses under contention, as measured, or not.
- On poisoning by a second reservation: "neither installs" (simple, safe), or "the later one installs if its write
  committed later". The second option needs version order again, and so is not adopted.
- Build it as a variant alongside the current core and compare (more code for a while), or replace in place (git
  as the fallback).

### Further alternatives (recorded 2026-10-26, not built)

The alternatives so far:

1. **Versions known to the writer.** Not adopted as a redesign. MongoDB's bulk writes go inside the adapter
   instead.
2. **L1 by invalidation and reservation.** The study above.

Three more make sense; three others were considered and do not fit.

**3. Revalidation by version (as HTTP's ETag): staleness bounded without trusting the feed.**

- L1 serves a value. When the value is older than a time T, the next access checks it in the background, with a
  read of versions only (`VersionsResult` exists). An equal version keeps the value; another one loads again.
- This is Caffeine's `refreshAfterWrite`, but a few bytes instead of a load.
- Robustness:
  - staleness is bounded by T even after a message lost without notice;
  - today only gap detection and the heartbeat cover that case.
- Reach: it works on every store, with a feed or without. SQL stores without a native feed (MariaDB, …) would get
  distribution without the polling feed's scan.
- Performance: checks are batched and asynchronous; the caller never waits for one.
- Best as a safety net beside 2: the feed makes changes arrive fast, revalidation guarantees the bound. It could
  also stand alone, for `NONE` mode with a bounded staleness.

**4. Writes never install in L1 (cache-aside, delete on write).**

- A write only removes its key from L1; only reads install, under a reservation.
- It is the simplest form of 2: no installs from writes, and no need to recognise one's own echo.
- Cost: the next read after a write goes to the store. Bad for write-then-read workloads, harmless for read-heavy
  ones.
- An option, or the first step of 2, extended to installs from writes once measured.

**5. Leases in the store (Facebook memcache).**

- A miss takes a lease from the store, in one operation with its read. A write revokes the lease, and a load writes
  only with a valid one.
- Gain: a stampede across instances is avoided: one loads, the others wait. Today Caffeine prevents it only within
  an instance.
- Cost: a write on every miss, and lease logic in every adapter.
- Only if stampedes across instances show up in measurements; not a core design.

**Considered and not a fit.**

| Design | Why not |
|---|---|
| One ordered log as the only truth (change data capture, e.g. PostgreSQL logical replication: apply up to a position, read one's own writes by waiting for one's position) | the strongest and clearest semantics, but heavy to run (replication slots, `wal_level=logical`), and one's own writes wait for the log: not simple |
| Ownership per key (Hazelcast, Infinispan: consistent hashing, writes routed to the owner) | needs cluster membership among the instances, a second system beside the store: the store is meant to be the only coordinator |
| Order by timestamp (last write wins as in Cassandra) | correctness would depend on clocks: ruled out in alternative 1 |

**How they combine.**

| Design | Robustness | Performance | Simplicity |
|---|---|---|---|
| 2 | high | extra misses under contention | simpler L1 |
| 2 + 3 | highest: bounded staleness even after a lost message | plus small asynchronous checks | simple, plus one rule |
| 2 with 4's rule | high | a read after each write | simplest |

Recommended target: 2 + 3 (superseded the same day by the target design below).

- 3 is replaced there by the polling feed as the safety net: it is a pull as well, already built, and bounded
  without depending on access.
- 4's rule is an option for read-heavy workloads, or 2's first step.
- 5 only if measured.
- 1 stays inside the MongoDB adapter.

### Target design: reservations in L1, versions read but never returned by writes (recorded 2026-10-26, not built)

**Status after the complexity budget (same day).**

- Of this target, only the one reservation registry and versions read from the store are kept, as a simplification
  of today's core.
- Writes keep returning their version.
- Op ids and writes without versions are for later, only if MongoDB's bulk writes are measured worth it.
- See "Positioning and the complexity budget".

The rest is kept for the reasoning.

**The final target (revised the same day; it supersedes "the design" below where they differ).** It has four parts:

- **Reservations in L1**, as in alternative 2.
- **Versions from reads and feeds.** The store keeps a per-key version (`old + 1`; a fresh row starts at a value from
  the store). Reads and messages carry it.
- **Op ids** for recognising one's own unconditional writes, and for resolving unknown outcomes.
- **Writes that return only applied or conflict.**

| Path | Version known? | Rule in L1 |
|---|---|---|
| a read or reload under a reservation | yes, the read returns it | install if its version is at least the highest version announced during the reservation, and at least what L1 holds |
| a message | yes, it carries it | at or below what is held: ignore; newer: remove (`INVALIDATE`) or reload under a reservation (`UPDATE`) |
| a conditional write | yes: `expected + 1`, computed by the writer | as a read |
| an unconditional `put` or `remove` | no | install only if no message came during the reservation; its echo, recognised by op id, fills in the version |

Why not op ids alone (the design below):

- Without order, a reload under a reservation is cancelled by every message during it. A key written by others
  faster than one round trip would then never be cached.
- Today's code avoids that by version order. So does this target: the newest read installs at once.
- Parking a result until its own echo arrives would also work, with commit order instead of versions. But it relies
  on the feed delivering in commit order, which the polling feed's re-scan breaks, and it needs a timeout.
- Versions read from the store need neither.

Compared with today:

- Order is kept in two places only: the install check and the handling of messages.
- Gone are versions in write results, catch-up, and the minimum rules spread across the core.
- Concurrent operations in one instance are decided by version, so the newer one installs.

Left out on purpose: one's own unconditional `put` or `remove` while others write the same key.

- The case: its version is unknown, so a message during its reservation stops the install, and the next read loads
  the key. One miss, only under contention on that key.
- The possible fix: park that write's value, and install it when its echo arrives (recognised by op id) if the
  echo's version is at least the highest one seen meanwhile. The echo carries the version, so this works with the
  polling feed too, and does not rely on commit order.
- Built only if the benchmark shows the miss matters.

**The three points to settle before switching (settled 2026-10-26).**

1. **Fresh rows: the version after an insert.**
   - A fresh row starts at a value from the store. In PostgreSQL that is `nextval` of the stamp sequence. It is
     above the old lifetime's last version because every write draws a stamp from the same sequence.
   - The writer cannot know that value without a return. Drawing blocks of values ahead (hi/lo) would break the
     guarantee: a block drawn long ago can lie below versions written since.
   - Without it, a load (the most common install) would be held with its version unknown. In `NONE` mode no echo
     fills the version in, so `compute` on a loaded key would always read first: the one-operation hot path lost.
   - Superseded the same day: the version is mandatory, returned by the store or computed by the adapter. See the
     one-number design's fallback.
   - Settled: the version in a write's result is **optional** per entry.
     - Applied with the version, or applied without it, or conflict.
     - PostgreSQL returns it (`RETURNING`, free). So does a conditional replace anywhere (`expected + 1`, computed by
       the adapter).
     - MongoDB's bulk writes and MariaDB's `UPDATE` do not. For those, the core holds the value with its version
       unknown: a message with the writer's op id fills the version in, any other message removes the value, and
       the hot path reads first.
2. **Reservations beside Caffeine's loads.**
   - Caffeine's future for a load in flight stays: it de-duplicates concurrent loads and serves the waiters, and
     replacing it would rebuild what Caffeine does well.
   - Today there are two registries for the same job:
     - `minimumVersions` for loads, checked after a load completed (`checkMinimumVersion`);
     - `writing` with `writeMinimums` for writes, checked in `install`.
   - Settled: **one registry, a reservation per key**, holding a count of operations in flight and the highest
     version announced meanwhile.
     - A load, a reload and a write each reserve before they read or write.
     - On completion: a load's future is compare-and-removed from Caffeine if older than the reservation's highest
       version, as today; a write installs under the registry's lock only if not older.
     - A gap or `invalidateAll` is the epoch, as today.
   - Consequence, and a finding: with versions read from the store, the target is **today's core simplified**, not
     a rebuild.
     - The rules for loads and writes merge into one.
     - Versions leave the write results (where unknown).
     - The scattered "drop older" cases become one check at release.
     - The switch can be done step by step, under the existing tests.
3. **The polling feed: scan by stamp, not by store time.**
   - Today it reads the changes "since the last poll plus an overlap", measured by `write_time`, the store's clock.
     A clock stepping back further than the overlap (a failover to a standby whose clock is behind) loses changes
     without notice.
   - Settled: scan by **stamp**.
     - A stamp is drawn before its commit, so a late commit can carry a stamp below the position already scanned.
     - The overlap is therefore kept in stamps, but its width is measured on the **scanner's own monotonic clock**
       (`System.nanoTime`): re-scan from the highest stamp seen at least the overlap ago.
     - That assumes a write commits within the overlap after drawing its stamp. A write slower than that is already
       handled: the writing instance touches it again (rewrites it as it is, so a later poll finds it), as
       `InternalPollingStore` does today for writes slower than the overlap or with an unknown outcome. The overlap
       is measured on local clocks only, never on a wall clock and never on the store's.
   - The index it needs, `(discriminator, stamp)`:
     - Measured on PostgreSQL 17, 200,000 rows, single-row updates inside the server (no round trip, no commit):
       10.2–10.7 µs without, 10.7–11.7 µs with, so about 1 µs more per write.
     - Against a write's round trip and commit (milliseconds) that is negligible. HOT updates were impossible already,
       as `write_time` in the maintenance index changes on every write.
     - MongoDB and JDBC need the same index. MongoDB's scan is only the safety net and gap repair; its feed is
       native.

Store-first-then-L1 costs in every design:

- In `INVALIDATE` mode, a write by another instance means one miss.
- In `UPDATE` mode, it means one reload per instance holding the key.
- Avoiding either would need values in messages; not adopted.

The rest of this section is the step before, kept for the reasoning: the scan, the safety net, and the costs still
apply.

**The goals.**

- Fast writes and fast reads, in batches too: one round trip per batch.
- A small, general SPI.
- A core that is not too complex to maintain.

**What brought it together.**

- Alternative 2 never orders by version; it only asks whether two things are the same state.
- An id the writer chooses itself answers that as well, and is known without a return.
- Alternative 1 needed clocks only for order: a fresh row starting above everything before. Without order, that
  need disappears.

**The design.**

| Part | What it is |
|---|---|
| Row | key, value, state, **op id**, **stamp**, basis |
| Op id | chosen by the writer, unique, never reused: 128 bits, an instance id drawn from a store sequence at start plus a local counter. It replaces the per-key version everywhere. |
| Stamp | assigned by the store, ordered across keys (a sequence, cluster time). It orders `invalidateAll`'s cut-off and the scan, as now. |
| Writes | conditional (`WHERE op = :expected`, or absent) or unconditional; return per entry **applied or conflict**, nothing else |
| L1 | alternative 2: reservations in the per-key registry, with equality of op ids instead of version order |

**What uses equality of op ids.**

- Recognising an echo: a message for exactly the state held.
- The hot path's compare-and-set.
- A load losing against a removal: the tombstone has an op id of its own.
- Resolving an unknown outcome: read the row; the op is the writer's, so the write applied.

**What it gives.**

- MongoDB: one bulk write per batch, a conflict being a duplicate-key error at its index. One round trip, no read
  back.
- JDBC: `executeBatch`, with an update count per entry; no `RETURNING`.
- PostgreSQL: one statement, as now.
- No version start values, no clocks, no version in write results.

**What it costs.**

- 16 bytes per row and per message instead of 8.
- MongoDB's race "a row expected, but purged meanwhile" inserts; it shows in the upserts and is repaired after.
- A rebuild of the store contract, the adapters, the core's L1 part, and every test that uses versions: the
  contract, the property test, the Lincheck models, the in-memory store. The public `StoredValue` shows no version,
  so users see no change.

**The SPI.** Every store does:

1. read keys: rows and the cut-off;
2. write entries: per entry, applied or conflict;
3. `invalidateAll`: the control row;
4. **scan**: the changes after a position, in store order.

A native feed is optional.

**The scan, and what is mandatory.** The scan returns the rows changed after a position (key, op id, state, stamp),
in order and limited. It reads again over a window of late commits, because a stamp or write time is assigned
before the commit and can commit out of order. That is what the polling feed does today, by write time on its
maintenance index.

| Use | Needed for correctness? |
|---|---|
| Distribution for a store without a native feed (the polling feed) | yes, there: it is that store's only feed |
| Repairing a gap: scan since the last position seen, instead of distrusting all of L1 | no: the epoch is correct without it, only more expensive (every held value checked on its next read) |
| A periodic scan beside a native feed, as a safety net | no: a bonus |

**Why the safety net is only a bonus.**

- Correctness needs every lost message to be noticed, not to be replayed.
- The native feeds report their losses:
  - a broken `LISTEN` connection, which reconnects;
  - a change stream whose resume point is gone from the oplog;
  - a full `NOTIFY` queue fails the writer's transaction, so nothing is lost silently;
  - a feed that falls silent is caught by the heartbeat.
- Every reported loss moves the epoch on.
- So the safety net only guards against a loss nobody reports: a bug in a driver, or in our own code. That is
  defence in depth, off by default.

**Why mandatory anyway.**

- It makes every distribution mode possible on every store.
- A gap is then repaired by scanning, not by distrusting everything.
- Its price: an index for the scan, kept on every write. PostgreSQL, MongoDB and JDBC have one on write time for
  maintenance already, so no extra index is needed.

### Costs of the cut-off, and optimisation candidates (recorded 2026-10-24, after step 2)

What the cut-off costs as built, per operation, against before (not measured yet as built; the measurements under
"`invalidateAll`, logical" were of the statements alone):

| Where | Cost | Why |
|---|---|---|
| every conditional write of a value (load, refresh, compute methods, `putIfAbsent`, bulk loads once per batch) | one read of no keys after it | the check against the cut-off |
| `compute`, `computeIfPresent`, `merge` with the key held trusted in L1 | one read before it | the decision must carry the cut-off of a read |
| a refresh | one read of no keys before the source | the same, for the held value it starts from |
| a load starting over after a conflict | one read | a conflict's row carries no cut-off |
| a conditional write with its outcome unknown, after an `invalidateAll` | one read and possibly an undo | the repair by value |
| `invalidateAll` | two control writes, two `INVALIDATED_ALL` messages, two clears of L1 per instance | the commit window |
| every read | the cut-off with it: PostgreSQL one primary-key subselect, MongoDB the control document already | the visibility filter |
| maintenance after an `invalidateAll` | deleting the hidden rows, in batches over its runs | no sweep |
| MongoDB documents | one more field (`s`, 12 bytes) | the stamp |

**Superseded (2026-10-25):** the decision's cut-off on every row removed the check after every conditional write,
its undo and repair, and the second control write; candidate 3 below became the hot path. The table above and the
list are history; the performance part starts from a new baseline ("Performance: the baseline").

**Candidates, by expected gain** (none built; each to be measured before and after):

1. **Fold the check into PostgreSQL's conditional write:** `AND (SELECT write_time FROM ... control FOR SHARE) = ?`
   in the statement; saves the read after every conditional write there. The share lock makes it exact (the write
   waits for a running `invalidateAll`).
2. **Let a conflict report the cut-off:** `WriteResult` of a conflict, or `putIfUnchanged`'s result as a whole,
   carrying `cutoff` (both adapters read it in the same round trip they read the conflict's row in); saves
   the read of a load starting over. An SPI addition, not a condition: an adapter not reporting it costs the read.
3. **Give a held value the cut-off of the read it came from:** values loaded or read from the store would carry it,
   so the compute methods could decide on them without a read; values this instance wrote itself would not (their
   cut-off is unknown) and are read as now. Saves the read before most compute calls on held keys.
4. **Skip the check where no `invalidateAll` can have happened since the read:** only provable with a feed that is up
   and delivered nothing of the kind meanwhile, and only if messages could not overtake each other across keys,
   which the contract allows; probably not worth its rules.
5. **Announce only the second control write:** halves the `INVALIDATED_ALL` messages and L1 clears; needs the store
   to tell the two writes apart (a parameter of `invalidateAll`), an SPI change for a rare operation.
6. **MongoDB: the check folded into a transaction** (read the control document and write in one multi-document
   transaction): exact, but a transaction per conditional write costs more than the read it saves; not a candidate
   unless measured otherwise.

### What the property test needs (worked through 2026-10-24)

The property test (several instances, writers and loaders concurrently, faults, two oracles per phase) covers the
race of loads with `invalidateAll` already: writers change the source and then invalidate everything, loaders run
without locks, and the **freshness** oracle (the store holds no value or the source's current one) finds an old
token a racing load left. What it cannot see today, and what it needs for the study:

1. **A third oracle, presence.** Freshness allows "no value", so a filter hiding too much, or a put after an
   `invalidateAll` lost to a clock (the residual), passes it. In this model writes and `invalidateAll` never overlap
   (`invalidateAll` takes every key's lock), so: a key whose source value was last set by a confirmed write (put,
   `putAll`, compute, replace), with no invalidation of it since, must be in the store with exactly that value.
2. **The commit window, as a fault of the in-memory store.** Its writes take their time and become visible in one
   step under its monitor, so the window between the two, which the two-write `invalidateAll` closes, never opens. A
   fault "commit delay" (take the time, wait, then make the write visible) opens it, for the control key and for any
   write.
3. **The racing conditional writes:** in this model compute methods hold their key's lock, which `invalidateAll` takes
   as well, so they never race it. Their outcome is consistent with the source anyway, so freshness would not see the
   race either: `f(old)` surviving is a fault only by Caffeine's per-key semantics. **Deterministic tests instead,**
   one per row of the table under "Conditional writes racing an `invalidateAll`": a fault that pauses an operation
   between its read and its write, an `invalidateAll` in that pause, then the expected outcome (absent, or the
   operation as on an absent key).
4. **Size pruning and the delete of hidden rows:** the test runs with a maximum size of 1,000 for 8 keys and an
   interval far above its duration, so neither ever runs. A variant with a small maximum size and maintenance every
   few milliseconds exercises silent deletes and the recreated keys' start value; convergence finds a holder whose
   copy a recreated key failed to replace.
5. **The polling feed as a backend:** the in-memory store with the generic poller instead of its own delivery,
   with the commit-delay fault above and below the overlap, so that the writers' repair of slow writes is
   exercised, and collapsed messages (only the latest per key) throughout.
6. **No clock regression in the property test:** the residuals would make it fail by design. Their bounds get tests
   of their own on the in-memory store's injectable clock (the writer recognising a hidden put when it knows T; a
   recreated key below a copy served until the maximum age).
7. **Mutation check,** as for the current `invalidateAll` (seven mutants then): each of these must make a test fail:
   a single-write `invalidateAll` (with the commit delay); no check after a conditional write; a check without the
   undo; the filter hiding `< T` instead of `<= T`; a recreated key starting at 1; polling without overlap; expiry
   keeping its old write time (with polling).

**Cost:** the in-memory store gains two faults (commit delay, a pause between read and write), the property test an
oracle, two variants and a backend; about seven deterministic tests. Runtime grows by the variants (a few seconds
each with the in-memory store). Nothing on the store contract.

### Open in the study

Nothing left; the next step is the decision whether to build it, and in which order.

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
