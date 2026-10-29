/*
 * Copyright © 2023-2026 Dr. Andreas Oberhoff (All rights reserved)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.oberhoff.distributedcaffeine.testing;

import java.time.Duration;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

/**
 * How the in-memory store behaves, one parameter per behaviour found in a real store (VISION.md, "In-memory store
 * profiles"), so that the core is tested against every kind of store in seconds. The whole in-memory suite runs under
 * the profile named by the system property {@code inmemory.profile} ({@link #ofProperty}); a test needing a particular
 * behaviour passes its profile to the database ({@link #with}).
 * <p>
 * A behaviour that is a property (how versions are chosen, how reads see the rows) is applied always; one that is an
 * event (a failover, an eviction, a lost message) is a fault the tests trigger on the database, and the profile says
 * only whether the store can produce it, so that the randomized tests inject it where it belongs.
 *
 * @param name                 the name, for reports
 * @param versioning           how the store applies writes and chooses their versions (dimension "versioning")
 * @param changes              how the instances learn about changes (dimension 0)
 * @param clock                how the store's clock counts (dimension 1)
 * @param clockStepBack        how far the store's clock may step back, declared in its clock allowance (dimension 1);
 *                             the randomized tests step it back by up to this
 * @param snapshotReads        whether a read of several rows sees them at one instant (dimension 2)
 * @param batch                how a write of several rows applies (dimension 3)
 * @param losesWritesOnFailover whether a failover can lose acknowledged writes (dimension 4)
 * @param tombstoneOnPurgedRow whether a write conditioned on a version of a row purged meanwhile leaves a tombstone
 *                             (dimension 5)
 * @param evicts               whether the store can delete rows silently (dimension 6)
 * @param lossyFeed            whether the feed can lose messages without reporting it (dimension 7)
 */
public record StoreProfile(String name, Versioning versioning, Changes changes, Clock clock, Duration clockStepBack,
                           boolean snapshotReads, Batch batch, boolean losesWritesOnFailover,
                           boolean tombstoneOnPurgedRow, boolean evicts, boolean lossyFeed) {

    /**
     * How a store applies a write and chooses its version (VISION.md, "Target design: versions applied as well as each
     * store can").
     */
    public enum Versioning {
        /**
         * At the proposed version, only above the stored one (an advance) or under its condition; refused otherwise.
         * Generic JDBC, DynamoDB batches.
         */
        CONDITIONAL,
        /**
         * Every write applied by assigning, in a domain of the store's own: a new row at the next value of a sequence
         * drawn by every write, an existing one at its version plus one; the proposal ignored. Conditions on a missing
         * row or a version still decide whether it applies. PostgreSQL (before step 2, and as planned).
         */
        ASSIGNING_OWN,
        /**
         * Every write applied by assigning, in the core's domain: the maximum of the stored version plus one, the
         * proposal, and the store's time in the core's format. Redis / Valkey with a Lua script.
         */
        ASSIGNING_SHARED,
        /**
         * A single write assigns as {@link #ASSIGNING_SHARED}, a batch is conditional. MongoDB ({@code
         * findOneAndUpdate} for one document, the bulk write for several).
         */
        MIXED
    }

    /**
     * How the instances learn about changes (dimension 0).
     */
    public enum Changes {
        /**
         * A feed of the store's own: PostgreSQL's LISTEN/NOTIFY, MongoDB's change streams, Redis Streams.
         */
        FEED,
        /**
         * A scan only, polled by the core ({@code InternalPollingStore}): every store without a feed, generic JDBC.
         */
        SCAN,
        /**
         * Neither: only {@code DistributionMode.NONE} works. A plain key-value store.
         */
        NONE
    }

    /**
     * How the store's clock counts the write times and ages (dimension 1).
     */
    public enum Clock {
        /**
         * Strictly increasing, never repeating: a hybrid logical clock or a commit timestamp (CockroachDB, Spanner).
         * The core needs no allowance.
         */
        MONOTONIC,
        /**
         * A wall clock in microseconds, repeats allowed: PostgreSQL, MySQL, Redis ({@code TIME}).
         */
        MICROS,
        /**
         * A wall clock in milliseconds, repeats allowed: MongoDB ({@code $$NOW}), Firestore.
         */
        MILLIS;

        /**
         * Returns the unit the clock counts in.
         *
         * @return the resolution, zero for a monotonic clock
         */
        public Duration resolution() {
            return switch (this) {
                case MONOTONIC -> Duration.ZERO;
                case MICROS -> Duration.ofNanos(1_000);
                case MILLIS -> Duration.ofMillis(1);
            };
        }
    }

    /**
     * How a write of several rows applies (dimension 3).
     */
    public enum Batch {
        /**
         * All or nothing: one statement (PostgreSQL), one script (Redis).
         */
        ATOMIC,
        /**
         * Row by row: a failure can leave part of the batch applied (MongoDB's unordered bulk write, the primitive
         * JDBC store).
         */
        PER_ROW
    }

    /**
     * Generic JDBC: conditional writes at the proposed versions, polled through its scan.
     */
    public static final StoreProfile JDBC = new StoreProfile("jdbc", Versioning.CONDITIONAL, Changes.SCAN,
            Clock.MICROS, Duration.ZERO, true, Batch.PER_ROW, false, false, false, false);

    /**
     * PostgreSQL: versions of its own, assigned on every write, a feed in commit order.
     */
    public static final StoreProfile POSTGRESQL = new StoreProfile("postgresql", Versioning.ASSIGNING_OWN,
            Changes.FEED, Clock.MICROS, Duration.ZERO, true, Batch.ATOMIC, false, false, false, false);

    /**
     * MongoDB: single writes assigned in the core's domain, batches conditional and per document, reads document by
     * document, a clock in milliseconds; a failover with {@code w:1} can lose writes, and a write on a purged row
     * leaves a tombstone.
     */
    public static final StoreProfile MONGODB = new StoreProfile("mongodb", Versioning.MIXED, Changes.FEED,
            Clock.MILLIS, Duration.ZERO, false, Batch.PER_ROW, true, true, false, false);

    /**
     * Redis / Valkey: every write assigned in the core's domain by a script, atomic scripts, asynchronous replication
     * (a failover can lose writes).
     */
    public static final StoreProfile REDIS = new StoreProfile("redis", Versioning.ASSIGNING_SHARED, Changes.FEED,
            Clock.MICROS, Duration.ZERO, true, Batch.ATOMIC, true, false, false, false);

    /**
     * Hostile: Redis with {@code maxmemory} and an eviction policy, deleting rows silently, and pub/sub losing
     * messages across a disconnect it did not notice.
     */
    public static final StoreProfile REDIS_HOSTILE = REDIS.with("redis-hostile", true, true);

    /**
     * Hostile: PostgreSQL on a wall clock that steps back by up to 5 ms (NTP), declared in its allowance.
     */
    public static final StoreProfile POSTGRESQL_STEPPING = new StoreProfile("postgresql-stepping",
            Versioning.ASSIGNING_OWN, Changes.FEED, Clock.MICROS, Duration.ofMillis(5), true, Batch.ATOMIC, false,
            false, false, false);

    public StoreProfile {
        requireNonNull(name, "name cannot be null");
        requireNonNull(versioning, "versioning cannot be null");
        requireNonNull(changes, "changes cannot be null");
        requireNonNull(clock, "clock cannot be null");
        requireNonNull(clockStepBack, "clockStepBack cannot be null");
        requireNonNull(batch, "batch cannot be null");
    }

    /**
     * The profile named by the system property {@code inmemory.profile}; {@link #JDBC} without it.
     *
     * @return the profile
     */
    public static StoreProfile ofProperty() {
        String name = System.getProperty("inmemory.profile", JDBC.name()).toLowerCase(Locale.ROOT);
        return switch (name) {
            case "jdbc" -> JDBC;
            case "postgresql" -> POSTGRESQL;
            case "mongodb" -> MONGODB;
            case "redis" -> REDIS;
            case "redis-hostile" -> REDIS_HOSTILE;
            case "postgresql-stepping" -> POSTGRESQL_STEPPING;
            default -> throw new IllegalArgumentException("unknown in-memory profile: " + name);
        };
    }

    /**
     * Returns this profile with another change source, for a test that needs one.
     *
     * @param changes how the instances learn about changes
     * @return the profile
     */
    public StoreProfile with(Changes changes) {
        return new StoreProfile(name + "-" + changes.name().toLowerCase(Locale.ROOT), versioning, changes, clock,
                clockStepBack, snapshotReads, batch, losesWritesOnFailover, tombstoneOnPurgedRow, evicts, lossyFeed);
    }

    /**
     * Returns this profile with another clock, for a test that needs one.
     *
     * @param clock     how the store's clock counts
     * @param stepBack  how far it may step back
     * @return the profile
     */
    public StoreProfile with(Clock clock, Duration stepBack) {
        return new StoreProfile(name + "-" + clock.name().toLowerCase(Locale.ROOT), versioning, changes, clock,
                stepBack, snapshotReads, batch, losesWritesOnFailover, tombstoneOnPurgedRow, evicts, lossyFeed);
    }

    private StoreProfile with(String name, boolean evicts, boolean lossyFeed) {
        return new StoreProfile(name, versioning, changes, clock, clockStepBack, snapshotReads, batch,
                losesWritesOnFailover, tombstoneOnPurgedRow, evicts, lossyFeed);
    }

    /**
     * Returns this profile with another batch behaviour, for a test that needs one.
     *
     * @param batch how a write of several rows applies
     * @return the profile
     */
    public StoreProfile with(Batch batch) {
        return new StoreProfile(name + "-" + batch.name().toLowerCase(Locale.ROOT), versioning, changes, clock,
                clockStepBack, snapshotReads, batch, losesWritesOnFailover, tombstoneOnPurgedRow, evicts, lossyFeed);
    }

    /**
     * Returns this profile leaving a tombstone on a write conditioned on a version of a purged row, or not.
     *
     * @param tombstoneOnPurgedRow whether such a write leaves a tombstone
     * @return the profile
     */
    public StoreProfile withTombstoneOnPurgedRow(boolean tombstoneOnPurgedRow) {
        return new StoreProfile(name + (tombstoneOnPurgedRow ? "-purge-tombstone" : "-no-purge-tombstone"),
                versioning, changes, clock, clockStepBack, snapshotReads, batch, losesWritesOnFailover,
                tombstoneOnPurgedRow, evicts, lossyFeed);
    }

    /**
     * Whether a write of the given batch size is applied by assigning.
     *
     * @param batchSize the number of writes in the call
     * @return true if the store assigns the versions
     */
    public boolean assigns(int batchSize) {
        return switch (versioning) {
            case CONDITIONAL -> false;
            case ASSIGNING_OWN, ASSIGNING_SHARED -> true;
            case MIXED -> batchSize == 1;
        };
    }

    /**
     * The store's clock allowance ({@code Store#clockAllowance}): its unit, for write times that repeat within it,
     * and how far it may step back.
     *
     * @return the allowance
     */
    public Duration clockAllowance() {
        return clock.resolution().plus(clockStepBack);
    }

    @Override
    public String toString() {
        return name;
    }
}
