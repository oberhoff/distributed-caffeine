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
package io.github.oberhoff.distributedcaffeine;

import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

/**
 * The bounds of the shared store and how the cache works with it. Every setting is optional: without a maximum age or
 * a maximum size the store is unbounded in that respect, as a Caffeine cache is without its bounds; every other setting
 * has a default that suits most uses.
 *
 * @author Andreas Oberhoff
 */
public final class StoreConfigurer {

    static final Duration DEFAULT_MAINTENANCE_INTERVAL = Duration.ofMinutes(1);
    static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(10);
    static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(1);
    // the maximum age without one set, and the largest one taken (a larger one is lowered to it): the largest duration
    // in nanoseconds a long holds, about 292 years, which the core and the stores compute with (Duration#toNanos)
    static final Duration UNBOUNDED_AGE = Duration.ofNanos(Long.MAX_VALUE);
    // the maximum size without one set: no size pruning
    static final long UNBOUNDED_SIZE = -1;

    private @Nullable Duration maximumAge;
    private long maximumSize = UNBOUNDED_SIZE;
    private boolean maximumAgeForStoreOnly;
    private Duration operationTimeout = StoreSettings.DEFAULT_OPERATION_TIMEOUT;
    private @Nullable Executor executor;
    private Duration maintenanceInterval = DEFAULT_MAINTENANCE_INTERVAL;
    private Duration heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private Duration pollInterval = DEFAULT_POLL_INTERVAL;

    StoreConfigurer() {
    }

    /**
     * Sets how old a value may get, counted from its write by the store's clock. The store never returns an older
     * value, and every instance drops its copy of it at that age, however its own expiry is set and whatever the
     * distribution mode, also if a change never reaches the instance, so an outdated copy is served no longer than that
     * (unless {@link #withMaximumAgeForStoreOnly()}). Maintenance removes the value from the store within about one
     * maintenance interval after. Without a local expiry set ({@code expireAfterWrite} or {@code expireAfter}), the
     * local copies expire at the maximum age, and {@code policy().expireAfterWrite()} shows it. Adjustable at runtime
     * through {@link DistributedPolicy#storeMaximumAge()}.
     * Optional: unbounded by default, as with {@code ChronoUnit.FOREVER.getDuration()}.
     *
     * @param maximumAge the maximum age (positive)
     * @return this configurer
     */
    public StoreConfigurer withMaximumAge(Duration maximumAge) {
        requireNonNull(maximumAge, "maximumAge cannot be null");
        if (maximumAge.isNegative() || maximumAge.isZero()) {
            throw new IllegalArgumentException("maximumAge must be positive");
        }
        if (this.maximumAge != null) {
            throw new IllegalStateException("maximumAge was already set to " + this.maximumAge);
        }
        this.maximumAge = maximumAge.compareTo(UNBOUNDED_AGE) > 0 ? UNBOUNDED_AGE : maximumAge;
        return this;
    }

    /**
     * Sets how many values the store keeps for this cache. Beyond it, the oldest writes are removed periodically,
     * without notifying the instances, which may keep their copies. Values younger than a few seconds are kept (three
     * heartbeat intervals with {@link #withMaximumAgeForStoreOnly()}), so a cache that replaces its whole size faster
     * than that briefly holds more. Optional: unbounded by default.
     *
     * @param maximumSize the maximum number of values (positive)
     * @return this configurer
     */
    public StoreConfigurer withMaximumSize(long maximumSize) {
        if (maximumSize <= 0) {
            throw new IllegalArgumentException("maximumSize must be positive");
        }
        if (this.maximumSize != UNBOUNDED_SIZE) {
            throw new IllegalStateException("maximumSize was already set to " + this.maximumSize);
        }
        this.maximumSize = maximumSize;
        return this;
    }

    /**
     * Applies the maximum age to the store only: a value expires in the store as set, but the local copies expire as
     * their Caffeine expiry says, not bounded by it, and the store's expiry does not drop them. A miss reads the store
     * as before, so a value written or loaded elsewhere within the maximum age is taken from it; later, a miss calls
     * the source. For local copies meant to live longer than the store should keep values. Nothing bounds how long a
     * copy is served if a change never reaches its instance and the loss goes unnoticed; once a loss is noticed, copies
     * whose values expired in the store are dropped. The atomic {@code asMap()} methods stay atomic across instances,
     * at one read more. Requires distribution (not {@link DistributionMode#NONE}) and a maximum age ({@link
     * #withMaximumAge(Duration)}) of at least five heartbeat intervals: 50 seconds with the default interval, shorter
     * with a shorter one. Values written in the last three heartbeat intervals are kept beyond the maximum size.
     *
     * @return this configurer
     */
    public StoreConfigurer withMaximumAgeForStoreOnly() {
        maximumAgeForStoreOnly = true;
        return this;
    }

    /**
     * Sets how long an operation waits for the store before it fails; without a bound, a network that silently stops
     * carrying would block callers for good. It bounds every call that reaches the store (a local miss, a write, a load
     * writing its value) and the checks when the cache is built. A read that times out fails. A write that times out
     * has an unknown outcome: {@code put} fails, while the atomic {@code asMap()} methods, loads and refreshes repeat
     * it and fail only if they cannot recognize their own write. With a JDBC connection pool, set the pool's wait for a
     * connection (such as HikariCP's {@code connectionTimeout}, 30 seconds by default) no higher, or an exhausted pool
     * keeps callers waiting longer. One wait it does not bound: a driver writing through a blocking socket (the JDBC
     * drivers, MongoDB's default transport, Jedis) has no timeout for sending, so a write too large for the socket's
     * buffer, such as a large {@code putAll}, sent to a server that vanished without closing the connection, waits
     * until the operating system gives up (about 15 minutes on Linux, {@code net.ipv4.tcp_retries2}). Optional: 10
     * seconds by default.
     *
     * @param operationTimeout the timeout (positive)
     * @return this configurer
     */
    public StoreConfigurer withOperationTimeout(Duration operationTimeout) {
        requireNonNull(operationTimeout, "operationTimeout cannot be null");
        if (operationTimeout.isNegative() || operationTimeout.isZero()) {
            throw new IllegalArgumentException("operationTimeout must be positive");
        }
        this.operationTimeout = operationTimeout;
        return this;
    }

    /**
     * Sets the executor for the cache's blocking store calls, such as virtual threads on Java 21 or later. It must run
     * each task on another thread than the submitting one (not {@code Runnable::run}). Owned and closed by the caller.
     * Optional: a pool of daemon threads by default.
     *
     * @param executor the executor
     * @return this configurer
     */
    public StoreConfigurer withExecutor(Executor executor) {
        this.executor = requireNonNull(executor, "executor cannot be null");
        return this;
    }

    /**
     * Sets how often values beyond the maximum age or size are removed from the store, with ±20 % jitter. Optional: one
     * minute by default.
     *
     * @param maintenanceInterval the interval (positive)
     * @return this configurer
     */
    public StoreConfigurer withMaintenanceInterval(Duration maintenanceInterval) {
        requireNonNull(maintenanceInterval, "maintenanceInterval cannot be null");
        if (maintenanceInterval.isNegative() || maintenanceInterval.isZero()) {
            throw new IllegalArgumentException("maintenanceInterval must be positive");
        }
        this.maintenanceInterval = maintenanceInterval;
        return this;
    }

    /**
     * Sets how often each instance checks that it still receives the changes made elsewhere, with ±20 % jitter. Changes
     * can stop arriving without any error; such a silence is noticed within about two intervals, and until the changes
     * arrive again, every read checks its copy against the store. Ignored without distribution. At most a minute with
     * {@link #withMaximumAgeForStoreOnly()}. Optional: ten seconds by default.
     *
     * @param heartbeatInterval the interval (positive)
     * @return this configurer
     */
    public StoreConfigurer withHeartbeatInterval(Duration heartbeatInterval) {
        requireNonNull(heartbeatInterval, "heartbeatInterval cannot be null");
        if (heartbeatInterval.isNegative() || heartbeatInterval.isZero()) {
            throw new IllegalArgumentException("heartbeatInterval must be positive");
        }
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    /**
     * Sets how often each instance asks the store for changes, for stores that cannot announce them (MariaDB, MySQL):
     * changes made elsewhere arrive within about an interval, at one query per interval and instance. Ignored by the
     * other adapters and without distribution. At most half the heartbeat interval. Optional: one second by default.
     *
     * @param pollInterval the interval (positive)
     * @return this configurer
     */
    public StoreConfigurer withPollInterval(Duration pollInterval) {
        requireNonNull(pollInterval, "pollInterval cannot be null");
        if (pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("pollInterval must be positive");
        }
        this.pollInterval = pollInterval;
        return this;
    }

    Duration maximumAge() {
        return maximumAge != null ? maximumAge : UNBOUNDED_AGE;
    }

    boolean maximumAgeForStoreOnly() {
        return maximumAgeForStoreOnly;
    }

    // whether a maximum age was set (maximumAge() returns the unbounded one otherwise)
    boolean hasMaximumAge() {
        return maximumAge != null;
    }

    long maximumSize() {
        return maximumSize;
    }

    Duration operationTimeout() {
        return operationTimeout;
    }

    @Nullable Executor executor() {
        return executor;
    }

    Duration maintenanceInterval() {
        return maintenanceInterval;
    }

    Duration heartbeatInterval() {
        return heartbeatInterval;
    }

    Duration pollInterval() {
        return pollInterval;
    }
}
