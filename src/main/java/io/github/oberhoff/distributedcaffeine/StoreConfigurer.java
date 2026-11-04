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
 * The bounds of the shared store and how the cache works with it. The maximum age and the maximum size are required;
 * every other setting has a default that suits most uses.
 *
 * @author Andreas Oberhoff
 */
public final class StoreConfigurer {

    static final Duration DEFAULT_MAINTENANCE_INTERVAL = Duration.ofMinutes(1);
    static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(10);
    static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(1);

    private @Nullable Duration maximumAge;
    private long maximumSize = -1;
    private Duration operationTimeout = StoreSettings.DEFAULT_OPERATION_TIMEOUT;
    private @Nullable Executor executor;
    private Duration maintenanceInterval = DEFAULT_MAINTENANCE_INTERVAL;
    private Duration heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private Duration pollInterval = DEFAULT_POLL_INTERVAL;

    StoreConfigurer() {
    }

    /**
     * Sets how old a value may get, counted from its write by the store's clock. The store never returns an older
     * value, and the instances drop their copies of it within about one maintenance interval, however their own expiry
     * is set. Required.
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
        this.maximumAge = maximumAge;
        return this;
    }

    /**
     * Sets how many values the store keeps for this cache. Beyond it, the oldest writes are removed periodically,
     * without notifying the instances, which may keep their copies. Values younger than a few seconds are kept, so a
     * cache that replaces its whole size faster than that briefly holds more. Required.
     *
     * @param maximumSize the maximum number of values (positive)
     * @return this configurer
     */
    public StoreConfigurer withMaximumSize(long maximumSize) {
        if (maximumSize <= 0) {
            throw new IllegalArgumentException("maximumSize must be positive");
        }
        if (this.maximumSize != -1) {
            throw new IllegalStateException("maximumSize was already set to " + this.maximumSize);
        }
        this.maximumSize = maximumSize;
        return this;
    }

    /**
     * Sets how long an operation waits for the store before it fails; without a bound, a network that silently stops
     * carrying would block callers for good. It bounds every call that reaches the store (a local miss, a write, a load
     * writing its value) and the checks when the cache is built. A read that times out fails; a write fails with its
     * outcome unknown. Neither is retried. With a JDBC connection pool, set the pool's wait for a connection (such as
     * HikariCP's {@code connectionTimeout}) no higher, or an exhausted pool keeps callers waiting longer. Optional: 30
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
     * arrive again, every read checks its copy against the store. Ignored without distribution. Optional: ten seconds
     * by default.
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

    @Nullable Duration maximumAge() {
        return maximumAge;
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
