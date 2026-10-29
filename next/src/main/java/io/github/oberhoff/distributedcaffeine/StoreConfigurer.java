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

import static java.util.Objects.requireNonNull;

/**
 * The bounds of the shared store, in which every value carries its write time.
 */
public final class StoreConfigurer {

    static final Duration DEFAULT_MAINTENANCE_INTERVAL = Duration.ofMinutes(1);
    static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(10);
    static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(1);

    private @Nullable Duration maximumAge;
    private long maximumSize = -1;
    private Duration maintenanceInterval = DEFAULT_MAINTENANCE_INTERVAL;
    private Duration heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private Duration pollInterval = DEFAULT_POLL_INTERVAL;
    private Duration timeout = StoreSettings.DEFAULT_TIMEOUT;

    StoreConfigurer() {
    }

    /**
     * Sets how old a value may get anywhere, counted from its write by the store's clock. The store never serves an
     * older value, and copies held by instances are dropped at the latest one maintenance interval (plus delivery)
     * later, however their local expiry is configured. Required.
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
     * Sets the maximum number of values the store keeps for this cache; the oldest writes are pruned first,
     * periodically, without notifying anyone (instances may keep their copies). Required, like the maximum age, so
     * that the store is bounded in both dimensions.
     * <p>
     * A value is pruned only once it is a few seconds old (four seconds; for a store polled for changes, twice the
     * poll interval plus two seconds if that is longer): a cache turning over its whole maximum size faster than that
     * holds more values than the maximum meanwhile.
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
     * Sets how often expiry and pruning run (with ±20 % jitter). Part of the bound on the maximum age. Optional: one
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
     * Sets how often each instance checks that the store's change feed still delivers (with ±20 % jitter): it
     * invalidates a reserved key and expects the announcement before its next check. A feed can stop delivering
     * without any error; until the announcements arrive again, the instance trusts nothing it holds (every read
     * checks the store), and then revalidates. So a silent feed is noticed within about two intervals. Ignored
     * without distribution ({@code DistributionMode.NONE}). Optional: ten seconds by default.
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
     * Sets how often each instance polls the store for changes, where the adapter's store has no change feed of its
     * own but offers a {@code ChangeScan} (VISION.md, "The polling feed"): every interval it reads the changes since
     * its last poll, so changes elsewhere reach it within about an interval. Must be at most half the heartbeat
     * interval, checked when the cache is built, so that the heartbeat does not take the pause between polls for a
     * silent feed. Ignored where the store has a change feed, and without distribution. Optional: one second by
     * default.
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

    /**
     * Sets how long an operation of the store waits for the store before it fails as the store: without a bound, a
     * client waits for good on a network that silently stopped carrying. A read that times out fails, a write fails
     * with its outcome unknown, and neither is retried (it waited its whole time already). The adapter applies it to
     * every operation and to its checks when the cache is built (MongoDB: the client-side operation timeout, unless
     * the client sets its own, the connection string option timeoutMS; PostgreSQL: the network timeout of the connection per
     * statement). Optional: 30 seconds by default.
     *
     * @param timeout the timeout (positive)
     * @return this configurer
     */
    public StoreConfigurer withTimeout(Duration timeout) {
        requireNonNull(timeout, "timeout cannot be null");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        this.timeout = timeout;
        return this;
    }

    @Nullable Duration maximumAge() {
        return maximumAge;
    }

    long maximumSize() {
        return maximumSize;
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

    Duration timeout() {
        return timeout;
    }
}
