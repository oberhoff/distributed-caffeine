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
package io.github.oberhoff.distributedcaffeine.spi;

import org.jspecify.annotations.Nullable;

/**
 * A store operation failed, classified by the store; the store does not retry. The core retries what is
 * retryable, as far as repeating the operation is safe (see "Write retries" in the design), with backoff, and takes
 * anything else a store throws for a failure that is not retryable, with the outcome of a write unknown.
 * <ul>
 *     <li>{@link #isOutcomeUnknown()}: whether a write may have been applied, wholly or in part (a timeout or a
 *     connection broken after sending; a batch failing after part of it was written, see
 *     {@link #afterPartlyApplied()}). For reads it carries no meaning.</li>
 *     <li>{@link #isRetryable()}: whether another attempt may succeed soon (a deadlock, a serialization failure, a
 *     connection that could not be obtained or broke, a failover in progress); not, if it failed for good (a
 *     schema that does not fit, a row that cannot be read) or after waiting its whole timeout already.</li>
 * </ul>
 */
public class StoreException extends RuntimeException {

    private final boolean outcomeUnknown;
    private final boolean retryable;

    /**
     * Constructs the exception.
     *
     * @param message        the detail message
     * @param cause          the cause, if any
     * @param outcomeUnknown {@code true} if a write may have been applied
     * @param retryable      {@code true} if another attempt may succeed soon
     */
    public StoreException(String message, @Nullable Throwable cause, boolean outcomeUnknown, boolean retryable) {
        super(message, cause);
        this.outcomeUnknown = outcomeUnknown;
        this.retryable = retryable;
    }

    /**
     * Constructs a failure that is not retryable.
     *
     * @param message        the detail message
     * @param cause          the cause
     * @param outcomeUnknown {@code true} if a write may have been applied
     */
    public StoreException(String message, Throwable cause, boolean outcomeUnknown) {
        this(message, cause, outcomeUnknown, false);
    }

    /**
     * Constructs a failure that is not retryable, without a cause.
     *
     * @param message        the detail message
     * @param outcomeUnknown {@code true} if a write may have been applied
     */
    public StoreException(String message, boolean outcomeUnknown) {
        this(message, null, outcomeUnknown, false);
    }

    /**
     * Returns this failure for a write of which a part was applied before it failed (a batch written in parts): its
     * outcome is unknown, retryable as this one is.
     *
     * @return the failure of the whole write
     */
    public StoreException afterPartlyApplied() {
        return isOutcomeUnknown() ? this : new StoreException(getMessage() + " (after part of the write was applied)",
                this, true, retryable);
    }

    /**
     * Returns whether a write may have been applied.
     *
     * @return {@code true} if the outcome is unknown
     */
    public boolean isOutcomeUnknown() {
        return outcomeUnknown;
    }

    /**
     * Returns whether another attempt may succeed soon.
     *
     * @return {@code true} if retryable
     */
    public boolean isRetryable() {
        return retryable;
    }
}
