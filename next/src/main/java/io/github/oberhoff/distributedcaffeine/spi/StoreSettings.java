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

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * What the cache configures for its store and hands to {@link Adapter#createStore}: an adapter applies it rather
 * than offering settings of its own for the same thing, so that every adapter is configured alike.
 *
 * @param keyFormat   how serialized keys are stored
 * @param valueFormat how serialized values are stored
 * @param timeout     how long an operation of the store waits for the store before it fails as the store (a write
 *                    then with its outcome unknown): without a bound, a client waits for good on a network that
 *                    silently stopped carrying. An adapter applies it per operation (and to its checks on creation),
 *                    and may keep a timeout the user configured on the client itself
 */
public record StoreSettings(Format keyFormat, Format valueFormat, Duration timeout) {

    /**
     * The timeout a cache uses unless configured otherwise.
     */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Validates the settings.
     *
     * @param keyFormat   how serialized keys are stored
     * @param valueFormat how serialized values are stored
     * @param timeout     the operation timeout (positive)
     */
    public StoreSettings {
        requireNonNull(keyFormat, "keyFormat cannot be null");
        requireNonNull(valueFormat, "valueFormat cannot be null");
        requireNonNull(timeout, "timeout cannot be null");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }

    /**
     * Returns settings with the given formats and the default timeout.
     *
     * @param keyFormat   how serialized keys are stored
     * @param valueFormat how serialized values are stored
     * @return the settings
     */
    public static StoreSettings of(Format keyFormat, Format valueFormat) {
        return new StoreSettings(keyFormat, valueFormat, DEFAULT_TIMEOUT);
    }

    /**
     * How the serialized keys or values of a cache are to be stored. Fixed per cache; the core derives it from the
     * configured serializer, so an adapter never sees a serializer or the user's types.
     */
    public enum Format {

        /**
         * Opaque bytes ({@code BinData} on MongoDB, {@code bytea} on PostgreSQL).
         */
        BINARY,

        /**
         * UTF-8 text (a string on MongoDB, {@code text} on PostgreSQL).
         */
        TEXT,

        /**
         * UTF-8 JSON text, stored in the store's own JSON representation (a BSON value on MongoDB, {@code jsonb} on
         * PostgreSQL).
         */
        JSON
    }
}
