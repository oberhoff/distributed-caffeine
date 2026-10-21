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

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A stored row as read from the store.
 *
 * @param hash    the hash of the key (the discriminator is the adapter's and never part of the SPI)
 * @param key     the serialized key, in the cache's key format (kept for diagnostics; the core matches by hash)
 * @param value   the serialized value in the cache's value format, {@code null} unless the state is
 *                {@link State#VALUE}
 * @param state   the state of the row
 * @param version the version the store assigned to the row's last write
 * @param age     how long ago the row was written, by the store's clock
 */
@SuppressWarnings("ArrayRecordComponent") // equals, hashCode and toString below compare and show the contents
public record Row(String hash, byte[] key, byte @Nullable [] value, State state, long version, Duration age) {

    public Row {
        requireNonNull(hash, "hash cannot be null");
        requireNonNull(key, "key cannot be null");
        requireNonNull(state, "state cannot be null");
        requireNonNull(age, "age cannot be null");
        if ((state == State.VALUE) == (value == null)) {
            throw new IllegalArgumentException("value must be present exactly for " + State.VALUE);
        }
    }

    @Override
    public boolean equals(@Nullable Object object) {
        return object instanceof Row that
                && hash.equals(that.hash)
                && Arrays.equals(key, that.key)
                && Arrays.equals(value, that.value)
                && state == that.state
                && version == that.version
                && age.equals(that.age);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, Arrays.hashCode(key), Arrays.hashCode(value), state, version, age);
    }

    @Override
    public String toString() {
        return "Row[hash=" + hash + ", state=" + state + ", version=" + version + ", age=" + age
                + ", key=" + key.length + " bytes, value=" + (value == null ? "none" : value.length + " bytes") + "]";
    }
}
