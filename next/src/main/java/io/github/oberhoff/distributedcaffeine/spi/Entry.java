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

import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A value to write.
 *
 * @param hash  the hash of the key
 * @param key   the serialized key, in the cache's key format
 * @param value the serialized value, in the cache's value format
 */
@SuppressWarnings("ArrayRecordComponent") // equals, hashCode and toString below compare and show the contents
public record Entry(String hash, byte[] key, byte[] value) {

    public Entry {
        requireNonNull(hash, "hash cannot be null");
        requireNonNull(key, "key cannot be null");
        requireNonNull(value, "value cannot be null");
    }

    @Override
    public boolean equals(@Nullable Object object) {
        return object instanceof Entry that
                && hash.equals(that.hash)
                && Arrays.equals(key, that.key)
                && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, Arrays.hashCode(key), Arrays.hashCode(value));
    }

    @Override
    public String toString() {
        return "Entry[hash=" + hash + ", key=" + key.length + " bytes, value=" + value.length + " bytes]";
    }
}
