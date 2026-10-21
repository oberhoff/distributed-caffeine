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

import static java.util.Objects.requireNonNull;

/**
 * An announcement of a committed write. Carries no value: a holder that needs the new value reads the row.
 *
 * @param kind    the kind of the write
 * @param hash    the hash of the written key, {@code null} only for {@link Kind#INVALIDATED_ALL}
 * @param version the version the store assigned to the write, {@code 0} for {@link Kind#INVALIDATED_ALL}
 */
public record Message(Kind kind, @Nullable String hash, long version) {

    public Message {
        requireNonNull(kind, "kind cannot be null");
        if ((kind == Kind.INVALIDATED_ALL) != (hash == null)) {
            throw new IllegalArgumentException("hash must be null exactly for " + Kind.INVALIDATED_ALL);
        }
    }

    /**
     * Returns the message announcing that every row of the cache was invalidated.
     *
     * @param version the store's time of the sweep: a value row with a lower version is invalidated
     * @return the message
     */
    public static Message invalidatedAll(long version) {
        return new Message(Kind.INVALIDATED_ALL, null, version);
    }
}
