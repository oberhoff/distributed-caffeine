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

import static java.util.Objects.requireNonNull;

/**
 * An announcement of a committed write. Carries no value: a holder that needs the new value reads the row. A write of
 * the {@link Store#CONTROL_HASH control row} is announced like any other; the core takes it as {@code invalidateAll}.
 *
 * @param kind    the kind of the write
 * @param hash    the hash of the written key
 * @param version the version the write carried
 */
public record Change(Kind kind, String hash, long version) {

    public Change {
        requireNonNull(kind, "kind cannot be null");
        requireNonNull(hash, "hash cannot be null");
    }

    /**
     * The kind of a {@link Change}, derived by the adapter from the write that produced it.
     */
    public enum Kind {

        /**
         * A row was written (a value, or anything the adapter cannot tell apart from it). Holders read the row (with
         * population) or drop the key (without); the row decides, the kind is only a shortcut.
         */
        WRITTEN,

        /**
         * A row became a tombstone. Holders drop the key without reading.
         */
        INVALIDATED,

        /**
         * A row exceeded its maximum age and was marked expired. Holders drop the key without reading.
         */
        EXPIRED
    }
}
