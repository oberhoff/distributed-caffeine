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
 * The result of a conditional write for one key: written at a version, or a conflict with the row that prevented it.
 *
 * @param written whether the entry was written
 * @param version if written, the version assigned to the write; if not, the version of the row that prevented it,
 *                whatever its state, even if it no longer counts as present (a value older than the maximum age), and
 *                {@code 0} if no row exists
 * @param current if not written, the row that prevented the write as {@link Store#read} would return it ({@code null}
 *                if it no longer counts as present); {@code null} if written
 */
public record WriteResult(boolean written, long version, @Nullable Row current) {

    public WriteResult {
        if (written && (version <= 0 || current != null)) {
            throw new IllegalArgumentException("a written result has a positive version and no current row");
        }
        if (current != null && current.version() != version) {
            throw new IllegalArgumentException("a conflict's version must be the current row's version");
        }
    }

    /**
     * Returns the result of a write that was applied.
     *
     * @param version the version assigned to the write
     * @return the result
     */
    public static WriteResult written(long version) {
        return new WriteResult(true, version, null);
    }

    /**
     * Returns the result of a write that was not applied.
     *
     * @param current the row that prevented it, as a read would return it, or {@code null}
     * @param version the version of that row, whatever its state; {@code 0} if no row exists
     * @return the result
     */
    public static WriteResult conflict(@Nullable Row current, long version) {
        return new WriteResult(false, version, current);
    }
}
