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
 * The result of a conditional write for one key.
 *
 * @param written        whether the entry was written
 * @param version        the version assigned to the write if written, otherwise {@code 0}
 * @param current        the row that prevented the write if not written (as {@link Store#read} would return it, so
 *                       {@code null} if it no longer counts as present), otherwise {@code null}
 * @param currentVersion the version of the row that prevented the write, whatever its state, even if it no longer
 *                       counts as present (a value older than the maximum age); {@code 0} if written or if no row
 *                       exists
 */
public record WriteResult(boolean written, long version, @Nullable Row current, long currentVersion) {

    public WriteResult {
        if (written && (version <= 0 || current != null || currentVersion != 0)) {
            throw new IllegalArgumentException("a written result has a positive version and no current row");
        }
        if (!written && version != 0) {
            throw new IllegalArgumentException("a conflict has no version");
        }
        if (current != null && current.version() != currentVersion) {
            throw new IllegalArgumentException("currentVersion must be the current row's version");
        }
    }

    public static WriteResult written(long version) {
        return new WriteResult(true, version, null, 0L);
    }

    public static WriteResult conflict(@Nullable Row current, long currentVersion) {
        return new WriteResult(false, 0L, current, currentVersion);
    }
}
