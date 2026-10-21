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

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * What a read found, and the cut-off of the cache: the stamp of its last {@link Store#invalidateAll()}
 * ({@link Row#stamp()} of the control row; 0 if there was none). The core counts every row written at or before it as
 * missing (VISION.md, "invalidateAll: a cut-off time"); the store returns such rows as they are, so that a conditional
 * write can name their version.
 *
 * @param rows   the rows found, by hash
 * @param cutoff the stamp of the cache's last {@link Store#invalidateAll()}, 0 if none
 */
public record ReadResult(Map<String, Row> rows, long cutoff) {

    public ReadResult {
        requireNonNull(rows, "rows cannot be null");
        if (cutoff < 0) {
            throw new IllegalArgumentException("cutoff cannot be negative");
        }
    }
}
