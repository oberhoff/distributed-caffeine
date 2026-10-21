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
 * What a read found, and when: the store's time of the read, in microseconds on the clock the versions come from. A
 * load creating a row that did not exist uses it as the row's version ({@link ExpectedEntry#ifMissing(Entry, long)}),
 * so that the version tells whether the load read before or after an {@code invalidateAll}.
 *
 * @param rows the rows found, by hash
 * @param time the store's time of the read, in microseconds
 */
public record ReadResult(Map<String, Row> rows, long time) {

    public ReadResult {
        requireNonNull(rows, "rows cannot be null");
        if (time <= 0) {
            throw new IllegalArgumentException("time must be positive");
        }
    }
}
