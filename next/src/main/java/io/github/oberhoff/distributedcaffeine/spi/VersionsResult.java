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
 * What a read of versions found ({@link Store#readVersions}): per hash the version, the stamp and the basis of the
 * value row's last write ({@link Row#stamp()}, {@link Row#basis()}), and the cut-off of the cache
 * ({@link ReadResult#cutoff()}).
 *
 * @param versions the versions of the value rows found, by hash
 * @param stamps   the stamps of the same rows, by hash
 * @param bases    the bases of the same rows, by hash
 * @param cutoff   the stamp of the cache's last {@link Store#invalidateAll()}, 0 if none
 */
public record VersionsResult(Map<String, Long> versions, Map<String, Long> stamps, Map<String, Long> bases,
                             long cutoff) {

    public VersionsResult {
        requireNonNull(versions, "versions cannot be null");
        requireNonNull(stamps, "stamps cannot be null");
        requireNonNull(bases, "bases cannot be null");
        if (!versions.keySet().equals(stamps.keySet()) || !versions.keySet().equals(bases.keySet())) {
            throw new IllegalArgumentException("versions, stamps and bases must name the same rows");
        }
        if (cutoff < 0) {
            throw new IllegalArgumentException("cutoff cannot be negative");
        }
    }
}
