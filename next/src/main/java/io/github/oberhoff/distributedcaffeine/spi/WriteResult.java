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
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * What a {@link Store#write} did with each write: refused it, or applied it, at the version it proposed or at one the
 * store assigned (VISION.md, "Target design: versions applied as well as each store can"). A store applying a write
 * conditionally (only above the stored version) applies it at the proposed version and names only the refused ones; a
 * store assigning versions (above the stored one, whatever was proposed) never refuses an advancing write and names
 * the version wherever it is not the proposed one.
 *
 * @param refused  the hashes whose writes were not applied (their condition did not hold)
 * @param assigned the versions of applied writes that the store assigned other than proposed, by hash
 */
public record WriteResult(Set<String> refused, Map<String, Long> assigned) {

    private static final WriteResult ALL_APPLIED = new WriteResult(Set.of(), Map.of());

    public WriteResult {
        requireNonNull(refused, "refused cannot be null");
        requireNonNull(assigned, "assigned cannot be null");
        for (String hash : assigned.keySet()) {
            if (refused.contains(hash)) {
                throw new IllegalArgumentException("a write cannot be both refused and assigned: " + hash);
            }
        }
    }

    /**
     * Every write applied at its proposed version.
     *
     * @return the result
     */
    public static WriteResult allApplied() {
        return ALL_APPLIED;
    }

    /**
     * The given writes refused, every other one applied at its proposed version.
     *
     * @param refused the hashes of the refused writes
     * @return the result
     */
    public static WriteResult refused(Set<String> refused) {
        return refused.isEmpty() ? ALL_APPLIED : new WriteResult(refused, Map.of());
    }

    /**
     * Whether the write was refused.
     *
     * @param write the write
     * @return true if it was not applied
     */
    public boolean isRefused(Write write) {
        return refused.contains(write.hash());
    }

    /**
     * The version the write was applied at: the one the store assigned, else the one it proposed.
     *
     * @param write an applied write
     * @return its version
     */
    public long version(Write write) {
        Long version = assigned.get(write.hash());
        return version != null ? version : write.version();
    }
}
