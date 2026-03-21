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
package io.github.oberhoff.distributedcaffeine;

import org.jspecify.annotations.NullMarked;

/**
 * Modes for defining the scope of distributed synchronization. Each mode includes/excludes different types of cache
 * operations (population, invalidation, eviction) which are then considered or not considered for distributed
 * synchronization between cache instances.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
public enum DistributionMode {

    /**
     * Includes population (manual or loading), invalidation (explicit removal) and eviction (size- or time-based
     * removal).
     * <p>
     * <b>Note:</b> This is the default distribution mode and corresponds to a full replication.
     */
    POPULATION_AND_INVALIDATION_AND_EVICTION,

    /**
     * Includes population (manual or loading) and invalidation (explicit removal), but excludes eviction (size- or
     * time-based removal).
     */
    POPULATION_AND_INVALIDATION,

    /**
     * Includes invalidation (explicit removal) and eviction (size- or time-based removal), but excludes population
     * (manual or loading).
     */
    INVALIDATION_AND_EVICTION,

    /**
     * Includes invalidation (explicit removal), but excludes population (manual or loading) and eviction (size- or
     * time-based removal).
     */
    INVALIDATION;

    /**
     * Indicates whether population is considered for distributed synchronization between cache instances or not.
     *
     * @return {@code true} if population is considered, otherwise {@code false}
     */
    public boolean isPopulationConsidered() {
        return this.equals(POPULATION_AND_INVALIDATION_AND_EVICTION) || this.equals(POPULATION_AND_INVALIDATION);
    }

    /**
     * Indicates whether invalidation is considered for distributed synchronization between cache instances or not.
     *
     * @return {@code true} if invalidation is considered, otherwise {@code false}
     */
    public boolean isInvalidationConsidered() {
        return this.equals(POPULATION_AND_INVALIDATION_AND_EVICTION) || this.equals(POPULATION_AND_INVALIDATION)
                || this.equals(INVALIDATION_AND_EVICTION) || this.equals(INVALIDATION);
    }

    /**
     * Indicates whether eviction is considered for distributed synchronization between cache instances or not.
     *
     * @return {@code true} if eviction is considered, otherwise {@code false}
     */
    public boolean isEvictionConsidered() {
        return this.equals(POPULATION_AND_INVALIDATION_AND_EVICTION) || this.equals(INVALIDATION_AND_EVICTION);
    }
}
