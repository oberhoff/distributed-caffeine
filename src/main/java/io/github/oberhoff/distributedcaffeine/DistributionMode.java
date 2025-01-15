/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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

/**
 * Modes for defining the scope of distributed synchronization. Each mode includes/excludes different types of cache
 * operations (population, invalidation, eviction) which are then considered or not considered for distributed
 * synchronization between cache instances.
 *
 * @author Andreas Oberhoff
 */
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
    INVALIDATION
}
