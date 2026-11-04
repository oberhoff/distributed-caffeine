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

/**
 * How the instances of a cache react to changes made elsewhere ({@code withDistributionMode}): any write, invalidation
 * or expiry of a key. Every mode reads and writes the store alike; the mode decides what happens to the copies other
 * instances hold. No mode adds a key to an instance that does not hold it.
 *
 * @author Andreas Oberhoff
 */
public enum DistributionMode {

    /**
     * No distribution: a change made elsewhere reaches this instance only when its copy expires or is evicted. Without
     * {@code expireAfterWrite} or {@code expireAfter}, copies expire after the store's maximum age.
     */
    NONE,

    /**
     * The default: a change elsewhere drops this instance's copy of the key; the next read gets the value from the
     * store. Costs a listening connection, shared by the caches on the same client, and a store read only for keys read
     * again.
     */
    INVALIDATE,

    /**
     * As {@link #INVALIDATE}, but a write elsewhere replaces this instance's copy with the new value at once, if this
     * instance holds the key; an invalidation or an expiry still drops it. Saves the next read a round trip, at one
     * store read per write and instance holding the key.
     */
    UPDATE
}
