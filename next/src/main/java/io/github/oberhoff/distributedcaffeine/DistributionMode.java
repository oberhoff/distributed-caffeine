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
 * How the instances of a cache react to changes made elsewhere ({@code withDistributionMode}). A change is any write
 * (including {@code put} and {@code replace}), invalidation or expiry of a key. Every mode reads and writes the shared
 * store alike; the mode decides what happens to the local copies (L1) of other instances. No mode ever adds a key to
 * an instance that does not hold it.
 */
public enum DistributionMode {

    /**
     * No distribution: no subscription, no listening connection. A change made elsewhere reaches this instance's
     * copy only when Caffeine evicts or expires it; if no {@code expireAfterWrite} or {@code expireAfter} is
     * configured, copies expire after the store's maximum age. Like JetCache's two-level cache without
     * {@code syncLocal}.
     */
    NONE,

    /**
     * The default: any change elsewhere, a write as well as an invalidation or an expiry, drops this instance's copy
     * of the key, and the next read loads it from the store. Costs a listening connection per instance (shared per
     * JVM) and a store read only for keys read again.
     */
    INVALIDATE,

    /**
     * As {@link #INVALIDATE}, but a write elsewhere updates this instance's copy by reading the new value at once,
     * only for keys this instance holds; an invalidation or an expiry elsewhere still drops the copy. Saves the next
     * read a round trip; costs one store read per write and instance holding the key, whether or not the copy is read
     * again.
     */
    UPDATE
}
