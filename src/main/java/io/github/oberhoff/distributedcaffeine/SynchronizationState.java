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
 * The state of a cache instance's synchronization with the store.
 *
 * @author Andreas Oberhoff
 */
public enum SynchronizationState {

    /**
     * The store answers and changes made elsewhere arrive.
     */
    SYNCHRONIZED,

    /**
     * The store does not answer, or changes made elsewhere do not arrive. Without the store, held values are served as
     * they are, misses are loaded from the source, and writes fail. Without the changes, every read checks its copy
     * against the store. The reason is logged; the instance recovers by itself.
     */
    DEGRADED,

    /**
     * The cache instance was closed; every operation throws, except the state and the stats of the distributed policy.
     */
    CLOSED
}
