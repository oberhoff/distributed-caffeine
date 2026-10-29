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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

// One adapter per cache (see "Adapter ownership"): an adapter object used for two open caches is almost always a
// mistake, so building with a claimed adapter fails. Released on close and on a failed build.
final class InternalClaims {

    private static final Set<Object> CLAIMED = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

    private InternalClaims() {
    }

    static void claim(Object adapter) {
        if (!CLAIMED.add(adapter)) {
            throw new IllegalStateException("The adapter is already used by another open cache; "
                    + "each cache needs an adapter of its own");
        }
    }

    static void release(Object adapter) {
        CLAIMED.remove(adapter);
    }

    // whether an open cache holds the adapter (tests check that closing and failed builds release it)
    static boolean isClaimed(Object adapter) {
        return CLAIMED.contains(adapter);
    }
}
