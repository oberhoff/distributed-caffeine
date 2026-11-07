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
package io.github.oberhoff.distributedcaffeine.hasher;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * A key that computes its own hash; needed for key types other than {@link String}, {@link Long}, {@link Integer} and
 * {@link UUID}, unless a {@link HashProvider} is set. Keys that are equal must have the same hash.
 *
 * @author Andreas Oberhoff
 */
@FunctionalInterface
public interface Hashable {

    /**
     * Returns the hash of this key, typically computed with a fresh hasher from the supplier.
     *
     * @param hasher the supplier of a fresh hasher
     * @return the hash
     */
    String getHash(Supplier<Hasher> hasher);
}
