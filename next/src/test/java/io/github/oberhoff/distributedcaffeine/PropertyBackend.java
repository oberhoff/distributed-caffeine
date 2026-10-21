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

import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;

import java.util.Random;
import java.util.function.Consumer;

/**
 * The store under {@link InternalPropertyTest}: opens a store per cache instance on one cache, injects the faults it
 * can, and tells when it has nothing left to deliver.
 */
public interface PropertyBackend extends AutoCloseable {

    /**
     * Opens a store on the backend's cache, as one more instance would.
     *
     * @param settings the formats and the timeout
     * @return the store
     */
    Store open(StoreSettings settings);

    /**
     * Injects one fault at a random moment of a phase (or none).
     *
     * @param random the phase's random
     * @param trace  records what was injected
     */
    void fault(Random random, Consumer<String> trace);

    /**
     * Ends every fault.
     */
    void heal();

    /**
     * Returns whether everything committed so far has been handed to the subscribers.
     *
     * @return whether delivery is idle
     */
    boolean isIdle();

    @Override
    default void close() {
    }
}
