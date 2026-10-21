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
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Assumptions;

/**
 * The store the cache suites run on: the in-memory store, whose faults a test can inject, or a real database. One
 * backend is one fresh cache (a table, a collection); every store it opens is one more instance on it.
 */
public interface CacheBackend extends AutoCloseable {

    /**
     * Opens a store on the backend's cache, as one more instance would.
     *
     * @param settings the formats and the timeout
     * @return the store
     */
    Store open(StoreSettings settings);

    /**
     * Opens a store in the formats of the suites' string serializers, for looking at rows directly.
     *
     * @return the store
     */
    default Store open() {
        return open(StoreSettings.of(Format.TEXT, Format.TEXT));
    }

    /**
     * Returns whether everything committed so far has been handed to the subscribers.
     *
     * @return whether delivery is idle
     */
    boolean isIdle();

    /**
     * Makes the delivery lose its continuity, so that every subscriber is told about a gap.
     */
    void induceGap();

    /**
     * Returns the in-memory database, or {@code null} on a real database.
     *
     * @return the in-memory database
     */
    default @Nullable InMemoryDatabase inMemory() {
        return null;
    }

    /**
     * Returns the in-memory database for injecting faults; a test that needs them is skipped on a real database.
     *
     * @return the in-memory database
     */
    default InMemoryDatabase faults() {
        InMemoryDatabase database = inMemory();
        if (database == null) {
            return Assumptions.abort("needs the in-memory store's faults");
        }
        return database;
    }

    @Override
    default void close() {
    }

    /**
     * Returns the backend of an in-memory database.
     *
     * @param database the database
     * @return the backend
     */
    static CacheBackend of(InMemoryDatabase database) {
        return new CacheBackend() {
            @Override
            public Store open(StoreSettings settings) {
                return database.openStore("cache");
            }

            @Override
            public boolean isIdle() {
                return database.isIdle();
            }

            @Override
            public void induceGap() {
                database.gap();
            }

            @Override
            public InMemoryDatabase inMemory() {
                return database;
            }
        };
    }
}
