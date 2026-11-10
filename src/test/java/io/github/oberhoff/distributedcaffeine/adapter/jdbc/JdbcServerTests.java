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
package io.github.oberhoff.distributedcaffeine.adapter.jdbc;

import io.github.oberhoff.distributedcaffeine.CacheBackend;
import io.github.oberhoff.distributedcaffeine.CaffeineParityTest;
import io.github.oberhoff.distributedcaffeine.InternalCoreTest;
import io.github.oberhoff.distributedcaffeine.InternalDistributionTest;
import io.github.oberhoff.distributedcaffeine.InternalRecoveryTest;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Nested;

/**
 * Every suite on one MariaDB or MySQL image through one driver (the subclass names both): those of
 * {@link JdbcCompatibilityTests}, and the cache suites (core, distribution, recovery, Caffeine parity), polled.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
abstract class JdbcServerTests extends JdbcCompatibilityTests {

    JdbcServerTests(String image, String driver) {
        super(image, driver);
    }

    @Nested
    class Core extends InternalCoreTest {

        @Override
        protected CacheBackend backend() {
            return new JdbcCacheBackend();
        }
    }

    @Nested
    class Distribution extends InternalDistributionTest {

        @Override
        protected CacheBackend backend() {
            return new JdbcCacheBackend();
        }
    }

    @Nested
    class Recovery extends InternalRecoveryTest {

        @Override
        protected CacheBackend backend() {
            return new JdbcCacheBackend();
        }
    }

    @Nested
    class CaffeineParity extends CaffeineParityTest {

        @Override
        protected CacheBackend backend() {
            return new JdbcCacheBackend();
        }
    }
}
