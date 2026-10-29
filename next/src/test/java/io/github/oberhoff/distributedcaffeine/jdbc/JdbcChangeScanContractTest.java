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
package io.github.oberhoff.distributedcaffeine.jdbc;

import io.github.oberhoff.distributedcaffeine.spi.ChangeScanContractTest;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import org.jspecify.annotations.NullMarked;

/**
 * The change scan contract on the plain JDBC adapter over MariaDB: the generic store built from its primitives, a fresh
 * table per test, another cache as another discriminator in it.
 */
@NullMarked
class JdbcChangeScanContractTest extends ChangeScanContractTest {

    @Override
    protected Fixture createFixture() {
        String table = JdbcTestDatabase.freshTable();
        return new Fixture() {
            @Override
            public Store openStore() {
                return JdbcTestDatabase.open(table, JdbcAdapter.DEFAULT_DISCRIMINATOR);
            }

            @Override
            public Store openStoreOfAnotherCache() {
                return JdbcTestDatabase.open(table, "other");
            }
        };
    }
}
