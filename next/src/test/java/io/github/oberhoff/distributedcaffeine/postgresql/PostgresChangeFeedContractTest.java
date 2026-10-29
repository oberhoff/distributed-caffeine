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
package io.github.oberhoff.distributedcaffeine.postgresql;

import io.github.oberhoff.distributedcaffeine.spi.ChangeFeedContractTest;
import org.jspecify.annotations.NullMarked;

/**
 * The change feed contract on PostgreSQL's LISTEN/NOTIFY, on the fixture of the {@link PostgresStoreContractTest}
 * with a listener, one listening session shared by the stores; a gap induced by terminating the listening backend.
 */
@NullMarked
abstract class PostgresChangeFeedContractTest extends ChangeFeedContractTest {

    @Override
    protected Fixture createFixture() {
        return PostgresStoreContractTest.fixture(true);
    }
}
