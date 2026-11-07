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

import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Nested;

/**
 * The failover tests on one MariaDB or MySQL image, a primary with an asynchronous replica, the applications through
 * one driver (the subclass names both). Each test builds a topology of its own, as a failover leaves one server.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
abstract class JdbcReplicationTests {

    private final String image;
    private final String driver;

    JdbcReplicationTests(String image, String driver) {
        this.image = image;
        this.driver = driver;
    }

    @Nested
    class Failover extends JdbcFailoverTest {

        @Override
        protected String image() {
            return image;
        }

        @Override
        protected String driver() {
            return driver;
        }
    }
}
