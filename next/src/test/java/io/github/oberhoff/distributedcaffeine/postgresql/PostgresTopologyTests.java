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

import io.github.oberhoff.distributedcaffeine.CacheBackend;
import io.github.oberhoff.distributedcaffeine.CaffeineParityTest;
import io.github.oberhoff.distributedcaffeine.InternalCoreTest;
import io.github.oberhoff.distributedcaffeine.InternalDistributionTest;
import io.github.oberhoff.distributedcaffeine.InternalRecoveryTest;
import io.github.oberhoff.distributedcaffeine.testing.Suite;
import io.github.oberhoff.distributedcaffeine.testing.Suite.Kind;
import io.github.oberhoff.distributedcaffeine.testing.SuiteCondition;
import io.github.oberhoff.distributedcaffeine.testing.Topology;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.util.Objects.requireNonNull;

/**
 * The suites on one PostgreSQL image and topology ({@link Setup} on the subclass): the store is started before the
 * suites and stopped after them, and each suite runs where the topology hosts it ({@link PostgresTopology}).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(SuiteCondition.class)
abstract class PostgresTopologyTests implements Topology.Tests {

    /**
     * The image and topology of a subclass.
     */
    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Setup {

        String image();

        PostgresTopology topology();
    }

    private PostgresTestDatabase database;

    @Override
    public PostgresTopology topology() {
        return setup().topology();
    }

    private Setup setup() {
        return requireNonNull(getClass().getAnnotation(Setup.class), getClass() + " names no image and topology");
    }

    @BeforeAll
    void startStore() {
        database = PostgresTestDatabase.start(setup().image(), setup().topology());
    }

    @AfterAll
    void stopStore() {
        if (database != null) {
            database.close();
        }
    }

    @Nested
    @Suite(Kind.STORE_CONTRACT)
    class StoreContract extends PostgresStoreContractTest {
    }

    @Nested
    @Suite(Kind.CHANGE_FEED_CONTRACT)
    class ChangeFeedContract extends PostgresChangeFeedContractTest {
    }

    @Nested
    @Suite(Kind.CORE)
    class Core extends InternalCoreTest {

        @Override
        protected CacheBackend backend() {
            return new PostgresCacheBackend();
        }
    }

    @Nested
    @Suite(Kind.CORE)
    class Distribution extends InternalDistributionTest {

        @Override
        protected CacheBackend backend() {
            return new PostgresCacheBackend();
        }
    }

    @Nested
    @Suite(Kind.CORE)
    class Recovery extends InternalRecoveryTest {

        @Override
        protected CacheBackend backend() {
            return new PostgresCacheBackend();
        }
    }

    @Nested
    @Suite(Kind.CORE)
    class CaffeineParity extends CaffeineParityTest {

        @Override
        protected CacheBackend backend() {
            return new PostgresCacheBackend();
        }
    }

    @Nested
    @Suite(Kind.PROPERTY)
    class Property extends PostgresPropertyTest {
    }

    @Nested
    @Suite(Kind.ADAPTER)
    class Adapter extends PostgresAdapterTest {
    }

    @Nested
    @Suite(Kind.ADAPTER)
    class Retry extends PostgresRetryTest {
    }

    @Nested
    @Suite(Kind.ADAPTER)
    class Connection extends PostgresConnectionTest {
    }

    @Nested
    @Suite(Kind.ADAPTER)
    class Restart extends PostgresRestartTest {
    }

    @Nested
    @Suite(Kind.FAILOVER)
    class Failover extends PostgresFailoverTest {
    }

    @Nested
    @Suite(Kind.POOLER)
    class Pooler extends PostgresPoolerTest {
    }

    @Nested
    @Suite(Kind.NOTIFY_QUEUE)
    class NotifyQueue extends PostgresNotifyQueueTest {
    }
}
