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
package io.github.oberhoff.distributedcaffeine.testing;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a suite nested in a topology's test class: it runs where the topology hosts it ({@link Topology#suites()}).
 * {@code -DallSuites=true} runs every general suite on every topology; {@code -Dsuites=CORE,FAILOVER} runs only
 * those, for a quick run.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(SuiteCondition.class)
public @interface Suite {

    /**
     * The suite.
     *
     * @return the suite
     */
    Kind value();

    /**
     * The suites a topology can host.
     */
    enum Kind {
        /** The storage contract. */
        STORE_CONTRACT(true),
        /** The change feed contract. */
        CHANGE_FEED_CONTRACT(true),
        /** The core, distribution, recovery and Caffeine parity suites. */
        CORE(true),
        /** The property test. */
        PROPERTY(true),
        /** The adapter's own tests on a single server: schema, formats, connections, restarts. */
        ADAPTER(false),
        /** Members failing and taking over. */
        FAILOVER(false),
        /** A sharded cluster. */
        SHARDING(false),
        /** A MongoDB oplog rolling over behind the change stream. */
        OPLOG(false),
        /** A full PostgreSQL notification queue. */
        NOTIFY_QUEUE(false),
        /** A PostgreSQL pooler in front of the server. */
        POOLER(false);

        private final boolean general;

        Kind(boolean general) {
            this.general = general;
        }

        /**
         * Returns whether the suite runs on any topology, so that {@code -DallSuites=true} adds it everywhere.
         *
         * @return whether it is general
         */
        public boolean isGeneral() {
            return general;
        }
    }
}
