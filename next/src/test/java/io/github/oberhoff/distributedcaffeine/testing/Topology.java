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

import java.util.Set;

/**
 * A topology of a store (a single server, a replica set, a primary with a standby, ...), and the suites it hosts.
 */
public interface Topology {

    /**
     * Returns the suites this topology hosts by default.
     *
     * @return the suites
     */
    Set<Suite.Kind> suites();

    /**
     * The test class of one image and topology, which starts the store before its nested suites and stops it after
     * them: one store at a time.
     */
    interface Tests {

        /**
         * The system property that leaves only the latest image of each store running (as on GitHub).
         */
        String RUNS_ON_GITHUB = "runsOnGitHub";

        /**
         * Returns the topology of this test class.
         *
         * @return the topology
         */
        Topology topology();
    }
}
