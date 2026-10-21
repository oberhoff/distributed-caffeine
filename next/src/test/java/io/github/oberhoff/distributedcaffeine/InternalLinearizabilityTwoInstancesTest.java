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

/**
 * Linearizability across two instances on a store that refuses a write decided under an older cut-off: the hot path
 * (a write decided on a held value, one store operation) is taken.
 */
public class InternalLinearizabilityTwoInstancesTest extends AbstractTwoInstanceLinearizabilityTest {

    @Override
    protected boolean checksBasis() {
        return true;
    }
}
