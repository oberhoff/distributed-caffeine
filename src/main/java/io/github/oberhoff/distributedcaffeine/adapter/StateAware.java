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
package io.github.oberhoff.distributedcaffeine.adapter;

/**
 * Interface representing objects that are aware of a state.
 *
 * @author Andreas Oberhoff
 */
public interface StateAware {

    /**
     * Sets the state as activated.
     * <p>
     * <b>Note:</b> Activating something that is activated already is supposed to do nothing. A cache instance counts
     * as activated only while all of its parts are, so a part deactivated on its own leaves it activating all of
     * them again - and a part that waits for itself to stop instead of returning blocks everything that cache
     * instance does for as long as it waits.
     */
    void activate();

    /**
     * Sets the state as deactivated.
     * <p>
     * <b>Note:</b> Deactivating something that is not activated is supposed to do nothing either, for the same
     * reason in reverse: deactivating a cache instance takes down whatever is still up, whether or not the cache
     * instance as a whole counts as activated.
     */
    void deactivate();

    /**
     * Indicates whether state is activated or not.
     *
     * @return {@code true} if state is activated, otherwise {@code false}
     */
    boolean isActivated();
}
