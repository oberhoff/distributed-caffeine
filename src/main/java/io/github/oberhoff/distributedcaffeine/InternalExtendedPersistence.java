/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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

import java.time.Duration;

import static java.util.Objects.nonNull;

class InternalExtendedPersistence {

    private final Integer extendedPersistenceSize;
    private final Duration extendedPersistenceTime;
    private final boolean extendedPersistenceLoader;

    InternalExtendedPersistence(Integer extendedPersistenceSize,
                                Duration extendedPersistenceTime,
                                boolean extendedPersistenceLoader) {
        this.extendedPersistenceSize = extendedPersistenceSize;
        this.extendedPersistenceTime = extendedPersistenceTime;
        this.extendedPersistenceLoader = extendedPersistenceLoader;
    }

    Integer getExtendedPersistenceSize() {
        return extendedPersistenceSize;
    }

    Duration getExtendedPersistenceTime() {
        return extendedPersistenceTime;
    }

    boolean hasExtendedPersistenceBySize() {
        return nonNull(extendedPersistenceSize);
    }

    boolean hasExtendedPersistenceByTime() {
        return nonNull(extendedPersistenceTime);
    }

    boolean hasExtendedPersistence() {
        return hasExtendedPersistenceBySize() || hasExtendedPersistenceByTime();
    }

    public boolean hasExtendedPersistenceLoader() {
        return extendedPersistenceLoader;
    }
}
