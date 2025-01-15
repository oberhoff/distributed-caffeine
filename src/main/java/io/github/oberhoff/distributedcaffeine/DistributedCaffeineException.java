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

/**
 * Unchecked runtime exception thrown by cache instances. In most cases it is just a wrapper around a checked exception.
 *
 * @author Andreas Oberhoff
 */
public final class DistributedCaffeineException extends RuntimeException {

    /**
     * Constructs a new {@link DistributedCaffeineException} with the specified detail message
     *
     * @param message the detail message
     */
    public DistributedCaffeineException(String message) {
        super(message);
    }

    /**
     * Constructs a new {@link DistributedCaffeineException} with the specified cause.
     *
     * @param cause the cause
     */
    public DistributedCaffeineException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new {@link DistributedCaffeineException} with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public DistributedCaffeineException(String message, Throwable cause) {
        super(message, cause);
    }
}
