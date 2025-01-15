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
package io.github.oberhoff.distributedcaffeine.serializer;

/**
 * Checked exception to be thrown if serialization or deserialization fails within a {@link Serializer} instance.
 *
 * @author Andreas Oberhoff
 */
@SuppressWarnings("unused")
public class SerializerException extends Exception {

    /**
     * Constructs a new {@link SerializerException} with the specified detail message
     *
     * @param message the detail message
     */
    public SerializerException(String message) {
        super(message);
    }

    /**
     * Constructs a new {@link SerializerException} with the specified cause.
     *
     * @param cause the cause
     */
    public SerializerException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new {@link SerializerException} with the specified message and cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public SerializerException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new {@link SerializerException} with the specified message, cause, suppression enabled or disabled
     * and writable stack trace enabled or disabled.
     *
     * @param message            the detail message
     * @param cause              the cause
     * @param enableSuppression  whether suppression is enabled or disabled
     * @param writableStackTrace whether the stack trace should be writable
     */
    public SerializerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
