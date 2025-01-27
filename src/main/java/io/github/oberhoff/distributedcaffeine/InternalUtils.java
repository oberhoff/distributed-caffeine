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

import org.bson.types.ObjectId;

import static java.util.Objects.requireNonNull;

class InternalUtils {

    static <T> void requireNonNullOnCondition(boolean condition, T object, String message) {
        if (condition) {
            requireNonNull(object, message);
        }
    }

    static void runFailable(FailableRunnable failableRunnable) {
        try {
            failableRunnable.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static <T> T getFailable(FailableSupplier<T> failableSupplier) {
        try {
            return failableSupplier.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static String extractOrigin(ObjectId objectId) {
        return objectId.toHexString().substring(8, 18);
    }

    @FunctionalInterface
    interface FailableRunnable {

        void run() throws Exception;
    }

    @FunctionalInterface
    interface FailableSupplier<T> {

        T get() throws Exception;
    }
}
