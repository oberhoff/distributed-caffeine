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
 * Interface to be used when implementing a custom serializer with JSON representation (encoded as String or BSON).
 *
 * @param <T> the type of the object to serialize
 * @author Andreas Oberhoff
 */
public interface JsonSerializer<T> extends StringSerializer<T> {

    /**
     * Indicates whether the JSON representation should be encoded as BSON or as string when stored in the MongoDB
     * collection.
     *
     * @return {@code true} for BSON encoding or {@code false} for string encoding
     */
    boolean storeAsBson();
}
