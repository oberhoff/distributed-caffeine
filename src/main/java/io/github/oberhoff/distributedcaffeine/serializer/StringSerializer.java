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
 * Interface to be used when implementing a custom serializer with string representation.
 *
 * @param <T> the type of the object to serialize
 * @author Andreas Oberhoff
 */
public interface StringSerializer<T> extends Serializer<T, String> {

    @Override
    String serialize(T object) throws SerializerException;

    @Override
    T deserialize(String value) throws SerializerException;
}
