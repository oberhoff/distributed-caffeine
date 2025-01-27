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

/**
 * This package contains classes and interfaces that provide serializing/deserializing functionality for Distributed
 * Caffeine cache instances.
 * <p>
 * Distributed Caffeine already has build-in serializers:
 * <ul>
 *      <li>{@link io.github.oberhoff.distributedcaffeine.serializer.FurySerializer}</li>
 *      <li>{@link io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer}</li>
 *      <li>{@link io.github.oberhoff.distributedcaffeine.serializer.JavaObjectSerializer}</li>
 * </ul>
 * <p>
 * If custom serializers are required, they must implement one of the following interfaces:
 * <ul>
 *      <li>{@link io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer} for serializing an object to a
 *      byte array representation</li>
 *      <li>{@link io.github.oberhoff.distributedcaffeine.serializer.StringSerializer} for serializing an object to a
 *      string representation</li>
 *      <li>{@link io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer} for serializing an object to a JSON
 *      representation (encoded as String or BSON)</li>
 * </ul>
 */
package io.github.oberhoff.distributedcaffeine.serializer;