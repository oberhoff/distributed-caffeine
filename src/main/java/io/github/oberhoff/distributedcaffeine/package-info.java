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
 * This package contains the main classes and interfaces for configuring, building and using Distributed Caffeine cache
 * instances.
 * <p>
 * Distributed Caffeine is a {@link com.github.benmanes.caffeine.cache.Caffeine}-based distributed cache using MongoDB
 * change streams for near real-time synchronization between multiple cache instances, especially across different
 * machines.
 * <p>
 * Cache instances can be configured and constructed using a builder returned by
 * {@link io.github.oberhoff.distributedcaffeine.DistributedCaffeine#newBuilder(MongoCollection)}. A cache instance can
 * be of type {@link io.github.oberhoff.distributedcaffeine.DistributedCache} (extends
 * {@link com.github.benmanes.caffeine.cache.Cache}) or of type
 * {@link io.github.oberhoff.distributedcaffeine.DistributedLoadingCache} (extends
 * {@link com.github.benmanes.caffeine.cache.LoadingCache}).
 * <p>
 * <b>Attention:</b> To ensure the integrity of distributed synchronization between cache instances, the following
 * minor restrictions apply:
 * <ul>
 *      <li>Reference-based eviction using Caffeine's weak or soft references for keys or values is not supported. Even
 *      for the use of Caffeine (stand-alone), it is advised to use the more predictable size- or time-based eviction
 *      instead.</li>
 * </ul>
 */
package io.github.oberhoff.distributedcaffeine;

import com.mongodb.client.MongoCollection;