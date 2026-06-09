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
/**
 * This package contains the main classes and interfaces for configuring, constructing and using Distributed Caffeine
 * cache instances.
 * <p>
 * {@link io.github.oberhoff.distributedcaffeine.DistributedCaffeine} is the starting point for configuring and
 * constructing cache instances using a builder pattern instance returned by
 * {@link io.github.oberhoff.distributedcaffeine.DistributedCaffeine#newBuilder(Adapter)}.
 * <p>
 * Cache instances can be of type {@link io.github.oberhoff.distributedcaffeine.DistributedCache} (extends
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
 *
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
 */
package io.github.oberhoff.distributedcaffeine;

import io.github.oberhoff.distributedcaffeine.adapter.Adapter;
