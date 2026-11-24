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
 * The distributed caches: their builder, its configuration, and Caffeine's cache interfaces with the distributed side
 * added.
 * <p>
 * The builder {@link DistributedCaffeine} describes how the two levels work together, and where expiry and eviction
 * differ from a plain Caffeine cache. It is configured through {@link CaffeineConfigurer}, {@link StoreConfigurer},
 * {@link SerializersConfigurer} and {@link DistributionMode}, and builds a {@link DistributedCache},
 * {@link DistributedLoadingCache}, {@link DistributedAsyncCache} or {@link DistributedAsyncLoadingCache}. The
 * distributed side is {@link DistributedPolicy} (with the {@link SynchronizationState} and the store's
 * {@link StoredValue}s) and {@link DistributedStats}. The adapters for the stores are in the {@code adapter}
 * packages.
 *
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
 */
@NullMarked
package io.github.oberhoff.distributedcaffeine;

import org.jspecify.annotations.NullMarked;
