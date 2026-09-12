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
 * This package contains classes and interfaces used to implement adapters that manages distributed synchronization
 * between cache instances and, optionally, persistence of cache entries.
 * <p>
 * <b>Note:</b> An adapter may use separate technologies for distribution and persistence, provided that every
 * mutation enters through a single serialization point and the other side is derived from it. Either direction
 * works: a store whose change log feeds distribution, or a log whose consumer maintains the store. Writing to both
 * independently is not supported, because two independent orders deciding the same cache entry cannot be
 * reconciled afterwards.
 *
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
 */
@NullMarked
package io.github.oberhoff.distributedcaffeine.adapter;

import org.jspecify.annotations.NullMarked;
