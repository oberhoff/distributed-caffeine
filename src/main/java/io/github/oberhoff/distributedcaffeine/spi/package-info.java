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
 * The service provider interface between the cache and a store adapter. A {@link Store} reads, writes and maintains
 * the values; for distribution, it also implements a {@link ChangeFeed} announcing its writes, or else a
 * {@link ChangeScan} the cache polls. The contract every implementation must fulfil is written out on {@link Store}
 * (storage, failures, settings), {@link ChangeFeed} (announcements, delivery, supervision) and {@link ChangeScan} (the
 * changes of a window), each point checked by the contract tests of its name; {@link StoreException} and
 * {@link ChangeSubscription} hold the details of failures and resuming.
 *
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
 */
@NullMarked
package io.github.oberhoff.distributedcaffeine.spi;

import org.jspecify.annotations.NullMarked;
