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
package io.github.oberhoff.distributedcaffeine.spi;

import java.time.Duration;
import java.util.List;

/**
 * What a store without a change feed of its own offers so that the core can poll it (VISION.md, "The polling feed"):
 * the changes of its cache within a recent window. A store implementing {@link ChangeFeed} needs none; one
 * implementing neither supports only {@code DistributionMode.NONE}. The core polls every poll interval, reading back
 * an overlap further than its last poll (a write takes its time before it becomes visible), and turns a poll that
 * failed for longer than the grace period of tombstones into a loss; nothing of that is the store's.
 * <p>
 * The contract, checked by {@code ChangeScanContractTest}:
 * <ol>
 *     <li><b>Within the window:</b> every row of the cache written within the given duration before the scan, by the
 *     store's clock (the write time of {@link Row#age()}), is reported with its latest version: a value row as
 *     {@link Change.Kind#WRITTEN}, a tombstone as {@link Change.Kind#INVALIDATED}, an expired row as
 *     {@link Change.Kind#EXPIRED}. Rows written earlier may be reported too. Reserved rows (the control row
 *     included) are reported like any other.</li>
 *     <li><b>No deletes:</b> deleted rows report nothing ({@link Store#pruneToSize}, {@link Store#pruneTombstones}),
 *     as in a change feed.</li>
 *     <li><b>Own cache only:</b> the rows of the store's own cache, none of another cache's, also where several caches
 *     share a table or collection.</li>
 * </ol>
 */
public interface ChangeScan {

    /**
     * Returns the changes of the cache's rows written within the given duration before the scan, by the store's
     * clock: per row its latest state and version, without values.
     *
     * @param within how far back, by the store's clock
     * @return the changes, in no particular order
     * @throws StoreException if the store cannot be read now; the core polls again
     */
    List<Change> changedWithin(Duration within);
}
