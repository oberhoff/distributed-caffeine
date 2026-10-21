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

/**
 * The announcements of a store's writes, which distribution needs: a {@link Store} that can announce its writes
 * also implements this. A store that cannot (a plain key-value store without notifications, say) leaves it out, and
 * its caches then support only {@code DistributionMode.NONE}: building one with distribution fails at once.
 * <p>
 * The contract; each point is checked by the tests named after it in {@code ChangeFeedContractTest}
 * ({@code contractN_...}):
 * <ol>
 *     <li><b>Exactly the committed writes:</b> a change is reported exactly when a write committed: never before, never
 *     for a rolled-back write, never for a read or a conditional write that did not apply. Each write method of
 *     {@link Store} names what it announces; deletes ({@link Store#pruneToSize}, {@link Store#pruneTombstones})
 *     announce nothing.</li>
 *     <li><b>Versions:</b> a change carries the version the write got, as returned to the writer.</li>
 *     <li><b>Delivery:</b> while subscribed, per key at least the latest write since the last delivery arrives (a feed
 *     may report every write, as a change stream does, or only the latest state of a key, as a poll does), and none
 *     older after a newer one: duplicates and reordering across keys are allowed. Where that cannot be guaranteed
 *     (changes may have been lost), the listener is told with {@link ChangeListener#onGap()}.</li>
 *     <li><b>Bounds:</b> a subscription receives every write committed after {@link #subscribe} returned, and nothing
 *     after it was closed.</li>
 *     <li><b>Loss and resume:</b> a lost delivery is reported ({@link ChangeListener#onLost}) and not restored by the
 *     feed itself; {@link ChangeSubscription#resume()} restores it from now on, and the core counts what was written
 *     meanwhile as missed (a gap).</li>
 *     <li><b>Reads follow changes:</b> once a change was delivered, a read through the subscribed store reflects
 *     the announced write: its version, or a later one.</li>
 *     <li><b>Own cache only:</b> a subscription receives the writes of its own cache, none of another cache's, also
 *     where several caches share a table or collection.</li>
 * </ol>
 * Supervising a subscription (when to try again, how long to wait, noticing a feed that stopped without a word) is
 * the core's: a feed reconnects only when asked.
 */
public interface ChangeFeed {

    /**
     * Subscribes to the announcements of the store's cache. Every write committed after this method returns is
     * delivered to the listener.
     *
     * @param listener the listener
     * @return the subscription
     */
    ChangeSubscription subscribe(ChangeListener listener);
}
