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
 * A subscription to the announcements of a store. The core supervises it: after {@link ChangeListener#onLost}, and when
 * its heartbeat suggests that delivery stopped without a word, it calls {@link #resume()} with backoff until delivery
 * is back. Closing it ends delivery.
 */
public interface ChangeSubscription extends AutoCloseable {

    /**
     * Restores delivery, trying once: re-establishes the listening connection or cursor if it was lost, and also if it
     * seems alive (the core asks when no announcement arrives although writes were made: a connection the network
     * stopped carrying without a word). A feed shared by several subscriptions re-establishes it at most once for a
     * loss, whoever asks first. Returns once every write committed afterwards is delivered (as after
     * {@link ChangeFeed#subscribe}). What was written while delivery was down need not be delivered: the core treats
     * every resume as a gap and checks what it holds against the store, so a feed resumes from now, not from a
     * position.
     *
     * @throws StoreException if delivery cannot be restored now; the core tries again later
     */
    void resume();

    @Override
    void close();
}
