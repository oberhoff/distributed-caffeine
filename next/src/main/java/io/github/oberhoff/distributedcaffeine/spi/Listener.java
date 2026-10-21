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

import java.util.List;

/**
 * Receives the announcements of a {@link Subscription}. Called from one thread at a time, in delivery order; the
 * core must not block in it beyond handing the messages on.
 */
public interface Listener {

    /**
     * Receives a batch of messages in delivery order. Duplicates and reordering across keys are allowed; per key the
     * messages arrive in commit order.
     *
     * @param messages the messages, never empty
     */
    void onMessages(List<Message> messages);

    /**
     * Reports that continuity was lost while delivery continues: messages may have been missed (a history the feed
     * could no longer resume from, every row removed at once). The core revalidates what it holds.
     */
    void onGap();

    /**
     * Reports that delivery stopped (the listening connection or cursor was lost). The feed does not restore it by
     * itself: the core calls {@link Subscription#resume()} until delivery is back, and checks every access against
     * the store meanwhile.
     *
     * @param cause what stopped delivery, if known
     */
    void onLost(Throwable cause);
}
