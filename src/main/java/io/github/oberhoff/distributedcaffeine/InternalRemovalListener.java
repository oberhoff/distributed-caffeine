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
package io.github.oberhoff.distributedcaffeine;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.jspecify.annotations.Nullable;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.Executor;

import static io.github.oberhoff.distributedcaffeine.InternalKey.kn;
import static io.github.oberhoff.distributedcaffeine.InternalValue.vn;
import static java.util.Objects.requireNonNull;

class InternalRemovalListener<K, V> implements RemovalListener<InternalKey<K>, InternalValue<V>>,
        InternalInitializable<K, V> {

    private final RemovalListener<K, V> removalListener;

    @SuppressWarnings("NotNullFieldNotInitialized")
    private Logger logger;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Executor executor;

    @SuppressWarnings({"java:S2637", "NullAway.Init"})
    InternalRemovalListener(RemovalListener<K, V> removalListener) {
        this.removalListener = requireNonNull(removalListener);
        // see also initialize()
    }

    @Override
    public void initialize(InternalInstanceRegistry<K, V> instanceRegistry) {
        this.logger = instanceRegistry.getLogger();
        this.executor = instanceRegistry.getExecutor();
    }

    @Override
    public void onRemoval(@Nullable InternalKey<K> key, @Nullable InternalValue<V> value, RemovalCause removalCause) {
        removalListener.onRemoval(kn(key), vn(value), removalCause);
    }

    // Reports a value that never made it into the cache - loaded or refreshed, and discarded because a write overtook
    // it - the way Caffeine reports it. Caffeine does not discard a load: the overtaking write waits for it and then
    // replaces or removes what it inserted, which reaches this listener as REPLACED or EXPLICIT. Its refresh, which
    // it does discard, is reported the same way. So every value a loader returned reaches this listener with Caffeine,
    // and whoever relies on that - to release what a value holds, for instance - would otherwise never hear of it.
    // Asynchronously on the executor of the cache, exactly as Caffeine notifies (BoundedLocalCache.notifyRemoval),
    // which catches whatever the listener or the executor throws as well (java:S1181): a listener failing must not
    // fail the load or refresh it is told about
    @SuppressWarnings("java:S1181")
    void notifyDiscarded(K key, V value, RemovalCause removalCause) {
        Runnable notification = () -> {
            try {
                removalListener.onRemoval(key, value, removalCause);
            } catch (Throwable t) {
                logger.log(Level.WARNING, "Exception thrown by removal listener", t);
            }
        };
        try {
            executor.execute(notification);
        } catch (Throwable t) {
            logger.log(Level.ERROR, "Exception thrown when submitting removal listener", t);
            notification.run();
        }
    }
}
