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

import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Caches sharing a client wait once for a store that does not answer ({@link Adapter#client()}; VISION.md, "Starting
 * without the store", design step 8). Every creation of a store runs through it, at {@code build()} and at a connect in
 * the background: a creation that waited at least a tenth of its operation timeout and failed as not reached is
 * remembered for the client, and creations through it within one operation timeout after fail at once, without
 * asking the store. The connects of caches started without their store run through it as well, so while the store is
 * silent, one creation per client and operation timeout asks it; once it answers, the caches connect within about one
 * operation timeout and their backoff.
 *
 * @author Andreas Oberhoff
 */
final class InternalReachability {

    // by client (compared by equals, identity for the drivers' clients and data sources): until when, on the
    // environment's clock, a creation fails at once; weak, so that a client discarded is forgotten
    private static final Map<Object, Long> NOT_ANSWERING = Collections.synchronizedMap(new WeakHashMap<>());

    private InternalReachability() {
    }

    // the store of the adapter, created unless the store did not answer another creation through its client within
    // the last operation timeout
    static Store create(Adapter<?, ?> adapter, StoreSettings settings, InternalEnvironment environment) {
        Object client = adapter.client();
        if (client == null) {
            return adapter.createStore(settings);
        }
        Duration timeout = settings.operationTimeout();
        long now = environment.nanoTime();
        Long until = NOT_ANSWERING.get(client);
        if (until != null && now - until < 0) {
            throw new StoreException("The store did not answer another cache on the same client within the operation "
                    + "timeout (" + timeout.toMillis() + " ms) a moment ago; not asked again before "
                    + Duration.ofNanos(until - now).toMillis() + " ms", null, false, true);
        }
        try {
            Store store = adapter.createStore(settings);
            NOT_ANSWERING.remove(client);
            return store;
        } catch (StoreException e) {
            long end = environment.nanoTime();
            if (!e.isRetryable()) {
                NOT_ANSWERING.remove(client);
            } else if (end - now >= timeout.toNanos() / 10) {
                NOT_ANSWERING.put(client, end + timeout.toNanos());
            }
            throw e;
        }
    }
}
