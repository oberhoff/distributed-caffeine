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
package io.github.oberhoff.distributedcaffeine.adapter.postgresql;

import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The listeners of this process, shared by whoever asks for the same one, and given up once nobody uses them any
 * more.
 */
final class PostgresListenerRegistry {

    private static final Map<Key, PostgresListener> LISTENERS = new HashMap<>();

    private PostgresListenerRegistry() {
        // utility class
    }

    /**
     * What makes two cache instances share a listener: the same data source to listen through, the same data source
     * that the probe proves the way from, and the same scope - which is what the sharing level decides. Data sources
     * are told apart by identity, because that is the only thing that can be told about them: two of them reaching
     * the same server cannot be recognized as such, and simply listen separately.
     */
    static final class Key {

        private final DataSource listenerDataSource;
        private final DataSource dataSource;
        private final Object scope;

        Key(DataSource listenerDataSource, DataSource dataSource, Object scope) {
            this.listenerDataSource = listenerDataSource;
            this.dataSource = dataSource;
            this.scope = scope;
        }

        // by identity on purpose, as described above
        @SuppressWarnings("ReferenceEquality")
        @Override
        public boolean equals(@Nullable Object object) {
            return object instanceof Key other
                    && listenerDataSource == other.listenerDataSource
                    && dataSource == other.dataSource
                    && scope.equals(other.scope);
        }

        @Override
        public int hashCode() {
            return Objects.hash(System.identityHashCode(listenerDataSource), System.identityHashCode(dataSource),
                    scope);
        }
    }

    // A listener whose start failed is replaced rather than handed out, so that whoever asks next gets a fresh attempt
    // instead of the failure of somebody else's
    static synchronized PostgresListener acquire(Key key, Supplier<PostgresListener> listenerSupplier) {
        PostgresListener listener = LISTENERS.get(key);
        if (listener == null || listener.isFailed()) {
            listener = listenerSupplier.get();
            LISTENERS.put(key, listener);
            listener.start();
        }
        listener.references++;
        return listener;
    }

    static synchronized void release(Key key, PostgresListener listener) {
        listener.references--;
        if (listener.references == 0) {
            // only if it was not replaced in the meantime, which would make the one in the map somebody else's
            LISTENERS.remove(key, listener);
            listener.close();
        }
    }
}
