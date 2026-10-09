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
package io.github.oberhoff.distributedcaffeine.adapter.mongodb;

import com.mongodb.client.MongoClient;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The watchers of this process, shared by whoever asks for the same one, and given up once nobody uses them any
 * more.
 */
final class MongoWatcherRegistry {

    private static final Map<Key, MongoWatcher> WATCHERS = new HashMap<>();

    private MongoWatcherRegistry() {
        // utility class
    }

    /**
     * What makes two cache instances share a watcher: the same client, the same database and the same scope - which
     * is what the sharing mode decides. Clients are told apart by identity, because that is the only thing that can
     * be told about them: two of them reaching the same database cannot be recognized as such, and simply watch
     * separately.
     */
    static final class Key {

        private final MongoClient mongoClient;
        private final String databaseName;
        private final Object scope;

        Key(MongoClient mongoClient, String databaseName, Object scope) {
            this.mongoClient = mongoClient;
            this.databaseName = databaseName;
            this.scope = scope;
        }

        // by identity on purpose, as described above
        @SuppressWarnings("ReferenceEquality")
        @Override
        public boolean equals(@Nullable Object object) {
            return object instanceof Key other
                    && mongoClient == other.mongoClient
                    && databaseName.equals(other.databaseName)
                    && scope.equals(other.scope);
        }

        @Override
        public int hashCode() {
            return Objects.hash(System.identityHashCode(mongoClient), databaseName, scope);
        }
    }

    // A watcher whose start failed is replaced rather than handed out, so that whoever asks next gets a fresh attempt
    // instead of the failure of somebody else's
    static synchronized MongoWatcher acquire(Key key, Supplier<MongoWatcher> watcherSupplier) {
        MongoWatcher watcher = WATCHERS.get(key);
        if (watcher == null || watcher.isFailed()) {
            watcher = watcherSupplier.get();
            WATCHERS.put(key, watcher);
        }
        watcher.references++;
        return watcher;
    }

    static synchronized void release(Key key, MongoWatcher watcher) {
        watcher.references--;
        if (watcher.references == 0) {
            // only if it was not replaced in the meantime, which would make the one in the map somebody else's
            WATCHERS.remove(key, watcher);
            watcher.close();
        }
    }
}
