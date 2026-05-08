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
import io.github.oberhoff.distributedcaffeine.adapter.AbstractAdapter;
import org.jspecify.annotations.NullMarked;

/**
 * Implementation of an adapter based on MongoDB as the underlying store.
 *
 * @param <K> the key type of the cache
 * @param <V> the value type of the cache
 * @author Andreas Oberhoff
 */
@NullMarked
public final class MongoAdapter<K, V> extends AbstractAdapter<K, V> {

    /**
     * Constructs an adapter based on MongoDB as the underlying store.
     *
     * @param mongoClient    the mongo client used by the adapter
     * @param databaseName   the database name used by the adapter
     * @param collectionName the collection name used by teh adapter
     */
    public MongoAdapter(MongoClient mongoClient, String databaseName, String collectionName) {
        super(new MongoRepository<>(mongoClient, databaseName, collectionName),
                new MongoSynchronizer<>(mongoClient, databaseName, collectionName),
                String.join(":", "mongodb", databaseName, collectionName));
    }
}
