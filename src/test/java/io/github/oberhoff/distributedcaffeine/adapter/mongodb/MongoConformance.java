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

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.github.oberhoff.distributedcaffeine.AdapterConformanceTest.AdapterConformance;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * MongoDB's side of the conformance suite, on a fresh collection of the running test database.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class MongoConformance implements AdapterConformance {

    private static final String DATABASE = "conformance";

    private final MongoClient client = MongoTestDatabase.get().client;
    private final String collection = MongoTestDatabase.freshCollection();
    private @Nullable MongoClient unreachable;

    @Override
    public Adapter<String, String> adapter(String discriminator) {
        return MongoAdapter.newBuilder(client, DATABASE, collection).withDiscriminator(discriminator).build();
    }

    @Override
    public void markOtherVersion() {
        documents().updateOne(Filters.eq("_id", MongoLocation.MARKER_ID),
                Updates.set("schema", "distributed-caffeine schema 2"));
    }

    @Override
    public boolean putForeignData() {
        documents().insertOne(new Document("_id", "foreign"));
        return true;
    }

    @Override
    public Adapter<String, String> unreachable() {
        unreachable = MongoClients.create("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=1000&connectTimeoutMS=500");
        return MongoAdapter.newBuilder(unreachable, DATABASE, collection).build();
    }

    @Override
    public String product() {
        return "MongoDB";
    }

    @Override
    public void close() {
        if (unreachable != null) {
            unreachable.close();
        }
    }

    // majority, as the adapter reads with majority read concern and would not yet see a write of w:1
    private MongoCollection<Document> documents() {
        return client.getDatabase(DATABASE).getCollection(collection).withWriteConcern(WriteConcern.MAJORITY);
    }
}
