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
package io.github.oberhoff.distributedcaffeine.mongodb;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

// On start (VISION.md, "Adapter setup" and "Migration"): a replica set with majority reads is required, sharded
// collections are refused; a collection with the schema marker of this version is used, a missing one is created
// (the marker document first, which creates the collection, so a concurrent start sees either nothing or the
// marker), one without the marker is refused or dropped and recreated with withSchemaReset(). A missing index only
// warns.
final class MongoSchema {

    private static final Logger LOGGER = System.getLogger("io.github.oberhoff.distributedcaffeine.mongodb");
    private static final int COMMAND_NOT_FOUND = 59;
    private static final BsonDocument INDEX = new BsonDocument("_id.d", new BsonInt32(1))
            .append("state", new BsonInt32(1)).append("writeTime", new BsonInt32(1));

    private MongoSchema() {
    }

    static MongoDatabase database(MongoClient client, MongoLocation location) {
        return client.getDatabase(location.databaseName)
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                // journaled, whatever writeConcernMajorityJournalDefault says: a confirmed write survives a crash of
                // the majority (store contract point 3; the change stream may resume past a lost write unnoticed)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    // the database for a store's own operations: bounded by the timeout unless the client sets one of its own
    static MongoDatabase database(MongoClient client, MongoLocation location, Duration timeout) {
        MongoDatabase database = database(client, location);
        return database.getTimeout(TimeUnit.MILLISECONDS) != null ? database
                : database.withTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    static void ensure(MongoClient client, MongoLocation location, boolean schemaReset, Duration timeout) {
        try {
            MongoDatabase database = database(client, location, timeout);
            checkDeployment(client, database, location);
            MongoCollection<BsonDocument> collection = database.getCollection(location.collectionName,
                    BsonDocument.class);
            boolean exists = database.listCollectionNames().into(new java.util.ArrayList<>())
                    .contains(location.collectionName);
            if (exists && !hasMarker(collection)) {
                if (!schemaReset) {
                    throw new StoreException("Collection " + location + " exists without the schema marker '"
                            + MongoLocation.MARKER + "' (e.g. a collection of distributed-caffeine 0.x). Stop all "
                            + "instances using it, then drop it or configure another name, or enable withSchemaReset().",
                            false);
                }
                LOGGER.log(Level.WARNING, "Dropping collection {0}, which has no schema marker (withSchemaReset)",
                        location);
                collection.drop();
                exists = false;
            }
            if (!exists) {
                collection.updateOne(Filters.eq("_id", MongoLocation.MARKER_ID),
                        Updates.setOnInsert("schema", MongoLocation.MARKER), new UpdateOptions().upsert(true));
                collection.createIndex(INDEX, new IndexOptions().name("distributed-caffeine"));
            }
            checkIndex(collection, location);
        } catch (MongoException e) {
            throw new StoreException("Checking collection " + location + " failed: " + e.getMessage(), e, false);
        }
    }

    // hello, or isMaster on servers that do not know it yet (MongoDB 4.4.0 and 4.4.1): the same answer for what is
    // read of it (msg, setName, localTime)
    static Document hello(Function<Document, Document> command) {
        try {
            return command.apply(new Document("hello", 1));
        } catch (MongoCommandException e) {
            if (e.getErrorCode() != COMMAND_NOT_FOUND) {
                throw e;
            }
            return command.apply(new Document("isMaster", 1));
        }
    }

    // change streams need a replica set (or mongos), the reads need majority read concern, and a sharded collection
    // could not keep the per-key order of its events
    private static void checkDeployment(MongoClient client, MongoDatabase database, MongoLocation location) {
        Document hello = hello(client.getDatabase("admin")::runCommand);
        boolean mongos = "isdbgrid".equals(hello.getString("msg"));
        if (!mongos && hello.getString("setName") == null) {
            throw new StoreException("MongoDB is a standalone server; distributed-caffeine needs a replica set "
                    + "(change streams and majority reads), a single-node one will do", false);
        }
        try {
            database.getCollection(location.collectionName).find().limit(1).first();
        } catch (MongoCommandException e) {
            throw new StoreException("MongoDB does not support majority read concern here: " + e.getMessage(), e,
                    false);
        }
        if (mongos) {
            Document sharded = client.getDatabase("config").getCollection("collections")
                    .find(Filters.and(Filters.eq("_id", location.databaseName + "." + location.collectionName),
                            Filters.ne("dropped", true))).first();
            if (sharded != null) {
                throw new StoreException("Collection " + location + " is sharded, which distributed-caffeine does "
                        + "not support", false);
            }
        }
    }

    private static boolean hasMarker(MongoCollection<BsonDocument> collection) {
        BsonDocument marker = collection.find(Filters.eq("_id", MongoLocation.MARKER_ID)).first();
        return marker != null && new BsonString(MongoLocation.MARKER).equals(marker.get("schema"));
    }

    private static void checkIndex(MongoCollection<BsonDocument> collection, MongoLocation location) {
        for (BsonDocument index : collection.listIndexes(BsonDocument.class)) {
            if (INDEX.equals(index.getDocument("key"))) {
                return;
            }
        }
        LOGGER.log(Level.WARNING, "Collection {0} has no index on (_id.d, state, writeTime); expiry and pruning will "
                + "be slow. Create it with: db.{1}.createIndex('{'\"_id.d\": 1, state: 1, writeTime: 1'}')", location,
                location.collectionName);
    }
}
