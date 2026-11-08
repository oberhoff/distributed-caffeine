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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
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
import org.bson.BsonType;
import org.bson.Document;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

/**
 * On start (VISION.md, "Adapter setup" and "Migration"): a replica set with majority reads is required, sharded
 * collections are refused; a collection with the schema marker of this version is used, a missing one is created (the
 * marker document first, which creates the collection, so a concurrent start sees either nothing or the marker), one
 * without the marker is refused or, with withSchemaReset() and if it may be dropped (see requireResettable), dropped
 * and recreated. A missing index only warns.
 *
 * @author Andreas Oberhoff
 */
final class MongoSchema {

    private static final Logger LOGGER = System.getLogger(MongoAdapter.class.getPackageName());
    private static final int COMMAND_NOT_FOUND = 59;
    private static final int UNAUTHORIZED = 13;
    // the field of the marker document naming the schema
    private static final String MARKER_FIELD = "schema";
    // the wire version of MongoDB 4.4, the oldest supported release (README, Requirements)
    private static final int MINIMUM_WIRE_VERSION = 9;
    // the hash last, so that pruning to size ranks by the index alone (write time, ties by hash): without it, every
    // batch sorted all the cache's values in memory (a million in 194 s, measured)
    private static final BsonDocument INDEX = new BsonDocument("_id.d", new BsonInt32(1))
            .append("state", new BsonInt32(1)).append("writeTime", new BsonInt32(1)).append("_id.h", new BsonInt32(1));

    private MongoSchema() {
    }

    static MongoDatabase database(MongoClient client, MongoLocation location) {
        return client.getDatabase(location.databaseName)
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                // majority, never w:1: change streams deliver only majority-committed writes, and with w:1 nothing
                // forces the journal flush they wait for, so messages arrive about 100 times later (measured: 52 ms
                // instead of 0.6 ms at p50). Journaled, whatever writeConcernMajorityJournalDefault says: a confirmed
                // write survives a crash of the majority (store contract point 3; the change stream may resume past a
                // lost write unnoticed)
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
                requireResettable(collection, location, schemaReset);
                LOGGER.log(Level.WARNING, "Dropping collection {0} of distributed-caffeine 0.x, or empty "
                        + "(withSchemaReset)", location);
                collection.drop();
                exists = false;
            }
            if (!exists) {
                collection.updateOne(Filters.eq("_id", MongoLocation.MARKER_ID),
                        Updates.setOnInsert(MARKER_FIELD, MongoLocation.MARKER), new UpdateOptions().upsert(true));
                collection.createIndex(INDEX, new IndexOptions().name("distributed-caffeine"));
                LOGGER.log(Level.INFO, "Created collection {0} with its schema marker and index", location);
            }
            checkIndex(collection, location);
            removeOldGeneration(collection, location);
        } catch (MongoException e) {
            // a user without a right the cache needs, named as the other adapters name theirs (PostgreSQL and
            // MariaDB/MySQL print their DDL): every operation of the cache falls under the built-in role readWrite
            if (e.getCode() == UNAUTHORIZED) {
                throw new StoreException("The user may not use collection " + location + " as the cache needs ("
                        + e.getMessage() + "); grant it the role readWrite on database " + location.databaseName
                        + " (through mongos also read on the config database)", e, false);
            }
            // a store not reached is worth another try, as in every operation; anything else is the setup's
            throw new StoreException("Checking collection " + location + " failed: " + e.getMessage(), e, false,
                    e instanceof MongoSocketException || e instanceof MongoTimeoutException);
        }
    }

    // The documents of distributed-caffeine 0.7.x that a rolling update with withSchemaReset() leaves behind: written
    // by old instances still running after the first new one dropped their collection, and ignored by this generation
    // forever. Only theirs have an ObjectId as _id (the server assigns it to their upserts; ours are the marker's
    // string and {d, h} documents), so the built-in _id index finds them without a scan (explain on MongoDB 4.4 and 9:
    // only their keys examined, none once they are gone). Run on every start, as a start after the rollout removes
    // what the last old instances wrote: running old instances lose their store copies, as with the drop. Best
    // effort: a failure (a timeout after a long rollout) only warns, deleteMany keeps what it removed, the next start
    // goes on. Deletes are outside the change stream's filter, so no cache of this generation hears of them.
    // To be removed in a later release, once no rollout from 0.7.x is expected (VISION.md, "Migration").
    private static void removeOldGeneration(MongoCollection<BsonDocument> collection, MongoLocation location) {
        try {
            long removed = collection.deleteMany(Filters.type("_id", BsonType.OBJECT_ID)).getDeletedCount();
            if (removed > 0) {
                LOGGER.log(Level.INFO, "Removed {0} documents of distributed-caffeine 0.7.x from collection {1}",
                        removed, location);
            }
        } catch (MongoException e) {
            LOGGER.log(Level.WARNING, "Removing documents of distributed-caffeine 0.7.x from collection {0} failed; "
                    + "the next start goes on: {1}", location, e.getMessage());
        }
    }

    // hello, or isMaster on servers that do not know it yet (MongoDB 4.4.0 and 4.4.1): the same answer for what is
    // read of it (msg, setName, localTime)
    static Document hello(UnaryOperator<Document> command) {
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
        // checked first, so that an old server is told so rather than failing a later check or operation unclearly;
        // hello reports the highest wire version the server speaks (through mongos, the router's)
        int wireVersion = hello.getInteger("maxWireVersion", 0);
        if (wireVersion < MINIMUM_WIRE_VERSION) {
            throw new StoreException("MongoDB with wire version " + wireVersion + " is older than the oldest "
                    + "supported, 4.4 (wire version " + MINIMUM_WIRE_VERSION + ")", false);
        }
        boolean mongos = "isdbgrid".equals(hello.getString("msg"));
        if (!mongos && hello.getString("setName") == null) {
            throw new StoreException("MongoDB is a standalone server; distributed-caffeine needs a replica set "
                    + "(change streams and majority reads), a single-node one will do", false);
        }
        try {
            database.getCollection(location.collectionName).find().limit(1).first();
        } catch (MongoCommandException e) {
            if (e.getCode() == UNAUTHORIZED) {
                throw e; // a missing right, named by ensure
            }
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

    // What withSchemaReset() may drop: a collection of distributed-caffeine 0.x (its documents have a top-level
    // discriminator), one of this version whose marker was lost (a value's _id is {d, h}; a start dropping it while
    // another one set it up recreates it so on the next write), or an empty one. Never one of another version of
    // this library, even with the option: two versions left with it on would drop each other's collection on every
    // start (a later version decides itself which earlier markers it resets). Never foreign documents either.
    private static void requireResettable(MongoCollection<BsonDocument> collection, MongoLocation location,
            boolean schemaReset) {
        BsonDocument marker = collection.find(Filters.eq("_id", MongoLocation.MARKER_ID)).first();
        if (marker != null) {
            throw new StoreException("Collection " + location + " was created by another version of "
                    + "distributed-caffeine (" + (marker.isString(MARKER_FIELD) ? "'" + marker.getString(MARKER_FIELD)
                    .getValue() + "'" : "an unknown schema") + "; this one uses '" + MongoLocation.MARKER + "'). Stop "
                    + "all instances of that version, then drop it or configure another name. withSchemaReset() "
                    + "never drops it, so that two versions cannot drop each other's collection.", false);
        }
        if (!schemaReset) {
            throw new StoreException("Collection " + location + " exists without the schema marker '"
                    + MongoLocation.MARKER + "' (e.g. a collection of distributed-caffeine 0.x). Stop all "
                    + "instances using it, then drop it or configure another name, or enable withSchemaReset().",
                    false);
        }
        boolean empty = collection.find().limit(1).first() == null;
        if (!empty && collection.find(Filters.or(Filters.exists("discriminator"), Filters.exists("_id.d")))
                .limit(1).first() == null) {
            throw new StoreException("Collection " + location + " holds documents that are not distributed-"
                    + "caffeine's; withSchemaReset() drops only collections of distributed-caffeine. Configure "
                    + "another name, or drop it if it is no longer needed.", false);
        }
    }

    private static boolean hasMarker(MongoCollection<BsonDocument> collection) {
        BsonDocument marker = collection.find(Filters.eq("_id", MongoLocation.MARKER_ID)).first();
        return marker != null && new BsonString(MongoLocation.MARKER).equals(marker.get(MARKER_FIELD));
    }

    private static void checkIndex(MongoCollection<BsonDocument> collection, MongoLocation location) {
        for (BsonDocument index : collection.listIndexes(BsonDocument.class)) {
            if (INDEX.equals(index.getDocument("key"))) {
                return;
            }
        }
        LOGGER.log(Level.WARNING, "Collection {0} has no index on (_id.d, state, writeTime, _id.h); expiry and pruning "
                + "will be slow. Create it with: db.{1}.createIndex('{'\"_id.d\": 1, state: 1, writeTime: 1, "
                + "\"_id.h\": 1'}')", location, location.collectionName);
    }
}
