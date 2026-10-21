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

import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.nio.charset.StandardCharsets;

// One cache's place in MongoDB: database, collection, discriminator and formats; the document ids (a row's
// {d, h}, the control document's {d, c} carrying the last invalidateAll's time, the schema marker's string id), and
// the conversion of serialized keys and values to BSON per format.
final class MongoLocation {

    static final String MARKER_ID = "distributed-caffeine";
    static final String MARKER = "distributed-caffeine schema 1";
    static final String CONTROL = "invalidate-all";
    private static final JsonWriterSettings JSON = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();

    final String databaseName;
    final String collectionName;
    final String discriminator;
    final Format keyFormat;
    final Format valueFormat;
    final BsonDocument controlId;

    MongoLocation(String databaseName, String collectionName, String discriminator, Format keyFormat,
                  Format valueFormat) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.discriminator = discriminator;
        this.keyFormat = keyFormat;
        this.valueFormat = valueFormat;
        this.controlId = new BsonDocument("d", new BsonString(discriminator)).append("c", new BsonString(CONTROL));
    }

    // always built in the same field order, so that equality on _id finds it
    BsonDocument id(String hash) {
        return new BsonDocument("d", new BsonString(discriminator)).append("h", new BsonString(hash));
    }

    static BsonValue toBson(byte[] bytes, Format format) {
        return switch (format) {
            case BINARY -> new BsonBinary(bytes);
            case TEXT -> new BsonString(new String(bytes, StandardCharsets.UTF_8));
            case JSON -> BsonDocument.parse("{\"v\": " + new String(bytes, StandardCharsets.UTF_8) + "}").get("v");
        };
    }

    static byte[] fromBson(BsonValue value, Format format) {
        return switch (format) {
            case BINARY -> value.asBinary().getData();
            case TEXT -> value.asString().getValue().getBytes(StandardCharsets.UTF_8);
            case JSON -> {
                String json = new BsonDocument("v", value).toJson(JSON);
                // the JSON of a document with the one field v: what follows its colon, without the closing brace
                yield json.substring(json.indexOf(':') + 1, json.length() - 1).trim().getBytes(StandardCharsets.UTF_8);
            }
        };
    }

    @Override
    public String toString() {
        return databaseName + "." + collectionName;
    }
}
