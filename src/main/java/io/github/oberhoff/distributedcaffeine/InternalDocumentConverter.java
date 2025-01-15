/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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

import io.github.oberhoff.distributedcaffeine.DistributedCaffeine.LazyInitializer;
import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.bson.Document;
import org.bson.types.Binary;

import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.EXPIRES;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.HASH;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.ID;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.KEY;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.STATUS;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.TOUCHED;
import static io.github.oberhoff.distributedcaffeine.InternalCacheDocument.VALUE;
import static java.lang.String.format;
import static java.util.Objects.isNull;

class InternalDocumentConverter<K, V> implements LazyInitializer<K, V> {

    private Serializer<K, ?> keySerializer;
    private Serializer<V, ?> valueSerializer;

    InternalDocumentConverter() {
        // see also initialize()
    }

    @Override
    public void initialize(DistributedCaffeine<K, V> distributedCaffeine) {
        this.keySerializer = distributedCaffeine.getKeySerializer();
        this.valueSerializer = distributedCaffeine.getValueSerializer();
    }

    Object toMongoKey(K key) throws Exception {
        return serialize(keySerializer, key);
    }

    Object toMongoValue(V value) throws Exception {
        return serialize(valueSerializer, value);
    }

    InternalCacheDocument<K, V> toCacheDocument(Document document) throws Exception {
        K deserializedKey = deserialize(keySerializer, KEY, document);
        V deserializedValue = deserialize(valueSerializer, VALUE, document);
        return new InternalCacheDocument<K, V>()
                .setId(document.getObjectId(ID))
                .setHash(document.getInteger(HASH))
                .setKey(deserializedKey)
                .setValue(deserializedValue)
                .setStatus(document.getString(STATUS))
                .setTouched(document.getDate(TOUCHED))
                .setExpires(document.getDate(EXPIRES));
    }

    @SuppressWarnings("unchecked")
    private <T> Object serialize(Serializer<T, ?> serializer, T value) throws Exception {
        Object serializedValue;
        if (isNull(value)) {
            serializedValue = null;
        } else if (serializer instanceof ByteArraySerializer) {
            ByteArraySerializer<T> byteArraySerializer = (ByteArraySerializer<T>) serializer;
            serializedValue = byteArraySerializer.serialize(value);
        } else if (serializer instanceof JsonSerializer) {
            JsonSerializer<T> jsonSerializer = (JsonSerializer<T>) serializer;
            if (jsonSerializer.storeAsBson()) {
                serializedValue = convertJsonToBson(jsonSerializer.serialize(value));
            } else {
                serializedValue = jsonSerializer.serialize(value);
            }
        } else if (serializer instanceof StringSerializer) {
            StringSerializer<T> stringSerializer = (StringSerializer<T>) serializer;
            serializedValue = stringSerializer.serialize(value);
        } else {
            throw new DistributedCaffeineException(format("Unknown serializer '%s' for serializing '%s'",
                    serializer.getClass().getName(), value));
        }
        return serializedValue;
    }

    @SuppressWarnings("unchecked")
    private <T> T deserialize(Serializer<T, ?> serializer, String documentKey, Document document) throws Exception {
        T deserializedValue;
        if (isNull(document.get(documentKey))) {
            deserializedValue = null;
        } else if (serializer instanceof ByteArraySerializer) {
            ByteArraySerializer<T> byteArraySerializer = (ByteArraySerializer<T>) serializer;
            deserializedValue = byteArraySerializer.deserialize(document.get(documentKey, Binary.class).getData());
        } else if (serializer instanceof JsonSerializer) {
            JsonSerializer<T> jsonSerializer = (JsonSerializer<T>) serializer;
            if (jsonSerializer.storeAsBson()) {
                deserializedValue = jsonSerializer.deserialize(convertBsonToJson(documentKey, document));
            } else {
                deserializedValue = jsonSerializer.deserialize(document.getString(documentKey));
            }
        } else if (serializer instanceof StringSerializer) {
            StringSerializer<T> stringSerializer = (StringSerializer<T>) serializer;
            deserializedValue = stringSerializer.deserialize(document.getString(documentKey));
        } else {
            throw new DistributedCaffeineException(format("Unknown serializer '%s' for deserializing '%s' of document '%s'",
                    serializer.getClass().getName(), documentKey, document));
        }
        return deserializedValue;
    }

    private Object convertJsonToBson(String json) {
        String jsonKey = "jsonKey";
        String documentJson = format("{\"%s\":%s}", jsonKey, json);
        Document document = Document.parse(documentJson);
        return document.get(jsonKey);
    }

    private String convertBsonToJson(String bsonKey, Document bson) {
        Document document = new Document(bsonKey, bson.get(bsonKey));
        String json = document.toJson();
        return json.substring(json.indexOf(':') + 1, json.lastIndexOf('}')).strip();
    }
}
