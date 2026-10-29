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

import com.mongodb.client.MongoClient;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The MongoDB adapter on a sharded cluster, reached through {@code mongos}: a collection that is not sharded works
 * (writes, reads and the change stream go through the router), a sharded one is refused at start, since its documents
 * would be spread over shards whose writes the version rules cannot order. On
 * {@link MongoTopology#SHARDED_CLUSTER}.
 */
@NullMarked
@Timeout(300)
abstract class MongoShardedTest {

    private static final Duration LONG = Duration.ofHours(1);
    private final MongoClient client = MongoTestDatabase.get().client;

    private DistributedCaffeine<String, String> builder(String collection) {
        return DistributedCaffeine.<String, String>newBuilder(MongoAdapter.newBuilder(client, "cluster", collection)
                        .build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000));
    }

    @Test
    void a_collection_that_is_not_sharded_works_through_the_router() {
        try (DistributedCache<String, String> writer = builder("plain").build();
             DistributedCache<String, String> holder = builder("plain").build()) {
            writer.put("k", "old");
            assertThat(holder.getIfPresent("k")).isEqualTo("old");

            writer.put("k", "new");

            await(() -> holder.policy().getIfPresentQuietly("k") == null);
            assertThat(holder.getIfPresent("k")).isEqualTo("new");
        }
    }

    @Test
    void a_sharded_collection_is_refused_at_start() {
        client.getDatabase("admin").runCommand(new Document("enableSharding", "cluster")); // needed before 6.0
        client.getDatabase("admin").runCommand(new Document("shardCollection", "cluster.spread")
                .append("key", new Document("_id", 1)));
        DistributedCaffeine<String, String> builder = builder("spread");

        assertThatThrownBy(builder::build).isInstanceOf(StoreException.class).hasMessageContaining("is sharded");
    }
}
