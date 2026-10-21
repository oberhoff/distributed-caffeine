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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import io.github.oberhoff.distributedcaffeine.CaffeineConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.DistributionMode;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The MongoDB adapter when the oplog rolls over behind the change stream, for real: on a single-member replica set
 * with an oplog of 1 MB that MongoDB truncates as soon as it can ({@link MongoTopology#SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG};
 * by default a checkpoint every 60 s and 5 minutes of history hold the truncation back), a stream held back from
 * reading, and writes enough to truncate the oplog past its position. The stream cannot resume from there, so it
 * reopens without its token and the caches are told about a gap; what changed meanwhile is found by the revalidation.
 */
@Timeout(300)
@SuppressWarnings("resource") // every cache a test opens is closed after it (@AfterEach)
abstract class MongoOplogTest {

    private static final Duration LONG = Duration.ofHours(1);
    private final MongoClient client = MongoTestDatabase.get().client;

    private final String collection = MongoTestDatabase.freshCollection();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        MongoWatcher.simulateLag(client, false);
        for (int index = closeables.size() - 1; index >= 0; index--) {
            closeables.get(index).close();
        }
    }

    private DistributedCache<String, String> cache(DistributionMode mode) {
        DistributedCache<String, String> cache = DistributedCaffeine.<String, String>newBuilder(
                        MongoAdapter.newBuilder(client, "oplog", collection).build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000))
                .withCaffeine(CaffeineConfigurer::recordStats)
                .withDistributionMode(mode)
                .build();
        closeables.add(cache);
        return cache;
    }

    // whether the oplog still holds a write to the cache's collection
    private boolean oplogHoldsWrites() {
        return client.getDatabase("local").getCollection("oplog.rs")
                .countDocuments(Filters.eq("ns", "oplog." + collection)) > 0;
    }

    @Test
    void an_oplog_rolled_over_behind_the_stream_is_a_gap_after_which_what_changed_is_found() {
        DistributedCache<String, String> writer = cache(DistributionMode.NONE);
        DistributedCache<String, String> holder = cache(DistributionMode.INVALIDATE);
        writer.put("k", "before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");
        long gaps = holder.distributedPolicy().stats().gapCount();

        MongoWatcher.simulateLag(client, true);
        writer.put("k", "written while the stream lagged");
        // many times the oplog's size, in another collection, until the oplog is truncated past the stream's position
        MongoCollection<Document> filler = client.getDatabase("oplog").getCollection("filler");
        String payload = "x".repeat(1_000_000);
        assertThat(awaitUpTo(() -> {
            for (int index = 0; index < 5; index++) {
                filler.insertOne(new Document("payload", payload));
            }
            return !oplogHoldsWrites();
        }, Duration.ofMinutes(2), Duration.ofMillis(500))).as("the oplog truncated past the stream").isTrue();
        MongoWatcher.simulateLag(client, false);

        // the next getMore fails with CappedPositionLost: the stream reopens without its token, a gap
        await(() -> holder.distributedPolicy().stats().gapCount() > gaps);
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("written while the stream lagged");
        writer.put("k", "written after the gap");
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("written after the gap");
    }
}
