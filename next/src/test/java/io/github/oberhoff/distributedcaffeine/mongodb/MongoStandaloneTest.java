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
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.DistributionMode;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The MongoDB adapter on a server that is no replica set: refused at start with a message naming the remedy, also
 * without distribution, since the store needs majority reads and the cluster time as well as change streams. On
 * {@link MongoTopology#STANDALONE_SERVER}.
 */
@NullMarked
@Timeout(120)
abstract class MongoStandaloneTest {

    private final MongoClient client = MongoTestDatabase.get().client;

    private DistributedCaffeine<String, String> builder(DistributionMode distributionMode) {
        return DistributedCaffeine.<String, String>newBuilder(MongoAdapter.newBuilder(client, "standalone",
                        MongoTestDatabase.freshCollection()).build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(1_000))
                .withDistributionMode(distributionMode);
    }

    @Test
    void a_standalone_server_is_refused_at_start_naming_the_remedy() {
        for (DistributionMode distributionMode : DistributionMode.values()) {
            DistributedCaffeine<String, String> builder = builder(distributionMode);

            assertThatThrownBy(builder::build).as(distributionMode.toString())
                    .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.getMessage())
                            .contains("standalone server", "a single-node one will do"));
        }
    }
}
