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
package io.github.oberhoff.distributedcaffeine;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.github.oberhoff.distributedcaffeine.adapter.jdbc.MariaDbAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.jdbc.MySqlAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.postgresql.PostgresAdapter;
import io.github.oberhoff.distributedcaffeine.adapter.redis.LettuceRedis;
import io.github.oberhoff.distributedcaffeine.adapter.redis.RedisAdapter;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.testing.InMemoryDatabase;
import io.github.oberhoff.distributedcaffeine.testing.LogCapture;
import io.lettuce.core.RedisClient;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.time.Duration;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The name of a cache, its adapter's {@code toString()}: the product, the place and the discriminator, never connection
 * details; and the cache's log messages beginning with it.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class AdapterNameTest {

    // a data source the adapters keep but never call while built
    private static final DataSource NO_DATA_SOURCE = (DataSource) Proxy.newProxyInstance(
            DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> {
                throw new UnsupportedOperationException(method.getName());
            });

    @Test
    void each_adapter_names_its_product_place_and_discriminator() {
        try (MongoClient client = MongoClients.create("mongodb://user:secret@localhost:1")) {
            assertThat(MongoAdapter.newBuilder(client, "orders", "cache").build())
                    .hasToString("MongoDB orders.cache/default");
        }
        assertThat(PostgresAdapter.newBuilder(NO_DATA_SOURCE, "public", "cache").withDiscriminator("eu").build())
                .hasToString("PostgreSQL public.cache/eu");
        assertThat(MariaDbAdapter.newBuilder(NO_DATA_SOURCE, "cache").build()).hasToString("MariaDB cache/default");
        assertThat(MySqlAdapter.newBuilder(NO_DATA_SOURCE, "cache").build()).hasToString("MySQL cache/default");
        RedisClient client = RedisClient.create("redis://user:secret@localhost:1");
        try {
            assertThat(RedisAdapter.newBuilder(LettuceRedis.of(client), "cache").build())
                    .hasToString("Redis cache/default");
        } finally {
            client.shutdown();
        }
    }

    @Test
    void the_caches_log_messages_begin_with_its_name() {
        InMemoryDatabase database = new InMemoryDatabase();
        Adapter<String, String> adapter = new Adapter<>() {
            @Override
            public Store createStore(StoreSettings settings) {
                return database.openStore("cache");
            }

            @Override
            public String toString() {
                return "Test orders/eu";
            }
        };
        try (DistributedLoadingCache<String, String> cache = DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withPollInterval(Duration.ofMillis(10)))
                .build(key -> "v");
             LogCapture log = new LogCapture()) {
            database.setDown(true);
            for (int i = 0; i < InternalStoreGuard.THRESHOLD; i++) {
                cache.get("k" + i);
            }

            await(() -> log.count("Test orders/eu: The store is not answering") > 0);
            assertThat(log.count("Test orders/eu: The store is not answering")).as(log.text()).isEqualTo(1);
        }
    }
}
