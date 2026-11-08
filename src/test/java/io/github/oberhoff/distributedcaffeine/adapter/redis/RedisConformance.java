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
package io.github.oberhoff.distributedcaffeine.adapter.redis;

import io.github.oberhoff.distributedcaffeine.AdapterConformanceTest.AdapterConformance;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.lettuce.core.RedisClient;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.Locale;

/**
 * The Redis family's side of the conformance suite, on a fresh keyspace of the running server.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class RedisConformance implements AdapterConformance {

    private final RedisTestServer server;
    private final String keyspace = RedisTestServer.freshName();
    private @Nullable RedisClient unreachable;

    RedisConformance(RedisTestServer server) {
        this.server = server;
    }

    @Override
    public Adapter<String, String> adapter(String discriminator) {
        return RedisAdapter.newBuilder(LettuceRedis.of(server.client), keyspace).withDiscriminator(discriminator)
                .build();
    }

    @Override
    public void markOtherVersion() {
        server.admin.sync().set(new RedisKeys(keyspace, RedisAdapter.DEFAULT_DISCRIMINATOR).schema(), "2");
    }

    // a keyspace is a hash tag of the cache's own, which no one else writes under
    @Override
    public boolean putForeignData() {
        return false;
    }

    @Override
    public Adapter<String, String> unreachable() {
        unreachable = RedisClient.create("redis://127.0.0.1:1");
        return RedisAdapter.newBuilder(LettuceRedis.of(unreachable), keyspace).build();
    }

    @Override
    public String product() {
        String image = server.image.toLowerCase(Locale.ROOT);
        return image.contains("valkey") ? "Valkey" : image.contains("dragonfly") ? "Dragonfly"
                : image.contains("kvrocks") ? "Kvrocks" : "Redis";
    }

    @Override
    public void close() {
        if (unreachable != null) {
            unreachable.shutdown();
        }
    }
}
