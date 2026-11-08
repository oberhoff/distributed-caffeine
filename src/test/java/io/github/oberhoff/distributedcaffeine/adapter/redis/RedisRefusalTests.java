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

import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.testing.TestStoreSettings;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A server the adapter refuses at store creation, through each client library (the subclass names its image and the
 * reason the refusal must give): one older than supported by its reported version, or one whose version is supported
 * but that lacks a command the store needs, which the probe finds. A server running no scripts is refused alike
 * ({@link RedisAclTests}, a user not allowed them).
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class RedisRefusalTests {

    private final RedisTestServer server;
    private final String reason;

    // refused by the probe for a missing command
    RedisRefusalTests(String image) {
        this(image, "lacks commands the Redis adapter needs");
    }

    RedisRefusalTests(String image, String reason) {
        this.server = new RedisTestServer(image);
        this.reason = reason;
    }

    @AfterAll
    void stopServer() {
        server.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"Lettuce", "Jedis", "Redisson"})
    // the store's close runs only once it was created, which the test fails on anyway
    @SuppressWarnings("java:S5778")
    void the_server_is_refused_at_store_creation_naming_why(String library) {
        RedisDriver driver = switch (library) {
            case "Lettuce" -> LettuceRedis.of(server.client);
            case "Jedis" -> JedisRedis.of(server.jedis);
            default -> RedissonRedis.of(server.redisson);
        };
        RedisAdapter<String, String> adapter = RedisAdapter.newBuilder(driver, RedisTestServer.freshName()).build();

        StoreSettings settings = TestStoreSettings.of(Format.TEXT, Format.TEXT);
        assertThatThrownBy(() -> {
            try (Store ignored = adapter.createStore(settings)) {
                throw new AssertionError("the store was created");
            }
        })
                .isInstanceOf(StoreException.class)
                .hasMessageContaining(reason)
                .satisfies(e -> org.assertj.core.api.Assertions.assertThat(((StoreException) e).isRetryable())
                        .isFalse());
    }
}
