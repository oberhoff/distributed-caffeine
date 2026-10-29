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
package io.github.oberhoff.distributedcaffeine.redis;

import io.github.oberhoff.distributedcaffeine.InternalPropertyTest;
import io.github.oberhoff.distributedcaffeine.PropertyBackend;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.testing.SilencingProxy;
import io.lettuce.core.RedisClient;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Random;
import java.util.function.Consumer;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The property test (freshness and convergence, see {@link InternalPropertyTest}) on Redis or Valkey: real latency,
 * real entries through the shared reader, and as faults a gap (simulated, as a skipped number reports it) and round
 * trips slowed down.
 */
@NullMarked
@Timeout(600)
abstract class RedisPropertyTest {

    // the server of the enclosing image
    protected abstract RedisTestServer server();

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4})
    void the_store_stays_fresh_and_every_instance_converges_on_it(long seed) throws Exception {
        assertThat(new InternalPropertyTest().run(seed, new RedisBackend(server()), 25)).as("phases checked")
                .isEqualTo(25);
    }

    private static final class RedisBackend implements PropertyBackend {

        // the instances' client, through a proxy that can slow every round trip down
        private final SilencingProxy proxy;
        private final RedisClient client;
        private final RedisCacheBackend backend;

        RedisBackend(RedisTestServer server) {
            try {
                this.proxy = new SilencingProxy(server.host(), server.port());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            this.client = RedisClient.create("redis://127.0.0.1:" + proxy.port());
            this.backend = new RedisCacheBackend(LettuceRedis.of(client));
        }

        @Override
        public Store open(StoreSettings settings) {
            return backend.open(settings);
        }

        @Override
        public void fault(Random random, Consumer<String> trace) {
            double choice = random.nextDouble();
            pause(random.nextInt(5));
            if (choice < 0.2) {
                trace.accept("FAULT gap");
                backend.induceGap();
            } else if (choice < 0.4) {
                long delay = 20L + random.nextInt(200);
                trace.accept("FAULT every round trip " + delay + " ms slower");
                proxy.delayRequests(Duration.ofMillis(delay));
            }
        }

        @Override
        public void heal() {
            proxy.delayRequests(Duration.ZERO);
        }

        @Override
        public boolean isIdle() {
            return backend.isIdle();
        }

        @Override
        public void close() {
            backend.close();
            client.shutdown();
            try {
                proxy.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
