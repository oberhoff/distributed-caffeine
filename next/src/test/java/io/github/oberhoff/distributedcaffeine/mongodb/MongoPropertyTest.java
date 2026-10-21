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
import com.mongodb.client.MongoClients;
import io.github.oberhoff.distributedcaffeine.InternalPropertyTest;
import io.github.oberhoff.distributedcaffeine.PropertyBackend;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.testing.SilencingProxy;
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
 * The property test (freshness and convergence, see {@link InternalPropertyTest}) on MongoDB: real latency, real change
 * events through the shared change stream, and as fault a lost history (simulated: the watcher reopens without its
 * resume token and reports a gap).
 */
@Timeout(600)
abstract class MongoPropertyTest {

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4})
    void the_store_stays_fresh_and_every_instance_converges_on_it(long seed) throws Exception {
        assertThat(new InternalPropertyTest().run(seed, new MongoBackend(), 25)).as("phases checked").isEqualTo(25);
    }

    private static final class MongoBackend implements PropertyBackend {

        private final MongoTestDatabase database = MongoTestDatabase.get();
        private final String collection = MongoTestDatabase.freshCollection();
        private final MongoLocation location = new MongoLocation("property", collection,
                MongoAdapter.DEFAULT_DISCRIMINATOR, Format.TEXT, Format.TEXT);
        // the instances' client, through a proxy that can slow every round trip down
        private final SilencingProxy proxy = proxy(database);
        private final MongoClient client = MongoClients.create("mongodb://127.0.0.1:" + proxy.port()
                + "/?directConnection=true");

        private static SilencingProxy proxy(MongoTestDatabase database) {
            try {
                return new SilencingProxy(database.host(), database.port());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public Store open(StoreSettings settings) {
            return MongoAdapter.newBuilder(client, "property", collection).build()
                    .createStore(settings);
        }

        @Override
        public void fault(Random random, Consumer<String> trace) {
            double choice = random.nextDouble();
            pause(random.nextInt(5));
            if (choice < 0.2) {
                trace.accept("FAULT history lost");
                MongoWatcher.simulateHistoryLost(client, location);
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
            return MongoWatcher.barrier(client, location);
        }

        @Override
        public void close() {
            client.close();
            try {
                proxy.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

    }
}
