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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A driver whose server answers as a single one but refuses a stream read across hash slots, as a managed service
 * sharding behind one endpoint does (Azure Managed Redis under its Enterprise cluster policy): a read of streams of
 * several hash tags fails with Redis's own {@code CROSSSLOT} error, as the client library reports it. Everything else
 * goes to the wrapped driver's server unchanged.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class RedisSlotEnforcingDriver extends RedisDriver {

    private final RedisDriver inner;
    // the reads refused, for the tests
    final AtomicInteger refused = new AtomicInteger();

    RedisSlotEnforcingDriver(RedisDriver inner) {
        this.inner = inner;
    }

    // the wrapped session is closed with the one returned, by its owner
    @SuppressWarnings("resource")
    @Override
    RedisSession open(Duration timeout) {
        RedisSession session = inner.open(timeout);
        return new RedisSession() {
            @Override
            public @Nullable Object eval(RedisScript script, List<String> keys, List<byte[]> arguments) {
                return session.eval(script, keys, arguments);
            }

            @Override
            public RedisServer server() {
                return session.server();
            }

            @Override
            public RedisStreamReader openReader() {
                return new Enforcing(session.openReader());
            }

            @Override
            public boolean clustered() {
                return session.clustered();
            }

            @Override
            public void close() {
                session.close();
            }
        };
    }

    private final class Enforcing implements RedisStreamReader {

        private final RedisStreamReader reader;

        Enforcing(RedisStreamReader reader) {
            this.reader = reader;
        }

        @Override
        public Map<String, List<Entry>> read(Map<String, String> afterIds, Duration block, int count) {
            refuseAcrossSlots(afterIds);
            return reader.read(afterIds, block, count);
        }

        @Override
        public CompletableFuture<Map<String, List<Entry>>> readAsync(Map<String, String> afterIds, Duration block,
                                                                     int count) {
            try {
                refuseAcrossSlots(afterIds);
            } catch (RedisCommandException e) {
                return CompletableFuture.failedFuture(e);
            }
            return reader.readAsync(afterIds, block, count);
        }

        // keys of several hash tags ({...}) are in several slots, which such a server refuses in one command
        private void refuseAcrossSlots(Map<String, String> afterIds) {
            long tags = afterIds.keySet().stream().map(key -> key.substring(key.indexOf('{'), key.indexOf('}') + 1))
                    .distinct().count();
            if (tags > 1) {
                refused.incrementAndGet();
                throw RedisCommandException.answered("XREAD", "CROSSSLOT Keys in request don't hash to the same slot",
                        new IllegalStateException("refused by the test"));
            }
        }

        @Override
        public void close() {
            reader.close();
        }
    }

    @Override
    public String toString() {
        return inner + " (refusing reads across slots)";
    }
}
