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

import org.redisson.api.RedissonClient;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * The entry for Redisson ({@code org.redisson:redisson}): wraps the application's client for the cache. Redisson is
 * needed on the classpath only where this class is used.
 * <p>
 * Redisson's own timeouts and retries apply within the cache's operation timeout. Redisson replaces a connection the
 * network silently stopped carrying only when its ping fails ({@code pingConnectionInterval}, 30 seconds by default);
 * until then, commands on it time out. A shorter interval, such as 1 second, recovers sooner.
 *
 * @author Andreas Oberhoff
 */
public final class RedissonRedis {

    // what the class Javadoc leaves out: a command Redisson sends again after the bound passed is still applied at most
    // once (the store keeps each write request's result). A server Redisson cannot reach fails a command as a timeout
    // rather than a connection failure, as Redisson retries past the bound.

    private RedissonRedis() {
    }

    /**
     * Wraps a client of a single server, of one behind Sentinel, or of a cluster.
     *
     * @param client the client
     * @return the driver
     */
    public static RedisDriver of(RedissonClient client) {
        requireNonNull(client, "client cannot be null");
        return new Driver(client);
    }

    private static final class Driver extends RedisDriver {

        private final RedissonClient client;

        Driver(RedissonClient client) {
            super(client);
            this.client = client;
        }

        @Override
        RedisSession open(Duration timeout) {
            return new RedissonSession(client, timeout);
        }

        @Override
        public String toString() {
            return "Redisson";
        }
    }
}
