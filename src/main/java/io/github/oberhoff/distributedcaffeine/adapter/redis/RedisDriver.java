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

import java.time.Duration;

/**
 * An application's Redis client, wrapped for the cache by its library's entry class ({@link LettuceRedis},
 * {@link JedisRedis}, {@link RedissonRedis}). The client stays the application's; what the cache opens through it, it
 * closes again.
 *
 * @author Andreas Oberhoff
 */
public abstract class RedisDriver {

    // only the bindings in this package make drivers: the commands they provide are the library's internals
    RedisDriver() {
    }

    /**
     * Opens a session of one cache: its commands run on connections of the client, each bounded by the timeout, so
     * that caches sharing a client may have different ones.
     *
     * @param timeout how long a command waits for an answer (positive)
     * @return the session
     */
    abstract RedisSession open(Duration timeout);
}
