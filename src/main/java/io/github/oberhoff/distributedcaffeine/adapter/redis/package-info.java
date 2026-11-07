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

/**
 * The adapter for Redis, Valkey, Dragonfly and Kvrocks ({@link RedisAdapter}), distributing changes through Redis
 * Streams. Its client comes through an entry class per client library ({@link LettuceRedis}, {@link JedisRedis},
 * {@link RedissonRedis}), so that only the library in use must be on the classpath.
 *
 * @author Andreas Oberhoff
 * @see <a href="https://github.com/oberhoff/distributed-caffeine">Distributed Caffeine on GitHub</a>
 */
@NullMarked
package io.github.oberhoff.distributedcaffeine.adapter.redis;

import org.jspecify.annotations.NullMarked;
