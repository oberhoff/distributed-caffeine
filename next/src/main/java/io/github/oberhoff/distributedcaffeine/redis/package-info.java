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
 * A store adapter for Redis and Valkey (being built, VISION.md "Redis adapters"): one store over a small set of
 * commands, which a binding per client library provides ({@link io.github.oberhoff.distributedcaffeine.redis.LettuceRedis}
 * so far), so that only the client library in use must be on the classpath.
 */
@NullMarked
package io.github.oberhoff.distributedcaffeine.redis;

import org.jspecify.annotations.NullMarked;
