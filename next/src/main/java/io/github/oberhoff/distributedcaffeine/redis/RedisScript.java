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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import static java.util.Objects.requireNonNull;

// A Lua script with its SHA-1, under which the server caches it (EVALSHA), and the shape of its reply, which a client
// library decoding replies by a declared type needs (Redisson; Lettuce and Jedis decode any); the name is for messages
// only
record RedisScript(String name, String source, String sha, Reply reply) {

    // what a script returns: an array (of integers, bulk strings, arrays and nils), an integer, or a bulk string; nil
    // is allowed for each
    enum Reply {
        ARRAY, INTEGER, BULK
    }

    RedisScript {
        requireNonNull(name, "name cannot be null");
        requireNonNull(source, "source cannot be null");
        requireNonNull(sha, "sha cannot be null");
        requireNonNull(reply, "reply cannot be null");
    }

    @SuppressWarnings("java:S4790") // SHA-1 names the script for EVALSHA, as Redis defines it; nothing is secured
    static RedisScript of(String name, Reply reply, String source) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(source.getBytes(StandardCharsets.UTF_8));
            return new RedisScript(name, source, HexFormat.of().formatHex(digest), reply);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 is required of every Java platform", e);
        }
    }

    @Override
    public String toString() {
        return "script " + name;
    }
}
