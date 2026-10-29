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

import org.jspecify.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

// What a server is, from INFO server, INFO memory and INFO replication (not CONFIG, which managed services often
// block): the product (Redis, Valkey or Dragonfly) and its version, the Redis version it is compatible with (the one
// checked), its mode (standalone, sentinel, cluster), its role (master, or slave for a replica; master where not
// reported), and its eviction policy (maxmemory_policy: Redis's and Valkey's, or Dragonfly's noeviction, or eviction in
// its --cache_mode), null where not reported
record RedisServer(String product, String version, String compatibleVersion, String mode, String role,
                   @Nullable String evictionPolicy) {

    // the oldest version supported: 6.2, for trimming streams by age (XADD … MINID)
    private static final int MINIMUM_MAJOR = 6;
    private static final int MINIMUM_MINOR = 2;

    RedisServer {
        requireNonNull(product, "product cannot be null");
        requireNonNull(version, "version cannot be null");
        requireNonNull(compatibleVersion, "compatibleVersion cannot be null");
        requireNonNull(mode, "mode cannot be null");
        requireNonNull(role, "role cannot be null");
    }

    // from the lines of INFO server ("key:value"). Valkey reports a valkey_version next to a redis_version kept for
    // compatibility (7.2.4 on every Valkey so far), so its own counts there (every Valkey is above the minimum);
    // Dragonfly reports a dragonfly_version ("df-v2.0.2") next to the Redis version it implements, which counts
    static RedisServer of(Map<String, String> info, Map<String, String> memory, Map<String, String> replication) {
        String role = replication.getOrDefault("role", "master");
        // Dragonfly before 1.7 reports no maxmemory_policy in --cache_mode, only cache_mode:cache
        String evictionPolicy = memory.get("maxmemory_policy");
        if (evictionPolicy == null && "cache".equals(memory.get("cache_mode"))) {
            evictionPolicy = "eviction";
        }
        String valkey = info.get("valkey_version");
        // dfly_version before Dragonfly 1.7
        String dragonfly = info.get("dragonfly_version");
        if (dragonfly == null) {
            dragonfly = info.get("dfly_version");
        }
        String redis = info.get("redis_version");
        String mode = info.getOrDefault("redis_mode", info.getOrDefault("server_mode", "standalone"));
        if (valkey != null) {
            return new RedisServer("Valkey", valkey, valkey, mode, role, evictionPolicy);
        }
        if (redis == null) {
            throw new IllegalStateException("INFO server reports no version: " + info.keySet());
        }
        if (dragonfly != null) {
            return new RedisServer("Dragonfly", dragonfly, redis, mode, role, evictionPolicy);
        }
        // Kvrocks (on disk, RocksDB) reports its own version beside the Redis version it implements, and no eviction
        String kvrocks = info.get("kvrocks_version");
        if (kvrocks != null) {
            return new RedisServer("Kvrocks", kvrocks, redis, mode, role, evictionPolicy);
        }
        return new RedisServer("Redis", redis, redis, mode, role, evictionPolicy);
    }

    // the fields of an INFO section's text: its "key:value" lines
    static Map<String, String> fields(String info) {
        Map<String, String> fields = new LinkedHashMap<>();
        info.lines().forEach(line -> {
            int colon = line.indexOf(':');
            if (colon > 0 && !line.startsWith("#")) {
                fields.put(line.substring(0, colon), line.substring(colon + 1).trim());
            }
        });
        return fields;
    }

    // whether the compatible version is at least the minimum supported
    boolean supported() {
        String version = compatibleVersion;
        int firstDot = version.indexOf('.');
        int actualMajor = Integer.parseInt(firstDot < 0 ? version : version.substring(0, firstDot));
        int actualMinor = 0;
        if (firstDot >= 0) {
            int secondDot = version.indexOf('.', firstDot + 1);
            actualMinor = Integer.parseInt(secondDot < 0 ? version.substring(firstDot + 1)
                    : version.substring(firstDot + 1, secondDot));
        }
        return actualMajor > MINIMUM_MAJOR || (actualMajor == MINIMUM_MAJOR && actualMinor >= MINIMUM_MINOR);
    }

    // whether the server is a replica, which applies only what its primary sends
    boolean replica() {
        return role.equals("slave") || role.equals("replica");
    }

    @Override
    public String toString() {
        return product + " " + version + " (" + mode + (replica() ? ", replica" : "") + ")";
    }
}
