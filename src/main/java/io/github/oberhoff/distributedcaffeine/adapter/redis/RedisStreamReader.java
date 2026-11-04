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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Reads streams on a connection of its own (XREAD BLOCK holds it while it waits). A read waits at most the given block
 * time for entries, plus the session's timeout for the answer; a connection that answers neither is dropped and the
 * read fails as a timeout
 *
 * @author Andreas Oberhoff
 */
interface RedisStreamReader extends AutoCloseable {

    // the entries of each stream after its given ID ("0-0" for all), at most count per stream, waiting up to block
    // for the first of any; by stream, only those with entries, none if none came
    Map<String, List<Entry>> read(Map<String, String> afterIds, Duration block, int count);

    // the same for one stream
    default List<Entry> read(String key, String afterId, Duration block, int count) {
        return read(Map.of(key, afterId), block, count).getOrDefault(key, List.of());
    }

    // the same, answered by a future, failing as the read would (a RedisCommandException). By default the read waits
    // on a thread of the feed's own (RedisFeed.BLOCKING_READS), a thread per read in flight; a client library reading
    // asynchronously answers without holding a thread
    default CompletableFuture<Map<String, List<Entry>>> readAsync(Map<String, String> afterIds, Duration block,
                                                                  int count) {
        return CompletableFuture.supplyAsync(() -> read(afterIds, block, count), RedisFeed.BLOCKING_READS);
    }

    @Override
    void close();

    // an entry: its ID ("<milliseconds>-<sequence>") and its fields, as text (the store's entries hold nothing else:
    // a number, a hash, a version's parts, a kind; Jedis's typed reads decode them as UTF-8 anyway)
    record Entry(String id, Map<String, String> fields) {

        public Entry {
            requireNonNull(id, "id cannot be null");
            fields = Map.copyOf(fields);
        }
    }
}
