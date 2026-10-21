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

import org.bson.BsonTimestamp;
import org.jspecify.annotations.Nullable;

/**
 * The latest cluster time a store knows of: of its own operations, and of the change events handed to its cache.
 * Every operation of the store runs in a causally consistent session advanced to it, so a member answers a read only
 * once its majority snapshot includes everything the cache already saw. Without it, a member that rolled back and was
 * elected primary again answered majority reads from a snapshot older than a majority-committed write whose event the
 * cache had already received (measured on MongoDB 5 by the failover test): the cache then held the older value, and
 * no further event would ever correct it.
 */
final class MongoClusterTime {

    private @Nullable BsonTimestamp latest;

    /**
     * Moves the time forward; an earlier time (or none) leaves it as it is.
     *
     * @param time the time seen
     */
    synchronized void advance(@Nullable BsonTimestamp time) {
        if (time != null && (latest == null || time.compareTo(latest) > 0)) {
            latest = time;
        }
    }

    /**
     * Returns the latest time known, or {@code null} before the first.
     *
     * @return the time
     */
    synchronized @Nullable BsonTimestamp get() {
        return latest;
    }
}
