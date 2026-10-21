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
package io.github.oberhoff.distributedcaffeine;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The stats counter of L1 (the user's, or Caffeine's default), wrapped so that the core can count in Caffeine's stats
 * what Caffeine would count but the core does not run through it (the remappings of {@code compute}), and leave out
 * what Caffeine counts only because the core runs it differently: an {@code AsyncCache.put} of a completed future is
 * no load in Caffeine, but the core holds an incomplete future until the write has its version.
 */
final class InternalCaffeineStats implements StatsCounter {

    private final StatsCounter delegate;
    private final AtomicInteger loadSuccessesToSkip = new AtomicInteger();

    InternalCaffeineStats(StatsCounter delegate) {
        this.delegate = delegate;
    }

    // the next load success Caffeine records is left out (which one does not matter, only the count)
    void skipLoadSuccess() {
        loadSuccessesToSkip.incrementAndGet();
    }

    @Override
    public void recordHits(int count) {
        delegate.recordHits(count);
    }

    @Override
    public void recordMisses(int count) {
        delegate.recordMisses(count);
    }

    @Override
    public void recordLoadSuccess(long loadTime) {
        if (loadSuccessesToSkip.getAndUpdate(skip -> Math.max(0, skip - 1)) > 0) {
            return;
        }
        delegate.recordLoadSuccess(loadTime);
    }

    @Override
    public void recordLoadFailure(long loadTime) {
        delegate.recordLoadFailure(loadTime);
    }

    @Override
    public void recordEviction(int weight, RemovalCause cause) {
        delegate.recordEviction(weight, cause);
    }

    @Override
    public CacheStats snapshot() {
        return delegate.snapshot();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
