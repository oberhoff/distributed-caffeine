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
package io.github.oberhoff.distributedcaffeine.testing;

import io.github.oberhoff.distributedcaffeine.spi.Change;
import io.github.oberhoff.distributedcaffeine.spi.ChangeScan;
import io.github.oberhoff.distributedcaffeine.spi.Store;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A store forwarding its storage and its change scan to another and offering no change feed, as an adapter for a
 * plain SQL or key-value store without notifications would: the core polls it (VISION.md, "The polling feed"). Counts
 * its scans, so that a test can wait for a poll that began after its last write.
 */
public class ScanOnlyStore extends StorageOnlyStore implements ChangeScan {

    private final ChangeScan scan;
    private final AtomicLong scans = new AtomicLong();

    public <S extends Store & ChangeScan> ScanOnlyStore(S delegate) {
        super(delegate);
        this.scan = delegate;
    }

    @Override
    public List<Change> changedWithin(Duration within) {
        scans.incrementAndGet();
        return scan.changedWithin(within);
    }

    /**
     * Returns how many scans began so far.
     *
     * @return the number of scans
     */
    public long scans() {
        return scans.get();
    }
}
