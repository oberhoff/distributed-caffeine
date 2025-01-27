/*
 * Copyright © 2023-2025 Dr. Andreas Oberhoff (All rights reserved)
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

import java.util.concurrent.locks.ReentrantLock;

class InternalSynchronizationLock {

    private final ReentrantLock lock;

    InternalSynchronizationLock() {
        lock = new ReentrantLock();
    }

    void lock() {
        lock.lock();
    }

    boolean tryLock() {
        return lock.tryLock();
    }

    void unlock() {
        lock.unlock();
    }

    void tryUnlock(boolean locked) {
        if (locked) {
            lock.unlock();
        }
    }

    void ensure() {
        if (!lock.isLocked()) {
            throw new IllegalStateException("No synchronization lock found");
        }
    }
}
