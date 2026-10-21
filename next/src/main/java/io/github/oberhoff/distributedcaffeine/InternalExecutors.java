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

import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// The library's default store executor: a cached pool of daemon threads, shared by all caches of the JVM, whose
// idle threads end after 60 s. Blocking store I/O must not run on ForkJoinPool.commonPool, which it would starve.
final class InternalExecutors {

    private static final AtomicInteger THREADS = new AtomicInteger();
    private static final Executor STORE_EXECUTOR = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS,
            new SynchronousQueue<>(), runnable -> {
        Thread thread = new Thread(runnable, "distributed-caffeine-store-" + THREADS.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });

    private InternalExecutors() {
    }

    static Executor defaultStoreExecutor() {
        return STORE_EXECUTOR;
    }
}
