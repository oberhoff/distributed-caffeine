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

import dev.failsafe.Policy;
import dev.failsafe.PolicyConfig;
import dev.failsafe.spi.AsyncExecutionInternal;
import dev.failsafe.spi.ExecutionInternal;
import dev.failsafe.spi.ExecutionResult;
import dev.failsafe.spi.FailsafeFuture;
import dev.failsafe.spi.PolicyExecutor;
import dev.failsafe.spi.Scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

class InternalTaskPolicy<R> implements Policy<R> {

    private final List<Consumer<ExecutionResult<R>>> postExecutionTasks;

    InternalTaskPolicy() {
        this.postExecutionTasks = new ArrayList<>();
    }

    InternalTaskPolicy<R> withPostExecutionTask(Consumer<ExecutionResult<R>> task) {
        postExecutionTasks.add(task);
        return this;
    }

    private void processPostExecutionTasks(ExecutionResult<R> result) {
        postExecutionTasks.forEach(task -> task.accept(result));
    }

    @Override
    public PolicyConfig<R> getConfig() {
        return new PolicyConfig<>() {
        };
    }

    @Override
    public PolicyExecutor<R> toExecutor(int policyIndex) {
        return new PolicyExecutor<>(this, policyIndex) {

            @Override
            public ExecutionResult<R> postExecute(ExecutionInternal<R> execution, ExecutionResult<R> result) {
                processPostExecutionTasks(result);
                return super.postExecute(execution, result);
            }

            @Override
            protected synchronized CompletableFuture<ExecutionResult<R>> postExecuteAsync(
                    AsyncExecutionInternal<R> execution, ExecutionResult<R> result, Scheduler scheduler,
                    FailsafeFuture<R> future) {
                processPostExecutionTasks(result);
                return super.postExecuteAsync(execution, result, scheduler, future);
            }
        };
    }
}
