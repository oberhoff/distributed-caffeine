package io.github.oberhoff.distributedcaffeine;

import com.github.benmanes.caffeine.cache.Scheduler;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class InternalScheduler implements Scheduler {

    private final Scheduler scheduler;

    public InternalScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public Future<? extends @Nullable Object> schedule(Executor executor, Runnable command, long delay, TimeUnit unit) {
        // use a custom executor service and no delay
        return scheduler.schedule(executor, command, delay, unit);
    }
}
