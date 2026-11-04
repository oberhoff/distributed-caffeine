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

import io.github.oberhoff.distributedcaffeine.adapter.redis.RedisCommandException.Kind;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * One connection of the library's through the user's Lettuce client, opened when first needed and opened anew after it
 * was dropped: to a single server (or the primary Sentinel names), or a cluster connection, which routes each command
 * to the node owning its keys' slot and follows the slot when it moves (MOVED, ASK). Every command waits at most the
 * timeout (plus what the command itself may wait, a blocking read's block time) for its answer: Lettuce's own timeout
 * is the client's, shared by every cache on it, and 60 s unless the user set another, so the bound per cache is kept
 * here. A connection that did not answer in time is dropped: on a network that silently stopped carrying, it would fail
 * every later command too, and Lettuce only notices that by TCP keepalive, minutes later if ever
 *
 * @author Andreas Oberhoff
 */
final class LettuceConnection implements AutoCloseable {

    private final Supplier<StatefulConnection<byte[], byte[]>> opener;
    private final Duration timeout;
    private final Object lock = new Object();
    private @Nullable StatefulConnection<byte[], byte[]> connection;
    private boolean closed;

    // opener: connects the user's client (RedisClient or RedisClusterClient) with byte arrays for keys and values
    LettuceConnection(Supplier<StatefulConnection<byte[], byte[]>> opener, Duration timeout) {
        this.opener = opener;
        this.timeout = timeout;
    }

    // runs a command, waiting at most the timeout plus the given extra time for its answer (null where Redis answers
    // nil, as a script may); failures classified
    <T extends @Nullable Object> T run(String what, Duration extra,
                                       Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<T>> command) {
        return run(what, extra, null, command);
    }

    // the same on the primary serving the key's slot where the connection is a cluster's (a command without the key
    // among its arguments, which a cluster connection would otherwise send anywhere, or everywhere: SCRIPT LOAD, which
    // Kvrocks's replicas refuse); elsewhere, and where the cluster's view names no primary for it, as above
    <T extends @Nullable Object> T run(String what, Duration extra, byte @Nullable [] key,
                                       Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<T>> command) {
        StatefulConnection<byte[], byte[]> used = connection(what);
        RedisFuture<T> future;
        try {
            RedisClusterAsyncCommands<byte[], byte[]> commands = async(used);
            if (key != null && used instanceof StatefulRedisClusterConnection<byte[], byte[]> cluster) {
                RedisClusterNode primary = cluster.getPartitions().getMasterBySlot(SlotHash.getSlot(key));
                if (primary != null) {
                    commands = cluster.getConnection(primary.getNodeId()).async();
                }
            }
            future = command.apply(commands);
        } catch (RuntimeException e) {
            throw classified(what, e, used);
        }
        Duration bound = timeout.plus(extra);
        try {
            return future.get(bound.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            drop(used);
            throw new RedisCommandException(Kind.TIMEOUT, what + " got no answer within " + bound.toMillis() + " ms",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // the command may have been sent and may run: as unknown as a timeout
            throw new RedisCommandException(Kind.TIMEOUT, what + " was interrupted waiting for its answer", e);
        } catch (ExecutionException e) {
            throw classified(what, e.getCause() != null ? e.getCause() : e, used);
        }
    }

    // a blocking read without a thread waiting for the answer: the future completes, on Lettuce's threads, with the
    // answer or with the command's failure classified, a timeout dropping the connection as above
    <T extends @Nullable Object> CompletableFuture<T> readAsync(Duration block,
                                                                Function<RedisClusterAsyncCommands<byte[], byte[]>,
                                                                        RedisFuture<T>> command) {
        String what = "XREAD";
        StatefulConnection<byte[], byte[]> used;
        RedisFuture<T> future;
        try {
            used = connection(what);
        } catch (RedisCommandException e) {
            return CompletableFuture.failedFuture(e);
        }
        try {
            future = command.apply(async(used));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(classified(what, e, used));
        }
        Duration bound = timeout.plus(block);
        CompletableFuture<T> bounded = future.toCompletableFuture().orTimeout(bound.toNanos(), TimeUnit.NANOSECONDS);
        return bounded.handle((answer, failure) -> {
            if (failure == null) {
                return answer;
            }
            Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                    ? failure.getCause() : failure;
            if (cause instanceof TimeoutException) {
                drop(used);
                throw new RedisCommandException(Kind.TIMEOUT, what + " got no answer within " + bound.toMillis()
                        + " ms", cause);
            }
            throw classified(what, cause, used);
        });
    }

    // the commands of either kind of connection: a cluster connection's route by key, the others' go to its server
    private static RedisClusterAsyncCommands<byte[], byte[]> async(StatefulConnection<byte[], byte[]> connection) {
        if (connection instanceof StatefulRedisClusterConnection<byte[], byte[]> cluster) {
            return cluster.async();
        }
        return ((StatefulRedisConnection<byte[], byte[]>) connection).async();
    }

    // opened under the lock, so that concurrent first commands open one connection; the wait is bounded by the
    // client's connect timeout (10 s unless the user set another)
    private StatefulConnection<byte[], byte[]> connection(String what) {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("The connection was closed");
            }
            StatefulConnection<byte[], byte[]> current = connection;
            if (current == null) {
                try {
                    current = opener.get();
                } catch (RuntimeException e) {
                    throw new RedisCommandException(Kind.CONNECTION, what + " could not connect: " + e.getMessage(),
                            e);
                }
                connection = current;
            }
            return current;
        }
    }

    // drops a connection that failed, unless another command dropped it already; the next command opens a new one.
    // The same connection object, not an equal one; closed without waiting, as a silent one would not answer
    @SuppressWarnings({"ReferenceEquality", "FutureReturnValueIgnored"})
    private void drop(StatefulConnection<byte[], byte[]> failed) {
        synchronized (lock) {
            if (connection == failed) {
                connection = null;
            }
        }
        failed.closeAsync();
    }

    private RedisCommandException classified(String what, Throwable e, StatefulConnection<byte[], byte[]> used) {
        String message = what + " failed: " + e.getMessage();
        if (e instanceof RedisCommandTimeoutException) {
            // the client's own timeout came first (set lower than the cache's)
            drop(used);
            return new RedisCommandException(Kind.TIMEOUT, message, e);
        }
        if (e instanceof RedisCommandExecutionException) {
            // the server answered with an error: the connection is fine, unless it reaches a replica (READONLY): a
            // primary demoted by a failover, which Lettuce only learns of on connecting, through Sentinel. Redis's
            // Sentinel closes the demoted primary's client connections, which makes Lettuce reconnect; Dragonfly does
            // not (found: every write refused for good after a failover), so the connection is dropped here and the
            // next command connects to the primary Sentinel names now
            RedisCommandException answered = RedisCommandException.answered(what, e.getMessage(), e);
            if (RedisCommandException.hasCode(String.valueOf(e.getMessage()), "READONLY")) {
                drop(used);
            }
            return answered;
        }
        // anything else (the connection closed or reset, Netty failing): what was sent may have run
        drop(used);
        return new RedisCommandException(Kind.CONNECTION, message, e);
    }

    @Override
    public void close() {
        StatefulConnection<byte[], byte[]> open;
        synchronized (lock) {
            closed = true;
            open = connection;
            connection = null;
        }
        if (open != null) {
            open.close();
        }
    }
}
