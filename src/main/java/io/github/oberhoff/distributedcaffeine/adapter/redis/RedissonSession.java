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
import org.jspecify.annotations.Nullable;
import org.redisson.api.RFuture;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.redisnode.RedisClusterMaster;
import org.redisson.api.redisnode.RedisNode;
import org.redisson.api.redisnode.RedisNodeAsync;
import org.redisson.api.redisnode.RedisNodes;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamMultiReadArgs;
import org.redisson.client.RedisBusyException;
import org.redisson.client.RedisClusterDownException;
import org.redisson.client.RedisConnectionException;
import org.redisson.client.RedisException;
import org.redisson.client.RedisLoadingException;
import org.redisson.client.RedisMasterDownException;
import org.redisson.client.RedisNoReplicasException;
import org.redisson.client.RedisNoScriptException;
import org.redisson.client.RedisOutOfMemoryException;
import org.redisson.client.RedisReadonlyException;
import org.redisson.client.RedisTimeoutException;
import org.redisson.client.RedisTryAgainException;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.client.codec.StringCodec;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * The commands of one cache over Redisson: through the client's async API, each waited for at most the timeout (plus
 * what the command itself may wait, a blocking read's block time), then cancelled. Scripts run on the primary
 * (READ_WRITE), their replies decoded by the type their RedisScript declares, values as bytes; stream entries are text.
 * Redisson's own timeout and retries apply within the bound; a write it sends again after the bound passed is still
 * applied once (RedisScripts#WRITE keeps each request's result). In a cluster, Redisson routes a script by its first
 * key and a read by its first stream (a cache's keys and a keyspace's streams share one slot), following redirects
 *
 * @author Andreas Oberhoff
 */
final class RedissonSession implements RedisSession {

    private final RedissonClient client;
    private final Duration timeout;
    private final RScript scripts;

    RedissonSession(RedissonClient client, Duration timeout) {
        this.client = client;
        this.timeout = timeout;
        this.scripts = client.getScript(ByteArrayCodec.INSTANCE);
    }

    @Override
    public @Nullable Object eval(RedisScript script, List<String> keys, List<byte[]> arguments) {
        List<Object> keyObjects = new ArrayList<>(keys);
        Object[] values = arguments.toArray();
        RScript.ReturnType type = switch (script.reply()) {
            case ARRAY -> RScript.ReturnType.LIST;
            case INTEGER -> RScript.ReturnType.LONG;
            case BULK -> RScript.ReturnType.VALUE;
        };
        try {
            return await(scripts.evalShaAsync(RScript.Mode.READ_WRITE, script.sha(), type, keyObjects, values),
                    timeout, script.toString());
        } catch (RedisCommandException e) {
            if (!(e.getCause() instanceof RedisNoScriptException)) {
                throw e;
            }
            // not cached on the server (a restart, a failover, SCRIPT FLUSH): loaded where the key is served (SCRIPT
            // LOAD; Kvrocks caches no script EVAL runs, so EVAL alone would miss every time) and run by its SHA again;
            // where loading is refused (a user without SCRIPT LOAD, a node taking none) or the script is still
            // missing, sent in full
            try {
                await(keys.isEmpty() ? scripts.scriptLoadAsync(script.source())
                        : scripts.scriptLoadAsync(keys.get(0), script.source()), timeout, "SCRIPT LOAD " + script);
                return await(scripts.evalShaAsync(RScript.Mode.READ_WRITE, script.sha(), type, keyObjects, values),
                        timeout, script.toString());
            } catch (RedisCommandException again) {
                if (RedisCommandException.mayHaveRun(again)) {
                    throw again;
                }
            }
            return await(scripts.evalAsync(RScript.Mode.READ_WRITE, script.source(), type, keyObjects, values),
                    timeout, script.toString());
        }
    }

    @Override
    public RedisServer server() {
        RedisNodeAsync node = primary();
        Map<String, String> server = requireNonNull(await(node.infoAsync(RedisNode.InfoSection.SERVER), timeout,
                "INFO server"), "INFO server answered nil");
        Map<String, String> memory = requireNonNull(await(node.infoAsync(RedisNode.InfoSection.MEMORY), timeout,
                "INFO memory"), "INFO memory answered nil");
        Map<String, String> replication = requireNonNull(await(node.infoAsync(RedisNode.InfoSection.REPLICATION),
                timeout, "INFO replication"), "INFO replication answered nil");
        return RedisServer.of(server, memory, replication);
    }

    // a node to ask what the server is: the single server, the primary Sentinel names at the time, or a cluster's
    // first primary (its nodes alike)
    private RedisNodeAsync primary() {
        if (client.getConfig().isClusterConfig()) {
            Iterator<RedisClusterMaster> masters = client.getRedisNodes(RedisNodes.CLUSTER).getMasters().iterator();
            if (!masters.hasNext()) {
                throw new RedisCommandException(Kind.CONNECTION, "INFO found no primary of the cluster",
                        new IllegalStateException("no primary"));
            }
            return masters.next();
        }
        if (client.getConfig().isSentinelConfig()) {
            return client.getRedisNodes(RedisNodes.SENTINEL_MASTER_SLAVE).getMaster();
        }
        if (client.getConfig().isSingleConfig()) {
            return client.getRedisNodes(RedisNodes.SINGLE).getInstance();
        }
        return client.getRedisNodes(RedisNodes.MASTER_SLAVE).getMaster();
    }

    @Override
    public RedisStreamReader openReader() {
        return new Reader(client, timeout);
    }

    @Override
    public boolean clustered() {
        return client.getConfig().isClusterConfig();
    }

    @Override
    public void close() {
        // nothing of its own: the client and its connections are the user's
    }

    // waits for a command, at most the bound, cancelling it after that; failures classified
    static <T extends @Nullable Object> T await(RFuture<T> future, Duration bound, String what) {
        try {
            return future.toCompletableFuture().get(bound.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            future.cancel(false);
            throw new RedisCommandException(Kind.TIMEOUT, what + " got no answer within " + bound.toMillis() + " ms",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(false);
            // the command may have been sent and may run: as unknown as a timeout
            throw new RedisCommandException(Kind.TIMEOUT, what + " was interrupted waiting for its answer", e);
        } catch (CancellationException e) {
            throw new RedisCommandException(Kind.CONNECTION, what + " was cancelled", e);
        } catch (ExecutionException e) {
            throw classified(what, e.getCause() != null ? e.getCause() : e);
        }
    }

    private static RedisCommandException classified(String what, Throwable e) {
        String message = what + " failed: " + e.getMessage();
        if (e instanceof RedisNoScriptException) {
            return new RedisCommandException(Kind.REFUSED, message, e);
        }
        if (e instanceof RedisReadonlyException || e instanceof RedisLoadingException || e instanceof RedisBusyException
                || e instanceof RedisTryAgainException || e instanceof RedisClusterDownException
                || e instanceof RedisMasterDownException || e instanceof RedisNoReplicasException) {
            return new RedisCommandException(Kind.UNAVAILABLE, message, e);
        }
        if (e instanceof RedisOutOfMemoryException) {
            return new RedisCommandException(Kind.OUT_OF_MEMORY, message, e);
        }
        if (e instanceof RedisTimeoutException) {
            // Redisson's own timeout came first (set lower than the cache's), after its retries
            return new RedisCommandException(Kind.TIMEOUT, message, e);
        }
        if (e instanceof RedisConnectionException) {
            return new RedisCommandException(Kind.CONNECTION, message, e);
        }
        if (e instanceof RedisException && e.getClass() == RedisException.class) {
            // an error the server answered with, carried as Redisson's general exception: by its code
            return RedisCommandException.answered(what, e.getMessage(), e);
        }
        // anything else (Redisson giving up after its retries, a reply it could not decode): it may have run
        return new RedisCommandException(Kind.CONNECTION, message, e);
    }

    // reads streams through Redisson's blocking read; closing it cancels the read waiting, which fails it
    private static final class Reader implements RedisStreamReader {

        private final RedissonClient client;
        private final Duration timeout;
        private @Nullable RFuture<?> waiting;
        private boolean closed;

        Reader(RedissonClient client, Duration timeout) {
            this.client = client;
            this.timeout = timeout;
        }

        @Override
        public Map<String, List<Entry>> read(Map<String, String> afterIds, Duration block, int count) {
            RFuture<Map<String, Map<StreamMessageId, Map<String, String>>>> future = start(afterIds, block, count);
            try {
                return entries(await(future, timeout.plus(block), "XREAD"));
            } finally {
                done();
            }
        }

        // without a thread waiting: Redisson answers on its own threads, which the feed hands on to its own
        @Override
        public CompletableFuture<Map<String, List<Entry>>> readAsync(Map<String, String> afterIds, Duration block,
                                                                     int count) {
            RFuture<Map<String, Map<StreamMessageId, Map<String, String>>>> future = start(afterIds, block, count);
            return boundedRead(future, timeout.plus(block)).whenComplete((ignored, failure) -> done())
                    .thenApply(Reader::entries);
        }

        private RFuture<Map<String, Map<StreamMessageId, Map<String, String>>>> start(Map<String, String> afterIds,
                                                                                      Duration block, int count) {
            if (block.isNegative() || block.isZero()) {
                // XREAD BLOCK 0 would wait for good
                throw new IllegalArgumentException("block must be positive");
            }
            Iterator<Map.Entry<String, String>> streams = afterIds.entrySet().iterator();
            if (!streams.hasNext()) {
                throw new IllegalArgumentException("afterIds must name a stream");
            }
            Map.Entry<String, String> first = streams.next();
            Map<String, StreamMessageId> others = new LinkedHashMap<>();
            streams.forEachRemaining(other -> others.put(other.getKey(), id(other.getValue())));
            RFuture<Map<String, Map<StreamMessageId, Map<String, String>>>> future = client
                    .<String, String>getStream(first.getKey(), StringCodec.INSTANCE)
                    .readAsync(StreamMultiReadArgs.greaterThan(id(first.getValue()), others).timeout(block)
                            .count(count));
            synchronized (this) {
                if (closed) {
                    future.cancel(false);
                    throw new IllegalStateException("The reader was closed");
                }
                waiting = future;
            }
            return future;
        }

        private synchronized void done() {
            waiting = null;
        }

        // nil (null) where the block time passed without entries, whatever the reply's type says
        private static Map<String, List<Entry>> entries(
                @Nullable Map<String, Map<StreamMessageId, Map<String, String>>> read) {
            Map<String, List<Entry>> entries = new LinkedHashMap<>();
            // Redisson's signature says never null; a read whose block time passed may still answer nil
            if (read != null) {
                read.forEach((stream, messages) -> {
                    List<Entry> of = new ArrayList<>();
                    messages.forEach((id, fields) -> of.add(new Entry(id.getId0() + "-" + id.getId1(), fields)));
                    entries.put(stream, of);
                });
            }
            return entries;
        }

        private static StreamMessageId id(String id) {
            int dash = id.indexOf('-');
            return dash < 0 ? new StreamMessageId(Long.parseLong(id))
                    : new StreamMessageId(Long.parseLong(id.substring(0, dash)),
                            Long.parseLong(id.substring(dash + 1)));
        }

        @Override
        public synchronized void close() {
            closed = true;
            RFuture<?> current = waiting;
            if (current != null) {
                current.cancel(false);
            }
        }

        // a read without a thread waiting: the future completes with the answer, or with the failure classified,
        // cancelling the command after the bound
        private static <T extends @Nullable Object> CompletableFuture<T> boundedRead(RFuture<T> future,
                                                                                     Duration bound) {
            String what = "XREAD";
            CompletableFuture<T> bounded = future.toCompletableFuture()
                    .orTimeout(bound.toNanos(), TimeUnit.NANOSECONDS);
            return bounded.handle((answer, failure) -> {
                if (failure == null) {
                    return answer;
                }
                Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                        ? failure.getCause() : failure;
                if (cause instanceof TimeoutException) {
                    future.cancel(false);
                    throw new RedisCommandException(Kind.TIMEOUT, what + " got no answer within " + bound.toMillis()
                            + " ms", cause);
                }
                if (cause instanceof CancellationException) {
                    throw new RedisCommandException(Kind.CONNECTION, what + " was cancelled", cause);
                }
                throw classified(what, cause);
            });
        }
    }
}
