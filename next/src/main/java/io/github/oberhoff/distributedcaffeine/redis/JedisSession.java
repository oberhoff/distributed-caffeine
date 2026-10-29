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
package io.github.oberhoff.distributedcaffeine.redis;

import io.github.oberhoff.distributedcaffeine.redis.RedisCommandException.Kind;
import org.jspecify.annotations.Nullable;
import redis.clients.jedis.CommandObject;
import redis.clients.jedis.CommandObjects;
import redis.clients.jedis.Connection;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisNoScriptException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.util.Pool;

import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

// The commands of one cache over Jedis: each on a connection borrowed from the client's pool for it, bounded by the
// timeout as the connection's socket timeout (restored before it goes back); a stream reader holds a connection of its
// own while it reads. Commands are built by Jedis's CommandObjects for the protocol the connection speaks (RESP2 or
// RESP3), so their replies come back typed alike; a script's reply comes back raw, false (RESP3's for Lua's false)
// read as nil. Jedis does not send a command again after a reset, so a write is never repeated here. In a cluster, a
// command goes to the node Jedis takes for the owner of its first key's slot (a script's keys share one); a node
// answering MOVED or ASK ran nothing, so Jedis's view is refreshed and the command sent once more
final class JedisSession implements RedisSession {

    // where connections come from: a pool (a single server's, or that of the primary Sentinel names), or, in a
    // cluster, the pool of the node owning a key's slot
    interface Source {

        // a connection for a command on the key (null: any node), given back with Lease#giveBack
        Lease borrow(@Nullable String key);

        // a node answered with a redirect, or could not be reached or did not answer (a primary failing, before its
        // replica took over): the cluster may have changed since Jedis last looked, which Jedis does not notice by
        // itself where commands bypass its executor, as here
        void changed();

        boolean clustered();
    }

    // a borrowed connection, and the pool it goes back to (null: the one Jedis set on the connection, its close
    // giving it back)
    record Lease(Connection connection, @Nullable Pool<Connection> pool) {

        // back to the pool, as broken where it broke; quietly, so that a pool failing to take it (a connection that
        // could not even be destroyed cleanly) never hides the command's own outcome
        void giveBack() {
            try {
                if (pool == null) {
                    connection.close();
                } else if (!connection.isBroken()) {
                    pool.returnResource(connection);
                } else {
                    pool.returnBrokenResource(connection);
                }
            } catch (RuntimeException ignored) {
                // the pool drops it either way
            }
        }

        // gives it back as broken, closing its socket: a read waiting on it fails
        void giveBackBroken() {
            connection.setBroken();
            giveBack();
        }

        // whether this is the lease of the connection: the same object, not an equal one
        @SuppressWarnings("ReferenceEquality")
        boolean of(Connection other) {
            return connection == other;
        }
    }

    private final Source source;
    private final Duration timeout;

    JedisSession(Source source, Duration timeout) {
        this.source = source;
        this.timeout = timeout;
    }

    @Override
    public @Nullable Object eval(RedisScript script, List<String> keys, List<byte[]> arguments) {
        List<byte[]> keyBytes = keys.stream().map(key -> key.getBytes(StandardCharsets.UTF_8)).toList();
        byte[] sha = script.sha().getBytes(StandardCharsets.UTF_8);
        String key = keys.isEmpty() ? null : keys.get(0);
        try {
            return nil(run(source, key, timeout, script.toString(),
                    objects -> objects.evalsha(sha, keyBytes, arguments)));
        } catch (RedisCommandException e) {
            if (!(e.getCause() instanceof JedisNoScriptException)) {
                throw e;
            }
            // not cached on the server (a restart, a failover, SCRIPT FLUSH): loaded where the key is served (SCRIPT
            // LOAD; Kvrocks caches no script EVAL runs, so EVAL alone would miss every time) and run by its SHA again;
            // where loading is refused (a user without SCRIPT LOAD, a node taking none) or the script is still
            // missing, sent in full
            byte[] text = script.source().getBytes(StandardCharsets.UTF_8);
            byte[] sampleKey = keyBytes.isEmpty() ? new byte[0] : keyBytes.get(0);
            try {
                run(source, key, timeout, "SCRIPT LOAD " + script, objects -> objects.scriptLoad(text,
                        sampleKey));
                return nil(run(source, key, timeout, script.toString(),
                        objects -> objects.evalsha(sha, keyBytes, arguments)));
            } catch (RedisCommandException again) {
                if (RedisCommandException.mayHaveRun(again)) {
                    throw again;
                }
            }
            return nil(run(source, key, timeout, script.toString(),
                    objects -> objects.eval(text, keyBytes, arguments)));
        }
    }

    @Override
    public RedisServer server() {
        String server = run(source, null, timeout, "INFO server", objects -> objects.info("server"));
        String memory = run(source, null, timeout, "INFO memory", objects -> objects.info("memory"));
        String replication = run(source, null, timeout, "INFO replication", objects -> objects.info("replication"));
        return RedisServer.of(RedisServer.fields(requireNonNull(server, "INFO server answered nil")),
                RedisServer.fields(requireNonNull(memory, "INFO memory answered nil")),
                RedisServer.fields(requireNonNull(replication, "INFO replication answered nil")));
    }

    @Override
    public RedisStreamReader openReader() {
        return new Reader(source, timeout);
    }

    @Override
    public boolean clustered() {
        return source.clustered();
    }

    @Override
    public void close() {
        // every connection went back to the pool after its command; the pool is the user's
    }

    // Lua's false comes back as RESP3's false on a RESP3 connection, as nil on RESP2: nil either way, also in arrays
    private static @Nullable Object nil(@Nullable Object reply) {
        if (Boolean.FALSE.equals(reply)) {
            return null;
        }
        if (reply instanceof List<?> list) {
            List<@Nullable Object> converted = new ArrayList<>(list.size());
            list.forEach(element -> converted.add(nil(element)));
            return converted;
        }
        return reply;
    }

    // one command on a connection borrowed for it: waiting at most the timeout for the answer, the connection's socket
    // timeout restored before it goes back, or back as broken where it timed out or broke; sent once more after a
    // redirect, which ran nothing
    private static <T extends @Nullable Object> T run(Source source, @Nullable String key, Duration bound,
                                                      String what, Function<CommandObjects, CommandObject<T>> command) {
        try {
            return runOnce(source, key, bound, what, command);
        } catch (RedisCommandException e) {
            if (redirected(e)) {
                source.changed();
                return runOnce(source, key, bound, what, command);
            }
            if (e.kind() == Kind.CONNECTION || e.kind() == Kind.TIMEOUT) {
                // not sent again (it may have run), but the next command goes where the cluster says now
                source.changed();
            }
            throw e;
        }
    }

    private static <T extends @Nullable Object> T runOnce(Source source, @Nullable String key, Duration bound,
                                                          String what,
                                                          Function<CommandObjects, CommandObject<T>> command) {
        Lease lease;
        try {
            lease = source.borrow(key);
        } catch (RuntimeException e) {
            // no connection within the pool's own wait, or none could be opened
            throw new RedisCommandException(Kind.CONNECTION, what + " could not connect: " + e.getMessage(), e);
        }
        try {
            return run(lease.connection(), bound, what, command, true);
        } finally {
            lease.giveBack();
        }
    }

    private static boolean redirected(RedisCommandException e) {
        return e.getCause() instanceof JedisRedirectionException;
    }

    // a command on a connection, borrowed for it (the socket timeout restored afterwards) or held by its caller (a
    // stream reader's: it sets the timeout before every read, and replaces a connection that broke)
    static <T extends @Nullable Object> T run(Connection connection, Duration bound, String what,
                                              Function<CommandObjects, CommandObject<T>> command, boolean borrowed) {
        int previous = connection.getSoTimeout();
        try {
            connection.setSoTimeout(Math.toIntExact(Math.min(Integer.MAX_VALUE, Math.max(1, bound.toMillis()))));
            return connection.executeCommand(command.apply(new CommandObjects(connection.getRedisProtocol())));
        } catch (JedisConnectionException e) {
            connection.setBroken();
            boolean timedOut = e.getCause() instanceof SocketTimeoutException;
            throw new RedisCommandException(timedOut ? Kind.TIMEOUT : Kind.CONNECTION, what + (timedOut
                    ? " got no answer within " + bound.toMillis() + " ms" : " failed: " + e.getMessage()), e);
        } catch (JedisDataException e) {
            // the server answered with an error: the connection is fine
            throw RedisCommandException.answered(what, e.getMessage(), e);
        } catch (RuntimeException e) {
            // anything else (a reply Jedis could not read): what was sent may have run
            connection.setBroken();
            throw new RedisCommandException(Kind.CONNECTION, what + " failed: " + e, e);
        } finally {
            if (!connection.isBroken() && borrowed) {
                connection.setSoTimeout(previous);
            }
        }
    }

    // ends blocking reads that got no answer in time (see Reader#read)
    private static final ScheduledExecutorService WATCHDOG = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "distributed-caffeine-jedis-watchdog");
        thread.setDaemon(true);
        return thread;
    });

    // reads streams on a connection it holds while it reads, borrowed when first needed (in a cluster, from the node
    // owning the streams' slot) and anew after one broke or the slot moved
    private static final class Reader implements RedisStreamReader {

        private final Source source;
        private final Duration timeout;
        private @Nullable Lease lease;
        private boolean closed;

        Reader(Source source, Duration timeout) {
            this.source = source;
            this.timeout = timeout;
        }

        @Override
        public Map<String, List<Entry>> read(Map<String, String> afterIds, Duration block, int count) {
            if (block.isNegative() || block.isZero()) {
                // XREAD BLOCK 0 would wait for good
                throw new IllegalArgumentException("block must be positive");
            }
            Map<String, StreamEntryID> offsets = new LinkedHashMap<>();
            afterIds.forEach((key, id) -> offsets.put(key, new StreamEntryID(id)));
            Connection held = connection(afterIds.keySet().iterator().next());
            Duration bound = timeout.plus(block);
            // Jedis waits for a blocking command's answer without a socket timeout (its client-wide one for blocking
            // commands, unbounded unless the user set one), so the connection is disconnected if no answer came in
            // time, which fails the read waiting on it
            AtomicBoolean expired = new AtomicBoolean();
            ScheduledFuture<?> watchdog = WATCHDOG.schedule(() -> {
                expired.set(true);
                held.setBroken();
                held.disconnect();
            }, bound.toMillis(), TimeUnit.MILLISECONDS);
            // nil (null) where the block time passed without entries, whatever the reply's type says
            @Nullable List<Map.Entry<String, List<StreamEntry>>> read;
            try {
                read = run(held, bound, "XREAD", objects -> objects.xread(
                        XReadParams.xReadParams().block(Math.toIntExact(block.toMillis())).count(count), offsets),
                        false);
            } catch (RedisCommandException e) {
                if (expired.get()) {
                    throw new RedisCommandException(Kind.TIMEOUT, "XREAD got no answer within " + bound.toMillis()
                            + " ms", e);
                }
                if (redirected(e)) {
                    // the next read borrows from the slot's new owner
                    source.changed();
                    release(held);
                } else if (e.kind() == Kind.CONNECTION || e.kind() == Kind.TIMEOUT) {
                    source.changed();
                }
                throw e;
            } finally {
                watchdog.cancel(false);
                if (held.isBroken()) {
                    release(held);
                }
            }
            return entries(read);
        }

        // the entries of each stream read; none where the reply is nil
        private static Map<String, List<Entry>> entries(@Nullable List<Map.Entry<String, List<StreamEntry>>> read) {
            Map<String, List<Entry>> entries = new LinkedHashMap<>();
            if (read != null) {
                for (Map.Entry<String, List<StreamEntry>> stream : read) {
                    entries.put(stream.getKey(), stream.getValue().stream()
                            .map(entry -> new Entry(entry.getID().toString(), entry.getFields())).toList());
                }
            }
            return entries;
        }

        private synchronized Connection connection(String stream) {
            if (closed) {
                throw new IllegalStateException("The reader was closed");
            }
            Lease current = lease;
            if (current == null) {
                try {
                    current = source.borrow(stream);
                } catch (RuntimeException e) {
                    throw new RedisCommandException(Kind.CONNECTION, "XREAD could not connect: " + e.getMessage(), e);
                }
                lease = current;
            }
            return current.connection();
        }

        // gives a connection back that broke or reads a slot moved elsewhere, so that the next read borrows another:
        // the same connection object, unless closing the reader gave it back meanwhile
        private synchronized void release(Connection released) {
            Lease current = lease;
            if (current != null && current.of(released)) {
                current.giveBack();
                lease = null;
            }
        }

        // closing the socket fails a read waiting on it, which the feed then takes for no loss (another generation)
        @Override
        public synchronized void close() {
            closed = true;
            Lease current = lease;
            if (current != null) {
                current.giveBackBroken();
            }
            lease = null;
        }
    }
}
