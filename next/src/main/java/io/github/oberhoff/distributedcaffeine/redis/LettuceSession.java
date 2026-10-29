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

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulConnection;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

// The commands of one cache over Lettuce: one connection for scripts and checks (Lettuce multiplexes it between
// threads), one more per stream reader
final class LettuceSession implements RedisSession {

    private final Supplier<StatefulConnection<byte[], byte[]>> opener;
    private final boolean clustered;
    private final Duration timeout;
    private final LettuceConnection commands;

    // opener: connects the user's client; clustered where it is a cluster client
    LettuceSession(Supplier<StatefulConnection<byte[], byte[]>> opener, boolean clustered, Duration timeout) {
        this.opener = opener;
        this.clustered = clustered;
        this.timeout = timeout;
        this.commands = new LettuceConnection(opener, timeout);
    }

    @Override
    public @Nullable Object eval(RedisScript script, List<String> keys, List<byte[]> arguments) {
        byte[][] keyBytes = keys.stream().map(key -> key.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);
        byte[][] argumentBytes = arguments.toArray(byte[][]::new);
        try {
            return commands.run(script.toString(), Duration.ZERO, async -> async.evalsha(script.sha(),
                    ScriptOutputType.OBJECT, keyBytes, argumentBytes));
        } catch (RedisCommandException e) {
            if (!(e.getCause() instanceof RedisNoScriptException)) {
                throw e;
            }
            // not cached on the server (a restart, a failover, SCRIPT FLUSH): loaded where the key is served (SCRIPT
            // LOAD; Kvrocks caches no script EVAL runs, so EVAL alone would miss every time) and run by its SHA again;
            // where loading is refused (a user without SCRIPT LOAD, a node taking none) or the script is still
            // missing, sent in full
            byte[] source = script.source().getBytes(StandardCharsets.UTF_8);
            try {
                commands.run("SCRIPT LOAD " + script, Duration.ZERO, keyBytes.length == 0 ? null : keyBytes[0],
                        async -> async.scriptLoad(source));
                return commands.run(script.toString(), Duration.ZERO, async -> async.evalsha(script.sha(),
                        ScriptOutputType.OBJECT, keyBytes, argumentBytes));
            } catch (RedisCommandException again) {
                if (RedisCommandException.mayHaveRun(again)) {
                    throw again;
                }
            }
            return commands.run(script.toString(), Duration.ZERO, async -> async.eval(source,
                    ScriptOutputType.OBJECT, keyBytes, argumentBytes));
        }
    }

    @Override
    public RedisServer server() {
        String server = requireNonNull(commands.run("INFO server", Duration.ZERO, async -> async.info("server")));
        String memory = requireNonNull(commands.run("INFO memory", Duration.ZERO, async -> async.info("memory")));
        String replication = requireNonNull(commands.run("INFO replication", Duration.ZERO,
                async -> async.info("replication")));
        return RedisServer.of(RedisServer.fields(server), RedisServer.fields(memory),
                RedisServer.fields(replication));
    }

    @Override
    public RedisStreamReader openReader() {
        return new Reader(new LettuceConnection(opener, timeout));
    }

    @Override
    public boolean clustered() {
        return clustered;
    }

    @Override
    public void close() {
        commands.close();
    }

    private record Reader(LettuceConnection connection) implements RedisStreamReader {

        @Override
        public Map<String, List<Entry>> read(Map<String, String> afterIds, Duration block, int count) {
            XReadArgs.StreamOffset<byte[]>[] offsets = offsets(afterIds, block);
            return entries(connection.run("XREAD", block,
                    async -> async.xread(XReadArgs.Builder.block(block).count(count), offsets)));
        }

        // without a thread waiting: Lettuce answers on its own threads, which the feed hands on to its own
        @Override
        public CompletableFuture<Map<String, List<Entry>>> readAsync(Map<String, String> afterIds, Duration block,
                                                                     int count) {
            XReadArgs.StreamOffset<byte[]>[] offsets = offsets(afterIds, block);
            return connection.readAsync(block,
                            async -> async.xread(XReadArgs.Builder.block(block).count(count), offsets))
                    .thenApply(Reader::entries);
        }

        // Lettuce's xread takes its offsets as generic varargs
        @SuppressWarnings("unchecked")
        private static XReadArgs.StreamOffset<byte[]>[] offsets(Map<String, String> afterIds, Duration block) {
            if (block.isNegative() || block.isZero()) {
                // XREAD BLOCK 0 would wait for good
                throw new IllegalArgumentException("block must be positive");
            }
            if (afterIds.isEmpty()) {
                throw new IllegalArgumentException("afterIds must name a stream");
            }
            XReadArgs.StreamOffset<byte[]>[] offsets = new XReadArgs.StreamOffset[afterIds.size()];
            int i = 0;
            for (Map.Entry<String, String> after : afterIds.entrySet()) {
                offsets[i++] = XReadArgs.StreamOffset.from(after.getKey().getBytes(StandardCharsets.UTF_8),
                        after.getValue());
            }
            return offsets;
        }

        private static Map<String, List<Entry>> entries(@Nullable List<StreamMessage<byte[], byte[]>> messages) {
            Map<String, List<Entry>> entries = new LinkedHashMap<>();
            if (messages != null) {
                for (StreamMessage<byte[], byte[]> message : messages) {
                    Map<String, String> fields = new LinkedHashMap<>();
                    message.getBody().forEach((field, value) -> fields.put(new String(field, StandardCharsets.UTF_8),
                            new String(value, StandardCharsets.UTF_8)));
                    entries.computeIfAbsent(new String(message.getStream(), StandardCharsets.UTF_8),
                            stream -> new ArrayList<>()).add(new Entry(message.getId(), fields));
                }
            }
            return entries;
        }

        @Override
        public void close() {
            connection.close();
        }
    }
}
