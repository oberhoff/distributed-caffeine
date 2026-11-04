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

import io.github.oberhoff.distributedcaffeine.spi.ChangeFeed;
import io.github.oberhoff.distributedcaffeine.spi.ChangeListener;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.VersionsResult;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * The store of one cache instance on Redis (also Valkey, Dragonfly, Kvrocks; VISION.md, "Redis adapters"): every
 * operation one Lua script ({@link RedisScripts}), so a batch of writes is atomic, decided and applied in one step, and
 * the ages of a read all come from one reading of the server's clock. Every write assigns its version in the core's
 * format, as MongoDB's single writes do: an advance is never refused. Keys and values are stored as the bytes they are,
 * whatever the format (a JSON type would only serve queries on the server). Every write and expiry is announced in its
 * script, in the cache's stream, which {@link RedisFeed} reads.
 * <p>
 * Failures: one attempt each, classified for the core, which retries: a server refusing for now (a replica, loading,
 * a cluster in transition) is retryable with nothing applied; a broken connection is retryable, a write's outcome
 * unknown; a timeout is neither retried nor, for a write, known; a script failing while it runs leaves a write's
 * outcome unknown, as Redis rolls nothing back.
 *
 * @author Andreas Oberhoff
 */
final class RedisStore implements Store, ChangeFeed {

    // the core's version format (InternalVersion): milliseconds since 2020 above 20 bits of counter and node
    static final long CORE_EPOCH_MILLIS = 1_577_836_800_000L;
    private static final int LOW_BITS = 20;
    private static final long LOW_MASK = (1L << LOW_BITS) - 1;
    // how far ahead of the server's clock a proposal may move a row's version (InternalVersion#WARN_AHEAD_MILLIS)
    private static final long PROPOSAL_AHEAD_MILLIS = 60_000;
    // how long the stream keeps its entries: the core's grace period of tombstones (InternalMaintenance), so that an
    // instance falling further behind than maintenance deletes is told of a gap
    private static final long STREAM_RETENTION_MILLIS = 5 * 60_000L;
    // how long a write request's result is kept for a client library sending the request again (RedisScripts#WRITE):
    // far longer than a reconnect takes
    private static final long REQUEST_RETENTION_MILLIS = 60_000;
    // keys per script: a command of about ten thousand arguments at most (a write takes nine per key)
    private static final int CHUNK = 1_000;

    private final RedisDriver driver;
    private final RedisSession session;
    private final RedisKeys keys;

    RedisStore(RedisDriver driver, RedisSession session, RedisKeys keys) {
        this.driver = driver;
        this.session = session;
        this.keys = keys;
    }

    // reads

    @Override
    public Map<String, Row> read(Collection<String> hashes, Duration maxAge) {
        Map<String, Row> rows = new LinkedHashMap<>();
        readChunks(hashes, maxAge, "R", (hash, fields, now) -> {
            State state = state((byte[]) fields.get(0));
            long version = version((Long) fields.get(1), (Long) fields.get(2));
            byte[] key = (byte[]) fields.get(4);
            byte[] value = state == State.VALUE ? (byte[]) fields.get(5) : null;
            rows.put(hash, new Row(hash, key, value, state, version, age(now, (Long) fields.get(3))));
        });
        return rows;
    }

    @Override
    public VersionsResult readVersions(Collection<String> hashes, Duration maxAge) {
        Map<String, Long> versions = new LinkedHashMap<>();
        Map<String, Duration> ages = new LinkedHashMap<>();
        readChunks(hashes, maxAge, "versions", (hash, fields, now) -> {
            versions.put(hash, version((Long) fields.get(0), (Long) fields.get(1)));
            ages.put(hash, age(now, (Long) fields.get(2)));
        });
        return new VersionsResult(versions, ages);
    }

    // what a read hands on per row found: its hash, its fields from the script, and the read's instant
    @FunctionalInterface
    private interface Found {
        void row(String hash, List<?> fields, long now);
    }

    // a read in chunks of keys, one script each (a command of a few thousand arguments at most: Dragonfly refuses
    // longer ones, and Redis runs nothing else while a script runs); every chunk measures the ages at the first one's
    // instant, so they all compare within the read (a row written after it, negative)
    private void readChunks(Collection<String> hashes, Duration maxAge, String what, Found found) {
        List<String> ordered = List.copyOf(new LinkedHashSet<>(hashes));
        byte[] now = new byte[0];
        long at = 0;
        for (int from = 0; from < ordered.size(); from += CHUNK) {
            List<String> chunk = ordered.subList(from, Math.min(ordered.size(), from + CHUNK));
            List<byte[]> arguments = List.of(number(micros(maxAge)), text(what), now);
            List<?> out = list(run(false, () -> session.eval(RedisScripts.READ, rowKeys(chunk), arguments)));
            if (from == 0) {
                at = (Long) out.get(0);
                now = number(at);
            }
            for (int i = 0; i < chunk.size(); i++) {
                Object row = out.get(i + 1);
                if (row != null) {
                    found.row(chunk.get(i), list(row), at);
                }
            }
        }
    }

    // Redis's TIME counts microseconds, from the wall clock: the default allowance of 10 ms covers its steps back

    // writes

    @Override
    public WriteResult write(Collection<Write> writes) {
        return write(writes, UUID.randomUUID().toString());
    }

    // a request applied at most once under its ID, whoever sends it again (RedisScripts#WRITE)
    WriteResult write(Collection<Write> writes, String requestId) {
        return write(writes, requestId, RedisScripts.WRITE);
    }

    // the same through another script taking the write script's keys and arguments (tests: the write script made not
    // to replicate, a write a failover loses)
    WriteResult write(Collection<Write> writes, String requestId, RedisScript script) {
        Map<String, Write> byHash = new LinkedHashMap<>();
        writes.forEach(write -> byHash.put(write.hash(), write));
        List<Write> ordered = List.copyOf(byHash.values());
        Set<String> refused = new LinkedHashSet<>();
        Map<String, Long> assigned = new LinkedHashMap<>();
        // in chunks, as reads are, each a request of its own; a chunk failing after another applied leaves the
        // batch's outcome unknown (the contract allows no atomicity across keys)
        for (int from = 0; from < ordered.size(); from += CHUNK) {
            List<Write> chunk = ordered.subList(from, Math.min(ordered.size(), from + CHUNK));
            try {
                writeChunk(chunk, requestId + "-" + from / CHUNK, script, refused, assigned);
            } catch (StoreException e) {
                throw from == 0 ? e : e.afterPartlyApplied();
            }
        }
        return new WriteResult(refused, assigned);
    }

    private void writeChunk(List<Write> chunk, String requestId, RedisScript script, Set<String> refused,
                            Map<String, Long> assigned) {
        List<String> scriptKeys = new ArrayList<>(List.of(keys.values(), keys.dead(), keys.stream(), keys.sequence(),
                keys.request(requestId)));
        List<byte[]> arguments = new ArrayList<>(prelude());
        arguments.add(number(REQUEST_RETENTION_MILLIS));
        for (Write write : chunk) {
            scriptKeys.add(keys.row(write.hash()));
            addArguments(arguments, write);
        }
        List<?> out = list(run(true, () -> session.eval(script, scriptKeys, arguments)));
        for (int i = 0; i < chunk.size(); i++) {
            Write write = chunk.get(i);
            List<?> result = list(out.get(i));
            if ((Long) result.get(0) == 0) {
                refused.add(write.hash());
            } else {
                long version = version((Long) result.get(1), (Long) result.get(2));
                if (version != write.version()) {
                    assigned.put(write.hash(), version);
                }
            }
        }
    }

    // one write's arguments to the write script: its hash, its condition with the version it expects, its version,
    // whether it is a tombstone, its key and its value
    private static void addArguments(List<byte[]> arguments, Write write) {
        arguments.add(text(write.hash()));
        arguments.add(text(condition(write)));
        long expected = write.isAdvance() || write.expectsMissing() ? 0 : write.expected();
        arguments.add(number(high(expected)));
        arguments.add(number(low(expected)));
        arguments.add(number(high(write.version())));
        arguments.add(number(low(write.version())));
        arguments.add(text(write.isTombstone() ? "T" : "V"));
        arguments.add(write.key() != null ? write.key() : new byte[0]);
        arguments.add(write.value() != null ? write.value() : new byte[0]);
    }

    // the write's condition as RedisScripts#WRITE reads it: A advance, M missing, V on the version it expects
    private static String condition(Write write) {
        if (write.isAdvance()) {
            return "A";
        }
        return write.expectsMissing() ? "M" : "V";
    }

    // maintenance: each a script, so every row is expired or deleted once however many instances run it

    @Override
    public int expire(Duration maxAge, long version, int limit) {
        List<String> due = candidates("E", number(micros(maxAge)), number(limit));
        return maintain(RedisScripts.EXPIRE, due, number(micros(maxAge)), number(high(version)), number(low(version)));
    }

    @Override
    public int pruneToSize(long maxSize, Duration minAge, int limit) {
        List<String> due = candidates("S", number(maxSize), number(micros(minAge)), number(limit));
        return maintain(RedisScripts.PRUNE_TO_SIZE, due, number(maxSize), number(micros(minAge)));
    }

    @Override
    public int pruneTombstones(Duration retention, int limit) {
        List<String> due = candidates("T", number(micros(retention)), number(limit));
        return maintain(RedisScripts.PRUNE_TOMBSTONES, due, number(micros(retention)));
    }

    // the hashes an operation may act on, read from the sorted sets (RedisScripts#CANDIDATES)
    private List<String> candidates(String operation, byte[]... parameters) {
        List<byte[]> arguments = new ArrayList<>(prelude());
        arguments.add(text(operation));
        arguments.addAll(List.of(parameters));
        List<?> out = list(run(false, () -> session.eval(RedisScripts.CANDIDATES, maintenanceKeys(), arguments)));
        return out.stream().map(hash -> new String((byte[]) requireNonNull(hash), StandardCharsets.UTF_8)).toList();
    }

    // acts on the candidates, in chunks, each row declared and checked again by the script
    private int maintain(RedisScript script, List<String> due, byte[]... parameters) {
        int changed = 0;
        for (int from = 0; from < due.size(); from += CHUNK) {
            List<String> chunk = due.subList(from, Math.min(due.size(), from + CHUNK));
            List<String> scriptKeys = new ArrayList<>(maintenanceKeys());
            scriptKeys.addAll(rowKeys(chunk));
            List<byte[]> arguments = new ArrayList<>(prelude());
            arguments.addAll(List.of(parameters));
            chunk.forEach(hash -> arguments.add(text(hash)));
            changed += count(run(true, () -> session.eval(script, scriptKeys, arguments)));
        }
        return changed;
    }

    // the change feed

    @Override
    public ChangeSubscription subscribe(ChangeListener listener) {
        return RedisFeed.subscribe(driver, this, keys.stream(), keys.keyspace(), session.clustered(), listener);
    }

    // where the stream stands now: the last entry's ID and number, from which a subscription reads
    RedisFeed.Position position() {
        List<?> out = list(run(false, () -> session.eval(RedisScripts.POSITION,
                List.of(keys.stream(), keys.sequence()), List.of())));
        return new RedisFeed.Position(new String((byte[]) out.get(0), StandardCharsets.UTF_8), (Long) out.get(1));
    }

    @Override
    public void close() {
        session.close();
    }

    @Override
    public String toString() {
        return "RedisStore[" + keys.name() + "]";
    }

    // helpers

    private List<String> rowKeys(List<String> hashes) {
        return hashes.stream().map(keys::row).toList();
    }

    private List<String> maintenanceKeys() {
        return List.of(keys.values(), keys.dead(), keys.stream(), keys.sequence(), keys.control());
    }

    private static List<byte[]> prelude() {
        return List.of(number(CORE_EPOCH_MILLIS), number(PROPOSAL_AHEAD_MILLIS), number(STREAM_RETENTION_MILLIS));
    }

    static long high(long version) {
        return version >>> LOW_BITS;
    }

    static long low(long version) {
        return version & LOW_MASK;
    }

    static long version(long high, long low) {
        return (high << LOW_BITS) | low;
    }

    // whole microseconds, saturated: a maximum age of years is still far below Long.MAX_VALUE microseconds
    private static long micros(Duration duration) {
        try {
            return duration.toNanos() / 1_000;
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE / 1_000;
        }
    }

    private static Duration age(long now, long written) {
        return Duration.ofNanos((now - written) * 1_000);
    }

    private static State state(byte[] code) {
        return switch ((char) code[0]) {
            case 'V' -> State.VALUE;
            case 'T' -> State.TOMBSTONE;
            case 'E' -> State.EXPIRED;
            default -> throw new StoreException("A row has an unknown state: " + new String(code,
                    StandardCharsets.UTF_8), false);
        };
    }

    private static byte[] text(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] number(long number) {
        return text(Long.toString(number));
    }

    private static List<?> list(@Nullable Object result) {
        return (List<?>) requireNonNull(result, "a script returned nil where it returns an array");
    }

    private static int count(@Nullable Object result) {
        return Math.toIntExact((Long) requireNonNull(result, "a script returned nil where it returns a number"));
    }

    // one attempt, the failure classified for the core (see the class comment)
    private <T extends @Nullable Object> T run(boolean write, Supplier<T> command) {
        try {
            return command.get();
        } catch (RedisCommandException e) {
            throw failure(e, write, keys.name());
        } catch (StoreException e) {
            throw e;
        } catch (RuntimeException e) {
            // a reply this store cannot read, or a driver failing unexpectedly
            throw new StoreException("Accessing " + keys.name() + " failed: " + e, e, write, false);
        }
    }

    static StoreException failure(RedisCommandException e, boolean write, String name) {
        String unknown = write ? ", the outcome unknown" : "";
        String accessing = "Accessing " + name;
        return switch (e.kind()) {
            case TIMEOUT -> new StoreException(accessing + " got no answer in time" + unknown + ": "
                    + e.getMessage(), e, write, false);
            case CONNECTION -> new StoreException(accessing + " lost its connection" + unknown + ": "
                    + e.getMessage(), e, write, true);
            case UNAVAILABLE -> new StoreException(accessing + " was refused for now: " + e.getMessage(), e,
                    false, true);
            case OUT_OF_MEMORY -> new StoreException("Redis is out of memory (maxmemory with noeviction) for " + name
                    + ": " + e.getMessage(), e, false, false);
            case SCRIPT -> new StoreException("A script failed on " + name + unknown + ": " + e.getMessage(), e, write,
                    false);
            case REFUSED -> new StoreException(accessing + " failed: " + e.getMessage(), e, false, false);
        };
    }
}
