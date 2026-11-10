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
import io.github.oberhoff.distributedcaffeine.testing.SilencingProxy;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A binding's commands against one server image: scripts with their results in Redis's terms, a script the server
 * lost, the description of the server, blocking stream reads, the timeout per session on a shared client, a network
 * that silently stopped carrying, and the classification of failures.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(120)
@SuppressWarnings("resource") // every session, reader, client and proxy a test opens is closed after it (@AfterEach)
abstract class RedisBindingTests {

    private static final AtomicInteger KEYS = new AtomicInteger();
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final RedisTestServer server = server();
    private final String image = server.image;
    // the test's own access to the server, beside the binding's
    private final StatefulRedisConnection<String, String> admin = server.admin;
    private final List<AutoCloseable> closeables = new ArrayList<>();

    // the server of the enclosing image
    protected abstract RedisTestServer server();

    // the binding under test: a driver of the server's client of its library
    protected abstract RedisDriver driver();

    // a driver of a client of its own reaching the given port (a proxy's), which the closeable closes
    protected abstract Proxied driverThrough(int port);

    // a driver and what closes its client
    record Proxied(RedisDriver driver, AutoCloseable client) {
    }

    // how a command fails on a server that cannot be reached: a connection failure, unless the client library goes on
    // retrying past the timeout (Redisson)
    protected Kind unreachable() {
        return Kind.CONNECTION;
    }

    // a command that fails at first, until the client library replaced a connection the network stopped carrying
    // (Lettuce and Jedis replace it with the timeout; Redisson by its own ping, within its interval)
    private static <T> T eventually(Supplier<T> command) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (true) {
            try {
                return command.get();
            } catch (RedisCommandException e) {
                if (System.nanoTime() > deadline) {
                    throw e;
                }
            }
        }
    }

    @AfterEach
    void closeAll() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
        closeables.clear();
    }

    private <T extends AutoCloseable> T track(T closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private RedisSession session(RedisDriver driver, Duration timeout) {
        return track(driver.open(timeout));
    }

    private RedisSession session() {
        return session(driver(), TIMEOUT);
    }

    // a client of its own reaching the server through a proxy, closed after the test
    private RedisDriver throughProxy(SilencingProxy proxy) {
        Proxied proxied = driverThrough(proxy.port());
        track(proxied.client());
        return proxied.driver();
    }

    private SilencingProxy proxy() throws Exception {
        return track(new SilencingProxy(server.host(), server.port()));
    }

    private static String freshKey() {
        return "test:" + KEYS.incrementAndGet();
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static final RedisScript ECHO = RedisScript.of("echo", RedisScript.Reply.ARRAY,
            "return {tonumber(ARGV[1]) + 1, ARGV[2], KEYS[1], false}");
    private static final RedisScript NOTHING = RedisScript.of("nothing", RedisScript.Reply.ARRAY, "return false");
    private static final RedisScript ADD = RedisScript.of("add", RedisScript.Reply.BULK,
            "return redis.call('XADD', KEYS[1], '*', 'field', ARGV[1])");

    // scripts

    @Test
    void a_script_returns_integers_bulk_strings_arrays_and_nils_with_bytes_unchanged() {
        RedisSession session = session();
        byte[] binary = {0, -1, 10, 13, 127, -128};

        Object result = session.eval(ECHO, List.of("the-key"), List.of(bytes("41"), binary));

        assertThat(result).isInstanceOf(List.class);
        List<?> values = (List<?>) result;
        assertThat(values).hasSize(4);
        assertThat(values.get(0)).isEqualTo(42L);
        assertThat((byte[]) values.get(1)).isEqualTo(binary);
        assertThat((byte[]) values.get(2)).isEqualTo(bytes("the-key"));
        assertThat(values.get(3)).isNull();
        assertThat(session.eval(NOTHING, List.of(), List.of())).isNull();
    }

    @Test
    void a_script_the_server_lost_is_sent_in_full_once_and_cached_again() {
        RedisSession session = session();
        session.eval(ECHO, List.of("k"), List.of(bytes("1"), bytes("x")));
        try {
            admin.sync().scriptFlush();
        } catch (io.lettuce.core.RedisCommandExecutionException e) {
            // Dragonfly before 1.7 has no SCRIPT FLUSH, so a lost script cannot be simulated there
            org.junit.jupiter.api.Assumptions.abort("SCRIPT FLUSH refused: " + e.getMessage());
        }
        assertThat(admin.sync().scriptExists(ECHO.sha())).containsExactly(false);

        Object result = session.eval(ECHO, List.of("k"), List.of(bytes("1"), bytes("x")));

        assertThat(((List<?>) requireNonNull(result)).get(0)).isEqualTo(2L);
        assertThat(admin.sync().scriptExists(ECHO.sha())).containsExactly(true);
    }

    @Test
    void a_script_failing_after_a_write_is_a_script_failure_and_its_write_stays() {
        RedisSession session = session();
        String key = freshKey();
        RedisScript failing = RedisScript.of("failing", RedisScript.Reply.INTEGER,
                "redis.call('SET', KEYS[1], 'written'); return redis.call('INCR', KEYS[1])");

        assertThatThrownBy(() -> session.eval(failing, List.of(key), List.of()))
                .isInstanceOfSatisfying(RedisCommandException.class, e -> assertThat(e.kind()).isEqualTo(Kind.SCRIPT));
        assertThat(admin.sync().get(key)).as("Redis rolls nothing back").isEqualTo("written");
    }

    @Test
    void a_write_beyond_maxmemory_with_noeviction_is_out_of_memory() {
        assumeTrue(server.evictionConfigurable(), "no maxmemory-policy to set (Dragonfly, Kvrocks)");
        RedisSession session = session();
        RedisScript write = RedisScript.of("write", RedisScript.Reply.BULK,
                "return redis.call('SET', KEYS[1], ARGV[1])");
        RedisCommands<String, String> sync = admin.sync();
        sync.configSet("maxmemory-policy", "noeviction");
        sync.configSet("maxmemory", "1");
        try {
            assertThatThrownBy(() -> session.eval(write, List.of(freshKey()), List.of(bytes("value"))))
                    .isInstanceOfSatisfying(RedisCommandException.class,
                            e -> assertThat(e.kind()).as(e.getMessage()).isEqualTo(Kind.OUT_OF_MEMORY));
        } finally {
            sync.configSet("maxmemory", "0");
        }
    }

    // the server

    @Test
    void the_server_is_described_with_its_product_version_mode_and_eviction_policy() {
        RedisSession session = session();
        RedisCommands<String, String> sync = admin.sync();
        if (server.image.contains("kvrocks")) {
            RedisServer kvrocks = session.server();
            assertThat(kvrocks.product()).isEqualTo("Kvrocks");
            assertThat(kvrocks.supported()).as("by the Redis version it implements: " + kvrocks).isTrue();
            assertThat(kvrocks.evictionPolicy()).as("on disk, nothing evicted").isNull();
            return;
        }
        if (server.dragonfly()) {
            RedisServer dragonfly = session.server();
            assertThat(dragonfly.product()).isEqualTo("Dragonfly");
            assertThat(dragonfly.supported()).as("by the Redis version it implements: " + dragonfly).isTrue();
            assertThat(dragonfly.evictionPolicy()).as("not in --cache_mode").isEqualTo("noeviction");
            return;
        }
        sync.configSet("maxmemory-policy", "noeviction");

        RedisServer described = session.server();

        assertThat(described.product()).isEqualTo(image.startsWith("valkey") ? "Valkey" : "Redis");
        assertThat(described.supported()).as(described.toString()).isTrue();
        assertThat(described.mode()).isEqualTo("standalone");
        assertThat(described.evictionPolicy()).isEqualTo("noeviction");

        sync.configSet("maxmemory-policy", "allkeys-lru");
        try {
            assertThat(session.server().evictionPolicy()).isEqualTo("allkeys-lru");
        } finally {
            sync.configSet("maxmemory-policy", "noeviction");
        }
    }

    // streams

    @Test
    void a_blocking_read_returns_an_entry_added_while_it_waits_and_nothing_when_none_comes() throws Exception {
        RedisSession session = session();
        RedisStreamReader reader = track(session.openReader());
        String key = freshKey();
        String value = "the value";

        // after 0-0, so that the entry is returned whether it lands before or while the read waits
        CompletableFuture<List<RedisStreamReader.Entry>> waiting = CompletableFuture.supplyAsync(
                () -> reader.read(key, "0-0", Duration.ofSeconds(10), 10));
        pause(200);
        String id = new String((byte[]) requireNonNull(session.eval(ADD, List.of(key), List.of(bytes(value)))),
                StandardCharsets.UTF_8);

        List<RedisStreamReader.Entry> entries = waiting.get(15, TimeUnit.SECONDS);
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).id()).isEqualTo(id);
        assertThat(entries.get(0).fields()).containsEntry("field", value);

        long started = System.nanoTime();
        assertThat(reader.read(key, id, Duration.ofMillis(300), 10)).isEmpty();
        assertThat(Duration.ofNanos(System.nanoTime() - started)).as("waited for the block time")
                .isGreaterThanOrEqualTo(Duration.ofMillis(250)).isLessThan(Duration.ofSeconds(3));
    }

    // timeouts and broken connections

    @Test
    void sessions_on_one_client_keep_their_own_timeouts() throws Exception {
        SilencingProxy proxy = proxy();
        RedisDriver driver = throughProxy(proxy);
        RedisSession impatient = session(driver, Duration.ofMillis(300));
        RedisSession patient = session(driver, Duration.ofSeconds(5));
        impatient.eval(NOTHING, List.of(), List.of());
        patient.eval(NOTHING, List.of(), List.of());

        proxy.delayRequests(Duration.ofSeconds(1));

        assertThatThrownBy(() -> impatient.eval(NOTHING, List.of(), List.of()))
                .isInstanceOfSatisfying(RedisCommandException.class, e -> assertThat(e.kind()).isEqualTo(Kind.TIMEOUT));
        assertThat(patient.eval(NOTHING, List.of(), List.of())).isNull();
    }

    @Test
    void a_command_on_a_silent_connection_fails_within_the_timeout_and_the_next_one_connects_anew() throws Exception {
        SilencingProxy proxy = proxy();
        RedisSession session = session(throughProxy(proxy), Duration.ofSeconds(1));
        session.eval(NOTHING, List.of(), List.of());

        proxy.silenceOpenConnections();
        long started = System.nanoTime();
        assertThatThrownBy(() -> session.eval(NOTHING, List.of(), List.of()))
                .isInstanceOfSatisfying(RedisCommandException.class, e -> assertThat(e.kind()).isEqualTo(Kind.TIMEOUT));
        assertThat(Duration.ofNanos(System.nanoTime() - started)).as("failing after the timeout, not hanging")
                .isLessThan(Duration.ofSeconds(3));

        // the silent connection is replaced; a new one is carried
        assertThat(eventually(() -> Optional.ofNullable(session.eval(NOTHING, List.of(), List.of())))).isEmpty();
    }

    // a connection closed while it sat idle (a restart, a failover) costs no command: Lettuce and Redisson reconnect
    // and send it again, the Jedis binding sends it once more on another connection of the pool
    @Test
    void a_command_on_a_connection_closed_while_idle_is_sent_on_another() throws Exception {
        SilencingProxy proxy = proxy();
        RedisSession session = session(throughProxy(proxy), TIMEOUT);
        session.eval(NOTHING, List.of(), List.of());

        proxy.retarget(server.host(), server.port());

        assertThat(session.eval(NOTHING, List.of(), List.of())).isNull();
    }

    @Test
    void a_blocking_read_on_a_silent_connection_fails_within_its_block_time_and_the_timeout() throws Exception {
        SilencingProxy proxy = proxy();
        RedisSession session = session(throughProxy(proxy), Duration.ofSeconds(1));
        RedisStreamReader reader = track(session.openReader());
        String key = freshKey();
        assertThat(reader.read(key, "0-0", Duration.ofMillis(100), 10)).isEmpty();

        proxy.silenceOpenConnections();
        long started = System.nanoTime();
        assertThatThrownBy(() -> reader.read(key, "0-0", Duration.ofMillis(500), 10))
                .isInstanceOfSatisfying(RedisCommandException.class, e -> assertThat(e.kind()).isEqualTo(Kind.TIMEOUT));
        assertThat(Duration.ofNanos(System.nanoTime() - started)).as("block time plus timeout, not hanging")
                .isGreaterThanOrEqualTo(Duration.ofMillis(1_400)).isLessThan(Duration.ofSeconds(4));

        assertThat(eventually(() -> reader.read(key, "0-0", Duration.ofMillis(100), 10))).isEmpty();
    }

    @Test
    void a_server_that_cannot_be_reached_is_a_connection_failure() throws Exception {
        SilencingProxy proxy = proxy();
        RedisDriver driver = throughProxy(proxy);
        RedisSession session = session(driver, Duration.ofSeconds(1));
        proxy.close();

        assertThatThrownBy(() -> session.eval(NOTHING, List.of(), List.of()))
                .isInstanceOfSatisfying(RedisCommandException.class,
                        e -> assertThat(e.kind()).isEqualTo(unreachable()));
    }
}
