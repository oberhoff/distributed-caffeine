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

import com.github.dockerjava.api.model.Ulimit;
import io.github.oberhoff.distributedcaffeine.spi.ChangeSubscription;
import io.github.oberhoff.distributedcaffeine.spi.RecordingListener;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.spi.WriteResult;
import io.github.oberhoff.distributedcaffeine.testing.TestStoreSettings;
import io.lettuce.core.KillArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * What only the Redis store does: a write request sent again by the client library (Lettuce does, after a connection
 * reset, for what it had no answer for) is answered from its kept result and applied once; and that very behavior,
 * which makes a reset of the feed's connection lose nothing.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(60)
@SuppressWarnings("resource") // every store and subscription a test opens is closed after it (@AfterEach)
abstract class RedisStoreTests {

    private static final Duration LONG = Duration.ofHours(1);
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final RedisTestServer server = server();
    private final RedisCommands<String, String> admin = server.admin.sync();
    private final String name = RedisTestServer.freshName();
    private final RedisDriver driver = LettuceRedis.of(server.client);
    private final List<AutoCloseable> closeables = new ArrayList<>();

    // the server of the enclosing image
    protected abstract RedisTestServer server();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
    }

    private RedisStore open() {
        Store store = RedisAdapter.newBuilder(driver, name).build()
                .createStore(TestStoreSettings.of(Format.BINARY, Format.BINARY));
        closeables.add(store);
        return (RedisStore) store;
    }

    // a value of the one key every test writes
    private static Write value(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return Write.value("h1", bytes, bytes, 1);
    }

    private long entries() {
        return admin.xlen(new RedisKeys(name, RedisAdapter.DEFAULT_DISCRIMINATOR).stream());
    }

    @Test
    void the_adapter_names_the_servers_product_once_its_store_was_created() {
        RedisAdapter<String, String> adapter = RedisAdapter.newBuilder(driver, name).build();
        assertThat(adapter).as("before asking the server").hasToString("Redis " + name + "/default");
        closeables.add(adapter.createStore(TestStoreSettings.of(Format.BINARY, Format.BINARY)));
        String image = server.image.toLowerCase(java.util.Locale.ROOT);
        String product = image.contains("valkey") ? "Valkey" : image.contains("dragonfly") ? "Dragonfly"
                : image.contains("kvrocks") ? "Kvrocks" : "Redis";
        assertThat(adapter).hasToString(product + " " + name + "/default");
    }

    @Test
    void a_keyspace_written_by_another_version_is_refused_naming_what_to_delete() {
        admin.set(new RedisKeys(name, RedisAdapter.DEFAULT_DISCRIMINATOR).schema(), "2");
        assertThatThrownBy(this::open).isInstanceOf(StoreException.class).hasMessageContaining("another version")
                .hasMessageContaining("delete its keys");
    }

    // a write request sent again

    @Test
    void an_advance_sent_again_is_answered_from_its_result_and_applied_once() {
        RedisStore store = open();
        Write write = value("v");

        WriteResult first = store.write(List.of(write), "request-1");
        WriteResult again = store.write(List.of(write), "request-1");

        assertThat(again).isEqualTo(first);
        assertThat(store.read(List.of("h1"), LONG).get("h1").version()).as("one version, not two")
                .isEqualTo(first.version(write));
        assertThat(entries()).as("announced once").isEqualTo(1);
    }

    @Test
    void a_conditional_write_sent_again_reports_it_applied_instead_of_conflicting_with_itself() {
        RedisStore store = open();
        Write initial = value("old");
        long stored = store.write(List.of(initial)).version(initial);
        Write conditional = value("new").ifVersion(stored);

        WriteResult first = store.write(List.of(conditional), "request-2");
        WriteResult again = store.write(List.of(conditional), "request-2");

        assertThat(first.isRefused(conditional)).isFalse();
        assertThat(again).isEqualTo(first);
        assertThat(entries()).isEqualTo(2);
    }

    @Test
    void a_refused_write_sent_again_stays_refused_and_a_new_request_is_decided_anew() {
        RedisStore store = open();
        store.write(List.of(value("there")));
        Write missing = value("late").ifMissing();

        assertThat(store.write(List.of(missing), "request-3").isRefused(missing)).isTrue();
        assertThat(store.write(List.of(missing), "request-3").isRefused(missing)).isTrue();

        Write tombstone = Write.tombstone("h1", 1);
        store.write(List.of(tombstone), "request-4");
        assertThat(store.read(List.of("h1"), LONG).get("h1").state()).isEqualTo(State.TOMBSTONE);
        assertThat(store.write(List.of(missing), "request-5").isRefused(missing)).as("a tombstone is a row")
                .isTrue();
    }

    @Test
    void a_request_keeps_its_result_for_a_minute_only() {
        RedisStore store = open();
        store.write(List.of(value("v")), "request-6");
        // kept per chunk of the request, this one its first and only

        assertThat(admin.pttl(new RedisKeys(name, RedisAdapter.DEFAULT_DISCRIMINATOR).request("request-6-0")))
                .isBetween(Duration.ofSeconds(50).toMillis(), Duration.ofMinutes(1).toMillis());
    }

    // the server

    @Test
    void an_evicting_server_is_warned_of_and_the_cache_still_starts() {
        assumeTrue(server.evictionConfigurable(), "no maxmemory-policy to set (Dragonfly, Kvrocks)");
        admin.configSet("maxmemory-policy", "allkeys-lru");
        try (RedisSession session = driver.open(Duration.ofSeconds(5))) {
            assertThat(RedisAdapter.evicts(session.server())).as("allkeys-lru").isTrue();
            assertThat(open()).as("warned of, not refused").isNotNull();
            admin.configSet("maxmemory-policy", "volatile-lru");
            assertThat(RedisAdapter.evicts(session.server())).as("volatile-lru never touches keys without a TTL")
                    .isFalse();
        } finally {
            admin.configSet("maxmemory-policy", "noeviction");
        }
    }

    @Test
    void a_dragonfly_in_cache_mode_is_warned_of_and_the_cache_still_starts() {
        assumeTrue(server.dragonfly(), "only Dragonfly has a cache mode");
        try (GenericContainer<?> evicting = new GenericContainer<>(server.image).withExposedPorts(6379)
                .withCommand("--cache_mode=true")
                .withCreateContainerCmdModifier(cmd -> requireNonNull(cmd.getHostConfig())
                        .withUlimits(List.of(new Ulimit("memlock", -1L, -1L))))
                .waitingFor(Wait.forListeningPort())) {
            evicting.start();
            io.lettuce.core.RedisClient client = io.lettuce.core.RedisClient.create("redis://" + evicting.getHost()
                    + ":" + evicting.getMappedPort(6379));
            try (RedisSession session = LettuceRedis.of(client).open(Duration.ofSeconds(5))) {
                assertThat(RedisAdapter.evicts(session.server())).as("reported as eviction").isTrue();
                Store store = RedisAdapter.newBuilder(LettuceRedis.of(client), name).build()
                        .createStore(TestStoreSettings.of(Format.BINARY, Format.BINARY));
                store.close();
            } finally {
                client.shutdown();
            }
        }
    }

    // keyspaces and discriminators

    @Test
    void a_discriminator_that_could_let_one_caches_keys_begin_with_anothers_is_refused() {
        RedisAdapter.Builder builder = RedisAdapter.newBuilder(driver, name);

        assertThatThrownBy(() -> builder.withDiscriminator("orders:r")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("colon");
        assertThatThrownBy(() -> RedisAdapter.newBuilder(driver, "app{1}")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("braces");
    }

    // the client library sending again

    @Test
    void a_reset_of_the_feeds_connection_loses_nothing_as_lettuce_sends_the_read_again() {
        RedisStore writer = open();
        RecordingListener listener = new RecordingListener();
        ChangeSubscription subscription = open().subscribe(listener);
        closeables.add(subscription);
        Write before = value("before");
        long announced = writer.write(List.of(before)).version(before);
        listener.await(changes -> changes.stream().anyMatch(change -> change.version() == announced), TIMEOUT);

        admin.clientList().lines().filter(client -> client.contains(" cmd=xread")).forEach(client -> admin.clientKill(
                KillArgs.Builder.id(Long.parseLong(client.substring(client.indexOf("id=") + 3, client.indexOf(' '))))));
        Write after = value("after");
        long version = writer.write(List.of(after)).version(after);

        listener.await(changes -> changes.stream().anyMatch(change -> change.version() == version), TIMEOUT);
        assertThat(listener.losses()).as("no loss: the read was sent again on a new connection").isZero();
    }
}
