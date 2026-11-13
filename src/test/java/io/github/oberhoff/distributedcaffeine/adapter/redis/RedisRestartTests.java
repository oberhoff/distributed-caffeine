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

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import io.github.oberhoff.distributedcaffeine.AdapterConformanceTest;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.testing.Ports;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static io.github.oberhoff.distributedcaffeine.AdapterConformanceTest.recovers;
import static io.github.oberhoff.distributedcaffeine.AdapterConformanceTest.startsDegraded;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * The Redis adapter while its server restarts: with persistence (an append-only file, synced on every write) every
 * value survives and two caches go on; without, the restart loses every row, the stream and its counter, and both
 * caches drop what they hold (the next entry's number starts at one again, which is a gap), then go on. Each test on
 * a server of its own, on a fixed free port, so that a restart keeps its address.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@Timeout(300)
abstract class RedisRestartTests {

    private static final Duration LONG = Duration.ofHours(1);
    private static final Duration CONVERGENCE = Duration.ofSeconds(60);
    private static final String KEY = "k";

    private final String name = RedisTestServer.freshName();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private @Nullable GenericContainer<?> container;
    private int port;

    // the image of the enclosing class
    protected abstract String image();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
        if (container != null) {
            container.stop();
        }
    }

    // a server of its own on a free port, published as the same port, with persistence or without
    @SuppressWarnings("resource") // stopped after the test
    private void start(boolean persistent) {
        assumeFalse(image().contains("dragonfly"), "Dragonfly persists by snapshots, not tested here");
        assumeFalse(image().contains("kvrocks"), "Kvrocks keeps everything on disk, with no setting to compare");
        String server = image().startsWith("valkey") ? "valkey-server" : "redis-server";
        port = Ports.startOnFree(1, chosen -> {
            int candidate = chosen.get(0);
            List<String> command = new ArrayList<>(List.of(server, "--port", String.valueOf(candidate)));
            command.addAll(persistent ? List.of("--appendonly", "yes", "--appendfsync", "always")
                    : List.of("--save", "", "--appendonly", "no"));
            GenericContainer<?> started = new GenericContainer<>(image())
                    .withCommand(command.toArray(String[]::new))
                    .withCreateContainerCmdModifier(cmd -> cmd
                            .withExposedPorts(ExposedPort.tcp(candidate))
                            .withHostConfig(Objects.requireNonNull(cmd.getHostConfig()).withPortBindings(
                                    new PortBinding(com.github.dockerjava.api.model.Ports.Binding.bindPort(candidate),
                                            ExposedPort.tcp(candidate)))))
                    .waitingFor(Wait.forLogMessage("(?is).*ready to accept connections.*", 1));
            started.start();
            container = started;
        }).get(0);
    }

    // restarts the server and waits until it answers again; the Docker client is Testcontainers' own, kept open
    @SuppressWarnings("resource")
    private void restart() {
        DockerClientFactory.instance().client().restartContainerCmd(Objects.requireNonNull(container).getContainerId())
                .exec();
        awaitAnswering();
    }

    // stops the server as a restart does, until started again (startAgain)
    @SuppressWarnings("resource")
    private void stop() {
        DockerClientFactory.instance().client().stopContainerCmd(Objects.requireNonNull(container).getContainerId())
                .exec();
    }

    @SuppressWarnings("resource")
    private void startAgain() {
        DockerClientFactory.instance().client().startContainerCmd(Objects.requireNonNull(container).getContainerId())
                .exec();
        awaitAnswering();
    }

    private void awaitAnswering() {
        try (RedisClient probe = RedisClient.create("redis://127.0.0.1:" + port)) {
            assertThat(awaitUpTo(() -> {
                try (StatefulRedisConnection<String, String> connection = probe.connect()) {
                    return "PONG".equals(connection.sync().ping());
                } catch (RuntimeException e) {
                    return false;
                }
            }, CONVERGENCE, Duration.ofMillis(200))).as("the server back").isTrue();
        }
    }

    // a cache of its own client, as an application would have; noticing a silent feed within seconds
    private DistributedCache<String, String> cache() {
        RedisClient client = RedisClient.create("redis://127.0.0.1:" + port);
        closeables.add(client::shutdown);
        DistributedCache<String, String> cache = DistributedCaffeine.<String, String>newBuilder(
                        RedisAdapter.newBuilder(LettuceRedis.of(client), name).build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withHashProvider((key, hasher) -> key)
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(10_000)
                        .withOperationTimeout(Duration.ofSeconds(2)).withHeartbeatInterval(Duration.ofSeconds(2)))
                .build();
        closeables.add(cache);
        return cache;
    }

    private static void put(DistributedCache<String, String> cache, String value) {
        assertThat(eventually(() -> {
            cache.put(KEY, value);
            return true;
        })).as("put " + value).isTrue();
    }

    private static void converges(DistributedCache<String, String> cache, @Nullable String value) {
        assertThat(awaitUpTo(() -> Objects.equals(value, eventually(() -> cache.getIfPresent(KEY))), CONVERGENCE,
                Duration.ofMillis(200))).as("converges on " + value).isTrue();
    }

    // a call that fails while the store is down, until it is back
    private static <T> @Nullable T eventually(Supplier<@Nullable T> call) {
        long deadline = System.nanoTime() + CONVERGENCE.toNanos();
        while (true) {
            try {
                return call.get();
            } catch (StoreException e) {
                if (System.nanoTime() > deadline) {
                    throw e;
                }
            }
        }
    }

    @Test
    void a_restart_with_persistence_keeps_every_value_and_the_caches_go_on() {
        start(true);
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = cache();
        put(writer, "before");
        converges(holder, "before");

        restart();

        converges(cache(), "before");
        put(writer, "after");
        converges(holder, "after");
        converges(writer, "after");
    }

    @Test
    void a_restart_without_persistence_loses_every_value_and_the_caches_drop_what_they_hold() {
        start(false);
        DistributedCache<String, String> writer = cache();
        DistributedCache<String, String> holder = cache();
        put(writer, "before");
        converges(holder, "before");

        restart();

        converges(holder, null);
        converges(writer, null);
        put(writer, "after");
        converges(holder, "after");
    }

    // a cache built while the server is stopped, as by an application restarting during the server's restart: it
    // starts without its store and, once the server is back, takes what is stored over what it loaded meanwhile and
    // distributes both ways (VISION.md, "Starting without the store")
    @Test
    void a_cache_built_while_the_server_is_stopped_connects_once_it_is_back() {
        start(true);
        DistributedCache<String, String> other = cache();
        other.put("held", "stored");

        stop();
        DistributedCache<String, String> started;
        try {
            started = cache();
            startsDegraded(started);
        } finally {
            startAgain();
        }

        recovers(other, started, CONVERGENCE);
    }

    // the same without persistence: the restart loses every value, so what both caches hold is dropped, the value
    // the started one loaded meanwhile included (never taken for stored), and they go on
    @Test
    void a_cache_built_while_the_server_is_stopped_and_loses_its_values_holds_none_of_them_once_back() {
        start(false);
        DistributedCache<String, String> other = cache();
        other.put("held", "stored");

        stop();
        DistributedCache<String, String> started;
        try {
            started = cache();
            startsDegraded(started);
        } finally {
            startAgain();
        }

        assertThat(awaitUpTo(() -> started.distributedPolicy().getSynchronizationState()
                == SynchronizationState.SYNCHRONIZED, CONVERGENCE, Duration.ofMillis(200))).as("connected").isTrue();
        assertThat(awaitUpTo(() -> eventually(() -> started.getIfPresent("held")) == null, CONVERGENCE,
                Duration.ofMillis(200))).as("the value loaded meanwhile dropped").isTrue();
        assertThat(awaitUpTo(() -> eventually(() -> other.getIfPresent("held")) == null, CONVERGENCE,
                Duration.ofMillis(200))).as("the value lost by the restart dropped").isTrue();
        AdapterConformanceTest.put(other, "held", "after", CONVERGENCE);
        assertThat(awaitUpTo(() -> "after".equals(eventually(() -> started.getIfPresent("held"))), CONVERGENCE,
                Duration.ofMillis(200))).as("distributing again").isTrue();
    }
}
