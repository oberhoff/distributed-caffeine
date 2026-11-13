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

import io.github.oberhoff.distributedcaffeine.AdapterConformanceTest.AdapterConformance;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.redisson.Redisson;
import org.redisson.config.Config;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * The Redis family's side of the conformance suite, on a fresh keyspace of the running server, through one client
 * library: the clients a test creates of its own are created as a starting application creates them, while the server
 * may be out (Redisson's with lazy initialization, since Redisson.create connects to the server otherwise).
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class RedisConformance implements AdapterConformance {

    private final RedisTestServer server;
    private final RedisOutage.Library library;
    private final String keyspace = RedisTestServer.freshName();
    private final List<AutoCloseable> clients = new ArrayList<>();

    RedisConformance(RedisTestServer server, RedisOutage.Library library) {
        this.server = server;
        this.library = library;
    }

    @Override
    public Adapter<String, String> adapter(String discriminator) {
        RedisDriver driver = switch (library) {
            case LETTUCE -> LettuceRedis.of(server.client);
            case JEDIS -> JedisRedis.of(server.jedis);
            case REDISSON -> RedissonRedis.of(server.redisson);
        };
        return RedisAdapter.newBuilder(driver, keyspace).withDiscriminator(discriminator).build();
    }

    @Override
    public void markOtherVersion() {
        server.admin.sync().set(new RedisKeys(keyspace, RedisAdapter.DEFAULT_DISCRIMINATOR).schema(), "2");
    }

    // a keyspace is a hash tag of the cache's own, which no one else writes under
    @Override
    public boolean putForeignData() {
        return false;
    }

    @Override
    public Adapter<String, String> unreachable() {
        return adapterOf("127.0.0.1", 1, null, null);
    }

    @Override
    public InetSocketAddress server() {
        return InetSocketAddress.createUnresolved(server.host(), server.port());
    }

    @Override
    public Adapter<String, String> through(String host, int port) {
        return adapterOf(host, port, null, null);
    }

    // a user the server does not know (ACLs since Redis 6); Kvrocks has no users, and the test server no password
    @Override
    public @Nullable Adapter<String, String> wrongCredentials() {
        return server.image.contains("kvrocks") ? null : adapterOf(server.host(), server.port(), "nobody", "wrong");
    }

    // several caches on one client of their own, each wrapping it in a driver of its own, as an application does
    @Override
    public List<Adapter<String, String>> sharingThrough(String host, int port, int count) {
        Supplier<RedisDriver> drivers = driversOf(host, port, null, null);
        List<Adapter<String, String>> adapters = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            adapters.add(RedisAdapter.newBuilder(drivers.get(), keyspace).withDiscriminator("shared" + i).build());
        }
        return adapters;
    }

    private Adapter<String, String> adapterOf(String host, int port, @Nullable String user,
                                              @Nullable String password) {
        return RedisAdapter.newBuilder(driversOf(host, port, user, password).get(), keyspace).build();
    }

    // a client of its own of the library, waiting at most a second to connect and two per command; each driver wraps
    // it anew
    private Supplier<RedisDriver> driversOf(String host, int port, @Nullable String user, @Nullable String password) {
        return switch (library) {
            case LETTUCE -> {
                RedisURI.Builder uri = RedisURI.builder().withHost(host).withPort(port)
                        .withTimeout(Duration.ofSeconds(2));
                RedisClient client = RedisClient.create(user == null ? uri.build()
                        : uri.withAuthentication(user, password == null ? "" : password).build());
                clients.add(client);
                yield () -> LettuceRedis.of(client);
            }
            case JEDIS -> {
                redis.clients.jedis.RedisClient client = user == null
                        ? redis.clients.jedis.RedisClient.create(host, port)
                        : redis.clients.jedis.RedisClient.create(host, port, user, password);
                clients.add(client);
                yield () -> JedisRedis.of(client);
            }
            case REDISSON -> {
                Config config = new Config().setLazyInitialization(true).setUsername(user).setPassword(password);
                config.useSingleServer().setAddress("redis://" + host + ":" + port)
                        .setConnectTimeout(1_000).setTimeout(2_000).setPingConnectionInterval(1_000);
                org.redisson.api.RedissonClient client = Redisson.create(config);
                clients.add(client::shutdown);
                yield () -> RedissonRedis.of(client);
            }
        };
    }

    @Override
    public String product() {
        String image = server.image.toLowerCase(Locale.ROOT);
        return image.contains("valkey") ? "Valkey" : image.contains("dragonfly") ? "Dragonfly"
                : image.contains("kvrocks") ? "Kvrocks" : "Redis";
    }

    @Override
    public void close() {
        for (AutoCloseable client : clients) {
            try {
                client.close();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
