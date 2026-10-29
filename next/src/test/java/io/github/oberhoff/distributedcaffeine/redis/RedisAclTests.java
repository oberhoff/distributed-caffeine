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

import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The adapter through a user allowed nothing but what it needs (as a managed service's restricted user, or an
 * operator's least privilege), on the server of the enclosing image: the commands it sends, those its scripts call
 * (Redis checks them against the user as well), and the connection handshakes of the client libraries, on the
 * library's keys only ({@code {dc:*}}). Both contracts through Lettuce; a cache pair through each library.
 */
@NullMarked
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SuppressWarnings("resource") // the server is the enclosing image's; the clients are closed after the tests
abstract class RedisAclTests {

    /**
     * What the adapter needs, as an ACL rule: its own commands (scripts, the stream read, the server's description),
     * the commands its scripts call, and the handshakes of the client libraries (HELLO, AUTH, PING, CLIENT SETINFO and
     * SETNAME, SELECT); on its keys only, no channels. Documented in {@link RedisAdapter}.
     */
    static final String RULES = "resetkeys ~{dc:* resetchannels -@all +evalsha +eval +xread +info +time +get +set "
            + "+del +incr +hget +hmget +hset +hdel +xadd +xrevrange +zadd +zcount +zrangebyscore +zrem +hello +auth "
            + "+ping +select +client|setname";
    // CLIENT SETINFO, sent by the libraries since Redis 7.2, which an older server does not know as a subcommand
    private static final String SETINFO = " +client|setinfo";
    private static final String USER = "dc-cache";
    // the same without EVALSHA and EVAL: a user running no scripts, as where a service or an operator disables them
    private static final String SCRIPTLESS = "dc-scriptless";
    private static final String PASSWORD = "dc-secret";

    private @Nullable RedisCredentials credentials;

    // the server of the enclosing image
    protected abstract RedisTestServer server();

    private record RedisCredentials(io.lettuce.core.RedisClient lettuce, redis.clients.jedis.RedisClient jedis,
                                    RedissonClient redisson) {
    }

    @BeforeAll
    void createUsers() {
        String rules = "on >" + PASSWORD + " " + RULES;
        createUser(USER, rules);
        createUser(SCRIPTLESS, rules.replace(" +evalsha +eval", ""));
        credentials = connect(USER);
    }

    // the most precise rules the server takes: CLIENT SETINFO where it knows it, then without; Dragonfly takes no
    // subcommand rules at all, so CLIENT as a whole there
    private void createUser(String user, String rules) {
        try {
            setUser(user, rules + SETINFO);
        } catch (RedisCommandExecutionException e) {
            // Kvrocks has no users at all (a password and namespaces instead)
            Assumptions.assumeFalse(String.valueOf(e.getMessage()).contains("unknown command"),
                    "the server has no ACL: " + e.getMessage());
            try {
                setUser(user, rules);
            } catch (RedisCommandExecutionException again) {
                setUser(user, rules.replace("+client|setname", "+client"));
            }
        }
    }

    // a client of each library as the user
    private RedisCredentials connect(String user) {
        String host = server().host();
        int port = server().port();
        io.lettuce.core.RedisClient lettuce = io.lettuce.core.RedisClient.create(RedisURI.builder()
                .withHost(host).withPort(port).withAuthentication(user, PASSWORD.toCharArray()).build());
        redis.clients.jedis.RedisClient jedis = redis.clients.jedis.RedisClient.builder()
                .hostAndPort(new HostAndPort(host, port))
                .clientConfig(DefaultJedisClientConfig.builder().user(user).password(PASSWORD).build())
                .build();
        Config config = new Config();
        config.setUsername(user).setPassword(PASSWORD).useSingleServer().setAddress("redis://" + host + ":" + port);
        return new RedisCredentials(lettuce, jedis, Redisson.create(config));
    }

    private static void close(RedisCredentials clients) {
        clients.redisson().shutdown();
        clients.jedis().close();
        clients.lettuce().shutdown();
    }

    @AfterAll
    void deleteUsers() {
        RedisCredentials current = credentials;
        if (current != null) {
            close(current);
            server().admin.sync().aclDeluser(USER, SCRIPTLESS);
        }
    }

    // ACL SETUSER with the rules, on a user of the tests' own (deleted after them)
    private void setUser(String user, String rules) {
        CommandArgs<String, String> arguments = new CommandArgs<>(StringCodec.UTF8).add("SETUSER").add(user);
        for (String rule : rules.split(" ", -1)) {
            arguments.add(rule);
        }
        server().admin.sync().dispatch(CommandType.ACL, new StatusOutput<>(StringCodec.UTF8), arguments);
    }

    private RedisCredentials credentials() {
        return requireNonNull(credentials, "the user is created before the tests");
    }

    private RedisDriver driver(String library) {
        return driver(library, credentials());
    }

    private static RedisDriver driver(String library, RedisCredentials clients) {
        return switch (library) {
            case "Lettuce" -> LettuceRedis.of(clients.lettuce());
            case "Jedis" -> JedisRedis.of(clients.jedis());
            default -> RedissonRedis.of(clients.redisson());
        };
    }

    // a server running no scripts for the user (as where a service or an operator disables them) cannot keep a cache:
    // refused at store creation, by the probe, naming the commands needed and the server's refusal
    @ParameterizedTest
    @ValueSource(strings = {"Lettuce", "Jedis", "Redisson"})
    void a_user_not_allowed_scripts_is_refused_at_store_creation(String library) {
        RedisCredentials scriptless = connect(SCRIPTLESS);
        try {
            RedisAdapter<String, String> adapter = RedisAdapter.newBuilder(driver(library, scriptless),
                    RedisTestServer.freshName()).build();

            assertThatThrownBy(() -> {
                try (Store ignored = adapter.createStore(StoreSettings.of(Format.TEXT, Format.TEXT))) {
                    throw new AssertionError("the store was created");
                }
            }).isInstanceOfSatisfying(StoreException.class, e -> {
                assertThat(e).hasMessageContaining("lacks commands the Redis adapter needs");
                assertThat(e.isRetryable()).isFalse();
            });
        } finally {
            close(scriptless);
        }
    }

    // two caches of the restricted user (each a driver of its own, so a reader of its own), maintenance every 100 ms
    // so that its scripts run too, a key's write, its message, an invalidateAll
    @ParameterizedTest
    @ValueSource(strings = {"Lettuce", "Jedis", "Redisson"})
    void a_cache_pair_works_with_nothing_but_the_permissions_documented(String library) {
        String name = RedisTestServer.freshName();
        List<DistributedCache<String, String>> caches = new ArrayList<>();
        try {
            for (int index = 0; index < 2; index++) {
                RedisAdapter<String, String> adapter = RedisAdapter.newBuilder(driver(library), name).build();
                caches.add(DistributedCaffeine.newBuilder(adapter)
                        .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                                .withValueSerializer(StringSerializer.ofString()))
                        .withStore(store -> store.withMaximumAge(Duration.ofHours(1)).withMaximumSize(10)
                                .withMaintenanceInterval(Duration.ofMillis(100)))
                        .build());
            }
            DistributedCache<String, String> writer = caches.get(0);
            DistributedCache<String, String> holder = caches.get(1);
            writer.put("k", "first");
            assertThat(holder.getIfPresent("k")).isEqualTo("first");
            writer.put("k", "second");
            await(() -> holder.policy().getIfPresentQuietly("k") == null);
            assertThat(holder.getIfPresent("k")).isEqualTo("second");
            for (int index = 0; index < 20; index++) {
                writer.put("filler" + index, "pruned beyond 10");
            }
            writer.invalidateAll();
            await(() -> holder.policy().getIfPresentQuietly("k") == null);
            assertThat(holder.getIfPresent("k")).isNull();
        } finally {
            for (DistributedCache<String, String> cache : caches) {
                cache.close();
            }
        }
    }

    @Nested
    class StoreContract extends RedisStoreContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return server().admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return LettuceRedis.of(credentials().lettuce());
        }
    }

    @Nested
    class ChangeFeedContract extends RedisChangeFeedContractTest {

        @Override
        protected RedisClusterCommands<String, String> admin() {
            return server().admin.sync();
        }

        @Override
        protected RedisDriver driver() {
            return LettuceRedis.of(credentials().lettuce());
        }
    }
}
