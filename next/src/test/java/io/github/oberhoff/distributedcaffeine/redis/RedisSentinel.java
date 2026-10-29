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

import com.github.dockerjava.api.command.InspectContainerCmd;
import com.github.dockerjava.api.command.KillContainerCmd;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ulimit;
import io.github.oberhoff.distributedcaffeine.testing.Ports;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.internal.HostAndPort;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.MappingSocketAddressResolver;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;
import org.jspecify.annotations.NullMarked;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.RedisSentinelClient;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;

/**
 * A primary, a replica and a Sentinel watching them ({@code primary}, quorum 1, a primary down after 1 s), on a
 * network of their own, each listening on a free host port published as the same port, so that an address Sentinel
 * reports (a container's IP and its port) reaches the node from here once mapped to {@code 127.0.0.1}, which the
 * client's resources do. A failover can be forced.
 */
@NullMarked
final class RedisSentinel implements RedisTopology, AutoCloseable {

    static final String PRIMARY = "primary";
    private static final Duration FAILOVER = Duration.ofSeconds(60);

    final String image;
    final ClientResources resources = ClientResources.builder()
            .socketAddressResolver(MappingSocketAddressResolver.create(
                    (Function<HostAndPort, HostAndPort>) address -> HostAndPort.of("127.0.0.1", address.getPort())))
            .build();
    // through Sentinel, as a user's would be: Lettuce's, Jedis's and Redisson's, each mapping the addresses Sentinel
    // reports
    final RedisClient client;
    final RedisSentinelClient jedis;
    final RedissonClient redisson;
    // the tests' own, to each node directly
    private final RedisClient admin = RedisClient.create();
    private final Network network = Network.newNetwork();
    private final List<GenericContainer<?>> nodes = new ArrayList<>();
    private final List<Integer> ports;
    // the node kill() killed, until revived
    private int killed = -1;

    RedisSentinel(String image) {
        this.image = image;
        this.ports = Ports.startOnFree(3, this::start);
        // a Kvrocks or Dragonfly replica logs nothing when it caught up, so its start is not waited for that
        if (kvrocks() || dragonfly()) {
            awaitInSync();
        }
        this.client = RedisClient.create(resources, RedisURI.Builder.sentinel("127.0.0.1", ports.get(2), PRIMARY)
                .build());
        this.jedis = jedisClient();
        Config config = new Config();
        config.useSentinelServers()
                .setMasterName(PRIMARY)
                .addSentinelAddress("redis://127.0.0.1:" + ports.get(2))
                .setCheckSentinelsList(false)
                .setSentinelsDiscovery(false)
                .setNatMapper(address -> new org.redisson.misc.RedisURI(address.getScheme(), "127.0.0.1",
                        address.getPort()));
        this.redisson = Redisson.create(config);
    }

    // a node's IP on the topology's network
    private static String ip(GenericContainer<?> node) {
        try (InspectContainerCmd inspect = node.getDockerClient().inspectContainerCmd(node.getContainerId())) {
            return String.valueOf(inspect.exec().getNetworkSettings().getNetworks().values().iterator().next()
                    .getIpAddress());
        }
    }

    // Redis 6.2.0's Sentinel never registers the old primary as a replica after the failover (no "+slave" line for it),
    // so never converts it when it returns; under load the topology then stayed broken for every later test (found:
    // Sentinel naming a replica as the primary). Fixed in later 6.2 releases, which kill and revive
    @Override
    public boolean revives() {
        return !image.equals("redis:6.2.0");
    }

    // the host port of the node Sentinel names the replica now
    int replicaPort() {
        return ports.get(1 - primary());
    }

    // Jedis asks Sentinel for the primary while being built, and fails at once where Sentinel does not name one yet
    // (found: "All sentinels down" right after Sentinel logged its monitor): built once Sentinel does
    private RedisSentinelClient jedisClient() {
        require(() -> {
            try (StatefulRedisSentinelConnection<String, String> sentinel = sentinel()) {
                return sentinel.sync().getMasterAddrByName(PRIMARY) != null;
            } catch (RuntimeException e) {
                return false;
            }
        }, "Sentinel naming the primary");
        return RedisSentinelClient.builder()
                .masterName(PRIMARY)
                .sentinels(Set.of(new redis.clients.jedis.HostAndPort("127.0.0.1", ports.get(2))))
                .clientConfig(DefaultJedisClientConfig.builder()
                        .hostAndPortMapper(address -> new redis.clients.jedis.HostAndPort("127.0.0.1",
                                address.getPort()))
                        .build())
                .build();
    }

    // node 0 the primary, node 1 its replica, node 2 Sentinel; on failure, what was started is stopped again
    private void start(List<Integer> chosen) {
        nodes.clear();
        boolean foreign = kvrocks() || dragonfly();
        String server = image.startsWith("valkey") ? "valkey-server" : "redis-server";
        String ready = "(?is).*ready to accept connections.*";
        int primary = chosen.get(0);
        int replica = chosen.get(1);
        try {
            // the primary first: Kvrocks and Dragonfly follow their primary by address only, so the replica needs its
            // IP
            GenericContainer<?> first = foreign
                    ? node(image, "node0", primary, ready, "--port", String.valueOf(primary))
                    : node(image, "node0", primary, ready, server, "--port", String.valueOf(primary));
            nodes.add(first);
            first.start();
            String primaryAddress = foreign ? ip(first) : "node0";
            if (kvrocks()) {
                // Kvrocks takes the primary as one option value
                nodes.add(node(image, "node1", replica, ready, "--port", String.valueOf(replica), "--slaveof",
                        primaryAddress + " " + primary));
            } else if (dragonfly()) {
                nodes.add(node(image, "node1", replica, ready, "--port", String.valueOf(replica),
                        "--replicaof=" + primaryAddress + ":" + primary));
            } else {
                nodes.add(node(image, "node1", replica, "(?is).*sync: Finished with success.*", server, "--port",
                        String.valueOf(replica), "--replicaof", "node0", String.valueOf(primary)));
            }
            // Sentinel is Redis's own, also over Kvrocks and Dragonfly
            String sentinelImage = foreign ? "redis:8" : image;
            String sentinelServer = foreign ? "redis-server" : server;
            String configuration = String.join("\\n", "port " + chosen.get(2), "sentinel resolve-hostnames yes",
                    "sentinel monitor " + PRIMARY + " " + primaryAddress + " " + primary + " 1",
                    "sentinel down-after-milliseconds " + PRIMARY + " 1000",
                    "sentinel failover-timeout " + PRIMARY + " 5000");
            nodes.add(node(sentinelImage, "node2", chosen.get(2), "(?is).*\\+monitor master.*", "sh", "-c",
                    "printf '" + configuration + "\\n' > /tmp/sentinel.conf && exec " + sentinelServer
                            + " /tmp/sentinel.conf --sentinel"));
            for (GenericContainer<?> node : nodes.subList(1, nodes.size())) {
                node.start();
            }
        } catch (RuntimeException e) {
            nodes.forEach(GenericContainer::stop);
            throw e;
        }
    }

    private boolean kvrocks() {
        return image.contains("kvrocks");
    }

    private boolean dragonfly() {
        return image.contains("dragonfly");
    }

    // Dragonfly (logging to a file) waited for by its port, with the locked memory its docs ask for; the others by
    // their log
    @SuppressWarnings("resource") // the nodes are stopped with the topology
    private GenericContainer<?> node(String nodeImage, String alias, int port, String ready, String... command) {
        boolean dragonflyNode = nodeImage.contains("dragonfly");
        return new GenericContainer<>(nodeImage)
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withCommand(command)
                .withExposedPorts(port)
                .withCreateContainerCmdModifier(cmd -> {
                    HostConfig host = Objects.requireNonNull(cmd.getHostConfig()).withPortBindings(new PortBinding(
                            com.github.dockerjava.api.model.Ports.Binding.bindPort(port), ExposedPort.tcp(port)));
                    if (dragonflyNode) {
                        host.withUlimits(List.of(new Ulimit("memlock", -1L, -1L)));
                    }
                    cmd.withHostConfig(host);
                })
                .waitingFor(dragonflyNode ? Wait.forListeningPort() : Wait.forLogMessage(ready, 1));
    }

    // Kvrocks's and Dragonfly's Lua have no redis.set_repl, so a write cannot be kept from the replica on demand
    @Override
    public boolean losesWritesOnDemand() {
        return !kvrocks() && !dragonfly();
    }

    // a connection to one data node directly, whatever its role
    StatefulRedisConnection<String, String> direct(int node) {
        return admin.connect(RedisURI.create("redis://127.0.0.1:" + ports.get(node)));
    }

    private StatefulRedisSentinelConnection<String, String> sentinel() {
        return admin.connectSentinel(RedisURI.create("redis://127.0.0.1:" + ports.get(2)));
    }

    // the node Sentinel names as the primary now (0 or 1), whatever the key
    @Override
    public int primary(String key) {
        return primary();
    }

    private int primary() {
        try (StatefulRedisSentinelConnection<String, String> sentinel = sentinel()) {
            SocketAddress address = sentinel.sync().getMasterAddrByName(PRIMARY);
            return ports.indexOf(((InetSocketAddress) address).getPort());
        }
    }

    // right after a failover the new replica resynchronises for a while, its link down; a write meanwhile reaches it
    // only later, and a failover before that loses the write (found: a test relying on replication lost its own value)
    @Override
    public void awaitInSync(String key) {
        awaitInSync();
    }

    private void awaitInSync() {
        require(() -> {
            int primary = primary();
            Map<String, String> leading = replication(primary);
            Map<String, String> following = replication(1 - primary);
            if (!"up".equals(following.get("master_link_status"))) {
                return false;
            }
            // Dragonfly reports no offsets, but its replicas' state and lag on the primary
            if (leading.get("master_repl_offset") == null) {
                String described = String.valueOf(leading.get("slave0"));
                return described.contains("state=online") && described.contains("lag=0");
            }
            return Long.parseLong(following.getOrDefault("master_repl_offset", "-1"))
                    >= Long.parseLong(leading.get("master_repl_offset"));
        }, "the replica in sync with the primary");
    }

    private Map<String, String> replication(int node) {
        try (StatefulRedisConnection<String, String> direct = direct(node)) {
            Map<String, String> fields = new java.util.LinkedHashMap<>();
            direct.sync().info("replication").lines().filter(line -> line.contains(":")).forEach(line ->
                    fields.put(line.substring(0, line.indexOf(':')), line.substring(line.indexOf(':') + 1).trim()));
            return fields;
        } catch (RuntimeException e) {
            return Map.of();
        }
    }

    @Override
    public void failover(String key) {
        awaitInSync();
        int before = primary();
        // Sentinel refuses while its view of the replica is stale (NOGOODSLAVE: right after a failover it has not yet
        // seen the old primary serve as a replica) or another failover runs (INPROG): asked again until it accepts
        require(() -> {
            try (StatefulRedisSentinelConnection<String, String> sentinel = sentinel()) {
                sentinel.sync().failover(PRIMARY);
                return true;
            } catch (RedisCommandExecutionException e) {
                String error = String.valueOf(e.getMessage());
                if (error.startsWith("NOGOODSLAVE") || error.startsWith("INPROG")) {
                    return false;
                }
                throw e;
            }
        }, "Sentinel accepting a failover");
        try {
            require(() -> primary() != before && role(before).equals("slave"), "a failover away from node " + before);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(e.getMessage() + "; " + sentinelView(), e);
        }
    }

    // Sentinel's view and its log's end, for a failover that did not happen
    private String sentinelView() {
        StringBuilder view = new StringBuilder();
        try (StatefulRedisSentinelConnection<String, String> sentinel = sentinel()) {
            view.append("primary ").append(sentinel.sync().master(PRIMARY)).append("; replicas ")
                    .append(sentinel.sync().replicas(PRIMARY));
        } catch (RuntimeException e) {
            view.append("Sentinel: ").append(e.getMessage());
        }
        String log = nodes.get(2).getLogs();
        view.append("; Sentinel's log: ").append(log.substring(Math.max(0, log.length() - 3_000)));
        for (int node = 0; node < 2; node++) {
            String nodeLog = nodes.get(node).getLogs();
            view.append("; node ").append(node).append("'s log: ")
                    .append(nodeLog.substring(Math.max(0, nodeLog.length() - 2_500)));
        }
        return view.toString();
    }

    // Sentinel notices the primary down after a second, then promotes the replica
    @Override
    public void kill(String key) {
        awaitInSync();
        int before = primary();
        GenericContainer<?> node = nodes.get(before);
        try (KillContainerCmd kill = node.getDockerClient().killContainerCmd(node.getContainerId())) {
            kill.exec();
        }
        killed = before;
        require(() -> primary() == 1 - before, "Sentinel promoting node " + (1 - before));
    }

    // Sentinel turns the old primary into a replica of the new one once it is back
    @Override
    public void revive() {
        if (killed < 0) {
            return;
        }
        GenericContainer<?> node = nodes.get(killed);
        String before = ip(node);
        try (StartContainerCmd start = node.getDockerClient().startContainerCmd(node.getContainerId())) {
            start.exec();
        }
        int revived = killed;
        killed = -1;
        // Sentinel turns a returning old primary into a replica of the new one
        try {
            require(() -> role(revived).equals("slave"), "node " + revived + " back as the replica");
        } catch (IllegalStateException e) {
            String log = node.getLogs();
            throw new IllegalStateException(e.getMessage() + "; its log's end: "
                    + log.substring(Math.max(0, log.length() - 1_500)) + "; its IP " + before + " before, " + ip(node)
                    + " after; role " + role(revived) + "; " + sentinelView(), e);
        }
        awaitInSync();
        // Sentinel keeps a stale entry of the revived node besides its new one (named ":<port>", without an address),
        // which a later failover may pick and then wait on until it times out (found: the next failovers ended for
        // timeout or failed back). A reset makes it forget its replicas and find them anew from the primary, as
        // Sentinel's docs advise after a topology change
        try (StatefulRedisSentinelConnection<String, String> sentinel = sentinel()) {
            sentinel.sync().reset(PRIMARY);
        }
        // Sentinel's own view too: until it sees the replica up again (flags no longer s_down or disconnected), it
        // promotes no one
        require(() -> {
            try (StatefulRedisSentinelConnection<String, String> sentinel = sentinel()) {
                return sentinel.sync().replicas(PRIMARY).stream().anyMatch(replica ->
                        String.valueOf(ports.get(revived)).equals(replica.get("port"))
                                && "slave".equals(replica.get("flags")));
            }
        }, "Sentinel seeing node " + revived + " as a healthy replica");
    }

    @Override
    public String describe(String key) {
        StringBuilder described = new StringBuilder();
        for (int node = 0; node < 2; node++) {
            try (StatefulRedisConnection<String, String> direct = direct(node)) {
                Map<String, String> replication = replication(node);
                described.append("node ").append(node).append(": ").append(replication.get("role"))
                        .append(", link ").append(replication.getOrDefault("master_link_status", "-"))
                        .append(", offset ").append(replication.get("master_repl_offset"))
                        .append(", ").append(key).append(' ').append(direct.sync().hgetall(key)).append("; ");
            } catch (RuntimeException e) {
                described.append("node ").append(node).append(": ").append(e.getMessage()).append("; ");
            }
        }
        return described.toString();
    }

    private static void require(BooleanSupplier condition, String what) {
        if (!awaitUpTo(condition, FAILOVER, Duration.ofMillis(200))) {
            throw new IllegalStateException("not within " + FAILOVER + ": " + what);
        }
    }

    // by INFO replication, as the adapter asks, not ROLE: Kvrocks 2.17.0 crashed answering ROLE while it resynchronised
    // as a replica (found 2026-11-02: a segmentation fault in Storage::LatestSeqNumber, the node gone)
    private String role(int node) {
        return replication(node).getOrDefault("role", "unreachable");
    }

    @Override
    public void close() {
        redisson.shutdown();
        jedis.close();
        client.shutdown();
        admin.shutdown();
        try {
            resources.shutdown().get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Lettuce's resources did not shut down", e);
        }
        nodes.forEach(GenericContainer::stop);
        network.close();
    }
}
