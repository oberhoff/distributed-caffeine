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

import com.github.dockerjava.api.command.KillContainerCmd;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ulimit;
import io.github.oberhoff.distributedcaffeine.testing.Ports;
import io.lettuce.core.MigrateArgs;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.internal.HostAndPort;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.MappingSocketAddressResolver;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.RedisClusterClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
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
 * A cluster on a network of its own: of Redis or Valkey, three primaries, each with a replica
 * ({@code --cluster-replicas 1}), a node considered failing after 1 s; of Dragonfly, three primaries configured as a
 * cluster ({@code --cluster_mode=yes}: Dragonfly has no gossip, its control plane pushes the slots to every node), or
 * one node answering as a cluster owning every slot ({@code --cluster_mode=emulated}). Each node listens on a free host
 * port published as the same port, so that an address the cluster reports (a container's IP and its port) reaches the
 * node from here once mapped to {@code 127.0.0.1}, which the clients do. In a Redis or Valkey cluster, the primary of a
 * key's slot can be failed over to its replica, and the slot moved to another primary.
 */
@NullMarked
final class RedisCluster implements RedisTopology, AutoCloseable {

    enum Kind {
        REPLICATED(6),
        DRAGONFLY(3),
        // Dragonfly with a replica per primary, failed over and resharded by the tests as its operator would; not in
        // the matrix: Lettuce's cluster client found no usable view for over a minute after a takeover ("Cannot
        // determine own partition"), VISION.md "Redis adapters", item 18
        DRAGONFLY_REPLICATED(6),
        DRAGONFLY_EMULATED(1),
        KVROCKS(6);

        final int nodes;

        Kind(int nodes) {
            this.nodes = nodes;
        }
    }

    private static final Duration SETTLING = Duration.ofSeconds(60);

    final String image;
    private final Kind kind;
    private final int count;
    final ClientResources resources = ClientResources.builder()
            .socketAddressResolver(MappingSocketAddressResolver.create(
                    (Function<HostAndPort, HostAndPort>) address -> HostAndPort.of("127.0.0.1", address.getPort())))
            .build();
    // a user's Lettuce client of the cluster, refreshing its view on redirects and failing connections (adaptive, on
    // by default since Lettuce 7) and every few seconds besides; and the tests' own connection through it
    final io.lettuce.core.cluster.RedisClusterClient client;
    final StatefulRedisClusterConnection<String, String> admin;
    // Jedis's and Redisson's, each mapping the addresses the cluster reports
    final RedisClusterClient jedis;
    final RedissonClient redisson;
    // the tests' own, to each node directly
    private final RedisClient direct;
    private final Network network = Network.newNetwork();
    private final List<GenericContainer<?>> nodes = new ArrayList<>();
    private final List<Integer> ports;
    // the node kill() killed, until revived
    private int killed = -1;
    // a Dragonfly primary that handed over (REPLTAKEOVER): locked, answering LOADING to everything, Lettuce's
    // connection handshake included, until told to follow
    private int locked = -1;
    // Kvrocks has no gossip and no failover of its own: the tests push its layout as its controller would. Each
    // slot's shard, each shard's primary and replica (-1 while it has none), and the layout's version
    private final int[] slotShard = new int[SlotHash.SLOT_COUNT];
    private final int[] shardPrimary = {0, 1, 2};
    private final int[] shardReplica = {3, 4, 5};
    private int layoutVersion;
    // Dragonfly's node IDs (CLUSTER MYID): its own, not set by the tests
    private final String[] dragonflyIds = new String[6];

    RedisCluster(String image) {
        this(image, image.contains("dragonfly") ? Kind.DRAGONFLY : Kind.REPLICATED);
    }

    RedisCluster(String image, Kind kind) {
        this.image = image;
        this.kind = kind;
        this.count = kind.nodes;
        this.ports = Ports.startOnFree(count, this::start);
        this.direct = RedisClient.create(resources);
        create();
        this.client = io.lettuce.core.cluster.RedisClusterClient.create(resources,
                RedisURI.create("redis://127.0.0.1:" + ports.get(0)));
        client.setOptions(ClusterClientOptions.builder()
                .socketOptions(SocketOptions.builder().connectTimeout(Duration.ofSeconds(2)).build())
                .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
                        .enablePeriodicRefresh(Duration.ofSeconds(5))
                        .build())
                .build());
        this.admin = client.connect();
        this.jedis = RedisClusterClient.builder()
                .nodes(Set.of(new redis.clients.jedis.HostAndPort("127.0.0.1", ports.get(0))))
                .clientConfig(DefaultJedisClientConfig.builder()
                        .hostAndPortMapper(address -> new redis.clients.jedis.HostAndPort("127.0.0.1",
                                address.getPort()))
                        .build())
                .build();
        Config config = new Config();
        config.useClusterServers()
                .addNodeAddress("redis://127.0.0.1:" + ports.get(0))
                .setScanInterval(1_000)
                .setNatMapper(address -> new org.redisson.misc.RedisURI(address.getScheme(), "127.0.0.1",
                        address.getPort()));
        this.redisson = Redisson.create(config);
    }

    private String server() {
        return image.startsWith("valkey") ? "valkey-server" : "redis-server";
    }

    private String cli() {
        return image.startsWith("valkey") ? "valkey-cli" : "redis-cli";
    }

    // every node in cluster mode, not yet joined; on failure, what was started is stopped again
    private void start(List<Integer> chosen) {
        nodes.clear();
        for (int node = 0; node < count; node++) {
            int port = chosen.get(node);
            nodes.add(switch (kind) {
                case REPLICATED -> node("node" + node, port, server(), "--port", String.valueOf(port),
                        "--cluster-enabled", "yes", "--cluster-node-timeout", "1000");
                case DRAGONFLY, DRAGONFLY_REPLICATED -> node("node" + node, port, "--port", String.valueOf(port),
                        "--cluster_mode=yes");
                case DRAGONFLY_EMULATED -> node("node" + node, port, "--port", String.valueOf(port),
                        "--cluster_mode=emulated");
                case KVROCKS -> node("node" + node, port, "--port", String.valueOf(port), "--cluster-enabled", "yes");
            });
        }
        try {
            for (GenericContainer<?> node : nodes) {
                node.start();
            }
        } catch (RuntimeException e) {
            nodes.forEach(GenericContainer::stop);
            throw e;
        }
    }

    // Dragonfly with the locked memory its docs ask for, and waited for by its port, as it logs to a file
    @SuppressWarnings("resource") // the nodes are stopped with the topology
    private GenericContainer<?> node(String alias, int port, String... command) {
        boolean dragonfly = kind != Kind.REPLICATED && kind != Kind.KVROCKS;
        return new GenericContainer<>(image)
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withCommand(command)
                .withExposedPorts(port)
                .withCreateContainerCmdModifier(cmd -> {
                    HostConfig host = Objects.requireNonNull(cmd.getHostConfig()).withPortBindings(new PortBinding(
                            com.github.dockerjava.api.model.Ports.Binding.bindPort(port), ExposedPort.tcp(port)));
                    if (dragonfly) {
                        host.withUlimits(List.of(new Ulimit("memlock", -1L, -1L)));
                    }
                    cmd.withHostConfig(host);
                })
                .waitingFor(dragonfly ? Wait.forListeningPort()
                        : Wait.forLogMessage("(?is).*ready to accept connections.*", 1));
    }

    // joins the nodes (by their IPs on the network, as hostnames need Redis 7): Redis or Valkey by redis-cli, the
    // slots spread over the first three, a replica each; Dragonfly by the same slot configuration pushed to every
    // node. Then waits until every node sees the cluster healthy and every replica in sync
    private void create() {
        int shards = kind == Kind.DRAGONFLY_EMULATED || kind == Kind.DRAGONFLY ? count : count / 2;
        if (kind == Kind.REPLICATED) {
            List<String> command = new ArrayList<>(List.of(cli(), "--cluster", "create"));
            for (int node = 0; node < count; node++) {
                command.add(ip(node) + ":" + ports.get(node));
            }
            command.addAll(List.of("--cluster-replicas", "1", "--cluster-yes"));
            exec(0, command);
        } else if (kind == Kind.KVROCKS) {
            for (int node = 0; node < count; node++) {
                clusterx(node, "SETNODEID", nodeId(node));
            }
            for (int slot = 0; slot < SlotHash.SLOT_COUNT; slot++) {
                slotShard[slot] = slot * 3 / SlotHash.SLOT_COUNT;
            }
            push();
        } else if (kind == Kind.DRAGONFLY || kind == Kind.DRAGONFLY_REPLICATED) {
            // the layout first, then each replica told to follow its primary (Dragonfly's layout names replicas, but
            // replication is started by REPLICAOF, as its operator does); none without replicas
            for (int node = 0; node < count; node++) {
                try (StatefulRedisConnection<String, String> connection = direct(node)) {
                    dragonflyIds[node] = connection.sync().clusterMyId();
                }
            }
            for (int slot = 0; slot < SlotHash.SLOT_COUNT; slot++) {
                slotShard[slot] = slot * 3 / SlotHash.SLOT_COUNT;
            }
            if (kind == Kind.DRAGONFLY) {
                Arrays.fill(shardReplica, -1);
            }
            push();
            for (int shard = 0; shard < 3 && kind == Kind.DRAGONFLY_REPLICATED; shard++) {
                follow(shardReplica[shard], shardPrimary[shard]);
            }
        }
        require(() -> {
            for (int node = 0; node < count; node++) {
                if (!"ok".equals(fields(node, "CLUSTER INFO").get("cluster_state"))
                        || shards(node).size() != shards) {
                    return false;
                }
            }
            return true;
        }, "every node seeing the cluster healthy");
        if (kind != Kind.DRAGONFLY_EMULATED && kind != Kind.DRAGONFLY) {
            for (Shard shard : shards(0).values()) {
                awaitInSync(shard.slots().get(0)[0]);
            }
        }
    }

    private void exec(int node, List<String> command) {
        try {
            ExecResult result = nodes.get(node).execInContainer(command.toArray(String[]::new));
            if (result.getExitCode() != 0 || result.getStdout().startsWith("ERR")) {
                throw new IllegalStateException("a command in node " + node + " failed: " + result.getStdout()
                        + result.getStderr());
            }
        } catch (IOException e) {
            throw new IllegalStateException("a command in node " + node + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted running a command in node " + node, e);
        }
    }

    // a Redis or Valkey cluster fails over by itself and moves slots by its commands; Kvrocks by the layout the tests
    // push as its controller; Dragonfly leaves both to its control plane
    @Override
    public boolean failsOver() {
        return kind == Kind.REPLICATED || kind == Kind.KVROCKS || kind == Kind.DRAGONFLY_REPLICATED;
    }

    // Kvrocks's Lua has no redis.set_repl
    @Override
    public boolean losesWritesOnDemand() {
        return kind == Kind.REPLICATED;
    }

    // a Kvrocks node's ID, set once (CLUSTERX SETNODEID) and kept across a restart (nodes.conf)
    private static String nodeId(int node) {
        return String.valueOf((char) ('a' + node)).repeat(40);
    }

    // a CLUSTERX subcommand on one Kvrocks node
    private void clusterx(int node, String... arguments) {
        CommandArgs<String, String> command = new CommandArgs<>(StringCodec.UTF8);
        for (String argument : arguments) {
            command.add(argument);
        }
        try (StatefulRedisConnection<String, String> connection = direct(node)) {
            connection.sync().dispatch(CLUSTERX, new StatusOutput<>(StringCodec.UTF8), command);
        }
    }

    private static final ProtocolKeyword CLUSTERX = new ProtocolKeyword() {
        @Override
        public byte[] getBytes() {
            return "CLUSTERX".getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public String toString() {
            return "CLUSTERX";
        }
    };

    // a node following another as its replica (REPLICAOF), or none (REPLICAOF NO ONE, for a negative primary)
    private void follow(int node, int primary) {
        try (StatefulRedisConnection<String, String> connection = direct(node)) {
            if (primary < 0) {
                connection.sync().replicaofNoOne();
            } else {
                connection.sync().replicaof(ip(primary), ports.get(primary));
            }
        }
    }

    // a command Lettuce does not know, on one node, its reply as Lettuce reads nested arrays and statuses
    private @Nullable List<Object> command(int node, String name, String... arguments) {
        CommandArgs<String, String> command = new CommandArgs<>(StringCodec.UTF8);
        for (String argument : arguments) {
            command.add(argument);
        }
        ProtocolKeyword keyword = new ProtocolKeyword() {
            @Override
            public byte[] getBytes() {
                return name.getBytes(StandardCharsets.US_ASCII);
            }

            @Override
            public String toString() {
                return name;
            }
        };
        try (StatefulRedisConnection<String, String> connection = direct(node)) {
            return connection.sync().dispatch(keyword, new NestedMultiOutput<>(StringCodec.UTF8), command);
        }
    }

    // pushes the layout as the cluster's controller does: Kvrocks's under a new version (CLUSTERX SETNODES), or
    // Dragonfly's (DFLYCLUSTER CONFIG), to every node running
    private void push() {
        push(-1, -1);
    }

    // the same, with Dragonfly's migration of one slot to another shard underway (none for a negative slot)
    private void push(int migrating, int toShard) {
        if (kind == Kind.DRAGONFLY || kind == Kind.DRAGONFLY_REPLICATED) {
            pushDragonfly(migrating, toShard);
            return;
        }
        layoutVersion++;
        StringBuilder layout = new StringBuilder();
        for (int shard = 0; shard < 3; shard++) {
            StringBuilder ranges = new StringBuilder();
            for (int slot = 0; slot < SlotHash.SLOT_COUNT; slot++) {
                if (slotShard[slot] == shard && (slot == 0 || slotShard[slot - 1] != shard)) {
                    int last = slot;
                    while (last + 1 < SlotHash.SLOT_COUNT && slotShard[last + 1] == shard) {
                        last++;
                    }
                    ranges.append(' ').append(slot == last ? String.valueOf(slot) : slot + "-" + last);
                }
            }
            int primary = shardPrimary[shard];
            layout.append(nodeId(primary)).append(' ').append(ip(primary)).append(' ').append(ports.get(primary))
                    .append(" master -").append(ranges).append('\n');
            int replica = shardReplica[shard];
            if (replica >= 0) {
                layout.append(nodeId(replica)).append(' ').append(ip(replica)).append(' ').append(ports.get(replica))
                        .append(" slave ").append(nodeId(primary)).append('\n');
            }
        }
        for (int node = 0; node < count; node++) {
            if (node != killed) {
                clusterx(node, "SETNODES", layout.toString(), String.valueOf(layoutVersion));
            }
        }
    }

    private String ip(int node) {
        ContainerNetwork attached = nodes.get(node).getContainerInfo().getNetworkSettings().getNetworks().values()
                .iterator().next();
        return Objects.requireNonNull(attached.getIpAddress());
    }

    // a connection to one node directly, whatever its role
    StatefulRedisConnection<String, String> direct(int node) {
        return direct.connect(RedisURI.create("redis://127.0.0.1:" + ports.get(node)));
    }

    // the primaries a node sees, by node id: their slots and the replicas following them
    private Map<String, Shard> shards(int node) {
        String described;
        try (StatefulRedisConnection<String, String> connection = direct(node)) {
            described = connection.sync().clusterNodes();
        }
        Map<String, Shard> shards = new LinkedHashMap<>();
        List<String[]> replicas = new ArrayList<>();
        for (String line : described.lines().filter(line -> !line.isBlank()).toList()) {
            String[] fields = line.split(" ", -1);
            int port = Integer.parseInt(fields[1].substring(fields[1].indexOf(':') + 1, fields[1].indexOf('@')));
            if (fields[2].contains("fail")) {
                continue;
            }
            if (fields[2].contains("master")) {
                List<int[]> slots = new ArrayList<>();
                for (int i = 8; i < fields.length; i++) {
                    if (fields[i].startsWith("[")) {
                        continue; // a slot in migration
                    }
                    String[] range = fields[i].split("-", -1);
                    slots.add(new int[]{Integer.parseInt(range[0]),
                            Integer.parseInt(range[range.length - 1])});
                }
                if (!slots.isEmpty()) {
                    shards.put(fields[0], new Shard(ports.indexOf(port), slots, new ArrayList<>()));
                }
            } else {
                replicas.add(new String[]{fields[3], String.valueOf(ports.indexOf(port))});
            }
        }
        for (String[] replica : replicas) {
            Shard shard = shards.get(replica[0]);
            if (shard != null) {
                shard.replicas().add(Integer.parseInt(replica[1]));
            }
        }
        return shards;
    }

    // Dragonfly's layout as JSON: per shard its slot ranges, its primary and replica, and a slot migrating to another
    // shard where one is
    private void pushDragonfly(int migrating, int toShard) {
        StringBuilder layout = new StringBuilder("[");
        for (int shard = 0; shard < 3; shard++) {
            if (shard > 0) {
                layout.append(',');
            }
            List<String> ranges = new ArrayList<>();
            for (int slot = 0; slot < SlotHash.SLOT_COUNT; slot++) {
                if (slotShard[slot] == shard && (slot == 0 || slotShard[slot - 1] != shard)) {
                    int last = slot;
                    while (last + 1 < SlotHash.SLOT_COUNT && slotShard[last + 1] == shard) {
                        last++;
                    }
                    ranges.add("{\"start\":" + slot + ",\"end\":" + last + "}");
                }
            }
            int primary = shardPrimary[shard];
            int replica = shardReplica[shard];
            layout.append("{\"slot_ranges\":[").append(String.join(",", ranges)).append("],\"master\":")
                    .append(dragonflyNode(primary)).append(",\"replicas\":[")
                    .append(replica >= 0 ? dragonflyNode(replica) : "").append(']');
            if (migrating >= 0 && slotShard[migrating] == shard) {
                int target = shardPrimary[toShard];
                layout.append(",\"migrations\":[{\"slot_ranges\":[{\"start\":").append(migrating)
                        .append(",\"end\":").append(migrating).append("}],\"node_id\":\"")
                        .append(dragonflyIds[target]).append("\",\"ip\":\"").append(ip(target))
                        .append("\",\"port\":").append(ports.get(target)).append('}').append(']');
            }
            layout.append('}');
        }
        layout.append(']');
        for (int node = 0; node < count; node++) {
            if (node != killed && node != locked) {
                // a replica still loading its resynchronisation answers LOADING for a moment: asked again
                int target = node;
                require(() -> {
                    command(target, "DFLYCLUSTER", "CONFIG", layout.toString());
                    return true;
                }, "node " + node + " taking the layout");
            }
        }
    }

    private String dragonflyNode(int node) {
        return "{\"id\":\"" + dragonflyIds[node] + "\",\"ip\":\"" + ip(node) + "\",\"port\":" + ports.get(node) + "}";
    }

    private record Shard(int primary, List<int[]> slots, List<Integer> replicas) {

        boolean owns(int slot) {
            return slots.stream().anyMatch(range -> range[0] <= slot && slot <= range[1]);
        }
    }

    // the shard owning the slot, as the first node answering sees it
    private Shard shard(int slot) {
        RuntimeException failure = null;
        for (int node = 0; node < count; node++) {
            try {
                for (Shard shard : shards(node).values()) {
                    if (shard.owns(slot)) {
                        return shard;
                    }
                }
            } catch (RuntimeException e) {
                failure = e;
            }
        }
        throw new IllegalStateException("no node names an owner of slot " + slot, failure);
    }

    private int owner(int slot) {
        return shard(slot).primary();
    }

    @Override
    public int primary(String key) {
        return owner(SlotHash.getSlot(key));
    }

    @Override
    public void awaitInSync(String key) {
        awaitInSync(SlotHash.getSlot(key));
    }

    private void awaitInSync(int slot) {
        require(() -> {
            Shard shard = shard(slot);
            if (shard.replicas().isEmpty()) {
                return false;
            }
            Map<String, String> leading = fields(shard.primary(), "INFO replication");
            Map<String, String> following = fields(shard.replicas().get(0), "INFO replication");
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
        }, "the replica of slot " + slot + " in sync with its primary");
    }

    // a manual failover (CLUSTER FAILOVER on the replica), which waits for the replica to catch up with its primary
    // and then swaps their roles: nothing replicated is lost
    @Override
    public void failover(String key) {
        int slot = SlotHash.getSlot(key);
        awaitInSync(slot);
        Shard before = shard(slot);
        int replica = before.replicas().get(0);
        if (kind == Kind.DRAGONFLY_REPLICATED) {
            // Dragonfly's planned failover: the replica takes over (REPLTAKEOVER, which locks the old primary once
            // it handed over everything), the roles swapped in a new layout, and the old primary told to follow
            command(replica, "REPLTAKEOVER", "10");
            int shard = slotShard[slot];
            shardPrimary[shard] = replica;
            shardReplica[shard] = before.primary();
            locked = before.primary();
            push();
            // through redis-cli in its container, which sends no handshake the locked node would refuse
            exec(before.primary(), List.of("redis-cli", "-p", String.valueOf(ports.get(before.primary())),
                    "REPLICAOF", ip(replica), String.valueOf(ports.get(replica))));
            locked = -1;
            require(() -> "slave".equals(fields(before.primary(), "INFO replication").get("role")),
                    "node " + before.primary() + " following node " + replica);
            push();
            awaitInSync(slot);
            require(() -> {
                Shard after = shard(slot);
                return after.primary() == replica && after.replicas().contains(before.primary());
            }, "a failover of slot " + slot + " from node " + before.primary() + " to node " + replica);
            awaitInSync(slot);
            return;
        }
        if (kind == Kind.KVROCKS) {
            // the controller's planned failover: the roles swapped in a new layout, once the replica caught up
            int shard = slotShard[slot];
            shardPrimary[shard] = replica;
            shardReplica[shard] = before.primary();
            push();
            require(() -> {
                Shard after = shard(slot);
                return after.primary() == replica && after.replicas().contains(before.primary());
            }, "a failover of slot " + slot + " from node " + before.primary() + " to node " + replica);
            awaitInSync(slot);
            return;
        }
        // a manual failover gives up silently when the replica does not catch up within its own timeout (found under
        // the load of the full matrix): asked again every few seconds until the roles swapped
        require(() -> {
            try (StatefulRedisConnection<String, String> connection = direct(replica)) {
                connection.sync().clusterFailover(false);
            }
            return awaitUpTo(() -> {
                Shard after = shard(slot);
                return after.primary() == replica && after.replicas().contains(before.primary());
            }, Duration.ofSeconds(5), Duration.ofMillis(200));
        }, "a failover of slot " + slot + " from node " + before.primary() + " to node " + replica);
        require(() -> fields(replica, "CLUSTER INFO").get("cluster_state").equals("ok"), "the cluster healthy again");
    }

    // moves the key's slot, with every key in it, from its primary to another, as resharding does: the target
    // importing, the source migrating (a key already moved answered with ASK, a script over keys on both sides with
    // TRYAGAIN), the keys migrated in batches, then the slot assigned to the target on every primary (MOVED)
    void move(String key) {
        int slot = SlotHash.getSlot(key);
        Shard from = shard(slot);
        Shard to = shards(from.primary()).values().stream().filter(shard -> shard.primary() != from.primary())
                .findFirst().orElseThrow();
        if (kind == Kind.KVROCKS) {
            moveKvrocks(slot, from, to);
            return;
        }
        if (kind == Kind.DRAGONFLY_REPLICATED) {
            // Dragonfly migrates a slot named in the layout's migrations itself; once its source reports the
            // migration finished, the final layout assigns the slot to the target
            int toShard = slotShard[to.slots().get(0)[0]];
            push(slot, toShard);
            require(() -> String.valueOf(command(from.primary(), "DFLYCLUSTER", "SLOT-MIGRATION-STATUS"))
                    .contains("FINISHED"), "slot " + slot + " migrated from node " + from.primary());
            slotShard[slot] = toShard;
            push();
            require(() -> shard(slot).primary() == to.primary(), "node " + to.primary() + " the owner of slot "
                    + slot);
            return;
        }
        try (StatefulRedisConnection<String, String> source = direct(from.primary());
             StatefulRedisConnection<String, String> target = direct(to.primary())) {
            String fromId = source.sync().clusterMyId();
            String toId = target.sync().clusterMyId();
            target.sync().clusterSetSlotImporting(slot, fromId);
            source.sync().clusterSetSlotMigrating(slot, toId);
            for (List<String> keys = source.sync().clusterGetKeysInSlot(slot, 100); !keys.isEmpty();
                 keys = source.sync().clusterGetKeysInSlot(slot, 100)) {
                source.sync().migrate(ip(to.primary()), ports.get(to.primary()), 0, 5_000,
                        MigrateArgs.Builder.keys(keys));
            }
            target.sync().clusterSetSlotNode(slot, toId);
            source.sync().clusterSetSlotNode(slot, toId);
            for (Shard other : shards(from.primary()).values()) {
                if (other.primary() != from.primary() && other.primary() != to.primary()) {
                    try (StatefulRedisConnection<String, String> connection = direct(other.primary())) {
                        connection.sync().clusterSetSlotNode(slot, toId);
                    }
                }
            }
        }
        require(() -> {
            for (int node = 0; node < count; node++) {
                Shard owner = null;
                for (Shard shard : shards(node).values()) {
                    if (shard.owns(slot)) {
                        owner = shard;
                    }
                }
                if (owner == null || owner.primary() != to.primary()) {
                    return false;
                }
            }
            return true;
        }, "every node naming node " + to.primary() + " the owner of slot " + slot);
    }

    // Kvrocks moves a slot's keys itself (CLUSTERX MIGRATE, on the source), and the controller then assigns the slot
    // to the target on every node (CLUSTERX SETSLOT … NODE, a new version)
    private void moveKvrocks(int slot, Shard from, Shard to) {
        clusterx(from.primary(), "MIGRATE", String.valueOf(slot), nodeId(to.primary()));
        require(() -> String.valueOf(fields(from.primary(), "CLUSTER INFO").get("migrating_state")).equals("success"),
                "slot " + slot + " migrated from node " + from.primary());
        slotShard[slot] = slotShard[to.slots().get(0)[0]];
        layoutVersion++;
        for (int node = 0; node < count; node++) {
            if (node != killed) {
                clusterx(node, "SETSLOT", String.valueOf(slot), "NODE", nodeId(to.primary()),
                        String.valueOf(layoutVersion));
            }
        }
        require(() -> shard(slot).primary() == to.primary(), "node " + to.primary() + " the owner of slot " + slot);
    }

    // the replica's own failover once the primary stays silent for the node timeout (1 s), the other primaries voting;
    // on Kvrocks the controller's, which promotes the replica in a new layout
    @Override
    public void kill(String key) {
        int slot = SlotHash.getSlot(key);
        awaitInSync(slot);
        Shard before = shard(slot);
        int replica = before.replicas().get(0);
        GenericContainer<?> node = nodes.get(before.primary());
        try (KillContainerCmd kill = node.getDockerClient().killContainerCmd(node.getContainerId())) {
            kill.exec();
        }
        killed = before.primary();
        if (kind == Kind.DRAGONFLY_REPLICATED) {
            // the operator's failover on a primary gone: its replica promoted (REPLICAOF NO ONE), a new layout
            follow(replica, -1);
        }
        if (kind == Kind.KVROCKS || kind == Kind.DRAGONFLY_REPLICATED) {
            int shard = slotShard[slot];
            shardPrimary[shard] = replica;
            shardReplica[shard] = -1;
            push();
        }
        require(() -> shard(slot).primary() == replica, "node " + replica + " promoted for slot " + slot);
    }

    // the node rejoins by the cluster configuration it kept (nodes.conf), as the replica of its old shard's new primary
    @Override
    public void revive() {
        if (killed < 0) {
            return;
        }
        GenericContainer<?> node = nodes.get(killed);
        try (StartContainerCmd start = node.getDockerClient().startContainerCmd(node.getContainerId())) {
            start.exec();
        }
        int revived = killed;
        killed = -1;
        if (kind == Kind.KVROCKS || kind == Kind.DRAGONFLY_REPLICATED) {
            // the controller adds it back as the replica of its shard's new primary, once it answers (Dragonfly's
            // told to follow it, Kvrocks's by the layout)
            require(() -> fields(revived, "INFO replication").get("role") != null, "node " + revived + " answering");
            for (int shard = 0; shard < 3; shard++) {
                if (shardReplica[shard] < 0) {
                    shardReplica[shard] = revived;
                    if (kind == Kind.DRAGONFLY_REPLICATED) {
                        follow(revived, shardPrimary[shard]);
                    }
                }
            }
            push();
        }
        require(() -> {
            for (Shard shard : shards(revived).values()) {
                if (shard.replicas().contains(revived)) {
                    awaitInSync(shard.slots().get(0)[0]);
                    return true;
                }
            }
            return false;
        }, "node " + revived + " back as a replica");
        require(() -> "ok".equals(fields(revived, "CLUSTER INFO").get("cluster_state")), "the cluster healthy again");
    }

    @Override
    public String describe(String key) {
        StringBuilder described = new StringBuilder();
        try {
            Shard shard = shard(SlotHash.getSlot(key));
            List<Integer> members = new ArrayList<>(List.of(shard.primary()));
            members.addAll(shard.replicas());
            for (int node : members) {
                Map<String, String> replication = fields(node, "INFO replication");
                described.append("node ").append(node).append(": ").append(replication.get("role"))
                        .append(", link ").append(replication.getOrDefault("master_link_status", "-"))
                        .append(", offset ").append(replication.get("master_repl_offset"))
                        .append(", ").append(key).append(' ').append(hash(node, key)).append("; ");
            }
        } catch (RuntimeException e) {
            described.append(e.getMessage()).append("; ");
        }
        return described.toString();
    }

    // a node's copy of a hash, read even where it is a replica (READONLY)
    private Map<String, String> hash(int node, String key) {
        try (StatefulRedisConnection<String, String> connection = direct(node)) {
            connection.sync().readOnly();
            return connection.sync().hgetall(key);
        }
    }

    // the fields of INFO replication or CLUSTER INFO of one node, empty where it does not answer
    private Map<String, String> fields(int node, String command) {
        try (StatefulRedisConnection<String, String> connection = direct(node)) {
            String text = command.equals("CLUSTER INFO") ? connection.sync().clusterInfo()
                    : connection.sync().info("replication");
            Map<String, String> fields = new LinkedHashMap<>();
            text.lines().filter(line -> line.contains(":")).forEach(line ->
                    fields.put(line.substring(0, line.indexOf(':')), line.substring(line.indexOf(':') + 1).trim()));
            return fields;
        } catch (RuntimeException e) {
            return Map.of();
        }
    }

    private void require(BooleanSupplier condition, String what) {
        if (!awaitUpTo(() -> {
            try {
                return condition.getAsBoolean();
            } catch (RuntimeException e) {
                return false;
            }
        }, SETTLING, Duration.ofMillis(200))) {
            throw new IllegalStateException("not within " + SETTLING + ": " + what + "; " + state());
        }
    }

    // each node's role, link and log's end as far as it answers, for a failure's message
    private String state() {
        StringBuilder state = new StringBuilder();
        for (int node = 0; node < count; node++) {
            Map<String, String> replication = fields(node, "INFO replication");
            state.append("node ").append(node).append(node == killed ? " (killed)" : "").append(": ")
                    .append(replication.getOrDefault("role", "unreachable")).append(", link ")
                    .append(replication.getOrDefault("master_link_status", "-")).append(", ")
                    .append(replication.getOrDefault("slave0", "")).append("; ");
        }
        for (int node = 0; node < count; node++) {
            if (node != killed) {
                String log = nodes.get(node).getLogs();
                state.append("node ").append(node).append("'s log: ")
                        .append(log.substring(Math.max(0, log.length() - 800))).append("; ");
            }
        }
        return state.toString();
    }

    @Override
    public void close() {
        redisson.shutdown();
        jedis.close();
        admin.close();
        client.shutdown();
        direct.shutdown();
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
