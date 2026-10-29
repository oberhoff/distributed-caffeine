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
package io.github.oberhoff.distributedcaffeine.mongodb;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static java.util.Objects.requireNonNull;

/**
 * The MongoDB store of the topology running now, started by its test class before its suites and stopped after them
 * ({@link MongoTopologyTests}): one at a time, so the suites find it with {@link #get()}. Built per topology:
 * <ul>
 *     <li>single-node replica set: {@code MongoDBContainer} with a replica set of itself;</li>
 *     <li>three-node replica set, with or without an arbiter: {@link MongoReplicaSet};</li>
 *     <li>sharded cluster: a config server and two shards, each a single-member replica set, and a router, on a
 *     network of their own; only the router is reached from here. The shards write a no-op every second, since a
 *     change stream through the router delivers an event only once every shard moved past it, and an idle shard
 *     moves on only by its periodic no-op (every 10 s by default);</li>
 *     <li>single-node replica set with a small oplog: 1 MB, truncated as soon as MongoDB can (a checkpoint every
 *     second, and from MongoDB 5.0 no snapshot history kept).</li>
 * </ul>
 */
@NullMarked
final class MongoTestDatabase implements AutoCloseable {

    private static final AtomicInteger COLLECTIONS = new AtomicInteger();
    private static final Duration READY = Duration.ofMinutes(2);
    private static volatile @Nullable MongoTestDatabase current;

    final MongoTopology topology;
    final MongoClient client;
    private final List<GenericContainer<?>> containers = new ArrayList<>();
    private final @Nullable Network network;
    private final @Nullable MongoReplicaSet replicaSet;
    private final String host;
    private final int port;

    @SuppressWarnings("resource") // the containers are stopped with the store
    private MongoTestDatabase(String image, MongoTopology topology) {
        this.topology = topology;
        switch (topology) {
            case SINGLE_NODE_REPLICA_SET -> {
                MongoDBContainer container = new MongoDBContainer(image).withReplicaSet();
                containers.add(container);
                container.start();
                network = null;
                replicaSet = null;
                host = container.getHost();
                port = container.getMappedPort(27017);
                client = MongoClients.create(container.getReplicaSetUrl());
            }
            case THREE_NODE_REPLICA_SET, THREE_NODE_REPLICA_SET_WITH_ARBITER -> {
                network = null;
                replicaSet = new MongoReplicaSet(image, topology == MongoTopology.THREE_NODE_REPLICA_SET_WITH_ARBITER);
                host = "localhost";
                port = replicaSet.preferredPort();
                client = MongoClients.create(replicaSet.settings().build());
            }
            case SHARDED_CLUSTER -> {
                network = Network.newNetwork();
                replicaSet = null;
                GenericContainer<?> router = startShardedCluster(image, network);
                host = router.getHost();
                port = router.getMappedPort(27017);
                client = MongoClients.create("mongodb://" + host + ":" + port);
                for (int index = 0; index < 2; index++) {
                    Document shard = new Document("addShard", "shard" + index + "/shard" + index + ":27018");
                    require(() -> {
                        try {
                            client.getDatabase("admin").runCommand(shard);
                            return true;
                        } catch (MongoException e) {
                            return false;
                        }
                    }, "shard " + index + " added");
                }
            }
            case SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG -> {
                network = null;
                replicaSet = null;
                List<String> command = new ArrayList<>(List.of("--replSet", "rs0", "--oplogSize", "1",
                        "--syncdelay", "1", "--bind_ip_all"));
                if (major(image) >= 5) {
                    command.addAll(List.of("--setParameter", "minSnapshotHistoryWindowInSeconds=0"));
                }
                GenericContainer<?> container = new GenericContainer<>(image)
                        .withCommand(command.toArray(String[]::new))
                        .withExposedPorts(27017)
                        .waitingFor(Wait.forLogMessage("(?is).*waiting for connections.*", 1));
                containers.add(container);
                container.start();
                host = container.getHost();
                port = container.getMappedPort(27017);
                client = MongoClients.create("mongodb://" + host + ":" + port + "/?directConnection=true");
                initiate(client.getDatabase("admin"), new Document("_id", "rs0")
                        .append("members", List.of(new Document("_id", 0).append("host", "localhost:27017"))));
            }
            default -> throw new IllegalStateException(topology + " is no MongoDB topology");
        }
    }

    /**
     * Starts the store of a topology; the suites find it with {@link #get()} until it is closed.
     *
     * @param image    the image
     * @param topology the topology
     * @return the store
     */
    static MongoTestDatabase start(String image, MongoTopology topology) {
        if (current != null) {
            throw new IllegalStateException("another MongoDB topology is running: one at a time");
        }
        MongoTestDatabase started = new MongoTestDatabase(image, topology);
        current = started;
        return started;
    }

    /**
     * Returns the store of the topology running now.
     *
     * @return the store
     */
    static MongoTestDatabase get() {
        return requireNonNull(current, "no MongoDB topology is running: the MongoDB suites run nested in "
                + "MongoIntegrationTest");
    }

    static String freshCollection() {
        return "cache_" + COLLECTIONS.incrementAndGet();
    }

    /**
     * Returns the host of the server the driver reaches first: the single node, the preferred member, the router.
     *
     * @return the host
     */
    String host() {
        return host;
    }

    /**
     * Returns the port of {@link #host()} on it.
     *
     * @return the port
     */
    int port() {
        return port;
    }

    /**
     * Returns the replica set of the three-node topologies.
     *
     * @return the replica set
     */
    MongoReplicaSet replicaSet() {
        return requireNonNull(replicaSet, topology + " has no replica set of several members");
    }

    // a config server, two shards and a router; the router, started last
    private GenericContainer<?> startShardedCluster(String image, Network network) {
        GenericContainer<?> config = member(image, network, "config", 27019, "--configsvr", "--replSet", "config",
                "--bind_ip_all", "--port", "27019");
        List<GenericContainer<?>> shards = new ArrayList<>();
        for (int index = 0; index < 2; index++) {
            shards.add(member(image, network, "shard" + index, 27018, "--shardsvr", "--replSet", "shard" + index,
                    "--bind_ip_all", "--port", "27018", "--setParameter", "periodicNoopIntervalSecs=1"));
        }
        GenericContainer<?> router = member(image, network, "router", 27017, "mongos", "--configdb",
                "config/config:27019", "--bind_ip_all", "--port", "27017");
        config.start();
        shards.parallelStream().forEach(GenericContainer::start);
        initiateMember(config, "config", "config:27019", true);
        for (int index = 0; index < shards.size(); index++) {
            initiateMember(shards.get(index), "shard" + index, "shard" + index + ":27018", false);
        }
        router.start();
        return router;
    }

    @SuppressWarnings("resource") // stopped with the store
    private GenericContainer<?> member(String image, Network network, String alias, int port, String... command) {
        GenericContainer<?> member = new GenericContainer<>(image)
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withExposedPorts(port)
                .withCommand(command)
                .waitingFor(Wait.forLogMessage("(?is).*waiting for connections.*", 1));
        containers.add(member);
        return member;
    }

    private static void initiateMember(GenericContainer<?> member, String name, String announced,
                                       boolean configServer) {
        int exposed = member.getExposedPorts().get(0);
        try (MongoClient direct = MongoClients.create("mongodb://" + member.getHost() + ":"
                + member.getMappedPort(exposed) + "/?directConnection=true")) {
            Document configuration = new Document("_id", name)
                    .append("members", List.of(new Document("_id", 0).append("host", announced)));
            if (configServer) {
                configuration.append("configsvr", true);
            }
            initiate(direct.getDatabase("admin"), configuration);
        }
    }

    // initiates a replica set of one member and waits until it is primary
    private static void initiate(MongoDatabase admin, Document configuration) {
        admin.runCommand(new Document("replSetInitiate", configuration));
        require(() -> isPrimary(admin), configuration.getString("_id") + " primary");
    }

    // hello from MongoDB 4.4.2 (and 5.0) on, isMaster before
    private static boolean isPrimary(MongoDatabase admin) {
        try {
            return admin.runCommand(new Document("hello", 1)).getBoolean("isWritablePrimary", false);
        } catch (MongoException e) {
            try {
                return admin.runCommand(new Document("isMaster", 1)).getBoolean("ismaster", false);
            } catch (MongoException again) {
                return false;
            }
        }
    }

    private static void require(java.util.function.BooleanSupplier condition, String what) {
        if (!awaitUpTo(condition, READY, Duration.ofMillis(250))) {
            throw new AssertionError(what + " not reached within " + READY.toSeconds() + " s");
        }
    }

    // the major version of an image tag such as mongo:4.4.0 or mongo:9
    static int major(String image) {
        String tag = image.substring(image.indexOf(':') + 1);
        int dot = tag.indexOf('.');
        return Integer.parseInt(dot < 0 ? tag : tag.substring(0, dot));
    }

    @Override
    public void close() {
        current = null;
        try {
            client.close();
        } finally {
            if (replicaSet != null) {
                replicaSet.close();
            }
            containers.forEach(GenericContainer::stop);
            if (network != null) {
                network.close();
            }
        }
    }
}
