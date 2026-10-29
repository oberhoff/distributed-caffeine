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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spi.dns.InetAddressResolver;
import io.github.oberhoff.distributedcaffeine.testing.Ports;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static java.util.stream.Collectors.joining;

/**
 * A replica set of three members (test commands enabled for fail points), the last one an arbiter if asked for, on a
 * network of their own, each published on a free host port as the same port and announced as {@code memberN:port},
 * which the driver resolves to localhost; member 0 is the preferred primary. Members can be stopped, started again on their data, and
 * killed; the primary can be stepped down and handed back. Elections are shortened (2 s), so that a failover takes
 * seconds rather than the default's ten or more.
 */
@NullMarked
final class MongoReplicaSet implements AutoCloseable {

    private static final Duration HEALTHY = Duration.ofMinutes(2);
    private static final InetAddressResolver MEMBER_RESOLVER = host -> host.matches("member\\d+")
            ? List.of(InetAddress.getByAddress(host, new byte[]{127, 0, 0, 1}))
            : List.of(InetAddress.getAllByName(host));

    private final Network network = Network.newNetwork();
    private final List<GenericContainer<?>> members = new ArrayList<>();
    private final List<Integer> ports;
    private final DockerClient docker = DockerClientFactory.instance().client();

    MongoReplicaSet(String image, boolean withArbiter) {
        ports = Ports.startOnFree(3, chosen -> start(image, chosen));
        List<Document> configurations = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
            boolean arbiter = withArbiter && index == 2;
            configurations.add(new Document("_id", index).append("host", "member" + index + ":" + ports.get(index))
                    .append("priority", arbiter ? 0 : index == 0 ? 2 : 1)
                    .append("arbiterOnly", arbiter));
        }
        try (MongoClient direct = direct(0)) {
            direct.getDatabase("admin").runCommand(new Document("replSetInitiate", new Document("_id", "rs0")
                    .append("members", configurations)
                    .append("settings", new Document("electionTimeoutMillis", 2_000)
                            .append("heartbeatIntervalMillis", 500)
                            .append("catchUpTimeoutMillis", 2_000))));
        }
        awaitHealthy();
    }

    // the members on the given ports; on failure, what was started is stopped again
    @SuppressWarnings("resource") // the members are stopped with the replica set
    private void start(String image, List<Integer> chosen) {
        members.clear();
        for (int index = 0; index < 3; index++) {
            int port = chosen.get(index);
            members.add(new GenericContainer<>(image)
                    .withNetwork(network)
                    .withNetworkAliases("member" + index)
                    .withCommand("--replSet", "rs0", "--bind_ip_all", "--port", String.valueOf(port),
                            "--setParameter", "enableTestCommands=1")
                    .withCreateContainerCmdModifier(cmd -> cmd
                            .withExposedPorts(ExposedPort.tcp(port))
                            .withHostConfig(Objects.requireNonNull(cmd.getHostConfig()).withPortBindings(
                                    new PortBinding(com.github.dockerjava.api.model.Ports.Binding.bindPort(port), ExposedPort.tcp(port)))))
                    .waitingFor(Wait.forLogMessage("(?is).*waiting for connections.*", 1)));
        }
        try {
            members.parallelStream().forEach(GenericContainer::start);
        } catch (RuntimeException e) {
            members.forEach(GenericContainer::stop);
            throw e;
        }
    }

    /**
     * Returns the host port of the preferred member.
     *
     * @return the port
     */
    int preferredPort() {
        return ports.get(0);
    }

    /**
     * Returns client settings for the whole replica set, resolving the members' names.
     *
     * @return the settings
     */
    MongoClientSettings.Builder settings() {
        String hosts = ports.stream().map(port -> "member" + ports.indexOf(port) + ":" + port).collect(joining(","));
        return MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://" + hosts + "/?replicaSet=rs0"))
                .inetAddressResolver(MEMBER_RESOLVER);
    }

    // a client connected to one member only, whichever role it has
    private MongoClient direct(int index) {
        return MongoClients.create("mongodb://localhost:" + ports.get(index)
                + "/?directConnection=true&serverSelectionTimeoutMS=2000&connectTimeoutMS=2000");
    }

    /**
     * Stops a member as gracefully as a rolling update does.
     *
     * @param index the member
     */
    void stop(int index) {
        docker.stopContainerCmd(members.get(index).getContainerId()).exec();
    }

    /**
     * Kills the preferred member, so that it hands nothing over.
     */
    void killPreferred() {
        docker.killContainerCmd(members.get(0).getContainerId()).exec();
    }

    /**
     * Freezes the preferred member (its processes paused): it answers nobody, as if cut off by the network, and goes
     * on where it was once thawed.
     */
    void pausePreferred() {
        docker.pauseContainerCmd(members.get(0).getContainerId()).exec();
    }

    /**
     * Thaws the preferred member; if it is not frozen, it is left as it is.
     */
    void unpausePreferred() {
        String id = members.get(0).getContainerId();
        if (Boolean.TRUE.equals(docker.inspectContainerCmd(id).exec().getState().getPaused())) {
            docker.unpauseContainerCmd(id).exec();
        }
    }

    /**
     * Stops a secondary from fetching the primary's oplog (a fail point, as MongoDB's own rollback tests use), or lets
     * it go on: what the primary writes meanwhile is on the primary alone.
     *
     * @param index    the member
     * @param stopped whether it stops fetching
     */
    void stopReplication(int index, boolean stopped) {
        try (MongoClient direct = direct(index)) {
            direct.getDatabase("admin").runCommand(new Document("configureFailPoint", "stopReplProducer")
                    .append("mode", stopped ? "alwaysOn" : "off"));
        }
    }

    /**
     * Returns what the preferred member logged so far.
     *
     * @return the log
     */
    String preferredLogs() {
        return members.get(0).getLogs();
    }

    /**
     * Starts a stopped or killed member again on the data it had.
     *
     * @param index the member
     */
    void start(int index) {
        docker.startContainerCmd(members.get(index).getContainerId()).exec();
    }

    /**
     * Returns the index of the member that is primary, as a member that answers sees it, or -1.
     *
     * @return the primary
     */
    int primary() {
        for (int index = 0; index < members.size(); index++) {
            try (MongoClient direct = direct(index)) {
                for (Document status : status(direct.getDatabase("admin"))) {
                    if (status.getInteger("state") == 1) {
                        return status.getInteger("_id");
                    }
                }
            } catch (MongoException e) {
                // not answering; ask the next one
            }
        }
        return -1;
    }

    /**
     * Has the preferred member step down and waits until another one is primary.
     */
    void stepDown() {
        try (MongoClient preferred = direct(0)) {
            try {
                preferred.getDatabase("admin").runCommand(new Document("replSetStepDown", 5)
                        .append("secondaryCatchUpPeriodSecs", 2));
            } catch (MongoException e) {
                // the stepping down may close the connection the command came on, which is the point
            }
        }
        awaitFailover();
    }

    /**
     * Waits until a member other than the preferred one is primary.
     */
    void awaitFailover() {
        require(() -> primary() > 0, "another member primary");
    }

    /**
     * Hands the primary back to the preferred member on purpose rather than leaving it to its priority, which takes
     * anything from seconds to a minute: unfreezing ends any step-down period, stepping up calls the election.
     */
    void handBack() {
        require(() -> {
            try (MongoClient preferred = direct(0)) {
                MongoDatabase admin = preferred.getDatabase("admin");
                admin.runCommand(new Document("replSetFreeze", 0));
                admin.runCommand(new Document("replSetStepUp", 1));
            } catch (MongoException e) {
                return false;
            }
            return primary() == 0;
        }, "the preferred member primary again");
        awaitHealthy();
    }

    /**
     * Waits until the preferred member is primary and every other one is a secondary; until then a majority write
     * waits for a member that is still starting up or catching up.
     */
    void awaitHealthy() {
        require(() -> {
            try (MongoClient preferred = direct(0)) {
                return status(preferred.getDatabase("admin")).stream().allMatch(status ->
                        status.getInteger("_id") == 0 ? status.getInteger("state") == 1
                                : Set.of(2, 7).contains(status.getInteger("state")));
            } catch (MongoException e) {
                return false;
            }
        }, "replica set healthy");
    }

    private static List<Document> status(MongoDatabase admin) {
        return admin.runCommand(new Document("replSetGetStatus", 1)).getList("members", Document.class);
    }

    private static void require(BooleanSupplier condition, String what) {
        if (!awaitUpTo(condition, HEALTHY, Duration.ofMillis(250))) {
            throw new AssertionError(what + " not reached within " + HEALTHY.toSeconds() + " s");
        }
    }

    @Override
    public void close() {
        members.forEach(GenericContainer::stop);
        network.close();
    }
}
