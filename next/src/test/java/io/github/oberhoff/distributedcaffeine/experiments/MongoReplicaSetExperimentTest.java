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
package io.github.oberhoff.distributedcaffeine.experiments;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spi.dns.InetAddressResolver;
import io.github.oberhoff.distributedcaffeine.testing.Awaiting;
import org.bson.Document;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.joining;

/**
 * The write concern measurement of {@link MongoWriteConcernExperimentTest}, on a 3-member replica set: three containers
 * on a network of their own, each published on a free host port as the same port and announced as
 * {@code memberN:port}, which the driver resolves to localhost. All members on one Docker host, so replication costs
 * no network time; what remains is the journal and the members' coordination.
 * <p>
 * Not run by default, only with {@code -Dexperiments=true}; run with
 * {@code mvn test -Dtest=MongoReplicaSetExperimentTest -Dexperiments=true}.
 */
@EnabledIfSystemProperty(named = "experiments", matches = "true")
// a measurement: it prints what it measures rather than asserting it, and paces itself with pauses
@SuppressWarnings({"java:S2699", "java:S2925"})
class MongoReplicaSetExperimentTest {

    private static final InetAddressResolver MEMBER_RESOLVER = host -> host.matches("member\\d+")
            ? List.of(InetAddress.getByAddress(host, new byte[]{127, 0, 0, 1}))
            : List.of(InetAddress.getAllByName(host));

    @ParameterizedTest
    @ValueSource(strings = {"mongo:5", "mongo:8", "mongo:9"})
    @SuppressWarnings("resource") // the members are stopped on close, before the network
    void measure(String image) throws Exception {
        List<GenericContainer<?>> members = new ArrayList<>();
        try (Network network = Network.newNetwork();
             AutoCloseable ignored = () -> members.forEach(GenericContainer::stop)) {
            List<Integer> ports = freePorts();
            List<Document> configurations = new ArrayList<>();
            for (int index = 0; index < 3; index++) {
                int port = ports.get(index);
                String alias = "member" + index;
                GenericContainer<?> member = new GenericContainer<>(image)
                        .withNetwork(network)
                        .withNetworkAliases(alias)
                        .withCommand("--replSet", "rs0", "--bind_ip_all", "--port", String.valueOf(port))
                        .withCreateContainerCmdModifier(cmd -> cmd
                                .withExposedPorts(ExposedPort.tcp(port))
                                .withHostConfig(Objects.requireNonNull(cmd.getHostConfig()).withPortBindings(
                                        new PortBinding(Ports.Binding.bindPort(port), ExposedPort.tcp(port)))))
                        .waitingFor(Wait.forLogMessage("(?is).*waiting for connections.*", 1));
                members.add(member);
                configurations.add(new Document("_id", index).append("host", alias + ":" + port)
                        .append("priority", index == 0 ? 2 : 1));
            }
            members.parallelStream().forEach(GenericContainer::start);

            String directUrl = "mongodb://localhost:" + ports.get(0) + "/?directConnection=true";
            try (MongoClient direct = MongoClients.create(directUrl)) {
                direct.getDatabase("admin").runCommand(new Document("replSetInitiate",
                        new Document("_id", "rs0").append("members", configurations)));
                awaitHealthy(direct.getDatabase("admin"));
            }

            String url = "mongodb://" + configurations.stream().map(c -> c.getString("host")).collect(joining(","))
                    + "/experiment?replicaSet=rs0";
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(url))
                    .inetAddressResolver(MEMBER_RESOLVER)
                    .build();
            try (MongoClient writerClient = MongoClients.create(settings);
                 MongoClient watcherClient = MongoClients.create(settings)) {
                MongoDatabase admin = writerClient.getDatabase("admin");
                StringBuilder out = new StringBuilder("\n=== write concern, 3 members, ").append(image).append(" (")
                        .append(admin.runCommand(new Document("buildInfo", 1)).getString("version")).append(") ===\n")
                        .append(String.format("%-40s %12s %12s %12s %12s %12s%n", "variant",
                                "ack p50 µs", "ack p99 µs", "event p50 µs", "event p99 µs", "ev-ack p50"));
                for (boolean journalDefault : new boolean[]{true, false, true, false}) {
                    MongoWriteConcernExperimentTest.setJournalDefault(admin, journalDefault);
                    for (WriteConcern writeConcern : List.of(WriteConcern.MAJORITY, WriteConcern.W1)) {
                        String variant = (writeConcern.equals(WriteConcern.MAJORITY) ? "majority" : "w:1")
                                + ", writeConcernMajorityJournalDefault=" + journalDefault;
                        out.append(MongoWriteConcernExperimentTest.run(variant, writerClient, watcherClient,
                                writeConcern));
                    }
                }
                System.out.println(out);
            }
        }
    }

    private static void awaitHealthy(MongoDatabase admin) {
        if (!Awaiting.awaitUpTo(() -> isHealthy(admin), Duration.ofMinutes(2), Duration.ofMillis(500))) {
            throw new IllegalStateException("replica set not healthy");
        }
    }

    private static boolean isHealthy(MongoDatabase admin) {
        try {
            List<Document> states = admin.runCommand(new Document("replSetGetStatus", 1))
                    .getList("members", Document.class);
            return "PRIMARY".equals(states.get(0).getString("stateStr"))
                    && states.stream().skip(1).allMatch(m -> "SECONDARY".equals(m.getString("stateStr")));
        } catch (RuntimeException e) {
            return false; // not initiated yet
        }
    }

    // one per member
    private static List<Integer> freePorts() throws Exception {
        try (ServerSocket first = new ServerSocket(0);
             ServerSocket second = new ServerSocket(0);
             ServerSocket third = new ServerSocket(0)) {
            return List.of(first.getLocalPort(), second.getLocalPort(), third.getLocalPort());
        }
    }
}
