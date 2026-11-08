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
package io.github.oberhoff.distributedcaffeine.adapter.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoTopologyTests.Setup;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoTopology.SHARDED_CLUSTER;
import static io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoTopology.SINGLE_NODE_REPLICA_SET;
import static io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoTopology.SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG;
import static io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoTopology.STANDALONE_SERVER;
import static io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoTopology.THREE_NODE_REPLICA_SET;
import static io.github.oberhoff.distributedcaffeine.adapter.mongodb.MongoTopology.THREE_NODE_REPLICA_SET_WITH_ARBITER;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Topology.Tests.RUNS_ON_GITHUB;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every MongoDB image and topology the suites run on, one at a time: from 4.4, the oldest supported, to the latest,
 * each major version's first release and its latest. The sharded cluster and the small oplog on the latest of each
 * major version (and 4.4.0). A standalone server on 4.4.0 (asked with {@code isMaster}) and the latest (asked with
 * {@code hello}). With {@code -DrunsOnGitHub=true}, only the latest image runs, on its single-server topologies
 * (single node, small oplog, standalone): the multi-node ones carry the long failover and sharding suites, which run
 * in the full matrix.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
class MongoIntegrationTest {

    @Nested
    @DisplayName("MongoDB 4.2 (refused: older than 4.4)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_2__Refused {

        // a plain server suffices: the version is checked before the deployment
        @Test
        void a_server_older_than_the_oldest_supported_is_refused_naming_it() {
            try (GenericContainer<?> server = new GenericContainer<>("mongo:4.2")) {
                server.withExposedPorts(27017).waitingFor(Wait.forLogMessage(".*[Ww]aiting for connections.*", 1));
                server.start();
                try (MongoClient client = MongoClients.create("mongodb://" + server.getHost() + ":"
                        + server.getMappedPort(27017))) {
                    DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(
                            MongoAdapter.newBuilder(client, "refused", "cache").build());
                    assertThatThrownBy(builder::build).isInstanceOf(StoreException.class)
                            .hasMessageContaining("older than the oldest supported, 4.4");
                }
            }
        }
    }

    @Nested
    @DisplayName("MongoDB 4 latest (refused: majority read concern disabled)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_latest__Majority_Read_Concern_Disabled_Refused {

        // only MongoDB 4.4 and older can switch it off
        @Test
        void a_replica_set_without_majority_reads_is_refused() throws Exception {
            try (GenericContainer<?> server = new GenericContainer<>("mongo:4")) {
                server.withExposedPorts(27017)
                        .withCommand("--replSet", "rs0", "--enableMajorityReadConcern", "false", "--bind_ip_all")
                        .waitingFor(Wait.forLogMessage(".*[Ww]aiting for connections.*", 1));
                server.start();
                initiate(server, "mongo", "");
                try (MongoClient client = MongoClients.create("mongodb://" + server.getHost() + ":"
                        + server.getMappedPort(27017) + "/?directConnection=true")) {
                    DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(
                            MongoAdapter.newBuilder(client, "refused", "cache").build());
                    assertThatThrownBy(builder::build).isInstanceOf(StoreException.class)
                            .hasMessageContaining("does not support majority read concern");
                }
            }
        }
    }

    @Nested
    @DisplayName("MongoDB 9 latest (refused: a user without readWrite)")
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_9_latest__Restricted_User_Refused {

        // a replica set with authentication needs a key file, written before the image's own entrypoint starts it
        @Test
        void a_user_without_the_role_the_cache_needs_is_told_which_one() throws Exception {
            try (GenericContainer<?> server = new GenericContainer<>("mongo:9")) {
                server.withExposedPorts(27017)
                        .withEnv("MONGO_INITDB_ROOT_USERNAME", "root").withEnv("MONGO_INITDB_ROOT_PASSWORD", "secret")
                        .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("bash"))
                        .withCommand("-c", "head -c 756 /dev/urandom | base64 > /tmp/key && chmod 400 /tmp/key "
                                + "&& chown mongodb:mongodb /tmp/key "
                                + "&& exec docker-entrypoint.sh mongod --replSet rs0 --keyFile /tmp/key --bind_ip_all")
                        .waitingFor(Wait.forLogMessage(".*[Ww]aiting for connections.*", 2));
                server.start();
                String root = "-u root -p secret";
                initiate(server, "mongosh", root);
                run(server, "mongosh " + root + " --quiet --eval 'db.getSiblingDB(\"app\").createUser("
                        + "{user: \"reader\", pwd: \"reader\", roles: [{role: \"read\", db: \"app\"}]})'");
                try (MongoClient client = MongoClients.create("mongodb://reader:reader@" + server.getHost() + ":"
                        + server.getMappedPort(27017) + "/app?authSource=app&directConnection=true")) {
                    DistributedCaffeine<String, String> builder = DistributedCaffeine.newBuilder(
                            MongoAdapter.newBuilder(client, "app", "cache").build());
                    assertThatThrownBy(builder::build).isInstanceOf(StoreException.class)
                            .hasMessageContaining("readWrite on database app");
                }
            }
        }
    }

    // a single-node replica set, initiated and waited for until it is primary
    private static void initiate(GenericContainer<?> server, String shell, String credentials) throws Exception {
        run(server, shell + " " + credentials + " --quiet --eval "
                + "'rs.initiate({_id: \"rs0\", members: [{_id: 0, host: \"localhost:27017\"}]})'");
        await(() -> isPrimary(server, shell, credentials));
    }

    private static boolean isPrimary(GenericContainer<?> server, String shell, String credentials) {
        try {
            return run(server, shell + " " + credentials + " --quiet --eval 'db.hello().isWritablePrimary'")
                    .contains("true");
        } catch (Exception e) {
            return false; // not answering yet
        }
    }

    private static String run(GenericContainer<?> server, String command) throws Exception {
        return server.execInContainer("bash", "-c", command).getStdout();
    }

    @Nested
    @DisplayName("MongoDB 4.4.0 (Single-Node Replica Set)")
    @Setup(image = "mongo:4.4.0", topology = SINGLE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_4_0__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 4.4.0 (Three-Node Replica Set)")
    @Setup(image = "mongo:4.4.0", topology = THREE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_4_0__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 4.4.0 (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:4.4.0", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_4_0__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 4.4.0 (Sharded Cluster with Two Shards)")
    @Setup(image = "mongo:4.4.0", topology = SHARDED_CLUSTER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_4_0__Sharded_Cluster_with_Two_Shards extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 4.4.0 (Single-Node Replica Set with Small Oplog)")
    @Setup(image = "mongo:4.4.0", topology = SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_4_0__Single_Node_Replica_Set_with_Small_Oplog extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 4.4.0 (Standalone Server)")
    @Setup(image = "mongo:4.4.0", topology = STANDALONE_SERVER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_4_0__Standalone_Server extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 4 latest (Single-Node Replica Set)")
    @Setup(image = "mongo:4", topology = SINGLE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_latest__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 4 latest (Three-Node Replica Set)")
    @Setup(image = "mongo:4", topology = THREE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_latest__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 4 latest (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:4", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_latest__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 4 latest (Sharded Cluster with Two Shards)")
    @Setup(image = "mongo:4", topology = SHARDED_CLUSTER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_latest__Sharded_Cluster_with_Two_Shards extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 4 latest (Single-Node Replica Set with Small Oplog)")
    @Setup(image = "mongo:4", topology = SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_4_latest__Single_Node_Replica_Set_with_Small_Oplog extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 5.0.0 (Single-Node Replica Set)")
    @Setup(image = "mongo:5.0.0", topology = SINGLE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_5_0_0__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 5.0.0 (Three-Node Replica Set)")
    @Setup(image = "mongo:5.0.0", topology = THREE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_5_0_0__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 5.0.0 (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:5.0.0", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_5_0_0__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 5 latest (Single-Node Replica Set)")
    @Setup(image = "mongo:5", topology = SINGLE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_5_latest__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 5 latest (Three-Node Replica Set)")
    @Setup(image = "mongo:5", topology = THREE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_5_latest__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 5 latest (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:5", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_5_latest__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 5 latest (Sharded Cluster with Two Shards)")
    @Setup(image = "mongo:5", topology = SHARDED_CLUSTER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_5_latest__Sharded_Cluster_with_Two_Shards extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 5 latest (Single-Node Replica Set with Small Oplog)")
    @Setup(image = "mongo:5", topology = SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_5_latest__Single_Node_Replica_Set_with_Small_Oplog extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 6.0.1 (Single-Node Replica Set)")
    @Setup(image = "mongo:6.0.1", topology = SINGLE_NODE_REPLICA_SET) // mongo:6.0.0 is not available
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_6_0_1__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 6.0.1 (Three-Node Replica Set)")
    @Setup(image = "mongo:6.0.1", topology = THREE_NODE_REPLICA_SET) // mongo:6.0.0 is not available
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_6_0_1__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 6.0.1 (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:6.0.1", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER) // mongo:6.0.0 is not available
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_6_0_1__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 6 latest (Single-Node Replica Set)")
    @Setup(image = "mongo:6", topology = SINGLE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_6_latest__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 6 latest (Three-Node Replica Set)")
    @Setup(image = "mongo:6", topology = THREE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_6_latest__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 6 latest (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:6", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_6_latest__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 6 latest (Sharded Cluster with Two Shards)")
    @Setup(image = "mongo:6", topology = SHARDED_CLUSTER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_6_latest__Sharded_Cluster_with_Two_Shards extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 6 latest (Single-Node Replica Set with Small Oplog)")
    @Setup(image = "mongo:6", topology = SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_6_latest__Single_Node_Replica_Set_with_Small_Oplog extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 7.0.0 (Single-Node Replica Set)")
    @Setup(image = "mongo:7.0.0", topology = SINGLE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_7_0_0__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 7.0.0 (Three-Node Replica Set)")
    @Setup(image = "mongo:7.0.0", topology = THREE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_7_0_0__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 7.0.0 (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:7.0.0", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_7_0_0__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 7 latest (Single-Node Replica Set)")
    @Setup(image = "mongo:7", topology = SINGLE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_7_latest__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 7 latest (Three-Node Replica Set)")
    @Setup(image = "mongo:7", topology = THREE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_7_latest__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 7 latest (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:7", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_7_latest__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 7 latest (Sharded Cluster with Two Shards)")
    @Setup(image = "mongo:7", topology = SHARDED_CLUSTER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_7_latest__Sharded_Cluster_with_Two_Shards extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 7 latest (Single-Node Replica Set with Small Oplog)")
    @Setup(image = "mongo:7", topology = SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_7_latest__Single_Node_Replica_Set_with_Small_Oplog extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 8.0.0 (Single-Node Replica Set)")
    @Setup(image = "mongo:8.0.0", topology = SINGLE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_8_0_0__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 8.0.0 (Three-Node Replica Set)")
    @Setup(image = "mongo:8.0.0", topology = THREE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_8_0_0__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 8.0.0 (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:8.0.0", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_8_0_0__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 8 latest (Single-Node Replica Set)")
    @Setup(image = "mongo:8", topology = SINGLE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_8_latest__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 8 latest (Three-Node Replica Set)")
    @Setup(image = "mongo:8", topology = THREE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_8_latest__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 8 latest (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:8", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_8_latest__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 8 latest (Sharded Cluster with Two Shards)")
    @Setup(image = "mongo:8", topology = SHARDED_CLUSTER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_8_latest__Sharded_Cluster_with_Two_Shards extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 8 latest (Single-Node Replica Set with Small Oplog)")
    @Setup(image = "mongo:8", topology = SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_8_latest__Single_Node_Replica_Set_with_Small_Oplog extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9.0.2 (Single-Node Replica Set)")
    @Setup(image = "mongo:9.0.2", topology = SINGLE_NODE_REPLICA_SET) // mongo:9.0.0 is not available
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_9_0_2__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9.0.2 (Three-Node Replica Set)")
    @Setup(image = "mongo:9.0.2", topology = THREE_NODE_REPLICA_SET) // mongo:9.0.0 is not available
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_9_0_2__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9.0.2 (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:9.0.2", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER) // mongo:9.0.0 is not available
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_9_0_2__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9 latest (Single-Node Replica Set)")
    @Setup(image = "mongo:9", topology = SINGLE_NODE_REPLICA_SET)
    class Mongo_9_latest__Single_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9 latest (Three-Node Replica Set)")
    @Setup(image = "mongo:9", topology = THREE_NODE_REPLICA_SET)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_9_latest__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9 latest (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:9", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_9_latest__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9 latest (Sharded Cluster with Two Shards)")
    @Setup(image = "mongo:9", topology = SHARDED_CLUSTER)
    @DisabledIfSystemProperty(named = RUNS_ON_GITHUB, matches = "true")
    class Mongo_9_latest__Sharded_Cluster_with_Two_Shards extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9 latest (Single-Node Replica Set with Small Oplog)")
    @Setup(image = "mongo:9", topology = SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG)
    class Mongo_9_latest__Single_Node_Replica_Set_with_Small_Oplog extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9 latest (Standalone Server)")
    @Setup(image = "mongo:9", topology = STANDALONE_SERVER)
    class Mongo_9_latest__Standalone_Server extends MongoTopologyTests {
    }
}
