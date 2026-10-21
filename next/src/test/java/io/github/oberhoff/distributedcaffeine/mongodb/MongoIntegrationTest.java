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

import io.github.oberhoff.distributedcaffeine.mongodb.MongoTopologyTests.Setup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import static io.github.oberhoff.distributedcaffeine.mongodb.MongoTopology.SHARDED_CLUSTER;
import static io.github.oberhoff.distributedcaffeine.mongodb.MongoTopology.SINGLE_NODE_REPLICA_SET;
import static io.github.oberhoff.distributedcaffeine.mongodb.MongoTopology.SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG;
import static io.github.oberhoff.distributedcaffeine.mongodb.MongoTopology.THREE_NODE_REPLICA_SET;
import static io.github.oberhoff.distributedcaffeine.mongodb.MongoTopology.THREE_NODE_REPLICA_SET_WITH_ARBITER;
import static io.github.oberhoff.distributedcaffeine.testing.Topology.Tests.RUNS_ON_GITHUB;

/**
 * Every MongoDB image and topology the suites run on, one at a time: from 4.4, the oldest supported, to the latest,
 * each major version's first release and its latest. The sharded cluster and the small oplog on the latest of each
 * major version (and 4.4.0). With {@code -DrunsOnGitHub=true}, only the latest image runs.
 */
class MongoIntegrationTest {

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
    class Mongo_9_latest__Three_Node_Replica_Set extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9 latest (Three-Node Replica Set with Arbiter)")
    @Setup(image = "mongo:9", topology = THREE_NODE_REPLICA_SET_WITH_ARBITER)
    class Mongo_9_latest__Three_Node_Replica_Set_with_Arbiter extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9 latest (Sharded Cluster with Two Shards)")
    @Setup(image = "mongo:9", topology = SHARDED_CLUSTER)
    class Mongo_9_latest__Sharded_Cluster_with_Two_Shards extends MongoTopologyTests {
    }

    @Nested
    @DisplayName("MongoDB 9 latest (Single-Node Replica Set with Small Oplog)")
    @Setup(image = "mongo:9", topology = SINGLE_NODE_REPLICA_SET_WITH_SMALL_OPLOG)
    class Mongo_9_latest__Single_Node_Replica_Set_with_Small_Oplog extends MongoTopologyTests {
    }
}
