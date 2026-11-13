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
import io.github.oberhoff.distributedcaffeine.AdapterOutageTest.Interruption;
import io.github.oberhoff.distributedcaffeine.AdapterOutageTest.Outage;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.testing.PausedContainers;
import org.jspecify.annotations.NullMarked;

import java.util.ArrayList;
import java.util.List;

/**
 * MongoDB's side of {@code AdapterOutageTest}: a topology of several addresses (a replica set of several members, a
 * sharded cluster) paused as a whole, hosted where the topology runs the outage suite; one of a single address meets
 * the same outage through a proxy ({@code AdapterConformanceTest}), and a standalone server is refused at the build.
 * A replica set is also stopped as a whole (as {@code MongoFailoverTest} restarts it under running caches) and left
 * without its majority (every member but the preferred one stopped, which then steps down).
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class MongoOutage implements Outage {

    private final MongoTestDatabase database = MongoTestDatabase.get();
    private final String collection = MongoTestDatabase.freshCollection();
    private final List<MongoClient> clients = new ArrayList<>();

    @Override
    public boolean supported() {
        return database.topology != MongoTopology.STANDALONE_SERVER;
    }

    @Override
    public Adapter<String, String> adapter() {
        MongoClient own = database.newClient();
        clients.add(own);
        return MongoAdapter.newBuilder(own, "outage", collection).build();
    }

    @Override
    public PausedContainers pause() {
        return new PausedContainers(database.allContainers());
    }

    private boolean replicaSet() {
        return database.topology == MongoTopology.THREE_NODE_REPLICA_SET
                || database.topology == MongoTopology.THREE_NODE_REPLICA_SET_WITH_ARBITER;
    }

    @Override
    public boolean stops() {
        return replicaSet();
    }

    @Override
    public Interruption stop() {
        return without(0);
    }

    @Override
    public boolean losesPrimary() {
        return replicaSet();
    }

    @Override
    public Interruption withoutPrimary() {
        return without(1);
    }

    // every member from the given one on stopped, the last first; closing starts them again, the preferred one
    // primary again
    private Interruption without(int from) {
        MongoReplicaSet replicaSet = database.replicaSet();
        int members = replicaSet.members().size();
        for (int index = members - 1; index >= from; index--) {
            replicaSet.stop(index);
        }
        return () -> {
            for (int index = from; index < members; index++) {
                replicaSet.start(index);
            }
            replicaSet.handBack();
        };
    }

    @Override
    public void close() {
        clients.forEach(MongoClient::close);
    }
}
