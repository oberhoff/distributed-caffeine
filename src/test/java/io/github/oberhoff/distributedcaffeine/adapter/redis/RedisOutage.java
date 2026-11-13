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

import io.github.oberhoff.distributedcaffeine.AdapterOutageTest.Interruption;
import io.github.oberhoff.distributedcaffeine.AdapterOutageTest.Outage;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.testing.PausedContainers;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Redis's side of {@code AdapterOutageTest}, on a topology of several addresses (Sentinel, a cluster) through one
 * client library: every node paused at once, and a cache's primary lost before Sentinel or the cluster fails over
 * (its failover held back until the build is done, then forced, as {@code RedisFailoverTests} has it happen under
 * running caches). Each adapter has a client of its own, created during the outage as a restarting application
 * creates it: Redisson's with lazy initialization, since Redisson.create connects to the servers otherwise. Jedis's
 * clients of Sentinel and of a cluster cannot be created while the servers are out (their constructors ask Sentinel
 * for the primary, or the cluster for its slots, and throw), so an application restarting then fails before its
 * cache; here they are created before the outage, as an application whose cache is built later would have them.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class RedisOutage implements Outage {

    /**
     * The client libraries.
     */
    enum Library {
        LETTUCE, JEDIS, REDISSON
    }

    /**
     * A driver and what shuts its client down.
     *
     * @param driver the driver
     * @param client its client
     */
    record Client(RedisDriver driver, AutoCloseable client) {
    }

    private final String name = RedisTestServer.freshName();
    private final Supplier<Client> clients;
    private final Supplier<List<GenericContainer<?>>> containers;
    private final @Nullable Function<String, Interruption> withoutPrimary;
    private final List<Client> opened = new ArrayList<>();
    // clients created before any outage, for a library that cannot create them during one
    private final Deque<Client> created = new ArrayDeque<>();

    // withoutPrimary: given a key of the cache's slot, its primary lost until closed; null where not supported.
    // createdBefore: the library's clients cannot be created while the servers are out, so the two a test uses (the
    // other instance's and the started one's) are created now
    RedisOutage(Supplier<Client> clients, Supplier<List<GenericContainer<?>>> containers,
                @Nullable Function<String, Interruption> withoutPrimary, boolean createdBefore) {
        this.clients = clients;
        this.containers = containers;
        this.withoutPrimary = withoutPrimary;
        if (createdBefore) {
            for (int i = 0; i < 2; i++) {
                Client client = clients.get();
                opened.add(client);
                created.add(client);
            }
        }
    }

    @Override
    public Adapter<String, String> adapter() {
        Client client = created.poll();
        if (client == null) {
            client = clients.get();
            opened.add(client);
        }
        return RedisAdapter.newBuilder(client.driver(), name).build();
    }

    @Override
    public PausedContainers pause() {
        return new PausedContainers(containers.get());
    }

    @Override
    public boolean losesPrimary() {
        return withoutPrimary != null;
    }

    @Override
    public Interruption withoutPrimary() {
        if (withoutPrimary == null) {
            throw new UnsupportedOperationException("no primary to lose here");
        }
        return withoutPrimary.apply(new RedisKeys(name, RedisAdapter.DEFAULT_DISCRIMINATOR).schema());
    }

    @Override
    public void close() {
        for (Client client : opened) {
            try {
                client.client().close();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
