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
package io.github.oberhoff.distributedcaffeine.testing;

import com.github.dockerjava.api.DockerClient;
import org.jspecify.annotations.NullMarked;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.List;

/**
 * Containers paused as {@code docker pause} does, until closed: their processes frozen, while the kernel still accepts
 * connections on their ports, so that a client connects and then hears nothing, as from a server paused or behind a
 * network that drops every packet. For a store reached through several addresses (a replica set, Sentinel, a cluster),
 * where no single proxy can stand in front of it ({@link SilencingProxy}). Every container is paused at once and
 * resumed at once, so that their clocks agree again; pauses are meant to be short (a second or two): a Sentinel frozen
 * longer than two seconds goes into TILT mode and refuses failovers for 30 s after.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
public final class PausedContainers implements AutoCloseable {

    private final DockerClient docker = DockerClientFactory.instance().client();
    private final List<String> paused = new ArrayList<>();

    /**
     * Pauses the containers.
     *
     * @param containers the containers, each running
     */
    public PausedContainers(List<? extends GenericContainer<?>> containers) {
        try {
            for (GenericContainer<?> container : containers) {
                docker.pauseContainerCmd(container.getContainerId()).exec();
                paused.add(container.getContainerId());
            }
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    /**
     * Resumes the containers; idempotent.
     */
    @Override
    public void close() {
        for (String id : paused) {
            docker.unpauseContainerCmd(id).exec();
        }
        paused.clear();
    }
}
