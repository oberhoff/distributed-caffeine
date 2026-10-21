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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * Fixed host ports for containers that must keep them across a restart (a member stopped and started again is
 * reached at the same address). A port is free when chosen, but another container may take it before Docker binds it,
 * so a start that fails on a taken port is retried with new ones.
 */
public final class Ports {

    private static final int ATTEMPTS = 3;
    private static final int LOWEST = 20_000;
    private static final int RANGE = 10_000;

    private Ports() {
    }

    /**
     * Chooses ports that are free now, and starts with them, choosing anew if one was taken meanwhile.
     *
     * @param count the number of ports
     * @param start starts the containers on the given ports; it stops what it started if it fails
     * @return the ports started with
     */
    public static List<Integer> startOnFree(int count, Consumer<List<Integer>> start) {
        for (int attempt = 1; ; attempt++) {
            List<Integer> ports = free(count);
            try {
                start.accept(ports);
                return ports;
            } catch (RuntimeException e) {
                if (attempt == ATTEMPTS || !taken(e)) {
                    throw e;
                }
            }
        }
    }

    // distinct ports, each free when chosen; below the ephemeral range (49152 and up on macOS and Windows, 32768 and
    // up on Linux), so that the outgoing connections of the test itself never take one while its container is stopped
    private static List<Integer> free(int count) {
        Set<Integer> ports = new LinkedHashSet<>();
        while (ports.size() < count) {
            int port = LOWEST + ThreadLocalRandom.current().nextInt(RANGE);
            try (ServerSocket socket = new ServerSocket(port)) {
                ports.add(socket.getLocalPort());
            } catch (IOException e) {
                // taken: another one
            }
        }
        return List.copyOf(ports);
    }

    private static boolean taken(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains("port is already allocated")) {
                return true;
            }
        }
        return false;
    }
}
