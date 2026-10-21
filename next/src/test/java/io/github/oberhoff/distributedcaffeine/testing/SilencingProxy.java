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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A TCP proxy that can stop carrying anything on the connections it holds without closing them, as a network that
 * drops packets does: what is sent into such a connection is accepted and dropped, and nothing comes back. It can let
 * a few more bytes through first, which cuts a message off in the middle. Connections accepted afterwards are carried
 * as usual.
 */
public final class SilencingProxy implements AutoCloseable {

    private final ServerSocket serverSocket;
    private final String targetHost;
    private final int targetPort;
    private final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "silencing-proxy");
        thread.setDaemon(true);
        return thread;
    });
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private final Set<Budget> budgets = ConcurrentHashMap.newKeySet();
    private volatile long delayMillis;
    private volatile long requestDelayMillis;

    /**
     * Starts a proxy on a free local port.
     *
     * @param targetHost the host to carry the connections to
     * @param targetPort the port to carry the connections to
     * @throws IOException if the port cannot be opened
     */
    public SilencingProxy(String targetHost, int targetPort) throws IOException {
        this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        executor.execute(this::accept);
    }

    /**
     * Returns the local port the proxy accepts connections on.
     *
     * @return the port
     */
    public int port() {
        return serverSocket.getLocalPort();
    }

    /**
     * Silences every open connection at once.
     */
    public void silenceOpenConnections() {
        silenceOpenConnectionsAfter(0);
    }

    /**
     * Silences every open connection after the given number of further bytes, whichever direction they travel in.
     *
     * @param bytes the bytes still carried per connection
     */
    public void silenceOpenConnectionsAfter(long bytes) {
        budgets.forEach(budget -> budget.limit(bytes));
    }

    /**
     * Delays everything the proxy carries from now on by the given time per chunk, in either direction: a slow
     * network or a database slow to answer, rather than a broken one.
     *
     * @param delay the delay per chunk
     */
    public void delay(Duration delay) {
        delayMillis = delay.toMillis();
    }

    /**
     * Delays every request clients send from now on (what a client sends after an answer, or first) by the given
     * time, and nothing else: every round trip takes that much longer, whatever the size of the request or the answer
     * (a database slow to answer, not a slow network).
     *
     * @param delay the delay per request
     */
    public void delayRequests(Duration delay) {
        requestDelayMillis = delay.toMillis();
    }

    /**
     * Sends bytes to every open connection's client as if the server had sent them, e.g. a message it cannot decode.
     *
     * @param bytes the bytes
     * @throws IOException if a connection cannot be written to
     */
    public void sendToClients(byte[] bytes) throws IOException {
        for (Socket client : clients) {
            if (!client.isClosed()) {
                OutputStream out = client.getOutputStream();
                out.write(bytes);
                out.flush();
            }
        }
    }

    private void accept() {
        while (!serverSocket.isClosed()) {
            try {
                Socket client = serverSocket.accept();
                Socket server = new Socket(targetHost, targetPort);
                sockets.add(client);
                sockets.add(server);
                clients.add(client);
                Budget budget = new Budget();
                budgets.add(budget);
                executor.execute(() -> pump(client, server, budget, true));
                executor.execute(() -> pump(server, client, budget, false));
            } catch (IOException e) {
                // closed, which is how accepting ends
            }
        }
    }

    private void pump(Socket from, Socket to, Budget budget, boolean request) {
        byte[] buffer = new byte[8192];
        try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
            int read;
            while ((read = in.read(buffer)) >= 0) {
                int allowed = budget.take(read);
                long delay = delayMillis + (request && budget.startsRequest() ? requestDelayMillis : 0);
                if (!request) {
                    budget.answered();
                }
                if (delay > 0) {
                    Awaiting.pause(delay);
                }
                if (allowed > 0) {
                    out.write(buffer, 0, allowed);
                    out.flush();
                }
            }
        } catch (IOException e) {
            // either side closed, which is how pumping ends
        } finally {
            // a silenced connection stays open: a reset is exactly what it must not receive
            if (!budget.isLimited()) {
                closeQuietly(from);
                closeQuietly(to);
            }
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            // nothing left to do about it
        }
    }

    @Override
    public void close() throws IOException {
        serverSocket.close();
        sockets.forEach(SilencingProxy::closeQuietly);
        executor.shutdownNow();
    }

    // how many more bytes a connection carries, shared by both of its directions (unlimited until limited), and
    // whether the client waits for an answer
    private static final class Budget {

        private long remaining = -1;
        private boolean awaitingAnswer;

        // whether a chunk from the client starts a new request rather than continuing one
        synchronized boolean startsRequest() {
            boolean starts = !awaitingAnswer;
            awaitingAnswer = true;
            return starts;
        }

        synchronized void answered() {
            awaitingAnswer = false;
        }

        synchronized void limit(long bytes) {
            remaining = bytes;
        }

        synchronized boolean isLimited() {
            return remaining >= 0;
        }

        synchronized int take(int read) {
            if (remaining < 0) {
                return read;
            }
            int allowed = (int) Math.min(read, remaining);
            remaining -= allowed;
            return allowed;
        }
    }
}
