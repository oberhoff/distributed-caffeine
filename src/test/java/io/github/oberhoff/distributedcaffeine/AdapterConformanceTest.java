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
package io.github.oberhoff.distributedcaffeine;

import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.testing.Awaiting;
import io.github.oberhoff.distributedcaffeine.testing.SilencingProxy;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The same situations on every adapter, with the same outcome: the exception, its flags and the gist of its message,
 * so that the adapters cannot drift apart again (the situation matrix of 2026-11-08). What only one store has stays in
 * its own tests.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
@SuppressWarnings("resource") // every cache and hook a test opens is closed after it (@AfterEach)
public abstract class AdapterConformanceTest {

    private static final Duration SHORT = Duration.ofSeconds(2);
    // the connect's backoff grows to 30 s, plus a check of the store bounded by the operation timeout
    private static final Duration CONNECT = Duration.ofSeconds(60);
    // an outage spanning several of the connect's attempts (the backoff starts at 250 ms and doubles)
    private static final Duration LONG_OUTAGE = Duration.ofSeconds(6);
    // a build that cannot reach the store: a few checks, each bounded by the operation timeout, but Redisson with lazy
    // initialization connects inside its first command, synchronously and with its own retries (four by default,
    // about 16 s against a server that is out)
    private static final Duration BUILD = Duration.ofSeconds(30);

    private final List<AutoCloseable> closeables = new ArrayList<>();

    /**
     * Returns the store's side of the situations, on a fresh place.
     *
     * @return the store's hooks
     */
    protected abstract AdapterConformance conformance();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private DistributedCache<String, String> build(Adapter<String, String> adapter) {
        return build(adapter, false);
    }

    private DistributedCache<String, String> build(Adapter<String, String> adapter, boolean failFast) {
        DistributedCache<String, String> cache = DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> failFast ? store.withOperationTimeout(SHORT).withFailFast()
                        : store.withOperationTimeout(SHORT))
                .build();
        closeables.add(cache);
        return cache;
    }

    // retryable is what starts a cache without its store (VISION.md, "Starting without the store"), so an adapter
    // must classify a server it cannot reach so
    @Test
    void a_store_that_cannot_be_reached_fails_the_build_as_retryable_with_fail_fast() {
        AdapterConformance store = track(conformance());
        assertThatThrownBy(() -> build(store.unreachable(), true))
                .isInstanceOfSatisfying(StoreException.class, e -> {
                    assertThat(e.isRetryable()).as("retryable: " + e.getMessage()).isTrue();
                    assertThat(e.isOutcomeUnknown()).as("nothing written").isFalse();
                });
    }

    @Test
    void a_store_that_cannot_be_reached_starts_the_cache_without_it() {
        AdapterConformance store = track(conformance());
        DistributedCache<String, String> cache = build(store.unreachable());

        assertThat(cache.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);
        assertThat(cache.get("k", key -> "source")).isEqualTo("source");
    }

    // a server reached through a proxy (SilencingProxy), so that a test can pause it or stop it without touching it
    private SilencingProxy proxy(AdapterConformance store) throws IOException {
        InetSocketAddress server = store.server();
        assumeTrue(server != null, "the store is not reached through one address");
        return track(new SilencingProxy(server.getHostString(), server.getPort()));
    }

    /**
     * Asserts a cache started without its store, as an outage while running is held to (InternalRecoveryTest):
     * {@code DEGRADED}, a miss loaded from the source and a write refused, each at once.
     *
     * @param started the cache
     */
    public static void startsDegraded(DistributedCache<String, String> started) {
        assertThat(started.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);
        long before = System.nanoTime();
        assertThat(started.get("held", key -> "source")).as("a miss loads from the source").isEqualTo("source");
        assertThatThrownBy(() -> started.put("refused", "while out")).as("a write is refused")
                .isInstanceOf(StoreException.class);
        assertThat(Duration.ofNanos(System.nanoTime() - before)).as("waiting for no store")
                .isLessThan(Duration.ofMillis(500));
    }

    /**
     * Asserts a cache started without its store recovering once connected, with another instance on the same place
     * that stored {@code "stored"} under {@code "held"} before: the value loaded meanwhile gives way to the stored
     * one, and changes travel both ways. Writes are repeated until the store takes them, as in the restart suites: a
     * store back from a restart may fail a first write with its outcome unknown (a pooler's stale connection).
     *
     * @param other   the other instance
     * @param started the cache started without its store
     * @param within  how long the connect may take
     */
    public static void recovers(DistributedCache<String, String> other, DistributedCache<String, String> started,
                                Duration within) {
        assertThat(Awaiting.awaitUpTo(() -> started.distributedPolicy().getSynchronizationState()
                == SynchronizationState.SYNCHRONIZED, within, Duration.ofMillis(50))).as("connected").isTrue();
        assertThat(Awaiting.awaitUpTo(() -> "stored".equals(started.getIfPresent("held")), within,
                Duration.ofMillis(50))).as("the value loaded meanwhile gave way to the stored one: "
                + started.getIfPresent("held")).isTrue();
        assertThat(Awaiting.awaitUpTo(() -> other.distributedPolicy().getSynchronizationState()
                == SynchronizationState.SYNCHRONIZED, within, Duration.ofMillis(50))).as("the other one too").isTrue();
        put(other, "held", "from the other", within);
        assertThat(Awaiting.awaitUpTo(() -> "from the other".equals(started.getIfPresent("held")), within,
                Duration.ofMillis(50))).as("a change of the other one reaches the started one").isTrue();
        put(started, "held", "from the started", within);
        assertThat(Awaiting.awaitUpTo(() -> "from the started".equals(other.getIfPresent("held")), within,
                Duration.ofMillis(50))).as("a change of the started one reaches the other one").isTrue();
    }

    /**
     * Puts a value, repeated until the store takes it.
     *
     * @param cache  the cache
     * @param key    the key
     * @param value  the value
     * @param within how long it may take
     */
    public static void put(DistributedCache<String, String> cache, String key, String value, Duration within) {
        assertThat(Awaiting.awaitUpTo(() -> {
            try {
                cache.put(key, value);
                return true;
            } catch (StoreException e) {
                return false;
            }
        }, within, Duration.ofMillis(100))).as("put " + value).isTrue();
    }

    // built while the store is out: within a bound, not hanging on a check without a timeout of its own (MongoDB's
    // deployment check once held the build for good)
    private DistributedCache<String, String> buildWithout(Adapter<String, String> adapter) {
        long before = System.nanoTime();
        DistributedCache<String, String> started = build(adapter);
        assertThat(Duration.ofNanos(System.nanoTime() - before)).as("the build waits a bounded time")
                .isLessThan(BUILD);
        return started;
    }

    // the other instance, reaching the store directly, with a value stored
    private DistributedCache<String, String> other(AdapterConformance store) {
        DistributedCache<String, String> other = build(store.adapter("default"));
        put(other, "held", "stored", CONNECT);
        return other;
    }

    // a server paused, or a network dropping everything, at the build: not reached within the operation timeout,
    // retryable, so the cache starts without it (VISION.md, "Starting without the store") and connects once it answers
    @Test
    void a_paused_store_starts_the_cache_without_it_which_connects_once_it_answers() throws IOException {
        AdapterConformance store = track(conformance());
        SilencingProxy proxy = proxy(store);
        DistributedCache<String, String> other = other(store);
        proxy.silenceNewConnections(true);
        DistributedCache<String, String> started = buildWithout(store.through("127.0.0.1", proxy.port()));
        startsDegraded(started);
        other.put("meanwhile", "written");

        proxy.silenceNewConnections(false);
        recovers(other, started, CONNECT);
        assertThat(started.getIfPresent("meanwhile")).as("written meanwhile").isEqualTo("written");
    }

    // silent for several of the connect's attempts, each bounded by the operation timeout: none hangs (a check
    // without a timeout of its own would hold the connect for good, as MongoDB's deployment check did), misses go on
    // loading from the source at once, and the cache connects once the store answers
    @Test
    void a_store_silent_for_several_attempts_starts_the_cache_without_it_which_connects_once_it_answers()
            throws IOException {
        AdapterConformance store = track(conformance());
        SilencingProxy proxy = proxy(store);
        DistributedCache<String, String> other = other(store);
        proxy.silenceNewConnections(true);
        DistributedCache<String, String> started = buildWithout(store.through("127.0.0.1", proxy.port()));
        startsDegraded(started);

        long end = System.nanoTime() + LONG_OUTAGE.toNanos();
        for (int i = 0; System.nanoTime() < end; i++) {
            long before = System.nanoTime();
            assertThat(started.get("miss " + i, key -> "source")).isEqualTo("source");
            assertThat(Duration.ofNanos(System.nanoTime() - before)).as("a miss waits for no store")
                    .isLessThan(Duration.ofMillis(500));
            assertThat(started.distributedPolicy().getSynchronizationState()).isEqualTo(SynchronizationState.DEGRADED);
            Awaiting.pause(250);
        }

        proxy.silenceNewConnections(false);
        recovers(other, started, CONNECT);
    }

    @Test
    void a_paused_store_fails_the_build_as_retryable_with_fail_fast() throws IOException {
        AdapterConformance store = track(conformance());
        SilencingProxy proxy = proxy(store);
        proxy.silenceNewConnections(true);

        assertThatThrownBy(() -> build(store.through("127.0.0.1", proxy.port()), true))
                .isInstanceOfSatisfying(StoreException.class, e -> {
                    assertThat(e.isRetryable()).as("retryable: " + e.getMessage()).isTrue();
                    assertThat(e.isOutcomeUnknown()).as("nothing written").isFalse();
                });
    }

    // a server stopped behind its address, then started again
    @Test
    void a_stopped_store_starts_the_cache_without_it_which_connects_once_it_is_back() throws IOException {
        AdapterConformance store = track(conformance());
        SilencingProxy proxy = proxy(store);
        InetSocketAddress server = requireNonNull(store.server());
        DistributedCache<String, String> other = other(store);
        proxy.retarget("127.0.0.1", 1);
        DistributedCache<String, String> started = buildWithout(store.through("127.0.0.1", proxy.port()));
        startsDegraded(started);

        proxy.retarget(server.getHostString(), server.getPort());
        recovers(other, started, CONNECT);
    }

    // caches sharing a client, built one after another while the store does not answer: the first waits for it, the
    // others start without it at once rather than each waiting (Adapter#client), and all connect once it answers
    @Test
    void caches_sharing_a_client_built_while_the_store_is_silent_wait_for_it_once() throws IOException {
        AdapterConformance store = track(conformance());
        SilencingProxy proxy = proxy(store);
        proxy.silenceNewConnections(true);
        List<Adapter<String, String>> adapters = store.sharingThrough("127.0.0.1", proxy.port(), 4);
        List<DistributedCache<String, String>> started = new ArrayList<>();
        started.add(buildWithout(adapters.get(0)));
        for (Adapter<String, String> adapter : adapters.subList(1, adapters.size())) {
            long before = System.nanoTime();
            started.add(build(adapter));
            assertThat(Duration.ofNanos(System.nanoTime() - before)).as("built without waiting for the store again")
                    .isLessThan(Duration.ofMillis(500));
        }
        started.forEach(AdapterConformanceTest::startsDegraded);

        proxy.silenceNewConnections(false);
        for (DistributedCache<String, String> cache : started) {
            assertThat(Awaiting.awaitUpTo(() -> cache.distributedPolicy().getSynchronizationState()
                    == SynchronizationState.SYNCHRONIZED, CONNECT, Duration.ofMillis(100))).as("connected").isTrue();
            put(cache, "written", "after", CONNECT);
        }
    }

    // a name that does not resolve: a DNS entry not there yet (a service starting) or gone, retryable as well
    @Test
    void a_host_that_does_not_resolve_fails_the_build_as_retryable_with_fail_fast() throws IOException {
        AdapterConformance store = track(conformance());
        proxy(store); // only where the store is reached through one address

        assertThatThrownBy(() -> build(store.through("unresolvable.invalid", 1), true))
                .isInstanceOfSatisfying(StoreException.class,
                        e -> assertThat(e.isRetryable()).as("retryable: " + e.getMessage()).isTrue());
    }

    // a server that answers and refuses the credentials: a configuration error, never a start without the store
    @Test
    void wrong_credentials_fail_the_build_for_good() {
        AdapterConformance store = track(conformance());
        Adapter<String, String> wrong = store.wrongCredentials();
        assumeTrue(wrong != null, "the server checks no credentials");

        assertThatThrownBy(() -> build(wrong))
                .isInstanceOfSatisfying(StoreException.class,
                        e -> assertThat(e.isRetryable()).as("retryable: " + e.getMessage()).isFalse());
    }

    @Test
    void a_place_of_another_version_is_refused_for_good_naming_it() {
        AdapterConformance store = track(conformance());
        build(store.adapter("default")).close();
        store.markOtherVersion();

        assertThatThrownBy(() -> build(store.adapter("default")))
                .isInstanceOfSatisfying(StoreException.class,
                        e -> assertThat(e.isRetryable()).as("retryable: " + e.getMessage()).isFalse())
                .hasMessageContaining("another version");
    }

    @Test
    void a_place_holding_foreign_data_is_refused_for_good() {
        AdapterConformance store = track(conformance());
        assumeTrue(store.putForeignData(), "the store keeps the cache's data apart by itself");

        assertThatThrownBy(() -> build(store.adapter("default")))
                .isInstanceOfSatisfying(StoreException.class,
                        e -> assertThat(e.isRetryable()).as("retryable: " + e.getMessage()).isFalse())
                .hasMessageContaining("without the schema marker");
    }

    @Test
    void a_blank_discriminator_is_rejected() {
        AdapterConformance store = track(conformance());
        assertThatThrownBy(() -> store.adapter("  ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void the_adapter_is_named_by_its_product_place_and_discriminator() {
        AdapterConformance store = track(conformance());
        Adapter<String, String> adapter = store.adapter("orders");
        build(adapter);

        assertThat(adapter.toString())
                .matches(Pattern.quote(store.product()) + " \\S+/orders")
                .doesNotContain("://").doesNotContain("password");
    }

    /**
     * What a store contributes to the situations; each method works on the one fresh place of a test.
     *
     * @author Andreas Oberhoff
     */
    public interface AdapterConformance extends AutoCloseable {

        /**
         * Returns an adapter on the test's place.
         *
         * @param discriminator the discriminator
         * @return the adapter
         */
        Adapter<String, String> adapter(String discriminator);

        /**
         * Marks the place, created by a cache built before, as written by another version of the library.
         */
        void markOtherVersion();

        /**
         * Fills the place, not created yet, with data of someone else.
         *
         * @return {@code false} where the store keeps the cache's data apart by itself, so that this cannot happen
         */
        boolean putForeignData();

        /**
         * Returns an adapter on a server that cannot be reached.
         *
         * @return the adapter
         */
        Adapter<String, String> unreachable();

        /**
         * Returns the address of the server the test's place is on, so that a proxy can stand in front of it.
         *
         * @return the address, or {@code null} where the store is not reached through one address (a replica set
         * whose client finds its members itself)
         */
        default @Nullable InetSocketAddress server() {
            return null;
        }

        /**
         * Returns an adapter on the test's place reached at the given address, with a client of its own that
         * {@link #close()} closes. Called only where {@link #server()} is not {@code null}.
         *
         * @param host the host
         * @param port the port
         * @return the adapter
         */
        default Adapter<String, String> through(String host, int port) {
            throw new UnsupportedOperationException("not reached through one address");
        }

        /**
         * Returns adapters of caches sharing one client of its own, reaching the store at the given address, each with
         * a discriminator of its own; {@link #close()} closes the client. Called only where {@link #server()} is not
         * {@code null}.
         *
         * @param host  the host
         * @param port  the port
         * @param count how many
         * @return the adapters
         */
        default List<Adapter<String, String>> sharingThrough(String host, int port, int count) {
            throw new UnsupportedOperationException("not reached through one address");
        }

        /**
         * Returns an adapter on the test's place whose credentials the server refuses.
         *
         * @return the adapter, or {@code null} where the server checks no credentials
         */
        default @Nullable Adapter<String, String> wrongCredentials() {
            return null;
        }

        /**
         * Returns the product the adapter names itself by once it was built.
         *
         * @return the product
         */
        String product();

        /**
         * Releases what the hooks opened, such as the client of the unreachable server.
         */
        @Override
        default void close() {
        }
    }
}
