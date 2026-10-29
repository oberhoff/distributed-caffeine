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

import io.github.oberhoff.distributedcaffeine.CaffeineConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedAsyncLoadingCache;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.DistributionMode;
import io.github.oberhoff.distributedcaffeine.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.Row;
import io.github.oberhoff.distributedcaffeine.spi.State;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import io.github.oberhoff.distributedcaffeine.spi.Write;
import io.github.oberhoff.distributedcaffeine.testing.SilencingProxy;
import org.bson.BsonDocument;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.awaitUpTo;
import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.pause;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The MongoDB adapter end to end: caches distributing through it in every mode and format, the schema checks on
 * start, names, and recovery after a lost history.
 */
@NullMarked
@Timeout(60)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
abstract class MongoAdapterTest {

    private static final Duration LONG = Duration.ofHours(1);

    private final MongoTestDatabase database = MongoTestDatabase.get();
    private final String collection = MongoTestDatabase.freshCollection();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        // every cache closed: the shared registry is empty again (a leak would grow with every cache opened)
        assertThat(MongoWatcher.watchedClients()).as("no client watched any more").isZero();
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private MongoAdapter.Builder adapter() {
        return MongoAdapter.newBuilder(database.client, "adapter", collection);
    }

    private DistributedCaffeine<String, String> strings(MongoAdapter<String, String> adapter) {
        return DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000));
    }

    private DistributedCache<String, String> cache(Function<DistributedCaffeine<String, String>,
            DistributedCaffeine<String, String>> configuration) {
        return track(configuration.apply(strings(adapter().build())).build());
    }

    // distribution

    @Test
    void a_write_elsewhere_drops_the_held_copy() {
        DistributedCache<String, String> writer = cache(Function.identity());
        DistributedCache<String, String> holder = cache(Function.identity());
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        writer.put("k", "new");

        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("new");
        holder.invalidate("k");
        await(() -> writer.policy().getIfPresentQuietly("k") == null);
        assertThat(writer.getIfPresent("k")).isNull();
    }

    @Test
    void with_population_a_write_elsewhere_replaces_the_held_copy() {
        DistributedCache<String, String> writer = cache(Function.identity());
        DistributedCache<String, String> holder = cache(builder ->
                builder.withDistributionMode(DistributionMode.UPDATE));
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        writer.put("k", "new");

        await(() -> "new".equals(holder.policy().getIfPresentQuietly("k")));
    }

    @Test
    void invalidateAll_reaches_every_instance_and_the_store() {
        DistributedCache<String, String> first = cache(Function.identity());
        DistributedCache<String, String> second = cache(Function.identity());
        first.putAll(Map.of("a", "1", "b", "2"));
        assertThat(second.getAllPresent(List.of("a", "b"))).isEqualTo(Map.of("a", "1", "b", "2"));

        first.invalidateAll();

        await(() -> second.asMap().isEmpty());
        assertThat(second.getAllPresent(List.of("a", "b"))).isEmpty();
    }

    @Test
    void caches_in_one_collection_are_told_apart_by_their_discriminator_and_share_one_stream() {
        DistributedCache<String, String> orders = track(strings(adapter().withDiscriminator("orders").build()).build());
        DistributedCache<String, String> users = track(strings(adapter().withDiscriminator("users").build()).build());
        DistributedCache<String, String> otherCollection = track(strings(MongoAdapter.newBuilder(database.client,
                "adapter", collection + "_other").build()).build());
        DistributedCache<String, String> ordersHolder = track(strings(adapter().withDiscriminator("orders").build())
                .build());

        orders.put("k", "order");
        users.put("k", "user");
        otherCollection.put("k", "other");
        assertThat(ordersHolder.getIfPresent("k")).isEqualTo("order");
        users.invalidateAll();
        orders.put("k", "order 2");

        assertThat(requireNonNull(orders.distributedPolicy().getFromStore("k")).value()).isEqualTo("order 2");
        assertThat(users.distributedPolicy().getFromStore("k")).isNull();
        assertThat(otherCollection.getIfPresent("k")).isEqualTo("other");
        await(() -> ordersHolder.policy().getIfPresentQuietly("k") == null);
        assertThat(ordersHolder.getIfPresent("k")).isEqualTo("order 2");
    }

    record Order(String id, int amount, List<String> tags) {
    }

    @Test
    void every_format_round_trips_and_values_are_never_read_as_operators() {
        DistributedCache<String, Order> binary = track(DistributedCaffeine.newBuilder(
                        MongoAdapter.newBuilder(database.client, "adapter", collection + "_binary").<String, Order>build())
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(100)).build());
        binary.put("k", new Order("o1", 3, List.of("a")));
        assertThat(requireNonNull(binary.distributedPolicy().getFromStore("k")).value())
                .isEqualTo(new Order("o1", 3, List.of("a")));

        DistributedCache<String, Order> json = track(DistributedCaffeine.newBuilder(
                        MongoAdapter.newBuilder(database.client, "adapter", collection + "_json").<String, Order>build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(new JacksonSerializer<>(Order.class, true)))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(100)).build());
        json.put("k", new Order("$o2", 5, List.of("$x", "y")));
        assertThat(requireNonNull(json.distributedPolicy().getFromStore("k")).value())
                .isEqualTo(new Order("$o2", 5, List.of("$x", "y")));
        BsonDocument stored = requireNonNull(database.client.getDatabase("adapter")
                .getCollection(collection + "_json", BsonDocument.class)
                .find(new Document("_id.h", new Document("$exists", true))).first());
        assertThat(requireNonNull(stored.get("value")).isDocument()).as("stored as BSON").isTrue();

        DistributedCache<String, String> text = cache(Function.identity());
        text.put("$key", "$set");
        text.put("k", "ünïcödé");
        assertThat(requireNonNull(text.distributedPolicy().getFromStore("$key")).value()).isEqualTo("$set");
        assertThat(requireNonNull(text.distributedPolicy().getFromStore("k")).value()).isEqualTo("ünïcödé");
    }

    @Test
    void an_asynchronous_loading_cache_loads_through_the_store() throws Exception {
        DistributedAsyncLoadingCache<String, String> loading = track(strings(adapter().build())
                .buildAsync((key, executor) -> CompletableFuture.completedFuture("loaded-" + key)));
        DistributedCache<String, String> reader = cache(Function.identity());

        assertThat(loading.get("k").get(10, TimeUnit.SECONDS)).isEqualTo("loaded-k");
        assertThat(reader.getIfPresent("k")).isEqualTo("loaded-k");
    }

    // recovery

    @Test
    void a_lost_history_is_a_gap_after_which_held_values_are_checked() {
        DistributedCache<String, String> writer = cache(builder -> builder
                .withCaffeine(CaffeineConfigurer::recordStats));
        DistributedCache<String, String> holder = cache(builder -> builder
                .withCaffeine(CaffeineConfigurer::recordStats));
        writer.put("k", "v");
        assertThat(holder.getIfPresent("k")).isEqualTo("v");

        MongoWatcher.simulateHistoryLost(database.client, new MongoLocation("adapter", collection,
                MongoAdapter.DEFAULT_DISCRIMINATOR, Format.TEXT, Format.TEXT));

        await(() -> holder.distributedPolicy().stats().gapCount() == 1);
        await(() -> holder.distributedPolicy().stats().revalidatedCount() >= 1);
        writer.put("k", "after the gap");
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("after the gap");
    }

    @Test
    void writes_during_an_interruption_arrive_when_the_stream_resumes() {
        DistributedCache<String, String> writer = cache(builder -> builder.withDistributionMode(DistributionMode.NONE));
        DistributedCache<String, String> holder =
                cache(builder -> builder.withCaffeine(CaffeineConfigurer::recordStats));
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        MongoWatcher.simulateInterruption(database.client, new MongoLocation("adapter", collection,
                MongoAdapter.DEFAULT_DISCRIMINATOR, Format.TEXT, Format.TEXT));
        // counted, not awaited as a state: the core resumes at once, so DEGRADED may last milliseconds only
        await(() -> holder.distributedPolicy().stats().interruptionCount() >= 1);
        writer.put("k", "written while interrupted");

        await(() -> holder.distributedPolicy().getSynchronizationState()
                == io.github.oberhoff.distributedcaffeine.SynchronizationState.SYNCHRONIZED);
        // resumed from the token, which is a gap: the write's event arrives or the revalidation finds it
        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("written while interrupted");
    }

    @Test
    void a_cache_joining_with_another_collection_while_the_stream_recovers_loses_no_event() {
        DistributedCache<String, String> holder =
                cache(builder -> builder.withCaffeine(CaffeineConfigurer::recordStats));
        long interruptions = holder.distributedPolicy().stats().interruptionCount();
        MongoWatcher.simulateInterruption(database.client, new MongoLocation("adapter", collection,
                MongoAdapter.DEFAULT_DISCRIMINATOR, Format.TEXT, Format.TEXT));
        // the core resumes at once; the join may come before, during or after it
        await(() -> holder.distributedPolicy().stats().interruptionCount() > interruptions);

        // widening the stream needs it open: a join while it is lost reopens it
        String other = MongoTestDatabase.freshCollection();
        DistributedCache<String, String> joining = track(strings(MongoAdapter.newBuilder(database.client, "adapter",
                other).build()).build());
        DistributedCache<String, String> writer = track(strings(MongoAdapter.newBuilder(database.client, "adapter",
                other).build()).withDistributionMode(DistributionMode.NONE).build());
        writer.put("k", "old");
        assertThat(joining.getIfPresent("k")).isEqualTo("old");
        writer.put("k", "new");

        await(() -> joining.policy().getIfPresentQuietly("k") == null);
        await(() -> holder.distributedPolicy().getSynchronizationState()
                == io.github.oberhoff.distributedcaffeine.SynchronizationState.SYNCHRONIZED);
    }

    @Test
    void a_slow_database_is_waited_for_and_not_taken_for_a_broken_one() throws Exception {
        SilencingProxy proxy = track(new SilencingProxy(database.host(),
                database.port()));
        com.mongodb.client.MongoClient client = track(com.mongodb.client.MongoClients.create(
                "mongodb://127.0.0.1:" + proxy.port() + "/?directConnection=true"));
        DistributedCache<String, String> writer = track(strings(MongoAdapter.newBuilder(client, "adapter", collection)
                .build()).withDistributionMode(DistributionMode.NONE).build());
        DistributedCache<String, String> holder = track(strings(MongoAdapter.newBuilder(client, "adapter", collection)
                .build()).build());
        writer.put("k", "before");
        assertThat(holder.getIfPresent("k")).isEqualTo("before");

        // every chunk either way a quarter of a second late: a round trip of at least half a second
        proxy.delay(Duration.ofMillis(250));
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        for (int index = 0; System.nanoTime() < until; index++) {
            String value = "slow " + index;
            writer.put("k", value);
            await(() -> holder.policy().getIfPresentQuietly("k") == null);
            assertThat(holder.getIfPresent("k")).isEqualTo(value);
            assertThat(holder.distributedPolicy().getSynchronizationState()).as("slow, not interrupted")
                    .isEqualTo(io.github.oberhoff.distributedcaffeine.SynchronizationState.SYNCHRONIZED);
        }
    }

    @Test
    void a_collection_dropped_under_the_caches_is_a_gap_and_watching_goes_on() {
        DistributedCache<String, String> writer = cache(builder -> builder.withDistributionMode(DistributionMode.NONE));
        DistributedCache<String, String> holder = cache(Function.identity());
        writer.putAll(Map.of("a", "1", "b", "2"));
        assertThat(holder.getAllPresent(List.of("a", "b"))).hasSize(2);

        database.client.getDatabase("adapter").getCollection(collection).drop();

        // every document is gone, which no write announces: the revalidation after the gap drops what is held
        await(() -> holder.policy().getIfPresentQuietly("a") == null
                && holder.policy().getIfPresentQuietly("b") == null);
        assertThat(holder.getIfPresent("a")).isNull();
        writer.put("a", "written after the drop");
        assertThat(holder.getIfPresent("a")).isEqualTo("written after the drop");
        writer.put("a", "written again");
        await(() -> holder.policy().getIfPresentQuietly("a") == null);
        assertThat(holder.getIfPresent("a")).isEqualTo("written again");
    }

    @Test
    void a_database_dropped_under_the_caches_is_a_gap_and_watching_goes_on() {
        String databaseName = "dropped_" + MongoTestDatabase.freshCollection();
        Function<DistributionMode, DistributedCache<String, String>> cacheOf = mode -> track(strings(
                MongoAdapter.newBuilder(database.client, databaseName, collection).build())
                .withDistributionMode(mode).build());
        DistributedCache<String, String> writer = cacheOf.apply(DistributionMode.NONE);
        DistributedCache<String, String> holder = cacheOf.apply(DistributionMode.INVALIDATE);
        writer.put("a", "1");
        assertThat(holder.getIfPresent("a")).isEqualTo("1");

        database.client.getDatabase(databaseName).drop();

        await(() -> holder.policy().getIfPresentQuietly("a") == null);
        writer.put("a", "written after the drop");
        assertThat(holder.getIfPresent("a")).isEqualTo("written after the drop");
        writer.put("a", "written again");
        await(() -> holder.policy().getIfPresentQuietly("a") == null);
        assertThat(holder.distributedPolicy().getSynchronizationState())
                .isEqualTo(io.github.oberhoff.distributedcaffeine.SynchronizationState.SYNCHRONIZED);
    }

    @Test
    void a_client_timeout_bounds_each_batch_of_a_large_read_not_the_whole_read() throws Exception {
        SilencingProxy proxy = track(new SilencingProxy(database.host(),
                database.port()));
        com.mongodb.client.MongoClient timed = track(com.mongodb.client.MongoClients.create(
                com.mongodb.MongoClientSettings.builder()
                        .applyConnectionString(new com.mongodb.ConnectionString(
                                "mongodb://127.0.0.1:" + proxy.port() + "/?directConnection=true"))
                        .timeout(2, TimeUnit.SECONDS)
                        .build()));
        var store = track(MongoAdapter.newBuilder(timed, "adapter", collection).build()
                .createStore(StoreSettings.of(Format.TEXT, Format.TEXT)));
        // 24 MB of values: a read of all of them takes several batches
        String value = "x".repeat(3_000);
        List<Document> rows = new ArrayList<>();
        for (int index = 0; index < 8_000; index++) {
            rows.add(new Document("_id", new Document("d", MongoAdapter.DEFAULT_DISCRIMINATOR).append("h", "h" + index))
                    .append("key", "k" + index).append("value", value).append("state", "VALUE")
                    .append("version", (long) index + 1).append("writeTime", java.time.Instant.now()));
        }
        database.client.getDatabase("adapter").getCollection(collection).insertMany(rows);

        // every round trip takes most of a second: the read as a whole takes longer than the timeout, each batch not
        proxy.delayRequests(Duration.ofMillis(1_200));
        assertThat(store.pruneToSize(100, Duration.ZERO)).isEqualTo(7_900);
    }

    @Test
    void a_change_stream_the_network_silently_stopped_carrying_is_resumed() throws Exception {
        SilencingProxy proxy = track(new SilencingProxy(database.host(),
                database.port()));
        com.mongodb.client.MongoClient proxied = track(com.mongodb.client.MongoClients.create(
                "mongodb://127.0.0.1:" + proxy.port() + "/?directConnection=true"));
        DistributedCache<String, String> writer = cache(builder -> builder.withDistributionMode(DistributionMode.NONE));
        DistributedCache<String, String> holder = track(strings(MongoAdapter.newBuilder(proxied, "adapter", collection)
                .build()).build());
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        // the stream's connection carries nothing any more, and is not closed either: its getMore would wait for good
        proxy.silenceOpenConnections();
        writer.put("k", "written while the stream was silent");

        assertThat(awaitUpTo(() -> holder.policy().getIfPresentQuietly("k") == null,
                Duration.ofSeconds(MongoWatcher.OPERATION_TIMEOUT_SECONDS + 20), Duration.ofMillis(50)))
                .as("resumed on a new connection, the write's event arrives").isTrue();
        assertThat(holder.getIfPresent("k")).isEqualTo("written while the stream was silent");
        await(() -> holder.distributedPolicy().getSynchronizationState()
                == io.github.oberhoff.distributedcaffeine.SynchronizationState.SYNCHRONIZED);
    }

    @Test
    void a_store_call_on_a_silent_connection_fails_within_the_timeout_instead_of_hanging() throws Exception {
        SilencingProxy proxy = track(new SilencingProxy(database.host(),
                database.port()));
        // the driver's defaults: no client timeout, no socket timeout
        com.mongodb.client.MongoClient proxied = track(com.mongodb.client.MongoClients.create(
                "mongodb://127.0.0.1:" + proxy.port() + "/?directConnection=true"));
        DistributedCache<String, String> cache = track(strings(MongoAdapter.newBuilder(proxied, "adapter", collection)
                .build()).withStore(store -> store.withTimeout(Duration.ofSeconds(2)))
                .withDistributionMode(DistributionMode.NONE).build());
        cache.put("k", "before");

        proxy.silenceOpenConnections();
        long started = System.nanoTime();
        assertThatThrownBy(() -> cache.put("k", "during"))
                .isInstanceOfSatisfying(StoreException.class, e -> assertThat(e.isOutcomeUnknown()).isTrue());
        assertThat(Duration.ofNanos(System.nanoTime() - started)).as("failing after the timeout, not hanging")
                .isLessThan(Duration.ofSeconds(6));

        // the timed-out connection is closed by the driver; a new one is carried
        assertThat(awaitUpTo(() -> {
            try {
                cache.put("k", "after");
                return true;
            } catch (StoreException e) {
                return false;
            }
        }, Duration.ofSeconds(20), Duration.ofMillis(200))).as("writing again").isTrue();
        assertThat(requireNonNull(cache.distributedPolicy().getFromStore("k")).value()).isEqualTo("after");
    }

    @Test
    void an_unreachable_server_fails_a_store_call_after_one_wait_not_three() throws Exception {
        SilencingProxy proxy = new SilencingProxy(database.host(),
                database.port());
        com.mongodb.client.MongoClient proxied = track(com.mongodb.client.MongoClients.create(
                "mongodb://127.0.0.1:" + proxy.port() + "/?directConnection=true&serverSelectionTimeoutMS=1000"));
        DistributedCache<String, String> cache = track(strings(MongoAdapter.newBuilder(proxied, "adapter", collection)
                .build()).withDistributionMode(DistributionMode.NONE).build());
        cache.put("k", "before");

        proxy.close(); // nothing answers on that port any more
        long started = System.nanoTime();
        assertThatThrownBy(() -> cache.put("k", "during")).isInstanceOf(StoreException.class);
        assertThat(Duration.ofNanos(System.nanoTime() - started)).as("one server selection, not three")
                .isLessThan(Duration.ofMillis(1_900));
    }

    @Test
    void caches_joining_the_shared_stream_with_other_collections_lose_no_event() throws Exception {
        DistributedCache<String, String> writer = cache(builder -> builder.withDistributionMode(DistributionMode.NONE));
        DistributedCache<String, String> holder = cache(builder -> builder
                .withCaffeine(CaffeineConfigurer::recordStats));
        java.util.concurrent.atomic.AtomicInteger written = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean writing = new java.util.concurrent.atomic.AtomicBoolean(true);
        Thread writes = new Thread(() -> {
            while (writing.get()) {
                writer.put("k" + written.get() % 10, String.valueOf(written.incrementAndGet()));
            }
        });
        writes.start();
        // every join with a new collection reopens the shared stream from its resume token, while the writes go on
        for (int i = 0; i < 6; i++) {
            track(strings(MongoAdapter.newBuilder(database.client, "adapter", collection + "_joining_" + i).build())
                    .build());
        }
        writing.set(false);
        writes.join();

        // one event per write: none lost at a reopen (and none twice)
        await(() -> holder.distributedPolicy().stats().writtenMessageCount() >= written.get());
        pause(300);
        assertThat(holder.distributedPolicy().stats().writtenMessageCount()).isEqualTo(written.get());
    }

    @Test
    void a_change_stream_that_stops_delivering_without_an_error_is_noticed_by_the_heartbeat() {
        DistributedCache<String, String> writer = cache(builder -> builder.withDistributionMode(DistributionMode.NONE));
        DistributedCache<String, String> holder = track(DistributedCaffeine.<String, String>newBuilder(
                        adapter().build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(1_000)
                        .withHeartbeatInterval(Duration.ofMillis(300)))
                .withCaffeine(CaffeineConfigurer::recordStats)
                .build());
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        // the stream is not read: no error, no event, as on a network that silently stopped carrying
        long interruptions = holder.distributedPolicy().stats().interruptionCount();
        long gaps = holder.distributedPolicy().stats().gapCount();
        MongoWatcher.simulateLag(database.client, true);
        try {
            writer.put("k", "new");

            await(() -> holder.distributedPolicy().stats().interruptionCount() > interruptions);
            assertThat(holder.getIfPresent("k")).as("checked against the store").isEqualTo("new");
        } finally {
            MongoWatcher.simulateLag(database.client, false);
        }
        await(() -> holder.distributedPolicy().stats().gapCount() > gaps);
        await(() -> holder.distributedPolicy().getSynchronizationState() == SynchronizationState.SYNCHRONIZED);
    }

    @Test
    void caches_leaving_narrow_the_shared_stream_and_lose_no_event_of_the_others() {
        DistributedCache<String, String> writer = cache(builder -> builder.withDistributionMode(DistributionMode.NONE));
        DistributedCache<String, String> holder = cache(builder -> builder
                .withCaffeine(CaffeineConfigurer::recordStats));
        List<DistributedCache<String, String>> leaving = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            // two caches (discriminators) per collection: the collection leaves with the last one
            for (String discriminator : List.of("first", "second")) {
                leaving.add(track(strings(MongoAdapter.newBuilder(database.client, "adapter",
                        collection + "_leaving_" + i).withDiscriminator(discriminator).build()).build()));
            }
        }
        assertThat(MongoWatcher.coveredCollections(database.client))
                .contains(collection + "_leaving_0", collection + "_leaving_1", collection + "_leaving_2");

        // the events of these writes are still unread when the stream narrows: it has to resume behind them
        MongoWatcher.simulateLag(database.client, true);
        int written = 50;
        for (int i = 0; i < written; i++) {
            writer.put("k" + i % 10, String.valueOf(i));
        }
        leaving.get(0).close();
        pause(200);
        assertThat(MongoWatcher.coveredCollections(database.client)).contains(collection + "_leaving_0");
        leaving.forEach(DistributedCache::close);
        await(() -> MongoWatcher.coveredCollections(database.client).equals(Set.of(collection)));
        MongoWatcher.simulateLag(database.client, false);

        // one event per write of the remaining collection: none lost at the reopen (and none twice)
        await(() -> holder.distributedPolicy().stats().writtenMessageCount() >= written);
        pause(300);
        assertThat(holder.distributedPolicy().stats().writtenMessageCount()).isEqualTo(written);
    }

    @Test
    void documents_of_no_cache_neither_stop_the_change_stream_nor_pass_as_rows() {
        DistributedCache<String, String> writer = cache(Function.identity());
        DistributedCache<String, String> holder = cache(Function.identity());
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        // written by hand or by another program into the cache's collection
        var documents = database.client.getDatabase("adapter").getCollection(collection);
        documents.insertOne(new Document("_id", new Document("d", MongoAdapter.DEFAULT_DISCRIMINATOR)
                .append("h", "foreign")).append("state", "UNKNOWN").append("version", 5L));
        documents.insertOne(new Document("_id", new Document("d", 42).append("h", "foreign")).append("version", 5L));
        writer.put("k", "new");

        await(() -> holder.policy().getIfPresentQuietly("k") == null);
        assertThat(holder.getIfPresent("k")).isEqualTo("new");
        try (var store = adapter().build().createStore(StoreSettings.of(Format.TEXT, Format.TEXT))) {
            assertThatThrownBy(() -> store.read(List.of("foreign"), LONG)).isInstanceOf(StoreException.class)
                    .hasMessageContaining("UNKNOWN");
        }
    }

    @Test
    void maintenance_leaves_reserved_documents_alone_and_deletes_what_the_control_row_hides() {
        try (var store = adapter().build().createStore(StoreSettings.of(Format.TEXT, Format.TEXT))) {
            byte[] bytes = "x".getBytes(StandardCharsets.UTF_8);
            // any versions: the store only stores them
            store.write(List.of(Write.value("hidden", bytes, bytes, 1)));
            // written after it (or in the same millisecond, which also hides it: at or before)
            store.write(List.of(Write.value(Store.CONTROL_HASH, bytes, bytes, 2)));

            assertThat(store.expire(Duration.ZERO, 3)).as("neither the hidden one nor the control row").isZero();
            assertThat(store.pruneToSize(0, Duration.ZERO)).as("neither counted nor deleted").isZero();
            assertThat(store.pruneTombstones(LONG)).as("the hidden one, without a grace period").isOne();
            assertThat(store.read(List.of("hidden", Store.CONTROL_HASH), LONG)).containsOnlyKeys(Store.CONTROL_HASH);
        }
    }

    @Test
    void a_bulk_write_reports_each_writes_outcome_and_a_write_on_a_pruned_version_leaves_no_value() {
        try (var store = adapter().build().createStore(StoreSettings.of(Format.TEXT, Format.TEXT))) {
            byte[] bytes = "x".getBytes(StandardCharsets.UTF_8);
            byte[] newer = "y".getBytes(StandardCharsets.UTF_8);
            store.write(List.of(Write.value("matching", bytes, bytes, 1), Write.value("other", bytes, bytes, 1),
                    Write.value("pruned", bytes, bytes, 1), Write.value("existing", bytes, bytes, 1)));
            // deleted under the write, as maintenance does after the read the write was decided on
            database.client.getDatabase("adapter").getCollection(collection)
                    .deleteOne(new Document("_id.h", "pruned"));

            Set<String> conflicts = store.write(List.of(
                    Write.value("matching", bytes, newer, 2).ifVersion(1),
                    Write.value("other", bytes, newer, 2).ifVersion(7),
                    Write.value("pruned", bytes, newer, 2).ifVersion(1),
                    Write.value("fresh", bytes, newer, 2).ifMissing(),
                    Write.value("existing", bytes, newer, 2).ifMissing())).refused();

            assertThat(conflicts).as("each by its own outcome, in one bulk write")
                    .containsExactlyInAnyOrder("other", "pruned", "existing");
            Map<String, Row> rows = store.read(List.of("matching", "other", "pruned", "fresh"), LONG);
            assertThat(rows.get("matching").version()).isEqualTo(2);
            assertThat(rows.get("other").version()).as("untouched").isEqualTo(1);
            assertThat(rows.get("pruned").state()).as("no value from a failed condition").isEqualTo(State.TOMBSTONE);
            assertThat(rows.get("pruned").value()).isNull();
            assertThat(rows.get("fresh").version()).isEqualTo(2);
            assertThat(store.read(List.of("existing"), LONG).get("existing").version()).as("untouched")
                    .isEqualTo(1);
        }
    }

    // schema and names

    @Test
    void the_collection_is_created_with_marker_and_index_and_reused() {
        cache(Function.identity()).put("k", "v");
        cache(Function.identity());

        var created = database.client.getDatabase("adapter").getCollection(collection);
        assertThat(requireNonNull(created.find(new Document("_id", MongoLocation.MARKER_ID)).first()).getString("schema"))
                .isEqualTo(MongoLocation.MARKER);
        assertThat(created.listIndexes().into(new ArrayList<>())).anyMatch(index ->
                index.get("key", Document.class).equals(new Document("_id.d", 1).append("state", 1).append("writeTime", 1)));
        assertThat(created.countDocuments(new Document("_id.c", MongoLocation.START))).as("one per discriminator")
                .isEqualTo(1);
    }

    @Test
    void a_collection_without_the_marker_is_refused_unless_it_is_reset() {
        database.client.getDatabase("adapter").getCollection(collection).insertOne(new Document("_id", "legacy"));

        Function<DistributedCaffeine<String, String>, DistributedCaffeine<String, String>> unchanged =
                Function.identity();
        assertThatThrownBy(() -> cache(unchanged)).isInstanceOf(StoreException.class)
                .hasMessageContaining("withSchemaReset");

        DistributedCache<String, String> reset = track(strings(adapter().withSchemaReset().build()).build());
        reset.put("k", "v");
        assertThat(reset.getIfPresent("k")).isEqualTo("v");
        assertThat(database.client.getDatabase("adapter").getCollection(collection)
                .countDocuments(new Document("_id", "legacy"))).isZero();
    }

    @Test
    void invalid_names_are_rejected() {
        assertThatThrownBy(() -> MongoAdapter.newBuilder(database.client, "a.b", "c"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MongoAdapter.newBuilder(database.client, "a", "c$d"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MongoAdapter.newBuilder(database.client, "a", "system.users"))
                .isInstanceOf(IllegalArgumentException.class);
        String tooLong = "c".repeat(260);
        assertThatThrownBy(() -> MongoAdapter.newBuilder(database.client, "a", tooLong))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("255 bytes");
    }
}
