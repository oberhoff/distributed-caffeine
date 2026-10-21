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
package io.github.oberhoff.distributedcaffeine.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.CaffeineConfigurer;
import io.github.oberhoff.distributedcaffeine.DistributedAsyncLoadingCache;
import io.github.oberhoff.distributedcaffeine.DistributedCache;
import io.github.oberhoff.distributedcaffeine.DistributedCaffeine;
import io.github.oberhoff.distributedcaffeine.DistributionMode;
import io.github.oberhoff.distributedcaffeine.SynchronizationState;
import io.github.oberhoff.distributedcaffeine.serializer.JacksonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.StringSerializer;
import io.github.oberhoff.distributedcaffeine.spi.StoreException;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.github.oberhoff.distributedcaffeine.testing.Awaiting.await;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The PostgreSQL adapter end to end: caches distributing through it in every mode and format, the schema checks on
 * start, and recovery when the listening session breaks.
 */
@Timeout(60)
@SuppressWarnings("resource") // every cache and store a test opens is closed after it (@AfterEach)
abstract class PostgresAdapterTest {

    private static final Duration LONG = Duration.ofHours(1);

    private final PostgresTestDatabase database = PostgresTestDatabase.get();
    private final String table = PostgresTestDatabase.freshTable();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private <C extends AutoCloseable> C track(C closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private PostgresAdapter.Builder adapter() {
        return PostgresAdapter.newBuilder(database.dataSource, "adapter", table).withListener(database.listener);
    }

    private DistributedCaffeine<String, String> strings(PostgresAdapter<String, String> adapter) {
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
    void caches_in_one_table_are_told_apart_by_their_discriminator() {
        DistributedCache<String, String> orders = track(strings(adapter().withDiscriminator("orders").build()).build());
        DistributedCache<String, String> users = track(strings(adapter().withDiscriminator("users").build()).build());

        orders.put("k", "order");
        users.put("k", "user");
        users.invalidateAll();

        assertThat(requireNonNull(orders.distributedPolicy().getFromStore("k")).value()).isEqualTo("order");
        assertThat(users.distributedPolicy().getFromStore("k")).isNull();
        assertThat(orders.getIfPresent("k")).isEqualTo("order");
    }

    record Order(String id, int amount) {
    }

    @Test
    void every_format_round_trips() {
        // BINARY (Fory, the default)
        DistributedCache<String, Order> binary = track(DistributedCaffeine.newBuilder(
                        PostgresAdapter.newBuilder(database.dataSource, "adapter", table + "_binary")
                                .withListener(database.listener).<String, Order>build())
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(100)).build());
        binary.put("k", new Order("o1", 3));
        assertThat(requireNonNull(binary.distributedPolicy().getFromStore("k")).value()).isEqualTo(new Order("o1", 3));

        // JSON (jsonb)
        DistributedCache<String, Order> json = track(DistributedCaffeine.newBuilder(
                        PostgresAdapter.newBuilder(database.dataSource, "adapter", table + "_json")
                                .withListener(database.listener).<String, Order>build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(new JacksonSerializer<>(Order.class, true)))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(100)).build());
        json.put("k", new Order("o2", 5));
        assertThat(requireNonNull(json.distributedPolicy().getFromStore("k")).value()).isEqualTo(new Order("o2", 5));
        assertThat(columnType(table + "_json", "value")).isEqualTo("jsonb");

        // TEXT
        DistributedCache<String, String> text = cache(Function.identity());
        text.put("k", "ünïcödé");
        assertThat(requireNonNull(text.distributedPolicy().getFromStore("k")).value()).isEqualTo("ünïcödé");
        assertThat(columnType(table, "value")).isEqualTo("text");
    }

    @Test
    void an_asynchronous_loading_cache_loads_through_the_store() throws Exception {
        DistributedAsyncLoadingCache<String, String> loading = track(strings(adapter().build())
                .buildAsync((key, executor) -> CompletableFuture.completedFuture("loaded-" + key)));
        DistributedCache<String, String> reader = cache(Function.identity());

        assertThat(loading.get("k").get(10, TimeUnit.SECONDS)).isEqualTo("loaded-k");
        assertThat(reader.getIfPresent("k")).isEqualTo("loaded-k");
    }

    @Test
    void the_listener_can_be_given_as_jdbc_details() {
        DistributedCache<String, String> writer = cache(Function.identity());
        DistributedCache<String, String> holder = track(strings(PostgresAdapter.newBuilder(database.dataSource,
                        "adapter", table)
                .withListener(database.jdbcUrl(), database.username(),
                        database.password())
                .build()).build());
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        writer.put("k", "new");

        await(() -> holder.policy().getIfPresentQuietly("k") == null);
    }

    @Test
    void without_distribution_no_listener_is_needed_and_with_it_a_missing_one_fails_the_build() {
        PostgresAdapter<String, String> withoutListener = PostgresAdapter.newBuilder(
                database.dataSource, "adapter", table).build();
        DistributedCache<String, String> none = track(strings(withoutListener)
                .withDistributionMode(DistributionMode.NONE).build());
        none.put("k", "v");
        assertThat(none.getIfPresent("k")).isEqualTo("v");
        none.close();

        DistributedCaffeine<String, String> builder = strings(withoutListener);
        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no change feed").hasMessageContaining("withListener");
    }

    @Test
    void a_row_of_no_cache_fails_as_the_store() {
        cache(Function.identity()).put("k", "v");
        database.execute("INSERT INTO adapter.\"" + table + "\" (discriminator, id, state, version, write_time) "
                + "VALUES ('" + PostgresAdapter.DEFAULT_DISCRIMINATOR + "', 'foreign', 'UNKNOWN', 5, now())");

        try (var store = adapter().build().createStore(StoreSettings.of(Format.TEXT, Format.TEXT))) {
            assertThatThrownBy(() -> store.read(List.of("foreign"), LONG)).isInstanceOf(StoreException.class)
                    .hasMessageContaining("UNKNOWN");
        }
    }

    @Test
    void a_table_dropped_while_caches_use_it_fails_clearly_and_is_created_when_a_cache_is_built_again() {
        DistributedCache<String, String> cache = cache(Function.identity());
        cache.put("a", "1");

        database.execute("DROP TABLE adapter.\"" + table + "\"");

        assertThatThrownBy(() -> cache.put("a", "2")).isInstanceOfSatisfying(StoreException.class,
                e -> assertThat(e.isRetryable()).isFalse()).hasMessageContaining("does not exist");
        DistributedCache<String, String> rebuilt = cache(Function.identity());
        rebuilt.put("a", "3");
        assertThat(rebuilt.getIfPresent("a")).isEqualTo("3");
    }

    // recovery

    @Test
    void a_broken_listening_session_degrades_reconnects_and_revalidates() {
        DistributedCache<String, String> writer = cache(builder -> builder.withDistributionMode(DistributionMode.NONE));
        DistributedCache<String, String> holder =
                cache(builder -> builder.withCaffeine(CaffeineConfigurer::recordStats));
        writer.put("k", "old");
        assertThat(holder.getIfPresent("k")).isEqualTo("old");

        long interruptions = holder.distributedPolicy().stats().interruptionCount();
        long gaps = holder.distributedPolicy().stats().gapCount();
        database.terminateListeners();
        // degraded from the loss until the resume, which is attempted at once: as short as a reconnect, so seen by
        // its count (beyond the ones of the cache's start), not by polling the state
        await(() -> holder.distributedPolicy().stats().interruptionCount() > interruptions);
        writer.put("k", "written while the session was down");

        await(() -> holder.distributedPolicy().stats().gapCount() > gaps
                && holder.distributedPolicy().getSynchronizationState() == SynchronizationState.SYNCHRONIZED);
        assertThat(holder.getIfPresent("k")).as("checked after the gap").isEqualTo("written while the session was down");
    }

    // schema

    @Test
    void the_table_is_created_with_marker_index_and_columns_and_reused() throws SQLException {
        cache(Function.identity()).put("k", "v");
        cache(Function.identity());

        assertThat(query("SELECT obj_description(to_regclass('adapter.\"" + table + "\"')::oid, 'pg_class')"))
                .isEqualTo(PostgresTable.MARKER);
        assertThat(query("SELECT count(*) FROM pg_indexes WHERE schemaname = 'adapter' AND tablename = '" + table
                + "' AND indexdef LIKE '%(discriminator, state, write_time)%'")).isEqualTo("1");
        assertThat(columnType(table, "key")).isEqualTo("text");
        assertThat(columnType(table, "version")).isEqualTo("bigint");
        assertThat(columnType(table, "stamp")).isEqualTo("bigint");
        assertThat(query("SELECT to_regclass('adapter.\"" + table + "_stamps\"') IS NOT NULL"))
                .as("the stamp sequence, created with the table").isEqualTo("t");
    }

    @Test
    void a_table_without_the_marker_is_refused_unless_it_is_reset() {
        database.execute("CREATE SCHEMA IF NOT EXISTS adapter");
        database.execute("CREATE TABLE adapter.\"" + table + "\" (id text, cache_entry bytea)");

        Function<DistributedCaffeine<String, String>, DistributedCaffeine<String, String>> unchanged =
                Function.identity();
        assertThatThrownBy(() -> cache(unchanged)).isInstanceOf(StoreException.class)
                .hasMessageContaining("withSchemaReset");

        DistributedCache<String, String> reset = track(strings(adapter().withSchemaReset().build()).build());
        reset.put("k", "v");
        assertThat(reset.getIfPresent("k")).isEqualTo("v");
    }

    @Test
    void a_table_created_for_other_serializers_is_refused() {
        cache(Function.identity());

        DistributedCaffeine<String, byte[]> binary = DistributedCaffeine.newBuilder(PostgresAdapter.newBuilder(
                        database.dataSource, "adapter", table).withListener(database.listener).<String, byte[]>build())
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString()))
                .withStore(store -> store.withMaximumAge(LONG).withMaximumSize(100));
        assertThatThrownBy(binary::build)
                .isInstanceOf(StoreException.class).hasMessageContaining("column value is text, expected bytea");
    }

    @Test
    void without_the_right_to_create_the_table_the_start_fails_with_the_ddl() {
        database.execute("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'reader') THEN "
                + "CREATE ROLE reader LOGIN PASSWORD 'reader'; END IF; END $$");
        database.execute("CREATE SCHEMA IF NOT EXISTS adapter");
        database.execute("GRANT USAGE ON SCHEMA adapter TO reader");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername("reader");
        config.setPassword("reader");
        try (HikariDataSource reader = new HikariDataSource(config)) {
            DistributedCaffeine<String, String> builder = strings(PostgresAdapter.newBuilder(reader, "adapter", table)
                    .build()).withDistributionMode(DistributionMode.NONE);
            assertThatThrownBy(builder::build)
                    .isInstanceOf(StoreException.class)
                    .hasMessageContaining("cannot be created")
                    .hasMessageContaining("CREATE TABLE \"adapter\".\"" + table + "\"");
        }
    }

    @Test
    void names_over_63_bytes_are_rejected_and_derived_names_shortened() {
        String long64 = "x".repeat(64);
        assertThatThrownBy(() -> PostgresAdapter.newBuilder(database.dataSource, "adapter", long64))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("63 bytes");
        assertThatThrownBy(() -> PostgresAdapter.newBuilder(database.dataSource, long64, "t"))
                .isInstanceOf(IllegalArgumentException.class);

        PostgresTable longest = new PostgresTable("s".repeat(63), "t".repeat(63), "d".repeat(100), Format.TEXT,
                Format.TEXT);
        assertThat(longest.channel.getBytes(java.nio.charset.StandardCharsets.UTF_8)).hasSizeLessThanOrEqualTo(63);
        assertThat(longest.indexName.getBytes(java.nio.charset.StandardCharsets.UTF_8)).hasSizeLessThanOrEqualTo(63);
        assertThat(new PostgresTable("s".repeat(63), "t".repeat(63), "e".repeat(100), Format.TEXT, Format.TEXT).channel)
                .as("distinct caches, distinct channels").isNotEqualTo(longest.channel);

        // a mixed-case name survives as given
        DistributedCache<String, String> mixed = track(strings(PostgresAdapter.newBuilder(
                database.dataSource, "Adapter", "Mixed" + table).withListener(database.listener).build()).build());
        mixed.put("k", "v");
        assertThat(mixed.getIfPresent("k")).isEqualTo("v");
    }

    private String columnType(String tableName, String column) {
        try {
            return query("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = to_regclass("
                    + "'adapter.\"" + tableName + "\"') AND attname = '" + column + "'");
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private String query(String sql) throws SQLException {
        DataSource dataSource = database.dataSource;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getString(1);
        }
    }
}
