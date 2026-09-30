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
package io.github.oberhoff.distributedcaffeine.adapter.postgresql;

import io.github.oberhoff.distributedcaffeine.adapter.AbstractRepository;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntryMetadata;
import io.github.oberhoff.distributedcaffeine.adapter.SerializerAware;
import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.HASH;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.KEY;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.OPERATION;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.STATUS;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.TIMESTAMP;
import static io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field.VALUE;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

final class PostgresRepository<K, V> extends AbstractRepository<K, V> {

    private static final Logger LOGGER = System.getLogger(PostgresRepository.class.getName());

    private final DataSource dataSource;
    private final String qualifiedTableName;
    private final String tableName;

    PostgresRepository(DataSource dataSource, String schemaName, String tableName) {
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.qualifiedTableName = quoted(schemaName) + "." + quoted(tableName);
        ensureTable();
    }

    // In the constructor, where the MongoDB adapter ensures its indexes - except that a table has to exist before
    // anything can be written at all, so a failure here is not recoverable and is raised rather than reported
    private void ensureTable() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            // the primary key is what the upsert below conflicts on, and it is the same uniqueness the MongoDB
            // adapter enforces with its index: one record per key and scope
            statement.execute(format("""
                    CREATE TABLE IF NOT EXISTS %s (
                        %s text NOT NULL,
                        %s text NOT NULL,
                        %s text,
                        %s bytea,
                        %s bytea,
                        %s text NOT NULL,
                        %s timestamptz NOT NULL,
                        PRIMARY KEY (%s, %s))""",
                    qualifiedTableName,
                    quoted(DISCRIMINATOR_FIELD), quoted(HASH.toString()), quoted(OPERATION.toString()),
                    quoted(KEY.toString()), quoted(VALUE.toString()), quoted(STATUS.toString()),
                    quoted(TIMESTAMP.toString()),
                    quoted(DISCRIMINATOR_FIELD), quoted(HASH.toString())));
            // serves everything filtering by status, with the timestamp trailing so that a range on it is still
            // covered - the same shape the MongoDB adapter uses, because the maintenance worker asks the same
            // questions of both
            statement.execute(format("CREATE INDEX IF NOT EXISTS %s ON %s (%s, %s, %s)",
                    quoted(tableName + "_status_timestamp_idx"), qualifiedTableName,
                    quoted(DISCRIMINATOR_FIELD), quoted(STATUS.toString()), quoted(TIMESTAMP.toString())));
        } catch (SQLException e) {
            throw new IllegalStateException(format("Preparing the table '%s' failed", qualifiedTableName), e);
        }
    }

    @Override
    public void publishCacheEntries(Collection<CacheEntry<K, V>> cacheEntries) throws Exception {
        if (cacheEntries.isEmpty()) {
            return;
        }
        // ON CONFLICT resolves the collision in the statement, so unlike the MongoDB adapter there is no
        // duplicate-key error to catch and no retry to do
        String sql = format("""
                        INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (%s, %s) DO UPDATE SET %s = EXCLUDED.%s, %s = EXCLUDED.%s, \
                        %s = EXCLUDED.%s, %s = EXCLUDED.%s, %s = EXCLUDED.%s""",
                qualifiedTableName,
                quoted(DISCRIMINATOR_FIELD), quoted(HASH.toString()), quoted(OPERATION.toString()),
                quoted(KEY.toString()), quoted(VALUE.toString()), quoted(STATUS.toString()),
                quoted(TIMESTAMP.toString()),
                quoted(DISCRIMINATOR_FIELD), quoted(HASH.toString()),
                quoted(OPERATION.toString()), quoted(OPERATION.toString()),
                quoted(KEY.toString()), quoted(KEY.toString()),
                quoted(VALUE.toString()), quoted(VALUE.toString()),
                quoted(STATUS.toString()), quoted(STATUS.toString()),
                quoted(TIMESTAMP.toString()), quoted(TIMESTAMP.toString()));
        try (Connection connection = dataSource.getConnection()) {
            // the write and the notification of it commit together, so no cache instance is ever told about a
            // record it cannot yet read - and a publish that fails leaves neither behind
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (CacheEntry<K, V> cacheEntry : cacheEntries) {
                        statement.setString(1, discriminator);
                        statement.setString(2, cacheEntry.getHash());
                        statement.setString(3, cacheEntry.getOperation());
                        statement.setBytes(4, toBytes(cacheEntry.getKey(), keySerializer));
                        statement.setBytes(5, toBytes(cacheEntry.getValue(), valueSerializer));
                        statement.setString(6, cacheEntry.getStatus().toString());
                        statement.setTimestamp(7, Timestamp.from(cacheEntry.getTimestamp()));
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                notifyHashes(connection, cacheEntries.stream()
                        .map(CacheEntry::getHash)
                        .distinct()
                        .toList());
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }

    // What distributes a write: the hashes it touched, so that whoever is listening reads exactly those records
    // back rather than looking for what might have changed.
    // Every write that changes what a cache entry says has to come through here. A change stream is a log of
    // everything that happens to a collection, so the MongoDB adapter distributes any write without doing
    // anything about it; a notification carries only what its writer announces, so here it is the writer's job -
    // and a write that forgets is one no other cache instance ever learns of
    private void notifyHashes(Connection connection, List<String> hashes) throws SQLException {
        if (hashes.isEmpty()) {
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_notify(?, ?)")) {
            for (String payload : PostgresChannel.payloadsOf(hashes)) {
                statement.setString(1, PostgresChannel.channelOf(identifier));
                statement.setString(2, payload);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    @Override
    public Stream<CacheEntry<K, V>> streamCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                                       boolean orderByTimestampAsc) throws Exception {
        return stream(select("*", hashes, statuses, null, orderByTimestampAsc), hashes, statuses, null,
                this::toCacheEntryOrNull);
    }

    @Override
    public Stream<CacheEntryMetadata> streamCacheEntryMetadata(@Nullable Set<String> hashes,
                                                               @Nullable Set<Status> statuses,
                                                               boolean orderByTimestampAsc) throws Exception {
        // key and value are the columns this exists to avoid reading at all, so they are left out of the projection
        String projection = Stream.of(HASH, OPERATION, STATUS, TIMESTAMP)
                .map(field -> quoted(field.toString()))
                .collect(joining(", "));
        return stream(select(projection, hashes, statuses, null, orderByTimestampAsc), hashes, statuses, null,
                this::toCacheEntryMetadataOrNull);
    }

    @Override
    public void updateStatusOfCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                           @Nullable Instant olderThan, Status newStatus) throws Exception {
        requireNonNull(newStatus, "newStatus cannot be null");
        // Returning what it touched, because the filter says which records to change and not which ones there
        // were: a transition is what maintenance does instead of deleting precisely so that it reaches every
        // cache instance, so the hashes have to come back to be announced
        String sql = format("UPDATE %s SET %s = ?, %s = NULL, %s = ? %s RETURNING %s", qualifiedTableName,
                quoted(STATUS.toString()), quoted(OPERATION.toString()), quoted(TIMESTAMP.toString()),
                where(hashes, statuses, olderThan), quoted(HASH.toString()));
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                List<String> updated = new ArrayList<>();
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, newStatus.toString());
                    // clearing the operation lets every cache instance apply it, as in the MongoDB adapter
                    statement.setTimestamp(2, Timestamp.from(Instant.now()));
                    bind(connection, statement, 3, hashes, statuses, olderThan);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) {
                            updated.add(resultSet.getString(1));
                        }
                    }
                }
                notifyHashes(connection, updated);
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }

    @Override
    public void deleteCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                   @Nullable Instant olderThan) throws Exception {
        String sql = format("DELETE FROM %s %s", qualifiedTableName, where(hashes, statuses, olderThan));
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(connection, statement, 1, hashes, statuses, olderThan);
            statement.executeUpdate();
        }
    }

    @Override
    public long countCacheEntries(@Nullable Set<Status> statuses) throws Exception {
        String sql = format("SELECT count(*) FROM %s %s", qualifiedTableName, where(null, statuses, null));
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(connection, statement, 1, null, statuses, null);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }

    // Every query filters by the discriminator and by nothing else unconditionally, so it leads the primary key and
    // the index alike, exactly as in the MongoDB adapter
    private String where(@Nullable Set<String> hashes, @Nullable Set<Status> statuses, @Nullable Instant olderThan) {
        List<String> conditions = new ArrayList<>();
        conditions.add(quoted(DISCRIMINATOR_FIELD) + " = ?");
        if (nonNull(hashes)) {
            conditions.add(quoted(HASH.toString()) + " = ANY (?)");
        }
        if (nonNull(statuses)) {
            conditions.add(quoted(STATUS.toString()) + " = ANY (?)");
        }
        if (nonNull(olderThan)) {
            conditions.add(quoted(TIMESTAMP.toString()) + " < ?");
        }
        return "WHERE " + String.join(" AND ", conditions);
    }

    private String select(String projection, @Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                          @Nullable Instant olderThan, boolean orderByTimestampAsc) {
        return format("SELECT %s FROM %s %s%s", projection, qualifiedTableName, where(hashes, statuses, olderThan),
                orderByTimestampAsc ? " ORDER BY " + quoted(TIMESTAMP.toString()) + " ASC" : "");
    }

    private int bind(Connection connection, PreparedStatement statement, int index, @Nullable Set<String> hashes,
                     @Nullable Set<Status> statuses, @Nullable Instant olderThan) throws SQLException {
        statement.setString(index++, discriminator);
        if (nonNull(hashes)) {
            statement.setArray(index++, connection.createArrayOf("text", hashes.toArray()));
        }
        if (nonNull(statuses)) {
            statement.setArray(index++, connection.createArrayOf("text", statuses.stream()
                    .map(Status::toString)
                    .toArray()));
        }
        if (nonNull(olderThan)) {
            statement.setTimestamp(index++, Timestamp.from(olderThan));
        }
        return index;
    }

    // Lazily, so that reading the store back does not first copy it into memory. The caller closes the stream,
    // which is what releases the result set, the statement and the connection behind it
    private <T> Stream<T> stream(String sql, @Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                 @Nullable Instant olderThan, RowReader<T> rowReader) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            bind(connection, statement, 1, hashes, statuses, olderThan);
            ResultSet resultSet = statement.executeQuery();
            Spliterator<T> spliterator = new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE,
                    Spliterator.ORDERED | Spliterator.NONNULL) {
                @Override
                public boolean tryAdvance(Consumer<? super T> action) {
                    try {
                        while (resultSet.next()) {
                            // a row that could not be read is skipped (and logged) rather than breaking the stream,
                            // which is what the contract of a repository asks for
                            T read = rowReader.read(resultSet);
                            if (nonNull(read)) {
                                action.accept(read);
                                return true;
                            }
                        }
                        return false;
                    } catch (SQLException e) {
                        throw new IllegalStateException(e);
                    }
                }
            };
            return StreamSupport.stream(spliterator, false)
                    .onClose(() -> close(resultSet, statement, connection));
        } catch (SQLException e) {
            close(connection);
            throw e;
        }
    }

    private @Nullable CacheEntry<K, V> toCacheEntryOrNull(ResultSet resultSet) {
        try {
            return CacheEntry.of(
                    requireNonNull(resultSet.getString(HASH.toString()), "hash cannot be null"),
                    resultSet.getString(OPERATION.toString()),
                    fromBytes(resultSet.getBytes(KEY.toString()), keySerializer),
                    fromBytes(resultSet.getBytes(VALUE.toString()), valueSerializer),
                    Status.of(requireNonNull(resultSet.getString(STATUS.toString()), "status cannot be null")),
                    requireNonNull(resultSet.getTimestamp(TIMESTAMP.toString()), "timestamp cannot be null")
                            .toInstant());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, format("Reading of cache entry failed at '%s'. Skipping...", identifier), e);
            return null;
        }
    }

    private @Nullable CacheEntryMetadata toCacheEntryMetadataOrNull(ResultSet resultSet) {
        try {
            return CacheEntryMetadata.of(
                    requireNonNull(resultSet.getString(HASH.toString()), "hash cannot be null"),
                    resultSet.getString(OPERATION.toString()),
                    Status.of(requireNonNull(resultSet.getString(STATUS.toString()), "status cannot be null")),
                    requireNonNull(resultSet.getTimestamp(TIMESTAMP.toString()), "timestamp cannot be null")
                            .toInstant());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    format("Reading of cache entry metadata failed at '%s'. Skipping...", identifier), e);
            return null;
        }
    }

    // A serializer produces either bytes or a string, and one column takes both: a string goes in as its UTF-8
    // encoding and comes back decoded, which keeps the column type independent of how a cache is configured
    private static <T> byte @Nullable [] toBytes(@Nullable T object, Serializer<T, ?> serializer) throws Exception {
        Object serialized = SerializerAware.serialize(object, serializer);
        if (isNull(serialized)) {
            return null;
        }
        return serialized instanceof byte[] bytes
                ? bytes
                : ((String) serialized).getBytes(StandardCharsets.UTF_8);
    }

    private static <T> @Nullable T fromBytes(byte @Nullable [] bytes, Serializer<T, ?> serializer) throws Exception {
        if (isNull(bytes)) {
            return null;
        }
        Object value = serializer instanceof ByteArraySerializer
                ? bytes
                : new String(bytes, StandardCharsets.UTF_8);
        return SerializerAware.deserialize(value, serializer);
    }

    // identifiers cannot be parameters, so they are quoted instead - which also keeps them case-sensitive and
    // lets the checks in the builder be the only place that decides what a legal name is
    private static String quoted(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static void close(AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Closing a JDBC resource failed", e);
            }
        }
    }

    @FunctionalInterface
    private interface RowReader<T> {

        @Nullable T read(ResultSet resultSet) throws SQLException;
    }
}
