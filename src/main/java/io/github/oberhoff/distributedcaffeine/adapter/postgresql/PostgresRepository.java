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

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.github.oberhoff.distributedcaffeine.adapter.AbstractRepository;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Field;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntry.Status;
import io.github.oberhoff.distributedcaffeine.adapter.CacheEntryMetadata;
import io.github.oberhoff.distributedcaffeine.adapter.SerializerAware;
import io.github.oberhoff.distributedcaffeine.serializer.ByteArraySerializer;
import io.github.oberhoff.distributedcaffeine.serializer.JsonSerializer;
import io.github.oberhoff.distributedcaffeine.serializer.Serializer;
import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import static java.util.stream.Collectors.toUnmodifiableSet;

// Every statement below is assembled from a format string, which is what the SQL inspections of an IDE cannot see
// through: they have no data source to resolve the pieces against, and they read a WHERE that arrives as "%s" as no
// WHERE at all - while in fact every statement here carries one, led by the discriminator
// java:S2077 - the statements here are formatted, but never from anything a caller supplies: a schema and a
// table name are checked against a plain-identifier pattern where they are configured and quoted again here, and
// everything else put into them is a column name taken from an enum. What a caller does supply travels as a
// parameter, which is what every ? in them is for
@SuppressWarnings({"java:S2077", "SqlNoDataSourceInspection", "SqlWithoutWhere"})
final class PostgresRepository<K, V> extends AbstractRepository<K, V> {

    private static final Logger LOGGER = System.getLogger(PostgresRepository.class.getName());
    // 0001-01-01T00:00:00Z, comfortably inside the range the column can represent
    private static final Instant EARLIEST = Instant.ofEpochSecond(-62135596800L);
    // SQL state class 40, transaction rollback: "40001" is a serialization failure and "40P01" a deadlock, and
    // both say the same thing to a caller that can simply ask again
    private static final String TRANSACTION_ROLLBACK = "40";
    // A deadlock is resolved by the server rolling one of the two transactions back, so the one that comes back
    // mostly finds the way clear. Few attempts, because what they are worth falls off sharply and a caller that
    // never gets an answer is worse than one that gets an exception it can log
    private static final int MAXIMUM_ATTEMPTS = 3;
    private static final Duration RETRY_DELAY = Duration.ofMillis(20);
    private static final double RETRY_JITTER_FACTOR = 1.0;

    // what information_schema reports for the types spelled in the DDL
    private static final Map<String, String> TYPE_ALIASES = Map.of("timestamptz", "timestamp with time zone");
    // a record carries one representation of its key and value and nulls for the others, and an operation only
    // while a write of this cache instance is outstanding
    private static final Set<String> NULLABLE_COLUMNS = Stream.concat(
                    Stream.of(OPERATION.toString()),
                    Stream.of(KEY, VALUE).flatMap(field -> Stream.of(Storage.values())
                            .map(storage -> columnName(field, storage))))
            .collect(toUnmodifiableSet());

    private final DataSource dataSource;
    private final String qualifiedTableName;
    private final String schemaName;
    private final String tableName;

    PostgresRepository(DataSource dataSource, String schemaName, String tableName) {
        this.dataSource = dataSource;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.qualifiedTableName = quoted(schemaName) + "." + quoted(tableName);
        ensureTable();
    }

    // In the constructor, where the MongoDB adapter ensures its indexes - except that a table has to exist before
    // anything can be written at all, so a failure here is not recoverable and is raised rather than reported.
    // The shape is checked whether or not this is the one that made the table, because CREATE TABLE IF NOT EXISTS
    // compares nothing: against a table from an older version of this library it does nothing at all and the first
    // write fails instead, somewhere far from here. Checked at construction, the same mismatch names the column
    // that is wrong
    private void ensureTable() {
        try (Connection connection = dataSource.getConnection()) {
            createOrFindTable(connection);
            validateTable(connection);
        } catch (SQLException e) {
            throw new IllegalStateException(format("Preparing the table '%s' failed", qualifiedTableName), e);
        }
    }

    // Creates the table where it may, and settles for finding it where it may not. A role granted only reading and
    // writing cannot create - PostgreSQL refuses CREATE TABLE IF NOT EXISTS on the privilege before it ever looks
    // at whether the table is there - and running the application under such a role against a table a migration
    // owns is an ordinary way to deploy. What matters is that the table is there and shaped right, not who made it,
    // so a refusal is only fatal when the table is missing as well
    private void createOrFindTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            createTable(statement);
        } catch (SQLException e) {
            if (readColumns(connection).isEmpty()) {
                throw new IllegalStateException(format("Creating the table '%s' failed and no table of that "
                        + "name is there to use instead", qualifiedTableName), e);
            }
            LOGGER.log(Level.DEBUG, format("Creating the table '%s' was refused, using the table already "
                    + "there", qualifiedTableName), e);
        }
    }

    private void createTable(Statement statement) throws SQLException {
        // A column per field and storage, of which exactly one is written and the others stay null. A single
        // binary column would take everything, but it would also make unreadable what the MongoDB adapter leaves
        // readable: there a string serializer stores a string and a JSON one stores JSON or a BSON document, so
        // somebody looking at the records sees them. Only bytes are opaque there, and only bytes are opaque here.
        // A null column costs a bit in the null bitmap, which is what makes this affordable.
        // The primary key is what the upsert conflicts on, and it is the same uniqueness the MongoDB adapter
        // enforces with its index: one record per key and scope
        String columns = columnTypes().entrySet().stream()
                .map(column -> format("%s %s%s", quoted(column.getKey()), column.getValue(),
                        NULLABLE_COLUMNS.contains(column.getKey()) ? "" : " NOT NULL"))
                .collect(joining(", "));
        statement.execute(format("CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s, %s))",
                qualifiedTableName, columns, quoted(DISCRIMINATOR_FIELD), quoted(HASH.toString())));
        // serves everything filtering by status, with the timestamp trailing so that a range on it is still
        // covered - the same shape the MongoDB adapter uses, because the maintenance worker asks the same
        // questions of both
        statement.execute(format("CREATE INDEX IF NOT EXISTS %s ON %s (%s, %s, %s)",
                quoted(PostgresIdentifier.limited(tableName + "_status_timestamp_idx")), qualifiedTableName,
                quoted(DISCRIMINATOR_FIELD), quoted(STATUS.toString()), quoted(TIMESTAMP.toString())));
    }

    // Named and typed as the statements here expect them, taken from the same places the statements take them
    // from, so that what is created, what is written and what is checked cannot drift apart
    private static Map<String, String> columnTypes() {
        Map<String, String> columnTypes = new LinkedHashMap<>();
        columnTypes.put(DISCRIMINATOR_FIELD, "text");
        columnTypes.put(HASH.toString(), "text");
        columnTypes.put(OPERATION.toString(), "text");
        Stream.of(KEY, VALUE).forEach(field -> Stream.of(Storage.values())
                .forEach(storage -> columnTypes.put(columnName(field, storage), storage.columnType())));
        columnTypes.put(STATUS.toString(), "text");
        columnTypes.put(TIMESTAMP.toString(), "timestamptz");
        return columnTypes;
    }

    private void validateTable(Connection connection) throws SQLException {
        Map<String, String> actual = readColumns(connection);
        List<String> complaints = columnTypes().entrySet().stream()
                .filter(expected -> !TYPE_ALIASES.getOrDefault(expected.getValue(), expected.getValue())
                        .equals(actual.get(expected.getKey())))
                .map(expected -> format("%s expected as %s but %s", expected.getKey(), expected.getValue(),
                        isNull(actual.get(expected.getKey())) ? "missing" : "found as " + actual.get(expected.getKey())))
                .toList();
        if (!complaints.isEmpty()) {
            throw new IllegalStateException(format("The table '%s' is not shaped as this version expects: %s",
                    qualifiedTableName, String.join(", ", complaints)));
        }
    }

    private Map<String, String> readColumns(Connection connection) throws SQLException {
        Map<String, String> columns = new LinkedHashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT column_name, data_type FROM information_schema.columns "
                        + "WHERE table_schema = ? AND table_name = ?")) {
            statement.setString(1, schemaName);
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    columns.put(resultSet.getString(1), resultSet.getString(2));
                }
            }
        }
        return columns;
    }

    @Override
    public void publishCacheEntries(Collection<CacheEntry<K, V>> cacheEntries) throws Exception {
        if (cacheEntries.isEmpty()) {
            return;
        }
        // ON CONFLICT resolves the collision in the statement, so unlike the MongoDB adapter there is no
        // duplicate-key error to catch and no retry to do. Every column of a field is written, not only the one in
        // use: an upsert replaces the record, so a column left alone would keep what an earlier configuration put
        // there and the record would claim two values at once
        String columns = Stream.concat(
                        Stream.of(DISCRIMINATOR_FIELD, HASH.toString(), OPERATION.toString()),
                        Stream.concat(fieldColumns(), Stream.of(STATUS.toString(), TIMESTAMP.toString())))
                .map(PostgresRepository::quoted)
                .collect(joining(", "));
        String placeholders = Stream.concat(
                        Stream.of("?", "?", "?"),
                        Stream.concat(fieldPlaceholders(), Stream.of("?", "?")))
                .collect(joining(", "));
        String assignments = Stream.concat(
                        Stream.of(OPERATION.toString()),
                        Stream.concat(fieldColumns(), Stream.of(STATUS.toString(), TIMESTAMP.toString())))
                .map(column -> format("%s = EXCLUDED.%s", quoted(column), quoted(column)))
                .collect(joining(", "));
        String sql = format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s, %s) DO UPDATE SET %s",
                qualifiedTableName, columns, placeholders,
                quoted(DISCRIMINATOR_FIELD), quoted(HASH.toString()), assignments);
        retryingRollback(() -> {
            try (Connection connection = dataSource.getConnection()) {
                // the write and the notification of it commit together, so no cache instance is ever told about a
                // record it cannot yet read - and a publish that fails leaves neither behind
                connection.setAutoCommit(false);
                try {
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        for (CacheEntry<K, V> cacheEntry : cacheEntries) {
                            // walked rather than numbered, so that a storage added to the enum widens the columns
                            // and what follows them together instead of silently binding into the wrong ones
                            int index = 1;
                            statement.setString(index++, discriminator);
                            statement.setString(index++, cacheEntry.getHash());
                            statement.setString(index++, cacheEntry.getOperation());
                            index = setField(statement, index, cacheEntry.getKey(), keySerializer);
                            index = setField(statement, index, cacheEntry.getValue(), valueSerializer);
                            statement.setString(index++, cacheEntry.getStatus().toString());
                            statement.setObject(index, toOffsetDateTime(cacheEntry.getTimestamp()),
                                    Types.TIMESTAMP_WITH_TIMEZONE);
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
        });
    }

    private static Stream<String> fieldColumns() {
        return Stream.of(KEY, VALUE)
                .flatMap(field -> Stream.of(Storage.values())
                        .map(storage -> columnName(field, storage)));
    }

    // the JSON column is the store's own representation rather than text that happens to be JSON, so what is bound
    // for it is cast on the way in - which keeps the driver out of this class
    private static Stream<String> fieldPlaceholders() {
        return Stream.of(KEY, VALUE)
                .flatMap(field -> Stream.of(Storage.values())
                        .map(storage -> storage == Storage.JSONB ? "?::jsonb" : "?"));
    }

    // Binds every column of the field and returns where the next one begins: the one the serializer asks for
    // takes the value and the others are nulled, walked in the order the columns were spelled in, which is what
    // ties the two together
    private <T> int setField(PreparedStatement statement, int index, @Nullable T object,
                             Serializer<T, ?> serializer) throws Exception {
        Storage storage = storageOf(serializer);
        Object serialized = SerializerAware.serialize(object, serializer);
        int nextIndex = index;
        for (Storage candidate : Storage.values()) {
            boolean chosen = candidate == storage && nonNull(serialized);
            if (candidate == Storage.BINARY) {
                statement.setBytes(nextIndex++, chosen ? (byte[]) serialized : null);
            } else {
                statement.setString(nextIndex++, chosen ? (String) serialized : null);
            }
        }
        return nextIndex;
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
        return stream(select("*", hashes, statuses, orderByTimestampAsc), hashes, statuses,
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
        return stream(select(projection, hashes, statuses, orderByTimestampAsc), hashes, statuses,
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
        retryingRollback(() -> {
            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);
                try {
                    lockScope(connection);
                    List<String> updated = new ArrayList<>();
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, newStatus.toString());
                        // clearing the operation lets every cache instance apply it, as in the MongoDB adapter
                        statement.setObject(2, toOffsetDateTime(Instant.now()), Types.TIMESTAMP_WITH_TIMEZONE);
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
        });
    }

    @Override
    public void deleteCacheEntries(@Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                   @Nullable Instant olderThan) throws Exception {
        String sql = format("DELETE FROM %s %s", qualifiedTableName, where(hashes, statuses, olderThan));
        retryingRollback(() -> {
            try (Connection connection = dataSource.getConnection()) {
                // a transaction of its own, because the lock below is held for exactly that long
                connection.setAutoCommit(false);
                try {
                    lockScope(connection);
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        bind(connection, statement, 1, hashes, statuses, olderThan);
                        statement.executeUpdate();
                    }
                    connection.commit();
                } catch (Exception e) {
                    connection.rollback();
                    throw e;
                } finally {
                    connection.setAutoCommit(true);
                }
            }
        });
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

    // Serializes the multi-row writes of one scope against each other, which is what keeps them from deadlocking.
    // Six writes sweep these records - two by hash, four by status and age - and their filters lead them through
    // the table in different orders, so two cache instances running maintenance at once can each hold what the
    // other waits for. PostgreSQL breaks that by killing one, which the retry repeats; measured on PostgreSQL 9.5
    // under the stress test, three attempts were not enough. One lock per scope removes the cycle instead of
    // narrowing it, and costs a round trip rather than the sorted sub-select that imposing a lock order would.
    // Keyed on the scope, so only writers over the same records queue - another discriminator touches none of
    // them. Transaction scoped, so a pooled connection cannot be handed back still holding it
    private void lockScope(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
            statement.setLong(1, Long.parseUnsignedLong(PostgresIdentifier.digestOf(identifier, 8), 16));
            statement.execute();
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

    // Without an age to filter by, unlike the writing statements: what reads the store back is asked for hashes or
    // for statuses, never for what is older than a moment
    private String select(String projection, @Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                          boolean orderByTimestampAsc) {
        return format("SELECT %s FROM %s %s%s", projection, qualifiedTableName, where(hashes, statuses, null),
                orderByTimestampAsc ? " ORDER BY " + quoted(TIMESTAMP.toString()) + " ASC" : "");
    }

    // Bound in the order the conditions were spelled in, which is what ties the two together
    private void bind(Connection connection, PreparedStatement statement, int index, @Nullable Set<String> hashes,
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
            statement.setObject(index, toOffsetDateTime(olderThan), Types.TIMESTAMP_WITH_TIMEZONE);
        }
    }

    // Lazily, so that reading the store back does not first copy it into memory. The caller closes the stream,
    // which is what releases the result set, the statement and the connection behind it
    private <T> Stream<T> stream(String sql, @Nullable Set<String> hashes, @Nullable Set<Status> statuses,
                                 RowReader<T> rowReader) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            bind(connection, statement, 1, hashes, statuses, null);
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
                    getField(resultSet, KEY, keySerializer),
                    getField(resultSet, VALUE, valueSerializer),
                    Status.of(requireNonNull(resultSet.getString(STATUS.toString()), "status cannot be null")),
                    requireNonNull(resultSet.getObject(TIMESTAMP.toString(), OffsetDateTime.class),
                            "timestamp cannot be null").toInstant());
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
                    requireNonNull(resultSet.getObject(TIMESTAMP.toString(), OffsetDateTime.class),
                            "timestamp cannot be null").toInstant());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    format("Reading of cache entry metadata failed at '%s'. Skipping...", identifier), e);
            return null;
        }
    }

    // Read from the column this cache instance writes, rather than from whichever one is not null: cache
    // instances sharing a discriminator have to be configured alike, and a read only ever returns records of its
    // own discriminator, so the column follows from the serializer and needs no looking at the record
    private <T> @Nullable T getField(ResultSet resultSet, Field field, Serializer<T, ?> serializer)
            throws Exception {
        Storage storage = storageOf(serializer);
        Object value = storage == Storage.BINARY
                ? resultSet.getBytes(columnName(field, storage))
                : resultSet.getString(columnName(field, storage));
        return SerializerAware.deserialize(value, serializer);
    }

    // Where a serialized object belongs. The MongoDB adapter makes the same distinction by what it puts in the
    // document - a binary, a string, or a BSON document when a JSON serializer asks for it - so the flag that
    // asks for it there is the flag that picks the store's own JSON here
    private static Storage storageOf(Serializer<?, ?> serializer) {
        if (serializer instanceof ByteArraySerializer) {
            return Storage.BINARY;
        }
        if (serializer instanceof JsonSerializer<?> jsonSerializer && jsonSerializer.storeAsBinaryJson()) {
            return Storage.JSONB;
        }
        return Storage.TEXT;
    }

    private static String columnName(Field field, Storage storage) {
        return field + "_" + storage.name().toLowerCase(Locale.ROOT);
    }

    // Runs a write again when the server rolled it back for something other than what it asked for.
    // Every cache instance sweeps the same records during maintenance, and the sweeps take their row locks in
    // whatever order their filters lead them through, so two of them can meet head on and PostgreSQL breaks the
    // deadlock by rolling one back. That is not a failure of the statement: the transaction is gone and nothing
    // of it was committed, and every write here asks for something idempotent - an upsert of a cache entry, a
    // transition of whatever matches to a status, a delete of whatever matches - so asking once more asks for
    // exactly what the first attempt did. The MongoDB adapter has no counterpart because it takes no locks
    // across rows to begin with.
    // The delay is jittered to the full width of itself, which is what keeps two transactions that were rolled
    // back against each other from coming back in step
    private final RetryPolicy<Void> rollbackRetryPolicy = RetryPolicy.<Void>builder()
            .handleIf(throwable -> throwable instanceof SQLException sqlException
                    && isTransactionRollback(sqlException))
            .withMaxAttempts(MAXIMUM_ATTEMPTS)
            .withDelay(RETRY_DELAY)
            .withJitter(RETRY_JITTER_FACTOR)
            .onRetry(event -> LOGGER.log(Level.DEBUG, format("Write was rolled back at '%s'. Retrying...",
                    identifier), event.getLastException()))
            .build();

    // java:S112 - what a repository may throw is Exception, because the Repository SPI says so: a serializer of
    // a caller's choosing sits in the middle of these calls and may raise anything at all. Narrowing it here would
    // narrow the contract rather than the failure
    @SuppressWarnings("java:S112")
    private void retryingRollback(SqlWork work) throws Exception {
        try {
            Failsafe.with(rollbackRetryPolicy).run(work::run);
        } catch (FailsafeException e) {
            // what a caller of a repository is owed is the exception the server raised, not the news that it was
            // attempted more than once - Failsafe wraps a checked one on its way out, so it is unwrapped again
            throw e.getCause() instanceof Exception cause ? cause : e;
        }
    }

    // Walked rather than read off the exception itself, because a batch reports the rollback as one exception
    // with the state on another in its chain
    private static boolean isTransactionRollback(SQLException exception) {
        for (Throwable throwable : exception) {
            if (throwable instanceof SQLException sqlException
                    && nonNull(sqlException.getSQLState())
                    && sqlException.getSQLState().startsWith(TRANSACTION_ROLLBACK)) {
                return true;
            }
        }
        return false;
    }

    // At UTC, deliberately and at every one of the three points a timestamp passes: what the column holds is a
    // point in time rather than a reading of a clock, and saying the offset here is what keeps the machine the
    // statement runs on out of it entirely - where binding a java.sql.Timestamp would have gone through whatever
    // the default time zone of that machine happened to be and arrived at the same place only by cancellation.
    // Clamped as well, because "older than everything" reaches this as a point in time and not as a concept:
    // maintenance expresses it with Instant.ofEpochMilli(Long.MIN_VALUE), a year the column cannot represent. The
    // floor is a date no cache entry can carry, so what the filter selects is unchanged
    private static OffsetDateTime toOffsetDateTime(Instant instant) {
        return (instant.isBefore(EARLIEST) ? EARLIEST : instant).atOffset(ZoneOffset.UTC);
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

    private enum Storage {

        /**
         * What a {@link ByteArraySerializer} produces, which nothing can make legible and which is stored as it is.
         */
        BINARY("bytea"),

        /**
         * What a string serializer produces, and a JSON one that is not asked for the store's own representation.
         */
        TEXT("text"),

        /**
         * What a JSON serializer asks for with {@link JsonSerializer#storeAsBinaryJson()}, which PostgreSQL both
         * validates and can be queried and indexed on. Deliberately {@code jsonb} rather than {@code json}, which
         * is a type of its own that keeps the text as it was written and has to parse it again on every access.
         */
        JSONB("jsonb");

        private final String columnType;

        Storage(String columnType) {
            this.columnType = columnType;
        }

        String columnType() {
            return columnType;
        }
    }

    // java:S112 - as above: this wraps a call that is declared to throw Exception, so it declares the same
    @SuppressWarnings("java:S112")
    @FunctionalInterface
    private interface SqlWork {

        void run() throws Exception;
    }

    @FunctionalInterface
    private interface RowReader<T> {

        @SuppressWarnings("RedundantThrows")
        @Nullable T read(ResultSet resultSet) throws SQLException;
    }
}
