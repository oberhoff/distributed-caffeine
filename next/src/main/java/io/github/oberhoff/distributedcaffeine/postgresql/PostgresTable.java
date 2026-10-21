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

import io.github.oberhoff.distributedcaffeine.spi.Format;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

// One cache's place in PostgreSQL: the quoted table, the discriminator, the column types of its formats, and the
// derived names (index, notification channel), shortened with a hash where PostgreSQL's 63-byte limit requires it.
final class PostgresTable {

    static final int MAXIMUM_NAME_BYTES = 63;
    static final String MARKER = "distributed-caffeine schema 1";

    final String schemaName;
    final String tableName;
    final String discriminator;
    final Format keyFormat;
    final Format valueFormat;
    // "schema"."table", quoted, so case is kept
    final String qualified;
    final String indexName;
    final String channel;
    // the channel of the whole table, whatever the discriminator: what removes every row at once (TRUNCATE, a
    // dropped table recreated) is announced there
    final String tableChannel;
    final String truncateTrigger;
    final String truncateFunction;

    PostgresTable(String schemaName, String tableName, String discriminator, Format keyFormat, Format valueFormat) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.discriminator = discriminator;
        this.keyFormat = keyFormat;
        this.valueFormat = valueFormat;
        this.qualified = quote(schemaName) + "." + quote(tableName);
        this.indexName = derivedName(tableName + "_maintenance");
        this.channel = derivedName("dc_" + schemaName + "." + tableName + "." + discriminator);
        this.tableChannel = derivedName("dc_" + schemaName + "." + tableName);
        this.truncateTrigger = derivedName(tableName + "_truncated");
        this.truncateFunction = quote(schemaName) + "." + quote("dc_truncated");
    }

    static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    // the column type of a format
    static String columnType(Format format) {
        return switch (format) {
            case BINARY -> "bytea";
            case TEXT -> "text";
            case JSON -> "jsonb";
        };
    }

    // a derived name, kept if it fits, otherwise shortened to a prefix and a hash of the whole
    static String derivedName(String name) {
        if (name.getBytes(StandardCharsets.UTF_8).length <= MAXIMUM_NAME_BYTES) {
            return name;
        }
        String hash = sha256(name).substring(0, 16);
        StringBuilder prefix = new StringBuilder();
        for (int codePoint : name.codePoints().toArray()) {
            String next = Character.toString(codePoint);
            if ((prefix + next).getBytes(StandardCharsets.UTF_8).length > MAXIMUM_NAME_BYTES - hash.length() - 1) {
                break;
            }
            prefix.append(next);
        }
        return prefix + "_" + hash;
    }

    private static String sha256(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    // the complete DDL, also for a migration tool or a DBA
    String ddl() {
        return """
                CREATE TABLE %1$s (
                    discriminator text NOT NULL,
                    id text NOT NULL,
                    key %2$s,
                    value %3$s,
                    state text NOT NULL,
                    version bigint NOT NULL,
                    write_time timestamptz NOT NULL,
                    PRIMARY KEY (discriminator, id));
                CREATE INDEX %4$s ON %1$s (discriminator, state, write_time);
                COMMENT ON TABLE %1$s IS '%5$s';
                %6$s""".formatted(qualified, columnType(keyFormat),
                columnType(valueFormat), quote(indexName), MARKER, truncateDdl());
    }

    // TRUNCATE removes every row without a write to announce it: a statement trigger announces it as invalidateAll
    // to every cache of the table (EXECUTE PROCEDURE, which PostgreSQL 9.6 and 10 still need)
    String truncateDdl() {
        return """
                CREATE OR REPLACE FUNCTION %1$s() RETURNS trigger LANGUAGE plpgsql AS $dc$
                BEGIN
                    PERFORM pg_notify(TG_ARGV[0], 'INVALIDATED_ALL||'
                        || (extract(epoch from clock_timestamp()) * 1000000)::bigint);
                    RETURN NULL;
                END $dc$;
                CREATE TRIGGER %2$s AFTER TRUNCATE ON %3$s FOR EACH STATEMENT EXECUTE PROCEDURE %1$s('%4$s');"""
                .formatted(truncateFunction, quote(truncateTrigger), qualified, tableChannel.replace("'", "''"));
    }

    @Override
    public String toString() {
        return schemaName + "." + tableName;
    }
}
