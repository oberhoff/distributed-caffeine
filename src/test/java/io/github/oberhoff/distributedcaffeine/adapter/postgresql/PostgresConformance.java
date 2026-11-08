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

import io.github.oberhoff.distributedcaffeine.AdapterConformanceTest.AdapterConformance;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import org.jspecify.annotations.NullMarked;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * PostgreSQL's side of the conformance suite, on a fresh table of the running test database.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class PostgresConformance implements AdapterConformance {

    private static final String SCHEMA = "conformance";

    private final PostgresTestDatabase database = PostgresTestDatabase.get();
    private final String table = PostgresTestDatabase.freshTable();

    @Override
    public Adapter<String, String> adapter(String discriminator) {
        return PostgresAdapter.newBuilder(database.dataSource, SCHEMA, table)
                .withListenerDataSource(database.listener).withDiscriminator(discriminator).build();
    }

    @Override
    public void markOtherVersion() {
        database.execute("COMMENT ON TABLE " + SCHEMA + ".\"" + table + "\" IS 'distributed-caffeine schema 2'");
    }

    @Override
    public boolean putForeignData() {
        database.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
        database.execute("CREATE TABLE " + SCHEMA + ".\"" + table + "\" (id text)");
        database.execute("INSERT INTO " + SCHEMA + ".\"" + table + "\" VALUES ('foreign')");
        return true;
    }

    @Override
    public Adapter<String, String> unreachable() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl("jdbc:postgresql://127.0.0.1:1/unreachable?connectTimeout=1");
        return PostgresAdapter.newBuilder(dataSource, SCHEMA, table).build();
    }

    @Override
    public String product() {
        return "PostgreSQL";
    }
}
