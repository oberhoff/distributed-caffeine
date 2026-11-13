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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.AdapterConformanceTest.AdapterConformance;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import org.jspecify.annotations.NullMarked;
import org.postgresql.ds.PGSimpleDataSource;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

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
    private final List<HikariDataSource> pools = new ArrayList<>();

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
    public InetSocketAddress server() {
        return InetSocketAddress.createUnresolved(PostgresTestDatabase.HOST, database.port());
    }

    @Override
    public Adapter<String, String> through(String host, int port) {
        PGSimpleDataSource dataSource = dataSource(host, port, PostgresTestDatabase.PASSWORD);
        return PostgresAdapter.newBuilder(dataSource, SCHEMA, table).build();
    }

    @Override
    public List<Adapter<String, String>> sharingThrough(String host, int port, int count) {
        PGSimpleDataSource dataSource = dataSource(host, port, PostgresTestDatabase.PASSWORD);
        List<Adapter<String, String>> adapters = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            adapters.add(PostgresAdapter.newBuilder(dataSource, SCHEMA, table).withDiscriminator("shared" + i).build());
        }
        return adapters;
    }

    // through a pool, which reports the refusal as its own timeout with the server's answer as the cause
    @Override
    public Adapter<String, String> wrongCredentials() {
        HikariConfig config = new HikariConfig();
        config.setDataSource(dataSource(PostgresTestDatabase.HOST, database.port(), "wrong"));
        config.setConnectionTimeout(1_000);
        config.setInitializationFailTimeout(-1);
        HikariDataSource pool = new HikariDataSource(config);
        pools.add(pool);
        return PostgresAdapter.newBuilder(pool, SCHEMA, table).build();
    }

    @Override
    public void close() {
        pools.forEach(HikariDataSource::close);
    }

    // a connection of its own per use, waiting at most a second to connect and to log in: without a login timeout, a
    // server that accepts connections but answers nothing holds the connect far beyond the operation timeout (a
    // minute, measured), which is the driver's and not bounded by the cache
    private static PGSimpleDataSource dataSource(String host, int port, String password) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl("jdbc:postgresql://" + host + ":" + port + "/" + PostgresTestDatabase.DATABASE
                + "?connectTimeout=1&loginTimeout=1");
        dataSource.setUser(PostgresTestDatabase.USER);
        dataSource.setPassword(password);
        return dataSource;
    }

    @Override
    public String product() {
        return "PostgreSQL";
    }
}
