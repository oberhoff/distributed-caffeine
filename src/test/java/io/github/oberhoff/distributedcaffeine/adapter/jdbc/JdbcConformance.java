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
package io.github.oberhoff.distributedcaffeine.adapter.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.AdapterConformanceTest.AdapterConformance;
import io.github.oberhoff.distributedcaffeine.spi.Adapter;
import org.jspecify.annotations.NullMarked;
import org.mariadb.jdbc.MariaDbDataSource;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * MariaDB's and MySQL's side of the conformance suite, on a fresh table of the running test database.
 *
 * @author Andreas Oberhoff
 */
@NullMarked
final class JdbcConformance implements AdapterConformance {

    private final String table = JdbcTestDatabase.freshTable();
    private final List<HikariDataSource> pools = new ArrayList<>();

    @Override
    public Adapter<String, String> adapter(String discriminator) {
        return JdbcTestDatabase.adapter(JdbcTestDatabase.dataSource(), table, discriminator);
    }

    @Override
    public void markOtherVersion() {
        execute("ALTER TABLE " + table + " COMMENT='distributed-caffeine schema 2'");
    }

    @Override
    public boolean putForeignData() {
        execute("CREATE TABLE " + table + " (id VARCHAR(10))");
        execute("INSERT INTO " + table + " VALUES ('foreign')");
        return true;
    }

    @Override
    public Adapter<String, String> unreachable() {
        try {
            return JdbcTestDatabase.adapter(new MariaDbDataSource("jdbc:mariadb://127.0.0.1:1/unreachable"
                    + "?connectTimeout=500"), table, JdbcTestDatabase.DEFAULT_DISCRIMINATOR);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public InetSocketAddress server() {
        return InetSocketAddress.createUnresolved(JdbcTestDatabase.host(), JdbcTestDatabase.port());
    }

    @Override
    public Adapter<String, String> through(String host, int port) {
        return adapterOf(JdbcTestDatabase.pool(host, port, null));
    }

    @Override
    public List<Adapter<String, String>> sharingThrough(String host, int port, int count) {
        HikariDataSource pool = JdbcTestDatabase.pool(host, port, null);
        pools.add(pool);
        List<Adapter<String, String>> adapters = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            adapters.add(JdbcTestDatabase.adapter(pool, table, "shared" + i));
        }
        return adapters;
    }

    @Override
    public Adapter<String, String> wrongCredentials() {
        return adapterOf(JdbcTestDatabase.pool(JdbcTestDatabase.host(), JdbcTestDatabase.port(), "wrong"));
    }

    private Adapter<String, String> adapterOf(HikariDataSource pool) {
        pools.add(pool);
        return JdbcTestDatabase.adapter(pool, table, JdbcTestDatabase.DEFAULT_DISCRIMINATOR);
    }

    @Override
    public void close() {
        pools.forEach(HikariDataSource::close);
    }

    @Override
    public String product() {
        return JdbcTestDatabase.mysql() ? "MySQL" : "MariaDB";
    }

    private static void execute(String sql) {
        try (Connection connection = JdbcTestDatabase.dataSource().getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
