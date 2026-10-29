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
package io.github.oberhoff.distributedcaffeine.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.oberhoff.distributedcaffeine.spi.Store;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings;
import io.github.oberhoff.distributedcaffeine.spi.StoreSettings.Format;
import org.jspecify.annotations.NullMarked;
import org.testcontainers.mariadb.MariaDBContainer;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A MariaDB in a container for the plain JDBC adapter's tests, behind a connection pool as an application would use
 * it: one per JVM (stopped with it), a fresh table per use.
 */
@NullMarked
final class JdbcTestDatabase {

    static final String IMAGE = "mariadb:11.8";
    private static final AtomicInteger TABLES = new AtomicInteger();
    // one per JVM, stopped with it by Testcontainers
    private static final MariaDBContainer CONTAINER = start();
    static final DataSource DATA_SOURCE = dataSource();

    private JdbcTestDatabase() {
    }

    private static MariaDBContainer start() {
        MariaDBContainer container = new MariaDBContainer(IMAGE);
        container.start();
        return container;
    }

    private static DataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(CONTAINER.getJdbcUrl());
        config.setUsername(CONTAINER.getUsername());
        config.setPassword(CONTAINER.getPassword());
        config.setMaximumPoolSize(16);
        return new HikariDataSource(config);
    }

    static String freshTable() {
        return "cache_" + TABLES.incrementAndGet();
    }

    static Store open(String table, String discriminator) {
        return JdbcAdapter.newBuilder(DATA_SOURCE, table).withDiscriminator(discriminator).build()
                .createStore(StoreSettings.of(Format.TEXT, Format.TEXT));
    }
}
