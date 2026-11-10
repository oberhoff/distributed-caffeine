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
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

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
        DistributedCache<String, String> cache = DistributedCaffeine.newBuilder(adapter)
                .withSerializers(serializers -> serializers.withKeySerializer(StringSerializer.ofString())
                        .withValueSerializer(StringSerializer.ofString()))
                .withStore(store -> store.withOperationTimeout(SHORT))
                .build();
        closeables.add(cache);
        return cache;
    }

    @Test
    void a_store_that_cannot_be_reached_fails_the_build_as_retryable() {
        AdapterConformance store = track(conformance());
        assertThatThrownBy(() -> build(store.unreachable()))
                .isInstanceOfSatisfying(StoreException.class, e -> {
                    assertThat(e.isRetryable()).as("retryable: " + e.getMessage()).isTrue();
                    assertThat(e.isOutcomeUnknown()).as("nothing written").isFalse();
                });
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
